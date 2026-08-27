//! The executor side of an attach: dial a scheduler, run its jobs locally.
//!
//! This is [`Worker`](super::runner::Worker) with storage swapped for a socket.
//! A worker pulls jobs from a [`Scheduler`](crate::scheduler::Scheduler) and
//! pushes results back into it; an executor pulls jobs from a [`FrameReader`]
//! and pushes results out through a [`FrameWriter`]. Everything between is the
//! same [`WorkerDispatcher`] every SDK already implements, so the prefork pool,
//! the Node dispatcher and the Java dispatcher all attach unchanged.
//!
//! Nothing here touches [`Storage`](crate::storage::Storage) — that is the
//! point. The executor image carries app code and no database credentials; the
//! scheduler image carries credentials and no app code. A job frame already
//! holds everything running a task needs.
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use flexiq_core::worker::{ExecutorClient, ExecutorConfig, TcpTransport, WorkerDispatcher};
//! # fn run(dispatcher: Arc<dyn WorkerDispatcher>) -> Result<(), Box<dyn std::error::Error>> {
//! let stream = std::net::TcpStream::connect("scheduler:7777")?;
//! let client = ExecutorClient::connect(
//!     Box::new(TcpTransport::new(stream)?),
//!     ExecutorConfig {
//!         tasks: vec!["resize".to_string()],
//!         slots: 4,
//!         ..ExecutorConfig::new("python", "0.21.0")
//!     },
//! )?;
//! client.spawn(dispatcher).wait();  // until the scheduler ends the session
//! # Ok(())
//! # }
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use tokio::sync::mpsc::error::TrySendError;

use super::auth::Secret;
use super::protocol::{
    decode_step_snapshot, ExecutorMessage, FrameReader, FrameWriter, Incoming, ProtocolError,
    SchedulerMessage, CAP_SIDE_CHANNEL, CAP_STEPS, PROTOCOL_VERSION,
};
use super::transport::{Connection, ReadHalf, Transport, WriteHalf};
use super::WorkerDispatcher;
use crate::error::QueueError;
use crate::job::Job;
use crate::scheduler::JobResult;
use crate::step::{StepFailure, StepLimits, StepSession, StepStore};
use crate::storage::records::{JobStep, NewJobStep, SleepOutcome, StepCommit, StepKind};

/// How often a waiting loop wakes to re-check its condition.
const POLL: Duration = Duration::from_millis(20);

/// Channel for jobs on their way to the pool.
type JobSender = tokio::sync::mpsc::Sender<Job>;

/// Tuning for an [`ExecutorClient`].
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    /// Stable identity announced in `hello`. Two executors attached under one
    /// id is an error on the scheduler, so this must be unique per process.
    pub executor_id: String,
    /// SDK this executor is built on, e.g. `"python"`.
    pub sdk: String,
    /// SDK version string, for the scheduler's inventory and logs.
    pub version: String,
    /// Tasks this executor has handlers for. The scheduler sends it nothing
    /// else, so a name missing here is a job that never arrives.
    pub tasks: Vec<String>,
    /// Jobs this executor can run concurrently.
    pub slots: u32,
    /// Optional behaviours this executor takes part in, announced in `hello`.
    ///
    /// Empty by default: a shell opts in by naming what it has actually wired
    /// up — [`CAP_STEPS`] once its job context can open a step session — and a
    /// scheduler sends nothing that was not claimed.
    pub capabilities: Vec<String>,
    /// Shared secret, when the scheduler requires one.
    pub token: Option<Secret>,
    /// How long the handshake may take before the attach is abandoned.
    pub handshake_timeout: Duration,
    /// How long a frame write may block before the scheduler is treated as
    /// wedged. Bounds the result loop against a peer that stops reading.
    pub write_timeout: Duration,
    /// How often to send a liveness heartbeat.
    pub heartbeat_interval: Duration,
    /// How long a drain waits for in-flight jobs to finish and their results to
    /// reach the scheduler before the connection is closed anyway.
    pub shutdown_drain: Duration,
    /// How long a durable step waits for the scheduler to acknowledge its
    /// commit.
    ///
    /// Bounded further by the job's own `timeout_ms`, when that is shorter:
    /// waiting past the job's deadline only delays a reap already decided.
    /// Running out is a **retryable** failure — an unconfirmed commit is
    /// indistinguishable from one that never happened, and the replay re-runs
    /// the step under the same downstream idempotency key.
    pub step_ack_timeout: Duration,
    /// How many side-channel operations may be queued before the oldest task
    /// log lines are dropped.
    ///
    /// Bounds what a task in a tight progress or logging loop can cost: it
    /// never blocks on the socket, and it never grows this queue without limit.
    pub side_channel_capacity: usize,
}

impl ExecutorConfig {
    /// Defaults for `sdk`/`version`, with a generated executor id.
    ///
    /// `tasks` is empty and `slots` is 1 — both are the caller's to set, and an
    /// executor advertising no tasks is deliberately inert rather than a peer
    /// that quietly receives everything.
    pub fn new(sdk: impl Into<String>, version: impl Into<String>) -> Self {
        let sdk = sdk.into();
        Self {
            executor_id: format!("{sdk}-executor-{}", uuid::Uuid::now_v7()),
            sdk,
            version: version.into(),
            tasks: Vec::new(),
            slots: 1,
            capabilities: Vec::new(),
            token: None,
            handshake_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(5),
            shutdown_drain: Duration::from_secs(30),
            step_ack_timeout: Duration::from_secs(30),
            side_channel_capacity: 4096,
        }
    }
}

/// Why an executor could not attach.
#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    /// The transport could not be dialled, split, or configured.
    #[error("attach transport failed: {0}")]
    Transport(#[from] std::io::Error),

    /// A frame was malformed, or the peer announced a version we do not speak.
    #[error(transparent)]
    Protocol(#[from] ProtocolError),

    /// The scheduler closed the connection instead of acknowledging the
    /// handshake.
    ///
    /// A refused peer never receives an ack, so this is what a rejected
    /// credential looks like from this side. Named rather than surfaced as a
    /// bare I/O error because a wrong or missing attach token is the likeliest
    /// deployment mistake, and "connection reset" would send the operator
    /// looking at the network instead.
    #[error("the scheduler refused the attach (check the attach token)")]
    Refused,
}

/// A completed handshake, not yet running.
///
/// Split from [`ExecutorClient::spawn`] so a caller can report a failed attach
/// — a bad token, an unreachable scheduler — before it builds an execution pool
/// it would only tear down again.
pub struct ExecutorClient {
    config: ExecutorConfig,
    scheduler_id: String,
    peer: String,
    /// What the scheduler said it can do for us. Empty against a scheduler
    /// built before capabilities existed.
    capabilities: Vec<String>,
    reader: FrameReader<ReadHalf>,
    link: Link,
}

impl ExecutorClient {
    /// Dial, handshake, and register with the scheduler on the far end.
    ///
    /// Writes `hello`, then requires `hello_ack` before anything else. The read
    /// is bounded by `handshake_timeout`; the bound is cleared afterwards, or an
    /// executor idling longer than the budget between jobs would tear itself
    /// down.
    pub fn connect(
        transport: Box<dyn Transport>,
        config: ExecutorConfig,
    ) -> Result<Self, ExecutorError> {
        let peer = transport.peer();
        let (read, write, connection) = transport.split()?;
        connection.set_write_timeout(Some(config.write_timeout))?;
        connection.set_read_timeout(Some(config.handshake_timeout))?;

        let mut reader = FrameReader::new(read);
        let mut writer = FrameWriter::new(write);

        writer.write_header(
            &ExecutorMessage::hello(
                config.executor_id.clone(),
                config.sdk.clone(),
                config.version.clone(),
                config.tasks.clone(),
                config.slots,
            )
            .capabilities(config.capabilities.clone())
            .token(config.token.clone())
            .build(),
        )?;

        let (scheduler_id, capabilities) = read_ack(&mut reader)?;
        connection.set_read_timeout(None)?;

        log::info!(
            "[flexiq] executor {} attached to scheduler {scheduler_id} at {peer} with {} slot(s)",
            config.executor_id,
            config.slots
        );
        if !capabilities.iter().any(|cap| cap == CAP_SIDE_CHANNEL) {
            log::info!(
                "[flexiq] scheduler {scheduler_id} advertises no side-channel; progress and task \
                 logs from tasks on executor {} will be dropped",
                config.executor_id
            );
        }

        Ok(Self {
            scheduler_id,
            peer,
            capabilities,
            reader,
            link: Link {
                writer: Mutex::new(writer),
                connection,
            },
            config,
        })
    }

    /// Identity the scheduler announced in its `hello_ack`.
    pub fn scheduler_id(&self) -> &str {
        &self.scheduler_id
    }

    /// Whether the scheduler will apply progress and task logs on our behalf.
    pub fn supports_side_channel(&self) -> bool {
        self.capabilities.iter().any(|cap| cap == CAP_SIDE_CHANNEL)
    }

    /// Whether durable steps work across this attach.
    ///
    /// False against a scheduler whose storage has no step store, or one built
    /// before steps existed. A step is then refused rather than run
    /// un-memoized — see [`ExecutorSteps`].
    pub fn supports_steps(&self) -> bool {
        self.capabilities.iter().any(|cap| cap == CAP_STEPS)
    }

    /// Peer label of the scheduler connection, for logs.
    pub fn peer(&self) -> &str {
        &self.peer
    }

    /// Start running jobs on `dispatcher`.
    ///
    /// Returns immediately; the returned handle is how the caller waits for the
    /// scheduler to end the session, or asks for a drain of its own.
    pub fn spawn(self, dispatcher: Arc<dyn WorkerDispatcher>) -> ExecutorHandle {
        let side_channel = self.supports_side_channel();
        let steps = self.supports_steps();
        let Self {
            config,
            reader,
            link,
            ..
        } = self;

        // Sized off the slot count for the same reason `Worker::spawn` does:
        // enough buffer that a free slot is never left idle waiting on the
        // channel, small enough that the executor is not hoarding jobs it has
        // no capacity to run.
        let capacity = (config.slots as usize).max(1) * 2;
        let (job_tx, job_rx) = tokio::sync::mpsc::channel(capacity);
        let (result_tx, result_rx) = crossbeam_channel::bounded(capacity);
        let (progress_wake, progress_rx) = crossbeam_channel::bounded(1);
        let (log_tx, log_rx) = crossbeam_channel::bounded(config.side_channel_capacity.max(1));

        let shared = Arc::new(Shared {
            link,
            executor_id: config.executor_id,
            slots: config.slots,
            free_slots: AtomicU32::new(config.slots),
            in_flight: AtomicU32::new(0),
            draining: AtomicBool::new(false),
            session_over: AtomicBool::new(false),
            results_flushed: AtomicBool::new(false),
            job_tx: Mutex::new(Some(job_tx)),
            side_channel,
            pending_progress: Mutex::new(HashMap::new()),
            progress_wake,
            log_tx,
            log_shed: log_rx.clone(),
            dropped_logs: AtomicU64::new(0),
            toggles: Mutex::new(HashMap::new()),
            steps,
            snapshots: Mutex::new(HashMap::new()),
            step_acks: Mutex::new(HashMap::new()),
            step_ack_timeout: config.step_ack_timeout,
        });

        let threads = vec![
            spawn_runtime(dispatcher.clone(), job_rx, result_tx),
            spawn_result_loop(shared.clone(), result_rx, progress_rx, log_rx),
            spawn_heartbeat(shared.clone(), config.heartbeat_interval),
            spawn_reader(shared.clone(), reader, dispatcher.clone()),
        ];

        ExecutorHandle {
            shared,
            dispatcher,
            shutdown_drain: config.shutdown_drain,
            threads,
        }
    }
}

/// Read the frame that completes the handshake, returning the scheduler's
/// identity and the optional behaviours it advertised.
fn read_ack(reader: &mut FrameReader<ReadHalf>) -> Result<(String, Vec<String>), ExecutorError> {
    match reader.read::<SchedulerMessage>() {
        Ok((
            SchedulerMessage::HelloAck {
                scheduler_id,
                protocol_version,
                capabilities,
            },
            _,
        )) => {
            if protocol_version != PROTOCOL_VERSION {
                return Err(ProtocolError::VersionMismatch {
                    ours: PROTOCOL_VERSION,
                    theirs: protocol_version,
                }
                .into());
            }
            Ok((scheduler_id, capabilities))
        }
        Ok(_) => Err(ProtocolError::UnexpectedFrame {
            expected: "hello_ack",
        }
        .into()),
        // A refused peer is closed on without an ack, so a clean EOF here is a
        // rejection rather than a transport fault.
        Err(ProtocolError::Eof) => Err(ExecutorError::Refused),
        Err(error) => Err(error.into()),
    }
}

/// Handle to a running executor.
pub struct ExecutorHandle {
    shared: Arc<Shared>,
    dispatcher: Arc<dyn WorkerDispatcher>,
    shutdown_drain: Duration,
    threads: Vec<JoinHandle<()>>,
}

/// A cheap, cloneable view of whether an executor's session is still open.
///
/// [`ExecutorHandle::wait`] consumes the handle, which a shell that needs to
/// observe the session from elsewhere — an async runtime resolving a promise,
/// say — cannot do while it also holds the handle to shut it down.
#[derive(Clone)]
pub struct ExecutorSession {
    shared: Arc<Shared>,
}

impl ExecutorSession {
    /// Whether this executor is still accepting work.
    ///
    /// False once the session ends *or* a local drain starts: a caller parked
    /// in [`ExecutorSession::wait`] has to be released by its own `stop()` too,
    /// and after a drain there is nothing left to wait for.
    pub fn is_running(&self) -> bool {
        !self.shared.session_over.load(Ordering::Acquire)
    }

    /// Block until this executor stops accepting work. Does not drain or join —
    /// that is [`ExecutorHandle::shutdown`]'s job.
    pub fn wait(&self) {
        while self.is_running() {
            thread::sleep(POLL);
        }
    }
}

/// Reports a running task's progress and logs to the scheduler.
///
/// An executor has no storage, so these are the operations it cannot perform
/// itself: the scheduler holds the connection and applies them on its behalf.
/// Cheap to clone and safe to call from any thread — a task body reaches one of
/// these through its SDK's job context.
///
/// Every method is fire-and-forget. Nothing here blocks on the socket, nothing
/// fails a job, and nothing is sent at all when the scheduler did not advertise
/// [`CAP_SIDE_CHANNEL`] — an executor never writes a frame its peer could not
/// understand.
#[derive(Clone)]
pub struct ExecutorSideChannel {
    shared: Arc<Shared>,
}

impl ExecutorSideChannel {
    /// Whether the attached scheduler applies these operations.
    ///
    /// False against a scheduler with no storage configured for it, or one
    /// built before the side-channel existed. Callers do not have to check —
    /// the methods below are no-ops either way — but an SDK can use it to warn
    /// once rather than silently dropping a task's progress bar.
    pub fn is_supported(&self) -> bool {
        self.shared.side_channel
    }

    /// Task log lines shed since this executor attached, because they were
    /// produced faster than they could be framed.
    ///
    /// Progress has no equivalent: it coalesces, so a backlog collapses instead
    /// of losing anything. Exposed because "the log line I wrote is not in the
    /// dashboard" is otherwise indistinguishable from a bug, and an SDK can
    /// surface this as the backpressure signal it is.
    pub fn dropped_task_logs(&self) -> u64 {
        self.shared.dropped_logs.load(Ordering::Relaxed)
    }

    /// Durable inline steps for jobs this executor is running.
    ///
    /// Separate from the rest of this handle because it is the one thing here
    /// that blocks and can fail: a step commit is what a task is waiting on,
    /// and "the write may or may not have landed" means "the step may or may
    /// not re-run".
    pub fn steps(&self) -> ExecutorSteps {
        ExecutorSteps {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Middleware the operator has disabled for a running job's task.
    ///
    /// Resolved by the scheduler at dispatch and carried on the job frame, so
    /// an executor honours a dashboard toggle without a settings read it has no
    /// storage to perform. Empty for an unknown job, or for one with nothing
    /// disabled — the same answer, and the same behaviour, either way.
    pub fn disabled_middleware(&self, job_id: &str) -> Vec<String> {
        self.shared.toggles_for(job_id)
    }

    /// Report a running job's progress (0-100).
    ///
    /// Coalescing: only the newest value per job survives a backlog, which is
    /// exactly right for a value that is idempotent-latest.
    pub fn report_progress(&self, job_id: &str, progress: i32) {
        if !self.shared.side_channel {
            return;
        }
        self.shared.queue_progress(job_id, progress);
    }

    /// Write one structured log line for a running job. `extra` is pre-encoded
    /// JSON; a published partial is this at level `result`.
    ///
    /// Log lines are data and cannot coalesce, so a flood sheds the oldest
    /// rather than growing without bound or blocking the task.
    pub fn write_task_log(
        &self,
        job_id: &str,
        task_name: &str,
        level: &str,
        message: &str,
        extra: Option<&str>,
    ) {
        if !self.shared.side_channel {
            return;
        }
        self.shared.queue_log(PendingLog {
            job_id: job_id.to_string(),
            task_name: task_name.to_string(),
            level: level.to_string(),
            message: message.to_string(),
            extra: extra.map(str::to_string),
        });
    }
}

impl ExecutorHandle {
    /// Id this executor attached under.
    pub fn executor_id(&self) -> &str {
        &self.shared.executor_id
    }

    /// A view another thread can watch the session through.
    pub fn session(&self) -> ExecutorSession {
        ExecutorSession {
            shared: self.shared.clone(),
        }
    }

    /// The handle a running task reports progress and logs through.
    pub fn side_channel(&self) -> ExecutorSideChannel {
        ExecutorSideChannel {
            shared: self.shared.clone(),
        }
    }

    /// Durable inline steps for jobs this executor is running.
    pub fn steps(&self) -> ExecutorSteps {
        ExecutorSteps {
            shared: self.shared.clone(),
        }
    }

    /// Whether this executor is still accepting work.
    pub fn is_running(&self) -> bool {
        !self.shared.session_over.load(Ordering::Acquire)
    }

    /// Block until this executor stops accepting work, then drain and join.
    pub fn wait(self) {
        while self.is_running() {
            thread::sleep(POLL);
        }
        self.teardown();
    }

    /// Block for at most `timeout`, returning whether the session has ended.
    ///
    /// The bounded form exists for shells whose signal handling needs the
    /// calling thread back periodically — a Python `SIGTERM` handler only runs
    /// when the main thread reacquires the GIL, which it cannot do while parked
    /// in [`ExecutorHandle::wait`].
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while self.is_running() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            thread::sleep(POLL.min(remaining));
        }
        true
    }

    /// Ask the scheduler to stop sending work and finish what is in flight.
    ///
    /// Returns without waiting, so it is safe on a signal-handling path.
    /// Idempotent.
    pub fn stop(&self) {
        self.shared.begin_drain();
        self.dispatcher.shutdown();
    }

    /// Drain, disconnect, and join every thread.
    pub fn shutdown(self) {
        self.stop();
        self.teardown();
    }

    /// Wait out the drain, close the connection, and join.
    ///
    /// Closing is what unparks the reader, which is otherwise blocked on a read
    /// only a scheduler frame would satisfy. It must not happen before results
    /// have flushed, or a finished job's outcome is lost and the job waits for
    /// the reaper instead of being recorded.
    fn teardown(mut self) {
        self.stop();

        let deadline = Instant::now() + self.shutdown_drain;
        while Instant::now() < deadline && !self.shared.results_flushed.load(Ordering::Acquire) {
            thread::sleep(POLL);
        }

        let stranded = self.shared.in_flight.load(Ordering::Relaxed);
        if stranded > 0 {
            log::warn!(
                "[flexiq] executor {} did not drain {stranded} job(s) within the shutdown \
                 budget; disconnecting — they will be reaped",
                self.shared.executor_id
            );
        }

        self.shared.link.connection.close();
        // Joining is only safe once the results are out. A task that ignores
        // its cancel keeps the pool's `run` from returning, so its thread never
        // exits — joining it would hang the very shutdown it was asked to
        // bound. Those threads are dropped instead, and the process is free to
        // exit.
        if self.shared.results_flushed.load(Ordering::Acquire) {
            for thread in self.threads.drain(..) {
                if thread.join().is_err() {
                    log::error!(
                        "[flexiq] an executor thread panicked during shutdown of {}",
                        self.shared.executor_id
                    );
                }
            }
        }
        log::info!("[flexiq] executor {} detached", self.shared.executor_id);
    }
}

/// Durable inline steps for an attached executor.
///
/// The executor holds no database credentials, so neither half of a step
/// happens here: the snapshot it replays from rode in on the dispatch, and every
/// commit crosses to the scheduler, which applies the write under the execution
/// claim *it* holds. What is local is everything that decides — step identity,
/// the sequence check, the caps, the divergence rule — because those are
/// [`StepSession`]'s and it runs wherever its store does.
///
/// Cheap to clone and safe to use from any thread. A task body reaches one of
/// these through its SDK's job context.
///
/// Unlike [`ExecutorSideChannel`], nothing here is fire-and-forget: a commit
/// blocks until the scheduler answers it, and a scheduler that never advertised
/// [`CAP_STEPS`] refuses rather than pretending. There is no version of "your
/// charge step silently lost its memo" that beats a failure naming the reason.
#[derive(Clone)]
pub struct ExecutorSteps {
    shared: Arc<Shared>,
}

impl ExecutorSteps {
    /// Whether durable steps work across this attach.
    ///
    /// False against a scheduler whose storage has no step store, or one built
    /// before steps existed. Callers need not check — [`open_session`] refuses
    /// on its own — but an SDK can use it to warn once at attach rather than at
    /// the first `step.run`.
    ///
    /// [`open_session`]: Self::open_session
    pub fn is_supported(&self) -> bool {
        self.shared.steps
    }

    /// Open a step session for a job this executor is running.
    ///
    /// Answers every `step.run` from the snapshot the dispatch carried, and
    /// commits each new one over the connection. `limits` is the shell's copy of
    /// the caps: it is what lets an over-cap step be refused with an error
    /// naming the value the caller passed, but the check that *holds* is the
    /// scheduler's, inside the write's own transaction.
    ///
    /// No `owner` argument, and deliberately: the fence is the scheduler's,
    /// resolved from the dispatch it recorded. An owner this side supplied would
    /// be an owner it could get wrong.
    pub fn open_session(
        &self,
        job: &Job,
        limits: StepLimits,
    ) -> crate::error::Result<StepSession<ExecutorStepStore>> {
        let store = ExecutorStepStore {
            shared: Arc::clone(&self.shared),
            ack_timeout: self.shared.step_ack_timeout.min(job_deadline(job)),
        };
        StepSession::open(store, job, limits)
    }
}

/// How long this job leaves for an ack, from its own execution timeout.
///
/// A commit that outlives the job's deadline is answering into an attempt the
/// scheduler has already decided to reap. `timeout_ms <= 0` means *no* timeout —
/// zero included, which is why this is not a bare `try_from` — so the configured
/// ceiling stands alone.
fn job_deadline(job: &Job) -> Duration {
    if job.timeout_ms <= 0 {
        return Duration::MAX;
    }
    Duration::from_millis(job.timeout_ms.unsigned_abs())
}

/// The [`StepStore`] behind [`ExecutorSteps`]: a socket where a worker would
/// have a database.
///
/// Every write is a request/response, and the answer is what the task is
/// blocked on. Built by [`ExecutorSteps::open_session`].
pub struct ExecutorStepStore {
    shared: Arc<Shared>,
    ack_timeout: Duration,
}

impl StepStore for ExecutorStepStore {
    fn supports_steps(&self) -> bool {
        self.shared.steps
    }

    /// The snapshot the dispatch carried. `namespace` is ignored: the scheduler
    /// scoped the read when it made it, and an executor cannot re-scope a read
    /// it did not perform.
    fn load_steps(
        &self,
        job_id: &str,
        _namespace: Option<&str>,
    ) -> crate::error::Result<Vec<JobStep>> {
        self.shared.snapshot_for(job_id)
    }

    /// `limits` are checked by the session before this is called, and again by
    /// the scheduler inside the write. Nothing to send: the caps are the
    /// operator's, and the operator runs the scheduler.
    fn commit_step(
        &self,
        step: &NewJobStep<'_>,
        _limits: &StepLimits,
        _namespace: Option<&str>,
    ) -> crate::error::Result<StepCommit> {
        let answer = self.shared.commit_step(
            step.job_id,
            step.seq,
            step.step_key,
            StepKind::Run,
            None,
            step.result.unwrap_or(&[]),
            self.ack_timeout,
        )?;
        if !answer.ok {
            return Err(answer.into_error(step.job_id));
        }
        Ok(if answer.already {
            StepCommit::AlreadyCommitted
        } else {
            StepCommit::Committed
        })
    }

    fn commit_sleep(
        &self,
        step: &NewJobStep<'_>,
        wake_at: i64,
        _limits: &StepLimits,
        _namespace: Option<&str>,
    ) -> crate::error::Result<SleepOutcome> {
        let answer = self.shared.commit_step(
            step.job_id,
            step.seq,
            step.step_key,
            StepKind::Sleep,
            Some(wake_at),
            &[],
            self.ack_timeout,
        )?;
        if !answer.ok {
            return Err(answer.into_error(step.job_id));
        }
        // The deadline storage settled on, which on a replay is the stored one
        // rather than the candidate this call proposed. An ack without it is a
        // broken scheduler, not a deadline to invent.
        let settled = answer.wake_at.ok_or_else(|| {
            QueueError::Other(format!(
                "the scheduler acknowledged the sleep of job {} without a deadline",
                step.job_id
            ))
        })?;
        Ok(if answer.already {
            SleepOutcome::AlreadySleeping { wake_at: settled }
        } else {
            SleepOutcome::Slept { wake_at: settled }
        })
    }
}

/// The scheduler connection: one writer, shared, plus its lifetime controls.
struct Link {
    /// Behind a lock because the result loop and the heartbeat both write.
    writer: Mutex<FrameWriter<WriteHalf>>,
    connection: Connection,
}

/// State every thread of a running executor shares.
struct Shared {
    link: Link,
    executor_id: String,
    /// Concurrency announced at handshake; the ceiling `free_slots` counts down
    /// from.
    slots: u32,
    /// Slots not currently occupied, published on each heartbeat.
    free_slots: AtomicU32,
    /// Jobs handed to the pool and not yet answered.
    in_flight: AtomicU32,
    /// Set once no further jobs will be accepted.
    draining: AtomicBool,
    /// Set when this executor stops accepting work — the reader's conversation
    /// ending, or a local drain.
    session_over: AtomicBool,
    /// Set once every result the pool produced has been written.
    results_flushed: AtomicBool,
    /// Dropped by `begin_drain`, which is what lets the pool's `run` return
    /// once it has finished the jobs it already holds. Held in an `Option` so a
    /// local shutdown can release it without waiting on the parked reader.
    job_tx: Mutex<Option<JobSender>>,
    /// Whether the scheduler advertised [`CAP_SIDE_CHANNEL`]. False turns
    /// [`ExecutorSideChannel`] into a no-op, so a frame the peer could not
    /// understand is never written.
    side_channel: bool,
    /// Job id → newest progress not yet framed. Progress is idempotent-latest,
    /// so a backlog collapses instead of growing.
    pending_progress: Mutex<HashMap<String, i32>>,
    /// Capacity 1: one pending wake-up already covers every value in the map.
    progress_wake: Sender<()>,
    log_tx: Sender<PendingLog>,
    /// A clone of the result loop's receiver, used only to shed the head of a
    /// full queue.
    log_shed: Receiver<PendingLog>,
    /// Log lines dropped under backpressure, reported once the session ends.
    dropped_logs: AtomicU64,
    /// Job id → middleware the scheduler resolved as disabled for it.
    ///
    /// Kept beside the job rather than on it because a toggle list is dashboard
    /// state, not a job column — the same reason [`CancelSignals`] holds
    /// frame-delivered cancels instead of the job carrying a flag. Entries are
    /// released when the job reports, so the map cannot grow for the life of
    /// the process.
    ///
    /// [`CancelSignals`]: super::cancel::CancelSignals
    toggles: Mutex<HashMap<String, Vec<String>>>,
    /// Whether the scheduler advertised [`CAP_STEPS`]. False refuses every
    /// durable step rather than running it un-memoized: an executor with no
    /// channel to commit on cannot make a step durable, and a step that
    /// silently loses its memo re-runs a charge.
    steps: bool,
    /// Job id → the steps its dispatch carried, decoded once on arrival.
    ///
    /// The error is kept rather than discarded: a snapshot that would not
    /// decode must fail the step, and "no steps recorded" is precisely the
    /// wrong answer. A job with no `job_steps` frame is simply absent here,
    /// which *is* an empty snapshot.
    snapshots: Mutex<HashMap<String, std::result::Result<Vec<JobStep>, String>>>,
    /// `(job_id, seq)` → where that commit's ack goes.
    ///
    /// Keyed on both because one executor runs many jobs concurrently; the
    /// pairing should not depend on a job having only one step in flight, even
    /// though it does. Cleared when the reader's conversation ends, which turns
    /// every waiter's park into a disconnect instead of a full timeout.
    step_acks: Mutex<HashMap<(String, i32), Sender<StepAnswer>>>,
    /// Ceiling on how long a commit waits for its ack.
    step_ack_timeout: Duration,
}

/// A scheduler's answer to one step commit.
#[derive(Debug, Clone)]
struct StepAnswer {
    ok: bool,
    already: bool,
    wake_at: Option<i64>,
    error: Option<String>,
    failure: Option<StepFailure>,
}

impl StepAnswer {
    /// Rebuild the error a refusal represents, so the shell above sees the same
    /// classification an in-process worker would.
    ///
    /// Only the message and the classification cross the wire, so the *variant*
    /// is not reconstructed: a divergence and a cap violation both arrive as
    /// [`QueueError::StepRefused`], which
    /// [`classify_step_failure`](crate::step::classify_step_failure) answers
    /// `Permanent` for, exactly as it does for the originals. The two cases a
    /// shell branches on independently — §3 divergence and §4 caps — are caught
    /// locally by the session before a frame is ever written, so they never take
    /// this path.
    fn into_error(self, job_id: &str) -> QueueError {
        let message = self.error.unwrap_or_else(|| {
            format!("the scheduler refused a step commit for job {job_id} without saying why")
        });
        match self.failure.unwrap_or(StepFailure::Retryable) {
            StepFailure::Superseded => QueueError::ClaimLost(job_id.to_string()),
            StepFailure::Permanent => QueueError::StepRefused(message),
            StepFailure::Retryable => QueueError::Other(message),
        }
    }
}

/// One task log line waiting to be framed to the scheduler.
struct PendingLog {
    job_id: String,
    task_name: String,
    level: String,
    message: String,
    extra: Option<String>,
}

impl Shared {
    /// Send one frame, logging rather than propagating: every caller is a
    /// worker thread whose only remedy is to stop, which the reader's own EOF
    /// already handles.
    fn send(&self, frame: &ExecutorMessage, payload: &[u8]) -> bool {
        let sent = self
            .link
            .writer
            .lock()
            .unwrap_or_else(recover)
            .write(frame, payload);
        match sent {
            Ok(()) => true,
            Err(error) => {
                log::warn!(
                    "[flexiq] executor {} failed to send a frame: {error}",
                    self.executor_id
                );
                false
            }
        }
    }

    /// Stop accepting work, announcing it in-protocol first.
    ///
    /// A heartbeat may only *shrink* the scheduler's view of capacity
    /// (`remote.rs`), so zeroing it is a standing "send me nothing more" that
    /// needs no new frame type. Dropping the job sender then lets the pool
    /// finish what it holds and return. Idempotent — a local signal and a
    /// `shutdown` frame can both arrive.
    fn begin_drain(&self) {
        if self.draining.swap(true, Ordering::AcqRel) {
            return;
        }
        self.free_slots.store(0, Ordering::Relaxed);
        self.job_tx.lock().unwrap_or_else(recover).take();
        // Release anyone parked in `wait`: the reader is still blocked on a read
        // only the scheduler could satisfy, so nothing else would wake them.
        self.session_over.store(true, Ordering::Release);
        // Announced last, because a scheduler that stopped reading blocks this
        // write for up to `write_timeout`. `stop` is documented as safe on a
        // signal-handling path, so the local drain must not wait on the peer.
        self.send(&ExecutorMessage::Heartbeat { free_slots: 0 }, &[]);
        log::info!(
            "[flexiq] executor {} draining; no further jobs will be accepted",
            self.executor_id
        );
    }

    /// A sender for the pool, or `None` once draining.
    fn job_sender(&self) -> Option<JobSender> {
        self.job_tx.lock().unwrap_or_else(recover).clone()
    }

    fn job_started(&self) {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        self.publish_free();
    }

    /// Saturating, so a result for a job this executor never counted cannot
    /// wrap the counter and strand the drain forever.
    fn job_finished(&self) {
        let _ = self
            .in_flight
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |running| {
                Some(running.saturating_sub(1))
            });
        self.publish_free();
    }

    /// Record the toggle list a dispatch carried.
    fn remember_toggles(&self, job_id: &str, disabled: Vec<String>) {
        // An empty list is the common case and the default answer, so storing
        // it would only grow the map for nothing.
        if disabled.is_empty() {
            return;
        }
        self.toggles
            .lock()
            .unwrap_or_else(recover)
            .insert(job_id.to_string(), disabled);
    }

    /// Middleware disabled for a running job, empty when there are none.
    fn toggles_for(&self, job_id: &str) -> Vec<String> {
        self.toggles
            .lock()
            .unwrap_or_else(recover)
            .get(job_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Release everything a finished job's dispatch carried beside the job
    /// itself — its toggle list and its step snapshot — so neither map grows
    /// for the life of the process.
    fn forget_dispatch(&self, job_id: &str) {
        self.toggles.lock().unwrap_or_else(recover).remove(job_id);
        self.snapshots.lock().unwrap_or_else(recover).remove(job_id);
    }

    /// Decode and keep the snapshot a `job_steps` frame carried.
    ///
    /// Decoded here, once, rather than when a session opens: a snapshot that
    /// will not parse is a fact about the dispatch, and finding out at the
    /// first `step.run` would be finding out after the job started.
    fn remember_snapshot(&self, job_id: String, payload: Vec<u8>) {
        let decoded = decode_step_snapshot(&job_id, &payload).map_err(|error| {
            log::warn!("[flexiq] executor {} {error}", self.executor_id);
            error.to_string()
        });
        self.snapshots
            .lock()
            .unwrap_or_else(recover)
            .insert(job_id, decoded);
    }

    /// The steps a job's dispatch carried. Absent means an empty snapshot: the
    /// scheduler sends no frame for a job with no steps.
    fn snapshot_for(&self, job_id: &str) -> crate::error::Result<Vec<JobStep>> {
        match self.snapshots.lock().unwrap_or_else(recover).get(job_id) {
            None => Ok(Vec::new()),
            Some(Ok(steps)) => Ok(steps.clone()),
            Some(Err(reason)) => Err(QueueError::Other(reason.clone())),
        }
    }

    /// Hand one ack to whoever is blocked on it.
    fn deliver_ack(&self, job_id: String, seq: i32, answer: StepAnswer) {
        let waiter = self
            .step_acks
            .lock()
            .unwrap_or_else(recover)
            .remove(&(job_id.clone(), seq));
        match waiter {
            // Capacity 1 and one send per waiter, so this cannot block.
            Some(tx) => {
                let _ = tx.send(answer);
            }
            // A duplicate ack, or one for a commit that already timed out. The
            // attempt has moved on either way, and the commit stays durable.
            None => log::debug!(
                "[flexiq] executor {} received a step ack for job {job_id} step {seq} that \
                 nothing was waiting on",
                self.executor_id
            ),
        }
    }

    /// Release everyone blocked on an ack, because none is coming.
    ///
    /// Dropping the senders turns each waiter's park into a disconnect rather
    /// than a full `step_ack_timeout` of silence.
    fn abandon_acks(&self) {
        self.step_acks.lock().unwrap_or_else(recover).clear();
    }

    /// Frame one step commit and block until the scheduler answers it.
    ///
    /// Blocking is the point: an unconfirmed commit is indistinguishable from
    /// one that never happened, and carrying on past it would re-run the step
    /// on the next attempt with the side effect already applied.
    #[allow(clippy::too_many_arguments)]
    fn commit_step(
        &self,
        job_id: &str,
        seq: i32,
        step_key: &str,
        kind: StepKind,
        wake_at: Option<i64>,
        result: &[u8],
        ack_timeout: Duration,
    ) -> crate::error::Result<StepAnswer> {
        if !self.steps {
            // Retryable on purpose: a heterogeneous fleet mid-rollout may place
            // the next attempt somewhere that can commit. Exhausting the
            // retries dead-letters it with an error naming the capability.
            return Err(QueueError::Other(format!(
                "step '{step_key}' of job {job_id} cannot be committed: the scheduler this \
                 executor is attached to offers no step store"
            )));
        }

        let (tx, rx) = crossbeam_channel::bounded(1);
        // Registered before the frame goes out, or a fast scheduler could
        // answer before there is anything to answer to.
        let _waiter = AckWaiter::register(self, job_id, seq, tx);

        let frame = ExecutorMessage::StepCommit {
            job_id: job_id.to_string(),
            seq,
            step_key: step_key.to_string(),
            kind,
            wake_at,
            payload_len: result.len(),
        };
        if !self.send(&frame, result) {
            return Err(QueueError::Other(format!(
                "step '{step_key}' of job {job_id} could not be sent to the scheduler"
            )));
        }

        match rx.recv_timeout(ack_timeout) {
            Ok(answer) => Ok(answer),
            Err(RecvTimeoutError::Timeout) => Err(QueueError::Other(format!(
                "the scheduler did not acknowledge step '{step_key}' of job {job_id} within \
                 {ack_timeout:?}"
            ))),
            Err(RecvTimeoutError::Disconnected) => Err(QueueError::Other(format!(
                "the connection to the scheduler ended before step '{step_key}' of job {job_id} \
                 was acknowledged"
            ))),
        }
    }

    /// Record the newest progress for a job and ask the result loop to frame it.
    ///
    /// The value lands in the map before the wake-up, so a full wake channel
    /// loses nothing: the flush already pending will pick this value up.
    fn queue_progress(&self, job_id: &str, progress: i32) {
        self.pending_progress
            .lock()
            .unwrap_or_else(recover)
            .insert(job_id.to_string(), progress);
        let _ = self.progress_wake.try_send(());
    }

    /// Queue one log line, shedding the oldest when the queue is full.
    ///
    /// Never blocks: a task in a logging loop must not be able to stall on the
    /// socket, so backpressure is paid in dropped lines rather than in latency.
    fn queue_log(&self, line: PendingLog) {
        let mut line = line;
        loop {
            match self.log_tx.try_send(line) {
                Ok(()) => return,
                Err(crossbeam_channel::TrySendError::Disconnected(_)) => return,
                Err(crossbeam_channel::TrySendError::Full(rejected)) => {
                    line = rejected;
                    // Drop-oldest, not drop-newest: when a task floods, the
                    // lines nearest the present are the ones worth keeping. A
                    // failed shed means a slot just freed, so the retry fits.
                    if self.log_shed.try_recv().is_ok() {
                        self.dropped_logs.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    /// Frame every progress value recorded since the last flush.
    fn flush_progress(&self) -> bool {
        let batch = std::mem::take(&mut *self.pending_progress.lock().unwrap_or_else(recover));
        for (job_id, progress) in batch {
            if !self.send(&ExecutorMessage::Progress { job_id, progress }, &[]) {
                return false;
            }
        }
        true
    }

    /// Frame one queued log line.
    fn send_log(&self, line: &PendingLog) -> bool {
        let (frame, payload) = ExecutorMessage::task_log(
            line.job_id.as_str(),
            line.task_name.as_str(),
            line.level.as_str(),
            line.message.as_str(),
            line.extra.as_deref(),
        );
        self.send(&frame, &payload)
    }

    /// Recompute free capacity from the in-flight count, unless draining — a
    /// drain has pinned it to zero, and a job finishing must not undo that.
    fn publish_free(&self) {
        if self.draining.load(Ordering::Acquire) {
            return;
        }
        let running = self.in_flight.load(Ordering::Relaxed);
        self.free_slots
            .store(self.slots.saturating_sub(running), Ordering::Relaxed);
    }
}

/// Recover a guard from a poisoned lock instead of cascading the panic. The
/// state behind it is a frame writer, which stays usable.
fn recover<T>(poisoned: PoisonError<T>) -> T {
    poisoned.into_inner()
}

/// Keeps the ack table honest: one registration, removed however the commit
/// that made it returns.
///
/// A leaked entry would route a later commit's ack — `(job_id, seq)` repeats
/// across attempts — to a channel nobody is listening on.
struct AckWaiter<'a> {
    shared: &'a Shared,
    key: (String, i32),
}

impl<'a> AckWaiter<'a> {
    fn register(shared: &'a Shared, job_id: &str, seq: i32, tx: Sender<StepAnswer>) -> Self {
        let key = (job_id.to_string(), seq);
        shared
            .step_acks
            .lock()
            .unwrap_or_else(recover)
            .insert(key.clone(), tx);
        Self { shared, key }
    }
}

impl Drop for AckWaiter<'_> {
    fn drop(&mut self) {
        self.shared
            .step_acks
            .lock()
            .unwrap_or_else(recover)
            .remove(&self.key);
    }
}

/// Runtime thread: drives the pool, exactly as `Worker::spawn` does.
///
/// `result_tx` moves in, so the result loop sees a disconnect the moment
/// execution is finished.
fn spawn_runtime(
    dispatcher: Arc<dyn WorkerDispatcher>,
    job_rx: tokio::sync::mpsc::Receiver<Job>,
    result_tx: Sender<JobResult>,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name("flexiq-executor-runtime".to_string())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("tokio runtime construction cannot fail with these settings");
            runtime.block_on(async move { dispatcher.run(job_rx, result_tx).await });
        })
        .expect("spawning the executor runtime thread cannot fail with a valid name")
}

/// Reader thread: the scheduler's half of the conversation.
fn spawn_reader(
    shared: Arc<Shared>,
    mut reader: FrameReader<ReadHalf>,
    dispatcher: Arc<dyn WorkerDispatcher>,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name("flexiq-executor-reader".to_string())
        .spawn(move || {
            // Once per type per session: a newer scheduler may send the frame we
            // cannot read on every dispatch.
            let mut reported_unknown: HashSet<String> = HashSet::new();
            loop {
                match reader.read_or_skip::<SchedulerMessage>() {
                    Ok(Incoming::Known(SchedulerMessage::Shutdown, _)) => {
                        log::info!(
                            "[flexiq] scheduler asked executor {} to shut down",
                            shared.executor_id
                        );
                        break;
                    }
                    Ok(Incoming::Known(SchedulerMessage::Cancel { job_id }, _)) => {
                        dispatcher.notify_cancel(&job_id);
                    }
                    // Arrives immediately before the job frame it belongs to,
                    // so a handler reaching for a memo on its first line already
                    // finds it.
                    Ok(Incoming::Known(SchedulerMessage::JobSteps { job_id, .. }, payload)) => {
                        shared.remember_snapshot(job_id, payload);
                    }
                    Ok(Incoming::Known(
                        SchedulerMessage::StepAck {
                            job_id,
                            seq,
                            ok,
                            already,
                            wake_at,
                            error,
                            failure,
                        },
                        _,
                    )) => shared.deliver_ack(
                        job_id,
                        seq,
                        StepAnswer {
                            ok,
                            already,
                            wake_at,
                            error,
                            failure,
                        },
                    ),
                    Ok(Incoming::Known(frame, payload)) => accept_job(&shared, frame, payload),
                    // A frame from a scheduler newer than this executor. The
                    // session is otherwise fine, and ending it would fail every
                    // job in flight over a frame this build never needed.
                    Ok(Incoming::Unknown { frame_type }) => {
                        if reported_unknown.insert(frame_type.clone()) {
                            log::warn!(
                                "[flexiq] scheduler sent a '{frame_type}' frame executor {} does \
                                 not know; ignoring it (upgrade the executor to use it)",
                                shared.executor_id
                            );
                        }
                    }
                    Err(ProtocolError::Eof) => {
                        log::info!(
                            "[flexiq] scheduler closed the connection to executor {}",
                            shared.executor_id
                        );
                        break;
                    }
                    // A closed connection during teardown lands here; the
                    // session is over either way.
                    Err(error) => {
                        if !shared.draining.load(Ordering::Acquire) {
                            log::warn!(
                                "[flexiq] executor {} read error: {error}",
                                shared.executor_id
                            );
                        }
                        break;
                    }
                }
            }

            // Whatever ended the loop, no more jobs are coming — and no ack
            // is either, so anyone parked on one is released now rather than
            // waiting out a full timeout for silence.
            shared.abandon_acks();
            shared.begin_drain();
            dispatcher.shutdown();
            shared.session_over.store(true, Ordering::Release);
        })
        .expect("spawning the executor reader thread cannot fail with a valid name")
}

/// Hand one dispatched job to the pool.
///
/// A job that cannot be run — the executor is draining, or the pool has already
/// stopped — is answered with a retryable failure rather than dropped. The
/// scheduler's reaper would recover it either way, but only after a reap cycle,
/// and the race is expected: a `job` already in flight when the zero-capacity
/// heartbeat lands is normal, not a fault.
fn accept_job(shared: &Arc<Shared>, frame: SchedulerMessage, payload: Vec<u8>) {
    let Some(dispatch) = frame.into_dispatch(payload) else {
        // `hello_ack` is the only frame left, and it is handshake-only.
        log::warn!(
            "[flexiq] executor {} received a handshake frame mid-stream",
            shared.executor_id
        );
        return;
    };
    let job = dispatch.job;

    // Recorded before the job is handed over, so a handler that reaches for the
    // toggle list on its very first line already finds it.
    shared.remember_toggles(&job.id, dispatch.disabled_middleware);

    let Some(sender) = shared.job_sender() else {
        decline(shared, &job, "the executor is draining");
        return;
    };

    shared.job_started();
    // `try_send` rather than a blocking send: the reader also carries cancels,
    // and parking it on a full channel would stall them behind the very jobs
    // they target. The channel holds twice the advertised slots and the
    // scheduler reserves a slot before dispatching, so a correct peer cannot
    // fill it.
    match sender.try_send(job) {
        Ok(()) => {}
        Err(TrySendError::Full(job)) => {
            shared.job_finished();
            decline(shared, &job, "the executor pool is saturated");
        }
        Err(TrySendError::Closed(job)) => {
            shared.job_finished();
            decline(shared, &job, "the executor pool has stopped");
        }
    }
}

/// Answer a job this executor will not run with a retryable failure, so the
/// scheduler reschedules it now instead of waiting for a reap.
fn decline(shared: &Arc<Shared>, job: &Job, reason: &str) {
    log::warn!(
        "[flexiq] executor {} declining job {}: {reason}",
        shared.executor_id,
        job.id
    );
    // This job reports here instead of through `send_result`, so the entry
    // `accept_job` recorded before it knew the job would be declined has to be
    // released on this path too.
    shared.forget_dispatch(&job.id);
    let (frame, payload) = ExecutorMessage::from_job_result(JobResult::Failure {
        job_id: job.id.clone(),
        error: format!("executor did not run '{}': {reason}", job.task_name),
        retry_count: job.retry_count,
        max_retries: job.max_retries,
        task_name: job.task_name.clone(),
        wall_time_ns: 0,
        should_retry: true,
        timed_out: false,
    });
    shared.send(&frame, &payload);
}

/// Result thread: every outcome the pool produces, framed back to the
/// scheduler, plus the side-channel traffic riding the same writer.
///
/// One thread for both because the writer is one lock: a task thread that
/// framed its own progress would contend with results for it, and could park a
/// task body on a wedged scheduler for the whole write timeout. Queueing
/// instead keeps the task's call free and the socket single-writer.
///
/// Ends when the pool has dropped its result sender and the queue is empty,
/// which is what releases the teardown — a result written after the socket
/// closed would be a job silently lost.
fn spawn_result_loop(
    shared: Arc<Shared>,
    result_rx: Receiver<JobResult>,
    progress_rx: Receiver<()>,
    log_rx: Receiver<PendingLog>,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name("flexiq-executor-results".to_string())
        .spawn(move || {
            let mut sending = true;
            while sending {
                crossbeam_channel::select! {
                    recv(result_rx) -> result => match result {
                        Ok(result) => {
                            // Everything the task already reported goes out
                            // first. The scheduler drops a side-channel frame
                            // for a job it no longer holds, so a result that
                            // overtook them would silently lose the task's
                            // final progress and its last log lines — the two
                            // it is most likely to care about.
                            sending = flush_side_channel(&shared, &log_rx)
                                && send_result(&shared, result);
                        }
                        Err(_) => break,
                    },
                    recv(progress_rx) -> woken => match woken {
                        Ok(()) => sending = shared.flush_progress(),
                        Err(_) => break,
                    },
                    recv(log_rx) -> line => match line {
                        Ok(line) => sending = shared.send_log(&line),
                        Err(_) => break,
                    },
                    // Bounded so a session that ends with every queue quiet is
                    // still noticed promptly.
                    default(POLL) => {}
                }
            }

            // Best-effort tail: what a task already reported belongs on the
            // wire, but only while the connection is good, and never at the
            // cost of holding the teardown open.
            if sending {
                flush_side_channel(&shared, &log_rx);
            }

            let dropped = shared.dropped_logs.load(Ordering::Relaxed);
            if dropped > 0 {
                log::warn!(
                    "[flexiq] executor {} dropped {dropped} task log line(s) that were produced \
                     faster than they could be sent",
                    shared.executor_id
                );
            }
            shared.results_flushed.store(true, Ordering::Release);
        })
        .expect("spawning the executor result thread cannot fail with a valid name")
}

/// Frame every side-channel operation queued so far.
///
/// Bounded work: both queues are capped, and each entry is one small frame on
/// an already-open socket. Returns whether the connection is still usable.
fn flush_side_channel(shared: &Arc<Shared>, log_rx: &Receiver<PendingLog>) -> bool {
    if !shared.flush_progress() {
        return false;
    }
    for line in log_rx.try_iter() {
        if !shared.send_log(&line) {
            return false;
        }
    }
    true
}

/// Frame one job outcome. Returns whether the connection is still usable.
fn send_result(shared: &Arc<Shared>, result: JobResult) -> bool {
    shared.forget_dispatch(result.job_id());
    let (frame, payload) = ExecutorMessage::from_job_result(result);
    let sent = shared.send(&frame, &payload);
    shared.job_finished();
    sent
}

/// Heartbeat thread: liveness plus current free capacity.
///
/// Stops once draining — the zero-capacity heartbeat `begin_drain` sent is the
/// last thing the scheduler needs to hear, and repeating it would only contend
/// for the writer while results are trying to flush.
fn spawn_heartbeat(shared: Arc<Shared>, interval: Duration) -> JoinHandle<()> {
    thread::Builder::new()
        .name("flexiq-executor-heartbeat".to_string())
        .spawn(move || loop {
            // Slept in slices so a drain is observed promptly rather than one
            // full interval late.
            let deadline = Instant::now() + interval;
            while Instant::now() < deadline {
                if shared.draining.load(Ordering::Acquire) {
                    return;
                }
                thread::sleep(POLL.min(deadline.saturating_duration_since(Instant::now())));
            }
            let free_slots = shared.free_slots.load(Ordering::Relaxed);
            if !shared.send(&ExecutorMessage::Heartbeat { free_slots }, &[]) {
                return;
            }
        })
        .expect("spawning the executor heartbeat thread cannot fail with a valid name")
}
