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
//! # use taskito_core::worker::{ExecutorClient, ExecutorConfig, TcpTransport, WorkerDispatcher};
//! # fn run(dispatcher: Arc<dyn WorkerDispatcher>) -> Result<(), Box<dyn std::error::Error>> {
//! let stream = std::net::TcpStream::connect("scheduler:7749")?;
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

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use tokio::sync::mpsc::error::TrySendError;

use super::auth::Secret;
use super::protocol::{
    ExecutorMessage, FrameReader, FrameWriter, ProtocolError, SchedulerMessage, PROTOCOL_VERSION,
};
use super::transport::{Connection, ReadHalf, Transport, WriteHalf};
use super::WorkerDispatcher;
use crate::job::Job;
use crate::scheduler::JobResult;

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
            token: None,
            handshake_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(5),
            shutdown_drain: Duration::from_secs(30),
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

        writer.write_header(&ExecutorMessage::Hello {
            executor_id: config.executor_id.clone(),
            sdk: config.sdk.clone(),
            version: config.version.clone(),
            tasks: config.tasks.clone(),
            slots: config.slots,
            protocol_version: PROTOCOL_VERSION,
            token: config.token.clone(),
        })?;

        let scheduler_id = read_ack(&mut reader)?;
        connection.set_read_timeout(None)?;

        log::info!(
            "[taskito] executor {} attached to scheduler {scheduler_id} at {peer} with {} slot(s)",
            config.executor_id,
            config.slots
        );

        Ok(Self {
            scheduler_id,
            peer,
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

    /// Peer label of the scheduler connection, for logs.
    pub fn peer(&self) -> &str {
        &self.peer
    }

    /// Start running jobs on `dispatcher`.
    ///
    /// Returns immediately; the returned handle is how the caller waits for the
    /// scheduler to end the session, or asks for a drain of its own.
    pub fn spawn(self, dispatcher: Arc<dyn WorkerDispatcher>) -> ExecutorHandle {
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
        });

        let threads = vec![
            spawn_runtime(dispatcher.clone(), job_rx, result_tx),
            spawn_result_loop(shared.clone(), result_rx),
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

/// Read the frame that completes the handshake.
fn read_ack(reader: &mut FrameReader<ReadHalf>) -> Result<String, ExecutorError> {
    match reader.read::<SchedulerMessage>() {
        Ok((
            SchedulerMessage::HelloAck {
                scheduler_id,
                protocol_version,
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
            Ok(scheduler_id)
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
    /// Whether the scheduler session is still open.
    pub fn is_running(&self) -> bool {
        !self.shared.session_over.load(Ordering::Acquire)
    }

    /// Block until the scheduler ends the session. Does not drain or join —
    /// that is [`ExecutorHandle::shutdown`]'s job.
    pub fn wait(&self) {
        while self.is_running() {
            thread::sleep(POLL);
        }
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

    /// Whether the scheduler session is still open.
    pub fn is_running(&self) -> bool {
        !self.shared.session_over.load(Ordering::Acquire)
    }

    /// Block until the scheduler ends the session, then drain and join.
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
                "[taskito] executor {} did not drain {stranded} job(s) within the shutdown \
                 budget; disconnecting — they will be reaped",
                self.shared.executor_id
            );
        }

        self.shared.link.connection.close();
        for thread in self.threads.drain(..) {
            if thread.join().is_err() {
                log::error!(
                    "[taskito] an executor thread panicked during shutdown of {}",
                    self.shared.executor_id
                );
            }
        }
        log::info!("[taskito] executor {} detached", self.shared.executor_id);
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
    /// Set when the reader's conversation with the scheduler has ended.
    session_over: AtomicBool,
    /// Set once every result the pool produced has been written.
    results_flushed: AtomicBool,
    /// Dropped by `begin_drain`, which is what lets the pool's `run` return
    /// once it has finished the jobs it already holds. Held in an `Option` so a
    /// local shutdown can release it without waiting on the parked reader.
    job_tx: Mutex<Option<JobSender>>,
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
                    "[taskito] executor {} failed to send a frame: {error}",
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
        self.send(&ExecutorMessage::Heartbeat { free_slots: 0 }, &[]);
        self.job_tx.lock().unwrap_or_else(recover).take();
        log::info!(
            "[taskito] executor {} draining; no further jobs will be accepted",
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
        .name("taskito-executor-runtime".to_string())
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
        .name("taskito-executor-reader".to_string())
        .spawn(move || {
            loop {
                match reader.read::<SchedulerMessage>() {
                    Ok((SchedulerMessage::Shutdown, _)) => {
                        log::info!(
                            "[taskito] scheduler asked executor {} to shut down",
                            shared.executor_id
                        );
                        break;
                    }
                    Ok((SchedulerMessage::Cancel { job_id }, _)) => {
                        dispatcher.notify_cancel(&job_id);
                    }
                    Ok((frame, payload)) => accept_job(&shared, frame, payload),
                    Err(ProtocolError::Eof) => {
                        log::info!(
                            "[taskito] scheduler closed the connection to executor {}",
                            shared.executor_id
                        );
                        break;
                    }
                    // A closed connection during teardown lands here; the
                    // session is over either way.
                    Err(error) => {
                        if !shared.draining.load(Ordering::Acquire) {
                            log::warn!(
                                "[taskito] executor {} read error: {error}",
                                shared.executor_id
                            );
                        }
                        break;
                    }
                }
            }

            // Whatever ended the loop, no more jobs are coming.
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
    let Some(job) = frame.into_job(payload) else {
        // `hello_ack` is the only frame left, and it is handshake-only.
        log::warn!(
            "[taskito] executor {} received a handshake frame mid-stream",
            shared.executor_id
        );
        return;
    };

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
        "[taskito] executor {} declining job {}: {reason}",
        shared.executor_id,
        job.id
    );
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
/// scheduler.
///
/// Ends when the pool has dropped its sender and the queue is empty, which is
/// what releases the teardown — a result written after the socket closed would
/// be a job silently lost.
fn spawn_result_loop(shared: Arc<Shared>, result_rx: Receiver<JobResult>) -> JoinHandle<()> {
    thread::Builder::new()
        .name("taskito-executor-results".to_string())
        .spawn(move || {
            loop {
                match result_rx.recv_timeout(POLL) {
                    Ok(result) => {
                        let (frame, payload) = ExecutorMessage::from_job_result(result);
                        let sent = shared.send(&frame, &payload);
                        shared.job_finished();
                        if !sent {
                            break;
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }
            shared.results_flushed.store(true, Ordering::Release);
        })
        .expect("spawning the executor result thread cannot fail with a valid name")
}

/// Heartbeat thread: liveness plus current free capacity.
///
/// Stops once draining — the zero-capacity heartbeat `begin_drain` sent is the
/// last thing the scheduler needs to hear, and repeating it would only contend
/// for the writer while results are trying to flush.
fn spawn_heartbeat(shared: Arc<Shared>, interval: Duration) -> JoinHandle<()> {
    thread::Builder::new()
        .name("taskito-executor-heartbeat".to_string())
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
