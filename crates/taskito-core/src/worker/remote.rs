//! Dispatch to executors attached over a [`Transport`].
//!
//! An executor dials in, announces the tasks it can run, and receives jobs for
//! those tasks only. The scheduler is untouched: this is a [`WorkerDispatcher`]
//! like any other, so the same claim, retry, and reaper machinery applies.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use crossbeam_channel::{Receiver, Sender, TrySendError};
use tokio::sync::Notify;

use super::protocol::{
    ExecutorMessage, FrameReader, FrameWriter, ProtocolError, SchedulerMessage, PROTOCOL_VERSION,
};
use super::transport::{Connection, ReadHalf, Transport, WriteHalf};
use super::WorkerDispatcher;
use crate::job::Job;
use crate::scheduler::JobResult;

/// Tuning for a [`RemoteDispatcher`].
#[derive(Debug, Clone)]
pub struct RemoteConfig {
    /// Identity announced in `hello_ack`.
    pub scheduler_id: String,
    /// How long the handshake may take before the connection is dropped, so a
    /// peer that connects and says nothing cannot pin an attach.
    pub handshake_timeout: Duration,
    /// How long a job waits for a slot on an executor advertising its task
    /// before it is failed back retryably.
    pub placement_timeout: Duration,
    /// How long a dispatch write may block before the executor is treated as
    /// wedged. Bounds the dispatch thread against a peer that stops reading.
    pub write_timeout: Duration,
    /// How long shutdown waits for attached executors to finish in-flight
    /// jobs before their connections are closed.
    pub shutdown_drain: Duration,
    /// Capacity of the cancel side-channel.
    pub cancel_capacity: usize,
}

impl Default for RemoteConfig {
    fn default() -> Self {
        Self {
            scheduler_id: format!("scheduler-{}", uuid::Uuid::now_v7()),
            handshake_timeout: Duration::from_secs(10),
            placement_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            shutdown_drain: Duration::from_secs(30),
            cancel_capacity: 1024,
        }
    }
}

/// Why an executor could not attach.
#[derive(Debug, thiserror::Error)]
pub enum AttachError {
    /// The transport could not be split or configured.
    #[error("attach transport failed: {0}")]
    Transport(#[from] std::io::Error),

    /// The handshake was malformed, timed out, or announced a version we do
    /// not speak ([`ProtocolError::VersionMismatch`]).
    #[error(transparent)]
    Protocol(#[from] ProtocolError),

    /// Another executor is already attached under this id.
    #[error("executor {0} is already attached")]
    DuplicateId(String),

    /// The dispatcher is shutting down and accepts no new executors.
    #[error("dispatcher is shutting down")]
    ShuttingDown,
}

/// A snapshot of one attached executor.
#[derive(Debug, Clone)]
pub struct AttachedExecutor {
    /// Identity the executor announced.
    pub executor_id: String,
    /// SDK it is built on.
    pub sdk: String,
    /// SDK version string.
    pub version: String,
    /// Tasks it advertised, sorted.
    pub tasks: Vec<String>,
    /// Concurrency it advertised.
    pub slots: u32,
    /// Slots free right now.
    pub free_slots: u32,
    /// Jobs currently running on it.
    pub in_flight: usize,
    /// Peer label, for logs.
    pub peer: String,
    /// Milliseconds since its last frame.
    pub idle_ms: u32,
}

/// Total advertised capacity across attached executors.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Capacity {
    /// Executors attached.
    pub executors: usize,
    /// Slots advertised in total.
    pub total_slots: u32,
    /// Slots free right now.
    pub free_slots: u32,
}

/// How often the shutdown drain re-checks whether executors have finished.
const DRAIN_POLL: Duration = Duration::from_millis(50);

/// Recover a guard from a poisoned lock instead of cascading the panic. The
/// state behind these locks is plain bookkeeping, so reading it stays safe.
fn recover<T>(poisoned: PoisonError<T>) -> T {
    poisoned.into_inner()
}

/// A [`WorkerDispatcher`] that runs jobs on executors attached over a socket.
///
/// Executors dial out, so the app needs no inbound port. Binding and accepting
/// belong to the caller: hand each accepted connection to [`Self::attach`].
/// Cloning is cheap and shares one registry, so a listener thread can hold a
/// handle while the scheduler owns the dispatcher.
#[derive(Clone)]
pub struct RemoteDispatcher {
    shared: Arc<Shared>,
}

impl RemoteDispatcher {
    /// Build a dispatcher with no executors attached yet.
    pub fn new(config: RemoteConfig) -> Self {
        Self {
            shared: Arc::new(Shared {
                config,
                executors: Mutex::new(HashMap::new()),
                capacity_changed: Notify::new(),
                result_tx: Mutex::new(None),
                cancel_tx: Mutex::new(None),
                readers: Mutex::new(Vec::new()),
                shutdown: AtomicBool::new(false),
                started_at: Instant::now(),
            }),
        }
    }

    /// Complete the handshake on `transport` and register the executor.
    pub fn attach(&self, transport: Box<dyn Transport>) -> Result<String, AttachError> {
        self.shared.attach(transport)
    }

    /// Snapshot every attached executor.
    pub fn executors(&self) -> Vec<AttachedExecutor> {
        self.shared.snapshot()
    }

    /// Advertised capacity across all attached executors.
    ///
    /// A scheduler sizes `SchedulerConfig::max_in_flight` from this rather than
    /// running a second, parallel limiter.
    pub fn capacity(&self) -> Capacity {
        self.shared.capacity()
    }
}

#[async_trait]
impl WorkerDispatcher for RemoteDispatcher {
    async fn run(&self, job_rx: tokio::sync::mpsc::Receiver<Job>, result_tx: Sender<JobResult>) {
        self.shared.clone().run(job_rx, result_tx).await;
    }

    fn shutdown(&self) {
        self.shared.shutdown.store(true, Ordering::SeqCst);
    }

    fn notify_cancel(&self, job_id: &str) {
        self.shared.notify_cancel(job_id);
    }
}

/// One attached executor: what it can run, what it is running, and how to
/// reach it.
struct Executor {
    id: String,
    sdk: String,
    version: String,
    tasks: HashSet<String>,
    slots: u32,
    free: AtomicU32,
    /// Job id → task name. Taking an entry is the exactly-once token for
    /// emitting that job's single `JobResult`.
    in_flight: Mutex<HashMap<String, String>>,
    writer: Mutex<FrameWriter<WriteHalf>>,
    connection: Connection,
    peer: String,
    /// Milliseconds since the dispatcher started, at the last frame read.
    last_seen_ms: AtomicU32,
}

impl Executor {
    fn is_busy(executor: &Arc<Self>) -> bool {
        !executor.in_flight.lock().unwrap_or_else(recover).is_empty()
    }

    fn snapshot(&self, now_ms: u32) -> AttachedExecutor {
        let mut tasks: Vec<String> = self.tasks.iter().cloned().collect();
        tasks.sort();
        AttachedExecutor {
            executor_id: self.id.clone(),
            sdk: self.sdk.clone(),
            version: self.version.clone(),
            tasks,
            slots: self.slots,
            free_slots: self.free.load(Ordering::Relaxed),
            in_flight: self.in_flight.lock().unwrap_or_else(recover).len(),
            peer: self.peer.clone(),
            idle_ms: now_ms.saturating_sub(self.last_seen_ms.load(Ordering::Relaxed)),
        }
    }
}

/// Where a job can go right now.
enum Placement {
    /// An executor advertising the task has a free slot, now reserved.
    Ready(Arc<Executor>),
    /// Some executor advertises the task but all of them are busy.
    Saturated,
    /// No attached executor advertises the task at all.
    Unadvertised,
}

/// State shared by the dispatcher handle, its reader threads, and its router.
struct Shared {
    config: RemoteConfig,
    executors: Mutex<HashMap<String, Arc<Executor>>>,
    /// Woken when an executor attaches, frees a slot, or detaches — the signal
    /// a job waiting for placement is parked on.
    capacity_changed: Notify,
    /// Installed by `run`. Reader threads may start before it exists, but no
    /// job can be dispatched until it does.
    result_tx: Mutex<Option<Sender<JobResult>>>,
    cancel_tx: Mutex<Option<Sender<String>>>,
    readers: Mutex<Vec<JoinHandle<()>>>,
    shutdown: AtomicBool,
    started_at: Instant,
}

impl Shared {
    /// Handshake and register. The ack is sent even when the version is
    /// rejected, so both ends log both versions instead of one side guessing.
    fn attach(self: &Arc<Self>, transport: Box<dyn Transport>) -> Result<String, AttachError> {
        if self.shutdown.load(Ordering::SeqCst) {
            return Err(AttachError::ShuttingDown);
        }
        let peer = transport.peer();
        let (read, write, connection) = transport.split()?;
        connection.set_write_timeout(Some(self.config.write_timeout))?;
        let mut reader = FrameReader::new(read);
        let mut writer = FrameWriter::new(write);

        // Bound the handshake only. An attached executor waiting between jobs
        // must block indefinitely, or it would be dropped every time it idled
        // past this budget.
        connection.set_read_timeout(Some(self.config.handshake_timeout))?;
        let hello = reader.read::<ExecutorMessage>()?.0;
        connection.set_read_timeout(None)?;
        let ExecutorMessage::Hello {
            executor_id,
            sdk,
            version,
            tasks,
            slots,
            protocol_version,
        } = hello
        else {
            return Err(ProtocolError::UnexpectedFrame { expected: "hello" }.into());
        };

        writer.write_header(&SchedulerMessage::HelloAck {
            scheduler_id: self.config.scheduler_id.clone(),
            protocol_version: PROTOCOL_VERSION,
        })?;

        if protocol_version != PROTOCOL_VERSION {
            log::warn!(
                "[taskito] rejecting executor {executor_id} ({sdk} {version}, {peer}): \
                 speaks worker protocol {protocol_version}, we speak {PROTOCOL_VERSION}"
            );
            return Err(ProtocolError::VersionMismatch {
                ours: PROTOCOL_VERSION,
                theirs: protocol_version,
            }
            .into());
        }

        let executor = Arc::new(Executor {
            id: executor_id.clone(),
            sdk,
            version,
            tasks: tasks.into_iter().collect(),
            slots,
            free: AtomicU32::new(slots),
            in_flight: Mutex::new(HashMap::new()),
            writer: Mutex::new(writer),
            connection,
            peer: peer.clone(),
            last_seen_ms: AtomicU32::new(self.elapsed_ms()),
        });

        {
            let mut executors = self.executors.lock().unwrap_or_else(recover);
            // Re-check under the registry lock: `drain_and_close` empties this
            // map, so an attach racing it would leave an executor nobody ever
            // shuts down or joins.
            if self.shutdown.load(Ordering::SeqCst) {
                return Err(AttachError::ShuttingDown);
            }
            if executors.contains_key(&executor_id) {
                return Err(AttachError::DuplicateId(executor_id));
            }
            executors.insert(executor_id.clone(), executor.clone());
        }

        let handle = Arc::clone(self).spawn_reader(executor, reader);
        {
            let mut readers = self.readers.lock().unwrap_or_else(recover);
            // Reap handles of already-detached executors so a reconnect loop
            // cannot grow this vector for the life of the process.
            readers.retain(|reader| !reader.is_finished());
            readers.push(handle);
        }
        self.capacity_changed.notify_waiters();

        log::info!("[taskito] executor {executor_id} attached from {peer} with {slots} slot(s)");
        Ok(executor_id)
    }

    fn snapshot(&self) -> Vec<AttachedExecutor> {
        let now = self.elapsed_ms();
        let executors = self.executors.lock().unwrap_or_else(recover);
        let mut snapshot: Vec<AttachedExecutor> =
            executors.values().map(|e| e.snapshot(now)).collect();
        snapshot.sort_by(|a, b| a.executor_id.cmp(&b.executor_id));
        snapshot
    }

    fn capacity(&self) -> Capacity {
        let executors = self.executors.lock().unwrap_or_else(recover);
        executors
            .values()
            .fold(Capacity::default(), |mut acc, executor| {
                acc.executors += 1;
                acc.total_slots += executor.slots;
                acc.free_slots += executor.free.load(Ordering::Relaxed);
                acc
            })
    }

    /// Milliseconds since the dispatcher was created — a monotonic clock for
    /// liveness that a wall-clock jump cannot move backwards.
    fn elapsed_ms(&self) -> u32 {
        self.started_at
            .elapsed()
            .as_millis()
            .min(u128::from(u32::MAX)) as u32
    }

    async fn run(
        self: Arc<Self>,
        mut job_rx: tokio::sync::mpsc::Receiver<Job>,
        result_tx: Sender<JobResult>,
    ) {
        *self.result_tx.lock().unwrap_or_else(recover) = Some(result_tx);

        let (cancel_tx, cancel_rx) = crossbeam_channel::bounded(self.config.cancel_capacity);
        self.set_cancel_sender(Some(cancel_tx));
        let cancel_router = Arc::clone(&self).spawn_cancel_router(cancel_rx);

        while let Some(job) = job_rx.recv().await {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }
            self.place(job).await;
        }

        // Stop accepting cancels so the router drains and exits while the
        // writers it uses are still alive.
        self.set_cancel_sender(None);
        self.drain_and_close().await;

        let readers = std::mem::take(&mut *self.readers.lock().unwrap_or_else(recover));
        for handle in readers {
            let _ = handle.join();
        }
        let _ = cancel_router.join();
    }

    /// Place one job, waiting for a slot if every advertising executor is busy.
    ///
    /// A job for a task nobody advertises, or one that waits out
    /// `placement_timeout`, comes back as a retryable failure: it reschedules
    /// under the normal retry policy and surfaces the misconfiguration rather
    /// than hiding it. One unplaceable job blocks the ones behind it, since the
    /// scheduler hands over a single job stream — the mitigation is per-task
    /// `max_concurrent`, which gates before the job is ever dequeued.
    async fn place(&self, job: Job) {
        let deadline = Instant::now() + self.config.placement_timeout;
        loop {
            // Register the waiter *before* checking capacity: `notify_waiters`
            // only wakes waiters already registered, so subscribing lazily
            // would lose a slot freed between the check and the await.
            let mut changed = std::pin::pin!(self.capacity_changed.notified());
            changed.as_mut().enable();

            let reason = match self.try_acquire(&job.task_name) {
                Placement::Ready(executor) => {
                    self.dispatch_to(&executor, job);
                    return;
                }
                Placement::Saturated => "every executor advertising it is busy",
                Placement::Unadvertised => "no attached executor advertises it",
            };

            if self.shutdown.load(Ordering::Relaxed) {
                self.fail_unplaceable(&job, "the dispatcher is shutting down");
                return;
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() || tokio::time::timeout(remaining, changed).await.is_err() {
                self.fail_unplaceable(&job, reason);
                return;
            }
        }
    }

    /// Pick the executor with the most free slots that advertises `task_name`,
    /// reserving one of its slots.
    fn try_acquire(&self, task_name: &str) -> Placement {
        let executors = self.executors.lock().unwrap_or_else(recover);
        let mut advertised = false;
        let mut best: Option<&Arc<Executor>> = None;

        for executor in executors.values() {
            if !executor.tasks.contains(task_name) {
                continue;
            }
            advertised = true;
            let free = executor.free.load(Ordering::Relaxed);
            if free == 0 {
                continue;
            }
            if best.is_none_or(|current| free > current.free.load(Ordering::Relaxed)) {
                best = Some(executor);
            }
        }

        match best {
            // Reserve under the registry lock so a later placement already
            // sees this job's slot as taken.
            Some(executor) => {
                executor.free.fetch_sub(1, Ordering::Relaxed);
                Placement::Ready(executor.clone())
            }
            None if advertised => Placement::Saturated,
            None => Placement::Unadvertised,
        }
    }

    /// Send a reserved job to its executor.
    ///
    /// Registers the job before writing so a fast executor cannot return a
    /// result the reader can't pair with an in-flight entry. A failed write
    /// means the connection is gone: the slot is released, the executor is
    /// dropped, and the job is left to the scheduler's reaper — the same
    /// recovery path a mid-job executor crash takes.
    fn dispatch_to(&self, executor: &Arc<Executor>, job: Job) {
        executor
            .in_flight
            .lock()
            .unwrap_or_else(recover)
            .insert(job.id.clone(), job.task_name.clone());

        let write = executor
            .writer
            .lock()
            .unwrap_or_else(recover)
            .write_job(&job);

        if let Err(e) = write {
            executor
                .in_flight
                .lock()
                .unwrap_or_else(recover)
                .remove(&job.id);
            executor.free.fetch_add(1, Ordering::Relaxed);
            log::error!(
                "[taskito] failed to send job {} to executor {}: {e}; will be reaped",
                job.id,
                executor.id
            );
            self.deregister(&executor.id);
        }
    }

    /// Hand a job that never reached an executor back as a retryable failure.
    fn fail_unplaceable(&self, job: &Job, reason: &str) {
        let error = format!("task '{}' was not dispatched: {reason}", job.task_name);
        log::warn!("[taskito] {error} (job {})", job.id);
        self.emit(JobResult::Failure {
            job_id: job.id.clone(),
            error,
            retry_count: job.retry_count,
            max_retries: job.max_retries,
            task_name: job.task_name.clone(),
            wall_time_ns: 0,
            should_retry: true,
            timed_out: false,
        });
    }

    /// Send a result to the scheduler, if `run` has installed the channel.
    fn emit(&self, result: JobResult) {
        let sender = self
            .result_tx
            .lock()
            .unwrap_or_else(recover)
            .as_ref()
            .cloned();
        match sender {
            Some(tx) => {
                if tx.send(result).is_err() {
                    log::debug!("[taskito] result channel closed; dropping executor result");
                }
            }
            None => log::warn!("[taskito] result arrived before the dispatcher started; dropping"),
        }
    }

    /// Reader thread for one executor: results in, capacity updates, and
    /// deregistration when the connection ends.
    fn spawn_reader(
        self: Arc<Self>,
        executor: Arc<Executor>,
        mut reader: FrameReader<ReadHalf>,
    ) -> JoinHandle<()> {
        thread::Builder::new()
            .name(format!("taskito-executor-{}", executor.id))
            .spawn(move || {
                loop {
                    match reader.read::<ExecutorMessage>() {
                        Ok((message, payload)) => {
                            executor
                                .last_seen_ms
                                .store(self.elapsed_ms(), Ordering::Relaxed);
                            self.handle_frame(&executor, message, payload);
                        }
                        Err(ProtocolError::Eof) => {
                            log::info!("[taskito] executor {} disconnected", executor.id);
                            break;
                        }
                        Err(e) => {
                            log::warn!("[taskito] executor {} read error: {e}", executor.id);
                            break;
                        }
                    }
                }
                self.abandon(&executor);
            })
            .expect("failed to spawn executor reader thread")
    }

    /// Process one frame from an executor.
    fn handle_frame(&self, executor: &Arc<Executor>, message: ExecutorMessage, payload: Vec<u8>) {
        if let ExecutorMessage::Heartbeat { free_slots } = message {
            // Local accounting is exact, so a heartbeat may only *shrink*
            // capacity — an executor shedding slots — never invent it.
            let _ = executor
                .free
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some(current.min(free_slots))
                });
            return;
        }

        let Some(result) = message.into_job_result(payload) else {
            log::warn!(
                "[taskito] executor {} sent a handshake frame mid-stream",
                executor.id
            );
            return;
        };

        // Taking the in-flight entry is the exactly-once token: a duplicate or
        // unknown result has no entry to take and is dropped.
        let known = executor
            .in_flight
            .lock()
            .unwrap_or_else(recover)
            .remove(result.job_id())
            .is_some();
        if !known {
            log::warn!(
                "[taskito] executor {} returned a result for unknown job {}",
                executor.id,
                result.job_id()
            );
            return;
        }

        executor.free.fetch_add(1, Ordering::Relaxed);
        self.capacity_changed.notify_waiters();
        self.emit(result);
    }

    /// Drop an executor whose connection ended, leaving its in-flight jobs to
    /// the scheduler's reaper — the same recovery a crashed worker gets, and
    /// the only correct answer when a lost result may still have run.
    fn abandon(&self, executor: &Arc<Executor>) {
        self.deregister(&executor.id);
        let abandoned: Vec<String> = executor
            .in_flight
            .lock()
            .unwrap_or_else(recover)
            .drain()
            .map(|(job_id, _)| job_id)
            .collect();
        if !abandoned.is_empty() {
            log::warn!(
                "[taskito] executor {} ({}) left {} job(s) in flight; they will be reaped: {}",
                executor.id,
                executor.peer,
                abandoned.len(),
                abandoned.join(", ")
            );
        }
    }

    /// Remove an executor from the registry. Idempotent — the writer and the
    /// reader can both discover a broken connection.
    fn deregister(&self, executor_id: &str) {
        let removed = self
            .executors
            .lock()
            .unwrap_or_else(recover)
            .remove(executor_id);
        if removed.is_some() {
            self.capacity_changed.notify_waiters();
        }
    }

    /// Cancel router: forwards requests to whichever executor holds the job.
    ///
    /// Runs on its own thread so the synchronous, infallible `notify_cancel`
    /// never blocks the caller on a wedged peer.
    fn spawn_cancel_router(self: Arc<Self>, cancel_rx: Receiver<String>) -> JoinHandle<()> {
        thread::Builder::new()
            .name("taskito-executor-cancel-router".into())
            .spawn(move || {
                for job_id in cancel_rx.iter() {
                    // No executor holds it: already finished, never dispatched,
                    // or gone. The storage cancel flag covers those.
                    let Some(executor) = self.executor_running(&job_id) else {
                        continue;
                    };
                    let sent = executor
                        .writer
                        .lock()
                        .unwrap_or_else(recover)
                        .write_cancel(&job_id);
                    if let Err(e) = sent {
                        log::warn!(
                            "[taskito] failed to forward cancel for {job_id} to executor {}: {e}",
                            executor.id
                        );
                    }
                }
            })
            .expect("failed to spawn executor cancel-router thread")
    }

    /// The executor currently running `job_id`, if any.
    fn executor_running(&self, job_id: &str) -> Option<Arc<Executor>> {
        let executors = self.executors.lock().unwrap_or_else(recover);
        executors
            .values()
            .find(|executor| {
                executor
                    .in_flight
                    .lock()
                    .unwrap_or_else(recover)
                    .contains_key(job_id)
            })
            .cloned()
    }

    /// Ask every attached executor to finish, wait out the drain budget, then
    /// close the connections.
    ///
    /// Closing is what bounds shutdown: a reader thread is parked on a blocking
    /// read, and an executor that stops responding would otherwise keep it —
    /// and the join below it — parked forever.
    async fn drain_and_close(&self) {
        let executors: Vec<Arc<Executor>> = self
            .executors
            .lock()
            .unwrap_or_else(recover)
            .drain()
            .map(|(_, executor)| executor)
            .collect();

        for executor in &executors {
            // Best-effort: the executor may already be gone.
            let _ = executor
                .writer
                .lock()
                .unwrap_or_else(recover)
                .write_shutdown();
        }

        // Awaited, not slept: `run` shares a runtime with the scheduler task,
        // and a blocking sleep here starves it for the whole drain budget.
        let deadline = Instant::now() + self.config.shutdown_drain;
        while Instant::now() < deadline && executors.iter().any(Executor::is_busy) {
            tokio::time::sleep(DRAIN_POLL).await;
        }

        for executor in &executors {
            let still_running = executor.in_flight.lock().unwrap_or_else(recover).len();
            if still_running > 0 {
                log::warn!(
                    "[taskito] executor {} did not drain {still_running} job(s) within the \
                     shutdown budget; closing — they will be reaped",
                    executor.id
                );
            }
            executor.connection.close();
        }
    }

    fn set_cancel_sender(&self, tx: Option<Sender<String>>) {
        *self.cancel_tx.lock().unwrap_or_else(recover) = tx;
    }

    fn notify_cancel(&self, job_id: &str) {
        let sender = self
            .cancel_tx
            .lock()
            .unwrap_or_else(recover)
            .as_ref()
            .cloned();
        let Some(tx) = sender else {
            return;
        };
        match tx.try_send(job_id.to_string()) {
            Ok(()) | Err(TrySendError::Disconnected(_)) => {}
            Err(TrySendError::Full(_)) => {
                log::warn!("[taskito] executor cancel channel full, dropping cancel for {job_id}");
            }
        }
    }
}
