//! Dispatch to executors attached over a [`Transport`].
//!
//! An executor dials in, announces the tasks it can run, and receives jobs for
//! those tasks only. The scheduler is untouched: this is a [`WorkerDispatcher`]
//! like any other, so the same claim, retry, and reaper machinery applies.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use crossbeam_channel::{Receiver, Sender, TrySendError};
use tokio::sync::Notify;

use super::auth::Secret;
use super::protocol::{
    ExecutorMessage, FrameReader, FrameWriter, ProtocolError, SchedulerMessage, CAP_SIDE_CHANNEL,
    PROTOCOL_VERSION,
};
use super::side_channel::SideChannel;
use super::transport::{Connection, ReadHalf, Transport, WriteHalf};
use super::WorkerDispatcher;
use crate::job::Job;
use crate::scheduler::JobResult;

/// Tuning for a [`RemoteDispatcher`].
#[derive(Clone)]
pub struct RemoteConfig {
    /// Identity announced in `hello_ack`.
    pub scheduler_id: String,
    /// Shared secret an executor must present in its `hello`.
    ///
    /// `None` accepts any peer that reaches the transport, which is only safe
    /// when the transport itself is the boundary — a pipe to a child process, a
    /// loopback socket, or a Unix socket with restrictive permissions. A
    /// listener reachable off-host must set this.
    pub auth_token: Option<Secret>,
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
    /// Storage this scheduler applies executor-reported progress and task logs
    /// through, and resolves middleware toggles from.
    ///
    /// `None` leaves attached executors as they were before the side-channel
    /// existed: they are told so in the handshake, never send the frames, and
    /// their tasks' progress and logs go nowhere. Every real deployment sets
    /// this — it is `None` only where there is no storage to point at, which in
    /// practice means tests.
    pub side_channel: Option<Arc<dyn SideChannel>>,
    /// How many side-channel operations may be queued for application before
    /// the oldest logs are dropped.
    pub side_channel_capacity: usize,
}

impl Default for RemoteConfig {
    fn default() -> Self {
        Self {
            scheduler_id: format!("scheduler-{}", uuid::Uuid::now_v7()),
            auth_token: None,
            handshake_timeout: Duration::from_secs(10),
            placement_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            shutdown_drain: Duration::from_secs(30),
            cancel_capacity: 1024,
            side_channel: None,
            side_channel_capacity: 4096,
        }
    }
}

impl std::fmt::Debug for RemoteConfig {
    /// Hand-written because a `dyn SideChannel` is not `Debug`, and requiring it
    /// would push the bound onto every implementation for the sake of a log line.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteConfig")
            .field("scheduler_id", &self.scheduler_id)
            .field("auth_token", &self.auth_token)
            .field("handshake_timeout", &self.handshake_timeout)
            .field("placement_timeout", &self.placement_timeout)
            .field("write_timeout", &self.write_timeout)
            .field("shutdown_drain", &self.shutdown_drain)
            .field("cancel_capacity", &self.cancel_capacity)
            .field("side_channel", &self.side_channel.is_some())
            .field("side_channel_capacity", &self.side_channel_capacity)
            .finish()
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

    /// The `hello` carried no shared secret, or the wrong one. The message
    /// deliberately says nothing about which: a peer probing the port learns
    /// only that it was refused.
    #[error("executor {0} presented no valid attach credential")]
    Unauthorized(String),

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
        // Started here rather than in `run`: a reader thread exists from the
        // first attach, and an executor may report progress before the
        // scheduler has dispatched anything through this handle.
        let (side_channel, drain) = match config.side_channel.clone() {
            Some(sink) => {
                let (pump, handle) = SideChannelPump::start(sink, config.side_channel_capacity);
                (Some(pump), Some(handle))
            }
            None => (None, None),
        };

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
                side_channel,
                side_channel_drain: Mutex::new(drain),
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

/// What the scheduler remembers about a job it handed to an executor.
///
/// More than the task name because a side-channel frame carries only a job id:
/// the namespace a log row belongs in has to come from somewhere, and the
/// dispatch already knew it.
#[derive(Debug, Clone)]
struct InFlight {
    task_name: String,
    namespace: Option<String>,
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
    /// Job id → what was dispatched. Taking an entry is the exactly-once token
    /// for emitting that job's single `JobResult`; holding one is also this
    /// executor's authority to report progress or logs against that job.
    in_flight: Mutex<HashMap<String, InFlight>>,
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

    /// What this executor is running `job_id` as, or `None` when it is not.
    ///
    /// The authority check for every side-channel frame: an executor may write
    /// progress and logs only against jobs the scheduler actually gave it.
    fn running(&self, job_id: &str) -> Option<InFlight> {
        self.in_flight
            .lock()
            .unwrap_or_else(recover)
            .get(job_id)
            .cloned()
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

/// One executor-reported log line, on its way to storage.
struct LogLine {
    job_id: String,
    task_name: String,
    level: String,
    message: String,
    extra: Option<String>,
    namespace: Option<String>,
}

/// Applies executor-reported progress and logs to storage, off the reader
/// thread.
///
/// The reader thread also carries job results, and results are the thing the
/// scheduler cannot afford to delay. Applying a row inline would put every
/// result on that connection behind a database write, so the writes are queued
/// here and drained by one thread instead.
///
/// The two kinds queue separately because they are different data. Progress is
/// idempotent-latest — only the newest value per job matters — so it collapses
/// into a map and a backlog costs one row per job rather than growing. A log
/// line *is* data and cannot be collapsed, so its queue is bounded and drops
/// oldest with a counter, the trade every log shipper makes.
struct SideChannelPump {
    sink: Arc<dyn SideChannel>,
    /// Job id → newest progress not yet applied.
    pending_progress: Mutex<HashMap<String, i32>>,
    /// Capacity 1: a pending wake-up already covers every value in the map, so
    /// a second one would only make the drain spin.
    progress_wake: Sender<()>,
    logs: Sender<LogLine>,
    /// A clone of the drain's receiver, used only to shed the head of a full
    /// queue. Discarding from the sending side is safe because the item is
    /// being dropped either way.
    log_shed: Receiver<LogLine>,
    /// Log lines discarded because the queue was full, reported at teardown so
    /// a silent gap never reads as a quiet task.
    dropped_logs: AtomicU64,
}

/// The drain thread's lifetime controls.
///
/// The queues cannot be what ends it: the pump holds their senders and lives as
/// long as the dispatcher, so a dedicated signal is what makes a bounded
/// shutdown possible.
struct SideChannelDrain {
    /// Never sent on — dropping it is the signal.
    close: Sender<()>,
    handle: JoinHandle<()>,
}

impl SideChannelPump {
    /// Start the pump and its drain thread.
    fn start(sink: Arc<dyn SideChannel>, capacity: usize) -> (Arc<Self>, SideChannelDrain) {
        let (progress_wake, progress_rx) = crossbeam_channel::bounded(1);
        let (logs, log_rx) = crossbeam_channel::bounded(capacity.max(1));
        let (close, close_rx) = crossbeam_channel::bounded::<()>(0);
        let pump = Arc::new(Self {
            sink,
            pending_progress: Mutex::new(HashMap::new()),
            progress_wake,
            logs,
            log_shed: log_rx.clone(),
            dropped_logs: AtomicU64::new(0),
        });
        let handle = Arc::clone(&pump).spawn_drain(progress_rx, log_rx, close_rx);
        (pump, SideChannelDrain { close, handle })
    }

    /// Record the newest progress for a job and ask for a flush.
    ///
    /// The value lands in the map before the wake-up, so a full wake channel
    /// loses nothing: the flush that is already pending will pick this value up.
    fn progress(&self, job_id: &str, progress: i32) {
        self.pending_progress
            .lock()
            .unwrap_or_else(recover)
            .insert(job_id.to_string(), progress);
        let _ = self.progress_wake.try_send(());
    }

    /// Queue one log line, shedding the oldest when the queue is full.
    fn log(&self, line: LogLine) {
        let mut line = line;
        loop {
            match self.logs.try_send(line) {
                Ok(()) => return,
                Err(TrySendError::Disconnected(_)) => return,
                Err(TrySendError::Full(rejected)) => {
                    line = rejected;
                    // Drop-oldest, not drop-newest: when a task floods, the
                    // lines nearest the present are the ones worth keeping. A
                    // failed shed means the drain just emptied a slot, so the
                    // retry fits.
                    if self.log_shed.try_recv().is_ok() {
                        self.dropped_logs.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    /// Apply every progress value recorded since the last flush.
    fn flush_progress(&self) {
        let batch = std::mem::take(&mut *self.pending_progress.lock().unwrap_or_else(recover));
        for (job_id, progress) in batch {
            self.sink.update_progress(&job_id, progress);
        }
    }

    fn write(&self, line: &LogLine) {
        self.sink.write_task_log(
            &line.job_id,
            &line.task_name,
            &line.level,
            &line.message,
            line.extra.as_deref(),
            line.namespace.as_deref(),
        );
    }

    /// Drain thread: apply work from both queues until asked to stop.
    fn spawn_drain(
        self: Arc<Self>,
        progress_rx: Receiver<()>,
        log_rx: Receiver<LogLine>,
        close_rx: Receiver<()>,
    ) -> JoinHandle<()> {
        thread::Builder::new()
            .name("taskito-side-channel".into())
            .spawn(move || {
                loop {
                    crossbeam_channel::select! {
                        recv(progress_rx) -> woken => match woken {
                            Ok(()) => self.flush_progress(),
                            Err(_) => break,
                        },
                        recv(log_rx) -> line => match line {
                            Ok(line) => self.write(&line),
                            Err(_) => break,
                        },
                        recv(close_rx) -> _ => break,
                    }
                }

                // Stopping is not the same as being empty, and what is still
                // queued belongs in storage.
                for line in log_rx.try_iter() {
                    self.write(&line);
                }
                self.flush_progress();

                let dropped = self.dropped_logs.load(Ordering::Relaxed);
                if dropped > 0 {
                    log::warn!(
                        "[taskito] dropped {dropped} executor task log line(s) that arrived \
                         faster than they could be written"
                    );
                }
            })
            .expect("failed to spawn the executor side-channel thread")
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
    /// Applies executor-reported progress and logs. `None` when the deployment
    /// configured no storage to apply them to, which is also what the handshake
    /// tells executors so they never send the frames.
    side_channel: Option<Arc<SideChannelPump>>,
    /// Stopped after the readers, so a frame read on the way down still lands.
    side_channel_drain: Mutex<Option<SideChannelDrain>>,
}

impl Drop for Shared {
    /// `run` stops the drain thread on the way out, but a dispatcher that is
    /// built and dropped without ever running would otherwise leave it alive
    /// for the life of the process. `stop_side_channel` takes the handle out of
    /// the mutex, so the second call is a no-op.
    fn drop(&mut self) {
        self.stop_side_channel();
    }
}

impl Shared {
    /// Handshake and register.
    ///
    /// The order is the security property: `hello`, then the credential, then
    /// anything back. An unauthenticated peer gets no ack and never enters the
    /// registry, so it can never be handed a job; returning drops the transport
    /// and closes the socket. Past that gate the ack is sent even on a version
    /// mismatch, so both ends log both versions.
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
        let ExecutorMessage::Hello {
            executor_id,
            sdk,
            version,
            tasks,
            slots,
            protocol_version,
            token,
        } = hello
        else {
            log::warn!("[taskito] attach from {peer} sent a frame before hello; dropping");
            return Err(ProtocolError::UnexpectedFrame { expected: "hello" }.into());
        };

        if let Some(expected) = &self.config.auth_token {
            if !token.is_some_and(|presented| expected.matches(&presented)) {
                // Vague on purpose: a peer probing the port learns only that it
                // was refused, not whether its token was missing or wrong.
                log::warn!(
                    "[taskito] rejecting executor {executor_id} ({sdk} {version}, {peer}): \
                     invalid attach credential"
                );
                return Err(AttachError::Unauthorized(executor_id));
            }
        }

        // Bound the handshake only. An attached executor waiting between jobs
        // must block indefinitely, or it would be dropped every time it idled
        // past this budget.
        connection.set_read_timeout(None)?;

        writer.write_header(&SchedulerMessage::HelloAck {
            scheduler_id: self.config.scheduler_id.clone(),
            protocol_version: PROTOCOL_VERSION,
            capabilities: self.capabilities(),
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

    /// Optional behaviours to announce in `hello_ack`.
    ///
    /// Announcing rather than versioning is the point: an executor that sees no
    /// capability sends no frame the scheduler could not handle, so the two
    /// sides upgrade independently.
    fn capabilities(&self) -> Vec<String> {
        match self.side_channel {
            Some(_) => vec![CAP_SIDE_CHANNEL.to_string()],
            None => Vec::new(),
        }
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
        self.stop_side_channel();
    }

    /// Close the side-channel queues and wait for what is in them to be written.
    ///
    /// After the readers have joined, so a progress report that arrived on the
    /// way down is queued before the senders are dropped, and the drain still
    /// flushes it.
    fn stop_side_channel(&self) {
        let drain = self
            .side_channel_drain
            .lock()
            .unwrap_or_else(recover)
            .take();
        let Some(SideChannelDrain { close, handle }) = drain else {
            return;
        };
        drop(close);
        let _ = handle.join();
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
                    let disabled = self.resolve_toggles(&job.task_name).await;
                    self.dispatch_to(&executor, job, disabled);
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

    /// Resolve the middleware a dispatch should carry, off the runtime thread.
    ///
    /// Resolved by the scheduler rather than read by the executor: it has no
    /// storage to read from. On a cache miss that is a settings read, and
    /// `place` runs on the runtime the scheduler task shares — the same
    /// constraint `drain_and_close` documents — so a slow settings backend must
    /// not be able to stall it. The pump's cache means most dispatches answer
    /// from memory and never reach the blocking pool.
    async fn resolve_toggles(&self, task_name: &str) -> Vec<String> {
        let Some(pump) = self.side_channel.as_ref() else {
            return Vec::new();
        };
        let sink = Arc::clone(&pump.sink);
        let task_name = task_name.to_string();
        tokio::task::spawn_blocking(move || sink.disabled_middleware(&task_name))
            .await
            .unwrap_or_else(|error| {
                log::warn!(
                    "[taskito] resolving the middleware disable list panicked ({error}); \
                     dispatching with none disabled"
                );
                Vec::new()
            })
    }

    /// Send a reserved job to its executor.
    ///
    /// Registers the job before writing so a fast executor cannot return a
    /// result the reader can't pair with an in-flight entry. A failed write
    /// means the connection is gone: the slot is released, the executor is
    /// dropped, and the job is left to the scheduler's reaper — the same
    /// recovery path a mid-job executor crash takes.
    fn dispatch_to(&self, executor: &Arc<Executor>, job: Job, disabled: Vec<String>) {
        executor.in_flight.lock().unwrap_or_else(recover).insert(
            job.id.clone(),
            InFlight {
                task_name: job.task_name.clone(),
                namespace: job.namespace.clone(),
            },
        );

        let write = executor
            .writer
            .lock()
            .unwrap_or_else(recover)
            .write_job_with(&job, disabled);

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

        // Handled before `into_job_result` on purpose: these are not results.
        // Taking the in-flight entry — which is what the result path does — is
        // the exactly-once token for the job's single outcome, and a progress
        // report must never spend it.
        let message = match message {
            ExecutorMessage::Progress { job_id, progress } => {
                self.apply_progress(executor, &job_id, progress);
                return;
            }
            ExecutorMessage::TaskLog {
                job_id,
                level,
                message: text,
                task_name: _,
                extra_len,
            } => {
                // `extra_len` is what says whether there was a blob at all: an
                // `extra` of `""` and no `extra` both arrive as an empty
                // payload, and only one of them should store NULL.
                let extra = extra_len.map(|_| payload);
                self.apply_task_log(executor, &job_id, &level, &text, extra);
                return;
            }
            other => other,
        };

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

    /// Record progress an executor reported for a job it is running.
    fn apply_progress(&self, executor: &Arc<Executor>, job_id: &str, progress: i32) {
        let Some((pump, _)) = self.side_channel_for(executor, job_id, "progress") else {
            return;
        };
        pump.progress(job_id, progress);
    }

    /// Record a log line — or a published partial — an executor reported.
    ///
    /// The row is attributed to the task the *scheduler* dispatched, not the
    /// name on the frame: it knows which is true, and a log row that says
    /// otherwise would misattribute work in the dashboard.
    fn apply_task_log(
        &self,
        executor: &Arc<Executor>,
        job_id: &str,
        level: &str,
        message: &str,
        extra: Option<Vec<u8>>,
    ) {
        let Some((pump, dispatched)) = self.side_channel_for(executor, job_id, "task log") else {
            return;
        };
        // The blob is whatever the SDK encoded; storage takes it as a string,
        // so a non-UTF-8 payload is a broken sender rather than data to store.
        let extra = extra.and_then(|blob| match String::from_utf8(blob) {
            Ok(extra) => Some(extra),
            Err(_) => {
                log::warn!(
                    "[taskito] executor {} sent a task log for job {job_id} whose extra blob is \
                     not UTF-8; dropping the blob",
                    executor.id
                );
                None
            }
        });
        pump.log(LogLine {
            job_id: job_id.to_string(),
            task_name: dispatched.task_name,
            level: level.to_string(),
            message: message.to_string(),
            extra,
            namespace: dispatched.namespace,
        });
    }

    /// The pump to apply a side-channel frame through, plus what the scheduler
    /// dispatched — once the frame has been shown to be one this executor is
    /// entitled to send.
    ///
    /// An executor may write only against jobs the scheduler gave it. Without
    /// that check any attached peer could rewrite another executor's progress
    /// or forge log lines on its jobs — an authenticated executor is trusted to
    /// run its own work, not to speak for the whole fleet.
    fn side_channel_for(
        &self,
        executor: &Arc<Executor>,
        job_id: &str,
        what: &str,
    ) -> Option<(&Arc<SideChannelPump>, InFlight)> {
        let Some(pump) = self.side_channel.as_ref() else {
            // Never advertised, so a correct executor never sent this.
            log::debug!(
                "[taskito] executor {} sent a {what} frame but this scheduler advertised no \
                 side-channel; dropping",
                executor.id
            );
            return None;
        };
        let Some(dispatched) = executor.running(job_id) else {
            log::warn!(
                "[taskito] executor {} sent a {what} for job {job_id}, which it is not running; \
                 dropping",
                executor.id
            );
            return None;
        };
        Some((pump, dispatched))
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
