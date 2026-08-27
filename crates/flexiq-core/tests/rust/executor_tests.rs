//! Tests for [`ExecutorClient`], driven against a real [`RemoteDispatcher`]
//! over [`MemoryTransport`] so no socket is bound.
//!
//! Both halves of the attach are the shipping implementation — a fake on either
//! side could only prove it agrees with itself.

use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use crossbeam_channel::{Receiver, Sender};
use serde::{Deserialize, Serialize};

use flexiq_core::job::{Job, JobStatus};
use flexiq_core::scheduler::JobResult;
use flexiq_core::step::{classify_step_failure, StepFailure, StepLimits};
use flexiq_core::storage::records::{JobStep, StepKind};
use flexiq_core::worker::auth::Secret;
use flexiq_core::worker::executor::{
    ExecutorClient, ExecutorConfig, ExecutorError, ExecutorHandle,
};
use flexiq_core::worker::protocol::{
    encode_step_snapshot, ExecutorMessage, Frame, FrameReader, FrameWriter, ProtocolError,
    SchedulerMessage, CAP_SIDE_CHANNEL, CAP_STEPS, PROTOCOL_VERSION,
};
use flexiq_core::worker::remote::{RemoteConfig, RemoteDispatcher};
use flexiq_core::worker::transport::{Connection, MemoryTransport, ReadHalf, Transport, WriteHalf};
use flexiq_core::worker::WorkerDispatcher;

const SETTLE: Duration = Duration::from_secs(5);

/// The drain budget every attached executor in these tests runs with.
const SHUTDOWN_DRAIN: Duration = Duration::from_secs(2);

/// What a [`TestPool`] should do with a job.
enum Behaviour {
    Succeed(Option<Vec<u8>>),
    Fail {
        should_retry: bool,
    },
    /// Park until released, so a test can hold a job in flight.
    Block(Receiver<()>),
    /// Park until the test drops its sender. Unlike [`Behaviour::Block`] no
    /// timeout releases it, so a shutdown that waits on the job never returns —
    /// which is what makes the drain budget observable.
    Wedge(Receiver<()>),
}

/// A minimal [`WorkerDispatcher`]: one job at a time, scripted per task name.
///
/// The SDK pools are the real consumers, but each needs a language runtime.
/// This stands in for them and, unlike [`NativeDispatcher`](super::NativeDispatcher),
/// records `notify_cancel` so the cancel path is observable.
struct TestPool {
    behaviours: Mutex<HashMap<String, Behaviour>>,
    /// Every job the pool was handed, as rebuilt from the wire.
    seen: Mutex<Vec<Job>>,
    cancels: Mutex<Vec<String>>,
    started: Sender<String>,
    shutdown: AtomicBool,
}

impl TestPool {
    fn new(started: Sender<String>) -> Arc<Self> {
        Arc::new(Self {
            behaviours: Mutex::new(HashMap::new()),
            seen: Mutex::new(Vec::new()),
            cancels: Mutex::new(Vec::new()),
            started,
            shutdown: AtomicBool::new(false),
        })
    }

    /// Script `task_name`. Consumed on first use, so a repeat of the same task
    /// falls through to the default success.
    fn on(&self, task_name: &str, behaviour: Behaviour) {
        self.behaviours
            .lock()
            .expect("behaviours")
            .insert(task_name.to_string(), behaviour);
    }

    fn cancelled(&self) -> Vec<String> {
        self.cancels.lock().expect("cancels").clone()
    }

    /// The job as the executor rebuilt it from the frame.
    fn received(&self, job_id: &str) -> Option<Job> {
        self.seen
            .lock()
            .expect("seen")
            .iter()
            .find(|job| job.id == job_id)
            .cloned()
    }

    /// Run one job, blocking the pool loop for as long as the task would.
    fn execute(&self, job: &Job) -> JobResult {
        self.seen.lock().expect("seen").push(job.clone());
        let _ = self.started.send(job.id.clone());

        // Taken out of the map rather than read under it: a blocking task holds
        // its release channel for the whole wait.
        let behaviour = self
            .behaviours
            .lock()
            .expect("behaviours")
            .remove(&job.task_name);

        match behaviour {
            Some(Behaviour::Block(release)) => {
                let _ = release.recv_timeout(SETTLE);
                success(job, None)
            }
            Some(Behaviour::Wedge(release)) => {
                let _ = release.recv();
                success(job, None)
            }
            Some(Behaviour::Fail { should_retry }) => JobResult::Failure {
                job_id: job.id.clone(),
                error: "deliberate failure".to_string(),
                retry_count: job.retry_count,
                max_retries: job.max_retries,
                task_name: job.task_name.clone(),
                wall_time_ns: 1,
                should_retry,
                timed_out: false,
            },
            Some(Behaviour::Succeed(result)) => success(job, result),
            // Unscripted tasks succeed: most tests care about the transport.
            None => success(job, None),
        }
    }
}

fn success(job: &Job, result: Option<Vec<u8>>) -> JobResult {
    JobResult::Success {
        job_id: job.id.clone(),
        result,
        task_name: job.task_name.clone(),
        wall_time_ns: 1,
    }
}

#[async_trait]
impl WorkerDispatcher for TestPool {
    async fn run(
        &self,
        mut job_rx: tokio::sync::mpsc::Receiver<Job>,
        result_tx: Sender<JobResult>,
    ) {
        while let Some(job) = job_rx.recv().await {
            // Executing on the runtime thread would block the reactor, which is
            // what a real pool avoids by handing work to processes or threads.
            let outcome = tokio::task::block_in_place(|| self.execute(&job));
            if result_tx.send(outcome).is_err() {
                break;
            }
        }
    }

    fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    fn notify_cancel(&self, job_id: &str) {
        self.cancels
            .lock()
            .expect("cancels")
            .push(job_id.to_string());
    }
}

/// A scheduler and an executor wired to each other.
struct Attached {
    dispatcher: RemoteDispatcher,
    handle: ExecutorHandle,
    pool: Arc<TestPool>,
    started: Receiver<String>,
}

/// Attach an executor advertising `tasks` to a fresh scheduler.
fn attach(tasks: &[&str], slots: u32) -> Attached {
    let (started_tx, started) = crossbeam_channel::unbounded();
    let pool = TestPool::new(started_tx);
    let dispatcher = scheduler(None);
    let handle = dial(&dispatcher, tasks, slots, None)
        .expect("attach")
        .spawn(pool.clone());
    Attached {
        dispatcher,
        handle,
        pool,
        started,
    }
}

fn scheduler(auth_token: Option<&str>) -> RemoteDispatcher {
    RemoteDispatcher::new(RemoteConfig {
        scheduler_id: "scheduler-test".to_string(),
        auth_token: auth_token.map(Secret::new),
        placement_timeout: Duration::from_secs(5),
        shutdown_drain: Duration::from_millis(200),
        ..RemoteConfig::default()
    })
}

/// Complete a handshake against `dispatcher`.
///
/// `attach` blocks reading `hello` while `connect` blocks reading the ack, so
/// the scheduler side has to run concurrently — the same shape as a listener
/// thread accepting a connection.
fn dial(
    dispatcher: &RemoteDispatcher,
    tasks: &[&str],
    slots: u32,
    token: Option<&str>,
) -> Result<ExecutorClient, ExecutorError> {
    let (scheduler_end, executor_end) = MemoryTransport::pair();
    let accepting = {
        let dispatcher = dispatcher.clone();
        thread::spawn(move || dispatcher.attach(Box::new(scheduler_end)))
    };

    let connected = ExecutorClient::connect(
        Box::new(executor_end),
        ExecutorConfig {
            executor_id: "exec-1".to_string(),
            tasks: tasks.iter().map(|task| (*task).to_string()).collect(),
            slots,
            token: token.map(Secret::new),
            // Fast enough that a capacity assertion does not wait on a
            // production-cadence heartbeat.
            heartbeat_interval: Duration::from_millis(50),
            shutdown_drain: SHUTDOWN_DRAIN,
            ..ExecutorConfig::new("test", "0.0.0")
        },
    );
    let _ = accepting.join();
    connected
}

/// A frame from a peer released after this build: a type tag with no variant
/// behind it, plus the payload length that makes it skippable.
#[derive(Debug, Serialize, Deserialize)]
struct FutureFrame {
    #[serde(rename = "type")]
    frame_type: String,
    payload_len: usize,
}

impl FutureFrame {
    fn new(frame_type: &str, payload: &[u8]) -> Self {
        Self {
            frame_type: frame_type.to_string(),
            payload_len: payload.len(),
        }
    }
}

impl Frame for FutureFrame {
    fn payload_len(&self) -> usize {
        self.payload_len
    }

    /// Never: the whole point of this frame is that the peer cannot name it.
    fn is_known_type(_tag: &str) -> bool {
        false
    }
}

/// A latch a test can shut to hold the executor's writer off the wire.
///
/// The result loop frames everything under one lock, so parking a write is how
/// the outbound queue behind it is filled deterministically — racing the drain
/// would only fill it by luck.
#[derive(Default)]
struct Gate {
    open: Mutex<bool>,
    changed: Condvar,
}

impl Gate {
    fn opened() -> Arc<Self> {
        Arc::new(Self {
            open: Mutex::new(true),
            changed: Condvar::new(),
        })
    }

    fn close(&self) {
        *self.open.lock().expect("gate") = false;
    }

    fn release(&self) {
        *self.open.lock().expect("gate") = true;
        self.changed.notify_all();
    }

    fn wait(&self) {
        let mut open = self.open.lock().expect("gate");
        while !*open {
            open = self.changed.wait(open).expect("gate");
        }
    }
}

/// A [`MemoryTransport`] whose write half parks while its [`Gate`] is shut.
struct StallingTransport {
    inner: Box<MemoryTransport>,
    gate: Arc<Gate>,
}

impl Transport for StallingTransport {
    fn split(self: Box<Self>) -> io::Result<(ReadHalf, WriteHalf, Connection)> {
        let (read, write, connection) = self.inner.split()?;
        Ok((
            read,
            Box::new(StallingWriter {
                inner: write,
                gate: self.gate,
            }),
            connection,
        ))
    }

    fn peer(&self) -> String {
        self.inner.peer()
    }
}

struct StallingWriter {
    inner: WriteHalf,
    gate: Arc<Gate>,
}

impl Write for StallingWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.gate.wait();
        self.inner.write(data)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// How a test wants the executor end of the attach built.
struct ExecutorTuning {
    /// Queued side-channel operations allowed before the oldest logs are shed.
    capacity: usize,
    /// Holds the executor's writer, so the queue behind it can be filled.
    gate: Option<Arc<Gate>>,
    /// What the executor announces in its own `hello`.
    capabilities: Vec<String>,
    /// How long a step commit waits for its ack.
    step_ack_timeout: Duration,
}

impl Default for ExecutorTuning {
    fn default() -> Self {
        let defaults = ExecutorConfig::new("test", "0.0.0");
        Self {
            capacity: defaults.side_channel_capacity,
            gate: None,
            capabilities: Vec::new(),
            step_ack_timeout: defaults.step_ack_timeout,
        }
    }
}

/// The scheduler end of the wire, hand-driven.
///
/// [`RemoteDispatcher`] is the right peer for most of these tests, but it is
/// also a correct one: it will not dispatch to an executor advertising no free
/// slots. Forcing that race needs a peer that writes whatever it is told to.
struct FakeScheduler {
    reader: FrameReader<ReadHalf>,
    writer: FrameWriter<WriteHalf>,
}

impl FakeScheduler {
    /// Handshake with an executor and return both ends live, advertising
    /// nothing optional.
    fn attach(tasks: &[&str], slots: u32) -> (Self, ExecutorHandle, Arc<TestPool>) {
        Self::attach_with(tasks, slots, Vec::new())
    }

    /// Handshake advertising `capabilities`, so a test can drive both sides of
    /// the negotiation.
    fn attach_with(
        tasks: &[&str],
        slots: u32,
        capabilities: Vec<String>,
    ) -> (Self, ExecutorHandle, Arc<TestPool>) {
        Self::attach_tuned(tasks, slots, capabilities, ExecutorTuning::default())
    }

    /// Handshake with both sides claiming [`CAP_STEPS`], and a short ack budget
    /// so a test that never answers a commit does not sit for the default 30s.
    fn attach_with_steps(tasks: &[&str], slots: u32) -> (Self, ExecutorHandle, Arc<TestPool>) {
        Self::attach_tuned(
            tasks,
            slots,
            vec![CAP_STEPS.to_string()],
            ExecutorTuning {
                capabilities: vec![CAP_STEPS.to_string()],
                step_ack_timeout: Duration::from_millis(300),
                ..ExecutorTuning::default()
            },
        )
    }

    /// Handshake with the outbound queue sized to `capacity` and the executor's
    /// writer behind a gate, so a test can hold frames off the wire and fill it.
    fn attach_stalled(
        tasks: &[&str],
        slots: u32,
        capacity: usize,
    ) -> (Self, ExecutorHandle, Arc<TestPool>, Arc<Gate>) {
        let gate = Gate::opened();
        let (scheduler, handle, pool) = Self::attach_tuned(
            tasks,
            slots,
            vec![CAP_SIDE_CHANNEL.to_string()],
            ExecutorTuning {
                capacity,
                gate: Some(gate.clone()),
                ..ExecutorTuning::default()
            },
        );
        (scheduler, handle, pool, gate)
    }

    fn attach_tuned(
        tasks: &[&str],
        slots: u32,
        capabilities: Vec<String>,
        tuning: ExecutorTuning,
    ) -> (Self, ExecutorHandle, Arc<TestPool>) {
        let (scheduler_end, executor_end) = MemoryTransport::pair();
        let executor_end: Box<dyn Transport> = match tuning.gate {
            Some(gate) => Box::new(StallingTransport {
                inner: Box::new(executor_end),
                gate,
            }),
            None => Box::new(executor_end),
        };

        // `connect` blocks on the ack, so the scheduler side runs concurrently.
        let accepting = thread::spawn(move || {
            let (read, write, _connection) = Box::new(scheduler_end)
                .split()
                .expect("split scheduler end");
            let mut scheduler = Self {
                reader: FrameReader::new(read),
                writer: FrameWriter::new(write),
            };
            match scheduler.reader.read::<ExecutorMessage>().expect("hello").0 {
                ExecutorMessage::Hello { .. } => {}
                other => panic!("expected hello, got {other:?}"),
            }
            scheduler
                .writer
                .write_header(&SchedulerMessage::HelloAck {
                    scheduler_id: "scheduler-fake".to_string(),
                    protocol_version: PROTOCOL_VERSION,
                    capabilities,
                })
                .expect("send ack");
            scheduler
        });

        let client = ExecutorClient::connect(
            executor_end,
            ExecutorConfig {
                executor_id: "exec-1".to_string(),
                tasks: tasks.iter().map(|task| (*task).to_string()).collect(),
                slots,
                heartbeat_interval: Duration::from_millis(50),
                shutdown_drain: Duration::from_secs(2),
                side_channel_capacity: tuning.capacity,
                capabilities: tuning.capabilities.clone(),
                step_ack_timeout: tuning.step_ack_timeout,
                ..ExecutorConfig::new("test", "0.0.0")
            },
        )
        .expect("attach");

        let (started_tx, _started) = crossbeam_channel::unbounded();
        let pool = TestPool::new(started_tx);
        let handle = client.spawn(pool.clone());
        (accepting.join().expect("handshake thread"), handle, pool)
    }

    fn send_job(&mut self, id: &str, task_name: &str, payload: &[u8]) {
        self.send_job_with(id, task_name, payload, Vec::new());
    }

    /// Write a frame type no executor built today can name.
    fn send_future_frame(&mut self, frame_type: &str, payload: &[u8]) {
        self.writer
            .write(&FutureFrame::new(frame_type, payload), payload)
            .expect("send a future frame");
    }

    fn send_job_with(
        &mut self,
        id: &str,
        task_name: &str,
        payload: &[u8],
        disabled_middleware: Vec<String>,
    ) {
        self.writer
            .write(
                &SchedulerMessage::Job {
                    id: id.to_string(),
                    task_name: task_name.to_string(),
                    payload_len: payload.len(),
                    retry_count: 1,
                    max_retries: 3,
                    queue: "default".to_string(),
                    timeout_ms: 30_000,
                    namespace: None,
                    disabled_middleware,
                    metadata: None,
                },
                payload,
            )
            .expect("send job");
    }

    /// Block until the executor reports the given free-slot count.
    fn expect_heartbeat(&mut self, free: u32) {
        let deadline = Instant::now() + SETTLE;
        loop {
            assert!(
                Instant::now() < deadline,
                "no heartbeat reporting {free} slots"
            );
            if let ExecutorMessage::Heartbeat { free_slots } = self.next_frame().0 {
                if free_slots == free {
                    return;
                }
            }
        }
    }

    /// The next frame that is not a heartbeat.
    fn expect_result(&mut self) -> ExecutorMessage {
        let deadline = Instant::now() + SETTLE;
        loop {
            assert!(Instant::now() < deadline, "no result frame arrived");
            let frame = self.next_frame().0;
            if !matches!(frame, ExecutorMessage::Heartbeat { .. }) {
                return frame;
            }
        }
    }

    fn next_frame(&mut self) -> (ExecutorMessage, Vec<u8>) {
        self.reader.read::<ExecutorMessage>().expect("read a frame")
    }
}

/// The executor end of the wire, hand-driven.
///
/// The mirror of [`FakeScheduler`], and needed for the same reason:
/// [`ExecutorClient`] only ever writes frames this build knows, so proving a
/// scheduler survives one it does not takes a peer that writes anything.
struct FakeExecutor {
    reader: FrameReader<ReadHalf>,
    writer: FrameWriter<WriteHalf>,
}

impl FakeExecutor {
    /// Handshake with a real [`RemoteDispatcher`], leaving both ends live.
    fn attach(dispatcher: &RemoteDispatcher, tasks: &[&str], slots: u32) -> Self {
        let (scheduler_end, executor_end) = MemoryTransport::pair();
        let accepting = {
            let dispatcher = dispatcher.clone();
            thread::spawn(move || dispatcher.attach(Box::new(scheduler_end)))
        };

        let (read, write, _connection) = Box::new(executor_end).split().expect("split");
        let mut executor = Self {
            reader: FrameReader::new(read),
            writer: FrameWriter::new(write),
        };
        executor
            .writer
            .write_header(
                &ExecutorMessage::hello(
                    "exec-fake",
                    "test",
                    "0.0.0",
                    tasks.iter().map(|task| (*task).to_string()).collect(),
                    slots,
                )
                .build(),
            )
            .expect("send hello");
        match executor.reader.read::<SchedulerMessage>().expect("ack").0 {
            SchedulerMessage::HelloAck { .. } => {}
            other => panic!("expected hello_ack, got {other:?}"),
        }
        accepting.join().expect("attach thread").expect("attach");
        executor
    }

    /// Write a frame type no scheduler built today can name.
    fn send_future_frame(&mut self, frame_type: &str, payload: &[u8]) {
        self.writer
            .write(&FutureFrame::new(frame_type, payload), payload)
            .expect("send a future frame");
    }

    /// Read the next dispatch, answering it with a success.
    fn run_next_job(&mut self) {
        let job_id = match self.reader.read::<SchedulerMessage>().expect("read").0 {
            SchedulerMessage::Job { id, .. } => id,
            other => panic!("expected a job, got {other:?}"),
        };
        self.writer
            .write_header(&ExecutorMessage::Success {
                job_id,
                result_len: None,
                task_name: "resize".to_string(),
                wall_time_ns: 1,
            })
            .expect("send success");
    }
}

fn make_job(id: &str, task_name: &str, payload: &[u8]) -> Job {
    Job {
        id: id.to_string(),
        queue: "default".to_string(),
        task_name: task_name.to_string(),
        payload: payload.to_vec(),
        status: JobStatus::Running,
        priority: 0,
        created_at: 0,
        scheduled_at: 0,
        started_at: None,
        completed_at: None,
        retry_count: 0,
        max_retries: 3,
        result: None,
        error: None,
        timeout_ms: 30_000,
        unique_key: None,
        progress: None,
        metadata: None,
        notes: None,
        cancel_requested: false,
        expires_at: None,
        result_ttl_ms: None,
        namespace: None,
        has_deps: false,
        debounce_key: None,
    }
}

/// Run `body` with the scheduler's dispatch loop live.
fn with_running<F>(dispatcher: &RemoteDispatcher, body: F)
where
    F: FnOnce(&tokio::sync::mpsc::Sender<Job>, &Receiver<JobResult>),
{
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");

    let (job_tx, job_rx) = tokio::sync::mpsc::channel(4);
    let (result_tx, result_rx) = crossbeam_channel::bounded(4);
    let running = {
        let dispatcher = dispatcher.clone();
        runtime.spawn(async move { dispatcher.run(job_rx, result_tx).await })
    };

    body(&job_tx, &result_rx);

    drop(job_tx);
    runtime.block_on(async { running.await.expect("run loop") });
}

fn expect_result(results: &Receiver<JobResult>) -> JobResult {
    results.recv_timeout(SETTLE).expect("a result")
}

/// `JobResult` deliberately has no `Debug` — it carries task payloads — so
/// assertion messages name the variant instead of dumping it.
fn kind(result: &JobResult) -> &'static str {
    match result {
        JobResult::Success { .. } => "success",
        JobResult::Failure { .. } => "failure",
        JobResult::Cancelled { .. } => "cancelled",
        JobResult::Slept { .. } => "slept",
        _ => "unknown",
    }
}

fn wait_until(mut condition: impl FnMut() -> bool, message: &str) {
    let deadline = Instant::now() + SETTLE;
    while !condition() {
        assert!(Instant::now() < deadline, "{message}");
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn the_handshake_registers_the_executor_with_the_scheduler() {
    let attached = attach(&["resize", "thumbnail"], 3);

    let executors = attached.dispatcher.executors();
    assert_eq!(executors.len(), 1);
    assert_eq!(executors[0].executor_id, "exec-1");
    assert_eq!(executors[0].tasks, ["resize", "thumbnail"]);
    assert_eq!(executors[0].sdk, "test");
    assert_eq!(executors[0].slots, 3);
    assert_eq!(attached.dispatcher.capacity().free_slots, 3);
    assert_eq!(attached.handle.executor_id(), "exec-1");

    attached.handle.shutdown();
}

#[test]
fn a_job_runs_on_the_executor_and_its_result_comes_back() {
    let attached = attach(&["resize"], 1);
    attached
        .pool
        .on("resize", Behaviour::Succeed(Some(b"out".to_vec())));

    with_running(&attached.dispatcher, |jobs, results| {
        jobs.blocking_send(make_job("job-1", "resize", b"in"))
            .expect("send job");

        match expect_result(results) {
            JobResult::Success {
                job_id,
                result,
                task_name,
                ..
            } => {
                assert_eq!(job_id, "job-1");
                assert_eq!(task_name, "resize");
                assert_eq!(result.as_deref(), Some(&b"out"[..]));
            }
            ref other => panic!("expected success, got {}", kind(other)),
        }
    });

    attached.handle.shutdown();
}

#[test]
fn the_payload_reaches_the_pool_verbatim() {
    // The CBOR envelope for f(1, "a") — the BINDING_CONTRACT test vector. A
    // wire that mangled it would break every cross-SDK attach.
    const ENVELOPE: &[u8] = &[0x02, 0x82, 0x82, 0x01, 0x61, 0x61, 0xa0];

    let attached = attach(&["resize"], 1);

    with_running(&attached.dispatcher, |jobs, results| {
        jobs.blocking_send(make_job("job-1", "resize", ENVELOPE))
            .expect("send job");
        assert!(matches!(expect_result(results), JobResult::Success { .. }));
    });

    let received = attached
        .pool
        .received("job-1")
        .expect("the pool saw the job");
    assert_eq!(
        received.payload, ENVELOPE,
        "the wire-envelope bytes must survive the hop unchanged"
    );

    attached.handle.shutdown();
}

#[test]
fn the_job_frame_carries_every_field_a_task_needs() {
    // Rebuilt on the far side from the frame alone — the executor never reads
    // storage, so a field missing here is invisible to the task that runs.
    let attached = attach(&["resize"], 1);

    with_running(&attached.dispatcher, |jobs, results| {
        let mut job = make_job("job-1", "resize", b"");
        job.retry_count = 2;
        job.max_retries = 7;
        job.timeout_ms = 1_234;
        job.queue = "images".to_string();
        job.namespace = Some("tenant-a".to_string());

        jobs.blocking_send(job).expect("send job");
        assert!(matches!(expect_result(results), JobResult::Success { .. }));
    });

    let received = attached
        .pool
        .received("job-1")
        .expect("the pool saw the job");
    assert_eq!(received.task_name, "resize");
    assert_eq!(received.queue, "images");
    assert_eq!(
        received.retry_count, 2,
        "retry_count drives backoff reporting"
    );
    assert_eq!(received.max_retries, 7);
    assert_eq!(received.timeout_ms, 1_234, "the pool enforces the timeout");
    assert_eq!(received.namespace.as_deref(), Some("tenant-a"));

    attached.handle.shutdown();
}

#[test]
fn a_task_failure_crosses_the_wire_with_its_retry_verdict() {
    let attached = attach(&["flaky"], 1);
    attached
        .pool
        .on("flaky", Behaviour::Fail { should_retry: true });

    with_running(&attached.dispatcher, |jobs, results| {
        jobs.blocking_send(make_job("job-1", "flaky", b""))
            .expect("send job");

        match expect_result(results) {
            JobResult::Failure {
                job_id,
                should_retry,
                timed_out,
                error,
                ..
            } => {
                assert_eq!(job_id, "job-1");
                assert!(should_retry, "the executor's verdict must survive the hop");
                assert!(!timed_out);
                assert_eq!(error, "deliberate failure");
            }
            ref other => panic!("expected a failure, got {}", kind(other)),
        }
    });

    attached.handle.shutdown();
}

#[test]
fn a_non_retryable_failure_stays_non_retryable() {
    // Only the executor can see the exception, so its verdict is the one that
    // counts; a wire defaulting this to `true` would retry poison jobs forever.
    let attached = attach(&["fatal"], 1);
    attached.pool.on(
        "fatal",
        Behaviour::Fail {
            should_retry: false,
        },
    );

    with_running(&attached.dispatcher, |jobs, results| {
        jobs.blocking_send(make_job("job-1", "fatal", b""))
            .expect("send job");
        match expect_result(results) {
            JobResult::Failure { should_retry, .. } => assert!(!should_retry),
            ref other => panic!("expected a failure, got {}", kind(other)),
        }
    });

    attached.handle.shutdown();
}

#[test]
fn an_empty_result_stays_distinct_from_no_result() {
    let attached = attach(&["empty"], 1);
    attached.pool.on("empty", Behaviour::Succeed(Some(vec![])));

    with_running(&attached.dispatcher, |jobs, results| {
        jobs.blocking_send(make_job("job-1", "empty", b""))
            .expect("send job");
        match expect_result(results) {
            JobResult::Success { result, .. } => {
                assert_eq!(result, Some(vec![]), "Some(empty) must not become None")
            }
            ref other => panic!("expected success, got {}", kind(other)),
        }
    });

    attached.handle.shutdown();
}

#[test]
fn a_task_returning_nothing_reports_no_result() {
    let attached = attach(&["nothing"], 1);
    attached.pool.on("nothing", Behaviour::Succeed(None));

    with_running(&attached.dispatcher, |jobs, results| {
        jobs.blocking_send(make_job("job-1", "nothing", b""))
            .expect("send job");
        match expect_result(results) {
            JobResult::Success { result, .. } => {
                assert_eq!(result, None, "None must not become Some(empty)")
            }
            ref other => panic!("expected success, got {}", kind(other)),
        }
    });

    attached.handle.shutdown();
}

#[test]
fn a_cancel_from_the_scheduler_reaches_the_pool() {
    let (release, released) = crossbeam_channel::bounded(1);
    let attached = attach(&["slow"], 1);
    attached.pool.on("slow", Behaviour::Block(released));

    with_running(&attached.dispatcher, |jobs, results| {
        jobs.blocking_send(make_job("job-1", "slow", b""))
            .expect("send job");
        assert_eq!(
            attached.started.recv_timeout(SETTLE).expect("job started"),
            "job-1"
        );

        attached.dispatcher.notify_cancel("job-1");
        wait_until(
            || attached.pool.cancelled() == ["job-1"],
            "the cancel never reached the pool",
        );

        let _ = release.send(());
        assert!(matches!(expect_result(results), JobResult::Success { .. }));
    });

    attached.handle.shutdown();
}

#[test]
fn free_capacity_falls_while_a_job_is_running() {
    let (release, released) = crossbeam_channel::bounded(1);
    let attached = attach(&["slow"], 2);
    attached.pool.on("slow", Behaviour::Block(released));

    with_running(&attached.dispatcher, |jobs, results| {
        jobs.blocking_send(make_job("job-1", "slow", b""))
            .expect("send job");
        attached.started.recv_timeout(SETTLE).expect("job started");

        // The executor's heartbeat reports one slot occupied. The scheduler
        // reserved that slot itself, so this asserts the two agree rather than
        // that either alone is right.
        wait_until(
            || attached.dispatcher.capacity().free_slots == 1,
            "the heartbeat never reported the occupied slot",
        );

        let _ = release.send(());
        assert!(matches!(expect_result(results), JobResult::Success { .. }));
    });

    attached.handle.shutdown();
}

#[test]
fn a_shutdown_frame_ends_the_session() {
    let attached = attach(&["resize"], 1);
    assert!(attached.handle.is_running());

    // `drain_and_close` writes `shutdown` to every attached executor.
    with_running(&attached.dispatcher, |_jobs, _results| {});

    wait_until(
        || !attached.handle.is_running(),
        "a shutdown frame must end the session",
    );
    attached.handle.wait();
}

#[test]
fn stop_finishes_in_flight_work_before_disconnecting() {
    // The SIGTERM path: an executor asked to stop must still report the job it
    // is holding, or that job waits for a reap it never needed.
    let (release, released) = crossbeam_channel::bounded(1);
    let attached = attach(&["slow"], 1);
    attached.pool.on("slow", Behaviour::Block(released));

    with_running(&attached.dispatcher, |jobs, results| {
        jobs.blocking_send(make_job("job-1", "slow", b""))
            .expect("send job");
        attached.started.recv_timeout(SETTLE).expect("job started");

        attached.handle.stop();

        // The zero-capacity heartbeat is what tells the scheduler to stop
        // dispatching; it must land before the connection goes away.
        wait_until(
            || attached.dispatcher.capacity().free_slots == 0,
            "the drain never announced zero capacity",
        );

        let _ = release.send(());
        match expect_result(results) {
            JobResult::Success { job_id, .. } => assert_eq!(job_id, "job-1"),
            ref other => panic!("in-flight work must still report, got {}", kind(other)),
        }
    });

    attached.handle.shutdown();
}

#[test]
fn a_drain_announces_zero_capacity_before_anything_else() {
    let (mut scheduler, handle, _pool) = FakeScheduler::attach(&["resize"], 2);
    scheduler.expect_heartbeat(2);

    handle.stop();

    // This is what makes the drain clean rather than a race: the scheduler is
    // told to stop dispatching in-protocol, before the connection goes away.
    scheduler.expect_heartbeat(0);
    handle.shutdown();
}

#[test]
fn a_job_arriving_after_the_drain_is_declined_retryably() {
    // A job already on the wire when the zero-capacity heartbeat landed. The
    // reaper would recover it either way, but declining reschedules it now
    // instead of costing a whole reap cycle.
    let (mut scheduler, handle, pool) = FakeScheduler::attach(&["resize"], 1);
    handle.stop();
    scheduler.expect_heartbeat(0);

    scheduler.send_job("job-late", "resize", b"");

    match scheduler.expect_result() {
        ExecutorMessage::Failure {
            job_id,
            should_retry,
            error,
            retry_count,
            timed_out,
            ..
        } => {
            assert_eq!(job_id, "job-late");
            assert!(should_retry, "a declined job must be retryable");
            assert!(!timed_out, "never started is not a timeout");
            assert_eq!(
                retry_count, 1,
                "the frame's retry count must be echoed back"
            );
            assert!(error.contains("draining"), "error explains why: {error}");
        }
        other => panic!("expected a retryable failure, got {other:?}"),
    }

    assert!(
        pool.received("job-late").is_none(),
        "a declined job must never reach the pool"
    );
    handle.shutdown();
}

#[test]
fn a_declined_job_releases_the_toggle_list_it_arrived_with() {
    // The list is recorded before the executor knows the job will be declined,
    // and a decline reports outside the result path that normally releases it.
    let (mut scheduler, handle, _pool) = FakeScheduler::attach(&["resize"], 1);
    let side_channel = handle.side_channel();
    handle.stop();
    scheduler.expect_heartbeat(0);

    scheduler.send_job_with("job-late", "resize", b"", vec!["tracing".to_string()]);
    assert!(matches!(
        scheduler.expect_result(),
        ExecutorMessage::Failure { .. }
    ));

    let deadline = Instant::now() + SETTLE;
    while !side_channel.disabled_middleware("job-late").is_empty() {
        assert!(
            Instant::now() < deadline,
            "a declined job left its toggle list behind"
        );
        thread::sleep(Duration::from_millis(10));
    }

    handle.shutdown();
}

#[test]
fn a_cancel_for_an_unknown_job_is_harmless() {
    // Cancels race completion by nature; one for a job that already finished
    // must not desync the stream or take the executor down.
    let (mut scheduler, handle, pool) = FakeScheduler::attach(&["resize"], 1);
    scheduler
        .writer
        .write_header(&SchedulerMessage::Cancel {
            job_id: "job-gone".to_string(),
        })
        .expect("send cancel");

    wait_until(
        || pool.cancelled() == ["job-gone"],
        "the cancel never reached the pool",
    );
    assert!(
        handle.is_running(),
        "a stray cancel must not end the session"
    );

    // The stream still works afterwards.
    scheduler.send_job("job-1", "resize", b"");
    assert!(matches!(
        scheduler.expect_result(),
        ExecutorMessage::Success { .. }
    ));
    handle.shutdown();
}

#[test]
fn a_missing_token_is_reported_as_a_refusal() {
    // The likeliest deployment mistake. It must not surface as a transport
    // error, or the operator goes looking at the network.
    let dispatcher = scheduler(Some("attach-token-0123456789abcdef"));
    let refused = dial(&dispatcher, &["resize"], 1, None)
        .err()
        .expect("an unauthenticated attach must be refused");

    assert!(
        matches!(refused, ExecutorError::Refused),
        "expected a refusal, got {refused}"
    );
    assert!(dispatcher.executors().is_empty());
}

#[test]
fn a_wrong_token_is_reported_as_a_refusal() {
    let dispatcher = scheduler(Some("attach-token-0123456789abcdef"));
    let refused = dial(&dispatcher, &["resize"], 1, Some("wrong-token"))
        .err()
        .expect("a bad credential must be refused");

    assert!(
        matches!(refused, ExecutorError::Refused),
        "expected a refusal, got {refused}"
    );
    assert!(dispatcher.executors().is_empty());
}

#[test]
fn the_right_token_attaches() {
    const TOKEN: &str = "attach-token-0123456789abcdef";
    let dispatcher = scheduler(Some(TOKEN));
    let client = dial(&dispatcher, &["resize"], 1, Some(TOKEN)).expect("attach");

    assert_eq!(client.scheduler_id(), "scheduler-test");
    assert_eq!(dispatcher.executors().len(), 1);

    let (started_tx, _started) = crossbeam_channel::unbounded();
    client.spawn(TestPool::new(started_tx)).shutdown();
}

#[test]
fn a_dead_peer_does_not_attach() {
    // Nothing answers the hello and the far end is gone. The executor must fail
    // rather than sit waiting for a job that is never coming.
    let (scheduler_end, executor_end) = MemoryTransport::pair();
    drop(scheduler_end);

    let error = ExecutorClient::connect(
        Box::new(executor_end),
        ExecutorConfig {
            tasks: vec!["resize".to_string()],
            handshake_timeout: Duration::from_millis(200),
            ..ExecutorConfig::new("test", "0.0.0")
        },
    )
    .err()
    .expect("a dead peer must not attach");

    assert!(
        matches!(
            error,
            ExecutorError::Refused | ExecutorError::Transport(_) | ExecutorError::Protocol(_)
        ),
        "expected a failed handshake, got {error}"
    );
}

#[test]
fn a_garbled_ack_is_a_protocol_error() {
    let (scheduler_end, executor_end) = MemoryTransport::pair();
    let responder = thread::spawn(move || {
        let (_read, mut write, _connection) = Box::new(scheduler_end)
            .split()
            .expect("split scheduler end");
        use std::io::Write;
        let _ = write.write_all(b"this is not a frame header\n");
        let _ = write.flush();
        // Hold the write half open so the reader sees the bytes, not an EOF.
        thread::sleep(Duration::from_millis(300));
    });

    let error = ExecutorClient::connect(
        Box::new(executor_end),
        ExecutorConfig {
            tasks: vec!["resize".to_string()],
            handshake_timeout: Duration::from_secs(1),
            ..ExecutorConfig::new("test", "0.0.0")
        },
    )
    .err()
    .expect("a garbled ack must not attach");

    assert!(
        matches!(error, ExecutorError::Protocol(ProtocolError::Json(_))),
        "expected a protocol error, got {error}"
    );
    let _ = responder.join();
}

#[test]
fn the_executor_detaches_from_the_scheduler_when_it_stops() {
    let attached = attach(&["resize"], 1);
    assert_eq!(attached.dispatcher.executors().len(), 1);

    attached.handle.shutdown();

    wait_until(
        || attached.dispatcher.executors().is_empty(),
        "the scheduler never saw the executor leave",
    );
}

#[test]
fn shutdown_is_bounded_when_a_job_never_finishes() {
    // A task that ignores its cancel must not hang the process forever: the
    // drain budget expires and the executor disconnects, leaving the job to the
    // scheduler's reaper. The job stays wedged for as long as `release` is held,
    // so nothing but the budget can end the shutdown.
    let (release, wedged) = crossbeam_channel::bounded::<()>(1);
    let attached = attach(&["stuck"], 1);
    attached.pool.on("stuck", Behaviour::Wedge(wedged));

    let started = Instant::now();
    with_running(&attached.dispatcher, |jobs, _results| {
        jobs.blocking_send(make_job("job-1", "stuck", b""))
            .expect("send job");
        attached.started.recv_timeout(SETTLE).expect("job started");
    });
    attached.handle.shutdown();

    assert!(
        started.elapsed() < SHUTDOWN_DRAIN * 3,
        "shutdown must be bounded by the drain budget (took {:?})",
        started.elapsed()
    );

    // Held until here so the job could not have finished on its own.
    drop(release);
}

#[test]
fn a_local_stop_releases_a_parked_waiter() {
    // Regression: `stop()` cannot unpark the reader, which is blocked on a read
    // only the scheduler could satisfy. If the session were only ended by the
    // reader, a shell that called `stop()` from a signal handler and then waited
    // would hang forever instead of shutting down.
    let (_scheduler, handle, _pool) = FakeScheduler::attach(&["resize"], 1);
    let session = handle.session();
    assert!(session.is_running());

    let waiting = thread::spawn(move || session.wait());
    thread::sleep(Duration::from_millis(50));
    assert!(!waiting.is_finished(), "the waiter must park while running");

    handle.stop();

    let deadline = Instant::now() + SETTLE;
    while !waiting.is_finished() {
        assert!(
            Instant::now() < deadline,
            "stop() never released the waiter"
        );
        thread::sleep(Duration::from_millis(10));
    }
    waiting.join().expect("waiter thread");
    handle.shutdown();
}

#[test]
fn wait_timeout_reports_a_session_that_is_still_open() {
    let attached = attach(&["resize"], 1);

    assert!(
        !attached.handle.wait_timeout(Duration::from_millis(100)),
        "a live session must not report as finished"
    );
    assert!(attached.handle.is_running());

    attached.handle.shutdown();
}

// ── Side-channel negotiation ────────────────────────────────────────

/// The frames a scheduler that advertised the side-channel receives.
fn drain_side_channel(
    scheduler: &mut FakeScheduler,
    want: usize,
) -> Vec<(ExecutorMessage, Vec<u8>)> {
    let deadline = Instant::now() + SETTLE;
    let mut seen = Vec::new();
    while seen.len() < want {
        assert!(
            Instant::now() < deadline,
            "only {} of {want} side-channel frame(s) arrived",
            seen.len()
        );
        let frame = scheduler.next_frame();
        if !matches!(frame.0, ExecutorMessage::Heartbeat { .. }) {
            seen.push(frame);
        }
    }
    seen
}

#[test]
fn progress_and_logs_reach_a_scheduler_that_advertised_the_side_channel() {
    let (mut scheduler, handle, _pool) =
        FakeScheduler::attach_with(&["resize"], 1, vec![CAP_SIDE_CHANNEL.to_string()]);
    let side_channel = handle.side_channel();
    assert!(side_channel.is_supported());

    side_channel.report_progress("job-1", 42);
    side_channel.write_task_log("job-1", "resize", "info", "halfway", None);
    side_channel.write_task_log("job-1", "resize", "result", "", Some(r#"{"step":3}"#));

    let mut progress = None;
    let mut logs = Vec::new();
    for (frame, payload) in drain_side_channel(&mut scheduler, 3) {
        match frame {
            ExecutorMessage::Progress {
                job_id,
                progress: p,
            } => progress = Some((job_id, p)),
            ExecutorMessage::TaskLog { level, message, .. } => logs.push((level, message, payload)),
            other => panic!("unexpected frame {other:?}"),
        }
    }

    assert_eq!(progress, Some(("job-1".to_string(), 42)));
    assert!(logs.contains(&("info".to_string(), "halfway".to_string(), Vec::new())));
    assert!(logs.contains(&(
        "result".to_string(),
        String::new(),
        br#"{"step":3}"#.to_vec()
    )));

    handle.shutdown();
}

#[test]
fn a_scheduler_that_advertised_nothing_is_never_sent_a_side_channel_frame() {
    // The negotiation path: an executor built with the side-channel attached to
    // a scheduler built without it must degrade to dropping, not to writing a
    // frame the peer would fail to parse.
    let (mut scheduler, handle, _pool) = FakeScheduler::attach(&["resize"], 1);
    let side_channel = handle.side_channel();
    assert!(!side_channel.is_supported());

    side_channel.report_progress("job-1", 42);
    side_channel.write_task_log("job-1", "resize", "info", "halfway", None);

    // A job round-trip is the proof: it can only be read once every frame ahead
    // of it has been, so a leaked progress frame would surface here.
    scheduler.send_job("job-1", "resize", b"payload");
    assert!(matches!(
        scheduler.expect_result(),
        ExecutorMessage::Success { job_id, .. } if job_id == "job-1"
    ));

    handle.shutdown();
}

#[test]
fn a_flood_of_progress_neither_blocks_the_task_nor_grows_without_bound() {
    // A progress-reporting loop is the shape that would otherwise stall a task
    // on the socket. Progress is idempotent-latest, so a backlog collapses.
    let (mut scheduler, handle, _pool) =
        FakeScheduler::attach_with(&["resize"], 1, vec![CAP_SIDE_CHANNEL.to_string()]);
    let side_channel = handle.side_channel();

    let flooding = Instant::now();
    for percent in 0..10_000 {
        side_channel.report_progress("job-1", percent % 101);
    }
    assert!(
        flooding.elapsed() < SETTLE,
        "reporting progress must never park the task on the scheduler"
    );

    // Whatever coalescing did, the reader is still in sync and the newest value
    // is what eventually lands.
    let deadline = Instant::now() + SETTLE;
    let mut latest = None;
    while latest != Some(9_999 % 101) {
        assert!(Instant::now() < deadline, "the last progress never arrived");
        if let ExecutorMessage::Progress { progress, .. } = scheduler.next_frame().0 {
            latest = Some(progress);
        }
    }

    handle.shutdown();
}

#[test]
fn a_full_outbound_queue_coalesces_progress_and_sheds_the_oldest_logs() {
    // The two halves of the backpressure contract, in the one state that
    // exercises both: a queue that is actually full. Progress is
    // idempotent-latest so a backlog collapses to one value; log lines are data
    // and cannot, so the only bounded answers are "park the task" or "drop".
    //
    // The writer is held shut rather than raced. With nothing draining, every
    // push past the capacity *has* to shed and every progress report *has* to
    // land on the same map entry — which is what makes either assertable at all
    // instead of a coin flip against the drain.
    const CAPACITY: usize = 4;
    const FLOOD: usize = 500;

    let (mut scheduler, handle, _pool, gate) =
        FakeScheduler::attach_stalled(&["resize"], 1, CAPACITY);
    let side_channel = handle.side_channel();

    gate.close();
    let flooding = Instant::now();
    for line in 0..FLOOD {
        side_channel.report_progress("job-1", (line % 101) as i32);
        side_channel.write_task_log("job-1", "resize", "info", &format!("line-{line}"), None);
    }
    assert!(
        flooding.elapsed() < SETTLE,
        "a reporting loop must never park the task on the scheduler"
    );

    // The result loop can hold at most one line beyond the queue — the one it
    // is parked mid-write on — so everything past that had to be shed.
    let dropped = side_channel.dropped_task_logs();
    let unavoidable = (FLOOD - CAPACITY - 1) as u64;
    assert!(
        dropped >= unavoidable,
        "expected at least {unavoidable} shed lines, counted {dropped}"
    );

    gate.release();

    // Drop-oldest, not drop-newest: under a flood the lines nearest the present
    // are the ones worth keeping, so the last one written must still arrive.
    let last_line = format!("line-{}", FLOOD - 1);
    let newest_percent = ((FLOOD - 1) % 101) as i32;
    let deadline = Instant::now() + SETTLE;
    let mut newest_log = None;
    let mut progress = Vec::new();
    while newest_log.as_deref() != Some(last_line.as_str())
        || progress.last() != Some(&newest_percent)
    {
        assert!(
            Instant::now() < deadline,
            "the queue never drained: last log {newest_log:?}, progress {progress:?}"
        );
        match scheduler.next_frame().0 {
            ExecutorMessage::TaskLog { message, .. } => newest_log = Some(message),
            ExecutorMessage::Progress {
                progress: value, ..
            } => progress.push(value),
            _ => {}
        }
    }

    // At most two: the flush already parked mid-write when the gate shut, plus
    // the one that drains everything reported behind it. Never one per report.
    assert!(
        progress.len() <= 2,
        "{FLOOD} reports must coalesce, got {progress:?}"
    );

    handle.shutdown();
}

#[test]
fn a_frame_from_a_newer_scheduler_is_ignored_rather_than_ending_the_session() {
    // An executor released before the scheduler must not treat a frame it has
    // no variant for as a fatal desync: ending the session there would fail
    // every job in flight over a frame this build never needed.
    let (mut scheduler, handle, _pool) = FakeScheduler::attach(&["resize"], 1);

    scheduler.send_future_frame("prefetch", b"payload-it-cannot-read");

    // A job behind the unknown frame is the proof: it can only be read once the
    // skip consumed exactly the right number of bytes.
    scheduler.send_job("job-1", "resize", b"payload");
    assert!(matches!(
        scheduler.expect_result(),
        ExecutorMessage::Success { job_id, .. } if job_id == "job-1"
    ));

    handle.shutdown();
}

#[test]
fn a_frame_from_a_newer_executor_is_ignored_rather_than_abandoning_the_attach() {
    // The same tolerance on the scheduler's reader. Dropping the attach here
    // would deregister a live executor and leave its in-flight jobs for the
    // reaper — a slow, visible failure caused by a frame nobody needed.
    let dispatcher = scheduler(None);
    let mut executor = FakeExecutor::attach(&dispatcher, &["resize"], 1);

    executor.send_future_frame("telemetry", b"payload-it-cannot-read");

    with_running(&dispatcher, |jobs, results| {
        jobs.blocking_send(make_job("job-1", "resize", b"in"))
            .expect("send job");
        executor.run_next_job();
        assert!(matches!(expect_result(results), JobResult::Success { .. }));
        // Asserted while the dispatcher is still running: its own shutdown
        // deregisters every executor, so afterwards this proves nothing.
        assert_eq!(
            dispatcher.executors().len(),
            1,
            "the executor must still be attached"
        );
    });
}

#[test]
fn a_dispatched_toggle_list_is_readable_until_the_job_reports() {
    let (mut scheduler, handle, pool) = FakeScheduler::attach(&["resize"], 1);
    let side_channel = handle.side_channel();
    assert!(
        side_channel.disabled_middleware("job-1").is_empty(),
        "an undispatched job has nothing disabled"
    );

    // Held in flight so the list can be observed while the task is running,
    // which is the only window a handler could read it in.
    let (release, held) = crossbeam_channel::bounded(1);
    pool.on("resize", Behaviour::Block(held));
    scheduler.send_job_with(
        "job-1",
        "resize",
        b"payload",
        vec!["tracing".to_string(), "app.mw.Audit".to_string()],
    );

    let deadline = Instant::now() + SETTLE;
    while side_channel.disabled_middleware("job-1").is_empty() {
        assert!(Instant::now() < deadline, "the toggle list never arrived");
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        side_channel.disabled_middleware("job-1"),
        ["tracing", "app.mw.Audit"]
    );

    // Released once the job reports, or the map would grow for the life of the
    // process.
    let _ = release.send(());
    assert!(matches!(
        scheduler.expect_result(),
        ExecutorMessage::Success { .. }
    ));
    let deadline = Instant::now() + SETTLE;
    while !side_channel.disabled_middleware("job-1").is_empty() {
        assert!(
            Instant::now() < deadline,
            "the toggle list was never released"
        );
        thread::sleep(Duration::from_millis(10));
    }

    handle.shutdown();
}

#[test]
fn a_result_never_overtakes_what_the_task_already_reported() {
    // The scheduler drops a side-channel frame naming a job it no longer
    // holds, and a result is what makes it stop holding one. So a task's final
    // progress — the value a progress bar ends on — is lost unless the
    // executor puts it on the wire ahead of the result.
    let (mut scheduler, handle, pool) =
        FakeScheduler::attach_with(&["resize"], 1, vec![CAP_SIDE_CHANNEL.to_string()]);
    let side_channel = handle.side_channel();

    let (release, held) = crossbeam_channel::bounded(1);
    pool.on("resize", Behaviour::Block(held));
    scheduler.send_job("job-1", "resize", b"payload");

    // Reported while the job is running, exactly as a task body would.
    side_channel.report_progress("job-1", 100);
    side_channel.write_task_log("job-1", "resize", "info", "done", None);
    let _ = release.send(());

    let deadline = Instant::now() + SETTLE;
    let mut seen = Vec::new();
    loop {
        assert!(
            Instant::now() < deadline,
            "no result frame arrived: {seen:?}"
        );
        match scheduler.next_frame().0 {
            ExecutorMessage::Heartbeat { .. } => continue,
            ExecutorMessage::Progress { .. } => seen.push("progress"),
            ExecutorMessage::TaskLog { .. } => seen.push("task_log"),
            ExecutorMessage::Success { .. } => break,
            other => panic!("unexpected frame {other:?}"),
        }
    }

    assert!(
        seen.contains(&"progress") && seen.contains(&"task_log"),
        "both must precede the result, saw {seen:?}"
    );

    handle.shutdown();
}

// ── Durable steps ───────────────────────────────────────────────────

impl FakeScheduler {
    /// Send the snapshot a dispatch would carry.
    fn send_snapshot(&mut self, job_id: &str, steps: &[JobStep]) {
        let payload = encode_step_snapshot(steps);
        self.writer
            .write(
                &SchedulerMessage::JobSteps {
                    job_id: job_id.to_string(),
                    payload_len: payload.len(),
                },
                &payload,
            )
            .expect("send snapshot");
    }

    /// The next `step_commit`, skipping heartbeats.
    fn expect_step_commit(&mut self) -> (String, i32, String, StepKind, Option<i64>, Vec<u8>) {
        let deadline = Instant::now() + SETTLE;
        loop {
            assert!(Instant::now() < deadline, "no step commit arrived");
            let (frame, payload) = self.next_frame();
            match frame {
                ExecutorMessage::Heartbeat { .. } => continue,
                ExecutorMessage::StepCommit {
                    job_id,
                    seq,
                    step_key,
                    kind,
                    wake_at,
                    ..
                } => return (job_id, seq, step_key, kind, wake_at, payload),
                other => panic!("expected a step commit, got {other:?}"),
            }
        }
    }

    fn ack_step(&mut self, job_id: &str, seq: i32, already: bool, wake_at: Option<i64>) {
        self.writer
            .write_header(&SchedulerMessage::StepAck {
                job_id: job_id.to_string(),
                seq,
                ok: true,
                already,
                wake_at,
                error: None,
                failure: None,
            })
            .expect("send ack");
    }

    fn refuse_step(&mut self, job_id: &str, seq: i32, error: &str, failure: StepFailure) {
        self.writer
            .write_header(&SchedulerMessage::StepAck {
                job_id: job_id.to_string(),
                seq,
                ok: false,
                already: false,
                wake_at: None,
                error: Some(error.to_string()),
                failure: Some(failure),
            })
            .expect("send refusal");
    }
}

/// A job as an executor sees one: what `into_dispatch` rebuilds from a frame.
fn running_job(id: &str) -> Job {
    Job {
        id: id.to_string(),
        queue: "default".to_string(),
        task_name: "charge".to_string(),
        payload: Vec::new(),
        status: JobStatus::Running,
        priority: 0,
        created_at: 0,
        scheduled_at: 0,
        started_at: None,
        completed_at: None,
        retry_count: 0,
        max_retries: 3,
        result: None,
        error: None,
        timeout_ms: 300_000,
        unique_key: None,
        progress: None,
        metadata: None,
        notes: None,
        cancel_requested: false,
        expires_at: None,
        result_ttl_ms: None,
        namespace: None,
        has_deps: false,
        debounce_key: None,
    }
}

fn memoized(seq: i32, step_key: &str, result: &[u8]) -> JobStep {
    JobStep {
        job_id: "job-1".to_string(),
        seq,
        step_key: step_key.to_string(),
        kind: StepKind::Run,
        result: Some(result.to_vec()),
        wake_at: None,
        created_at: 0,
    }
}

#[test]
fn a_step_commit_blocks_until_the_scheduler_answers_it() {
    // The whole reason this frame pair exists: an unconfirmed commit is
    // indistinguishable from one that never happened, so the task must not
    // proceed past it.
    let (mut scheduler, handle, _pool) = FakeScheduler::attach_with_steps(&["charge"], 1);
    let job = running_job("job-1");
    let mut session = handle
        .steps()
        .open_session(&job, StepLimits::default())
        .expect("open a session");

    let running = thread::spawn(move || session.run("charge", None, |_| Ok(b"receipt".to_vec())));

    let (job_id, seq, step_key, kind, wake_at, payload) = scheduler.expect_step_commit();
    assert_eq!(
        (job_id.as_str(), seq, step_key.as_str()),
        ("job-1", 0, "charge#0")
    );
    assert_eq!(kind, StepKind::Run);
    assert_eq!(wake_at, None);
    assert_eq!(
        payload, b"receipt",
        "the blob is the encoded result, verbatim"
    );

    scheduler.ack_step("job-1", 0, false, None);
    assert_eq!(
        running.join().expect("the step thread").expect("commit"),
        b"receipt"
    );
    handle.shutdown();
}

#[test]
fn a_refused_commit_arrives_with_the_classification_the_scheduler_made() {
    let (mut scheduler, handle, _pool) = FakeScheduler::attach_with_steps(&["charge"], 1);
    let job = running_job("job-1");
    let mut session = handle
        .steps()
        .open_session(&job, StepLimits::default())
        .expect("open a session");

    let running = thread::spawn(move || session.run("charge", None, |_| Ok(b"receipt".to_vec())));
    scheduler.expect_step_commit();
    scheduler.refuse_step(
        "job-1",
        0,
        "step divergence on job job-1 at position 0",
        StepFailure::Permanent,
    );

    let error = running
        .join()
        .expect("the step thread")
        .expect_err("a refused commit must fail the step");
    // The message reads whole — no wrapper prefix — and the verdict survived
    // the crossing, which is what keeps a retry from burning the budget.
    assert_eq!(
        error.to_string(),
        "step divergence on job job-1 at position 0"
    );
    assert_eq!(classify_step_failure(&error), StepFailure::Permanent);
    handle.shutdown();
}

#[test]
fn a_lost_fence_comes_back_as_a_lost_claim() {
    // `Superseded` is the one verdict that ends an attempt without a result:
    // the job is running correctly under another owner, and failing it here
    // would kill that run.
    let (mut scheduler, handle, _pool) = FakeScheduler::attach_with_steps(&["charge"], 1);
    let job = running_job("job-1");
    let mut session = handle
        .steps()
        .open_session(&job, StepLimits::default())
        .expect("open a session");

    let running = thread::spawn(move || session.run("charge", None, |_| Ok(b"receipt".to_vec())));
    scheduler.expect_step_commit();
    scheduler.refuse_step("job-1", 0, "claim lost", StepFailure::Superseded);

    let error = running
        .join()
        .expect("the step thread")
        .expect_err("refused");
    assert_eq!(classify_step_failure(&error), StepFailure::Superseded);
    assert_eq!(error.to_string(), "execution claim lost for job job-1");
    handle.shutdown();
}

#[test]
fn an_ack_for_another_step_leaves_this_one_waiting() {
    // Correlation is `(job_id, seq)`. One executor runs many jobs, and the
    // pairing must not depend on only one step being in flight.
    let (mut scheduler, handle, _pool) = FakeScheduler::attach_with_steps(&["charge"], 1);
    let job = running_job("job-1");
    let mut session = handle
        .steps()
        .open_session(&job, StepLimits::default())
        .expect("open a session");

    let (done_tx, done) = crossbeam_channel::bounded(1);
    let running = thread::spawn(move || {
        let outcome = session.run("charge", None, |_| Ok(b"receipt".to_vec()));
        let _ = done_tx.send(());
        outcome
    });

    scheduler.expect_step_commit();
    scheduler.ack_step("job-1", 7, false, None);
    scheduler.ack_step("job-2", 0, false, None);
    assert!(
        done.recv_timeout(Duration::from_millis(150)).is_err(),
        "an ack for another step must not release this one"
    );

    scheduler.ack_step("job-1", 0, false, None);
    assert_eq!(running.join().expect("thread").expect("commit"), b"receipt");
    handle.shutdown();
}

#[test]
fn a_scheduler_that_stops_answering_fails_the_step_retryably() {
    // The only genuinely uncertain case, and the replay re-runs the step under
    // the same downstream idempotency key, which is what makes it safe.
    let (mut scheduler, handle, _pool) = FakeScheduler::attach_with_steps(&["charge"], 1);
    let job = running_job("job-1");
    let mut session = handle
        .steps()
        .open_session(&job, StepLimits::default())
        .expect("open a session");

    let running = thread::spawn(move || session.run("charge", None, |_| Ok(b"receipt".to_vec())));
    scheduler.expect_step_commit();

    let error = running
        .join()
        .expect("the step thread")
        .expect_err("an unanswered commit must fail the step");
    assert_eq!(classify_step_failure(&error), StepFailure::Retryable);
    assert!(error.to_string().contains("did not acknowledge"), "{error}");
    handle.shutdown();
}

#[test]
fn a_snapshot_that_rode_the_dispatch_answers_a_memo_hit() {
    // No storage read, and no closure: the executor replays from the bytes the
    // scheduler already had in hand.
    let (mut scheduler, handle, _pool) = FakeScheduler::attach_with_steps(&["charge"], 1);
    scheduler.send_snapshot("job-1", &[memoized(0, "charge#0", b"receipt")]);

    // The frame has to land before the session reads it; the reader thread is
    // the only thing between them.
    let job = running_job("job-1");
    let deadline = Instant::now() + SETTLE;
    let mut session = loop {
        let session = handle
            .steps()
            .open_session(&job, StepLimits::default())
            .expect("open a session");
        if session.sequence().recorded_keys() == vec!["charge#0".to_string()] {
            break session;
        }
        assert!(Instant::now() < deadline, "the snapshot never arrived");
        thread::sleep(Duration::from_millis(10));
    };

    let replayed = session
        .run("charge", None, |_| panic!("a memoized step must not run"))
        .expect("memo");
    assert_eq!(replayed, b"receipt");
    handle.shutdown();
}

#[test]
fn a_scheduler_without_the_capability_refuses_every_step() {
    // §9.4. Refusing is the whole point: there is no version of "your charge
    // step silently lost its memo" that beats a failure naming the reason.
    let (_scheduler, handle, _pool) = FakeScheduler::attach(&["charge"], 1);
    let steps = handle.steps();
    assert!(!steps.is_supported());

    let error = steps
        .open_session(&running_job("job-1"), StepLimits::default())
        .err()
        .expect("a session must not open without a step store");
    assert!(error.to_string().contains("does not implement"), "{error}");
    handle.shutdown();
}
