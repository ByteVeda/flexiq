//! Tests for [`RemoteDispatcher`], driven over [`MemoryTransport`] so no
//! socket is bound. A `FakeExecutor` plays the far end of the connection.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};

use flexiq_core::job::{now_millis, Job, JobStatus, NewJob};
use flexiq_core::scheduler::{JobResult, SchedulerConfig};
use flexiq_core::step::StepFailure;
use flexiq_core::storage::records::StepKind;
use flexiq_core::storage::sqlite::SqliteStorage;
use flexiq_core::storage::{Storage, StorageBackend};
use flexiq_core::worker::auth::Secret;
use flexiq_core::worker::protocol::{
    decode_step_snapshot, ExecutorMessage, FrameReader, FrameWriter, ProtocolError,
    SchedulerMessage, CAP_SIDE_CHANNEL, CAP_STEPS, PROTOCOL_VERSION,
};
use flexiq_core::worker::remote::{AttachError, RemoteConfig, RemoteDispatcher};
use flexiq_core::worker::side_channel::{SideChannel, StorageSideChannel};
use flexiq_core::worker::transport::{MemoryTransport, ReadHalf, Transport, WriteHalf};
use flexiq_core::worker::Worker;
use flexiq_core::worker::WorkerDispatcher;

const SETTLE: Duration = Duration::from_secs(5);

/// The executor side of an attached connection.
struct FakeExecutor {
    reader: FrameReader<ReadHalf>,
    writer: FrameWriter<WriteHalf>,
}

impl FakeExecutor {
    /// Attach to `dispatcher`, announcing `tasks` and `slots`.
    fn attach(
        dispatcher: &RemoteDispatcher,
        executor_id: &str,
        tasks: &[&str],
        slots: u32,
    ) -> Result<Self, AttachError> {
        let (executor, attached) = Self::dial(
            dispatcher,
            executor_id,
            tasks,
            slots,
            PROTOCOL_VERSION,
            None,
        );
        attached.map(|_| executor)
    }

    /// Attach announcing `protocol_version`, keeping the executor end whatever
    /// the outcome so a rejected handshake can still be inspected.
    fn attach_with_version(
        dispatcher: &RemoteDispatcher,
        executor_id: &str,
        tasks: &[&str],
        slots: u32,
        protocol_version: u32,
    ) -> (Self, Result<String, AttachError>) {
        Self::dial(
            dispatcher,
            executor_id,
            tasks,
            slots,
            protocol_version,
            None,
        )
    }

    /// Attach announcing `capabilities`, so a test can drive both sides of the
    /// negotiation.
    fn attach_with_capabilities(
        dispatcher: &RemoteDispatcher,
        executor_id: &str,
        tasks: &[&str],
        slots: u32,
        capabilities: &[&str],
    ) -> Result<Self, AttachError> {
        let (executor, attached) = Self::dial_with(
            dispatcher,
            executor_id,
            tasks,
            slots,
            PROTOCOL_VERSION,
            None,
            capabilities,
        );
        attached.map(|_| executor)
    }

    fn dial(
        dispatcher: &RemoteDispatcher,
        executor_id: &str,
        tasks: &[&str],
        slots: u32,
        protocol_version: u32,
        token: Option<&str>,
    ) -> (Self, Result<String, AttachError>) {
        Self::dial_with(
            dispatcher,
            executor_id,
            tasks,
            slots,
            protocol_version,
            token,
            &[],
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn dial_with(
        dispatcher: &RemoteDispatcher,
        executor_id: &str,
        tasks: &[&str],
        slots: u32,
        protocol_version: u32,
        token: Option<&str>,
        capabilities: &[&str],
    ) -> (Self, Result<String, AttachError>) {
        let (scheduler_end, executor_end) = MemoryTransport::pair();
        let (read, write, _timeout) = Box::new(executor_end).split().expect("split executor end");
        let mut executor = Self {
            reader: FrameReader::new(read),
            writer: FrameWriter::new(write),
        };

        executor
            .writer
            .write_header(
                &ExecutorMessage::hello(
                    executor_id,
                    "test",
                    "0.0.0",
                    tasks.iter().map(|t| (*t).to_string()).collect(),
                    slots,
                )
                .protocol_version(protocol_version)
                .token(token.map(Secret::new))
                .capabilities(capabilities.iter().map(|c| (*c).to_string()).collect())
                .build(),
            )
            .expect("send hello");

        let attached = dispatcher.attach(Box::new(scheduler_end));
        (executor, attached)
    }

    fn read(&mut self) -> Result<(SchedulerMessage, Vec<u8>), ProtocolError> {
        self.reader.read::<SchedulerMessage>()
    }

    fn expect_hello_ack(&mut self) -> u32 {
        self.expect_capabilities().0
    }

    /// The handshake ack, as the version plus what the scheduler advertised.
    fn expect_capabilities(&mut self) -> (u32, Vec<String>) {
        match self.read().expect("read ack").0 {
            SchedulerMessage::HelloAck {
                protocol_version,
                capabilities,
                ..
            } => (protocol_version, capabilities),
            other => panic!("expected hello_ack, got {other:?}"),
        }
    }

    fn expect_shutdown(&mut self) {
        loop {
            match self.read().expect("read frame").0 {
                SchedulerMessage::HelloAck { .. } => continue,
                SchedulerMessage::Shutdown => return,
                other => panic!("expected shutdown, got {other:?}"),
            }
        }
    }

    /// Read the next job frame, skipping the handshake ack if still queued.
    fn expect_job(&mut self) -> (String, String, Vec<u8>) {
        loop {
            match self.read().expect("read frame") {
                (SchedulerMessage::HelloAck { .. }, _) => continue,
                (SchedulerMessage::Job { id, task_name, .. }, payload) => {
                    return (id, task_name, payload)
                }
                (other, _) => panic!("expected a job frame, got {other:?}"),
            }
        }
    }

    /// Read the next job frame along with the snapshot that preceded it, if
    /// any. `None` means no `job_steps` frame arrived — an empty snapshot.
    fn expect_job_with_snapshot(
        &mut self,
    ) -> (String, Option<Vec<flexiq_core::storage::records::JobStep>>) {
        let mut snapshot = None;
        loop {
            match self.read().expect("read frame") {
                (SchedulerMessage::HelloAck { .. }, _) => continue,
                (SchedulerMessage::JobSteps { job_id, .. }, payload) => {
                    snapshot = Some(decode_step_snapshot(&job_id, &payload).expect("decode"));
                }
                (SchedulerMessage::Job { id, .. }, _) => return (id, snapshot),
                (other, _) => panic!("expected a job frame, got {other:?}"),
            }
        }
    }

    fn commit_step(&mut self, job_id: &str, seq: i32, step_key: &str, result: &[u8]) {
        self.writer
            .write(
                &ExecutorMessage::StepCommit {
                    job_id: job_id.to_string(),
                    seq,
                    step_key: step_key.to_string(),
                    kind: StepKind::Run,
                    wake_at: None,
                    payload_len: result.len(),
                },
                result,
            )
            .expect("send step commit");
    }

    fn commit_sleep(&mut self, job_id: &str, seq: i32, step_key: &str, wake_at: i64) {
        self.writer
            .write_header(&ExecutorMessage::StepCommit {
                job_id: job_id.to_string(),
                seq,
                step_key: step_key.to_string(),
                kind: StepKind::Sleep,
                wake_at: Some(wake_at),
                payload_len: 0,
            })
            .expect("send sleep commit");
    }

    /// The next `step_ack`, skipping anything queued ahead of it.
    fn expect_step_ack(&mut self) -> (i32, bool, bool, Option<i64>, Option<StepFailure>) {
        loop {
            match self.read().expect("read frame").0 {
                SchedulerMessage::StepAck {
                    seq,
                    ok,
                    already,
                    wake_at,
                    failure,
                    ..
                } => return (seq, ok, already, wake_at, failure),
                SchedulerMessage::HelloAck { .. }
                | SchedulerMessage::Job { .. }
                | SchedulerMessage::JobSteps { .. } => continue,
                other => panic!("expected a step ack, got {other:?}"),
            }
        }
    }

    /// End the attempt in a sleep, the frame an executor writes *after* its
    /// sleep commit has been acknowledged.
    fn slept(&mut self, job_id: &str, task_name: &str, wake_at: i64) {
        self.writer
            .write_header(&ExecutorMessage::Slept {
                job_id: job_id.to_string(),
                task_name: task_name.to_string(),
                wake_at,
                wall_time_ns: 1,
            })
            .expect("send slept");
    }

    fn succeed(&mut self, job_id: &str, task_name: &str, result: Option<&[u8]>) {
        self.writer
            .write(
                &ExecutorMessage::Success {
                    job_id: job_id.to_string(),
                    result_len: result.map(<[u8]>::len),
                    task_name: task_name.to_string(),
                    wall_time_ns: 1,
                },
                result.unwrap_or(&[]),
            )
            .expect("send success");
    }
}

fn dispatcher_with(placement_timeout: Duration) -> RemoteDispatcher {
    RemoteDispatcher::new(RemoteConfig {
        scheduler_id: "scheduler-test".to_string(),
        placement_timeout,
        // Keep teardown snappy: no test leaves work in flight on purpose
        // except the one that asserts the drain budget is honoured.
        shutdown_drain: Duration::from_millis(200),
        ..RemoteConfig::default()
    })
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

/// Run `body` with the dispatcher's `run` loop live on a current-thread runtime.
fn with_running<F>(dispatcher: &RemoteDispatcher, capacity: usize, body: F)
where
    F: FnOnce(&tokio::sync::mpsc::Sender<Job>, &Receiver<JobResult>),
{
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");

    let (job_tx, job_rx) = tokio::sync::mpsc::channel(capacity);
    let (result_tx, result_rx) = crossbeam_channel::bounded(capacity);

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

#[test]
fn handshake_registers_the_executor() {
    let dispatcher = dispatcher_with(Duration::from_millis(200));
    let mut executor =
        FakeExecutor::attach(&dispatcher, "exec-1", &["resize", "thumbnail"], 3).expect("attach");

    assert_eq!(executor.expect_hello_ack(), PROTOCOL_VERSION);

    let attached = dispatcher.executors();
    assert_eq!(attached.len(), 1);
    assert_eq!(attached[0].executor_id, "exec-1");
    assert_eq!(attached[0].tasks, ["resize", "thumbnail"]);
    assert_eq!(attached[0].slots, 3);
    assert_eq!(attached[0].free_slots, 3);

    let capacity = dispatcher.capacity();
    assert_eq!(capacity.executors, 1);
    assert_eq!(capacity.total_slots, 3);
    assert_eq!(capacity.free_slots, 3);
}

#[test]
fn version_mismatch_is_rejected_but_still_acked() {
    let dispatcher = dispatcher_with(Duration::from_millis(200));
    let (mut executor, attached) = FakeExecutor::attach_with_version(
        &dispatcher,
        "exec-old",
        &["resize"],
        1,
        PROTOCOL_VERSION + 1,
    );

    match attached.expect_err("mismatched version must be rejected") {
        AttachError::Protocol(ProtocolError::VersionMismatch { ours, theirs }) => {
            assert_eq!(ours, PROTOCOL_VERSION);
            assert_eq!(theirs, PROTOCOL_VERSION + 1);
        }
        other => panic!("expected a version mismatch, got {other:?}"),
    }
    // The ack still went out, so the executor can name both versions too.
    assert_eq!(executor.expect_hello_ack(), PROTOCOL_VERSION);
    assert!(dispatcher.executors().is_empty());
}

#[test]
fn duplicate_executor_id_is_rejected() {
    let dispatcher = dispatcher_with(Duration::from_millis(200));
    let _first = FakeExecutor::attach(&dispatcher, "exec-1", &["resize"], 1).expect("attach");
    let err = FakeExecutor::attach(&dispatcher, "exec-1", &["resize"], 1)
        .err()
        .expect("a duplicate id must be rejected");
    assert!(matches!(err, AttachError::DuplicateId(id) if id == "exec-1"));
    assert_eq!(dispatcher.executors().len(), 1);
}

#[test]
fn job_round_trips_to_an_executor_and_back() {
    let dispatcher = dispatcher_with(Duration::from_secs(5));
    let mut executor = FakeExecutor::attach(&dispatcher, "exec-1", &["resize"], 1).expect("attach");

    with_running(&dispatcher, 4, |jobs, results| {
        jobs.blocking_send(make_job("job-1", "resize", b"in"))
            .expect("send job");

        let (job_id, task_name, payload) = executor.expect_job();
        assert_eq!(job_id, "job-1");
        assert_eq!(task_name, "resize");
        assert_eq!(payload, b"in");

        executor.succeed("job-1", "resize", Some(b"out"));

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
}

#[test]
fn unadvertised_task_is_never_sent_and_fails_retryably() {
    let dispatcher = dispatcher_with(Duration::from_millis(100));
    let mut executor = FakeExecutor::attach(&dispatcher, "exec-1", &["resize"], 1).expect("attach");
    assert_eq!(executor.expect_hello_ack(), PROTOCOL_VERSION);

    with_running(&dispatcher, 4, |jobs, results| {
        jobs.blocking_send(make_job("job-1", "transcode", b""))
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
                assert!(should_retry, "an unplaced job must be retryable");
                assert!(!timed_out, "never dispatched is not a timeout");
                assert!(error.contains("transcode"), "error names the task: {error}");
            }
            ref other => panic!("expected a retryable failure, got {}", kind(other)),
        }
    });

    // The next frame after the ack is the shutdown — no job was ever written.
    executor.expect_shutdown();
}

#[test]
fn a_single_slot_admits_one_job_at_a_time() {
    let dispatcher = dispatcher_with(Duration::from_secs(5));
    let mut executor = FakeExecutor::attach(&dispatcher, "exec-1", &["resize"], 1).expect("attach");

    with_running(&dispatcher, 4, |jobs, results| {
        jobs.blocking_send(make_job("job-1", "resize", b""))
            .expect("send first");
        jobs.blocking_send(make_job("job-2", "resize", b""))
            .expect("send second");

        let (first, _, _) = executor.expect_job();
        assert_eq!(first, "job-1");
        assert_eq!(dispatcher.capacity().free_slots, 0, "slot must be reserved");

        // The second job stays unplaced for as long as the slot is occupied —
        // asserted against the dispatcher's own bookkeeping rather than a
        // read timeout, so the test cannot pass on a slow reader.
        let watch = Instant::now() + Duration::from_millis(300);
        while Instant::now() < watch {
            assert_eq!(
                dispatcher.executors()[0].in_flight,
                1,
                "only one job may be in flight on a single-slot executor"
            );
            std::thread::sleep(Duration::from_millis(10));
        }

        executor.succeed("job-1", "resize", None);
        assert!(matches!(expect_result(results), JobResult::Success { .. }));

        let (second, _, _) = executor.expect_job();
        assert_eq!(second, "job-2");
        executor.succeed("job-2", "resize", None);
        assert!(matches!(expect_result(results), JobResult::Success { .. }));
    });
}

#[test]
fn cancel_reaches_the_executor_running_the_job() {
    let dispatcher = dispatcher_with(Duration::from_secs(5));
    let mut executor = FakeExecutor::attach(&dispatcher, "exec-1", &["resize"], 1).expect("attach");

    with_running(&dispatcher, 4, |jobs, results| {
        jobs.blocking_send(make_job("job-1", "resize", b""))
            .expect("send job");
        let (job_id, _, _) = executor.expect_job();
        assert_eq!(job_id, "job-1");

        dispatcher.notify_cancel("job-1");
        match executor.read().expect("read cancel").0 {
            SchedulerMessage::Cancel { job_id } => assert_eq!(job_id, "job-1"),
            other => panic!("expected a cancel frame, got {other:?}"),
        }

        executor
            .writer
            .write_header(&ExecutorMessage::Cancelled {
                job_id: "job-1".to_string(),
                task_name: "resize".to_string(),
                wall_time_ns: 1,
            })
            .expect("send cancelled");
        assert!(matches!(
            expect_result(results),
            JobResult::Cancelled { .. }
        ));
    });
}

#[test]
fn executor_drop_leaves_its_in_flight_job_to_the_reaper() {
    let dispatcher = dispatcher_with(Duration::from_millis(100));
    let mut executor = FakeExecutor::attach(&dispatcher, "exec-1", &["resize"], 1).expect("attach");

    with_running(&dispatcher, 4, |jobs, results| {
        jobs.blocking_send(make_job("job-1", "resize", b""))
            .expect("send job");
        let (job_id, _, _) = executor.expect_job();
        assert_eq!(job_id, "job-1");

        // The executor dies mid-job.
        drop(executor);

        // No result is synthesised: the job may have run, so recovery belongs
        // to the scheduler's reaper, not to the dispatcher.
        match results.recv_timeout(Duration::from_millis(500)) {
            Err(RecvTimeoutError::Timeout) => {}
            Ok(ref unexpected) => panic!(
                "the dispatcher must not synthesise a {} for an abandoned job",
                kind(unexpected)
            ),
            Err(RecvTimeoutError::Disconnected) => panic!("result channel closed"),
        }

        let deadline = Instant::now() + SETTLE;
        while !dispatcher.executors().is_empty() {
            assert!(Instant::now() < deadline, "executor was never deregistered");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(dispatcher.capacity().executors, 0);
    });
}

#[test]
fn heartbeat_can_shrink_capacity_but_not_invent_it() {
    let dispatcher = dispatcher_with(Duration::from_secs(5));
    let mut executor = FakeExecutor::attach(&dispatcher, "exec-1", &["resize"], 4).expect("attach");
    assert_eq!(executor.expect_hello_ack(), PROTOCOL_VERSION);

    executor
        .writer
        .write_header(&ExecutorMessage::Heartbeat { free_slots: 1 })
        .expect("send heartbeat");
    wait_until(|| dispatcher.capacity().free_slots == 1, "capacity shrinks");

    executor
        .writer
        .write_header(&ExecutorMessage::Heartbeat { free_slots: 9 })
        .expect("send heartbeat");
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(
        dispatcher.capacity().free_slots,
        1,
        "a heartbeat must never invent capacity"
    );
}

#[test]
fn a_dropped_executor_leaves_its_job_for_the_scheduler_to_recover() {
    let storage = StorageBackend::Sqlite(SqliteStorage::in_memory().expect("in-memory sqlite"));
    let dispatcher = dispatcher_with(Duration::from_millis(200));
    let mut executor = FakeExecutor::attach(&dispatcher, "exec-1", &["resize"], 1).expect("attach");

    let handle = Worker::new(storage.clone())
        .num_workers(1)
        .scheduler_config(SchedulerConfig {
            poll_interval: Duration::from_millis(10),
            reap_interval: 1,
            ..SchedulerConfig::default()
        })
        .dispatcher("remote", Arc::new(dispatcher.clone()))
        .spawn()
        .expect("spawn");

    let job = storage
        .enqueue(NewJob {
            queue: "default".to_string(),
            task_name: "resize".to_string(),
            payload: b"in".to_vec(),
            priority: 0,
            scheduled_at: now_millis(),
            max_retries: 3,
            // Short enough that the stale-job sweep recovers it promptly.
            timeout_ms: 500,
            unique_key: None,
            metadata: None,
            notes: None,
            depends_on: vec![],
            expires_at: None,
            result_ttl_ms: None,
            namespace: None,
            debounce_key: None,
        })
        .expect("enqueue");

    let (job_id, _, _) = executor.expect_job();
    assert_eq!(job_id, job.id);

    // The executor dies holding the job; the scheduler must recover it.
    drop(executor);
    wait_until(
        || {
            storage
                .get_job(&job.id, None)
                .expect("get_job")
                .is_some_and(|recovered| recovered.retry_count > 0)
        },
        "job was never recovered after the executor dropped",
    );

    handle.shutdown().expect("shutdown");
}

#[test]
fn attach_is_refused_once_shutdown_has_started() {
    // An executor attaching after `drain_and_close` drained the registry would
    // never be told to stop and never be joined — a leaked reader thread.
    let dispatcher = dispatcher_with(Duration::from_millis(200));
    WorkerDispatcher::shutdown(&dispatcher);

    let (_executor, attached) = FakeExecutor::attach_with_version(
        &dispatcher,
        "exec-late",
        &["resize"],
        1,
        PROTOCOL_VERSION,
    );
    assert!(matches!(
        attached.expect_err("attach must be refused during shutdown"),
        AttachError::ShuttingDown
    ));
    assert!(dispatcher.executors().is_empty());
}

#[test]
fn an_idle_executor_outlives_the_handshake_budget() {
    // Regression: the handshake read timeout used to leak onto the attached
    // connection, so every executor was dropped once it idled past it.
    let dispatcher = RemoteDispatcher::new(RemoteConfig {
        scheduler_id: "scheduler-test".to_string(),
        handshake_timeout: Duration::from_millis(50),
        placement_timeout: Duration::from_secs(5),
        shutdown_drain: Duration::from_millis(200),
        ..RemoteConfig::default()
    });
    let mut executor = FakeExecutor::attach(&dispatcher, "exec-1", &["resize"], 1).expect("attach");

    std::thread::sleep(Duration::from_millis(250));
    assert_eq!(
        dispatcher.executors().len(),
        1,
        "an idle executor must not be dropped"
    );

    with_running(&dispatcher, 4, |jobs, results| {
        jobs.blocking_send(make_job("job-1", "resize", b""))
            .expect("send job");
        let (job_id, _, _) = executor.expect_job();
        assert_eq!(job_id, "job-1");
        executor.succeed("job-1", "resize", None);
        assert!(matches!(expect_result(results), JobResult::Success { .. }));
    });
}

#[test]
fn shutdown_is_bounded_by_the_drain_budget() {
    // Regression: shutdown joined reader threads parked on a blocking read, so
    // an executor that stopped responding hung the worker forever.
    let dispatcher = dispatcher_with(Duration::from_secs(5));
    let mut executor = FakeExecutor::attach(&dispatcher, "exec-1", &["resize"], 1).expect("attach");

    let started = Instant::now();
    with_running(&dispatcher, 4, |jobs, _results| {
        jobs.blocking_send(make_job("job-1", "resize", b""))
            .expect("send job");
        // The executor takes the job and then goes silent — it never replies
        // and never closes its end.
        let (job_id, _, _) = executor.expect_job();
        assert_eq!(job_id, "job-1");
    });

    assert!(
        started.elapsed() < SETTLE,
        "shutdown must not wait on an unresponsive executor (took {:?})",
        started.elapsed()
    );
}

/// How many jobs one attached executor is running, by id.
fn in_flight_at(dispatcher: &RemoteDispatcher, executor_id: &str) -> usize {
    dispatcher
        .executors()
        .into_iter()
        .find(|executor| executor.executor_id == executor_id)
        .map(|executor| executor.in_flight)
        .unwrap_or_default()
}

fn wait_until(mut condition: impl FnMut() -> bool, message: &str) {
    let deadline = Instant::now() + SETTLE;
    while !condition() {
        assert!(Instant::now() < deadline, "{message}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

// ── Side-channel ────────────────────────────────────────────────────

/// A [`SideChannel`] that records instead of writing, so a test can assert on
/// exactly what the dispatcher decided to apply.
#[derive(Default)]
struct RecordingSink {
    progress: Mutex<Vec<(String, i32)>>,
    logs: Mutex<Vec<AppliedLog>>,
    disabled: Mutex<HashMap<String, Vec<String>>>,
    /// How long a progress write takes, for tests that need the drain thread to
    /// lose a race it would otherwise sometimes win. Zero everywhere else.
    progress_delay: Mutex<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AppliedLog {
    job_id: String,
    task_name: String,
    level: String,
    message: String,
    extra: Option<String>,
    namespace: Option<String>,
}

impl RecordingSink {
    fn disable(&self, task_name: &str, middleware: &[&str]) {
        self.disabled.lock().expect("disabled").insert(
            task_name.to_string(),
            middleware.iter().map(|name| (*name).to_string()).collect(),
        );
    }

    fn delay_progress(&self, delay: Duration) {
        *self.progress_delay.lock().expect("progress delay") = delay;
    }

    fn progress(&self) -> Vec<(String, i32)> {
        self.progress.lock().expect("progress").clone()
    }

    fn logs(&self) -> Vec<AppliedLog> {
        self.logs.lock().expect("logs").clone()
    }
}

impl SideChannel for RecordingSink {
    fn update_progress(&self, job_id: &str, progress: i32, _namespace: Option<&str>) {
        let delay = *self.progress_delay.lock().expect("progress delay");
        if !delay.is_zero() {
            std::thread::sleep(delay);
        }
        self.progress
            .lock()
            .expect("progress")
            .push((job_id.to_string(), progress));
    }

    fn write_task_log(
        &self,
        job_id: &str,
        task_name: &str,
        level: &str,
        message: &str,
        extra: Option<&str>,
        namespace: Option<&str>,
    ) {
        self.logs.lock().expect("logs").push(AppliedLog {
            job_id: job_id.to_string(),
            task_name: task_name.to_string(),
            level: level.to_string(),
            message: message.to_string(),
            extra: extra.map(str::to_string),
            namespace: namespace.map(str::to_string),
        });
    }

    fn disabled_middleware(&self, task_name: &str) -> Vec<String> {
        self.disabled
            .lock()
            .expect("disabled")
            .get(task_name)
            .cloned()
            .unwrap_or_default()
    }
}

/// A dispatcher applying its side-channel through `sink`.
fn dispatcher_with_sink(sink: Arc<RecordingSink>) -> RemoteDispatcher {
    RemoteDispatcher::new(RemoteConfig {
        scheduler_id: "scheduler-test".to_string(),
        placement_timeout: Duration::from_secs(5),
        shutdown_drain: Duration::from_millis(200),
        side_channel: Some(sink),
        ..RemoteConfig::default()
    })
}

impl FakeExecutor {
    fn expect_ack_capabilities(&mut self) -> Vec<String> {
        match self.read().expect("read ack").0 {
            SchedulerMessage::HelloAck { capabilities, .. } => capabilities,
            other => panic!("expected hello_ack, got {other:?}"),
        }
    }

    /// Read the next job frame in full, skipping the handshake ack.
    fn expect_dispatch(&mut self) -> (Job, Vec<String>) {
        loop {
            match self.read().expect("read frame") {
                (SchedulerMessage::HelloAck { .. }, _) => continue,
                (frame, payload) => {
                    let dispatch = frame.into_dispatch(payload).expect("a job frame");
                    return (dispatch.job, dispatch.disabled_middleware);
                }
            }
        }
    }

    fn report_progress(&mut self, job_id: &str, progress: i32) {
        self.writer
            .write_header(&ExecutorMessage::Progress {
                job_id: job_id.to_string(),
                progress,
            })
            .expect("send progress");
    }

    fn report_log(
        &mut self,
        job_id: &str,
        task_name: &str,
        level: &str,
        message: &str,
        extra: Option<&str>,
    ) {
        let (frame, payload) = ExecutorMessage::task_log(job_id, task_name, level, message, extra);
        self.writer.write(&frame, &payload).expect("send task log");
    }
}

#[test]
fn a_scheduler_with_storage_advertises_the_side_channel() {
    let dispatcher = dispatcher_with_sink(Arc::new(RecordingSink::default()));
    let mut executor = FakeExecutor::attach(&dispatcher, "exec-1", &["resize"], 1).expect("attach");
    assert_eq!(executor.expect_ack_capabilities(), [CAP_SIDE_CHANNEL]);
}

#[test]
fn a_scheduler_without_storage_advertises_nothing() {
    // The negotiation contract from the other side: with nothing to apply
    // through, the ack promises nothing and a correct executor sends no frame.
    let dispatcher = dispatcher_with(Duration::from_secs(5));
    let mut executor = FakeExecutor::attach(&dispatcher, "exec-1", &["resize"], 1).expect("attach");
    assert!(executor.expect_ack_capabilities().is_empty());
}

#[test]
fn a_dispatch_carries_the_toggles_and_metadata_an_executor_cannot_read() {
    let sink = Arc::new(RecordingSink::default());
    sink.disable("resize", &["tracing", "app.mw.Audit"]);
    let dispatcher = dispatcher_with_sink(sink);
    let mut executor = FakeExecutor::attach(&dispatcher, "exec-1", &["resize"], 1).expect("attach");

    with_running(&dispatcher, 4, |jobs, results| {
        let mut job = make_job("job-1", "resize", b"payload");
        job.metadata = Some(r#"{"trace_id":"abc"}"#.to_string());
        jobs.blocking_send(job).expect("send job");

        let (dispatched, disabled) = executor.expect_dispatch();
        assert_eq!(disabled, ["tracing", "app.mw.Audit"]);
        assert_eq!(
            dispatched.metadata.as_deref(),
            Some(r#"{"trace_id":"abc"}"#),
            "middleware reads metadata, and an executor cannot fetch the row"
        );

        executor.succeed("job-1", "resize", None);
        assert_eq!(kind(&expect_result(results)), "success");
    });
}

#[test]
fn progress_and_logs_from_a_running_job_are_applied() {
    let sink = Arc::new(RecordingSink::default());
    let dispatcher = dispatcher_with_sink(sink.clone());
    let mut executor = FakeExecutor::attach(&dispatcher, "exec-1", &["resize"], 1).expect("attach");

    with_running(&dispatcher, 4, |jobs, results| {
        let mut job = make_job("job-1", "resize", b"payload");
        job.namespace = Some("tenant-a".to_string());
        jobs.blocking_send(job).expect("send job");
        executor.expect_dispatch();

        executor.report_progress("job-1", 50);
        executor.report_log("job-1", "resize", "info", "halfway", None);
        executor.report_log("job-1", "resize", "result", "", Some(r#"{"step":3}"#));

        wait_until(
            || sink.progress().contains(&("job-1".to_string(), 50)),
            "progress was never applied",
        );
        wait_until(
            || sink.logs().len() == 2,
            "the log lines were never applied",
        );

        let logs = sink.logs();
        assert_eq!(
            logs[0],
            AppliedLog {
                job_id: "job-1".to_string(),
                task_name: "resize".to_string(),
                level: "info".to_string(),
                message: "halfway".to_string(),
                extra: None,
                // From the dispatch: only the scheduler knows the namespace.
                namespace: Some("tenant-a".to_string()),
            }
        );
        assert_eq!(logs[1].level, "result");
        assert_eq!(logs[1].extra.as_deref(), Some(r#"{"step":3}"#));

        executor.succeed("job-1", "resize", None);
        assert_eq!(kind(&expect_result(results)), "success");
    });
}

#[test]
fn an_executor_cannot_write_against_a_job_it_is_not_running() {
    // An authenticated executor is trusted to run its own work, not to speak
    // for the fleet: without this check any attached peer could rewrite another
    // executor's progress or forge log lines on its jobs.
    let sink = Arc::new(RecordingSink::default());
    let dispatcher = dispatcher_with_sink(sink.clone());
    let mut executor = FakeExecutor::attach(&dispatcher, "exec-1", &["resize"], 1).expect("attach");

    with_running(&dispatcher, 4, |jobs, results| {
        jobs.blocking_send(make_job("job-1", "resize", b"payload"))
            .expect("send job");
        executor.expect_dispatch();

        executor.report_progress("someone-elses-job", 99);
        executor.report_log("someone-elses-job", "resize", "info", "forged", None);
        // Ordered behind them on the same connection, so its arrival proves the
        // two above were seen and dropped rather than merely still in flight.
        executor.report_progress("job-1", 10);

        wait_until(
            || sink.progress().contains(&("job-1".to_string(), 10)),
            "the executor's own progress was never applied",
        );
        assert!(
            !sink
                .progress()
                .iter()
                .any(|(job_id, _)| job_id == "someone-elses-job"),
            "progress for another executor's job must be dropped"
        );
        assert!(sink.logs().is_empty(), "a forged log line must be dropped");

        executor.succeed("job-1", "resize", None);
        assert_eq!(kind(&expect_result(results)), "success");
    });
}

#[test]
fn a_side_channel_frame_never_settles_the_job_it_names() {
    // Progress and logs are handled before the result path on purpose: taking
    // the in-flight entry is the exactly-once token for a job's one outcome,
    // and a progress report must not spend it.
    let sink = Arc::new(RecordingSink::default());
    let dispatcher = dispatcher_with_sink(sink.clone());
    let mut executor = FakeExecutor::attach(&dispatcher, "exec-1", &["resize"], 1).expect("attach");

    with_running(&dispatcher, 4, |jobs, results| {
        jobs.blocking_send(make_job("job-1", "resize", b"payload"))
            .expect("send job");
        executor.expect_dispatch();

        executor.report_progress("job-1", 10);
        wait_until(|| !sink.progress().is_empty(), "progress was never applied");

        assert!(
            results.recv_timeout(Duration::from_millis(200)).is_err(),
            "a side-channel frame must not produce a job result"
        );

        // The real outcome still lands, which it could not if the in-flight
        // entry had already been taken.
        executor.succeed("job-1", "resize", Some(b"done"));
        assert_eq!(kind(&expect_result(results)), "success");
    });
}

#[test]
fn a_jobs_final_progress_is_applied_before_its_result() {
    // Completing a job archives its row, and `update_progress` writes only the
    // live table — so progress that lands after the result is not late, it is
    // lost. The delay makes the drain thread lose a race it would otherwise
    // sometimes win, which is what made this flaky rather than always wrong.
    let sink = Arc::new(RecordingSink::default());
    sink.delay_progress(Duration::from_millis(200));
    let dispatcher = dispatcher_with_sink(sink.clone());
    let mut executor = FakeExecutor::attach(&dispatcher, "exec-1", &["resize"], 1).expect("attach");

    with_running(&dispatcher, 4, |jobs, results| {
        jobs.blocking_send(make_job("job-1", "resize", b"payload"))
            .expect("send job");
        executor.expect_dispatch();

        // Ordered on the wire, the way a real executor flushes its queues
        // before framing a result.
        executor.report_progress("job-1", 100);
        executor.succeed("job-1", "resize", None);

        assert_eq!(kind(&expect_result(results)), "success");
        assert!(
            sink.progress().contains(&("job-1".to_string(), 100)),
            "the result overtook the task's final progress: {:?}",
            sink.progress()
        );
    });
}

#[test]
fn an_empty_extra_is_stored_as_empty_rather_than_absent() {
    // The frame keeps `Some(0)` and `None` apart, so the row has to as well: an
    // `extra` of `""` that lands as NULL is data the sender did send.
    let sink = Arc::new(RecordingSink::default());
    let dispatcher = dispatcher_with_sink(sink.clone());
    let mut executor = FakeExecutor::attach(&dispatcher, "exec-1", &["resize"], 1).expect("attach");

    with_running(&dispatcher, 4, |jobs, results| {
        jobs.blocking_send(make_job("job-1", "resize", b"payload"))
            .expect("send job");
        executor.expect_dispatch();

        executor.report_log("job-1", "resize", "info", "empty", Some(""));
        executor.report_log("job-1", "resize", "info", "absent", None);

        wait_until(
            || sink.logs().len() == 2,
            "both log lines were never applied",
        );
        let logs = sink.logs();
        assert_eq!(logs[0].extra.as_deref(), Some(""));
        assert_eq!(logs[1].extra, None);

        executor.succeed("job-1", "resize", None);
        assert_eq!(kind(&expect_result(results)), "success");
    });
}

#[test]
fn dropping_a_dispatcher_that_never_ran_stops_the_drain_thread() {
    // The pump starts in `new` but only `run` stops it, so a dispatcher that is
    // built and dropped without running would leak the thread — and with it the
    // sink — for the life of the process.
    let sink = Arc::new(RecordingSink::default());
    let dispatcher = dispatcher_with_sink(Arc::clone(&sink));
    drop(dispatcher);

    assert_eq!(
        Arc::strong_count(&sink),
        1,
        "the drain thread outlived the dispatcher that started it"
    );
}

/// A [`SideChannel`] whose settings read parks until the test releases it,
/// standing in for a slow settings backend.
struct ParkingSink {
    entered: Sender<()>,
    release: Receiver<()>,
}

impl SideChannel for ParkingSink {
    fn update_progress(&self, _job_id: &str, _progress: i32, _namespace: Option<&str>) {}

    fn write_task_log(
        &self,
        _job_id: &str,
        _task_name: &str,
        _level: &str,
        _message: &str,
        _extra: Option<&str>,
        _namespace: Option<&str>,
    ) {
    }

    fn disabled_middleware(&self, _task_name: &str) -> Vec<String> {
        let _ = self.entered.try_send(());
        // Returns as soon as the test drops its sender. Bounded rather than
        // waiting forever so a regression fails the assertion below instead of
        // wedging the runtime this parks on.
        let _ = self.release.recv_timeout(SETTLE * 2);
        vec!["tracing".to_string()]
    }
}

#[test]
fn resolving_toggles_does_not_stall_the_runtime_the_scheduler_shares() {
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
    let (release_tx, release_rx) = crossbeam_channel::bounded::<()>(0);
    let dispatcher = RemoteDispatcher::new(RemoteConfig {
        scheduler_id: "scheduler-test".to_string(),
        placement_timeout: Duration::from_secs(5),
        shutdown_drain: Duration::from_millis(200),
        side_channel: Some(Arc::new(ParkingSink {
            entered: entered_tx,
            release: release_rx,
        })),
        ..RemoteConfig::default()
    });
    let mut executor = FakeExecutor::attach(&dispatcher, "exec-1", &["resize"], 1).expect("attach");

    // One worker thread, so anything that blocks on it blocks the scheduler
    // task too — the constraint `drain_and_close` documents.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("runtime");
    let (job_tx, job_rx) = tokio::sync::mpsc::channel(4);
    let (result_tx, result_rx) = crossbeam_channel::bounded(4);
    let running = {
        let dispatcher = dispatcher.clone();
        runtime.spawn(async move { dispatcher.run(job_rx, result_tx).await })
    };

    job_tx
        .blocking_send(make_job("job-1", "resize", b"payload"))
        .expect("send job");
    entered_rx
        .recv_timeout(SETTLE)
        .expect("the toggle read never started");

    // The read is parked. Had it been running on the worker thread, nothing
    // else on this runtime could make progress.
    let ran = Arc::new(AtomicBool::new(false));
    runtime.spawn({
        let ran = Arc::clone(&ran);
        async move { ran.store(true, Ordering::SeqCst) }
    });
    wait_until(
        || ran.load(Ordering::SeqCst),
        "a parked settings read starved the runtime",
    );

    drop(release_tx);
    let (_, disabled) = executor.expect_dispatch();
    assert_eq!(
        disabled,
        ["tracing"],
        "the resolved list must still reach the dispatch"
    );

    executor.succeed("job-1", "resize", None);
    assert_eq!(kind(&expect_result(&result_rx)), "success");
    drop(job_tx);
    runtime.block_on(async { running.await.expect("run loop") });
}

/// Captures `log` records so the registry-divergence warnings can be asserted.
///
/// The whole point of the check is a line an operator reads, so a test that
/// only exercised the decision helper would leave the wiring unproven. The test
/// binary runs its tests in one process and in parallel, so unrelated records
/// land in the same buffer — every assertion below filters on an executor-id
/// prefix unique to its own test.
mod capture {
    use std::sync::{Mutex, Once, OnceLock};

    static RECORDS: OnceLock<Mutex<Vec<String>>> = OnceLock::new();

    fn records() -> &'static Mutex<Vec<String>> {
        RECORDS.get_or_init(|| Mutex::new(Vec::new()))
    }

    struct Collector;

    static COLLECTOR: Collector = Collector;

    impl log::Log for Collector {
        fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
            metadata.level() <= log::Level::Warn
        }

        fn log(&self, record: &log::Record<'_>) {
            if self.enabled(record.metadata()) {
                records()
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(record.args().to_string());
            }
        }

        fn flush(&self) {}
    }

    /// Install the collector. Only the first call takes effect, which is why
    /// every test that reads warnings calls it rather than relying on ordering.
    pub fn install() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            if log::set_logger(&COLLECTOR).is_ok() {
                log::set_max_level(log::LevelFilter::Warn);
            }
        });
    }

    /// Warnings recorded so far that mention `needle`.
    pub fn warnings_mentioning(needle: &str) -> Vec<String> {
        records()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|line| line.contains(needle))
            .cloned()
            .collect()
    }
}

#[test]
fn divergent_task_registries_warn_and_still_attach() {
    capture::install();
    let dispatcher = dispatcher_with(Duration::from_millis(200));

    let _first =
        FakeExecutor::attach(&dispatcher, "diverge-a", &["alpha", "shared"], 1).expect("attach a");
    let _second =
        FakeExecutor::attach(&dispatcher, "diverge-b", &["beta", "shared"], 1).expect("attach b");

    let warnings = capture::warnings_mentioning("diverge-");
    assert_eq!(
        warnings.len(),
        1,
        "one divergence, one warning; got {warnings:?}"
    );
    let warning = &warnings[0];
    assert!(
        warning.contains("only on diverge-b: beta"),
        "the warning must name what only the joiner runs: {warning}"
    );
    assert!(
        warning.contains("only on diverge-a: alpha"),
        "the warning must name what only the peer runs: {warning}"
    );
    assert!(
        !warning.contains("shared"),
        "a task both of them run is not the problem: {warning}"
    );

    // Divergence is a warning, never a rejection: the registries may differ on
    // purpose, and refusing the attach would turn a diagnostic into an outage.
    assert_eq!(dispatcher.executors().len(), 2);
}

#[test]
fn identical_task_registries_attach_without_a_warning() {
    capture::install();
    let dispatcher = dispatcher_with(Duration::from_millis(200));

    let _first =
        FakeExecutor::attach(&dispatcher, "agree-a", &["alpha", "beta"], 1).expect("attach a");
    // Announced in the other order, to pin that the fingerprint is over the set
    // and not over the list as it happened to be built — announcement order
    // follows import order, which whatever discovered the tasks decides.
    let _second =
        FakeExecutor::attach(&dispatcher, "agree-b", &["beta", "alpha"], 1).expect("attach b");

    assert!(
        capture::warnings_mentioning("agree-").is_empty(),
        "matching registries must be silent: {:?}",
        capture::warnings_mentioning("agree-")
    );
    assert_eq!(dispatcher.executors().len(), 2);
}

/// An executor advertising nothing is deliberately inert, not a registry that
/// differs. Neither ordering may produce a warning — otherwise every scheduler
/// running one alongside real executors would be permanently noisy.
#[test]
fn an_executor_advertising_no_tasks_never_looks_divergent() {
    capture::install();
    let dispatcher = dispatcher_with(Duration::from_millis(200));

    let _inert_first = FakeExecutor::attach(&dispatcher, "silent-first", &[], 1)
        .expect("an executor with no tasks must still attach");
    let _real =
        FakeExecutor::attach(&dispatcher, "silent-real", &["beta", "gamma"], 1).expect("attach");
    let _inert_last =
        FakeExecutor::attach(&dispatcher, "silent-last", &[], 1).expect("attach inert");

    assert!(
        capture::warnings_mentioning("silent-").is_empty(),
        "an executor advertising nothing must not make a fleet look divergent: {:?}",
        capture::warnings_mentioning("silent-")
    );
    assert_eq!(dispatcher.executors().len(), 3);
}

/// The other half of "accepted without a warning storm": a fleet rolling onto a
/// new registry says so once, not once per worker.
#[test]
fn a_fleet_rolling_onto_one_registry_warns_once() {
    capture::install();
    let dispatcher = dispatcher_with(Duration::from_millis(200));

    let _old = FakeExecutor::attach(&dispatcher, "storm-old", &["alpha"], 1).expect("attach old");
    let _new = (0..4)
        .map(|i| {
            FakeExecutor::attach(&dispatcher, &format!("storm-new-{i}"), &["beta"], 1)
                .unwrap_or_else(|e| panic!("attach new-{i}: {e}"))
        })
        .collect::<Vec<_>>();

    let warnings = capture::warnings_mentioning("storm-");
    assert_eq!(
        warnings.len(),
        1,
        "the first executor on the new registry reports it and the rest match it; got {warnings:?}"
    );
    assert!(
        warnings[0].contains("executor storm-new-0"),
        "the first joiner is the one that reports: {}",
        warnings[0]
    );
}

// ── Durable steps ───────────────────────────────────────────────────

/// A scheduler whose side channel is real storage, so a step commit is a row.
fn dispatcher_with_storage(storage: &SqliteStorage) -> RemoteDispatcher {
    dispatcher_with_storage_placing_within(storage, Duration::from_secs(5))
}

/// The same, for a test that asserts a job *fails* placement and would
/// otherwise wait out the default budget to do it.
fn dispatcher_with_storage_placing_within(
    storage: &SqliteStorage,
    placement_timeout: Duration,
) -> RemoteDispatcher {
    let dispatcher = RemoteDispatcher::new(RemoteConfig {
        scheduler_id: "scheduler-test".to_string(),
        placement_timeout,
        shutdown_drain: Duration::from_millis(200),
        side_channel: Some(Arc::new(StorageSideChannel::new(StorageBackend::Sqlite(
            storage.clone(),
        )))),
        ..RemoteConfig::default()
    });
    // What a `Worker` does at startup. Without it there is no fence to write
    // under, and every commit is refused rather than guessed at.
    dispatcher.set_claim_owner("scheduler-test");
    dispatcher
}

/// Enqueue a job and claim it under `owner`, the way the poller does before it
/// hands one to a dispatcher.
fn claimed_job(storage: &SqliteStorage, task_name: &str, owner: &str) -> Job {
    let job = storage
        .enqueue(NewJob {
            queue: "default".to_string(),
            task_name: task_name.to_string(),
            payload: vec![],
            priority: 0,
            scheduled_at: now_millis(),
            max_retries: 3,
            timeout_ms: 300_000,
            unique_key: None,
            metadata: None,
            notes: None,
            depends_on: vec![],
            expires_at: None,
            result_ttl_ms: None,
            namespace: None,
            debounce_key: None,
        })
        .expect("enqueue");
    storage
        .dequeue("default", now_millis() + 1_000, None)
        .expect("dequeue");
    assert!(storage.claim_execution(&job.id, owner).expect("claim"));
    storage.get_job(&job.id, None).expect("get").expect("job")
}

#[test]
fn a_step_commit_is_written_under_the_schedulers_own_claim_owner() {
    // The property the whole frame design exists for: the executor said which
    // job and which step, and nothing else. The owner came from the dispatch.
    let storage = SqliteStorage::in_memory().expect("storage");
    let dispatcher = dispatcher_with_storage(&storage);
    let mut executor =
        FakeExecutor::attach_with_capabilities(&dispatcher, "exec-1", &["charge"], 1, &[CAP_STEPS])
            .expect("attach");

    let job = claimed_job(&storage, "charge", "scheduler-test");
    let job_id = job.id.clone();

    with_running(&dispatcher, 4, |jobs, _results| {
        jobs.blocking_send(job).expect("dispatch");
        let (dispatched, snapshot) = executor.expect_job_with_snapshot();
        assert_eq!(dispatched, job_id);
        assert!(snapshot.is_none(), "a first attempt has no steps to replay");

        executor.commit_step(&job_id, 0, "charge#0", b"receipt");
        let (seq, ok, already, _, failure) = executor.expect_step_ack();
        assert_eq!((seq, ok, already), (0, true, false), "{failure:?}");
    });

    let stored = storage.get_job_steps(&job_id, None).expect("steps");
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].step_key, "charge#0");
    assert_eq!(stored[0].result.as_deref(), Some(b"receipt".as_slice()));
}

#[test]
fn a_retransmitted_commit_is_acknowledged_rather_than_refused() {
    // An ack can be lost, and the executor's only recourse is to send the frame
    // again. Treating that as a second commit would fail the durable path
    // exactly when the network is already failing.
    let storage = SqliteStorage::in_memory().expect("storage");
    let dispatcher = dispatcher_with_storage(&storage);
    let mut executor =
        FakeExecutor::attach_with_capabilities(&dispatcher, "exec-1", &["charge"], 1, &[CAP_STEPS])
            .expect("attach");

    let job = claimed_job(&storage, "charge", "scheduler-test");
    let job_id = job.id.clone();

    with_running(&dispatcher, 4, |jobs, _results| {
        jobs.blocking_send(job).expect("dispatch");
        executor.expect_job_with_snapshot();

        executor.commit_step(&job_id, 0, "charge#0", b"receipt");
        assert!(!executor.expect_step_ack().2, "the first commit is new");

        executor.commit_step(&job_id, 0, "charge#0", b"receipt");
        let (_, ok, already, _, _) = executor.expect_step_ack();
        assert!(ok && already, "a byte-identical re-commit is a success");
    });

    assert_eq!(
        storage.get_job_steps(&job_id, None).expect("steps").len(),
        1
    );
}

#[test]
fn a_commit_for_a_job_the_executor_is_not_running_loses_the_fence() {
    // An authenticated executor is trusted to run its own work, not to speak
    // for the fleet. `Superseded` is the one classification that ends an
    // attempt without a result, which is right: the job is someone else's.
    let storage = SqliteStorage::in_memory().expect("storage");
    let dispatcher = dispatcher_with_storage(&storage);
    let mut executor =
        FakeExecutor::attach_with_capabilities(&dispatcher, "exec-1", &["charge"], 1, &[CAP_STEPS])
            .expect("attach");

    let job = claimed_job(&storage, "charge", "scheduler-test");
    executor.expect_hello_ack();
    executor.commit_step(&job.id, 0, "charge#0", b"receipt");

    let (_, ok, _, _, failure) = executor.expect_step_ack();
    assert!(!ok);
    assert_eq!(failure, Some(StepFailure::Superseded));
    assert!(storage
        .get_job_steps(&job.id, None)
        .expect("steps")
        .is_empty());
}

#[test]
fn a_forged_position_is_refused_permanently() {
    // Deterministic: the same code produces the same wrong `seq` next attempt,
    // so a retry would only burn the budget reproducing the error.
    let storage = SqliteStorage::in_memory().expect("storage");
    let dispatcher = dispatcher_with_storage(&storage);
    let mut executor =
        FakeExecutor::attach_with_capabilities(&dispatcher, "exec-1", &["charge"], 1, &[CAP_STEPS])
            .expect("attach");

    let job = claimed_job(&storage, "charge", "scheduler-test");
    let job_id = job.id.clone();

    with_running(&dispatcher, 4, |jobs, _results| {
        jobs.blocking_send(job).expect("dispatch");
        executor.expect_job_with_snapshot();

        executor.commit_step(&job_id, 7, "charge#0", b"receipt");
        let (_, ok, _, _, failure) = executor.expect_step_ack();
        assert!(!ok);
        assert_eq!(failure, Some(StepFailure::Permanent));
    });

    assert!(storage
        .get_job_steps(&job_id, None)
        .expect("steps")
        .is_empty());
}

#[test]
fn a_replayed_jobs_snapshot_rides_its_dispatch() {
    // §9.1: one read per attempt, and it is the scheduler's. Without this the
    // executor would have to reach for a database it has no credentials for.
    let storage = SqliteStorage::in_memory().expect("storage");
    let dispatcher = dispatcher_with_storage(&storage);
    let mut executor =
        FakeExecutor::attach_with_capabilities(&dispatcher, "exec-1", &["charge"], 1, &[CAP_STEPS])
            .expect("attach");

    let job = claimed_job(&storage, "charge", "scheduler-test");
    let job_id = job.id.clone();
    storage
        .record_step_result(
            &flexiq_core::storage::records::NewJobStep {
                job_id: &job_id,
                seq: 0,
                step_key: "charge#0",
                kind: StepKind::Run,
                result: Some(b"receipt"),
            },
            "scheduler-test",
            0,
            &flexiq_core::step::StepLimits::default(),
            None,
        )
        .expect("commit a step the earlier attempt made");

    with_running(&dispatcher, 4, |jobs, _results| {
        jobs.blocking_send(job).expect("dispatch");
        let (dispatched, snapshot) = executor.expect_job_with_snapshot();
        assert_eq!(dispatched, job_id);
        let snapshot = snapshot.expect("the snapshot must precede the job frame");
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].step_key, "charge#0");
        assert_eq!(snapshot[0].result.as_deref(), Some(b"receipt".as_slice()));
    });
}

#[test]
fn an_executor_that_never_claimed_steps_is_sent_no_snapshot() {
    // The read is not free, and a peer that would only discard the frame should
    // not be paying for it.
    let storage = SqliteStorage::in_memory().expect("storage");
    let dispatcher = dispatcher_with_storage(&storage);
    let mut executor = FakeExecutor::attach(&dispatcher, "exec-1", &["charge"], 1).expect("attach");

    let job = claimed_job(&storage, "charge", "scheduler-test");
    let job_id = job.id.clone();
    storage
        .record_step_result(
            &flexiq_core::storage::records::NewJobStep {
                job_id: &job_id,
                seq: 0,
                step_key: "charge#0",
                kind: StepKind::Run,
                result: Some(b"receipt"),
            },
            "scheduler-test",
            0,
            &flexiq_core::step::StepLimits::default(),
            None,
        )
        .expect("commit");

    with_running(&dispatcher, 4, |jobs, _results| {
        jobs.blocking_send(job).expect("dispatch");
        let (_, snapshot) = executor.expect_job_with_snapshot();
        assert!(snapshot.is_none());
    });
}

#[test]
fn a_sleep_commit_reschedules_the_job_and_echoes_the_stored_deadline() {
    let storage = SqliteStorage::in_memory().expect("storage");
    let dispatcher = dispatcher_with_storage(&storage);
    let mut executor =
        FakeExecutor::attach_with_capabilities(&dispatcher, "exec-1", &["cool"], 1, &[CAP_STEPS])
            .expect("attach");

    let job = claimed_job(&storage, "cool", "scheduler-test");
    let job_id = job.id.clone();
    let deadline = now_millis() + 3_600_000;

    with_running(&dispatcher, 4, |jobs, results| {
        jobs.blocking_send(job).expect("dispatch");
        executor.expect_job_with_snapshot();

        executor.commit_sleep(&job_id, 0, "cool_off#0", deadline);
        let (_, ok, already, wake_at, failure) = executor.expect_step_ack();
        assert!(ok, "{failure:?}");
        assert!(!already);
        assert_eq!(wake_at, Some(deadline));

        // Only then does the attempt end: a sleep that could not be persisted
        // is a failed attempt, not a silent one.
        executor.slept(&job_id, "cool", deadline);
        assert_eq!(kind(&expect_result(results)), "slept");

        let slept = storage.get_job(&job_id, None).expect("get").expect("job");
        assert_eq!(slept.status, JobStatus::Pending);
        assert_eq!(slept.scheduled_at, deadline);

        // The wake: the job comes back through the poller and the replay walks
        // into the same sleep. It must get the *stored* deadline back — else a
        // duration sleep pushes its own deadline an hour further out every time
        // the job crashes into it.
        storage
            .dequeue("default", deadline + 1_000, None)
            .expect("dequeue at the deadline");
        assert!(storage
            .claim_execution(&job_id, "scheduler-test")
            .expect("reclaim"));
        let woken = storage.get_job(&job_id, None).expect("get").expect("job");

        jobs.blocking_send(woken).expect("dispatch the wake");
        let (_, snapshot) = executor.expect_job_with_snapshot();
        let snapshot = snapshot.expect("the sleep is a step row, so it replays");
        assert_eq!(snapshot[0].kind, StepKind::Sleep);
        assert_eq!(snapshot[0].wake_at, Some(deadline));

        executor.commit_sleep(&job_id, 0, "cool_off#0", deadline + 3_600_000);
        let (_, ok, already, wake_at, failure) = executor.expect_step_ack();
        assert!(ok && already, "{failure:?}");
        assert_eq!(wake_at, Some(deadline), "the stored deadline stands");
    });
}

#[test]
fn a_scheduler_with_no_step_store_withholds_the_capability() {
    // Withheld rather than faked: an executor told steps work and then unable
    // to commit one has already run the side effect by the time it finds out.
    let sink = Arc::new(RecordingSink::default());
    let dispatcher = dispatcher_with_sink(sink);
    let mut executor =
        FakeExecutor::attach_with_capabilities(&dispatcher, "exec-1", &["charge"], 1, &[CAP_STEPS])
            .expect("attach");

    let (_, capabilities) = executor.expect_capabilities();
    assert!(capabilities.contains(&CAP_SIDE_CHANNEL.to_string()));
    assert!(!capabilities.contains(&CAP_STEPS.to_string()));
}

#[test]
fn a_scheduler_with_a_step_store_advertises_the_capability() {
    let storage = SqliteStorage::in_memory().expect("storage");
    let dispatcher = dispatcher_with_storage(&storage);
    let mut executor =
        FakeExecutor::attach_with_capabilities(&dispatcher, "exec-1", &["charge"], 1, &[CAP_STEPS])
            .expect("attach");

    let (_, capabilities) = executor.expect_capabilities();
    assert!(capabilities.contains(&CAP_STEPS.to_string()));
}

#[test]
fn a_job_that_slept_is_reported_even_if_the_connection_dies_first() {
    // The one abandoned job that no reaper will ever recover: `sleep_job` left
    // it `Pending` at its deadline, and a `Pending` job is not stale. Without
    // this the scheduler holds its in-flight slot forever.
    let storage = SqliteStorage::in_memory().expect("storage");
    let dispatcher = dispatcher_with_storage(&storage);
    let mut executor =
        FakeExecutor::attach_with_capabilities(&dispatcher, "exec-1", &["cool"], 1, &[CAP_STEPS])
            .expect("attach");

    let job = claimed_job(&storage, "cool", "scheduler-test");
    let job_id = job.id.clone();
    let deadline = now_millis() + 3_600_000;

    with_running(&dispatcher, 4, |jobs, results| {
        jobs.blocking_send(job).expect("dispatch");
        executor.expect_job_with_snapshot();

        executor.commit_sleep(&job_id, 0, "cool_off#0", deadline);
        let (_, ok, _, wake_at, failure) = executor.expect_step_ack();
        assert!(ok, "{failure:?}");
        assert_eq!(wake_at, Some(deadline));

        // The executor dies between the ack and its `slept` frame.
        drop(executor);

        let reported = expect_result(results);
        assert_eq!(kind(&reported), "slept");
        let JobResult::Slept {
            job_id: reported_id,
            wake_at: reported_wake,
            ..
        } = reported
        else {
            unreachable!("just asserted the variant")
        };
        assert_eq!(reported_id, job_id);
        assert_eq!(reported_wake, deadline);
    });
}

// ── Overlapping dispatches of one job id ────────────────────────────

#[test]
fn a_superseded_dispatch_never_relabels_the_running_attempts_fence() {
    // The fence an executor's commit is written under comes from the dispatch
    // record, because the executor cannot be trusted to supply its own attempt.
    // A second dispatch of one id on one connection would make that record say
    // the wrong thing — and the wrong thing is the live attempt, which is
    // precisely the write the fence exists to refuse.
    let storage = SqliteStorage::in_memory().expect("storage");
    let dispatcher = dispatcher_with_storage_placing_within(&storage, Duration::from_millis(200));
    let mut executor =
        FakeExecutor::attach_with_capabilities(&dispatcher, "exec-1", &["charge"], 2, &[CAP_STEPS])
            .expect("attach");

    let job = claimed_job(&storage, "charge", "scheduler-test");
    let job_id = job.id.clone();

    with_running(&dispatcher, 4, |jobs, results| {
        jobs.blocking_send(job).expect("dispatch attempt 0");
        let (dispatched, _) = executor.expect_job_with_snapshot();
        assert_eq!(dispatched, job_id);

        // What the reaper does to a slow attempt: it is indistinguishable from
        // a dead one, so the job moves on to attempt 1 while attempt 0 is still
        // running here — and this executor still has a free slot to be given
        // the new attempt on.
        storage.retry(&job_id, now_millis(), None).expect("retry");
        storage
            .dequeue("default", now_millis() + 1_000, None)
            .expect("dequeue");
        assert!(storage
            .claim_execution(&job_id, "scheduler-test")
            .expect("re-claim"));
        let live = storage.get_job(&job_id, None).expect("get").expect("job");
        assert_eq!(live.retry_count, 1, "storage is at the second attempt");
        jobs.blocking_send(live).expect("dispatch attempt 1");

        // Refused, and retryably: the attempt is placed again once no
        // connection is running an earlier one of it.
        match expect_result(results) {
            JobResult::Failure {
                job_id: failed,
                should_retry,
                error,
                ..
            } => {
                assert_eq!(failed, job_id);
                assert!(should_retry, "a superseded placement must be retryable");
                assert!(
                    error.contains("already running"),
                    "the reason names the aliasing: {error}"
                );
            }
            ref other => panic!("expected a retryable failure, got {}", kind(other)),
        }

        // Attempt 0, still running, commits. Fenced on the attempt it was
        // dispatched at, which storage has moved past — so it is refused. Under
        // an aliased entry it would be fenced on attempt 1 and land in the live
        // attempt's sequence.
        executor.commit_step(&job_id, 0, "charge#0", b"receipt");
        let (_, ok, _, _, failure) = executor.expect_step_ack();
        assert!(
            !ok,
            "a stale attempt must not commit under the live attempt's fence"
        );
        assert_eq!(failure, Some(StepFailure::Superseded));
    });

    assert!(storage
        .get_job_steps(&job_id, None)
        .expect("steps")
        .is_empty());
}

#[test]
fn a_superseded_dispatch_leaves_the_running_attempt_reportable() {
    // Taking the in-flight entry is the exactly-once token for a job's single
    // outcome. Two dispatches of one id share one entry, so whichever attempt
    // reports first spends it and the other's result is dropped as unknown —
    // leaving the job with no outcome at all until the reaper takes it.
    let dispatcher = dispatcher_with(Duration::from_millis(200));
    let mut executor = FakeExecutor::attach(&dispatcher, "exec-1", &["resize"], 2).expect("attach");

    with_running(&dispatcher, 4, |jobs, results| {
        jobs.blocking_send(make_job("job-1", "resize", b""))
            .expect("dispatch attempt 0");
        let (dispatched, _, _) = executor.expect_job();
        assert_eq!(dispatched, "job-1");

        let mut superseding = make_job("job-1", "resize", b"");
        superseding.retry_count = 1;
        jobs.blocking_send(superseding).expect("dispatch attempt 1");

        match expect_result(results) {
            JobResult::Failure {
                job_id,
                should_retry,
                ..
            } => {
                assert_eq!(job_id, "job-1");
                assert!(should_retry);
            }
            ref other => panic!("expected a retryable failure, got {}", kind(other)),
        }

        // The running attempt still holds its own entry, and can still report.
        assert_eq!(
            dispatcher.executors()[0].in_flight,
            1,
            "one dispatch of a job id per connection"
        );
        executor.succeed("job-1", "resize", None);
        assert_eq!(kind(&expect_result(results)), "success");
    });

    // The next frame after the first job is the shutdown: the second dispatch
    // was never written to a connection already running that id.
    executor.expect_shutdown();
}

#[test]
fn a_job_an_executor_is_already_running_is_placed_on_its_peer() {
    // The placement half. Both executors advertise the task and the busy one
    // has the most free slots, so it wins on capacity alone — it must lose on
    // the job id.
    let dispatcher = dispatcher_with(Duration::from_secs(5));
    let mut busy = FakeExecutor::attach(&dispatcher, "exec-1", &["resize"], 3).expect("attach");
    let mut peer = FakeExecutor::attach(&dispatcher, "exec-2", &["resize"], 1).expect("attach");

    with_running(&dispatcher, 4, |jobs, results| {
        jobs.blocking_send(make_job("job-1", "resize", b""))
            .expect("dispatch attempt 0");
        let (first, _, _) = busy.expect_job();
        assert_eq!(first, "job-1", "the executor with the most free slots wins");

        let mut superseding = make_job("job-1", "resize", b"");
        superseding.retry_count = 1;
        jobs.blocking_send(superseding).expect("dispatch attempt 1");

        // Asserted against the dispatcher's own bookkeeping before the frame is
        // read: a placement that went to the wrong executor must fail the test
        // rather than park it on a read that will never return.
        wait_until(
            || in_flight_at(&dispatcher, "exec-2") == 1,
            "the second attempt must be placed on the executor not running it",
        );
        assert_eq!(
            in_flight_at(&dispatcher, "exec-1"),
            1,
            "the busy executor keeps the attempt it is running, and only that one"
        );
        let (second, _, _) = peer.expect_job();
        assert_eq!(second, "job-1");

        // Two connections, two entries, two reportable outcomes — which is the
        // point: the fence sorts out which attempt still speaks for the job,
        // and it can only do that if both are reported.
        busy.succeed("job-1", "resize", None);
        peer.succeed("job-1", "resize", None);
        assert_eq!(kind(&expect_result(results)), "success");
        assert_eq!(kind(&expect_result(results)), "success");
    });
}
