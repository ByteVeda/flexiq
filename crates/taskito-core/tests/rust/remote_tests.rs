//! Tests for [`RemoteDispatcher`], driven over [`MemoryTransport`] so no
//! socket is bound. A `FakeExecutor` plays the far end of the connection.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError};

use taskito_core::job::{now_millis, Job, JobStatus, NewJob};
use taskito_core::scheduler::{JobResult, SchedulerConfig};
use taskito_core::storage::sqlite::SqliteStorage;
use taskito_core::storage::{Storage, StorageBackend};
use taskito_core::worker::auth::Secret;
use taskito_core::worker::protocol::{
    ExecutorMessage, FrameReader, FrameWriter, ProtocolError, SchedulerMessage, CAP_SIDE_CHANNEL,
    PROTOCOL_VERSION,
};
use taskito_core::worker::remote::{AttachError, RemoteConfig, RemoteDispatcher};
use taskito_core::worker::side_channel::SideChannel;
use taskito_core::worker::transport::{MemoryTransport, ReadHalf, Transport, WriteHalf};
use taskito_core::worker::Worker;
use taskito_core::worker::WorkerDispatcher;

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

    fn dial(
        dispatcher: &RemoteDispatcher,
        executor_id: &str,
        tasks: &[&str],
        slots: u32,
        protocol_version: u32,
        token: Option<&str>,
    ) -> (Self, Result<String, AttachError>) {
        let (scheduler_end, executor_end) = MemoryTransport::pair();
        let (read, write, _timeout) = Box::new(executor_end).split().expect("split executor end");
        let mut executor = Self {
            reader: FrameReader::new(read),
            writer: FrameWriter::new(write),
        };

        executor
            .writer
            .write_header(&ExecutorMessage::Hello {
                executor_id: executor_id.to_string(),
                sdk: "test".to_string(),
                version: "0.0.0".to_string(),
                tasks: tasks.iter().map(|t| (*t).to_string()).collect(),
                slots,
                protocol_version,
                token: token.map(Secret::new),
            })
            .expect("send hello");

        let attached = dispatcher.attach(Box::new(scheduler_end));
        (executor, attached)
    }

    fn read(&mut self) -> Result<(SchedulerMessage, Vec<u8>), ProtocolError> {
        self.reader.read::<SchedulerMessage>()
    }

    fn expect_hello_ack(&mut self) -> u32 {
        match self.read().expect("read ack").0 {
            SchedulerMessage::HelloAck {
                protocol_version, ..
            } => protocol_version,
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
        })
        .expect("enqueue");

    let (job_id, _, _) = executor.expect_job();
    assert_eq!(job_id, job.id);

    // The executor dies holding the job; the scheduler must recover it.
    drop(executor);
    wait_until(
        || {
            storage
                .get_job(&job.id)
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

    fn progress(&self) -> Vec<(String, i32)> {
        self.progress.lock().expect("progress").clone()
    }

    fn logs(&self) -> Vec<AppliedLog> {
        self.logs.lock().expect("logs").clone()
    }
}

impl SideChannel for RecordingSink {
    fn update_progress(&self, job_id: &str, progress: i32) {
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
