//! Tests for [`ExecutorClient`], driven against a real [`RemoteDispatcher`]
//! over [`MemoryTransport`] so no socket is bound.
//!
//! Both halves of the attach are the shipping implementation — a fake on either
//! side could only prove it agrees with itself.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use crossbeam_channel::{Receiver, Sender};

use super::auth::Secret;
use super::executor::{ExecutorClient, ExecutorConfig, ExecutorError, ExecutorHandle};
use super::protocol::{
    ExecutorMessage, FrameReader, FrameWriter, ProtocolError, SchedulerMessage, PROTOCOL_VERSION,
};
use super::remote::{RemoteConfig, RemoteDispatcher};
use super::transport::{MemoryTransport, ReadHalf, Transport, WriteHalf};
use super::WorkerDispatcher;
use crate::job::{Job, JobStatus};
use crate::scheduler::JobResult;

const SETTLE: Duration = Duration::from_secs(5);

/// What a [`TestPool`] should do with a job.
enum Behaviour {
    Succeed(Option<Vec<u8>>),
    Fail {
        should_retry: bool,
    },
    /// Park until released, so a test can hold a job in flight.
    Block(Receiver<()>),
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
            shutdown_drain: Duration::from_secs(2),
            ..ExecutorConfig::new("test", "0.0.0")
        },
    );
    let _ = accepting.join();
    connected
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
    /// Handshake with an executor and return both ends live.
    fn attach(tasks: &[&str], slots: u32) -> (Self, ExecutorHandle, Arc<TestPool>) {
        let (scheduler_end, executor_end) = MemoryTransport::pair();

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
                })
                .expect("send ack");
            scheduler
        });

        let client = ExecutorClient::connect(
            Box::new(executor_end),
            ExecutorConfig {
                executor_id: "exec-1".to_string(),
                tasks: tasks.iter().map(|task| (*task).to_string()).collect(),
                slots,
                heartbeat_interval: Duration::from_millis(50),
                shutdown_drain: Duration::from_secs(2),
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
    // scheduler's reaper.
    let (_release, released) = crossbeam_channel::bounded::<()>(1);
    let attached = attach(&["stuck"], 1);
    attached.pool.on("stuck", Behaviour::Block(released));

    let started = Instant::now();
    with_running(&attached.dispatcher, |jobs, _results| {
        jobs.blocking_send(make_job("job-1", "stuck", b""))
            .expect("send job");
        attached.started.recv_timeout(SETTLE).expect("job started");
    });
    attached.handle.shutdown();

    assert!(
        started.elapsed() < SETTLE * 3,
        "shutdown must be bounded by the drain budget (took {:?})",
        started.elapsed()
    );
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
