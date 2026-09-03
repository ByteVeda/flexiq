//! Sending one executor away without dropping its work.
//!
//! `RemoteDispatcher::detach` exists because a connection with a bounded
//! lifetime cannot simply be closed. The window it closes is narrow and easy to
//! reason your way past, so it is pinned here directly: a job is *matched* to an
//! executor before its frame is written, and between those two moments it has no
//! in-flight entry to find it by.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;

use flexiq_core::job::{Job, JobStatus};
use flexiq_core::scheduler::JobResult;
use flexiq_core::worker::protocol::{FrameReader, FrameWriter};
use flexiq_core::worker::side_channel::SideChannel;
use flexiq_core::worker::transport::{ReadHalf, WriteHalf};
use flexiq_core::worker::MemoryTransport;
use flexiq_core::{
    ExecutorMessage, ProtocolError, RemoteConfig, RemoteDispatcher, SchedulerMessage, Transport,
    WorkerDispatcher,
};

/// A side channel whose middleware lookup can be held open.
///
/// `place` reserves a slot, then awaits this, then writes the job frame. Holding
/// it is how a test stands inside the gap between "matched" and "dispatched" —
/// the gap `detach` has to wait out.
struct GatedSideChannel {
    gate: Mutex<Option<Arc<Barrier>>>,
    lookups: AtomicUsize,
}

impl GatedSideChannel {
    fn open() -> Arc<Self> {
        Arc::new(Self {
            gate: Mutex::new(None),
            lookups: AtomicUsize::new(0),
        })
    }

    /// Make the next lookup block until [`release`](Self::release) is called.
    fn hold(&self) -> Arc<Barrier> {
        let barrier = Arc::new(Barrier::new(2));
        *self.gate.lock().expect("gate") = Some(barrier.clone());
        barrier
    }

    fn lookups(&self) -> usize {
        self.lookups.load(Ordering::Relaxed)
    }
}

impl SideChannel for GatedSideChannel {
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
        self.lookups.fetch_add(1, Ordering::Relaxed);
        let gate = self.gate.lock().expect("gate").take();
        if let Some(barrier) = gate {
            // Twice: once to tell the test the reservation is out, once to be
            // let go again.
            barrier.wait();
            barrier.wait();
        }
        Vec::new()
    }
}

/// The executor side of a `MemoryTransport` pair, already handshaken.
struct FakeExecutor {
    reader: FrameReader<ReadHalf>,
    writer: FrameWriter<WriteHalf>,
}

impl FakeExecutor {
    fn attach(dispatcher: &RemoteDispatcher, executor_id: &str, slots: u32) -> Self {
        let (scheduler_end, executor_end) = MemoryTransport::pair();
        let (read, write, _connection) = Box::new(executor_end).split().expect("split");
        let mut writer = FrameWriter::new(write);
        writer
            .write_header(
                &ExecutorMessage::hello(
                    executor_id,
                    "test",
                    "0.0.0",
                    vec!["charge".to_string()],
                    slots,
                )
                .build(),
            )
            .expect("send hello");

        let attached = dispatcher
            .attach(Box::new(scheduler_end))
            .expect("the handshake must succeed");
        assert_eq!(attached, executor_id);

        let mut executor = Self {
            reader: FrameReader::new(read),
            writer,
        };
        assert!(matches!(
            executor.read().expect("read the ack"),
            SchedulerMessage::HelloAck { .. }
        ));
        executor
    }

    fn read(&mut self) -> Result<SchedulerMessage, ProtocolError> {
        self.reader
            .read::<SchedulerMessage>()
            .map(|(frame, _)| frame)
    }

    fn succeed(&mut self, job_id: &str) {
        self.writer
            .write_header(&ExecutorMessage::Success {
                job_id: job_id.to_string(),
                result_len: None,
                task_name: "charge".to_string(),
                wall_time_ns: 1,
                lease: None,
            })
            .expect("report success");
    }
}

fn dispatcher_with(side_channel: Arc<GatedSideChannel>) -> RemoteDispatcher {
    RemoteDispatcher::new(RemoteConfig {
        scheduler_id: "scheduler-test".to_string(),
        placement_timeout: Duration::from_secs(5),
        shutdown_drain: Duration::from_millis(200),
        side_channel: Some(side_channel),
        ..RemoteConfig::default()
    })
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build a runtime")
}

#[test]
fn a_job_matched_before_the_detach_is_still_delivered() {
    let side_channel = GatedSideChannel::open();
    let dispatcher = dispatcher_with(side_channel.clone());
    let mut executor = FakeExecutor::attach(&dispatcher, "exec-1", 1);

    let runtime = runtime();
    let (job_tx, job_rx) = tokio::sync::mpsc::channel(1);
    let (result_tx, result_rx) = crossbeam_channel::bounded(1);
    let running = {
        let dispatcher = dispatcher.clone();
        runtime.spawn(async move { dispatcher.run(job_rx, result_tx).await })
    };

    // The job reserves the executor's only slot and then parks inside the
    // middleware lookup — matched, with nothing in the in-flight map to say so.
    let gate = side_channel.hold();
    job_tx.blocking_send(make_job("job-1")).expect("queue");
    gate.wait();

    // Detaching now must wait for that reservation. `in_flight` is empty, so a
    // drain that trusted it would close the connection here and strand the job.
    let detaching = {
        let dispatcher = dispatcher.clone();
        runtime.spawn(async move { dispatcher.detach("exec-1", Duration::from_secs(5)).await })
    };
    std::thread::sleep(Duration::from_millis(150));
    gate.wait();

    let frame = executor.read().expect("the matched job must still arrive");
    let SchedulerMessage::Job { id, .. } = frame else {
        panic!("expected a job frame, got {frame:?}");
    };
    assert_eq!(id, "job-1");

    executor.succeed("job-1");
    let result = result_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("a result");
    assert!(
        matches!(result, JobResult::Success { ref job_id, .. } if job_id == "job-1"),
        "the drained job must report its own success"
    );

    assert!(runtime.block_on(detaching).expect("detach task").to_owned());
    drop(job_tx);
    runtime.block_on(async { running.await.expect("run loop") });
}

#[test]
fn a_detached_executor_is_no_longer_matched_work() {
    let side_channel = GatedSideChannel::open();
    let dispatcher = dispatcher_with(side_channel.clone());
    let mut executor = FakeExecutor::attach(&dispatcher, "exec-1", 2);

    let runtime = runtime();
    assert!(runtime.block_on(dispatcher.detach("exec-1", Duration::from_secs(5))));

    let (job_tx, job_rx) = tokio::sync::mpsc::channel(1);
    let (result_tx, result_rx) = crossbeam_channel::bounded(1);
    let running = {
        let dispatcher = dispatcher.clone();
        runtime.spawn(async move { dispatcher.run(job_rx, result_tx).await })
    };
    job_tx.blocking_send(make_job("job-1")).expect("queue");

    // Nothing is dispatched, and the job waits out its placement budget rather
    // than reaching a closed stream.
    let result = result_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("a result");
    assert!(
        matches!(result, JobResult::Failure { should_retry, .. } if should_retry),
        "an unplaceable job must come back retryably"
    );
    assert_eq!(
        side_channel.lookups(),
        0,
        "a detached executor must never be matched, so nothing is resolved for it"
    );
    assert!(
        matches!(executor.read(), Err(ProtocolError::Eof)),
        "the detached connection is closed, not merely unmatched"
    );

    drop(job_tx);
    runtime.block_on(async { running.await.expect("run loop") });
}

#[test]
fn detaching_an_executor_that_is_not_attached_says_so() {
    let dispatcher = dispatcher_with(GatedSideChannel::open());
    let runtime = runtime();
    assert!(!runtime.block_on(dispatcher.detach("nobody", Duration::from_millis(50))));
}

#[test]
fn a_replacement_stream_takes_the_work_the_draining_one_refused() {
    // The rotation case end to end: one executor goes away, another attaches
    // under a new id, and the job that was waiting is placed on it. A draining
    // executor still counts as advertising its tasks, or this job would have
    // failed as "nobody advertises it" instead of waiting.
    let side_channel = GatedSideChannel::open();
    let dispatcher = dispatcher_with(side_channel);
    let _departing = FakeExecutor::attach(&dispatcher, "exec-1", 1);

    let runtime = runtime();
    let (job_tx, job_rx) = tokio::sync::mpsc::channel(1);
    let (result_tx, result_rx) = crossbeam_channel::bounded(1);
    let running = {
        let dispatcher = dispatcher.clone();
        runtime.spawn(async move { dispatcher.run(job_rx, result_tx).await })
    };

    assert!(runtime.block_on(dispatcher.detach("exec-1", Duration::from_secs(5))));
    job_tx.blocking_send(make_job("job-1")).expect("queue");

    std::thread::sleep(Duration::from_millis(100));
    let mut arriving = FakeExecutor::attach(&dispatcher, "exec-2", 1);
    let frame = arriving.read().expect("the replacement is dispatched to");
    assert!(
        matches!(&frame, SchedulerMessage::Job { id, .. } if id == "job-1"),
        "expected the waiting job, got {frame:?}"
    );

    arriving.succeed("job-1");
    let result = result_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("a result");
    assert!(matches!(result, JobResult::Success { .. }));

    drop(job_tx);
    runtime.block_on(async { running.await.expect("run loop") });
}

fn make_job(id: &str) -> Job {
    Job {
        id: id.to_string(),
        queue: "default".to_string(),
        task_name: "charge".to_string(),
        payload: b"payload".to_vec(),
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
