//! Attach authentication: what a peer without the shared secret can reach.
//!
//! Driven over `MemoryTransport` rather than a socket — the property under test
//! is the handshake state machine, not the bind. The assertions are about what
//! a refused peer observes: no ack, no registry entry, and no job frame.

use std::time::Duration;

use taskito_core::job::{Job, JobStatus};
use taskito_core::scheduler::JobResult;
use taskito_core::worker::protocol::{FrameReader, FrameWriter};
use taskito_core::worker::transport::{ReadHalf, WriteHalf};
use taskito_core::worker::MemoryTransport;
use taskito_core::{
    AttachError, ExecutorMessage, ProtocolError, RemoteConfig, RemoteDispatcher, SchedulerMessage,
    Secret, Transport, WorkerDispatcher, PROTOCOL_VERSION,
};

/// The secret an authenticated dispatcher expects in `hello`.
const ATTACH_TOKEN: &str = "attach-token-0123456789abcdef";

/// A token of the same length as [`ATTACH_TOKEN`], differing in one byte, so a
/// rejection cannot be explained by the length check alone.
const WRONG_TOKEN: &str = "attach-token-0123456789abcdeg";

/// A dispatcher requiring [`ATTACH_TOKEN`], with short budgets so a job nobody
/// can run fails back quickly.
fn authenticated_dispatcher() -> RemoteDispatcher {
    RemoteDispatcher::new(RemoteConfig {
        scheduler_id: "scheduler-test".to_string(),
        auth_token: Some(Secret::new(ATTACH_TOKEN)),
        placement_timeout: Duration::from_millis(200),
        shutdown_drain: Duration::from_millis(200),
        ..RemoteConfig::default()
    })
}

/// The executor side of a connection, kept whatever the handshake's outcome so
/// a refused peer can still be inspected.
///
/// The write half is held even though nothing writes again: dropping it closes
/// the connection, which would detach the executor the moment it registered.
struct FakeExecutor {
    reader: FrameReader<ReadHalf>,
    _writer: FrameWriter<WriteHalf>,
}

impl FakeExecutor {
    /// Send `hello` carrying `token` and let the dispatcher answer it.
    fn attach(
        dispatcher: &RemoteDispatcher,
        executor_id: &str,
        token: Option<&str>,
    ) -> (Self, Result<String, AttachError>) {
        Self::attach_speaking(dispatcher, executor_id, token, PROTOCOL_VERSION)
    }

    fn attach_speaking(
        dispatcher: &RemoteDispatcher,
        executor_id: &str,
        token: Option<&str>,
        protocol_version: u32,
    ) -> (Self, Result<String, AttachError>) {
        let (scheduler_end, executor_end) = MemoryTransport::pair();
        let (read, write, _connection) = Box::new(executor_end).split().expect("split");
        let mut writer = FrameWriter::new(write);

        writer
            .write_header(&ExecutorMessage::Hello {
                executor_id: executor_id.to_string(),
                sdk: "test".to_string(),
                version: "0.0.0".to_string(),
                tasks: vec!["greet".to_string()],
                slots: 1,
                protocol_version,
                token: token.map(Secret::new),
            })
            .expect("send hello");

        let attached = dispatcher.attach(Box::new(scheduler_end));
        (
            Self {
                reader: FrameReader::new(read),
                _writer: writer,
            },
            attached,
        )
    }

    fn read(&mut self) -> Result<SchedulerMessage, ProtocolError> {
        self.reader
            .read::<SchedulerMessage>()
            .map(|(frame, _)| frame)
    }
}

#[test]
fn the_configured_token_attaches() {
    let dispatcher = authenticated_dispatcher();
    let (mut executor, attached) = FakeExecutor::attach(&dispatcher, "exec-1", Some(ATTACH_TOKEN));

    assert_eq!(attached.expect("a valid token must attach"), "exec-1");
    assert!(matches!(
        executor.read().expect("read the ack"),
        SchedulerMessage::HelloAck { .. }
    ));
    assert_eq!(dispatcher.capacity().executors, 1);
}

#[test]
fn a_wrong_token_is_refused_without_an_ack() {
    let dispatcher = authenticated_dispatcher();
    let (mut executor, attached) = FakeExecutor::attach(&dispatcher, "exec-1", Some(WRONG_TOKEN));

    assert!(
        matches!(&attached, Err(AttachError::Unauthorized(id)) if id == "exec-1"),
        "a wrong token must be refused"
    );
    assert_eq!(dispatcher.capacity().executors, 0);
    // The dropped transport closes the connection, so the peer sees EOF instead
    // of a scheduler identity it was never entitled to.
    assert!(matches!(executor.read(), Err(ProtocolError::Eof)));
}

#[test]
fn a_missing_token_is_refused() {
    let dispatcher = authenticated_dispatcher();
    let (_executor, attached) = FakeExecutor::attach(&dispatcher, "exec-1", None);

    assert!(matches!(attached, Err(AttachError::Unauthorized(_))));
    assert_eq!(dispatcher.capacity().executors, 0);
}

#[test]
fn a_dispatcher_without_a_token_ignores_one() {
    let dispatcher = RemoteDispatcher::new(RemoteConfig::default());
    let (_executor, attached) = FakeExecutor::attach(&dispatcher, "exec-1", Some("anything"));

    assert_eq!(attached.expect("no credential is required"), "exec-1");
}

#[test]
fn a_valid_token_does_not_bypass_the_version_check() {
    let dispatcher = authenticated_dispatcher();
    let (_executor, attached) = FakeExecutor::attach_speaking(
        &dispatcher,
        "exec-1",
        Some(ATTACH_TOKEN),
        PROTOCOL_VERSION + 1,
    );

    assert!(matches!(
        attached,
        Err(AttachError::Protocol(ProtocolError::VersionMismatch { .. }))
    ));
}

#[test]
fn a_refused_peer_is_never_dispatched_a_job() {
    let dispatcher = authenticated_dispatcher();
    let (mut executor, attached) = FakeExecutor::attach(&dispatcher, "exec-1", Some(WRONG_TOKEN));
    assert!(attached.is_err());

    let result = run_one_job(&dispatcher, make_job("job-1", "greet"));
    assert!(
        matches!(result, JobResult::Failure { should_retry, .. } if should_retry),
        "the job must fail back retryably rather than reach the refused peer"
    );
    assert!(
        matches!(executor.read(), Err(ProtocolError::Eof)),
        "no frame may reach a peer that failed the handshake"
    );
}

/// Drive `dispatcher`'s run loop for exactly one job and return its result.
fn run_one_job(dispatcher: &RemoteDispatcher, job: Job) -> JobResult {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build a runtime");

    let (job_tx, job_rx) = tokio::sync::mpsc::channel(1);
    let (result_tx, result_rx) = crossbeam_channel::bounded(1);
    let running = {
        let dispatcher = dispatcher.clone();
        runtime.spawn(async move { dispatcher.run(job_rx, result_tx).await })
    };

    job_tx.blocking_send(job).expect("queue the job");
    let result = result_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("a result");

    drop(job_tx);
    runtime.block_on(async { running.await.expect("run loop") });
    result
}

fn make_job(id: &str, task_name: &str) -> Job {
    Job {
        id: id.to_string(),
        queue: "default".to_string(),
        task_name: task_name.to_string(),
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
