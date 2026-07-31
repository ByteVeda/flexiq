//! End-to-end: an executor dials the attach listener, the scheduler starts on
//! that attach, and a queued job runs on the executor.
//!
//! The executor here is a hand-rolled frame speaker rather than an SDK, which
//! is the point — it proves the wire contract is all an executor needs.

mod support;

use std::io::BufReader;
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use taskito_core::worker::protocol::{FrameReader, FrameWriter, ProtocolError};
use taskito_core::{
    ExecutorMessage, JobStatus, NewJob, RemoteConfig, RemoteDispatcher, SchedulerMessage, Secret,
    Storage, PROTOCOL_VERSION,
};
use taskito_server::config::listen::AttachListen;
use taskito_server::runtime::listener;
use taskito_server::runtime::scheduler::{SchedulerSettings, SchedulerSupervisor};
use taskito_server::runtime::shutdown::Shutdown;

use support::{poll_until, temp_storage};

/// A job that fails once still has retries left, so the second dispatch is
/// what proves the retry path survives the network hop.
const MAX_RETRIES: i32 = 3;

/// The shared secret the authenticated harness expects.
const ATTACH_TOKEN: &str = "attach-token-0123456789abcdef";

#[test]
fn an_attached_executor_runs_a_queued_job() {
    let storage = temp_storage("attach-success");
    let job = storage
        .enqueue(new_job("greet"))
        .expect("enqueue the job under test");

    let harness = Harness::start(&storage);
    let mut executor = Executor::attach(harness.port(), &["greet"]);

    let dispatched = executor.expect_job();
    executor.succeed(&dispatched);

    poll_until(Duration::from_secs(10), || {
        matches!(
            storage.get_job(&job.id).expect("read the job back"),
            Some(ref current) if current.status == JobStatus::Complete
        )
    })
    .expect("the job must complete on the attached executor");

    harness.stop();
}

#[test]
fn a_failure_on_the_executor_is_retried() {
    let storage = temp_storage("attach-retry");
    let job = storage
        .enqueue(new_job("flaky"))
        .expect("enqueue the job under test");

    let harness = Harness::start(&storage);
    let mut executor = Executor::attach(harness.port(), &["flaky"]);

    let first = executor.expect_job();
    executor.fail(&first, 0);
    // The scheduler reschedules with backoff, so the second dispatch is the
    // assertion: the retry came back to the same attached executor.
    let second = executor.expect_job();
    assert_eq!(second.0, first.0, "the retry must be the same job");
    assert_eq!(second.2, 1, "the retry must carry the incremented count");
    executor.succeed(&second);

    poll_until(Duration::from_secs(15), || {
        matches!(
            storage.get_job(&job.id).expect("read the job back"),
            Some(ref current) if current.status == JobStatus::Complete
        )
    })
    .expect("the retried job must complete");

    harness.stop();
}

#[test]
fn the_scheduler_stays_off_until_an_executor_attaches() {
    let storage = temp_storage("attach-lazy");
    let harness = Harness::start(&storage);

    assert!(
        !harness.supervisor.is_running(),
        "an idle listener must not claim jobs"
    );

    let _executor = Executor::attach(harness.port(), &["greet"]);
    poll_until(Duration::from_secs(5), || harness.supervisor.is_running())
        .expect("the first attach must start the scheduler");

    harness.stop();
}

#[test]
fn a_bad_token_is_refused_before_any_job_reaches_it() {
    let storage = temp_storage("attach-bad-token");
    let job = storage
        .enqueue(new_job("greet"))
        .expect("enqueue the job under test");

    let harness = Harness::start_with_token(&storage, Some(ATTACH_TOKEN));
    let mut impostor = Executor::dial(harness.port(), &["greet"], Some("wrong-token-0123456789"));
    impostor.expect_refused();

    // The listener stays usable: the same job runs on a peer that knows the
    // secret, which also proves the queued job was never handed to the impostor.
    let mut executor = Executor::attach_with_token(harness.port(), &["greet"], Some(ATTACH_TOKEN));
    let dispatched = executor.expect_job();
    assert_eq!(dispatched.0, job.id);
    executor.succeed(&dispatched);

    poll_until(Duration::from_secs(10), || {
        matches!(
            storage.get_job(&job.id).expect("read the job back"),
            Some(ref current) if current.status == JobStatus::Complete
        )
    })
    .expect("the job must complete on the authenticated executor");

    harness.stop();
}

#[test]
fn a_missing_token_is_refused_and_starts_no_scheduler() {
    let storage = temp_storage("attach-missing-token");
    let harness = Harness::start_with_token(&storage, Some(ATTACH_TOKEN));

    let mut impostor = Executor::dial(harness.port(), &["greet"], None);
    impostor.expect_refused();

    assert!(
        !harness.supervisor.is_running(),
        "a refused attach must not start the scheduler"
    );

    harness.stop();
}

fn new_job(task_name: &str) -> NewJob {
    NewJob {
        queue: "default".to_string(),
        task_name: task_name.to_string(),
        payload: b"payload".to_vec(),
        priority: 0,
        scheduled_at: taskito_core::now_millis(),
        max_retries: MAX_RETRIES,
        timeout_ms: 30_000,
        unique_key: None,
        metadata: None,
        notes: None,
        depends_on: vec![],
        expires_at: None,
        result_ttl_ms: None,
        namespace: None,
    }
}

/// A running listener + supervisor pair, torn down in the right order.
struct Harness {
    supervisor: Arc<SchedulerSupervisor>,
    listener: Option<listener::ListenerHandle>,
    shutdown: Shutdown,
}

impl Harness {
    fn start(storage: &taskito_core::StorageBackend) -> Self {
        Self::start_with_token(storage, None)
    }

    fn start_with_token(storage: &taskito_core::StorageBackend, token: Option<&str>) -> Self {
        let dispatcher = RemoteDispatcher::new(RemoteConfig {
            auth_token: token.map(Secret::new),
            // Keep the tests quick: a job nobody advertises should fail back
            // fast rather than hold the run open.
            placement_timeout: Duration::from_secs(5),
            ..RemoteConfig::default()
        });
        let supervisor = Arc::new(SchedulerSupervisor::new(
            storage.clone(),
            dispatcher.clone(),
            SchedulerSettings {
                queues: vec!["default".to_string()],
                namespace: None,
                workers: Some(2),
                maintenance: false,
            },
        ));
        let shutdown = Shutdown::default();
        let listener = listener::spawn(
            // Port 0: the OS picks a free port, so parallel tests never clash.
            AttachListen::Tcp("127.0.0.1:0".parse().expect("valid address")),
            dispatcher,
            supervisor.clone(),
            shutdown.clone(),
        )
        .expect("the listener must bind");

        Self {
            supervisor,
            listener: Some(listener),
            shutdown,
        }
    }

    fn port(&self) -> u16 {
        self.listener
            .as_ref()
            .and_then(listener::ListenerHandle::local_addr)
            .expect("a TCP listener always reports its port")
            .port()
    }

    fn stop(mut self) {
        self.shutdown.trigger();
        if let Some(listener) = self.listener.take() {
            listener.join();
        }
        self.supervisor.shutdown();
    }
}

/// The executor side of the wire: handshake, then job frames in, result frames
/// out.
struct Executor {
    reader: FrameReader<BufReader<TcpStream>>,
    writer: FrameWriter<TcpStream>,
}

/// `(job_id, task_name, retry_count)` of a dispatched job.
type Dispatched = (String, String, i32);

impl Executor {
    fn attach(port: u16, tasks: &[&str]) -> Self {
        Self::attach_with_token(port, tasks, None)
    }

    /// Attach and wait for the ack, which only an accepted peer receives.
    fn attach_with_token(port: u16, tasks: &[&str], token: Option<&str>) -> Self {
        let mut executor = Self::dial(port, tasks, token);
        let (ack, _) = executor
            .reader
            .read::<SchedulerMessage>()
            .expect("the scheduler must answer the handshake");
        assert!(
            matches!(ack, SchedulerMessage::HelloAck { protocol_version, .. }
                if protocol_version == PROTOCOL_VERSION),
            "expected hello_ack, got {ack:?}"
        );
        executor
    }

    /// Connect and send `hello` without waiting for an answer — a refused peer
    /// never gets one.
    fn dial(port: u16, tasks: &[&str], token: Option<&str>) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to the listener");
        stream
            .set_read_timeout(Some(Duration::from_secs(15)))
            .expect("set a read timeout");
        let mut writer =
            FrameWriter::new(stream.try_clone().expect("clone the stream for writing"));
        let reader = FrameReader::new(BufReader::new(stream));

        writer
            .write_header(&ExecutorMessage::Hello {
                executor_id: format!("test-executor-{port}"),
                sdk: "test".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                tasks: tasks.iter().map(|task| task.to_string()).collect(),
                slots: 2,
                protocol_version: PROTOCOL_VERSION,
                token: token.map(Secret::new),
            })
            .expect("send hello");

        Self { reader, writer }
    }

    /// Assert the scheduler closed the connection instead of answering.
    fn expect_refused(&mut self) {
        match self.reader.read::<SchedulerMessage>() {
            Err(ProtocolError::Eof) | Err(ProtocolError::Io(_)) => {}
            Ok((frame, _)) => panic!("a refused peer must receive nothing, got {frame:?}"),
            Err(error) => panic!("expected the connection to close, got {error}"),
        }
    }

    fn expect_job(&mut self) -> Dispatched {
        let (frame, payload) = self
            .reader
            .read::<SchedulerMessage>()
            .expect("read a scheduler frame");
        match frame {
            SchedulerMessage::Job {
                id,
                task_name,
                retry_count,
                payload_len,
                ..
            } => {
                assert_eq!(payload.len(), payload_len, "declared length must hold");
                (id, task_name, retry_count)
            }
            // The scheduler sends nothing else to an idle executor, so
            // anything here is a protocol regression worth failing on.
            other => panic!("expected a job frame, got {other:?}"),
        }
    }

    fn succeed(&mut self, job: &Dispatched) {
        self.writer
            .write_header(&ExecutorMessage::Success {
                job_id: job.0.clone(),
                result_len: None,
                task_name: job.1.clone(),
                wall_time_ns: 1_000,
            })
            .expect("send the success frame");
    }

    fn fail(&mut self, job: &Dispatched, retry_count: i32) {
        self.writer
            .write_header(&ExecutorMessage::Failure {
                job_id: job.0.clone(),
                error: "deliberate failure".to_string(),
                retry_count,
                max_retries: MAX_RETRIES,
                task_name: job.1.clone(),
                wall_time_ns: 1_000,
                should_retry: true,
                timed_out: false,
            })
            .expect("send the failure frame");
    }
}
