//! End-to-end: an executor attaches over `flexiq.executor.v1` and runs work.
//!
//! These are `attach_e2e.rs`'s scenarios over the gRPC transport, plus the four
//! things only this transport has to answer: a killed stream, a rotated stream,
//! a heartbeat that is not on the stream, and a socket executor and a gRPC
//! executor sharing one scheduler.
//!
//! The executor is hand-rolled against the *generated* client and builds its own
//! protobuf frames, exactly as `attach_e2e.rs` hand-rolls a socket speaker. It
//! deliberately does not reuse the server's own conversions: a test that shares
//! them cannot see an asymmetry between the two, which is the bug most worth
//! catching here.
#![cfg(feature = "grpc")]

mod support;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use flexiq_core::{
    JobStatus, NewJob, RemoteConfig, RemoteDispatcher, SchedulerMessage, Storage,
    StorageSideChannel, CAP_SIDE_CHANNEL, PROTOCOL_VERSION,
};
use flexiq_server::config::grpc::GrpcConfig;
use flexiq_server::config::listen::ListenAddress;
use flexiq_server::grpc::executor::{ExecutorDoor, Rotation, SESSION_METADATA};
use flexiq_server::grpc::limits::EXECUTOR_MAX_MESSAGE_BYTES;
use flexiq_server::grpc::pb::executor as pb;
use flexiq_server::grpc::pb::executor::executor_service_client::ExecutorServiceClient;
use flexiq_server::grpc::Listener;
use flexiq_server::runtime::listener;
use flexiq_server::runtime::scheduler::{SchedulerSettings, SchedulerSupervisor};
use flexiq_server::runtime::shutdown::Shutdown;
use flexiq_server::tokens::{Scope, ScopeSet};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Code, Streaming};

use support::{mint_token, temp_storage, Bearer, TempStorage};

/// The one namespace this door serves. Jobs are enqueued into it, because the
/// gRPC role refuses to serve the ambiguous unnamespaced one.
const NAMESPACE: &str = "grpc-attach-tests";

/// A job that fails once still has retries left, so the second dispatch is what
/// proves the retry path survives the network hop.
const MAX_RETRIES: i32 = 3;

/// Generous: a job an executor is holding on purpose must not be reaped out
/// from under the test that is holding it.
const JOB_TIMEOUT_MS: i64 = 30_000;

/// Short enough that the stale-job reaper rescues an abandoned job inside a
/// test's patience, rather than in the minutes a production timeout allows.
const ABANDONED_TIMEOUT_MS: i64 = 1_000;

// ── Log capture, for the one assertion that is a log line ─────────

static LOGS: Mutex<Vec<String>> = Mutex::new(Vec::new());

struct Capture;

impl log::Log for Capture {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        if std::env::var_os("FLEXIQ_TEST_LOG").is_some() {
            eprintln!("[{}] {}", record.level(), record.args());
        }
        LOGS.lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(record.args().to_string());
    }

    fn flush(&self) {}
}

/// Record `warn!` and above, so the registry divergence warning — which has no
/// other observable — can be asserted on.
fn capture_logs() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        let _ = log::set_boxed_logger(Box::new(Capture));
        log::set_max_level(if std::env::var_os("FLEXIQ_TEST_LOG").is_some() {
            log::LevelFilter::Debug
        } else {
            log::LevelFilter::Warn
        });
    });
}

fn logged(fragment: &str) -> bool {
    LOGS.lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .any(|line| line.contains(fragment))
}

// ── Harness ──────────────────────────────────────────────────────

/// A gRPC listener serving the executor door, its dispatcher and supervisor.
struct Harness {
    storage: TempStorage,
    dispatcher: RemoteDispatcher,
    supervisor: Arc<SchedulerSupervisor>,
    door: ExecutorDoor,
    addr: SocketAddr,
    token: String,
    attach: Option<listener::ListenerHandle>,
    shutdown: Shutdown,
    served: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Harness {
    async fn start(label: &str) -> Self {
        Self::build(label, Rotation::new(None), false).await
    }

    /// A harness whose executor streams end after `max_age`.
    async fn rotating(label: &str, max_age: Duration) -> Self {
        Self::build(label, Rotation::new(Some(max_age)), false).await
    }

    /// A harness serving both doors on the same dispatcher.
    async fn with_socket_door(label: &str) -> Self {
        Self::build(label, Rotation::new(None), true).await
    }

    async fn build(label: &str, rotation: Rotation, socket_door: bool) -> Self {
        capture_logs();
        let storage = temp_storage(label);
        let token = mint_token(&storage, NAMESPACE, ScopeSet::of(&[Scope::Execute]));

        let dispatcher = RemoteDispatcher::new(RemoteConfig {
            // A job nobody advertises should fail back fast rather than hold
            // the run open.
            placement_timeout: Duration::from_secs(5),
            // Exactly what `runtime::run` installs, so these tests exercise the
            // wiring a deployment actually gets.
            side_channel: Some(Arc::new(StorageSideChannel::new((*storage).clone()))),
            ..RemoteConfig::default()
        });
        let supervisor = Arc::new(SchedulerSupervisor::new(
            (*storage).clone(),
            dispatcher.clone(),
            SchedulerSettings {
                queues: vec!["default".to_string()],
                namespace: Some(NAMESPACE.to_string()),
                workers: Some(2),
                maintenance: false,
            },
        ));
        let shutdown = Shutdown::default();

        let attach = socket_door.then(|| {
            listener::spawn(
                ListenAddress::Tcp("127.0.0.1:0".parse().expect("valid address")),
                dispatcher.clone(),
                supervisor.clone(),
                shutdown.clone(),
            )
            .expect("the socket listener must bind")
        });

        let listener = Listener::bind(&GrpcConfig {
            listen: ListenAddress::Tcp("127.0.0.1:0".parse().expect("valid address")),
            namespace: NAMESPACE.to_string(),
            executor_stream_max_age: Duration::ZERO,
        })
        .await
        .expect("bind");
        let addr = listener
            .local_addr()
            .expect("a TCP listener knows what it bound");
        let door = ExecutorDoor::new(dispatcher.clone(), supervisor.clone(), rotation);
        // Cloned, not moved: the clone shares the session registry, which is
        // what lets a test see whether a finished stream left one behind.
        let served =
            tokio::spawn(listener.serve((*storage).clone(), Some(door.clone()), shutdown.clone()));

        Self {
            storage,
            dispatcher,
            supervisor,
            door,
            addr,
            token,
            attach,
            shutdown,
            served,
        }
    }

    fn socket_port(&self) -> u16 {
        self.attach
            .as_ref()
            .and_then(listener::ListenerHandle::local_addr)
            .expect("a TCP listener always reports its port")
            .port()
    }

    fn enqueue(&self, task_name: &str) -> String {
        self.enqueue_with(task_name, b"payload".to_vec(), JOB_TIMEOUT_MS)
    }

    fn enqueue_with(&self, task_name: &str, payload: Vec<u8>, timeout_ms: i64) -> String {
        self.storage
            .enqueue(NewJob {
                queue: "default".to_string(),
                task_name: task_name.to_string(),
                payload,
                priority: 0,
                scheduled_at: flexiq_core::now_millis(),
                max_retries: MAX_RETRIES,
                timeout_ms,
                unique_key: None,
                metadata: None,
                notes: None,
                depends_on: vec![],
                expires_at: None,
                result_ttl_ms: None,
                namespace: Some(NAMESPACE.to_string()),
                debounce_key: None,
            })
            .expect("enqueue the job under test")
            .id
    }

    fn status(&self, job_id: &str) -> Option<JobStatus> {
        self.storage
            .get_job(job_id, Some(NAMESPACE))
            .expect("read the job back")
            .map(|job| job.status)
    }

    async fn stop(mut self) {
        self.shutdown.trigger();
        if let Some(attach) = self.attach.take() {
            tokio::task::spawn_blocking(move || attach.join())
                .await
                .expect("join the socket listener");
        }
        // Before the listener is awaited, and on a thread of its own. An attach
        // stream is an in-flight request that a graceful gRPC listener waits
        // for, and the stream only ends when the dispatcher's drain closes the
        // connection — which is what this call runs.
        let supervisor = Arc::clone(&self.supervisor);
        tokio::task::spawn_blocking(move || supervisor.shutdown())
            .await
            .expect("shut the scheduler down");
        tokio::time::timeout(Duration::from_secs(60), self.served)
            .await
            .expect("the listener must stop rather than wait on a stream forever")
            .expect("the serve task must not panic")
            .expect("a shutdown is not an error");
    }
}

/// Poll `condition` until it holds or `timeout` elapses.
///
/// Async, unlike `support::poll_until`: the listener runs on this runtime, and
/// a blocking sleep here would stop serving the thing being waited for.
async fn poll_until(timeout: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if condition() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

// ── The executor side of the wire ─────────────────────────────────

type Client = ExecutorServiceClient<InterceptedService<Channel, Bearer>>;

/// `(job_id, task_name, retry_count)` of a dispatched job.
type Dispatched = (String, String, i32);

/// A hand-rolled gRPC executor: protobuf frames in, protobuf frames out.
struct Executor {
    client: Client,
    tx: mpsc::Sender<pb::AttachRequest>,
    inbound: Streaming<pb::AttachResponse>,
    session: Vec<u8>,
}

async fn connect(addr: SocketAddr, token: &str) -> Client {
    let channel = Channel::from_shared(format!("http://{addr}"))
        .expect("a valid endpoint")
        .connect()
        .await
        .expect("the listener must accept a connection");
    // A client's own default is 4 MiB, so an executor that does not raise it
    // cannot receive the payloads this door is sized to carry.
    ExecutorServiceClient::with_interceptor(channel, Bearer::new(token))
        .max_decoding_message_size(EXECUTOR_MAX_MESSAGE_BYTES)
        .max_encoding_message_size(EXECUTOR_MAX_MESSAGE_BYTES)
}

fn hello(id: &str, tasks: &[&str], slots: u32) -> pb::AttachRequest {
    pb::AttachRequest {
        frame: Some(pb::attach_request::Frame::Hello(pb::HelloFrame {
            executor_id: id.to_string(),
            sdk: "test".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            tasks: tasks.iter().map(|task| (*task).to_string()).collect(),
            slots,
            protocol_version: PROTOCOL_VERSION,
            capabilities: Vec::new(),
        })),
    }
}

impl Executor {
    /// Open a stream and complete the handshake.
    async fn attach(harness: &Harness, id: &str, tasks: &[&str]) -> Self {
        let mut executor = Self::dial(harness, &harness.token.clone(), id, tasks, 2)
            .await
            .expect("the attach must be accepted");
        let capabilities = executor.expect_ack().await;
        assert!(
            capabilities.iter().any(|cap| cap == CAP_SIDE_CHANNEL),
            "a scheduler with storage must advertise the side-channel, got {capabilities:?}"
        );
        executor
    }

    /// Open a stream and send `hello`, without waiting for the ack.
    async fn dial(
        harness: &Harness,
        token: &str,
        id: &str,
        tasks: &[&str],
        slots: u32,
    ) -> Result<Self, tonic::Status> {
        let mut client = connect(harness.addr, token).await;
        let (tx, rx) = mpsc::channel(16);
        // Buffered before the call: the handshake blocks on `hello`, and
        // nothing else is written until it lands.
        tx.send(hello(id, tasks, slots))
            .await
            .expect("queue the hello frame");

        let response = client.attach(ReceiverStream::new(rx)).await?;
        let session = response
            .metadata()
            .get_bin(SESSION_METADATA)
            .expect("the attach response must carry a session")
            .to_bytes()
            .expect("a base64 metadata value")
            .to_vec();
        Ok(Self {
            client,
            tx,
            inbound: response.into_inner(),
            session,
        })
    }

    async fn frame(&mut self) -> Option<pb::attach_response::Frame> {
        self.inbound
            .message()
            .await
            .expect("the stream must not error")
            .and_then(|response| response.frame)
    }

    async fn expect_ack(&mut self) -> Vec<String> {
        match self.frame().await {
            Some(pb::attach_response::Frame::HelloAck(ack)) => {
                assert_eq!(ack.protocol_version, PROTOCOL_VERSION);
                ack.capabilities
            }
            other => panic!("expected hello_ack, got {other:?}"),
        }
    }

    async fn expect_job(&mut self) -> Dispatched {
        match self.frame().await {
            Some(pb::attach_response::Frame::Job(job)) => (job.id, job.task_name, job.retry_count),
            other => panic!("expected a job frame, got {other:?}"),
        }
    }

    /// The next job frame, or `None` if the stream ended or nothing arrived.
    async fn next_job(&mut self, within: Duration) -> Option<Dispatched> {
        match tokio::time::timeout(within, self.frame()).await {
            Ok(Some(pb::attach_response::Frame::Job(job))) => {
                Some((job.id, job.task_name, job.retry_count))
            }
            Ok(Some(other)) => panic!("expected a job frame, got {other:?}"),
            Ok(None) | Err(_) => None,
        }
    }

    async fn send(&self, frame: pb::attach_request::Frame) {
        self.tx
            .send(pb::AttachRequest { frame: Some(frame) })
            .await
            .expect("the stream must accept the frame");
    }

    async fn succeed(&self, job: &Dispatched) {
        self.send(pb::attach_request::Frame::Success(pb::SuccessFrame {
            job_id: job.0.clone(),
            task_name: job.1.clone(),
            result: None,
            wall_time: None,
            // This executor never advertises the lease capability, so the
            // scheduler dispatches it none and requires none back.
            lease: None,
        }))
        .await;
    }

    async fn fail(&self, job: &Dispatched, retry_count: i32) {
        self.send(pb::attach_request::Frame::Failure(pb::FailureFrame {
            job_id: job.0.clone(),
            task_name: job.1.clone(),
            error: "deliberate failure".to_string(),
            retry_count,
            max_retries: MAX_RETRIES,
            wall_time: None,
            should_retry: true,
            timed_out: false,
            lease: None,
        }))
        .await;
    }

    /// End the stream from the executor's side, as a real client does once it
    /// sees the response stream finish.
    ///
    /// An HTTP/2 stream is open until *both* halves are closed, so a client
    /// that keeps its request half open holds the listener's graceful shutdown
    /// past the point where the scheduler has already let it go. The listener
    /// stops waiting after its own grace period; a test should not spend it.
    fn close(self) {}

    async fn heartbeat(&mut self, free_slots: u32) -> Result<(), tonic::Status> {
        self.client
            .heartbeat(pb::HeartbeatRequest {
                session: self.session.clone(),
                free_slots,
            })
            .await
            .map(|_| ())
    }
}

// ── The `attach_e2e.rs` scenarios, over gRPC ──────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_attached_executor_runs_a_queued_job() {
    let harness = Harness::start("grpc-attach-success").await;
    let job_id = harness.enqueue("greet");

    let mut executor = Executor::attach(&harness, "exec-1", &["greet"]).await;
    let dispatched = executor.expect_job().await;
    assert_eq!(dispatched.0, job_id);
    executor.succeed(&dispatched).await;

    assert!(
        poll_until(Duration::from_secs(15), || harness.status(&job_id)
            == Some(JobStatus::Complete))
        .await,
        "the job must complete on the attached executor"
    );

    executor.close();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failure_on_the_executor_is_retried() {
    let harness = Harness::start("grpc-attach-retry").await;
    let job_id = harness.enqueue("flaky");

    let mut executor = Executor::attach(&harness, "exec-1", &["flaky"]).await;
    let first = executor.expect_job().await;
    executor.fail(&first, 0).await;

    let second = executor.expect_job().await;
    assert_eq!(second.0, first.0, "the retry must be the same job");
    assert_eq!(second.2, 1, "the retry must carry the incremented count");
    executor.succeed(&second).await;

    assert!(
        poll_until(Duration::from_secs(20), || harness.status(&job_id)
            == Some(JobStatus::Complete))
        .await,
        "the retried job must complete"
    );

    executor.close();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_scheduler_stays_off_until_an_executor_attaches() {
    let harness = Harness::start("grpc-attach-lazy").await;
    assert!(
        !harness.supervisor.is_running(),
        "an idle listener must not claim jobs"
    );

    let mut executor = Executor::attach(&harness, "exec-1", &["greet"]).await;
    assert!(
        poll_until(Duration::from_secs(5), || harness.supervisor.is_running()).await,
        "the first attach must start the scheduler"
    );
    // Keep the stream alive until the assertion has been made.
    let _ = executor.heartbeat(2).await;

    executor.close();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_credential_without_the_execute_scope_never_reaches_a_job() {
    // The socket door's shared secret moved to the layer, so this is the gRPC
    // form of `a_bad_token_is_refused_before_any_job_reaches_it`.
    let harness = Harness::start("grpc-attach-scope").await;
    let job_id = harness.enqueue("greet");
    let produce_only = mint_token(&harness.storage, NAMESPACE, ScopeSet::of(&[Scope::Produce]));

    let refused = Executor::dial(&harness, &produce_only, "impostor", &["greet"], 2)
        .await
        .err()
        .expect("a produce-only token must not open an executor stream");
    assert_eq!(refused.code(), Code::PermissionDenied);

    // The listener stays usable, and the queued job runs on a peer that does
    // hold the scope — which also proves it never reached the impostor.
    let mut executor = Executor::attach(&harness, "exec-1", &["greet"]).await;
    let dispatched = executor.expect_job().await;
    assert_eq!(dispatched.0, job_id);
    executor.succeed(&dispatched).await;

    assert!(
        poll_until(Duration::from_secs(15), || harness.status(&job_id)
            == Some(JobStatus::Complete))
        .await,
        "the job must complete on the scoped executor"
    );

    executor.close();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_credential_is_refused_and_starts_no_scheduler() {
    let harness = Harness::start("grpc-attach-nocred").await;

    let refused = Executor::dial(&harness, "", "impostor", &["greet"], 2)
        .await
        .err()
        .expect("an uncredentialled attach must be refused");
    assert!(
        matches!(
            refused.code(),
            Code::Unauthenticated | Code::PermissionDenied
        ),
        "unexpected code {:?}",
        refused.code()
    );
    assert!(
        !harness.supervisor.is_running(),
        "a refused attach must not start the scheduler"
    );

    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_executor_reports_progress_and_logs_through_the_scheduler() {
    // The whole point of the side channel: the executor holds no database
    // credential, so these rows can only reach storage via the scheduler.
    let harness = Harness::start("grpc-attach-side-channel").await;
    let job_id = harness.enqueue("greet");

    let mut executor = Executor::attach(&harness, "exec-1", &["greet"]).await;
    let dispatched = executor.expect_job().await;

    executor
        .send(pb::attach_request::Frame::Progress(pb::ProgressFrame {
            job_id: dispatched.0.clone(),
            progress: 50,
            lease: None,
        }))
        .await;
    executor
        .send(pb::attach_request::Frame::TaskLog(pb::TaskLogFrame {
            job_id: dispatched.0.clone(),
            task_name: dispatched.1.clone(),
            level: "info".to_string(),
            message: "halfway".to_string(),
            extra: None,
            lease: None,
        }))
        .await;
    executor
        .send(pb::attach_request::Frame::TaskLog(pb::TaskLogFrame {
            job_id: dispatched.0.clone(),
            task_name: dispatched.1.clone(),
            level: "result".to_string(),
            message: String::new(),
            extra: Some(br#"{"step":3}"#.to_vec()),
            lease: None,
        }))
        .await;

    assert!(
        poll_until(Duration::from_secs(15), || {
            harness
                .storage
                .get_job(&job_id, Some(NAMESPACE))
                .expect("read the job back")
                .and_then(|job| job.progress)
                == Some(50)
        })
        .await,
        "the executor's progress must reach storage"
    );
    assert!(
        poll_until(Duration::from_secs(15), || {
            harness
                .storage
                .get_task_logs(&job_id, Some(NAMESPACE))
                .expect("read the logs")
                .len()
                == 2
        })
        .await,
        "the executor's task logs must reach storage"
    );

    let logs = harness
        .storage
        .get_task_logs(&job_id, Some(NAMESPACE))
        .expect("read the logs");
    let info = logs
        .iter()
        .find(|entry| entry.level == "info")
        .expect("the info line");
    assert_eq!(info.message, "halfway");
    assert_eq!(info.task_name, "greet");
    // A published partial is a `result`-level log, carried as the frame's blob
    // rather than in its header.
    let partial = logs
        .iter()
        .find(|entry| entry.level == "result")
        .expect("the published partial");
    assert_eq!(partial.extra.as_deref(), Some(r#"{"step":3}"#));

    executor.succeed(&dispatched).await;
    assert!(
        poll_until(Duration::from_secs(15), || harness.status(&job_id)
            == Some(JobStatus::Complete))
        .await,
        "the job must still complete after side-channel traffic"
    );

    executor.close();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dashboard_toggle_rides_the_dispatch_frame() {
    let harness = Harness::start("grpc-attach-toggles").await;
    harness
        .storage
        .set_setting("middleware:disabled:greet", r#"["tracing"]"#)
        .expect("disable a middleware the way a dashboard does");
    let job_id = harness.enqueue("greet");

    let mut executor = Executor::attach(&harness, "exec-1", &["greet"]).await;
    let Some(pb::attach_response::Frame::Job(job)) = executor.frame().await else {
        panic!("expected a job frame");
    };
    assert_eq!(
        job.disabled_middleware,
        ["tracing"],
        "an executor cannot read settings, so the toggle must arrive on the frame"
    );
    assert_eq!(job.namespace.as_deref(), Some(NAMESPACE));

    executor
        .succeed(&(job.id.clone(), job.task_name.clone(), job.retry_count))
        .await;
    assert!(
        poll_until(Duration::from_secs(15), || harness.status(&job_id)
            == Some(JobStatus::Complete))
        .await,
        "the job must complete"
    );

    executor.close();
    harness.stop().await;
}

// ── What only this transport has to answer ────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killing_the_stream_mid_job_retries_it_once_and_once_only() {
    let harness = Harness::start("grpc-attach-killed").await;
    // A short timeout, because recovery from a killed stream is the reaper's
    // and the reaper waits out the job's own budget.
    let job_id = harness.enqueue_with("greet", b"payload".to_vec(), ABANDONED_TIMEOUT_MS);

    let mut victim = Executor::attach(&harness, "exec-1", &["greet"]).await;
    let dispatched = victim.expect_job().await;
    assert_eq!(dispatched.2, 0, "the first dispatch is the first attempt");

    // Dropping both halves of the call is a kill, not a graceful end: the
    // scheduler gets no result frame at all, and recovery is the reaper's.
    drop(victim);

    let mut replacement = Executor::attach(&harness, "exec-2", &["greet"]).await;
    let retried = replacement
        .next_job(Duration::from_secs(30))
        .await
        .expect("the reaper must return the abandoned job to the queue");
    assert_eq!(retried.0, job_id);
    assert_eq!(retried.2, 1, "the kill costs exactly one attempt");
    replacement.succeed(&retried).await;

    assert!(
        poll_until(Duration::from_secs(15), || harness.status(&job_id)
            == Some(JobStatus::Complete))
        .await,
        "the retried job must complete"
    );
    // Once, and once only: a completed job is never dispatched again, and a
    // second reap of the same claim would show up here.
    assert!(
        replacement.next_job(Duration::from_secs(8)).await.is_none(),
        "the job must not be dispatched a third time"
    );

    replacement.close();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rotating_stream_does_not_drop_the_job_it_is_running() {
    // The stream ends on a timer while a job is in flight. The executor must
    // still be able to report it, and the job must complete — not be abandoned
    // to the reaper, which is what closing the connection alone would do.
    //
    // The drain budget is the rotation period, so the period has to be longer
    // than what is left of the job when the timer fires. A job that outlives a
    // whole rotation is the pathological case the budget deliberately does not
    // wait for.
    let harness = Harness::rotating("grpc-attach-rotate", Duration::from_secs(3)).await;
    let job_id = harness.enqueue("greet");

    let mut executor = Executor::attach(&harness, "exec-1", &["greet"]).await;
    let dispatched = executor.expect_job().await;

    // Past the rotation deadline, and inside its drain: the drain is what holds
    // the connection open, and without it this frame would reach a closed
    // stream and the job would be abandoned.
    tokio::time::sleep(Duration::from_secs(4)).await;
    executor.succeed(&dispatched).await;

    assert!(
        poll_until(Duration::from_secs(20), || harness.status(&job_id)
            == Some(JobStatus::Complete))
        .await,
        "a rotation must not cost the job it was running"
    );

    // The stream ended, and ended *cleanly* — a `shutdown` frame would have
    // arrived here instead, and it means stop rather than reconnect.
    assert!(
        tokio::time::timeout(Duration::from_secs(10), executor.frame())
            .await
            .expect("the stream must end rather than hang")
            .is_none(),
        "a rotated stream ends with no frame, so a real executor reconnects"
    );
    executor.close();

    // And the door takes it back, with the work carrying on.
    let mut reconnected = Executor::attach(&harness, "exec-2", &["greet"]).await;
    let next = harness.enqueue("greet");
    let dispatched = reconnected.expect_job().await;
    assert_eq!(dispatched.0, next);
    reconnected.succeed(&dispatched).await;
    assert!(
        poll_until(Duration::from_secs(20), || harness.status(&next)
            == Some(JobStatus::Complete))
        .await,
        "a reconnected executor runs work like any other"
    );

    reconnected.close();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_socket_executor_and_a_grpc_executor_share_one_scheduler() {
    use std::io::BufReader;
    use std::net::TcpStream;

    use flexiq_core::worker::protocol::{FrameReader, FrameWriter};
    use flexiq_core::ExecutorMessage;

    let harness = Harness::with_socket_door("grpc-attach-mixed").await;

    // A socket executor that knows one task…
    let port = harness.socket_port();
    let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to the socket listener");
    stream
        .set_read_timeout(Some(Duration::from_secs(20)))
        .expect("set a read timeout");
    let mut socket_writer = FrameWriter::new(stream.try_clone().expect("clone for writing"));
    let mut socket_reader = FrameReader::new(BufReader::new(stream));
    socket_writer
        .write_header(
            &ExecutorMessage::hello(
                "socket-executor",
                "test",
                "0.0.0",
                vec!["over-tcp".to_string()],
                2,
            )
            .build(),
        )
        .expect("send hello");
    assert!(matches!(
        socket_reader
            .read::<SchedulerMessage>()
            .expect("the scheduler must answer")
            .0,
        SchedulerMessage::HelloAck { .. }
    ));

    // …and a gRPC executor that knows a different one. Two registries that
    // disagree is exactly what the divergence warning exists to say out loud.
    let mut grpc = Executor::attach(&harness, "grpc-executor", &["over-grpc"]).await;
    assert!(
        logged("advertises task registry"),
        "one dispatcher, so a registry divergence must be reported across the pair"
    );
    assert!(
        logged("socket-executor") && logged("grpc-executor"),
        "the warning must name both peers, whichever transport they arrived on"
    );

    // Both receive work, from the same scheduler, in the same run.
    let over_grpc = harness.enqueue("over-grpc");
    let over_tcp = harness.enqueue("over-tcp");

    let dispatched = grpc.expect_job().await;
    assert_eq!(dispatched.0, over_grpc);
    grpc.succeed(&dispatched).await;

    let socket_job = tokio::task::spawn_blocking(move || {
        let (frame, payload) = socket_reader
            .read::<SchedulerMessage>()
            .expect("read a scheduler frame");
        let dispatch = frame.into_dispatch(payload).expect("a job frame");
        socket_writer
            .write_header(&ExecutorMessage::Success {
                job_id: dispatch.job.id.clone(),
                result_len: None,
                task_name: dispatch.job.task_name.clone(),
                wall_time_ns: 1_000,
                lease: None,
            })
            .expect("send the success frame");
        dispatch.job.id
    })
    .await
    .expect("the socket executor thread");
    assert_eq!(socket_job, over_tcp);

    for job_id in [&over_grpc, &over_tcp] {
        assert!(
            poll_until(Duration::from_secs(20), || harness.status(job_id)
                == Some(JobStatus::Complete))
            .await,
            "both transports must complete their own job"
        );
    }

    grpc.close();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_maximum_payload_survives_the_grpc_transport() {
    // The 68 MiB message cap exists for exactly this: a payload limit and a
    // message limit are different numbers, and a cap set to the payload limit
    // would reject the largest payload a socket executor already accepts.
    let harness = Harness::start("grpc-attach-max-payload").await;
    let payload = vec![0x5a_u8; flexiq_core::worker::protocol::MAX_PAYLOAD_BYTES];
    let job_id = harness.enqueue_with("greet", payload.clone(), JOB_TIMEOUT_MS);

    let mut executor = Executor::attach(&harness, "exec-1", &["greet"]).await;
    let Some(pb::attach_response::Frame::Job(job)) = executor.frame().await else {
        panic!("expected a job frame");
    };
    assert_eq!(job.id, job_id);
    assert_eq!(
        job.payload.len(),
        payload.len(),
        "the largest legal payload must arrive whole"
    );
    assert_eq!(job.payload, payload);

    executor
        .succeed(&(job.id.clone(), job.task_name.clone(), job.retry_count))
        .await;
    assert!(
        poll_until(Duration::from_secs(30), || harness.status(&job_id)
            == Some(JobStatus::Complete))
        .await,
        "the job must complete"
    );

    executor.close();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unrecognised_frame_is_skipped_rather_than_fatal() {
    // A peer built against a newer package sends an arm this build has no name
    // for, which decodes to no arm at all. The stream must survive it.
    let harness = Harness::start("grpc-attach-unknown-frame").await;
    let job_id = harness.enqueue("greet");

    let mut executor = Executor::attach(&harness, "exec-1", &["greet"]).await;
    executor
        .tx
        .send(pb::AttachRequest { frame: None })
        .await
        .expect("the stream must accept it");

    let dispatched = executor.expect_job().await;
    executor.succeed(&dispatched).await;
    assert!(
        poll_until(Duration::from_secs(15), || harness.status(&job_id)
            == Some(JobStatus::Complete))
        .await,
        "an unknown frame must not end the stream"
    );

    executor.close();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_heartbeat_reports_capacity_without_riding_the_dispatch_stream() {
    let harness = Harness::start("grpc-attach-heartbeat").await;
    let mut executor = Executor::attach(&harness, "exec-1", &["greet"]).await;

    assert!(
        poll_until(Duration::from_secs(5), || harness
            .dispatcher
            .capacity()
            .free_slots
            == 2)
        .await,
        "the handshake advertises two free slots"
    );

    executor
        .heartbeat(1)
        .await
        .expect("the heartbeat is served");
    assert!(
        poll_until(Duration::from_secs(5), || harness
            .dispatcher
            .capacity()
            .free_slots
            == 1)
        .await,
        "a unary heartbeat must reach the same accounting a stream frame would"
    );

    executor.close();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_heartbeat_for_a_session_nobody_holds_is_refused() {
    let harness = Harness::start("grpc-attach-heartbeat-unknown").await;
    let mut executor = Executor::attach(&harness, "exec-1", &["greet"]).await;

    let mut client = connect(harness.addr, &harness.token).await;
    let refused = client
        .heartbeat(pb::HeartbeatRequest {
            session: vec![0u8; 16],
            free_slots: 0,
        })
        .await
        .expect_err("a session nobody was handed names no stream");
    assert_eq!(refused.code(), Code::NotFound);

    // The real one still works, so the refusal is about the session and not
    // about the credential.
    executor
        .heartbeat(2)
        .await
        .expect("the heartbeat is served");

    executor.close();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_stream_under_one_executor_id_is_refused() {
    let harness = Harness::start("grpc-attach-duplicate").await;
    let mut first = Executor::attach(&harness, "exec-1", &["greet"]).await;

    let mut second = Executor::dial(&harness, &harness.token.clone(), "exec-1", &["greet"], 2)
        .await
        .expect("the stream itself opens; the refusal is on it");
    // The refusal arrives on the stream, after the ack the handshake always
    // sends, so a client can tell a duplicate id from a dead port.
    let status = loop {
        match second.inbound.message().await {
            Ok(Some(_)) => continue,
            Ok(None) => panic!("a duplicate id must be refused, not accepted silently"),
            Err(status) => break status,
        }
    };
    assert_eq!(status.code(), Code::AlreadyExists);

    // The first stream is untouched.
    first.heartbeat(2).await.expect("the first stream survives");

    first.close();
    second.close();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_that_never_closes_its_half_cannot_hold_the_listener_open() {
    // An HTTP/2 stream is open until *both* halves are, and a graceful listener
    // waits for every one of them. An executor that froze with its request half
    // open would hold this process past whatever termination grace period the
    // orchestrator allows, turning a clean drain into a `SIGKILL`.
    //
    // So the executor below is deliberately *not* closed, and the listener has
    // to stop anyway.
    let harness = Harness::start("grpc-attach-impolite").await;
    let executor = Executor::attach(&harness, "exec-1", &["greet"]).await;

    let started = tokio::time::Instant::now();
    harness.stop().await;
    assert!(
        started.elapsed() < Duration::from_secs(45),
        "the listener waited {:?} on a stream the client never closed",
        started.elapsed()
    );

    executor.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stream_that_ends_leaves_no_session_behind() {
    // The session map is keyed by a token minted per stream, so a reconnect
    // loop would grow it for the life of the process if a finished stream did
    // not take its own entry with it.
    let harness = Harness::start("grpc-attach-sessions").await;
    let executor = Executor::attach(&harness, "exec-1", &["greet"]).await;
    assert_eq!(harness.door.sessions().len(), 1);

    drop(executor);
    assert!(
        poll_until(Duration::from_secs(10), || harness
            .door
            .sessions()
            .is_empty())
        .await,
        "a stream that ended must not leave its session registered"
    );

    harness.stop().await;
}
