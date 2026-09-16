//! End-to-end: the server's scheduler dispatches by POSTing to a push target.
//!
//! Nothing here ever attaches. There is no attach listener in this binary and
//! no `RemoteDispatcher` is ever built, which is the whole point of GitHub
//! issue #843: the deployment has a dispatch path because an operator
//! configured one, not because a peer dialled in.
//!
//! The target is a hand-rolled axum stub rather than a real executor, for the
//! same reason `attach_e2e.rs` hand-rolls a frame speaker: what is being
//! proved is that the contract is all a target needs.
#![cfg(feature = "http-target")]

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Response, StatusCode};
use axum::routing::post;
use axum::Router;
use flexiq_core::net::Allowlist;
use flexiq_core::worker::http_target::{ACCEPTED_NOT_SETTLED, HDR_JOB_ID, HDR_OUTCOME, HDR_TASK};
use flexiq_core::{
    now_millis, HttpDispatchTarget, HttpTargetConfig, JobStatus, NewJob, Storage, StorageBackend,
    StorageSideChannel,
};
use flexiq_server::config::{Config, Env};
use flexiq_server::runtime::scheduler::{DispatchPath, SchedulerSettings, SchedulerSupervisor};

use support::{poll_until, temp_storage};

/// The pool type a push deployment registers under. A literal, not
/// `DispatchPath::pool_type()`: an assertion that reads the value out of the
/// code under test would agree with whatever that code chose.
const PUSH_POOL_TYPE: &str = "http-push";

/// `RetryPolicy`'s default `base_delay_ms`, as a literal for the same reason.
/// The first retry's full-jitter window is `base_delay_ms * 2^0`, so a retry
/// scheduled further out than this came from a second, push-specific backoff.
const FIRST_RETRY_WINDOW_MS: i64 = 1_000;

/// Retries a test job is enqueued with, so a single failure retries rather
/// than dead-letters.
const MAX_RETRIES: i32 = 3;

// ── The stub target ─────────────────────────────────────────────────

/// One dispatch the stub received.
#[derive(Clone, Debug)]
struct Received {
    /// Headers as sent, names lowercased.
    headers: Vec<(String, String)>,
    /// The request body, which is the job's payload verbatim.
    body: Vec<u8>,
}

impl Received {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

/// What the stub answers every dispatch with.
#[derive(Clone)]
struct Reply {
    status: u16,
    /// `x-flexiq-outcome`, when the reply sends one.
    outcome: Option<String>,
    body: Vec<u8>,
    /// How long the stub holds the request before answering.
    delay: Duration,
}

impl Reply {
    fn status(status: u16) -> Self {
        Self {
            status,
            outcome: None,
            body: Vec::new(),
            delay: Duration::ZERO,
        }
    }

    fn outcome(outcome: &str) -> Self {
        Self {
            outcome: Some(outcome.to_string()),
            ..Self::status(200)
        }
    }

    fn body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.body = body.into();
        self
    }

    fn after(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }
}

struct Inner {
    reply: Reply,
    received: Mutex<Vec<Received>>,
    in_flight: AtomicUsize,
    peak_in_flight: AtomicUsize,
}

/// A stub push target on an ephemeral loopback port.
///
/// It owns its own runtime: these tests are blocking (the scheduler is three
/// OS threads and `poll_until` sleeps), so the stub cannot borrow the test's.
struct PushTarget {
    inner: Arc<Inner>,
    runtime: Option<tokio::runtime::Runtime>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    /// URL to point a `PushTargetConfig` at.
    url: String,
}

impl PushTarget {
    /// Bind a port and answer every dispatch with `reply`.
    fn start(reply: Reply) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("the stub's runtime builds");
        let inner = Arc::new(Inner {
            reply,
            received: Mutex::new(Vec::new()),
            in_flight: AtomicUsize::new(0),
            peak_in_flight: AtomicUsize::new(0),
        });

        let (shutdown, stopped) = tokio::sync::oneshot::channel();
        let (bound, url) = runtime.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("the stub binds an ephemeral loopback port");
            let url = format!(
                "http://{}/dispatch",
                listener.local_addr().expect("the bound address")
            );
            (listener, url)
        });

        let app = Router::new()
            .route("/dispatch", post(receive))
            .with_state(inner.clone());
        runtime.spawn(async move {
            let _ = axum::serve(bound, app)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await;
        });

        Self {
            inner,
            runtime: Some(runtime),
            shutdown: Some(shutdown),
            url,
        }
    }

    /// Every dispatch received so far, oldest first.
    fn received(&self) -> Vec<Received> {
        self.inner.received.lock().expect("stub lock").clone()
    }

    /// The most dispatches this target was handling at the same moment.
    fn peak_in_flight(&self) -> usize {
        self.inner.peak_in_flight.load(Ordering::SeqCst)
    }
}

impl Drop for PushTarget {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(runtime) = self.runtime.take() {
            // Bounded: a test that dropped the stub while a dispatch was still
            // parked in it should fail on its own assertion, not hang here.
            runtime.shutdown_timeout(Duration::from_secs(5));
        }
    }
}

async fn receive(
    State(inner): State<Arc<Inner>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response<axum::body::Body> {
    // Recorded before the reply is delayed, so a test can observe a dispatch
    // the target has not answered yet.
    inner.received.lock().expect("stub lock").push(Received {
        headers: headers
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_ascii_lowercase(),
                    value.to_str().unwrap_or_default().to_string(),
                )
            })
            .collect(),
        body: body.to_vec(),
    });

    let now = inner.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
    inner.peak_in_flight.fetch_max(now, Ordering::SeqCst);
    if !inner.reply.delay.is_zero() {
        tokio::time::sleep(inner.reply.delay).await;
    }
    inner.in_flight.fetch_sub(1, Ordering::SeqCst);

    let mut response = Response::builder()
        .status(StatusCode::from_u16(inner.reply.status).expect("the stub's status is a real one"));
    if let Some(outcome) = &inner.reply.outcome {
        response = response.header(HDR_OUTCOME, outcome);
    }
    response
        .body(axum::body::Body::from(inner.reply.body.clone()))
        .expect("the stub's response builds")
}

// ── The deployment under test ───────────────────────────────────────

/// A target aimed at `url`, configured the way `runtime::push_target` would,
/// save for one knob.
///
/// `allow_loopback` is that knob: the stub necessarily binds loopback, which
/// `flexiq-server`'s own path refuses unconditionally — the variable that asks
/// for it is a *rejected* variable, not a supported one — so it is set here,
/// on the library knob `flexiq-core` documents for exactly this case.
fn target(url: &str, capacity: u32, storage: &StorageBackend) -> HttpTargetConfig {
    HttpTargetConfig {
        allow_loopback: true,
        side_channel: Some(Arc::new(StorageSideChannel::new(storage.clone()))),
        ..HttpTargetConfig::new(
            url,
            capacity,
            Allowlist::parse("127.0.0.0/8").expect("the test allowlist parses"),
        )
    }
}

/// A push deployment: the supervisor `runtime::run` builds, over a
/// `DispatchPath::Push`, with no attach listener anywhere.
struct Deployment {
    supervisor: Arc<SchedulerSupervisor>,
}

impl Deployment {
    /// Build the supervisor without starting it.
    fn build(storage: &StorageBackend, config: HttpTargetConfig, workers: Option<usize>) -> Self {
        let target = HttpDispatchTarget::new(config).expect("the stub target must build");
        Self {
            supervisor: Arc::new(SchedulerSupervisor::new(
                storage.clone(),
                DispatchPath::Push(Arc::new(target)),
                SchedulerSettings {
                    queues: vec!["default".to_string()],
                    namespace: None,
                    workers,
                    maintenance: false,
                },
            )),
        }
    }

    /// What `runtime::run` does at boot on a path that starts eagerly.
    fn boot(&self) {
        self.supervisor
            .ensure_started()
            .expect("a push deployment's scheduler must start at boot");
    }

    /// Build and boot in one step, for the tests that are not about the boot.
    fn start(storage: &StorageBackend, config: HttpTargetConfig, workers: Option<usize>) -> Self {
        let deployment = Self::build(storage, config, workers);
        deployment.boot();
        deployment
    }

    fn stop(self) {
        self.supervisor.shutdown();
    }
}

fn new_job(task_name: &str) -> NewJob {
    NewJob {
        queue: "default".to_string(),
        task_name: task_name.to_string(),
        payload: b"payload".to_vec(),
        priority: 0,
        scheduled_at: now_millis(),
        max_retries: MAX_RETRIES,
        timeout_ms: 30_000,
        unique_key: None,
        metadata: None,
        notes: None,
        depends_on: vec![],
        expires_at: None,
        result_ttl_ms: None,
        namespace: None,
        debounce_key: None,
    }
}

// ── The tests ───────────────────────────────────────────────────────

#[test]
fn a_claimed_job_reaches_the_target_and_its_result_is_stored() {
    let storage = temp_storage("push-success");
    let job = storage
        .enqueue(new_job("greet"))
        .expect("enqueue the job under test");

    let stub = PushTarget::start(Reply::outcome("success").body(b"the-result".to_vec()));
    let deployment = Deployment::start(&storage, target(&stub.url, 2, &storage), None);

    poll_until(Duration::from_secs(15), || {
        matches!(
            storage.get_job(&job.id, None).expect("read the job back"),
            Some(ref current) if current.status == JobStatus::Complete
        )
    })
    .expect("the job must complete on the push target");

    let completed = storage
        .get_job(&job.id, None)
        .expect("read the job back")
        .expect("the job is still there");
    assert_eq!(
        completed.result.as_deref(),
        Some(b"the-result".as_slice()),
        "the target's response body is the job's result"
    );

    let received = stub.received();
    assert_eq!(received.len(), 1, "exactly one dispatch");
    assert_eq!(received[0].header(HDR_JOB_ID), Some(job.id.as_str()));
    assert_eq!(received[0].header(HDR_TASK), Some("greet"));
    assert_eq!(
        received[0].body,
        b"payload".to_vec(),
        "the body is the job's payload verbatim"
    );

    deployment.stop();
}

#[test]
fn a_failing_target_retries_on_the_existing_backoff() {
    let storage = temp_storage("push-retry");
    let job = storage
        .enqueue(new_job("flaky"))
        .expect("enqueue the job under test");

    // The delay is what makes the assertion deterministic rather than a race:
    // the stub records the dispatch on arrival, so the test can stop the
    // deployment while the first attempt is still parked. Shutdown then drains
    // that one attempt and nothing else is ever dispatched, which fixes
    // `retry_count` at exactly 1.
    let stub = PushTarget::start(Reply::status(500).after(Duration::from_millis(500)));
    let before = now_millis();
    let deployment = Deployment::start(&storage, target(&stub.url, 2, &storage), None);

    poll_until(Duration::from_secs(15), || !stub.received().is_empty())
        .expect("the target must be dispatched to");
    deployment.stop();
    let after = now_millis();

    let retried = storage
        .get_job(&job.id, None)
        .expect("read the job back")
        .expect("a retried job is not deleted");
    assert_eq!(
        retried.status,
        JobStatus::Pending,
        "a 5xx is retryable, so the job goes back to pending rather than dead-lettering"
    );
    assert_eq!(retried.retry_count, 1, "the retry count must be bumped");
    assert!(
        storage
            .list_dead(10, 0, None)
            .expect("read the dead-letter queue")
            .is_empty(),
        "a retryable failure must not dead-letter"
    );

    // The job was rescheduled forward from the failure — `before` is read
    // ahead of the first dispatch, so anything at or after it is a fresh
    // `next_retry_at` rather than the `scheduled_at` it was enqueued with.
    assert!(
        retried.scheduled_at >= before,
        "the retry must be scheduled from the failure, got {} vs {before}",
        retried.scheduled_at
    );
    // And inside the window the existing policy would pick. Full jitter's
    // lower bound is zero (a retry can legitimately be due immediately), so
    // the ceiling is what discriminates: a second, push-specific backoff —
    // a flat 30s, say — lands outside it.
    assert!(
        retried.scheduled_at <= after + FIRST_RETRY_WINDOW_MS,
        "the retry must fall inside the existing full-jitter window of \
         {FIRST_RETRY_WINDOW_MS} ms, got {} ms past the failure",
        retried.scheduled_at - after
    );
}

#[test]
fn a_target_that_returns_202_dead_letters_with_a_reason_an_operator_can_read() {
    let storage = temp_storage("push-202");
    let job = storage
        .enqueue(new_job("accepted"))
        .expect("enqueue the job under test");

    let stub = PushTarget::start(Reply::status(202));
    let deployment = Deployment::start(&storage, target(&stub.url, 2, &storage), None);

    poll_until(Duration::from_secs(15), || {
        !storage
            .list_dead(10, 0, None)
            .expect("read the dead-letter queue")
            .is_empty()
    })
    .expect("a 202 must dead-letter rather than retry");

    let dead = storage
        .list_dead(10, 0, None)
        .expect("read the dead-letter queue");
    assert_eq!(dead.len(), 1, "exactly one dead-letter entry");
    assert_eq!(dead[0].original_job_id, job.id);
    let error = dead[0]
        .error
        .clone()
        .expect("a dead-letter carries a reason");
    assert!(
        error.contains(ACCEPTED_NOT_SETTLED),
        "the reason must carry the greppable prefix, got: {error}"
    );
    assert!(
        error.contains("#845"),
        "the reason must name the issue that tracks settling a 202, got: {error}"
    );

    assert_eq!(
        stub.received().len(),
        1,
        "a 202 is not retryable, so it must be dispatched once and not again"
    );

    deployment.stop();
}

#[test]
fn the_scheduler_starts_without_anyone_attaching() {
    let storage = temp_storage("push-eager");
    let job = storage
        .enqueue(new_job("greet"))
        .expect("enqueue the job under test");

    let stub = PushTarget::start(Reply::outcome("success"));
    let config = target(&stub.url, 2, &storage);

    // The contrast, asserted rather than described: the attach path waits for
    // a peer, the push path has none to wait for. A build that made both lazy
    // — or both eager — fails here.
    let push = DispatchPath::Push(Arc::new(
        HttpDispatchTarget::new(config.clone()).expect("the stub target must build"),
    ));
    assert!(
        push.starts_eagerly(),
        "a push deployment has no attach that would ever start its scheduler"
    );
    assert!(
        !DispatchPath::Attach(flexiq_core::RemoteDispatcher::new(
            flexiq_core::RemoteConfig::default()
        ))
        .starts_eagerly(),
        "the attach path must stay lazy: starting before an executor advertises \
         anything is a retry storm against an idle deployment"
    );

    let deployment = Deployment::build(&storage, config, None);
    assert!(
        !deployment.supervisor.is_running(),
        "the supervisor must not start itself; `runtime::run` is what boots it"
    );
    deployment.boot();

    poll_until(Duration::from_secs(15), || {
        matches!(
            storage.get_job(&job.id, None).expect("read the job back"),
            Some(ref current) if current.status == JobStatus::Complete
        )
    })
    .expect("the queue must drain with nothing ever attaching");

    // What `queue.workers()` reads. Nothing in this binary ever attached, so
    // a `remote` row here would mean the scheduler is reporting a pool that
    // does not exist.
    let workers = storage.list_workers().expect("read the worker registry");
    assert_eq!(workers.len(), 1, "one scheduler, one registration");
    assert_eq!(
        workers[0].pool_type.as_deref(),
        Some(PUSH_POOL_TYPE),
        "the registry must name the pool that is actually running the jobs"
    );

    deployment.stop();
}

#[test]
fn the_target_is_called_at_most_capacity_times_at_once() {
    /// Small enough that the scheduler reaches it well inside the test, and
    /// below `WORKERS` so the cap under test is the target's own.
    const CAPACITY: u32 = 2;
    /// Deliberately above `CAPACITY`: `max_in_flight` derives from this, so
    /// with it larger the only thing bounding concurrent dispatches is the
    /// target's own slot semaphore.
    const WORKERS: usize = 8;
    const JOBS: usize = 6;

    let storage = temp_storage("push-capacity");
    for _ in 0..JOBS {
        storage
            .enqueue(new_job("greet"))
            .expect("enqueue a job under test");
    }

    // Long enough that the scheduler, which claims one job per 50 ms poll,
    // has every slot filled before the first answer comes back.
    let stub = PushTarget::start(Reply::outcome("success").after(Duration::from_millis(250)));
    let deployment = Deployment::start(
        &storage,
        target(&stub.url, CAPACITY, &storage),
        Some(WORKERS),
    );

    poll_until(Duration::from_secs(30), || {
        storage.stats(None).expect("read the queue stats").completed == JOBS as i64
    })
    .expect("every job must complete on the push target");

    assert_eq!(
        stub.peak_in_flight(),
        CAPACITY as usize,
        "the target must be worked to its configured capacity and never past it"
    );

    deployment.stop();
}

#[test]
fn a_push_deployment_refuses_to_start_with_a_bad_allowlist() {
    let storage = temp_storage("push-bad-allowlist");
    let job = storage
        .enqueue(new_job("greet"))
        .expect("enqueue the job under test");

    // Syntactically fine, so config validation has nothing to say: the
    // allowlist parses and the URL parses. What it does not do is permit the
    // host the URL names.
    let env: Env = [
        ("FLEXIQ_DSN", ":memory:"),
        (
            "FLEXIQ_PUSH_TARGET_URL",
            "https://push.example.com/dispatch",
        ),
        ("FLEXIQ_PUSH_TARGET_CAPACITY", "4"),
        ("FLEXIQ_PUSH_TARGET_ALLOW", "10.0.0.0/8"),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_string(), value.to_string()))
    .collect();
    let config = Config::from_map(&env).expect("the configuration itself is valid");
    let push = config.push.as_ref().expect("a push section");

    // `HttpDispatchTarget` carries no `Debug` — it owns a client and a lease
    // book — so the `Ok` arm is unwrapped by hand rather than through
    // `expect_err`.
    let error = match flexiq_server::runtime::push_target(push, &storage) {
        Err(error) => error,
        Ok(_) => panic!("a host the allowlist does not permit must refuse to build"),
    };
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains("push.example.com") && rendered.contains("allowlist"),
        "the failure must name the host it refused, got: {rendered}"
    );

    // Startup, not first job: the refusal happened where `runtime::run` builds
    // the target, before any scheduler existed, so the queued job is untouched
    // rather than dead-lettered by a target that failed on its first dispatch.
    let untouched = storage
        .get_job(&job.id, None)
        .expect("read the job back")
        .expect("the job is still there");
    assert_eq!(untouched.status, JobStatus::Pending);
    assert_eq!(untouched.retry_count, 0);
    assert!(
        storage
            .list_dead(10, 0, None)
            .expect("read the dead-letter queue")
            .is_empty(),
        "nothing may dead-letter over a target that never started"
    );
}

/// The executor door is a `grpc` type, so there is nothing to observe about it
/// in a build without that feature — see this commit's report.
#[cfg(feature = "grpc")]
#[test]
fn the_executor_door_is_absent_under_push() {
    fn env(pairs: &[(&str, &str)]) -> Env {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    let base = [
        ("FLEXIQ_DSN", ":memory:"),
        ("FLEXIQ_GRPC_LISTEN", "127.0.0.1:0"),
        ("FLEXIQ_NAMESPACE", "push-tests"),
    ];
    let mut push = base.to_vec();
    push.extend([
        (
            "FLEXIQ_PUSH_TARGET_URL",
            "https://push.example.com/dispatch",
        ),
        ("FLEXIQ_PUSH_TARGET_CAPACITY", "4"),
        ("FLEXIQ_PUSH_TARGET_ALLOW", "push.example.com"),
    ]);

    // `executors_can_attach` is the one predicate that gates both the
    // `RemoteDispatcher` and the `ExecutorDoor` built from it, so a `false`
    // here is the door not being built. The gRPC-only deployment below is the
    // discriminator: the producer door is untouched either way, and it is the
    // push target alone that takes the executor door away.
    assert!(
        !flexiq_server::runtime::executors_can_attach(
            &Config::from_map(&env(&push)).expect("a push + gRPC deployment is valid")
        ),
        "a push deployment dials out; there is nothing for an executor to attach to"
    );
    assert!(
        flexiq_server::runtime::executors_can_attach(
            &Config::from_map(&env(&base)).expect("a gRPC-only deployment is valid")
        ),
        "without a push target the gRPC door still carries executors"
    );
}
