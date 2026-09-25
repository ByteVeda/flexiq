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
use flexiq_core::worker::http_target::{
    ACCEPTED_NOT_SETTLED, HDR_JOB_ID, HDR_LEASE, HDR_OUTCOME, HDR_TASK,
};
use flexiq_core::worker::WorkerDispatcher;
use flexiq_core::{
    now_millis, HttpDispatchTarget, HttpTargetConfig, JobStatus, Lease, NewJob, SettleRefused,
    SettledOutcome, Storage, StorageBackend, StorageSideChannel, MAX_LEASE_EXTENSION,
};
use flexiq_server::config::push::SETTLE_VAR;
use flexiq_server::config::{Config, Env};
use flexiq_server::dashboard::stores::overrides::{self, Scope};
use flexiq_server::runtime::scheduler::{
    DispatchPath, Lane, SchedulerSettings, SchedulerSupervisor,
};

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
    /// The same target the scheduler dispatches through.
    ///
    /// Kept because a settle is delivered *to the dispatcher*, not to the
    /// scheduler: the waiting attempt that a settle relieves holds the permit
    /// and the result channel, which is why the gRPC door needs this handle
    /// too.
    target: Arc<HttpDispatchTarget>,
}

impl Deployment {
    /// Build the supervisor without starting it.
    fn build(storage: &StorageBackend, config: HttpTargetConfig, workers: Option<usize>) -> Self {
        let target = Arc::new(HttpDispatchTarget::new(config).expect("the stub target must build"));
        Self {
            target: Arc::clone(&target),
            supervisor: Arc::new(SchedulerSupervisor::new(
                storage.clone(),
                DispatchPath::Push(target),
                SchedulerSettings {
                    queues: vec!["default".to_string()],
                    namespace: None,
                    workers,
                    maintenance: false,
                    push_dispatch: None,
                    events: None,
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
    // Names the variable rather than the issue that used to track it: #845 is
    // implemented, so the actionable thing is the switch that turns it on.
    assert!(
        error.contains(SETTLE_VAR),
        "the reason must name the variable that turns callbacks on, got: {error}"
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
    let workers = storage
        .list_workers(None)
        .expect("read the worker registry");
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
    let push = &config.push.as_ref().expect("a push section").targets[0].config;

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
fn nothing_attaches_under_push() {
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

    // `executors_can_attach` gates the `RemoteDispatcher`, so a `false` here
    // is nothing being able to attach. It is no longer the same thing as the
    // executor door being absent: since #845 a push deployment may still serve
    // that door, settle-only, so a target can report on work that outlived its
    // request. `Attach` and `Heartbeat` refuse there; the four reporting RPCs
    // do not. The gRPC-only deployment below is the discriminator, and the
    // producer door is untouched either way.
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

// ── Settle callbacks (#845) ─────────────────────────────────────────
//
// A 202 hands the job off: the request ends, the target keeps working, and it
// reports later. Everything below is about the fence, because a late settle
// arriving after the attempt was retried elsewhere is the *normal* failure
// mode of this design rather than an edge case.

/// A target that accepts a 202 and waits for the callback.
fn settle_target(url: &str, capacity: u32, storage: &StorageBackend) -> HttpTargetConfig {
    HttpTargetConfig {
        settle_callbacks: true,
        ..target(url, capacity, storage)
    }
}

/// Wait for a job to be accepted, and answer with the lease it was dispatched
/// under — which is what a real target reads off `x-flexiq-lease`.
fn accepted_lease(stub: &PushTarget, deployment: &Deployment) -> Lease {
    poll_until(Duration::from_secs(15), || {
        deployment.target.awaiting_settle() == 1
    })
    .expect("the target must be waiting for a settle");

    let received = stub.received();
    let raw = received
        .first()
        .and_then(|dispatch| dispatch.header(HDR_LEASE))
        .expect("a dispatch carries a lease")
        .to_string();
    Lease::from_wire(raw.as_bytes()).expect("the header is a lease this scheduler minted")
}

#[test]
fn a_202_is_settled_by_a_later_callback() {
    let storage = temp_storage("push-settle-later");
    let job = storage
        .enqueue(new_job("settled_late"))
        .expect("enqueue the job");

    let stub = PushTarget::start(Reply::status(202));
    let deployment = Deployment::start(&storage, settle_target(&stub.url, 2, &storage), None);
    let lease = accepted_lease(&stub, &deployment);

    // The request is long over; nothing is holding the connection.
    assert_eq!(
        storage
            .get_job(&job.id, None)
            .expect("read the job")
            .expect("the job exists")
            .status,
        JobStatus::Running,
        "an accepted dispatch stays Running until it is settled"
    );

    deployment
        .target
        .settle(
            &job.id,
            &lease,
            SettledOutcome::Success {
                result: Some(b"\x02\xf6".to_vec()),
                wall_time_ns: 1_000,
            },
        )
        .expect("a settle under the dispatch's own lease is applied");

    poll_until(Duration::from_secs(15), || {
        storage
            .get_job(&job.id, None)
            .ok()
            .flatten()
            .is_some_and(|job| job.status == JobStatus::Complete)
    })
    .expect("the settled job must complete");

    assert_eq!(
        stub.received().len(),
        1,
        "the job is dispatched once; the answer came out of band"
    );
    deployment.stop();
}

#[test]
fn a_settle_is_single_use_for_the_attempt_it_names() {
    let storage = temp_storage("push-settle-once");
    let job = storage
        .enqueue(new_job("settled_twice"))
        .expect("enqueue the job");

    let stub = PushTarget::start(Reply::status(202));
    let deployment = Deployment::start(&storage, settle_target(&stub.url, 2, &storage), None);
    let lease = accepted_lease(&stub, &deployment);

    let outcome = || SettledOutcome::Cancelled { wall_time_ns: 1 };
    deployment
        .target
        .settle(&job.id, &lease, outcome())
        .expect("the first settle wins the marker");

    // The second is refused, and it must be refused by the *durable consume* —
    // not merely by the registry entry having been removed. Mutation check:
    // this assertion has to survive the registry, so it is the marker that is
    // gone. Both refusals are `Fenced`, never a success.
    let second = deployment.target.settle(&job.id, &lease, outcome());
    assert!(
        matches!(
            second,
            Err(SettleRefused::Fenced) | Err(SettleRefused::NotHere)
        ),
        "a second settle under one lease must be refused, got {second:?}"
    );

    deployment.stop();
}

#[test]
fn a_settle_under_a_superseded_lease_is_refused() {
    let storage = temp_storage("push-settle-stale");
    let job = storage
        .enqueue(new_job("settled_stale"))
        .expect("enqueue the job");

    let stub = PushTarget::start(Reply::status(202));
    let deployment = Deployment::start(&storage, settle_target(&stub.url, 2, &storage), None);
    let live = accepted_lease(&stub, &deployment);

    // A lease this scheduler could have minted, for a dispatch it never made.
    // Deliberately *well-formed*: a value that failed to decode would be
    // refused by `Lease::from_wire` and would prove nothing about the fence.
    let stale_epoch = live.epoch().expect("a minted lease carries an epoch") ^ 1;
    let stale = Lease::from_epoch(stale_epoch);

    let refused = deployment.target.settle(
        &job.id,
        &stale,
        SettledOutcome::Success {
            result: None,
            wall_time_ns: 1,
        },
    );
    assert!(
        matches!(refused, Err(SettleRefused::Fenced)),
        "a settle naming another dispatch must be fenced out, got {refused:?}"
    );

    // And it changed nothing: the job is still running, still awaiting its own
    // settle. A refusal that had settled the job would be the double
    // settlement the fence exists to prevent.
    assert_eq!(
        storage
            .get_job(&job.id, None)
            .expect("read the job")
            .expect("the job exists")
            .status,
        JobStatus::Running,
        "a refused settle must not move the job"
    );
    assert_eq!(
        deployment.target.awaiting_settle(),
        1,
        "the real dispatch is still waiting"
    );

    // The live lease still works, which proves the refusal above consumed
    // nothing rather than merely answering late.
    deployment
        .target
        .settle(
            &job.id,
            &live,
            SettledOutcome::Success {
                result: None,
                wall_time_ns: 1,
            },
        )
        .expect("the current dispatch can still settle");

    deployment.stop();
}

#[test]
fn a_settle_for_another_replicas_dispatch_says_so() {
    // The one refusal that is not about staleness: the caller's lease may be
    // perfectly current and simply have reached the wrong scheduler. An
    // operator reading "already settled" would go looking for a race that
    // never happened.
    let storage = temp_storage("push-settle-elsewhere");
    let stub = PushTarget::start(Reply::status(202));
    let deployment = Deployment::start(&storage, settle_target(&stub.url, 2, &storage), None);

    let refused = deployment.target.settle(
        "a-job-this-replica-never-dispatched",
        &Lease::from_epoch(7),
        SettledOutcome::Cancelled { wall_time_ns: 1 },
    );
    assert!(
        matches!(refused, Err(SettleRefused::NotHere)),
        "a settle for an unknown dispatch is a routing problem, got {refused:?}"
    );

    deployment.stop();
}

#[test]
fn an_extension_moves_the_deadline_and_is_clamped() {
    let storage = temp_storage("push-settle-extend");
    let job = storage
        .enqueue(new_job("extended"))
        .expect("enqueue the job");

    let stub = PushTarget::start(Reply::status(202));
    let deployment = Deployment::start(&storage, settle_target(&stub.url, 2, &storage), None);
    let lease = accepted_lease(&stub, &deployment);

    let before = now_millis();
    let granted = deployment
        .target
        .extend_lease(&job.id, &lease, Duration::from_secs(600))
        .expect("the current dispatch may ask for longer");
    assert!(
        granted >= before + 600_000,
        "an extension must actually move the deadline out"
    );

    // Clamped, not refused: asking for a week gets an hour and is told so.
    let clamped = deployment
        .target
        .extend_lease(&job.id, &lease, Duration::from_secs(7 * 24 * 3_600))
        .expect("an over-long request is clamped rather than refused");
    assert!(
        clamped <= now_millis() + MAX_LEASE_EXTENSION.as_millis() as i64,
        "an extension past the ceiling must be clamped to it"
    );

    // A stale lease buys nothing: a superseded attempt must not be able to
    // keep a claim alive under the attempt that replaced it.
    let stale = Lease::from_epoch(lease.epoch().expect("an epoch") ^ 1);
    assert!(
        deployment
            .target
            .extend_lease(&job.id, &stale, Duration::from_secs(60))
            .is_err(),
        "a superseded attempt cannot buy itself more time"
    );

    // Settled before the deployment stops, so the test leaves no attempt
    // parked on an hour-long deadline. That case is real, and
    // `a_shutdown_releases_an_accepted_dispatch` is where it is proved —
    // on a drain budget short enough to assert against.
    deployment
        .target
        .settle(
            &job.id,
            &lease,
            SettledOutcome::Success {
                result: None,
                wall_time_ns: 1,
            },
        )
        .expect("the extended dispatch settles normally");

    deployment.stop();
}

#[test]
fn cancelling_an_accepted_dispatch_settles_it_and_fences_the_callback() {
    // The scheduler-side cancel, driven by `notify_cancel` directly. It must
    // answer the same way
    // in both windows: a cancel before the target accepted settles
    // `Cancelled`, so a cancel after it accepted has to as well, or push would
    // promise one thing and do another depending on timing the caller cannot
    // see.
    let storage = temp_storage("push-settle-cancel");
    let job = storage
        .enqueue(new_job("cancelled_after_accept"))
        .expect("enqueue the job");

    let stub = PushTarget::start(Reply::status(202));
    let deployment = Deployment::start(&storage, settle_target(&stub.url, 2, &storage), None);
    let lease = accepted_lease(&stub, &deployment);

    deployment.target.notify_cancel(&job.id);

    poll_until(Duration::from_secs(15), || {
        storage
            .get_job(&job.id, None)
            .ok()
            .flatten()
            .is_some_and(|job| job.status == JobStatus::Cancelled)
    })
    .expect("a cancelled accepted dispatch must settle Cancelled");

    // Not retried into a second attempt: a cancel is a decision, and the
    // shutdown drain's retryable abandonment is a different claimant sharing
    // the same authority to take the marker.
    assert_eq!(
        stub.received().len(),
        1,
        "a cancel must not put the job back for another dispatch"
    );

    // And the marker went with it, so the target's eventual callback is
    // fenced out rather than landing on a job that is already settled.
    let refused = deployment.target.settle(
        &job.id,
        &lease,
        SettledOutcome::Success {
            result: None,
            wall_time_ns: 1,
        },
    );
    // Refused as *cancelled*, not as misrouted: the target asked the replica
    // that dispatched it, under the lease it was given (#846).
    assert!(
        matches!(refused, Err(SettleRefused::Cancelled)),
        "a settle after a cancel must be refused as cancelled, got {refused:?}"
    );

    deployment.stop();
}

#[test]
fn a_cancel_written_to_storage_reaches_an_accepted_dispatch_and_its_poll() {
    // #846: nothing calls `notify_cancel` here. The cancel is only the storage
    // flag — what `CancelJob`, the dashboard, or an SDK on another host
    // writes — and the worker's relay has to carry it to the target.
    let storage = temp_storage("push-cancel-relay");
    let job = storage
        .enqueue(new_job("cancelled_from_storage"))
        .expect("enqueue the job");

    let stub = PushTarget::start(Reply::status(202));
    let deployment = Deployment::start(&storage, settle_target(&stub.url, 2, &storage), None);
    let lease = accepted_lease(&stub, &deployment);

    assert!(storage
        .request_cancel(&job.id, None)
        .expect("request the cancel"));

    poll_until(Duration::from_secs(15), || {
        storage
            .get_job(&job.id, None)
            .ok()
            .flatten()
            .is_some_and(|job| job.status == JobStatus::Cancelled)
    })
    .expect("a cancel written to storage must settle the accepted dispatch Cancelled");

    // The target's poll: every reporting call it could make says "cancelled".
    let extended = deployment
        .target
        .extend_lease(&job.id, &lease, Duration::from_secs(60));
    assert!(
        matches!(extended, Err(SettleRefused::Cancelled)),
        "an extension after a cancel must say cancelled, got {extended:?}"
    );
    let progress = deployment.target.report_progress(&job.id, &lease, 50);
    assert!(
        matches!(progress, Err(SettleRefused::Cancelled)),
        "a progress report after a cancel must say cancelled, got {progress:?}"
    );
    // And only to the lease the dispatch was made under.
    let stranger =
        deployment
            .target
            .extend_lease(&job.id, &Lease::from_epoch(1), Duration::from_secs(60));
    assert!(
        matches!(stranger, Err(SettleRefused::NotHere)),
        "another lease must learn nothing, got {stranger:?}"
    );

    deployment.stop();
}

#[test]
fn a_cancel_written_to_storage_abandons_an_in_request_dispatch() {
    // The synchronous half: the target is still holding the request open, so
    // the relay's cancel abandons it and the attempt settles `Cancelled`.
    let storage = temp_storage("push-cancel-relay-inflight");
    let job = storage
        .enqueue(new_job("cancelled_mid_request"))
        .expect("enqueue the job");

    let stub = PushTarget::start(Reply::outcome("success").after(Duration::from_secs(20)));
    let deployment = Deployment::start(&storage, target(&stub.url, 2, &storage), None);

    poll_until(Duration::from_secs(15), || !stub.received().is_empty())
        .expect("the job must be dispatched");
    assert!(storage
        .request_cancel(&job.id, None)
        .expect("request the cancel"));

    poll_until(Duration::from_secs(10), || {
        storage
            .get_job(&job.id, None)
            .ok()
            .flatten()
            .is_some_and(|job| job.status == JobStatus::Cancelled)
    })
    .expect("the relay must cancel a request still in flight, well before the target answers");

    deployment.stop();
}

#[test]
fn a_shutdown_releases_an_accepted_dispatch() {
    // An accepted dispatch can be parked on a deadline hours away, and a
    // shutdown must not wait for it. The abandon signal is what reaches it —
    // and if it ever stops reaching it, this deployment stops shutting down
    // at all, which is why the budget is asserted rather than assumed.
    let storage = temp_storage("push-settle-drain");
    let job = storage.enqueue(new_job("parked")).expect("enqueue the job");

    let stub = PushTarget::start(Reply::status(202));
    let config = HttpTargetConfig {
        // Short, so the two drains this path spends are a second, not a
        // minute. The budget being spent twice is push's own shape.
        shutdown_drain: Duration::from_millis(500),
        ..settle_target(&stub.url, 2, &storage)
    };
    let deployment = Deployment::start(&storage, config, None);
    let lease = accepted_lease(&stub, &deployment);

    deployment
        .target
        .extend_lease(&job.id, &lease, Duration::from_secs(3_600))
        .expect("park it well past the test's own patience");

    let started = std::time::Instant::now();
    deployment.stop();
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "a shutdown must abandon a parked accepted dispatch, not wait out its deadline"
    );
}

#[test]
fn an_accepted_dispatch_that_is_never_settled_says_so() {
    let storage = temp_storage("push-settle-never");
    // A short timeout, so the settle deadline the accept records is one the
    // test can outlive.
    let mut job = new_job("never_settled");
    job.timeout_ms = 1_500;
    job.max_retries = 0;
    let job = storage.enqueue(job).expect("enqueue the job");

    let stub = PushTarget::start(Reply::status(202));
    let deployment = Deployment::start(&storage, settle_target(&stub.url, 2, &storage), None);

    poll_until(Duration::from_secs(20), || {
        !storage
            .list_dead(10, 0, None)
            .expect("read the dead-letter queue")
            .is_empty()
    })
    .expect("an accepted dispatch that is never settled must eventually fail");

    let dead = storage
        .list_dead(10, 0, None)
        .expect("read the dead-letter queue");
    assert_eq!(dead[0].original_job_id, job.id);
    let error = dead[0]
        .error
        .clone()
        .expect("a dead-letter carries a reason");
    assert!(
        error.contains(ACCEPTED_NOT_SETTLED),
        "the reason keeps the greppable prefix, got: {error}"
    );
    // The distinction #845 asks for: an operator must be able to tell
    // "accepted, never settled" from an ordinary timeout.
    assert!(
        error.contains("accepted") && error.contains("never settled"),
        "the reason must say the target took the job and went quiet, got: {error}"
    );
    assert!(
        !error.contains(SETTLE_VAR),
        "this is not the callbacks-are-off refusal, got: {error}"
    );

    deployment.stop();
}

#[test]
fn settle_callbacks_need_the_grpc_door() {
    // A 202 is a hand-off to somewhere, and the executor door is that
    // somewhere. Refused at boot rather than starting a deployment that
    // accepts a hand-off and then has no inbound surface for the answer.
    fn env(pairs: &[(&str, &str)]) -> Env {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    let mut push = vec![
        ("FLEXIQ_DSN", ":memory:"),
        (
            "FLEXIQ_PUSH_TARGET_URL",
            "https://push.example.com/dispatch",
        ),
        ("FLEXIQ_PUSH_TARGET_CAPACITY", "4"),
        ("FLEXIQ_PUSH_TARGET_ALLOW", "push.example.com"),
    ];
    assert!(
        Config::from_map(&env(&push)).is_ok(),
        "push without callbacks needs no door"
    );

    push.push((SETTLE_VAR, "grpc"));
    let refused = Config::from_map(&env(&push)).expect_err("callbacks without a door are refused");
    assert!(
        refused.to_string().contains("FLEXIQ_GRPC_LISTEN"),
        "the refusal must name the listener to set, got: {refused}"
    );

    push.extend([
        ("FLEXIQ_GRPC_LISTEN", "127.0.0.1:0"),
        ("FLEXIQ_NAMESPACE", "push-tests"),
    ]);
    // Only a build that *has* the door can pair with it. Without the feature
    // `FLEXIQ_GRPC_LISTEN` is refused outright by `config::grpc::from_env`
    // ("built without the `grpc` cargo feature"), which is the right answer
    // and a different one — so the assertion is per-build rather than one that
    // quietly means two things.
    let paired = Config::from_map(&env(&push));
    if cfg!(feature = "grpc") {
        assert!(
            paired.is_ok(),
            "callbacks plus the door is a valid deployment"
        );
    } else {
        let refused = paired.expect_err("a build with no door cannot serve one");
        assert!(
            refused.to_string().contains("grpc"),
            "the refusal must name the missing feature, got: {refused}"
        );
    }

    // And a transport this build does not speak is named rather than ignored.
    //
    // Built *without* the gRPC vars on purpose: with them, a build lacking the
    // feature refuses at `grpc::from_env` before `parse_settle` is ever
    // reached, and this assertion would pass for the wrong reason. The error
    // is matched on the value, so only the settle parser can produce it.
    let mut bogus: Vec<(&str, &str)> = push
        .iter()
        .copied()
        .filter(|(key, _)| !matches!(*key, SETTLE_VAR | "FLEXIQ_GRPC_LISTEN"))
        .collect();
    bogus.push((SETTLE_VAR, "carrier-pigeon"));
    let refused = Config::from_map(&env(&bogus))
        .expect_err("an unknown settle transport must be refused, not read as off");
    assert!(
        refused.to_string().contains("carrier-pigeon"),
        "the refusal must echo the value it did not understand, got: {refused}"
    );
}

#[test]
fn a_queue_override_caps_a_push_deployment() {
    // Before overrides reached `flexiq-server`, a push deployment ran every
    // job on the core defaults: this cap was set and nothing read it.
    const CAPACITY: u32 = 4;
    const JOBS: usize = 4;

    let storage = temp_storage("push-override");
    let cap = serde_json::json!({"max_concurrent": 1});
    overrides::set(
        Scope::Queue,
        &*storage,
        None,
        "default",
        cap.as_object().expect("an object"),
    )
    .expect("store the queue override");
    for _ in 0..JOBS {
        storage
            .enqueue(new_job("greet"))
            .expect("enqueue a job under test");
    }

    let stub = PushTarget::start(Reply::outcome("success").after(Duration::from_millis(250)));
    let deployment = Deployment::start(&storage, target(&stub.url, CAPACITY, &storage), None);

    poll_until(Duration::from_secs(30), || {
        storage.stats(None).expect("read the queue stats").completed == JOBS as i64
    })
    .expect("every job must complete on the push target");

    assert_eq!(
        stub.peak_in_flight(),
        1,
        "the queue override's cap must bound the target below its own capacity"
    );

    deployment.stop();
}

#[test]
fn each_named_target_receives_only_its_own_queues() {
    let storage = temp_storage("push-named");
    let mut orders = new_job("orders.process");
    orders.queue = "orders".to_string();
    let mut invoices = new_job("billing.invoice");
    invoices.queue = "invoices".to_string();
    storage.enqueue(orders).expect("enqueue an orders job");
    storage.enqueue(invoices).expect("enqueue an invoices job");

    let orders_stub = PushTarget::start(Reply::outcome("success"));
    let billing_stub = PushTarget::start(Reply::outcome("success"));

    let lane = |name: &str, url: &str, queue: &str| Lane {
        name: Some(name.to_string()),
        path: DispatchPath::Push(Arc::new(
            HttpDispatchTarget::new(target(url, 1, &storage)).expect("the stub target builds"),
        )),
        settings: SchedulerSettings {
            queues: vec![queue.to_string()],
            namespace: None,
            workers: None,
            maintenance: false,
            push_dispatch: None,
            events: None,
        },
    };
    let supervisor = SchedulerSupervisor::with_lanes(
        storage.clone(),
        vec![
            lane("orders", &orders_stub.url, "orders"),
            lane("billing", &billing_stub.url, "invoices"),
        ],
    );
    supervisor
        .ensure_started()
        .expect("every lane starts at boot");
    assert!(supervisor.is_running());

    poll_until(Duration::from_secs(30), || {
        storage.stats(None).expect("read the queue stats").completed == 2
    })
    .expect("both jobs must complete, each on its own target");

    let queues = |stub: &PushTarget| -> Vec<String> {
        stub.received()
            .iter()
            .map(|received| {
                received
                    .header("x-flexiq-queue")
                    .expect("every dispatch names its queue")
                    .to_string()
            })
            .collect()
    };
    assert_eq!(queues(&orders_stub), vec!["orders"]);
    assert_eq!(queues(&billing_stub), vec!["invoices"]);

    supervisor.shutdown();
    assert!(!supervisor.is_running());
}

/// With several targets, the door must hand a settle to the one that made the
/// dispatch: every other target has never heard of the job and would refuse
/// it as a report that reached the wrong replica.
#[cfg(feature = "grpc")]
#[test]
fn the_door_hands_a_settle_to_the_target_that_made_the_dispatch() {
    use flexiq_server::grpc::pb::executor as pb;
    use flexiq_server::grpc::pb::executor::executor_service_server::ExecutorService as _;
    use flexiq_server::grpc::ExecutorDoor;

    let storage = temp_storage("push-named-settle");
    let mut job = new_job("billing.invoice");
    job.queue = "invoices".to_string();
    let job = storage.enqueue(job).expect("enqueue the job");

    let orders_stub = PushTarget::start(Reply::outcome("success"));
    let billing_stub = PushTarget::start(Reply::status(202));
    let build = |url: &str| {
        Arc::new(
            HttpDispatchTarget::new(settle_target(url, 1, &storage))
                .expect("the stub target builds"),
        )
    };
    let orders = build(&orders_stub.url);
    let billing = build(&billing_stub.url);

    let lane = |name: &str, target: &Arc<HttpDispatchTarget>, queue: &str| Lane {
        name: Some(name.to_string()),
        path: DispatchPath::Push(Arc::clone(target)),
        settings: SchedulerSettings {
            queues: vec![queue.to_string()],
            namespace: None,
            workers: None,
            maintenance: false,
            push_dispatch: None,
            events: None,
        },
    };
    let supervisor = Arc::new(SchedulerSupervisor::with_lanes(
        storage.clone(),
        vec![
            lane("orders", &orders, "orders"),
            lane("billing", &billing, "invoices"),
        ],
    ));
    supervisor.ensure_started().expect("every lane starts");

    poll_until(Duration::from_secs(15), || billing.awaiting_settle() == 1)
        .expect("the billing target must be waiting for a settle");
    let lease = billing_stub
        .received()
        .first()
        .and_then(|dispatch| dispatch.header(HDR_LEASE))
        .expect("a dispatch carries a lease")
        .as_bytes()
        .to_vec();

    // The target that does *not* hold the job is listed first, so a door that
    // did not route would hand it the settle and have it refused.
    let door = ExecutorDoor::settle_only(
        vec![Arc::clone(&orders), Arc::clone(&billing)],
        Arc::clone(&supervisor),
    );
    assert_eq!(door.awaiting_settle(), Some(1));

    let runtime = tokio::runtime::Runtime::new().expect("a runtime for the door");
    runtime
        .block_on(door.settle(tonic::Request::new(pb::SettleRequest {
            outcome: Some(pb::settle_request::Outcome::Cancelled(pb::CancelledFrame {
                job_id: job.id.clone(),
                task_name: job.task_name.clone(),
                wall_time: None,
                lease: Some(lease),
            })),
        })))
        .expect("the settle reaches the target holding the dispatch");

    poll_until(Duration::from_secs(15), || {
        storage
            .get_job(&job.id, None)
            .ok()
            .flatten()
            .is_some_and(|job| job.status == JobStatus::Cancelled)
    })
    .expect("the settled job must end as the settle said");
    assert!(orders_stub.received().is_empty());

    drop(runtime);
    supervisor.shutdown();
}
