//! End-to-end tests for `HttpDispatchTarget`: claim → POST → settle, against
//! a hand-rolled HTTP/1.1 stub on loopback.
//!
//! The stub is built here rather than reused from `crate::http::testing`:
//! that one is `#[cfg(test)]`, so it is not reachable from an integration
//! test, and it can neither send response headers (the outcome header is the
//! whole contract) nor delay an answer (every deadline, cancel and shutdown
//! test needs one). `flexiq-core` carries no `axum` and gets none.
//!
//! Every test runs on a multi-thread runtime: the dispatcher settles through a
//! blocking `crossbeam_channel::Sender`, which must not be the only thing a
//! single-threaded runtime is doing.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use flexiq_core::job::{now_millis, Job, JobStatus};
use flexiq_core::lease::{Lease, LeaseBook};
use flexiq_core::net::Allowlist;
use flexiq_core::scheduler::JobResult;
use flexiq_core::worker::http_target::{
    HttpDispatchTarget, HttpTargetConfig, HttpTargetError, ACCEPTED_NOT_SETTLED,
    ENVELOPE_CONTENT_TYPE, HDR_ATTEMPT, HDR_DEADLINE_MS, HDR_DISABLED_MIDDLEWARE,
    HDR_IDEMPOTENCY_KEY, HDR_JOB_ID, HDR_LEASE, HDR_MAX_ATTEMPTS, HDR_METADATA, HDR_NAMESPACE,
    HDR_OUTCOME, HDR_PROTOCOL_VERSION, HDR_QUEUE, HDR_RETRY, HDR_TASK,
};
use flexiq_core::worker::{SideChannel, WorkerDispatcher};

// ── The stub ────────────────────────────────────────────────────────

/// One request the stub received.
#[derive(Clone)]
struct Received {
    method: String,
    /// Path plus query, exactly as sent on the request line.
    target: String,
    /// Header names lowercased; values as sent.
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

impl Received {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(String::as_str)
    }
}

/// What the stub answers one request with.
#[derive(Clone)]
struct Reply {
    status: u16,
    headers: Vec<(&'static str, String)>,
    body: Vec<u8>,
    /// How long the stub holds the request before answering.
    delay: Duration,
}

impl Reply {
    fn status(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Vec::new(),
            delay: Duration::ZERO,
        }
    }

    fn outcome(status: u16, outcome: &str) -> Self {
        Self::status(status).header(HDR_OUTCOME, outcome)
    }

    fn header(mut self, name: &'static str, value: &str) -> Self {
        self.headers.push((name, value.to_string()));
        self
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

struct StubState {
    responder: Box<dyn Fn(&Received) -> Reply + Send + Sync>,
    received: Mutex<Vec<Received>>,
    in_flight: AtomicUsize,
    peak_in_flight: AtomicUsize,
}

/// A scriptable HTTP/1.1 server on an ephemeral loopback port.
///
/// One request per connection: every reply it writes carries
/// `Connection: close`, so the client never gets to reuse the socket and the
/// stub never has to implement keep-alive or pipelining.
struct Stub {
    state: Arc<StubState>,
    base_url: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Stub {
    /// Answer every request with `reply`.
    async fn always(reply: Reply) -> Self {
        Self::with(move |_| reply.clone()).await
    }

    /// Answer each request from `responder`, which sees what arrived.
    async fn with(responder: impl Fn(&Received) -> Reply + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("stub binds an ephemeral loopback port");
        let base_url = format!(
            "http://{}",
            listener
                .local_addr()
                .expect("bound listener has a local address")
        );

        let state = Arc::new(StubState {
            responder: Box::new(responder),
            received: Mutex::new(Vec::new()),
            in_flight: AtomicUsize::new(0),
            peak_in_flight: AtomicUsize::new(0),
        });

        let (shutdown, mut stopped) = tokio::sync::oneshot::channel();
        let accept_state = Arc::clone(&state);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = &mut stopped => break,
                    accepted = listener.accept() => {
                        let Ok((socket, _)) = accepted else { break };
                        tokio::spawn(serve_one(socket, Arc::clone(&accept_state)));
                    }
                }
            }
        });

        Self {
            state,
            base_url,
            shutdown: Some(shutdown),
        }
    }

    fn base_url(&self) -> &str {
        &self.base_url
    }

    fn received(&self) -> Vec<Received> {
        self.state.received.lock().expect("stub lock").clone()
    }

    fn request_count(&self) -> usize {
        self.state.received.lock().expect("stub lock").len()
    }

    /// The most requests the stub ever held at once.
    fn peak_in_flight(&self) -> usize {
        self.state.peak_in_flight.load(Ordering::SeqCst)
    }
}

impl Drop for Stub {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

/// A header block larger than this is not a request any test here sends; the
/// bound exists so a malformed one cannot hang the accept loop.
const MAX_HEAD_BYTES: usize = 64 * 1024;

async fn serve_one(mut socket: TcpStream, state: Arc<StubState>) {
    let Some((request_line, headers, mut body)) = read_head(&mut socket).await else {
        return;
    };
    let mut parts = request_line.split(' ');
    let (Some(method), Some(target)) = (parts.next(), parts.next()) else {
        return;
    };

    let content_length = headers
        .iter()
        .find(|(name, _)| name == "content-length")
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        .unwrap_or(0);
    let mut chunk = [0u8; 4096];
    while body.len() < content_length {
        match socket.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(n) => body.extend_from_slice(&chunk[..n]),
        }
    }
    body.truncate(content_length);

    let received = Received {
        method: method.to_string(),
        target: target.to_string(),
        headers: headers.into_iter().collect(),
        body,
    };

    // Recorded and counted before the reply is computed, so a test that
    // asserts "the stub received nothing" cannot pass on a race.
    let reply = {
        let mut guard = state.received.lock().expect("stub lock");
        let reply = (state.responder)(&received);
        guard.push(received);
        reply
    };

    let now = state.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
    state.peak_in_flight.fetch_max(now, Ordering::SeqCst);

    if !reply.delay.is_zero() {
        tokio::time::sleep(reply.delay).await;
    }

    let mut head = format!(
        "HTTP/1.1 {} Stub\r\nContent-Length: {}\r\nConnection: close\r\n",
        reply.status,
        reply.body.len()
    );
    for (name, value) in &reply.headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    // Best-effort: a client that gave up on its deadline has already gone.
    let _ = socket.write_all(head.as_bytes()).await;
    let _ = socket.write_all(&reply.body).await;
    let _ = socket.shutdown().await;

    state.in_flight.fetch_sub(1, Ordering::SeqCst);
}

/// Reads up to and including the blank line ending the header block, then
/// returns the request line, the parsed headers (names lowercased) and
/// whatever body bytes were already buffered past that point.
async fn read_head(socket: &mut TcpStream) -> Option<(String, Vec<(String, String)>, Vec<u8>)> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos;
        }
        if buf.len() > MAX_HEAD_BYTES {
            return None;
        }
        match socket.read(&mut chunk).await {
            Ok(0) | Err(_) => return None,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    };

    let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let body = buf[header_end + 4..].to_vec();

    let mut lines = head.split("\r\n");
    let request_line = lines.next()?.to_string();
    let headers = lines
        .filter(|line| !line.is_empty())
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_string()))
        .collect();
    Some((request_line, headers, body))
}

// ── The harness ─────────────────────────────────────────────────────

/// A target at `base_url`, permitted to reach loopback.
fn config_for(base_url: &str, capacity: u32) -> HttpTargetConfig {
    let mut config = HttpTargetConfig::new(
        format!("{base_url}/handler"),
        capacity,
        Allowlist::parse("127.0.0.0/8").expect("test allowlist parses"),
    );
    // The stub binds loopback, which the unconditional refusals reject unless
    // an embedder explicitly relaxes them — which is exactly the knob this is.
    config.allow_loopback = true;
    config
}

/// A dispatcher running against a job channel and a result channel.
struct Harness {
    target: Arc<HttpDispatchTarget>,
    job_tx: Option<tokio::sync::mpsc::Sender<Job>>,
    results: crossbeam_channel::Receiver<JobResult>,
    run: tokio::task::JoinHandle<()>,
    leases: Arc<LeaseBook>,
}

impl Harness {
    fn start(config: HttpTargetConfig) -> Self {
        let target = Arc::new(HttpDispatchTarget::new(config).expect("the target builds"));
        let leases = Arc::new(LeaseBook::default());
        target.set_lease_book(Arc::clone(&leases));

        let (job_tx, job_rx) = tokio::sync::mpsc::channel(128);
        // Generous: the dispatcher's send is blocking, and a test that only
        // collects at the end must not be able to wedge it.
        let (result_tx, results) = crossbeam_channel::bounded(256);
        let run = tokio::spawn({
            let target = Arc::clone(&target);
            async move { target.run(job_rx, result_tx).await }
        });

        Self {
            target,
            job_tx: Some(job_tx),
            results,
            run,
            leases,
        }
    }

    async fn dispatch(&self, job: Job) {
        self.job_tx
            .as_ref()
            .expect("the job channel is still open")
            .send(job)
            .await
            .expect("the dispatcher is still receiving");
    }

    /// The next settled result, or `None` if none arrives within `within`.
    ///
    /// Polled rather than blocked on: `recv_timeout` would park a runtime
    /// worker for the whole budget.
    async fn next_result(&self, within: Duration) -> Option<JobResult> {
        let deadline = Instant::now() + within;
        loop {
            match self.results.try_recv() {
                Ok(result) => return Some(result),
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    if Instant::now() >= deadline {
                        return None;
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                Err(crossbeam_channel::TryRecvError::Disconnected) => return None,
            }
        }
    }

    async fn expect_result(&self, within: Duration) -> JobResult {
        self.next_result(within)
            .await
            .expect("the dispatcher settles every job it is handed")
    }

    async fn collect(&self, count: usize, within: Duration) -> Vec<JobResult> {
        let deadline = Instant::now() + within;
        let mut collected = Vec::with_capacity(count);
        while collected.len() < count {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match self.next_result(remaining).await {
                Some(result) => collected.push(result),
                None => break,
            }
        }
        collected
    }

    /// Close the job channel, which is what makes `run` leave its loop and
    /// start the shutdown drain.
    fn close_jobs(&mut self) {
        self.job_tx = None;
    }

    /// Close the job channel and wait for `run` to return.
    async fn stop(mut self, within: Duration) -> bool {
        self.close_jobs();
        tokio::time::timeout(within, &mut self.run).await.is_ok()
    }
}

/// A running job carrying the smallest real wire envelope there is.
fn a_job(id: &str) -> Job {
    Job {
        id: id.to_string(),
        queue: "default".to_string(),
        task_name: "resize".to_string(),
        payload: flexiq_core::wire::encode_call(&[], &[]),
        status: JobStatus::Running,
        priority: 0,
        created_at: 0,
        scheduled_at: 0,
        started_at: Some(now_millis()),
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

/// A `JobResult`'s variant name.
///
/// `JobResult` carries no `Debug` — a success holds the task's result bytes —
/// so an assertion that wants to say what it got instead says this.
fn describe(result: &JobResult) -> &'static str {
    match result {
        JobResult::Success { .. } => "Success",
        JobResult::Failure { .. } => "Failure",
        JobResult::Cancelled { .. } => "Cancelled",
        JobResult::Slept { .. } => "Slept",
        // `JobResult` is `#[non_exhaustive]`, so a wildcard is required even
        // though every variant above is named.
        _ => "an unrecognized variant",
    }
}

/// A side channel that answers a toggle lookup with `disabled`, after `delay`.
///
/// The delay is the point of one of the two tests below: resolving the disable
/// list runs on the blocking pool *before* any request is built, so it is the
/// cheapest way to stall an attempt somewhere the request budget has to reach
/// but a per-request HTTP timeout never would.
struct Toggles {
    disabled: Vec<String>,
    delay: Duration,
}

impl SideChannel for Toggles {
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
        // `std::thread::sleep`, not `tokio::time::sleep`: this runs on the
        // blocking pool, exactly as a real settings read would.
        if !self.delay.is_zero() {
            std::thread::sleep(self.delay);
        }
        self.disabled.clone()
    }
}

fn failure_of(result: &JobResult) -> (&str, bool, bool) {
    match result {
        JobResult::Failure {
            error,
            should_retry,
            timed_out,
            ..
        } => (error, *should_retry, *timed_out),
        other => panic!("expected a failure, got a {}", describe(other)),
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dispatch_carries_the_envelope_unchanged() {
    let stub = Stub::always(Reply::outcome(200, "success")).await;
    let harness = Harness::start(config_for(stub.base_url(), 1));

    let job = a_job("job-envelope");
    let payload = job.payload.clone();
    harness.dispatch(job).await;
    harness.expect_result(Duration::from_secs(5)).await;

    let received = stub.received();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].method, "POST");
    assert_eq!(received[0].target, "/handler");
    assert_eq!(
        received[0].body, payload,
        "the body must be the job's payload byte for byte"
    );
    assert_eq!(
        received[0].body.first(),
        Some(&0x02),
        "the tagged wire envelope must reach the target with its tag byte intact"
    );
    assert_eq!(
        received[0].header("content-type"),
        Some(ENVELOPE_CONTENT_TYPE)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dispatch_carries_the_job_the_attempt_and_the_lease() {
    let stub = Stub::always(Reply::outcome(200, "success")).await;
    let mut config = config_for(stub.base_url(), 1);
    // A ceiling well under the job's 30s timeout, so the budget is exactly the
    // ceiling and the deadline header has one correct value rather than a
    // range the raw job deadline would also satisfy.
    config.request_timeout = Duration::from_secs(5);
    let harness = Harness::start(config);

    let lease = Lease::from_epoch(987_654_321);
    let token = String::from_utf8_lossy(lease.as_bytes()).into_owned();
    harness.leases.issue("job-headers", lease);

    let mut job = a_job("job-headers");
    job.retry_count = 2;
    job.max_retries = 7;
    job.task_name = "resize".to_string();
    job.queue = "images".to_string();
    job.namespace = Some("tenant-a".to_string());
    harness.dispatch(job).await;
    harness.expect_result(Duration::from_secs(5)).await;

    let received = stub.received();
    let request = &received[0];
    assert_eq!(request.header(HDR_JOB_ID), Some("job-headers"));
    assert_eq!(request.header(HDR_ATTEMPT), Some("2"));
    assert_eq!(request.header(HDR_MAX_ATTEMPTS), Some("7"));
    assert_eq!(request.header(HDR_TASK), Some("resize"));
    assert_eq!(request.header(HDR_QUEUE), Some("images"));
    assert_eq!(request.header(HDR_NAMESPACE), Some("tenant-a"));
    assert_eq!(request.header(HDR_LEASE), Some(token.as_str()));
    assert_eq!(
        request.header(HDR_IDEMPOTENCY_KEY),
        Some(format!("job-headers.2.{token}").as_str()),
        "the key is job, attempt and lease"
    );
    assert!(
        request.header(HDR_PROTOCOL_VERSION).is_some(),
        "a target has to be told which protocol it is answering"
    );
    assert_eq!(
        request.header(HDR_DEADLINE_MS),
        Some("5000"),
        "the header carries the request budget the dispatcher will actually wait — the \
         5s ceiling here, not the job's ~30s remaining timeout"
    );
    assert!(
        request.header(HDR_DISABLED_MIDDLEWARE).is_none(),
        "a target with no side channel disables nothing, and an empty header is noise"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dispatch_with_no_lease_omits_the_header_and_shortens_the_key() {
    let stub = Stub::always(Reply::outcome(200, "success")).await;
    let harness = Harness::start(config_for(stub.base_url(), 1));

    // No `leases.issue` — the book holds nothing for this job.
    let mut job = a_job("job-leaseless");
    job.retry_count = 1;
    harness.dispatch(job).await;
    harness.expect_result(Duration::from_secs(5)).await;

    let received = stub.received();
    assert_eq!(received[0].header(HDR_LEASE), None);
    assert_eq!(
        received[0].header(HDR_IDEMPOTENCY_KEY),
        Some("job-leaseless.1")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_success_body_settles_the_job() {
    let stub = Stub::always(Reply::outcome(200, "success").body("done")).await;
    let harness = Harness::start(config_for(stub.base_url(), 1));

    harness.dispatch(a_job("job-success")).await;

    match harness.expect_result(Duration::from_secs(5)).await {
        JobResult::Success {
            job_id,
            result,
            task_name,
            ..
        } => {
            assert_eq!(job_id, "job-success");
            assert_eq!(result, Some(b"done".to_vec()));
            assert_eq!(task_name, "resize");
        }
        other => panic!("expected a success, got a {}", describe(&other)),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failure_body_settles_the_job_retryably() {
    let stub = Stub::always(
        Reply::outcome(200, "failure")
            .header(HDR_RETRY, "true")
            .body(r#"{"errtype":"ValueError","message":"bad input"}"#),
    )
    .await;
    let harness = Harness::start(config_for(stub.base_url(), 1));

    harness.dispatch(a_job("job-failure")).await;

    let result = harness.expect_result(Duration::from_secs(5)).await;
    let (error, should_retry, timed_out) = failure_of(&result);
    assert!(should_retry);
    assert!(!timed_out, "a task that raised did not time out");
    assert_eq!(error, r#"{"errtype":"ValueError","message":"bad input"}"#);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_attempt_that_stalls_before_the_request_still_settles_on_its_budget() {
    // The budget has to bound the *whole* attempt, not just the exchange.
    // Everything before the request — the toggle lookup here, and signing,
    // which dials a credential endpoint through a client that carries no
    // request timeout at all — would otherwise suspend the attempt with no
    // deadline over it: no result, and the semaphore permit held forever.
    let stub = Stub::always(Reply::outcome(200, "success")).await;
    let mut config = config_for(stub.base_url(), 1);
    config.request_timeout = Duration::from_millis(300);
    config.side_channel = Some(Arc::new(Toggles {
        disabled: Vec::new(),
        delay: Duration::from_secs(2),
    }));
    let harness = Harness::start(config);

    harness.dispatch(a_job("job-stalled")).await;

    let started = Instant::now();
    let result = harness.expect_result(Duration::from_millis(1_500)).await;
    let (_, should_retry, timed_out) = failure_of(&result);
    assert!(should_retry, "a stalled attempt is worth retrying");
    assert!(timed_out, "it ended on the attempt's own deadline");
    assert!(
        started.elapsed() < Duration::from_millis(1_500),
        "the attempt settled on its 300ms budget, not on the 2s stall"
    );
    assert_eq!(
        stub.request_count(),
        0,
        "the budget expired before a request was ever built"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_side_channels_disable_list_rides_the_dispatch() {
    let stub = Stub::always(Reply::outcome(200, "success")).await;
    let mut config = config_for(stub.base_url(), 1);
    config.side_channel = Some(Arc::new(Toggles {
        disabled: vec!["otel".to_string(), "sentry".to_string()],
        delay: Duration::ZERO,
    }));
    let harness = Harness::start(config);

    harness.dispatch(a_job("job-toggles")).await;
    harness.expect_result(Duration::from_secs(5)).await;

    assert_eq!(
        stub.received()[0].header(HDR_DISABLED_MIDDLEWARE),
        Some("otel,sentry"),
        "the operator's disable list is resolved by the scheduler and carried as a header"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_redirect_is_refused_rather_than_followed() {
    // `DispatchClient` sets `redirect::Policy::none()`: a 3xx could carry a
    // signed body to a host that never passed the egress guard.
    let stub =
        Stub::always(Reply::status(302).header("location", "https://elsewhere.example.com/")).await;
    let harness = Harness::start(config_for(stub.base_url(), 1));

    harness.dispatch(a_job("job-3xx")).await;

    let result = harness.expect_result(Duration::from_secs(5)).await;
    let (error, should_retry, _) = failure_of(&result);
    assert!(
        !should_retry,
        "a redirect does not resolve itself on a retry"
    );
    assert!(
        error.contains("302 redirect"),
        "{error} must say it refused to follow"
    );
    assert_eq!(
        stub.request_count(),
        1,
        "exactly one request: the redirect was refused, not chased"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slept_outcome_is_refused_because_push_has_no_step_session() {
    let stub = Stub::always(Reply::outcome(200, "slept")).await;
    let harness = Harness::start(config_for(stub.base_url(), 1));

    harness.dispatch(a_job("job-slept")).await;

    let result = harness.expect_result(Duration::from_secs(5)).await;
    let (error, should_retry, _) = failure_of(&result);
    assert!(!should_retry);
    assert!(error.contains("slept"), "{error}");
    assert!(
        error.contains("step session"),
        "{error} must say why a push dispatch cannot honour it"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_target_that_cannot_be_dialled_fails_retryably_without_naming_the_url() {
    // A port nothing is listening on: bound to learn a free one, then dropped.
    // This is the only refusal built from a live `reqwest::Error`, so it is the
    // only end-to-end exercise of the `without_url()` stripping — without which
    // reqwest's own `Display` would put the full target URL, path and query
    // included, into a stored job error.
    let closed = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener binds");
    let address = closed.local_addr().expect("listener has a local address");
    drop(closed);

    let mut config = config_for(&format!("http://{address}"), 1);
    config.connect_timeout = Duration::from_secs(2);
    let harness = Harness::start(config);

    harness.dispatch(a_job("job-refused")).await;

    let result = harness.expect_result(Duration::from_secs(10)).await;
    let (error, should_retry, timed_out) = failure_of(&result);
    assert!(should_retry, "a connection failure is worth retrying");
    assert!(!timed_out, "nothing timed out; the connection was refused");
    assert!(
        error.contains("could not be reached"),
        "{error} must say what happened"
    );
    assert!(
        !error.contains("/handler"),
        "the dialled URL must be stripped — `target` is the origin, and the path \
         reaching a stored job error means reqwest's own Display got through: {error}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_500_is_retryable_and_names_the_status() {
    let stub = Stub::always(Reply::status(503)).await;
    let harness = Harness::start(config_for(stub.base_url(), 1));

    harness.dispatch(a_job("job-5xx")).await;

    let result = harness.expect_result(Duration::from_secs(5)).await;
    let (error, should_retry, timed_out) = failure_of(&result);
    assert!(should_retry, "a 5xx is the target's problem, not the job's");
    assert!(!timed_out);
    assert!(
        error.contains("server error 503"),
        "{error} must name the status — matched on the phrase, not on a bare \"503\" a \n         loopback port could supply"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_202_dead_letters_once_rather_than_retrying_forever() {
    let stub = Stub::always(Reply::status(202)).await;
    let harness = Harness::start(config_for(stub.base_url(), 1));

    harness.dispatch(a_job("job-202")).await;

    let result = harness.expect_result(Duration::from_secs(5)).await;
    let (error, should_retry, _) = failure_of(&result);
    assert!(
        !should_retry,
        "a target that keeps answering 202 would otherwise be retried until the cap"
    );
    assert!(error.contains(ACCEPTED_NOT_SETTLED), "{error}");
    assert!(
        harness
            .next_result(Duration::from_millis(300))
            .await
            .is_none(),
        "a 202 settles exactly once"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_that_outruns_its_deadline_fails_retryably_without_orphaning_the_lease() {
    // The stub answers long after the dispatcher has given up.
    let stub = Stub::always(Reply::outcome(200, "success").after(Duration::from_secs(3))).await;
    let mut config = config_for(stub.base_url(), 1);
    config.request_timeout = Duration::from_millis(300);
    let harness = Harness::start(config);

    harness.leases.issue("job-deadline", Lease::from_epoch(11));
    harness.dispatch(a_job("job-deadline")).await;

    let started = Instant::now();
    let result = harness.expect_result(Duration::from_secs(2)).await;
    let (error, should_retry, timed_out) = failure_of(&result);
    assert!(should_retry, "a target that was too slow is worth retrying");
    assert!(timed_out, "the attempt ended on its own deadline");
    assert!(
        error.contains("did not answer"),
        "{error} must say what happened"
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the dispatcher gave up on its budget, not on the stub's"
    );

    // And nothing more, once the stub finally answers into a dead connection.
    assert!(
        harness.next_result(Duration::from_secs(4)).await.is_none(),
        "a late answer must not settle the job a second time"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_response_under_a_stale_lease_is_refused() {
    let stub = Stub::always(Reply::outcome(200, "success").after(Duration::from_millis(400))).await;
    let harness = Harness::start(config_for(stub.base_url(), 1));

    harness.leases.issue("job-stale", Lease::from_epoch(1));
    harness.dispatch(a_job("job-stale")).await;

    // Re-issued mid-flight: the job was re-dispatched under a new claim while
    // this attempt was still waiting, so the answer it is about to get belongs
    // to a dispatch nobody is listening for any more.
    tokio::time::sleep(Duration::from_millis(100)).await;
    harness.leases.issue("job-stale", Lease::from_epoch(2));

    if let Some(result) = harness.next_result(Duration::from_secs(2)).await {
        panic!(
            "a response under a superseded lease must produce no JobResult at all, got a {}",
            describe(&result)
        );
    }
    assert_eq!(
        stub.request_count(),
        1,
        "the request itself was made — it is the answer that is dropped"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrency_is_bounded_by_the_configured_capacity() {
    let stub = Stub::always(Reply::outcome(200, "success").after(Duration::from_millis(80))).await;
    let harness = Harness::start(config_for(stub.base_url(), 2));

    for index in 0..10 {
        harness.dispatch(a_job(&format!("job-{index}"))).await;
    }

    let results = harness.collect(10, Duration::from_secs(15)).await;
    assert_eq!(results.len(), 10);
    assert!(
        stub.peak_in_flight() <= 2,
        "capacity 2 must never be exceeded; the stub saw {} at once",
        stub.peak_in_flight()
    );
    assert_eq!(stub.request_count(), 10);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_job_handed_in_produces_exactly_one_result() {
    // Ten classes, five jobs each. Eight are things a target can answer;
    // the last two are refused locally and never reach the stub at all —
    // between them they cover every way `run_one` can return.
    const CLASSES: [&str; 10] = [
        "success",
        "failure-retry",
        "failure-fatal",
        "server-error",
        "client-error",
        "accepted",
        "missing-outcome",
        "unknown-outcome",
        "oversized",
        "no-budget",
    ];

    let stub = Stub::with(|received| {
        match received.header(HDR_TASK).unwrap_or_default() {
            "success" => Reply::outcome(200, "success").body("ok"),
            "failure-retry" => Reply::outcome(200, "failure")
                .header(HDR_RETRY, "true")
                .body("transient"),
            "failure-fatal" => Reply::outcome(200, "failure")
                .header(HDR_RETRY, "false")
                .body("permanent"),
            "server-error" => Reply::status(500),
            "client-error" => Reply::status(422),
            "accepted" => Reply::status(202),
            "missing-outcome" => Reply::status(200),
            "unknown-outcome" => Reply::outcome(200, "exploded"),
            // Nothing else should ever arrive; answering 418 makes a leak
            // visible rather than letting it pass as a success.
            _ => Reply::status(418),
        }
    })
    .await;

    let mut config = config_for(stub.base_url(), 4);
    // Small enough that the "oversized" class is refused before it is sent,
    // and still larger than every other payload here.
    config.max_request_bytes = 64;
    let harness = Harness::start(config);

    let mut expected_ids = Vec::new();
    for class in CLASSES {
        for index in 0..5 {
            let id = format!("{class}-{index}");
            let mut job = a_job(&id);
            job.task_name = class.to_string();
            match class {
                "oversized" => job.payload = vec![0x02; 128],
                // Started long enough ago that `started_at + timeout_ms` is
                // already behind us: the reaper owns this job, so no request
                // is made.
                "no-budget" => {
                    job.timeout_ms = 1_000;
                    job.started_at = Some(now_millis() - 60_000);
                }
                _ => {}
            }
            expected_ids.push(id);
            harness.dispatch(job).await;
        }
    }

    let results = harness
        .collect(CLASSES.len() * 5, Duration::from_secs(30))
        .await;
    assert_eq!(
        results.len(),
        50,
        "every job handed in settles exactly once — never zero, never two"
    );

    let mut settled: HashMap<String, usize> = HashMap::new();
    for result in &results {
        *settled.entry(result.job_id().to_string()).or_default() += 1;
    }
    for id in &expected_ids {
        assert_eq!(settled.get(id).copied(), Some(1), "job {id} settled once");
    }

    // And each class settled the way its response class says it should.
    let by_id: HashMap<&str, &JobResult> = results.iter().map(|r| (r.job_id(), r)).collect();
    for class in CLASSES {
        let result = by_id[format!("{class}-0").as_str()];
        match class {
            "success" => assert!(matches!(result, JobResult::Success { .. }), "{class}"),
            "failure-retry" | "server-error" | "no-budget" => {
                let (_, should_retry, _) = failure_of(result);
                assert!(should_retry, "{class} must be retried");
            }
            _ => {
                let (_, should_retry, _) = failure_of(result);
                assert!(!should_retry, "{class} must not be retried");
            }
        }
    }

    // The two locally refused classes never reached the target.
    for received in stub.received() {
        let task = received.header(HDR_TASK).unwrap_or_default();
        assert!(
            task != "oversized" && task != "no-budget",
            "{task} must be refused before it is dialled"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_drains_in_flight_requests_then_stops() {
    let stub = Stub::always(Reply::outcome(200, "success").after(Duration::from_millis(200))).await;
    let mut config = config_for(stub.base_url(), 4);
    config.shutdown_drain = Duration::from_secs(10);
    let harness = Harness::start(config);

    for index in 0..4 {
        harness.dispatch(a_job(&format!("job-drain-{index}"))).await;
    }
    // Let the requests reach the stub before the shutdown flag is set.
    tokio::time::sleep(Duration::from_millis(60)).await;
    harness.target.shutdown();

    let results = harness.collect(4, Duration::from_secs(10)).await;
    assert_eq!(results.len(), 4, "shutdown drains what is in flight");
    assert!(
        results
            .iter()
            .all(|result| matches!(result, JobResult::Success { .. })),
        "a drained request settles on the target's answer, not on the shutdown"
    );
    assert!(
        harness.stop(Duration::from_secs(10)).await,
        "run returns once the job channel closes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_still_running_at_the_drain_deadline_is_abandoned_rather_than_orphaned() {
    // Not in the brief's list, but the brief's rule is: anything still running
    // at drain expiry is abandoned *and emitted as a retryable failure*. An
    // abandoned request that emitted nothing would leave a lease for the
    // reaper, which is the one thing this dispatcher must never do.
    let stub = Stub::always(Reply::outcome(200, "success").after(Duration::from_secs(5))).await;
    let mut config = config_for(stub.base_url(), 2);
    config.shutdown_drain = Duration::from_millis(300);
    let mut harness = Harness::start(config);

    harness.dispatch(a_job("job-abandoned")).await;
    tokio::time::sleep(Duration::from_millis(60)).await;
    harness.target.shutdown();
    harness.close_jobs();

    let result = harness.expect_result(Duration::from_secs(10)).await;
    assert_eq!(result.job_id(), "job-abandoned");
    let (error, should_retry, timed_out) = failure_of(&result);
    assert!(should_retry, "an abandoned request is worth retrying");
    assert!(
        !timed_out,
        "the job's own timeout did not expire; the dispatcher went away"
    );
    assert!(error.contains("shutdown drain"), "{error}");
    assert!(
        harness.stop(Duration::from_secs(15)).await,
        "run returns once the drain has expired and every attempt has settled"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancel_aborts_the_request_and_settles_the_attempt_as_cancelled() {
    let stub = Stub::always(Reply::outcome(200, "success").after(Duration::from_secs(5))).await;
    let harness = Harness::start(config_for(stub.base_url(), 1));

    harness.dispatch(a_job("job-cancel")).await;
    // Once the request is on the wire, so the cancel races the response and
    // not the registration.
    tokio::time::sleep(Duration::from_millis(100)).await;
    harness.target.notify_cancel("job-cancel");

    match harness.expect_result(Duration::from_secs(3)).await {
        JobResult::Cancelled { job_id, .. } => assert_eq!(job_id, "job-cancel"),
        other => panic!("expected a cancellation, got a {}", describe(&other)),
    }
    assert_eq!(
        stub.request_count(),
        1,
        "the target was dialled; what a cancel stops is FlexiQ waiting, not the target working"
    );
}

#[test]
fn the_target_refuses_a_host_that_is_not_on_the_allowlist() {
    let config = HttpTargetConfig::new(
        "https://evil.example.com/hook",
        1,
        Allowlist::parse("api.example.com").expect("test allowlist parses"),
    );

    // `HttpDispatchTarget` carries no `Debug` — it holds a live client and a
    // signer — so `expect_err` cannot be used here; `matches!` needs none.
    let refused = HttpDispatchTarget::new(config);

    assert!(
        matches!(refused, Err(HttpTargetError::HostRefused(ref host)) if host == "evil.example.com"),
        "an operator has to see this at boot, not in a dead-letter queue an hour later"
    );
}

#[test]
fn the_target_refuses_loopback_even_when_the_allowlist_names_it() {
    // `allow_loopback` stays false: naming `127.0.0.0/8` on the allowlist is
    // not, on its own, permission to reach the host the scheduler runs on.
    let config = HttpTargetConfig::new(
        "http://127.0.0.1:8080/hook",
        1,
        Allowlist::parse("127.0.0.0/8").expect("test allowlist parses"),
    );

    let refused = HttpDispatchTarget::new(config);

    assert!(matches!(refused, Err(HttpTargetError::HostRefused(ref host)) if host == "127.0.0.1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_oversized_payload_is_refused_before_it_is_sent() {
    let stub = Stub::always(Reply::outcome(200, "success")).await;
    let mut config = config_for(stub.base_url(), 1);
    config.max_request_bytes = 16;
    let harness = Harness::start(config);

    let mut job = a_job("job-oversized");
    job.payload = vec![0x02; 256];
    harness.dispatch(job).await;

    let result = harness.expect_result(Duration::from_secs(5)).await;
    let (error, should_retry, _) = failure_of(&result);
    assert!(!should_retry, "a payload does not shrink on a retry");
    assert!(
        error.contains("request of 256 bytes exceeds the 16 byte cap"),
        "{error}"
    );
    assert_eq!(
        stub.request_count(),
        0,
        "the target must never see a request the cap already refused"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_response_body_is_capped() {
    let stub = Stub::always(Reply::outcome(200, "success").body(vec![b'x'; 4096])).await;
    let mut config = config_for(stub.base_url(), 1);
    config.max_response_bytes = 32;
    let harness = Harness::start(config);

    harness.dispatch(a_job("job-big-body")).await;

    let result = harness.expect_result(Duration::from_secs(5)).await;
    let (error, should_retry, _) = failure_of(&result);
    assert!(
        !should_retry,
        "a response that does not fit is not a transient condition"
    );
    assert!(
        error.contains("response exceeded the 32 byte cap"),
        "{error} must name the cap"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_rides_base64url_and_is_dropped_when_oversized() {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;

    let stub = Stub::always(Reply::outcome(200, "success")).await;
    let mut config = config_for(stub.base_url(), 1);
    config.max_metadata_header_bytes = 64;
    let harness = Harness::start(config);

    let metadata = r#"{"tenant":"acme","trace":"abc"}"#;
    let mut small = a_job("job-metadata-small");
    small.metadata = Some(metadata.to_string());
    harness.dispatch(small).await;
    let first = harness.expect_result(Duration::from_secs(5)).await;

    let mut large = a_job("job-metadata-large");
    large.metadata = Some(format!(r#"{{"blob":"{}"}}"#, "z".repeat(512)));
    harness.dispatch(large).await;
    let second = harness.expect_result(Duration::from_secs(5)).await;

    let received = stub.received();
    let encoded = received[0]
        .header(HDR_METADATA)
        .expect("metadata that fits rides along");
    assert_eq!(
        URL_SAFE_NO_PAD
            .decode(encoded)
            .expect("the header is base64url without padding"),
        metadata.as_bytes(),
        "the blob must survive the encoding unchanged"
    );

    assert!(
        received[1].header(HDR_METADATA).is_none(),
        "an oversized blob is dropped, not truncated and not sent"
    );
    assert!(
        matches!(first, JobResult::Success { .. }) && matches!(second, JobResult::Success { .. }),
        "metadata is advisory input to middleware; dropping the header must not fail a job"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_client_never_retransmits_a_post_of_its_own_accord() {
    let stub = Stub::always(Reply::outcome(200, "success").after(Duration::from_secs(3))).await;
    let mut config = config_for(stub.base_url(), 1);
    config.request_timeout = Duration::from_millis(250);
    let harness = Harness::start(config);

    harness.dispatch(a_job("job-no-retransmit")).await;
    let result = harness.expect_result(Duration::from_secs(3)).await;
    let (_, should_retry, timed_out) = failure_of(&result);
    assert!(should_retry && timed_out);

    // Well past the stub's own delay: a client-side retry would have shown up
    // as a second request by now. FlexiQ's retry is the scheduler's, and it
    // does not happen inside one dispatch.
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(
        stub.request_count(),
        1,
        "one dispatch is one POST, however it ends"
    );
}

#[test]
fn a_zero_capacity_target_is_refused_at_construction() {
    let config = HttpTargetConfig::new(
        "https://api.example.com/hook",
        0,
        Allowlist::parse("api.example.com").expect("test allowlist parses"),
    );

    let refused = HttpDispatchTarget::new(config);

    assert!(matches!(refused, Err(HttpTargetError::ZeroCapacity)));
}
