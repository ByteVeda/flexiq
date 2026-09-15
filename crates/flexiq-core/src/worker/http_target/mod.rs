//! One push target: an operator-configured URL every claimed job is POSTed
//! to, for platforms that start a process from an inbound request (Cloud Run,
//! Lambda, and similar).
//!
//! A push target is a [`WorkerDispatcher`],
//! deliberately not a [`Transport`](crate::worker::transport::Transport): an
//! HTTP request/response pair has no duplex stream to split and sends no
//! `hello` frame, so it announces no slots — its capacity is configuration,
//! not negotiation. `HttpTargetConfig`'s `capacity` field is that number.
//!
//! This module is the configuration, the URL validation and the dispatcher
//! itself; the `contract` submodule is what actually goes on the wire, and
//! `attempt` is the per-attempt state machine.
//!
//! # Exactly one result per job
//!
//! Every job handed to [`WorkerDispatcher::run`] produces **exactly one**
//! [`JobResult`], or is dropped by the lease re-check because someone else
//! already settled it. Never zero by accident, never two. Every path out of
//! the private `attempt` module's `run_one` settles: no budget, an oversized
//! payload, a header that will not render, a signing failure, a transport
//! failure, a deadline, a cancel, a refusal, an outcome — and an abandonment
//! at the shutdown drain deadline, which settles as a retryable failure
//! rather than orphaning the lease.
//!
//! # Why a late target cannot corrupt FlexiQ state
//!
//! The budget expires, so the response future is dropped and one
//! `Failure { should_retry, timed_out }` is emitted. `release_in_flight`
//! retires the lease; `authorize_attempt` says `Authorized`, because nothing
//! has superseded the attempt yet; the retry bumps `retry_count` and revokes
//! the claim in its own transaction. The next dispatch wins a *new* claim
//! under a new epoch, so a new lease is issued and the old one is stale, and
//! the original target eventually finishes into a closed connection.
//!
//! Duplicate **side effects** are real, and they are the target's to dedupe —
//! which is what [`idempotency_key`] is for. Duplicate **FlexiQ state** is
//! impossible.
//!
//! # What `cancel()` promises, per topology
//!
//! | Topology | What `cancel()` promises |
//! |---|---|
//! | Native | The handler observes the storage flag and stops. |
//! | Attach | The scheduler sends a `cancel` frame; the executor stops. |
//! | **Push** | The request is abandoned, the attempt settles `Cancelled`, the target keeps running and its side effects still happen, and its result is fenced out on arrival. |
//!
//! There is deliberately no storage-polling cancel loop here: reaching the
//! target's own process is issue #846's design, and a second cancel path
//! invented now would have to be unwound when it lands.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use crossbeam_channel::Sender;
use tokio::sync::{watch, Notify, Semaphore};
use tokio::task::JoinSet;

use crate::http::{DispatchClient, EgressPolicy, OutboundAuth, Signer};
use crate::job::Job;
use crate::lease::{Lease, LeaseBook};
use crate::net::Allowlist;
use crate::scheduler::JobResult;
use crate::worker::{Capacity, SideChannel, WorkerDispatcher};

mod attempt;
mod contract;
pub use contract::{
    idempotency_key, Outcome, Refusal, ACCEPTED_NOT_SETTLED, ENVELOPE_CONTENT_TYPE, HDR_ATTEMPT,
    HDR_DEADLINE_MS, HDR_DISABLED_MIDDLEWARE, HDR_IDEMPOTENCY_KEY, HDR_JOB_ID, HDR_LEASE,
    HDR_MAX_ATTEMPTS, HDR_METADATA, HDR_NAMESPACE, HDR_OUTCOME, HDR_PROTOCOL_VERSION, HDR_QUEUE,
    HDR_RETRY, HDR_TASK,
};

/// How to reach one push target, and what it is allowed to do.
#[derive(Clone)]
pub struct HttpTargetConfig {
    /// Absolute `http`/`https` URL every job is POSTed to.
    pub url: String,
    /// Jobs this target may be running at once.
    ///
    /// Configuration, not negotiation: a push target sends no `hello` and
    /// announces no slots, so there is nothing to read this from.
    pub capacity: u32,
    /// Ceiling on one request, before the job's own timeout is considered.
    pub request_timeout: Duration,
    /// Ceiling on establishing the connection.
    pub connect_timeout: Duration,
    /// How long `shutdown` waits for in-flight requests before abandoning them.
    pub shutdown_drain: Duration,
    /// Destinations this target may resolve to. Deny by default.
    pub allow: Allowlist,
    /// Permit loopback and link-local destinations.
    ///
    /// A library knob for this crate's own tests and for an embedder that
    /// genuinely dispatches to a sidecar on localhost. `flexiq-server` never
    /// sets it, and refuses the environment variable that asks for it.
    pub allow_loopback: bool,
    /// Longest payload this target is sent.
    pub max_request_bytes: usize,
    /// Longest response body read back.
    pub max_response_bytes: usize,
    /// Longest encoded `x-flexiq-metadata` header sent. Over this, the header
    /// is dropped rather than the job failed.
    pub max_metadata_header_bytes: usize,
    /// `User-Agent` sent with every dispatch.
    pub user_agent: String,
    /// How the scheduler proves to the target that it is the scheduler.
    pub auth: OutboundAuth,
    /// Where per-dispatch middleware toggles are resolved from.
    ///
    /// `None` dispatches with an empty disable list, which is what an embedder
    /// with no dashboard wants.
    pub side_channel: Option<std::sync::Arc<dyn SideChannel>>,
}

impl HttpTargetConfig {
    /// A target at `url`, permitted to reach `allow`, running `capacity` jobs
    /// at once. Every other field takes its default.
    pub fn new(url: impl Into<String>, capacity: u32, allow: Allowlist) -> Self {
        Self {
            url: url.into(),
            capacity,
            // A cold-start executor may still be importing handler modules;
            // the job's own timeout narrows this further once it starts.
            request_timeout: Duration::from_secs(60),
            // A push target is a configured, presumably nearby endpoint; five
            // seconds is generous for a TCP+TLS handshake and fails a black
            // hole fast.
            connect_timeout: Duration::from_secs(5),
            // Matches `RemoteConfig::shutdown_drain`, so an operator tunes one
            // number regardless of which dispatcher is running.
            shutdown_drain: Duration::from_secs(30),
            allow,
            // Deny by default; an embedder flips this only for a sidecar it
            // controls. `flexiq-server` never sets it.
            allow_loopback: false,
            // 8 MiB: room for a real task payload without accepting an
            // unbounded request body.
            max_request_bytes: 8 * 1024 * 1024,
            // 1 MiB: a target's answer is a status and an outcome header, not
            // a copy of the task's own result.
            max_response_bytes: 1024 * 1024,
            // 8 KiB: past any realistic encoded metadata blob, short of the
            // header caps common proxies enforce.
            max_metadata_header_bytes: 8 * 1024,
            user_agent: format!("flexiq-core/{}", env!("CARGO_PKG_VERSION")),
            // Send nothing until an operator says what to send. A target
            // that needs no credential is the only one this is correct for,
            // and it is the only default that cannot be a wrong guess.
            auth: OutboundAuth::None,
            // No dashboard, no toggles: an embedder that installs one gets
            // the resolved disable list on every dispatch, and one that does
            // not dispatches with an empty one.
            side_channel: None,
        }
    }
}

/// Hand-written for two reasons: a `dyn SideChannel` is not `Debug`, and
/// requiring it would push the bound onto every implementation for the sake
/// of a log line; and [`Self::auth`] holds credential material, so it is
/// rendered through its own already-redacted `Debug` rather than skipped —
/// which scheme is configured is worth seeing, and the secret behind it is
/// not.
impl std::fmt::Debug for HttpTargetConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpTargetConfig")
            .field("url", &self.url)
            .field("capacity", &self.capacity)
            .field("request_timeout", &self.request_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("shutdown_drain", &self.shutdown_drain)
            .field("allow", &self.allow)
            .field("allow_loopback", &self.allow_loopback)
            .field("max_request_bytes", &self.max_request_bytes)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("max_metadata_header_bytes", &self.max_metadata_header_bytes)
            .field("user_agent", &self.user_agent)
            .field("auth", &self.auth)
            .field("side_channel", &self.side_channel.is_some())
            .finish()
    }
}

/// Why a push target could not be built, or a dispatch could not be made.
#[derive(Debug, thiserror::Error)]
pub enum HttpTargetError {
    /// The configured URL was empty or whitespace-only.
    #[error("push target URL is empty")]
    MissingUrl,
    /// The URL could not be parsed at all.
    #[error("push target URL is not usable: {0}")]
    Url(String),
    /// The URL's scheme is neither `http` nor `https`.
    #[error("push target URL scheme must be http or https, got '{0}'")]
    Scheme(String),
    /// The URL has no host component.
    #[error("push target URL must include a hostname")]
    NoHost,
    /// Credentials in the authority are never sent and would be silently
    /// dropped, so a URL carrying them is refused rather than half-honoured.
    #[error("push target URL must not carry userinfo")]
    Userinfo,
    /// The URL's host is not named by [`HttpTargetConfig::allow`].
    #[error("push target host '{0}' is not on the allowlist")]
    HostRefused(String),
    /// [`HttpTargetConfig::capacity`] was `0`.
    #[error("push target capacity must be at least 1")]
    ZeroCapacity,
    /// The HTTP client could not be built.
    #[error("push target client could not be built: {0}")]
    Client(String),
    /// The configured outbound auth scheme could not be turned into a signer.
    #[error("push target auth could not be configured: {0}")]
    Auth(#[from] crate::http::AuthError),
}

/// Parse and vet a target URL: absolute, `http`/`https`, a host, no userinfo,
/// and a host `policy` permits.
///
/// Name-based rules are settled here; the addresses a name resolves to are
/// vetted at connect time by the resolver in `crate::http`, because a name
/// that resolves publicly now can be rebound before the socket opens. An
/// IP-literal host never reaches that resolver — the connector dials it
/// directly — so `EgressPolicy::permits_host` applies the unconditional
/// refusals to it right here instead.
pub(crate) fn validate_target_url(
    url: &str,
    policy: &EgressPolicy,
) -> Result<url::Url, HttpTargetError> {
    if url.trim().is_empty() {
        return Err(HttpTargetError::MissingUrl);
    }

    // `url` itself refuses an empty authority on a special scheme
    // (`https:///x`) with `ParseError::EmptyHost` before a `Url` ever comes
    // back, so that specific failure is read as `NoHost` rather than the
    // generic parse error.
    let parsed = url::Url::parse(url).map_err(|error| match error {
        url::ParseError::EmptyHost => HttpTargetError::NoHost,
        other => HttpTargetError::Url(other.to_string()),
    })?;

    match parsed.scheme() {
        "http" | "https" => {}
        other => return Err(HttpTargetError::Scheme(other.to_string())),
    }

    // Read through `url::Host` rather than `Url::host_str`: the latter keeps
    // an IPv6 literal's brackets, but `EgressPolicy::permits_host` (like
    // `Allowlist::permits_host` underneath it) expects the unbracketed form
    // every other caller gives it.
    let host = match parsed.host() {
        Some(url::Host::Domain(domain)) => domain.to_string(),
        Some(url::Host::Ipv4(v4)) => v4.to_string(),
        Some(url::Host::Ipv6(v6)) => v6.to_string(),
        None => return Err(HttpTargetError::NoHost),
    };

    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(HttpTargetError::Userinfo);
    }

    if !policy.permits_host(&host) {
        return Err(HttpTargetError::HostRefused(host));
    }

    Ok(parsed)
}

/// Recover a guard from a poisoned lock instead of cascading the panic.
///
/// The same choice `remote.rs` makes for the same reason: the state behind
/// these locks is plain bookkeeping — a cancel channel and a lease book
/// handle — so reading it after a panic elsewhere stays safe, and a second
/// panic here would take down a dispatch loop that is otherwise fine.
fn recover<T>(poisoned: PoisonError<T>) -> T {
    poisoned.into_inner()
}

/// Everything one attempt needs, shared by the dispatch loop and every task
/// it spawns.
struct Shared {
    /// Target configuration, minus the credential: [`HttpDispatchTarget::new`]
    /// moves `auth` into the signer and leaves [`OutboundAuth::None`] behind,
    /// so nothing that renders this config later can reprint the material.
    config: HttpTargetConfig,
    /// The validated, parsed target. The `url::Url` rather than the string,
    /// because the signer has to canonicalise the same parse the client
    /// resolves with.
    url: url::Url,
    /// Origin of [`Self::url`], for logs and for the error a refusal stores.
    target: String,
    /// The guarded client every dispatch is dialled through.
    client: DispatchClient,
    /// The configured scheme, or `None` for [`OutboundAuth::None`].
    signer: Option<Arc<dyn Signer>>,
    /// Slots. Lives here rather than inside `run` because
    /// [`HttpDispatchTarget::capacity`] has to answer before `run` starts and
    /// `shutdown` has to be able to close it.
    semaphore: Arc<Semaphore>,
    /// Set by `shutdown`; stops `run` taking another job off the channel.
    shutdown: AtomicBool,
    /// Set once the drain budget expires, so every attempt still in flight
    /// gives up and settles rather than being aborted with nothing emitted.
    /// Never cleared — by the time it is set, `run` has already stopped
    /// spawning attempts that could observe it.
    abandon: watch::Sender<bool>,
    /// The scheduler's lease book, once a worker has installed one.
    leases: Mutex<Option<Arc<LeaseBook>>>,
    /// One wake-up channel per in-flight job, for [`WorkerDispatcher::notify_cancel`].
    cancels: Mutex<HashMap<String, Arc<Notify>>>,
}

impl Shared {
    /// The lease book, once a worker has installed one.
    fn lease_book(&self) -> Option<Arc<LeaseBook>> {
        self.leases.lock().unwrap_or_else(recover).clone()
    }

    /// Register a wake-up channel for `job_id`, replacing any earlier one.
    ///
    /// Installed before the request is built, so a cancel that arrives while
    /// the side channel is still being consulted is stored rather than lost —
    /// [`Notify::notify_one`] leaves a permit behind for a waiter that has
    /// not parked yet.
    fn register_cancel(&self, job_id: &str) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        self.cancels
            .lock()
            .unwrap_or_else(recover)
            .insert(job_id.to_string(), Arc::clone(&notify));
        notify
    }

    /// Forget `job_id`'s wake-up channel, but only when it is still the one
    /// this attempt registered.
    ///
    /// Guarded the way [`LeaseBook::retire`] is guarded, and for the same
    /// reason: a straggler tidying up after itself must not evict the channel
    /// a newer dispatch of the same id just installed.
    fn unregister_cancel(&self, job_id: &str, notify: &Arc<Notify>) {
        let mut cancels = self.cancels.lock().unwrap_or_else(recover);
        if cancels
            .get(job_id)
            .is_some_and(|held| Arc::ptr_eq(held, notify))
        {
            cancels.remove(job_id);
        }
    }

    /// A receiver that resolves once the drain budget has expired.
    fn abandon_signal(&self) -> watch::Receiver<bool> {
        self.abandon.subscribe()
    }

    /// Whether the dispatch this attempt made is still the current one.
    ///
    /// The same disagreement test as `Shared::frame_is_current` in
    /// `remote.rs`, with the same three absences answering "current" without
    /// comparing anything:
    ///
    /// - no book — this dispatcher was never given one, so it mints no leases;
    /// - no entry for the job — nothing was dispatched under a lease, or the
    ///   dispatch has already settled, and in both cases the storage fence is
    ///   what still decides;
    /// - no lease on this dispatch — the book held nothing for the job when
    ///   the request went out, so there is nothing to disagree with.
    fn dispatch_is_current(&self, job_id: &str, lease: Option<&Lease>) -> bool {
        let Some(book) = self.lease_book() else {
            return true;
        };
        let Some(current) = book.current(job_id) else {
            return true;
        };
        match lease {
            Some(lease) => *lease == current,
            None => true,
        }
    }
}

/// A dispatch target the scheduler calls, rather than one that calls in.
///
/// A sibling of [`NativeDispatcher`](crate::worker::NativeDispatcher) and
/// [`RemoteDispatcher`](crate::worker::RemoteDispatcher), deliberately **not**
/// a [`Transport`](crate::worker::Transport): an HTTP request/response pair
/// has no duplex stream to split, no `hello` frame, and therefore announces no
/// slots — its capacity is configuration.
pub struct HttpDispatchTarget {
    shared: Arc<Shared>,
}

impl HttpDispatchTarget {
    /// Validate the target URL, build the guarded client, and build the signer.
    ///
    /// A misconfigured target fails here, at construction, rather than at the
    /// first job — an operator sees it at boot instead of in a dead-letter
    /// queue an hour later.
    pub fn new(config: HttpTargetConfig) -> Result<Self, HttpTargetError> {
        let mut config = config;
        if config.capacity == 0 {
            return Err(HttpTargetError::ZeroCapacity);
        }

        let policy = Arc::new(EgressPolicy::from_target(&config));
        let url = validate_target_url(&config.url, &policy)?;
        let client = DispatchClient::new(Arc::clone(&policy), config.connect_timeout)?;

        // Moved out rather than cloned: the credential belongs to the signer
        // from here on, and the config this target keeps is one a log line
        // can render without a second redaction rule to get right.
        let auth = std::mem::replace(&mut config.auth, OutboundAuth::None);
        let signer = auth.signer(&client, &url)?;

        let capacity = config.capacity as usize;
        let (abandon, _) = watch::channel(false);
        Ok(Self {
            shared: Arc::new(Shared {
                // The origin, not the whole URL: this value lands in
                // `job_errors`, in the dead-letter queue and in every log
                // line about the target, and an operator's path or query is
                // exactly where a capability token would sit. `contract.rs`
                // already promises a refusal "carries the target's origin".
                target: url.origin().ascii_serialization(),
                url,
                config,
                client,
                signer,
                semaphore: Arc::new(Semaphore::new(capacity)),
                shutdown: AtomicBool::new(false),
                abandon,
                leases: Mutex::new(None),
                cancels: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// Total and free slots.
    ///
    /// A concrete method, not a trait one: putting it on [`WorkerDispatcher`]
    /// would make four other dispatchers invent a number, and a defaulted zero
    /// is the same silent no-op [`WorkerDispatcher::set_lease_book`]'s default
    /// already demonstrates the cost of.
    pub fn capacity(&self) -> Capacity {
        Capacity {
            // One target is one endpoint. Zero would read as slots coming
            // from nowhere; nothing is *attached*, but something is there.
            executors: 1,
            total_slots: self.shared.config.capacity,
            free_slots: self.shared.semaphore.available_permits() as u32,
        }
    }

    /// The configured target, for a log line. Carries no credential: userinfo
    /// is refused at construction and this is the origin, without the path or
    /// query an operator may have put a token in.
    pub fn target(&self) -> &str {
        &self.shared.target
    }
}

/// Drive `tasks` to completion.
async fn join_all(tasks: &mut JoinSet<()>) {
    while tasks.join_next().await.is_some() {}
}

#[async_trait]
impl WorkerDispatcher for HttpDispatchTarget {
    async fn run(
        &self,
        mut job_rx: tokio::sync::mpsc::Receiver<Job>,
        result_tx: Sender<JobResult>,
    ) {
        // Once, at the top: forgetting `set_lease_book` is a silent no-op
        // with no compile-time signal, and the cost is that a straggler's
        // answer cannot be told from the current dispatch's.
        if self.shared.lease_book().is_none() {
            log::warn!(
                "[flexiq] push target {} is dispatching without a lease book; call \
                 WorkerDispatcher::set_lease_book so a late target's answer can be fenced out",
                self.shared.target
            );
        }

        let mut tasks: JoinSet<()> = JoinSet::new();
        while let Some(job) = job_rx.recv().await {
            if self.shared.shutdown.load(Ordering::SeqCst) {
                break;
            }
            // Parks only at real capacity — unlike `RemoteDispatcher::place`,
            // which serializes placement because it has to pick an executor.
            let permit = match Arc::clone(&self.shared.semaphore).acquire_owned().await {
                Ok(permit) => permit,
                // Closed by `shutdown`, which is the only closer.
                Err(_) => break,
            };
            tasks.spawn(attempt::run_one(
                Arc::clone(&self.shared),
                job,
                permit,
                result_tx.clone(),
            ));
        }

        let drain = self.shared.config.shutdown_drain;
        if tokio::time::timeout(drain, join_all(&mut tasks))
            .await
            .is_err()
        {
            log::warn!(
                "[flexiq] push target {} did not drain {} request(s) within the shutdown \
                 budget; abandoning them — each settles as a retryable failure",
                self.shared.target,
                tasks.len()
            );
            // Signalled, not aborted: an aborted task emits nothing, and a
            // job handed in with no result is a lease nobody retires until
            // the reaper gets to it. Each attempt drops its request and
            // settles instead.
            let _ = self.shared.abandon.send(true);
            // Unbounded, like `RemoteDispatcher::drain_and_close`'s final
            // reader join: what is left is a settle and a channel send, and
            // a crossbeam send to a dropped receiver returns `Err` rather
            // than parking. The budget above is what bounds the *requests*.
            join_all(&mut tasks).await;
        }
    }

    fn shutdown(&self) {
        self.shared.shutdown.store(true, Ordering::SeqCst);
        // Wakes a `run` parked on `acquire_owned` at full capacity; held
        // permits are unaffected, so in-flight attempts still drain.
        self.shared.semaphore.close();
    }

    fn notify_cancel(&self, job_id: &str) {
        let notify = self
            .shared
            .cancels
            .lock()
            .unwrap_or_else(recover)
            .get(job_id)
            .cloned();
        if let Some(notify) = notify {
            // `notify_one`, not `notify_waiters`: a cancel that arrives
            // between registration and the `select!` leaves a permit behind
            // instead of being dropped on the floor.
            notify.notify_one();
        }
        // A job still waiting for a slot has registered no channel yet, so its
        // cancel is dropped: there is no request to abandon. Stated rather
        // than papered over — the same race every pool that keys a cancel on
        // an in-flight job has.
    }

    fn set_lease_book(&self, leases: Arc<LeaseBook>) {
        *self.shared.leases.lock().unwrap_or_else(recover) = Some(leases);
    }

    /// Deliberately empty rather than left to the trait default.
    ///
    /// A push target performs no fenced write on the scheduler's behalf — no
    /// step commits, no side-channel writes originate here — so the claim
    /// owner has nowhere to go. Written out so the skip is visible at this
    /// dispatcher rather than inherited silently.
    fn set_claim_owner(&self, _owner: &str) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allow(entries: &str) -> Allowlist {
        Allowlist::parse(entries).expect("test allowlist parses")
    }

    /// `allow_loopback: false` — every test below that wants the relaxation
    /// builds its own policy explicitly, so the default here cannot mask it.
    fn policy(entries: &str) -> EgressPolicy {
        EgressPolicy::new(allow(entries), false)
    }

    #[test]
    fn a_url_off_the_allowlist_is_refused_naming_the_host() {
        let policy = policy("api.example.com");
        let error = validate_target_url("https://evil.example.com/hook", &policy).unwrap_err();
        assert!(matches!(error, HttpTargetError::HostRefused(host) if host == "evil.example.com"));
    }

    #[test]
    fn a_non_http_scheme_is_refused() {
        let policy = policy("files.example.com");
        let error = validate_target_url("ftp://files.example.com/hook", &policy).unwrap_err();
        assert!(matches!(error, HttpTargetError::Scheme(scheme) if scheme == "ftp"));
    }

    #[test]
    fn an_empty_url_is_refused() {
        let policy = policy("api.example.com");
        assert!(matches!(
            validate_target_url("", &policy),
            Err(HttpTargetError::MissingUrl)
        ));
        assert!(matches!(
            validate_target_url("   ", &policy),
            Err(HttpTargetError::MissingUrl)
        ));
    }

    #[test]
    fn userinfo_in_the_authority_is_refused() {
        let policy = policy("api.example.com");
        let error =
            validate_target_url("https://user:pw@api.example.com/hook", &policy).unwrap_err();
        assert!(matches!(error, HttpTargetError::Userinfo));
    }

    #[test]
    fn a_url_with_no_host_is_refused() {
        // Not `"https:///nohost"`: the `url` crate collapses repeated
        // slashes after a special scheme's `//`, so that string parses with
        // host `"nohost"` rather than an empty one. An empty authority with
        // nothing after it is what actually triggers `url`'s own
        // `EmptyHost`, which this module reads as `NoHost`.
        let policy = policy("api.example.com");
        let error = validate_target_url("https:///", &policy).unwrap_err();
        assert!(matches!(error, HttpTargetError::NoHost));
    }

    #[test]
    fn a_permitted_host_round_trips_with_its_path_and_query() {
        let policy = policy("api.example.com");
        let parsed = validate_target_url("https://api.example.com/hook?job=1", &policy)
            .expect("permitted host must be accepted");
        assert_eq!(parsed.path(), "/hook");
        assert_eq!(parsed.query(), Some("job=1"));
    }

    #[test]
    fn an_ip_literal_host_is_matched_through_the_allowlist_address_path() {
        let permitted = policy("10.0.0.0/8");
        let parsed = validate_target_url("http://10.1.2.3:8080/hook", &permitted)
            .expect("an address inside the CIDR must be accepted");
        assert_eq!(parsed.host_str(), Some("10.1.2.3"));

        let elsewhere = policy("9.0.0.0/8");
        let refused = validate_target_url("http://10.1.2.3:8080/hook", &elsewhere);
        assert!(matches!(refused, Err(HttpTargetError::HostRefused(_))));
    }

    #[test]
    fn a_literal_loopback_host_is_refused_even_when_the_allowlist_names_it() {
        // The URL-layer regression test for the same finding
        // `http::egress::tests::loopback_is_refused_even_when_the_allowlist_names_it`
        // covers at the policy layer: naming `127.0.0.0/8` on the allowlist
        // is not, on its own, permission to reach `127.0.0.1` — and an
        // IP-literal host has no resolver to catch what this URL-parsing
        // layer lets through.
        let policy = policy("127.0.0.0/8");
        let error = validate_target_url("http://127.0.0.1:8080/hook", &policy).unwrap_err();
        assert!(matches!(error, HttpTargetError::HostRefused(host) if host == "127.0.0.1"));
    }

    #[test]
    fn the_configs_debug_names_the_scheme_without_the_credential() {
        // Same technique as `http::auth`'s own `the_config_never_reaches_a_
        // formatter`: alternating letter/digit, so every 4-character window
        // carries a digit and cannot coincide with a purely alphabetic run
        // elsewhere in the rendered `Debug`.
        let secret_value = "a1b2c3d4e5f6g7h8";
        let mut config =
            HttpTargetConfig::new("https://api.example.com/hook", 4, allow("api.example.com"));
        config.auth = OutboundAuth::Bearer(crate::worker::Secret::new(secret_value));

        let rendered = format!("{config:?}");

        assert!(!rendered.contains(secret_value));
        for window in secret_value.as_bytes().windows(4) {
            let fragment = std::str::from_utf8(window).expect("ascii");
            assert!(
                !rendered.contains(fragment),
                "{fragment} leaked into the rendered config"
            );
        }
        assert!(
            rendered.contains("Bearer"),
            "which scheme is configured is worth seeing: {rendered}"
        );
        assert!(
            rendered.contains("side_channel: false"),
            "a `dyn SideChannel` renders as whether there is one: {rendered}"
        );
    }

    #[test]
    fn a_target_reports_the_capacity_it_was_configured_with() {
        let target = HttpDispatchTarget::new(HttpTargetConfig::new(
            "https://api.example.com/hook",
            3,
            allow("api.example.com"),
        ))
        .expect("an allowlisted https target builds");

        let capacity = target.capacity();

        assert_eq!(capacity.total_slots, 3);
        assert_eq!(
            capacity.free_slots, 3,
            "nothing is in flight before `run` starts"
        );
        assert_eq!(capacity.executors, 1, "one target is one endpoint");
    }

    #[test]
    fn the_targets_label_is_the_origin_and_drops_the_query() {
        // `target()` is interpolated into every stored job error and every log
        // line about this dispatcher, and an operator's query string is
        // exactly where a capability token would sit.
        let target = HttpDispatchTarget::new(HttpTargetConfig::new(
            "https://api.example.com/hook?token=a1b2c3d4",
            1,
            allow("api.example.com"),
        ))
        .expect("an allowlisted https target builds");

        assert_eq!(target.target(), "https://api.example.com");
    }
}
