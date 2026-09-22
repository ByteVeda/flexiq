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
//! Every job that reaches an **attempt** produces **exactly one**
//! [`JobResult`], or is dropped by the lease re-check because someone else
//! already settled it. Never zero by accident, never two. Every path out of
//! the private `attempt` module's `run_one` settles: no budget, an oversized
//! payload, a header that will not render, a signing failure, a transport
//! failure, a deadline, a cancel, a refusal, an outcome — and an abandonment
//! at the shutdown drain deadline, which settles as a retryable failure
//! rather than orphaning the lease.
//!
//! Three cases never reach an attempt, all three are shutdown-only, and each
//! leaves a lease for the stale-job reap. Stated rather than hidden:
//!
//! - a job `run` has already taken off the channel when it observes the
//!   shutdown flag;
//! - a job it has already taken when the semaphore turns out to be closed;
//! - an attempt still unable to settle a full drain budget *after* the abandon
//!   signal, which `run` aborts rather than hang. See `run`'s own comment for
//!   the one condition that reaches it.
//!
//! The first two windows are one job wide each, and both stay open. Closing
//! them means settling a job that was never dispatched, which changes the
//! shape of a dispatch loop [`NativeDispatcher`](crate::worker::NativeDispatcher)
//! shares — one change for every dispatcher at once, not something a
//! scheduler's wiring can do on this one's behalf. Until then the stale-job
//! reap recovers the lease, which is the same path a worker that dies
//! mid-dispatch already takes.
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
//! | **Push, in the request** | The request is abandoned and the attempt settles `Cancelled`. The target is told only by the closed connection; its side effects still happen and its answer is fenced out. |
//! | **Push, accepted (`202`)** | The attempt settles `Cancelled`. The target's next report under its lease is refused [`SettleRefused::Cancelled`] by this replica, while it still remembers the ending (the last 1024; after that, `NotHere` — also a stop) — a target that polls stops then; one that never reports runs to the end and its `Settle` is refused. |
//!
//! Push stops the *result*, and tells a target that asks; only native and
//! attach stop the *work*. A cancel reaches this dispatcher as
//! [`WorkerDispatcher::notify_cancel`], which the worker's cancel relay calls
//! for every in-flight job whose storage flag is set — this module polls
//! nothing itself. Reaching into a target's running process is not attempted:
//! most platforms that start one from a request cannot route a second request
//! to it.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use crossbeam_channel::Sender;
use tokio::sync::{oneshot, watch, Notify, Semaphore};
use tokio::task::JoinSet;

use crate::http::{DispatchClient, EgressPolicy, OutboundAuth, Signer};
use crate::job::{now_millis, Job};
use crate::lease::{Lease, LeaseBook, MAX_LEASE_EXTENSION};
use crate::net::Allowlist;
use crate::scheduler::JobResult;
use crate::storage::records::{SettleClaimant, SettleGrant};
use crate::worker::{Capacity, SideChannel, WorkerDispatcher};

mod attempt;
mod contract;
pub use contract::{
    idempotency_key, Disposition, Outcome, Refusal, ACCEPTED_NOT_SETTLED, ENVELOPE_CONTENT_TYPE,
    HDR_ATTEMPT, HDR_DEADLINE_MS, HDR_DISABLED_MIDDLEWARE, HDR_IDEMPOTENCY_KEY, HDR_JOB_ID,
    HDR_LEASE, HDR_MAX_ATTEMPTS, HDR_METADATA, HDR_NAMESPACE, HDR_OUTCOME, HDR_PROTOCOL_VERSION,
    HDR_QUEUE, HDR_RETRY, HDR_TASK,
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
    ///
    /// The budget for **one** wait, and `shutdown` takes two: it waits this
    /// long for the requests themselves, signals the abandon, then waits this
    /// long again for each attempt to settle. A shutdown therefore runs to at
    /// most `2 × shutdown_drain`, which is the figure a deployment's
    /// termination grace period has to cover — not this one.
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
    /// Whether a `202 Accepted` hands the job off to be settled later.
    ///
    /// Off by default, and deliberately. A `202` has always been a refusal
    /// that dead-letters in one attempt with a greppable reason, so turning it
    /// into a wait silently would change a shipped promise — and the class of
    /// bug it would hide is a framework answering `202` by default, which is
    /// exactly what the mandatory `x-flexiq-outcome` header exists to catch.
    ///
    /// Requires a [`side_channel`](Self::side_channel) that
    /// [supports the settle marker](SideChannel::supports_settle): a dispatch
    /// that cannot be fenced must not be accepted.
    pub settle_callbacks: bool,
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
            // number regardless of which dispatcher is running — but this path
            // spends it twice (see the field's docs), so the same 30 here is a
            // 60-second worst case rather than a 30-second one.
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
            // Opt-in: see the field's docs for why this cannot default on.
            settle_callbacks: false,
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
    /// The URL is cleartext `http` to somewhere other than loopback, or
    /// loopback without [`HttpTargetConfig::allow_loopback`].
    ///
    /// Every dispatch carries the job payload, the lease and the idempotency
    /// key, and three of the four outbound-auth schemes put credential
    /// material in a header. HMAC signs the request without encrypting it, so
    /// it is no exception.
    #[error(
        "push target host '{0}' must be reached over https: cleartext http would put the job \
         payload, the lease and any outbound credential on the wire in the clear. Only a \
         loopback host — an address in 127.0.0.0/8 or ::1, or the name 'localhost' — may use \
         http, and only with the loopback relaxation enabled"
    )]
    InsecureTransport(String),
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
/// a host `policy` permits, and — unless that host is loopback and the
/// loopback relaxation is on — `https` rather than cleartext `http`.
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

    // After the allowlist, not before: a host that is refused outright should
    // say so, rather than being told to fix its scheme first and then refused
    // again for the reason that was always going to decide it.
    if parsed.scheme() == "http" && !cleartext_permitted(&host, policy) {
        return Err(HttpTargetError::InsecureTransport(host));
    }

    Ok(parsed)
}

/// Whether cleartext `http` may be spoken to `host`.
///
/// `https` is the rule, and this is its one relaxation: a loopback host, and
/// only when the same [`HttpTargetConfig::allow_loopback`] knob that lets the
/// egress policy dial loopback at all is set. A same-host sidecar and a local
/// development server keep working; nothing that leaves the host does.
///
/// `localhost` is taken at its name rather than resolved — RFC 6761 §6.3
/// reserves the name for the loopback interface, and there is nothing to
/// resolve at construction time anyway. A resolver that answers something
/// else would get cleartext; that is accepted because reaching it needs
/// `allow_loopback` deliberately set, which is a library-only knob for an
/// embedder dispatching to its own host — `flexiq-server` never sets it and
/// refuses the environment variable that asks for it.
fn cleartext_permitted(host: &str, policy: &EgressPolicy) -> bool {
    if !policy.allows_loopback() {
        return false;
    }
    // The same normalization `EgressPolicy::permits_host` applies: one
    // trailing dot is the root-relative form of the same name.
    let normalized = host.strip_suffix('.').unwrap_or(host);
    if let Ok(address) = normalized.parse::<std::net::IpAddr>() {
        return crate::net::is_loopback_address(address);
    }
    normalized.eq_ignore_ascii_case("localhost")
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
    ///
    /// Written with `send_replace`, never `send`: `send` returns `Err` **and
    /// leaves the value unchanged** when no receiver is currently subscribed,
    /// and an attempt suspended before it subscribes would then never see the
    /// give-up at all. Never cleared — by the time it is set, `run` has
    /// stopped spawning attempts.
    abandon: watch::Sender<bool>,
    /// The scheduler's lease book, once a worker has installed one.
    leases: Mutex<Option<Arc<LeaseBook>>>,
    /// One wake-up channel per in-flight job, for [`WorkerDispatcher::notify_cancel`].
    cancels: Mutex<HashMap<String, Arc<Notify>>>,
    /// The owner every claim this scheduler wins is recorded under, once a
    /// worker has told us. Needed because `await_settle` is a fenced write on
    /// the scheduler's behalf, and the fence is `(owner, attempt, epoch)`.
    claim_owner: Mutex<Option<String>>,
    /// The channel `run` was handed, so a settle arriving outside any attempt
    /// can hand the scheduler its result. `None` before `run` starts.
    results: Mutex<Option<Sender<JobResult>>>,
    /// Dispatches this process accepted and is still waiting to be settled.
    ///
    /// Process-local, and that is the whole of it: the waiting attempt task,
    /// its permit and its result channel all live here, so a `Settle` has to
    /// reach the replica that dispatched. A call that lands elsewhere is
    /// refused by name rather than half-applied.
    accepted: Mutex<HashMap<String, Accepted>>,
    /// Accepted dispatches this process stopped holding open, and why — so a
    /// target that asks "is this still mine?" is told it was cancelled, rather
    /// than that it reached the wrong replica. Locked only after `accepted`.
    ended: Mutex<EndedDispatches>,
}

/// How many ended dispatches are remembered. A target polls on the order of
/// seconds, so the one asking about a dispatch is almost always still in here;
/// one that falls out is told `NotHere`, which is still a "stop".
const ENDED_CAPACITY: usize = 1024;

/// Why an accepted dispatch stopped being held open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ending {
    /// An operator cancelled the job.
    Cancelled,
    /// Anything else: a settle won, the deadline passed, or the drain ran out.
    Superseded,
}

/// A bounded memory of ended accepted dispatches, oldest evicted first.
#[derive(Default)]
struct EndedDispatches {
    by_job: HashMap<String, (Option<Lease>, Ending)>,
    order: VecDeque<String>,
}

impl EndedDispatches {
    fn record(&mut self, job_id: &str, lease: Option<Lease>, ending: Ending) {
        // A re-dispatched job ends again under a new lease. Its old position
        // must go too, or eviction reaches it early and drops the fresh entry.
        if self
            .by_job
            .insert(job_id.to_string(), (lease, ending))
            .is_some()
        {
            self.order.retain(|id| id != job_id);
        }
        self.order.push_back(job_id.to_string());
        while self.order.len() > ENDED_CAPACITY {
            if let Some(oldest) = self.order.pop_front() {
                self.by_job.remove(&oldest);
            }
        }
    }

    /// The refusal a report under `lease` earns. Only the lease the dispatch
    /// was made under learns *why* it ended; any other is told `NotHere`, the
    /// answer it would have had before this record existed.
    fn refusal(&self, job_id: &str, lease: &Lease) -> SettleRefused {
        match self.by_job.get(job_id) {
            Some((held, ending)) if held.as_ref().is_none_or(|held| held == lease) => {
                match ending {
                    Ending::Cancelled => SettleRefused::Cancelled,
                    Ending::Superseded => SettleRefused::Fenced,
                }
            }
            _ => SettleRefused::NotHere,
        }
    }
}

/// One dispatch the target accepted and has not settled.
struct Accepted {
    /// The lease the dispatch was made under, so a settle naming another one
    /// is refused before it reaches storage.
    lease: Option<Lease>,
    /// The job's namespace, which every fenced write has to be scoped by.
    namespace: Option<String>,
    /// The task this job runs, needed to build the `JobResult` a later settle
    /// emits — a settle frame names it, but the scheduler's record is what the
    /// result must carry.
    task_name: String,
    /// The attempt the dispatch was made at, the second part of the fence.
    attempt: i32,
    /// Retry budget, carried so a settled failure lands on the same
    /// retry-or-dead-letter decision an in-request failure would.
    max_retries: i32,
    /// Whether the durable settle marker this dispatch is fenced on exists yet.
    ///
    /// The entry is registered *before* the marker is written, so that a
    /// callback racing the write has somewhere to land rather than being lost.
    /// It must not be told `Fenced` in that window: the contract says a fenced
    /// report is never resent, so the target would throw away a result nothing
    /// had refused. Until this flips, a report is
    /// [`NotReady`](SettleRefused::NotReady) — the one refusal here that is
    /// retryable, because nothing has been decided yet.
    ready: bool,
    /// Wakes the waiting attempt so it stops waiting and releases its permit.
    ///
    /// Carries nothing: whoever consumed the settle marker also emits the
    /// result, so this says "you are relieved", not "here is the answer". A
    /// channel that carried the outcome would make two places able to emit it.
    relieve: Option<oneshot::Sender<()>>,
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

    /// The owner claims are recorded under, once a worker has installed one.
    fn claim_owner(&self) -> Option<String> {
        self.claim_owner.lock().unwrap_or_else(recover).clone()
    }

    /// The side channel, if this deployment installed one.
    fn side_channel(&self) -> Option<&Arc<dyn SideChannel>> {
        self.config.side_channel.as_ref()
    }

    /// Record a dispatch as accepted, returning the handle its attempt waits on.
    fn register_accepted(&self, job: &Job, lease: Option<Lease>) -> oneshot::Receiver<()> {
        let (relieve, wait) = oneshot::channel();
        self.accepted.lock().unwrap_or_else(recover).insert(
            job.id.clone(),
            Accepted {
                lease,
                namespace: job.namespace.clone(),
                task_name: job.task_name.clone(),
                attempt: job.retry_count,
                max_retries: job.max_retries,
                ready: false,
                relieve: Some(relieve),
            },
        );
        wait
    }

    /// Mark an accepted dispatch's settle marker as durable, so reports on it
    /// stop being answered "not ready yet".
    fn mark_accepted_ready(&self, job_id: &str) {
        if let Some(held) = self.accepted.lock().unwrap_or_else(recover).get_mut(job_id) {
            held.ready = true;
        }
    }

    /// The attempt an accepted dispatch was made at.
    fn accepted_attempt(&self, job_id: &str) -> Option<i32> {
        self.accepted
            .lock()
            .unwrap_or_else(recover)
            .get(job_id)
            .map(|held| held.attempt)
    }

    /// Wake the attempt waiting on an accepted dispatch so it stops waiting
    /// and releases its permit.
    ///
    /// Carries no outcome: whoever consumed the settle marker has already
    /// emitted the result, and a second path to `result_tx` is the second
    /// settlement the marker exists to prevent.
    fn relieve_accepted(&self, job_id: &str) {
        let relieve = self
            .accepted
            .lock()
            .unwrap_or_else(recover)
            .get_mut(job_id)
            .and_then(|held| held.relieve.take());
        if let Some(relieve) = relieve {
            // The receiver is gone when the attempt already stopped waiting —
            // it lost the race for the marker, and has nothing left to do.
            let _ = relieve.send(());
        }
    }

    /// Emit the result of a dispatch settled out of band.
    ///
    /// Called only by whoever consumed the settle marker. The channel is the
    /// one `run` was handed: a settle and the attempt that accepted it put
    /// their results in the same place, so the scheduler cannot tell them
    /// apart — which is the point.
    fn emit_settled(&self, job_id: &str, outcome: SettledOutcome) {
        let (task_name, attempt, max_retries) = {
            let accepted = self.accepted.lock().unwrap_or_else(recover);
            match accepted.get(job_id) {
                Some(held) => (held.task_name.clone(), held.attempt, held.max_retries),
                // Consumed between the caller's read and this one. Nothing to
                // build a result from, and the marker is spent either way.
                None => return,
            }
        };
        let result = outcome.into_result(job_id, &task_name, attempt, max_retries);
        let sent = self
            .results
            .lock()
            .unwrap_or_else(recover)
            .as_ref()
            .map(|tx| tx.send(result));
        if !matches!(sent, Some(Ok(()))) {
            log::error!(
                "[flexiq] job {job_id} was settled out of band but its result could not be \
                 handed to the scheduler; the stale-job reaper will retry the job"
            );
        }
    }

    /// Stop holding an accepted dispatch open, remembering why — but only when
    /// the lease still matches.
    ///
    /// Guarded like [`Shared::unregister_cancel`]: a straggler tidying up
    /// after itself must not evict the entry a newer dispatch just installed.
    /// The ending is recorded under the same lock that removes the entry, so a
    /// report arriving in between never finds neither and reads `NotHere`.
    fn retire_accepted(&self, job_id: &str, lease: Option<&Lease>, ending: Ending) {
        let mut accepted = self.accepted.lock().unwrap_or_else(recover);
        if accepted
            .get(job_id)
            .is_some_and(|held| held.lease.as_ref() == lease)
        {
            accepted.remove(job_id);
            self.ended
                .lock()
                .unwrap_or_else(recover)
                .record(job_id, lease.cloned(), ending);
        }
    }

    /// What a report on a dispatch this process is *not* holding open is
    /// told. Safe under the `accepted` lock: `ended` is always taken second.
    fn not_held(&self, job_id: &str, lease: &Lease) -> SettleRefused {
        self.ended
            .lock()
            .unwrap_or_else(recover)
            .refusal(job_id, lease)
    }

    /// How many dispatches this process has accepted and not yet settled.
    fn accepted_count(&self) -> usize {
        self.accepted.lock().unwrap_or_else(recover).len()
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
                claim_owner: Mutex::new(None),
                results: Mutex::new(None),
                accepted: Mutex::new(HashMap::new()),
                ended: Mutex::new(EndedDispatches::default()),
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

    /// Whether this target accepts a `202` and waits for a later settle.
    ///
    /// Read by `flexiq-server` to decide whether to serve the executor door at
    /// all: with callbacks off there is nothing for a target to report to.
    pub fn accepts_settle_callbacks(&self) -> bool {
        self.shared.config.settle_callbacks
    }

    /// How many dispatches this process has accepted and is still waiting on.
    ///
    /// Process-local by construction — the waiting attempts live here — so a
    /// metric built on it is "accepted here", never a cluster total.
    pub fn awaiting_settle(&self) -> usize {
        self.shared.accepted_count()
    }

    /// Settle a dispatch this target accepted, from outside the request that
    /// carried it.
    ///
    /// The whole of #845 in one method. In order, and the order matters:
    ///
    /// 1. The dispatch must be one *this* process is holding open. The waiting
    ///    attempt owns the permit and the result channel, so a call that
    ///    reached another replica is refused by name rather than half-applied.
    /// 2. The lease must be the one the dispatch was made under, checked here
    ///    against the registry and again, durably, in the consume below.
    /// 3. The settle marker is consumed. Whoever consumes it emits the result
    ///    — that is what makes a settle single-use, and what stops the
    ///    deadline and this call from both answering for one dispatch.
    /// 4. Only then is the result emitted and the waiting attempt relieved.
    pub fn settle(&self, job_id: &str, lease: &Lease, outcome: SettledOutcome) -> SettleResult {
        let shared = &self.shared;

        let (expected, namespace) = {
            let accepted = shared.accepted.lock().unwrap_or_else(recover);
            match accepted.get(job_id) {
                // Read under the same lock as the lease: a readiness checked
                // separately could go stale between the two reads, which is
                // the race this flag exists to close.
                Some(held) if !held.ready => return Err(SettleRefused::NotReady),
                Some(held) => (held.lease.clone(), held.namespace.clone()),
                None => return Err(shared.not_held(job_id, lease)),
            }
        };

        // Checked before storage so the common misdirection — a settle for a
        // dispatch that already lost its lease — is answered without a write.
        // The durable consume below is what actually enforces it.
        let Some(epoch) = lease.epoch() else {
            return Err(SettleRefused::Fenced);
        };
        if expected.as_ref().is_some_and(|held| held != lease) {
            return Err(SettleRefused::Fenced);
        }

        let channel = shared.side_channel().ok_or(SettleRefused::Unsupported)?;
        match channel.claim_settle(job_id, SettleClaimant::Lease(epoch), namespace.as_deref()) {
            Ok(SettleGrant::Granted) => {}
            Ok(SettleGrant::Refused) => return Err(SettleRefused::Fenced),
            Err(error) => return Err(SettleRefused::Storage(error.to_string())),
        }

        shared.emit_settled(job_id, outcome);
        shared.relieve_accepted(job_id);
        Ok(())
    }

    /// Push an accepted dispatch's deadline out, returning the deadline that
    /// was actually stored.
    ///
    /// Clamped to [`MAX_LEASE_EXTENSION`] rather than refused: making "ask
    /// again with a smaller number" the correct client behaviour would be a
    /// retry loop written into the contract for no benefit.
    pub fn extend_lease(
        &self,
        job_id: &str,
        lease: &Lease,
        extend_by: Duration,
    ) -> Result<i64, SettleRefused> {
        let shared = &self.shared;

        let (expected, namespace) = {
            let accepted = shared.accepted.lock().unwrap_or_else(recover);
            match accepted.get(job_id) {
                // Read under the same lock as the lease: a readiness checked
                // separately could go stale between the two reads, which is
                // the race this flag exists to close.
                Some(held) if !held.ready => return Err(SettleRefused::NotReady),
                Some(held) => (held.lease.clone(), held.namespace.clone()),
                None => return Err(shared.not_held(job_id, lease)),
            }
        };
        let Some(epoch) = lease.epoch() else {
            return Err(SettleRefused::Fenced);
        };
        if expected.as_ref().is_some_and(|held| held != lease) {
            return Err(SettleRefused::Fenced);
        }

        let owner = shared.claim_owner().ok_or(SettleRefused::Unsupported)?;
        let channel = shared.side_channel().ok_or(SettleRefused::Unsupported)?;
        let attempt = shared
            .accepted_attempt(job_id)
            .ok_or_else(|| shared.not_held(job_id, lease))?;

        let granted = extend_by.min(MAX_LEASE_EXTENSION);
        let deadline = now_millis().saturating_add(granted.as_millis() as i64);
        match channel.await_settle(
            job_id,
            &owner,
            attempt,
            Some(epoch),
            deadline,
            namespace.as_deref(),
        ) {
            // Monotonic in storage, so this is the stored value and not
            // necessarily the one just proposed.
            Ok(Some(stored)) => Ok(stored),
            Ok(None) => Err(SettleRefused::Fenced),
            Err(error) => Err(SettleRefused::Storage(error.to_string())),
        }
    }

    /// Report progress for a dispatch this target accepted.
    ///
    /// Fire and forget, like the frame it mirrors: `Ok` means the report was
    /// taken, not that a row was written, because a task that only wanted to
    /// report progress must never be blocked by the scheduler's database.
    ///
    /// **Not** gated on the durable fence, and the asymmetry is deliberate.
    /// This advances an attempt rather than settling one, so the question is
    /// "is this the dispatch we are holding open", which the in-process
    /// registry answers — the same question `frame_is_current` asks of the
    /// identical frame on the attach stream. Consuming the settle marker here
    /// would settle the job on a progress report.
    pub fn report_progress(
        &self,
        job_id: &str,
        lease: &Lease,
        progress: i32,
    ) -> Result<(), SettleRefused> {
        let namespace = self.accepted_namespace(job_id, lease)?;
        let channel = self
            .shared
            .side_channel()
            .ok_or(SettleRefused::Unsupported)?;
        channel.update_progress(job_id, progress, namespace.as_deref());
        Ok(())
    }

    /// Write one structured log line for a dispatch this target accepted. As
    /// [`report_progress`](Self::report_progress), and fenced the same way.
    pub fn write_task_log(
        &self,
        job_id: &str,
        lease: &Lease,
        task_name: &str,
        level: &str,
        message: &str,
        extra: Option<&str>,
    ) -> Result<(), SettleRefused> {
        let namespace = self.accepted_namespace(job_id, lease)?;
        let channel = self
            .shared
            .side_channel()
            .ok_or(SettleRefused::Unsupported)?;
        channel.write_task_log(
            job_id,
            task_name,
            level,
            message,
            extra,
            namespace.as_deref(),
        );
        Ok(())
    }

    /// The namespace of an accepted dispatch, once the lease has been checked
    /// against the one it was made under.
    fn accepted_namespace(
        &self,
        job_id: &str,
        lease: &Lease,
    ) -> Result<Option<String>, SettleRefused> {
        let accepted = self.shared.accepted.lock().unwrap_or_else(recover);
        let Some(held) = accepted.get(job_id) else {
            return Err(self.shared.not_held(job_id, lease));
        };
        if !held.ready {
            return Err(SettleRefused::NotReady);
        }
        if held
            .lease
            .as_ref()
            .is_some_and(|expected| expected != lease)
        {
            return Err(SettleRefused::Fenced);
        }
        Ok(held.namespace.clone())
    }
}

/// The outcome a target reports for a dispatch it accepted earlier.
///
/// The three settling frames, and only those: an accepted dispatch has no step
/// session, so there is no `slept` here for the same reason there is no
/// `x-flexiq-outcome: slept` on the request path.
#[derive(Debug, Clone)]
pub enum SettledOutcome {
    /// The task completed. `None` means it returned nothing; `Some(vec![])`
    /// means it returned an empty value. They are different answers.
    Success {
        /// The encoded result, as the tagged envelope.
        result: Option<Vec<u8>>,
        /// Wall-clock nanoseconds the attempt ran.
        wall_time_ns: i64,
    },
    /// The task raised, or the target reports it that way.
    Failure {
        /// Canonical JSON `TaskError` when the target wrote one, free prose
        /// otherwise.
        error: String,
        /// The target decides: only it saw the exception.
        should_retry: bool,
        /// Whether the failure was an execution timeout.
        timed_out: bool,
        /// Wall-clock nanoseconds the attempt ran.
        wall_time_ns: i64,
    },
    /// The task observed a cancel and stopped.
    Cancelled {
        /// Wall-clock nanoseconds the attempt ran.
        wall_time_ns: i64,
    },
}

impl SettledOutcome {
    /// Build the settled [`JobResult`], through the same
    /// `ExecutorMessage::into_job_result` an in-request answer goes through.
    ///
    /// Deliberately not a second construction site: a job settled a minute
    /// after its request ended has to reach the scheduler as the same shape as
    /// one settled inside it, or the retry and dead-letter paths get two
    /// subtly different inputs.
    fn into_result(
        self,
        job_id: &str,
        task_name: &str,
        attempt: i32,
        max_retries: i32,
    ) -> JobResult {
        let message = match self {
            SettledOutcome::Success {
                result,
                wall_time_ns,
            } => {
                let payload = result.unwrap_or_default();
                let result_len = (!payload.is_empty()).then_some(payload.len());
                return settled(
                    crate::worker::protocol::ExecutorMessage::Success {
                        job_id: job_id.to_string(),
                        result_len,
                        task_name: task_name.to_string(),
                        wall_time_ns,
                        lease: None,
                    },
                    payload,
                );
            }
            SettledOutcome::Failure {
                error,
                should_retry,
                timed_out,
                wall_time_ns,
            } => crate::worker::protocol::ExecutorMessage::Failure {
                job_id: job_id.to_string(),
                error,
                retry_count: attempt,
                max_retries,
                task_name: task_name.to_string(),
                wall_time_ns,
                should_retry,
                timed_out,
                lease: None,
            },
            SettledOutcome::Cancelled { wall_time_ns } => {
                crate::worker::protocol::ExecutorMessage::Cancelled {
                    job_id: job_id.to_string(),
                    task_name: task_name.to_string(),
                    wall_time_ns,
                    lease: None,
                }
            }
        };
        settled(message, Vec::new())
    }
}

/// See `contract::settle`: `into_job_result` answers `None` only for frames
/// this module never builds.
fn settled(message: crate::worker::protocol::ExecutorMessage, payload: Vec<u8>) -> JobResult {
    message
        .into_job_result(payload)
        .expect("into_job_result returns None only for frames this module never builds")
}

/// Why an out-of-band settle was not applied.
#[derive(Debug, Clone)]
pub enum SettleRefused {
    /// No accepted dispatch for this job in **this** process.
    ///
    /// Distinct from [`Fenced`](Self::Fenced) on purpose: the caller's lease
    /// may be perfectly good and simply have arrived at the wrong replica,
    /// which is an operator's routing problem and not a stale attempt. The two
    /// want different answers from the person reading the error.
    NotHere,
    /// The dispatch is accepted but its durable settle marker is not written
    /// yet — a callback that beat the scheduler's own bookkeeping.
    ///
    /// **The one refusal here a caller should retry.** Every other one means a
    /// decision was made; this one means none has been. Telling a target
    /// `Fenced` in this window would have it throw away a perfectly good
    /// result on the strength of the contract's "never resend a fenced
    /// report", and the attempt would then wait out its whole deadline having
    /// refused the answer it was waiting for.
    NotReady,
    /// The lease is absent, undecodable, or not the one this dispatch was made
    /// under — or the marker was already consumed. The attempt was settled by
    /// someone else and this answer is thrown away.
    Fenced,
    /// The job was cancelled while this dispatch was accepted; the attempt is
    /// already settled `Cancelled`.
    ///
    /// Told only to a caller presenting the lease the dispatch was made under,
    /// and never retryable — it is the "stop working" answer a target polling
    /// `ExtendLease` or `ReportProgress` is asking for.
    Cancelled,
    /// This deployment cannot fence an out-of-band settle at all.
    Unsupported,
    /// The fence could not be evaluated. Refusing rather than guessing: the
    /// one thing that must not happen is an unfenced settle landing.
    Storage(String),
}

/// What [`HttpDispatchTarget::settle`] answers.
pub type SettleResult = Result<(), SettleRefused>;

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

        // Kept so a settle arriving outside any attempt still has somewhere to
        // put its result: the gRPC door calls `settle` on this target, not on
        // a task, and the two must reach the scheduler through one channel.
        *self.shared.results.lock().unwrap_or_else(recover) = Some(result_tx.clone());

        if self.shared.config.settle_callbacks {
            log::info!(
                "[flexiq] push target {} accepts 202 and waits for a settle callback",
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
            //
            // `send_replace`, not `send`: `send` fails and leaves the value
            // `false` when nothing is subscribed right now, and an attempt
            // suspended between its spawn and its `select!` would then miss
            // the give-up permanently and settle on its own budget instead —
            // recording a timeout for a shutdown that was not one.
            self.shared.abandon.send_replace(true);
            // Bounded again, because settling is not unconditionally quick:
            // it ends in a blocking send on a bounded channel, which parks
            // while a receiver is alive but no longer draining. A caller that
            // drops the result receiver before awaiting `run` never reaches
            // this arm.
            if tokio::time::timeout(drain, join_all(&mut tasks))
                .await
                .is_err()
            {
                log::error!(
                    "[flexiq] push target {} still has {} attempt(s) unable to settle after \
                     the abandon signal; aborting them — their leases are the reaper's. The \
                     usual cause is a result receiver that is alive but no longer draining.",
                    self.shared.target,
                    tasks.len()
                );
                // The third of the module doc's three exceptions, and the
                // last resort: a permanent hang would take the whole worker
                // down with it, and a reaped lease is recoverable where a
                // wedged shutdown is not.
                tasks.shutdown().await;
            }
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

    /// Kept, because this dispatcher *does* perform a fenced write on the
    /// scheduler's behalf once settle callbacks are on.
    ///
    /// Accepting a `202` records a settle marker on the execution claim, and
    /// that write is fenced on `(owner, attempt, epoch)` like every other
    /// write on a dispatch. The owner is the one part of the triple this side
    /// cannot derive: the attempt is the job's `retry_count` and the epoch is
    /// the lease, but the owner belongs to whoever won the claim. An owner
    /// this dispatcher made up would be an owner it could forge, which is the
    /// whole reason the value arrives this way.
    ///
    /// (It was deliberately empty until #845, on the grounds that no fenced
    /// write originated here. That stopped being true.)
    fn set_claim_owner(&self, owner: &str) {
        *self.shared.claim_owner.lock().unwrap_or_else(recover) = Some(owner.to_string());
    }
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
        // `https`, because a cleartext target off loopback is refused for a
        // different reason entirely — see `cleartext_http_is_refused_off_loopback`.
        let permitted = policy("10.0.0.0/8");
        let parsed = validate_target_url("https://10.1.2.3:8443/hook", &permitted)
            .expect("an address inside the CIDR must be accepted");
        assert_eq!(parsed.host_str(), Some("10.1.2.3"));

        let elsewhere = policy("9.0.0.0/8");
        let refused = validate_target_url("https://10.1.2.3:8443/hook", &elsewhere);
        assert!(matches!(refused, Err(HttpTargetError::HostRefused(_))));
    }

    #[test]
    fn cleartext_http_is_refused_off_loopback() {
        // Every dispatch carries the payload, the lease and the idempotency
        // key, and bearer, OIDC and SigV4 all put credential material in a
        // header; HMAC signs without encrypting. None of that may go out in
        // the clear, however the allowlist is written.
        let by_name = policy("api.example.com");
        let error = validate_target_url("http://api.example.com/hook", &by_name).unwrap_err();
        assert!(
            matches!(error, HttpTargetError::InsecureTransport(ref host) if host == "api.example.com")
        );

        let by_cidr = policy("10.0.0.0/8");
        let error = validate_target_url("http://10.1.2.3:8080/hook", &by_cidr).unwrap_err();
        assert!(matches!(error, HttpTargetError::InsecureTransport(_)));

        // The relaxation does not widen to a non-loopback host.
        let relaxed = EgressPolicy::new(allow("api.example.com"), true);
        let error = validate_target_url("http://api.example.com/hook", &relaxed).unwrap_err();
        assert!(matches!(error, HttpTargetError::InsecureTransport(_)));
    }

    #[test]
    fn cleartext_http_to_loopback_needs_the_relaxation() {
        // Without the knob the host is refused as a destination first, which
        // is the more specific answer; with it, cleartext to loopback is the
        // one case that is allowed through.
        let strict = policy("127.0.0.0/8,localhost");
        let error = validate_target_url("http://127.0.0.1:8080/hook", &strict).unwrap_err();
        assert!(matches!(error, HttpTargetError::HostRefused(_)));

        let relaxed = EgressPolicy::new(allow("127.0.0.0/8,::1,localhost"), true);
        for url in [
            "http://127.0.0.1:8080/hook",
            "http://[::1]:8080/hook",
            "http://localhost:8080/hook",
            "http://LocalHost.:8080/hook",
        ] {
            assert!(
                validate_target_url(url, &relaxed).is_ok(),
                "{url} is a same-host sidecar, which the relaxation exists for"
            );
        }
    }

    #[test]
    fn https_is_accepted_wherever_the_allowlist_permits_the_host() {
        // The mirror of the two tests above: the transport rule constrains
        // `http` only, and never narrows what `https` may reach.
        let policy = policy("api.example.com");
        assert!(validate_target_url("https://api.example.com/hook", &policy).is_ok());
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

    #[test]
    fn an_ended_dispatch_tells_only_its_own_lease_why() {
        let mut ended = EndedDispatches::default();
        ended.record("cancelled", Some(Lease::from_epoch(7)), Ending::Cancelled);
        ended.record("lost", Some(Lease::from_epoch(7)), Ending::Superseded);

        assert!(matches!(
            ended.refusal("cancelled", &Lease::from_epoch(7)),
            SettleRefused::Cancelled
        ));
        assert!(matches!(
            ended.refusal("lost", &Lease::from_epoch(7)),
            SettleRefused::Fenced
        ));
        // Another lease learns nothing it could not have learned before.
        assert!(matches!(
            ended.refusal("cancelled", &Lease::from_epoch(8)),
            SettleRefused::NotHere
        ));
        assert!(matches!(
            ended.refusal("never-seen", &Lease::from_epoch(7)),
            SettleRefused::NotHere
        ));
    }

    #[test]
    fn a_job_that_ends_again_is_evicted_by_its_latest_ending() {
        let mut ended = EndedDispatches::default();
        ended.record("again", Some(Lease::from_epoch(1)), Ending::Superseded);
        for n in 0..ENDED_CAPACITY - 1 {
            ended.record(&format!("job-{n}"), None, Ending::Superseded);
        }
        // Re-dispatched and cancelled: the newest entry, not the oldest.
        ended.record("again", Some(Lease::from_epoch(2)), Ending::Cancelled);
        ended.record("one-more", None, Ending::Superseded);

        assert_eq!(ended.order.len(), ENDED_CAPACITY);
        assert!(matches!(
            ended.refusal("again", &Lease::from_epoch(2)),
            SettleRefused::Cancelled
        ));
    }

    #[test]
    fn the_ended_record_is_bounded_oldest_first() {
        let mut ended = EndedDispatches::default();
        for n in 0..=ENDED_CAPACITY {
            ended.record(&format!("job-{n}"), None, Ending::Cancelled);
        }
        assert_eq!(ended.by_job.len(), ENDED_CAPACITY);
        assert!(matches!(
            ended.refusal("job-0", &Lease::from_epoch(1)),
            SettleRefused::NotHere
        ));
        assert!(matches!(
            ended.refusal(&format!("job-{ENDED_CAPACITY}"), &Lease::from_epoch(1)),
            SettleRefused::Cancelled
        ));
    }
}
