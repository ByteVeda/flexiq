//! One push dispatch attempt: the budget it gets, the request it builds, and
//! the single result it settles.
//!
//! Split out from `mod.rs` because this is the half with the concurrency in
//! it. Everything here runs inside one spawned task holding one semaphore
//! permit, and every path out of [`run_one`] either sends exactly one
//! [`JobResult`] or is dropped by the lease re-check — see the module doc on
//! the parent for why that invariant is the whole design.
//!
//! The invariant is about a job that *reaches* [`run_one`]. The parent's doc
//! names the three shutdown-only cases where one does not, each of which
//! leaves a lease to the stale-job reap.

use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use crossbeam_channel::Sender;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use tokio::sync::{watch, Notify, OwnedSemaphorePermit};

use super::contract::{
    self, Disposition, Outcome, Refusal, ENVELOPE_CONTENT_TYPE, HDR_ATTEMPT, HDR_DEADLINE_MS,
    HDR_DISABLED_MIDDLEWARE, HDR_IDEMPOTENCY_KEY, HDR_JOB_ID, HDR_LEASE, HDR_MAX_ATTEMPTS,
    HDR_METADATA, HDR_NAMESPACE, HDR_OUTCOME, HDR_PROTOCOL_VERSION, HDR_QUEUE, HDR_RETRY, HDR_TASK,
};
use super::Shared;
use crate::http::{read_bounded, BodyRead, SigningRequest};
use crate::job::{now_millis, Job};
use crate::lease::Lease;
use crate::scheduler::JobResult;
use crate::storage::records::{SettleClaimant, SettleGrant};
use crate::worker::protocol::{Dispatch, PROTOCOL_VERSION};
use crate::worker::SideChannel;

/// `Content-Type` header name. Spelled rather than taken from
/// `reqwest::header::CONTENT_TYPE` so it reaches [`put`] as the same
/// `&'static str` every other header does, and so a failure names it the way
/// it appears on the wire.
const HDR_CONTENT_TYPE: &str = "content-type";
/// `User-Agent` header name, for the same reason as [`HDR_CONTENT_TYPE`].
const HDR_USER_AGENT: &str = "user-agent";

/// Margin between the dispatcher's own deadline and the scheduler's stale-job
/// reap, so the settlement comes from the thing that knows what happened.
pub(super) const REAP_MARGIN: Duration = Duration::from_millis(500);

/// How long this request may take.
///
/// Measured from `job.started_at`, which is what `reap_stale_jobs` measures
/// from (`started_at + timeout_ms < now`) — a budget re-based on when the
/// permit was acquired would hand the reaper a job the dispatcher is still
/// waiting on.
///
/// `None` means there is no time left to dial at all.
pub(super) fn request_budget(job: &Job, now_ms: i64, ceiling: Duration) -> Option<Duration> {
    // No execution timeout means nothing for the reaper to fire on, so the
    // target's own ceiling is the only bound there is.
    if job.timeout_ms <= 0 {
        return Some(ceiling);
    }

    // A job the scheduler is dispatching has been stamped, but the field is
    // nullable; "unset" means the attempt is starting now, which is the value
    // the reaper would compare against a moment later anyway.
    let started_at = job.started_at.unwrap_or(now_ms);
    let margin = i64::try_from(REAP_MARGIN.as_millis()).unwrap_or(i64::MAX);
    let remaining = started_at
        .saturating_add(job.timeout_ms)
        .saturating_sub(margin)
        .saturating_sub(now_ms);
    if remaining <= 0 {
        return None;
    }

    // `remaining` is positive here, so the cast cannot wrap.
    Some(ceiling.min(Duration::from_millis(remaining as u64)))
}

/// Dispatch one job and settle it exactly once.
pub(super) async fn run_one(
    shared: Arc<Shared>,
    job: Job,
    permit: OwnedSemaphorePermit,
    result_tx: Sender<JobResult>,
) {
    // Held for the whole attempt: a slot is free only once the target has
    // answered, been given up on, or been abandoned.
    let _permit = permit;
    let started = Instant::now();

    // Both established before the first `.await`, and deliberately: they are
    // what bound everything below. A cancel arriving while the toggle list is
    // still being resolved has somewhere to land, and an abandon signalled
    // before this attempt reached the `select!` is still observed — a
    // `watch` receiver sees the value it subscribed to, not only later
    // changes.
    let notify = shared.register_cancel(&job.id);
    let abandon = shared.abandon_signal();

    // Read synchronously, before anything can suspend: the lease this
    // dispatch was made under has to be known even on a path that gives up
    // before a request is built, because the re-check below needs it.
    let lease = shared.lease_book().and_then(|book| book.current(&job.id));
    let budget = request_budget(&job, now_millis(), shared.config.request_timeout);
    let mut dispatch = Dispatch {
        job,
        disabled_middleware: Vec::new(),
        lease,
    };

    let settled = match budget {
        // Not dialled at all: the reaper is about to take this job, and a
        // request started now would answer into a claim somebody else holds.
        None => Err(Refusal::NoBudget),
        Some(budget) => guarded_attempt(&shared, &mut dispatch, &notify, abandon, budget).await,
    };

    // A `202` ends the request, not the attempt. This task keeps its permit
    // and swaps what it is waiting on — so the "exactly one result per job"
    // invariant above holds unchanged, and capacity keeps meaning "jobs
    // outstanding at this target", which is the only backpressure push has.
    let settled = match settled {
        Ok((Disposition::Accepted, _)) => {
            match await_settlement(&shared, &dispatch, &notify).await {
                // Nobody else won the marker, so this task still owes exactly
                // one result — whatever it was that ended the wait.
                Some(settlement) => settlement,
                // Someone else consumed the marker and emitted for us. Owing
                // nothing is the one correct contribution a loser can make.
                None => {
                    shared.unregister_cancel(&dispatch.job.id, &notify);
                    shared.unregister_accepted(&dispatch.job.id, dispatch.lease.as_ref());
                    return;
                }
            }
        }
        Ok((Disposition::Settled(outcome), body)) => Ok((outcome, body)),
        Err(refusal) => Err(refusal),
    };
    shared.unregister_cancel(&dispatch.job.id, &notify);
    shared.unregister_accepted(&dispatch.job.id, dispatch.lease.as_ref());

    let wall_time_ns = i64::try_from(started.elapsed().as_nanos()).unwrap_or(i64::MAX);

    // The last gate before anything is emitted. On disagreement the result is
    // dropped: someone else already settled this job, and emitting would
    // settle it twice.
    if !shared.dispatch_is_current(&dispatch.job.id, dispatch.lease.as_ref()) {
        log::error!(
            "[flexiq] push target {} answered for job {} under a lease that is no longer \
             current; refusing it — the job was re-dispatched while that attempt was still \
             running",
            shared.target,
            dispatch.job.id
        );
        return;
    }

    let result = match settled {
        Ok((outcome, body)) => contract::into_result(&dispatch.job, outcome, body, wall_time_ns),
        Err(refusal) => {
            contract::refusal_result(&dispatch.job, &refusal, &shared.target, wall_time_ns)
        }
    };
    let _ = result_tx.send(result);
}

/// Resolve the middleware this dispatch should say is disabled.
///
/// On `spawn_blocking` for the same reason `RemoteDispatcher::resolve_toggles`
/// is: a cache miss is a settings read, and this runs on the runtime the
/// scheduler task shares, so a slow settings backend must not be able to stall
/// it. An empty list when there is no side channel — an embedder with no
/// dashboard has no toggles to honour.
async fn resolve_toggles(shared: &Shared, task_name: &str) -> Vec<String> {
    let Some(channel) = shared.config.side_channel.as_ref() else {
        return Vec::new();
    };
    let channel = Arc::clone(channel);
    let task_name = task_name.to_string();
    tokio::task::spawn_blocking(move || channel.disabled_middleware(&task_name))
        .await
        .unwrap_or_else(|error| {
            log::warn!(
                "[flexiq] resolving the middleware disable list panicked ({error}); \
                 dispatching with none disabled"
            );
            Vec::new()
        })
}

/// Race the whole attempt against the budget, the cancel and the abandon
/// signal.
///
/// Every suspension point the attempt has is inside this `select!`, and that
/// is the point. Resolving the toggle list is a settings read, and signing
/// dials a credential endpoint through a client that carries a *connect*
/// timeout and deliberately no request timeout — so an endpoint that completes
/// the handshake and then goes quiet would, if either ran outside the race,
/// suspend this attempt for the life of the process: no `JobResult`, the
/// semaphore permit never released, and the shutdown drain waiting on a task
/// that will never finish. A hang is strictly worse than a failure.
async fn guarded_attempt(
    shared: &Shared,
    dispatch: &mut Dispatch,
    notify: &Notify,
    mut abandon: watch::Receiver<bool>,
    budget: Duration,
) -> Result<(Disposition, Vec<u8>), Refusal> {
    tokio::select! {
        // Biased, with the attempt first: when the target has already
        // answered, that answer is real information and beats a deadline or a
        // cancel that became ready in the same poll.
        biased;
        attempted = attempt(shared, dispatch, budget) => attempted,
        () = notify.notified() => Ok((Disposition::Settled(Outcome::Cancelled), Vec::new())),
        // The drain budget expired. Settled, not aborted: an aborted task
        // emits nothing, and a job with no result is a lease nobody retires.
        _ = abandon.wait_for(|abandoned| *abandoned) => Err(Refusal::Abandoned),
        () = tokio::time::sleep(budget) => Err(Refusal::Deadline(budget)),
    }
}

/// Wait for a dispatch the target accepted to be settled from outside the
/// request that carried it.
///
/// Returns `None` when somebody else won the settle marker, in which case this
/// attempt emits nothing at all. Returns `Some(settlement)` when *this* task
/// won it and therefore still owes exactly one result — an outcome for a
/// cancel, a refusal for a drain or a deadline.
///
/// The race is arbitrated durably, never here. Two claimants can take the
/// marker: a `Settle` reaching this replica, and this process giving the
/// dispatch up — below, or in the stale-job reaper after a restart. A `Settle`
/// that reached a *different* replica is not one of them: it is refused as
/// `NotHere` and consumes nothing, because the permit and the result channel
/// live in the process that dispatched.
async fn await_settlement(
    shared: &Arc<Shared>,
    dispatch: &Dispatch,
    notify: &Notify,
) -> Option<Result<(Outcome, Vec<u8>), Refusal>> {
    let job = &dispatch.job;
    let namespace = job.namespace.as_deref();

    let Some(channel) = shared.side_channel().cloned() else {
        // Refused at config time, so reaching this means the config check and
        // this path disagree. Fail the job rather than wait on a marker that
        // was never written.
        return Some(Err(Refusal::Accepted202));
    };
    let Some(owner) = shared.claim_owner() else {
        return Some(Err(Refusal::Accepted202));
    };

    // Registered before the marker is written: a `Settle` racing the write
    // finds the entry and is refused on the fence, which is recoverable. The
    // other order loses the settle entirely.
    let relieved = shared.register_accepted(job, dispatch.lease.clone());

    let epoch = dispatch.lease.as_ref().and_then(Lease::epoch);
    // The deadline starts at the job's own, which is what the reaper measures
    // against; `ExtendLease` is the only thing that moves it.
    let deadline_ms = job
        .started_at
        .unwrap_or_else(now_millis)
        .saturating_add(job.timeout_ms);
    let recorded = blocking_settle(&channel, {
        let job_id = job.id.clone();
        let owner = owner.clone();
        let namespace = namespace.map(str::to_owned);
        let attempt = job.retry_count;
        move |channel| {
            channel.await_settle(
                &job_id,
                &owner,
                attempt,
                epoch,
                deadline_ms,
                namespace.as_deref(),
            )
        }
    })
    .await;

    match recorded {
        Ok(Some(_)) => {}
        // The attempt was superseded between the dispatch and the answer. It
        // may not buy itself time, and it may not settle either.
        Ok(None) => return None,
        Err(error) => {
            log::error!(
                "[flexiq] could not record that push target {} accepted job {}: {error}",
                shared.target,
                job.id
            );
            return Some(Err(Refusal::Accepted202));
        }
    }

    // Only now may a callback be fenced. Until the marker is durable a report
    // is answered "not ready, try again" rather than "fenced, do not retry":
    // the target beat our own bookkeeping, and refusing it for good would
    // throw away a result nothing had actually refused, leaving this attempt
    // to wait out a deadline whose answer had already arrived.
    shared.mark_accepted_ready(&job.id);

    let mut abandon = shared.abandon_signal();
    // Two things travel together here and must not be conflated: the
    // *claimant*, which is the authority to take the marker, and the
    // *settlement*, which is what the job ends up as. A cancel and a drain
    // are the same authority — this process giving the dispatch up on
    // purpose, regardless of its deadline — and different settlements.
    let (claimant, settlement) = tokio::select! {
        biased;
        // Somebody settled it. Nothing left to consume and nothing to emit.
        _ = relieved => return None,
        // An operator cancelled. Settles `Cancelled`, exactly as a cancel
        // does before the target accepted — the per-topology table in this
        // module's docs promises one answer for push, not one per window.
        () = notify.notified() => (
            SettleClaimant::Abandoned,
            Ok((Outcome::Cancelled, Vec::new())),
        ),
        // The drain ran out. Retryable: the job was not cancelled, this
        // process simply stopped being able to wait for it.
        _ = abandon.wait_for(|abandoned| *abandoned) => (
            SettleClaimant::Abandoned,
            Err(Refusal::Abandoned),
        ),
        () = sleep_until_deadline(deadline_ms) => (
            SettleClaimant::Expired { now: now_millis() },
            Err(Refusal::AcceptedNotSettled),
        ),
    };

    let consumed = blocking_settle(&channel, {
        let job_id = job.id.clone();
        let namespace = namespace.map(str::to_owned);
        move |channel| channel.claim_settle(&job_id, claimant, namespace.as_deref())
    })
    .await;

    match consumed {
        // This task won the marker, so it owes the one result.
        Ok(SettleGrant::Granted) => Some(settlement),
        // A `Settle` landed in the gap between the timer firing and the
        // consume. It settled the job; this task has nothing to add.
        Ok(SettleGrant::Refused) => None,
        Err(error) => {
            // Fail closed. Emitting a timeout without having won the marker is
            // how one dispatch gets two outcomes, which is the single thing
            // this whole path exists to prevent. The reaper recovers the job.
            log::error!(
                "[flexiq] could not resolve the settle fence for job {}: {error}; leaving it to \
                 the stale-job reaper",
                job.id
            );
            None
        }
    }
}

/// Run one synchronous side-channel call off the runtime.
///
/// Both settle calls reach a Diesel `write_transaction` or a Redis script, and
/// this task runs on the runtime every dispatch request shares — the same
/// argument [`resolve_toggles`] makes for a settings read. A slow or bursting
/// backend must not be able to occupy runtime workers other dispatches need.
async fn blocking_settle<T, F>(channel: &Arc<dyn SideChannel>, work: F) -> crate::error::Result<T>
where
    F: FnOnce(&Arc<dyn SideChannel>) -> crate::error::Result<T> + Send + 'static,
    T: Send + 'static,
{
    let channel = Arc::clone(channel);
    tokio::task::spawn_blocking(move || work(&channel))
        .await
        .unwrap_or_else(|error| {
            // Reported as a failure rather than unwrapped: this is the fence,
            // and a panicking one must refuse rather than be taken at its word.
            Err(crate::error::QueueError::Config(format!(
                "the settle fence could not be evaluated: {error}"
            )))
        })
}

/// Sleep until a wall-clock deadline, re-derived from the clock the reaper
/// reads rather than from a duration captured earlier.
///
/// An accepted dispatch can wait for hours, and a `Duration` computed once at
/// the start would drift against `now_millis` across a suspend. Already past
/// resolves immediately.
async fn sleep_until_deadline(deadline_ms: i64) {
    loop {
        let remaining = deadline_ms.saturating_sub(now_millis());
        if remaining <= 0 {
            return;
        }
        // Capped per iteration so a far-future deadline is re-checked against
        // the wall clock rather than trusted to one long timer.
        let step = Duration::from_millis(remaining.min(60_000) as u64);
        tokio::time::sleep(step).await;
    }
}

/// Everything between "there is a dispatch to make" and "there is something to
/// settle", in the order the brief fixes: toggles, size, headers, signature,
/// then the exchange.
///
/// Every `.await` below is bounded by [`guarded_attempt`]'s `select!`, which
/// is the only reason it is safe for any of them to have no timeout of its own.
async fn attempt(
    shared: &Shared,
    dispatch: &mut Dispatch,
    budget: Duration,
) -> Result<(Disposition, Vec<u8>), Refusal> {
    dispatch.disabled_middleware = resolve_toggles(shared, &dispatch.job.task_name).await;

    let len = dispatch.job.payload.len();
    let cap = shared.config.max_request_bytes;
    if len > cap {
        return Err(Refusal::RequestTooLarge { len, cap });
    }

    let mut headers = dispatch_headers(shared, dispatch, budget)?;

    if let Some(signer) = shared.signer.as_ref() {
        // Signed after every other header is set, and nothing is added
        // afterwards but the signer's own output: a scheme that covers
        // headers signs what it was given, and a header added later makes a
        // correct receiver canonicalise a different request and reject a
        // valid signature. SigV4 is the scheme that reads this map — every
        // header in it lands in `SignedHeaders`, `x-flexiq-*` included. HMAC
        // covers six fixed fields and none of these, so a target verifying
        // HMAC has nothing authenticating which job it was handed.
        let signed = signer
            .sign(&SigningRequest {
                method: "POST",
                url: &shared.url,
                body: &dispatch.job.payload,
                headers: &headers,
            })
            .await
            .map_err(|error| {
                // Safe to render whole, here and in the job error below:
                // `AuthError` carries no URL and no endpoint response body —
                // its `Transport` variant is built from
                // `reqwest::Error::without_url`, which is that variant's
                // documented invariant, and every other variant is built from
                // a closed set of literals.
                log::warn!(
                    "[flexiq] push target {}: {} could not sign the dispatch for job {}: {error}",
                    shared.target,
                    signer.scheme(),
                    dispatch.job.id
                );
                Refusal::Signing {
                    scheme: signer.scheme(),
                    retryable: error.retryable(),
                    reason: error.to_string(),
                }
            })?;
        headers.extend(signed);
    }

    // Moved, not cloned: nothing downstream reads the payload again —
    // `into_result` and `refusal_result` settle from the job's identity, its
    // attempt and its task name — and a payload may be megabytes.
    let body = std::mem::take(&mut dispatch.job.payload);
    let request = shared
        .client
        .inner()
        .post(shared.url.clone())
        .headers(headers)
        .body(body);

    exchange(shared, request).await
}

/// Send the request and read the answer back.
async fn exchange(
    shared: &Shared,
    request: reqwest::RequestBuilder,
) -> Result<(Disposition, Vec<u8>), Refusal> {
    let response = request.send().await.map_err(|error| {
        // `without_url`: `Refusal::message` already names the target, and the
        // URL reqwest would otherwise interpolate is the one place an
        // operator's query-string credential could reach a stored job error.
        Refusal::Transport(error.without_url().to_string())
    })?;

    let status = response.status().as_u16();
    // Read off the response before `read_bounded` consumes it.
    let outcome = header_str(response.headers(), HDR_OUTCOME);
    let retry = header_str(response.headers(), HDR_RETRY);

    // Classified before the body is read, and the response dropped unread on
    // a refusal: a status refusal is complete information no body can change,
    // and a 5xx with an oversized body has to stay a retryable `ServerError`
    // rather than become a fatal `ResponseTooLarge`.
    let disposition = contract::classify(
        status,
        outcome.as_deref(),
        retry.as_deref(),
        shared.config.settle_callbacks,
    )?;

    let cap = shared.config.max_response_bytes;
    // Both failure arms refuse rather than store short: a partial body is not
    // what the target said, and a result assembled from half of it would be
    // wrong rather than incomplete. They stay apart because their retry
    // decisions differ — an oversized body is the target's, and will be
    // oversized again; a broken connection is the network's, and may not be.
    let body = match read_bounded(response, cap).await {
        BodyRead::Complete(body) => body,
        BodyRead::Truncated => return Err(Refusal::ResponseTooLarge { cap }),
        BodyRead::Broken(error) => return Err(Refusal::ResponseIncomplete(error)),
    };
    Ok((disposition, body))
}

/// One header's value as an owned `String`, or `None` when it is absent or
/// not valid ASCII.
fn header_str(headers: &HeaderMap, name: &str) -> Option<String> {
    headers.get(name)?.to_str().ok().map(str::to_string)
}

/// Everything an executor would otherwise read off a `job` frame, as headers.
fn dispatch_headers(
    shared: &Shared,
    dispatch: &Dispatch,
    budget: Duration,
) -> Result<HeaderMap, Refusal> {
    let job = &dispatch.job;
    let mut headers = HeaderMap::new();

    put(&mut headers, HDR_CONTENT_TYPE, ENVELOPE_CONTENT_TYPE, false)?;
    put(
        &mut headers,
        HDR_USER_AGENT,
        &shared.config.user_agent,
        false,
    )?;
    put(
        &mut headers,
        HDR_PROTOCOL_VERSION,
        &PROTOCOL_VERSION.to_string(),
        false,
    )?;
    put(&mut headers, HDR_JOB_ID, &job.id, false)?;
    put(
        &mut headers,
        HDR_ATTEMPT,
        &job.retry_count.to_string(),
        false,
    )?;
    put(
        &mut headers,
        HDR_MAX_ATTEMPTS,
        &job.max_retries.to_string(),
        false,
    )?;
    put(&mut headers, HDR_TASK, &job.task_name, false)?;
    put(&mut headers, HDR_QUEUE, &job.queue, false)?;
    // Absent for the default namespace, which is what `HDR_NAMESPACE`
    // documents: a header naming it would have to invent a spelling for
    // "none" that every receiver then has to agree with.
    if let Some(namespace) = job.namespace.as_ref() {
        put(&mut headers, HDR_NAMESPACE, namespace, false)?;
    }
    if let Some(lease) = dispatch.lease.as_ref() {
        // Sensitive: a lease is what authorizes a completion, so a log line
        // rendering this map must print `Sensitive` rather than the token.
        put(
            &mut headers,
            HDR_LEASE,
            &String::from_utf8_lossy(lease.as_bytes()),
            true,
        )?;
    }
    // Sensitive for the same reason, unconditionally: the key ends in the
    // lease token whenever there is one, and a rule that holds only sometimes
    // is a rule that gets the other case wrong.
    put(
        &mut headers,
        HDR_IDEMPOTENCY_KEY,
        &contract::idempotency_key(dispatch),
        true,
    )?;
    put(
        &mut headers,
        HDR_DEADLINE_MS,
        &budget.as_millis().to_string(),
        false,
    )?;
    if !dispatch.disabled_middleware.is_empty() {
        put(
            &mut headers,
            HDR_DISABLED_MIDDLEWARE,
            &dispatch.disabled_middleware.join(","),
            false,
        )?;
    }
    if let Some(metadata) = job.metadata.as_ref() {
        // base64url without padding: the blob is arbitrary JSON, and a header
        // value is a far narrower alphabet than that.
        let encoded = URL_SAFE_NO_PAD.encode(metadata);
        let cap = shared.config.max_metadata_header_bytes;
        if encoded.len() > cap {
            // Dropped, not fatal: metadata is advisory input to middleware,
            // which is not worth failing a job over.
            log::warn!(
                "[flexiq] push target {}: dropping {HDR_METADATA} for job {} — {} encoded \
                 bytes exceeds the {cap} byte cap; middleware sees no metadata for this \
                 dispatch",
                shared.target,
                job.id,
                encoded.len()
            );
        } else {
            put(&mut headers, HDR_METADATA, &encoded, false)?;
        }
    }

    Ok(headers)
}

/// Set one header, refusing rather than panicking on a value that cannot be
/// rendered.
///
/// `sensitive` is not advisory: it is what makes `{headers:?}` print
/// `Sensitive` in place of the value, which is the only thing standing between
/// a dispatch log and a lease somebody else can settle a job with.
fn put(
    headers: &mut HeaderMap,
    name: &'static str,
    value: &str,
    sensitive: bool,
) -> Result<(), Refusal> {
    let header_name =
        HeaderName::from_bytes(name.as_bytes()).map_err(|_| Refusal::Header { name })?;
    let mut header_value = HeaderValue::from_str(value).map_err(|_| Refusal::Header { name })?;
    header_value.set_sensitive(sensitive);
    headers.insert(header_name, header_value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::JobStatus;

    /// A running job started at `started_at` with a `timeout_ms` execution
    /// timeout, which is all `request_budget` reads.
    fn a_job(started_at: Option<i64>, timeout_ms: i64) -> Job {
        Job {
            id: "job-1".into(),
            queue: "default".into(),
            task_name: "resize".into(),
            payload: Vec::new(),
            status: JobStatus::Running,
            priority: 0,
            created_at: 0,
            scheduled_at: 0,
            started_at,
            completed_at: None,
            retry_count: 0,
            max_retries: 3,
            result: None,
            error: None,
            timeout_ms,
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

    #[test]
    fn the_budget_is_the_smaller_of_the_ceiling_and_what_is_left_of_the_job_timeout() {
        let ceiling = Duration::from_secs(60);

        // A 30s job that has just started: the job's timeout is the smaller
        // of the two, so it is what bounds the request.
        let job = a_job(Some(1_000), 30_000);
        let budget = request_budget(&job, 1_000, ceiling).expect("30s of timeout is time enough");
        assert_eq!(budget, Duration::from_millis(30_000 - 500));

        // A 10-minute job: now the target's own ceiling is the smaller.
        let long = a_job(Some(1_000), 600_000);
        assert_eq!(
            request_budget(&long, 1_000, ceiling),
            Some(ceiling),
            "the ceiling bounds a job whose timeout is further away than it is"
        );
    }

    #[test]
    fn a_job_whose_timeout_already_elapsed_gets_no_request() {
        // `reap_stale_jobs` fires on `started_at + timeout_ms < now`, so this
        // job is already the reaper's.
        let job = a_job(Some(1_000), 5_000);
        assert_eq!(request_budget(&job, 10_000, Duration::from_secs(60)), None);

        // And inside the margin, where the reap is imminent rather than past.
        assert_eq!(request_budget(&job, 5_800, Duration::from_secs(60)), None);
    }

    #[test]
    fn a_job_with_no_timeout_gets_the_target_ceiling() {
        let ceiling = Duration::from_secs(45);
        for timeout_ms in [0, -1] {
            let job = a_job(Some(1_000), timeout_ms);
            assert_eq!(
                request_budget(&job, 1_000, ceiling),
                Some(ceiling),
                "timeout_ms {timeout_ms} leaves nothing for the reaper to fire on"
            );
        }
    }

    #[test]
    fn the_budget_leaves_the_reaper_a_margin() {
        // The property, not the arithmetic: whenever the job's own timeout is
        // what bounds the request, the request gives up at least `REAP_MARGIN`
        // before `reap_stale_jobs` would fire — so the settlement comes from
        // the dispatcher, which knows what happened, and not from the reaper,
        // which does not.
        let ceiling = Duration::from_secs(600);
        let started_at = 1_000;
        let timeout_ms = 30_000;
        let reaped_at = started_at + timeout_ms;

        for now_ms in [started_at, started_at + 1, started_at + 29_000] {
            let job = a_job(Some(started_at), timeout_ms);
            let budget = request_budget(&job, now_ms, ceiling).expect("time remains");
            let gives_up_at = now_ms + budget.as_millis() as i64;
            assert!(
                reaped_at - gives_up_at >= REAP_MARGIN.as_millis() as i64,
                "at now={now_ms} the request gives up at {gives_up_at}, leaving less than \
                 the margin before the reap at {reaped_at}"
            );
        }
    }
}
