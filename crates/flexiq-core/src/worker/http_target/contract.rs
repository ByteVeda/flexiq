//! What goes on the wire for one push dispatch, and how a target's answer is
//! read back.
//!
//! The request body is the job's payload verbatim — the same tagged wire
//! envelope the attach protocol carries as a `job` frame's payload,
//! unchanged. Everything else an executor would otherwise read off that frame
//! — id, attempt, task, queue, namespace, the lease, the deadline, disabled
//! middleware, metadata — travels as a header instead: HTTP already has a
//! place for it, and there is no frame to carry it in.

use std::time::Duration;

use crate::job::Job;
use crate::scheduler::JobResult;
use crate::worker::protocol::{Dispatch, ExecutorMessage};

/// Protocol version this dispatch speaks.
pub const HDR_PROTOCOL_VERSION: &str = "x-flexiq-protocol-version";
/// The job's id.
pub const HDR_JOB_ID: &str = "x-flexiq-job-id";
/// Retries already attempted before this dispatch.
pub const HDR_ATTEMPT: &str = "x-flexiq-attempt";
/// The job's retry cap.
pub const HDR_MAX_ATTEMPTS: &str = "x-flexiq-max-attempts";
/// The task to run.
pub const HDR_TASK: &str = "x-flexiq-task";
/// Queue the job came from.
pub const HDR_QUEUE: &str = "x-flexiq-queue";
/// Namespace the job is scoped to, when it is not the default one.
pub const HDR_NAMESPACE: &str = "x-flexiq-namespace";
/// The lease this dispatch was made under.
pub const HDR_LEASE: &str = "x-flexiq-lease";
/// [`idempotency_key`]'s value, for a target that dedupes on it.
pub const HDR_IDEMPOTENCY_KEY: &str = "x-flexiq-idempotency-key";
/// Milliseconds this dispatch will wait for an answer, at most.
///
/// The request budget, not the job's raw remaining timeout: it is the job's
/// own execution deadline less the reaper's margin, capped by the target's
/// configured request ceiling. Sending the raw deadline would promise a
/// target more time than the scheduler will actually wait for it, and an
/// answer that arrives after this is fenced out on arrival.
///
/// An upper bound rather than an exact window, deliberately advertised as
/// one: the guard's budget starts before the toggles are resolved and the
/// request is signed, so that work comes out of the same number — hundreds of
/// milliseconds when an identity token has to be fetched uncached.
pub const HDR_DEADLINE_MS: &str = "x-flexiq-deadline-ms";
/// Comma-separated middleware the operator has disabled for this task.
pub const HDR_DISABLED_MIDDLEWARE: &str = "x-flexiq-disabled-middleware";
/// The job's metadata blob, as stored.
pub const HDR_METADATA: &str = "x-flexiq-metadata";
/// What the target says happened: `success`, `failure`, `cancelled`, or
/// `slept`.
pub const HDR_OUTCOME: &str = "x-flexiq-outcome";
/// On a `failure` outcome, whether to retry: `true`/`1` or `false`/`0`.
pub const HDR_RETRY: &str = "x-flexiq-retry";
/// Content type of the request body: the tagged wire envelope, unchanged.
pub const ENVELOPE_CONTENT_TYPE: &str = "application/vnd.flexiq.envelope";
/// Prefix of the error a 202 dead-letters with, so an operator can grep for it.
pub const ACCEPTED_NOT_SETTLED: &str = "push.accepted_not_settled";

/// The key a target dedupes on: `<job id>.<attempt>.<lease>`, or
/// `<job id>.<attempt>` when the scheduler held no lease.
///
/// The durable fence is `(owner, attempt, epoch)` and the lease is the epoch
/// rendered opaque. `owner` is deliberately absent — no dispatch frame carries
/// it either, because an owner a peer holds is an owner it can forge.
///
/// Two requests with the same key are the same attempt of the same job under
/// the same claim, and a memoized answer is correct. Two that differ only in
/// the lease are two claims of one attempt — what a requeue produces — and are
/// distinct, because only one of their responses will be accepted.
pub fn idempotency_key(dispatch: &Dispatch) -> String {
    let job_id = &dispatch.job.id;
    let attempt = dispatch.job.retry_count;
    match &dispatch.lease {
        // `Lease` has no `Display`, and its `Debug` is redacted
        // (`<redacted>`) — building this from `{lease:?}` would silently
        // collapse every job onto the same key. `as_bytes` is the base64url
        // token itself, always ASCII, which is what actually varies.
        Some(lease) => format!(
            "{job_id}.{attempt}.{}",
            String::from_utf8_lossy(lease.as_bytes())
        ),
        None => format!("{job_id}.{attempt}"),
    }
}

/// What a target said happened, when it answered at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The task ran to completion.
    Success,
    /// The task raised, or the target reports it that way.
    Failure {
        /// Retry decision read from [`HDR_RETRY`]. `None` when the header was
        /// absent or unparseable.
        should_retry: Option<bool>,
    },
    /// The task observed a cancel and stopped.
    Cancelled,
}

/// Why a response is not an outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// The target answered 202 Accepted: it took the job but has not
    /// settled it, and a push dispatch has nothing further to wait on.
    Accepted202,
    /// The target answered with a redirect, which is never followed.
    Redirect(u16),
    /// The target answered with a 4xx. Not retryable, except the three
    /// status codes [`Refusal::should_retry`] names.
    ClientError(u16),
    /// The target answered with a 5xx, or a status outside 1xx-5xx.
    ServerError(u16),
    /// The target answered 2xx but sent no [`HDR_OUTCOME`] header.
    MissingOutcome,
    /// The target's [`HDR_OUTCOME`] value is not one this build recognizes.
    /// Bounded to a small, fixed length — this lands in `job_errors` and the
    /// dead-letter queue, and a header value is not capped by anything
    /// upstream of here.
    UnknownOutcome(String),
    /// The target answered with outcome `slept`; a push dispatch has no step
    /// session for it to resume.
    SleptRefused,
    /// The request would have exceeded the configured byte cap.
    RequestTooLarge {
        /// The request's actual length.
        len: usize,
        /// The configured cap it exceeded.
        cap: usize,
    },
    /// The response body exceeded the configured byte cap before it could be
    /// read in full.
    ResponseTooLarge {
        /// The configured cap it exceeded.
        cap: usize,
    },
    /// The connection broke part-way through the response body.
    ///
    /// Distinct from [`Refusal::Transport`], which is a request that never got
    /// a response at all, and from [`Refusal::ResponseTooLarge`], which is a
    /// target that answered with more than it was allowed to. Here the target
    /// answered, the status and headers were read, and the body stopped
    /// arriving. It is its own variant because it is the only one of the three
    /// that is both *retryable* and *not the target's fault* — folding it into
    /// `ResponseTooLarge` (which it once was) failed a job permanently, under
    /// a reason that was also untrue.
    ResponseIncomplete(String),
    /// The target did not answer within the configured deadline.
    Deadline(Duration),
    /// The request could not be sent, or the connection failed outright.
    Transport(String),
    /// No retry budget remained for this dispatch.
    NoBudget,
    /// The dispatcher stopped waiting: the request was still in flight when
    /// the shutdown drain budget expired.
    ///
    /// Distinct from [`Refusal::Deadline`], which is the *attempt's* own
    /// deadline — `min(request ceiling, job timeout less the reap margin)` —
    /// and is recorded as an execution timeout. Nothing timed out here; the
    /// scheduler went away. The job is retried with no timeout in its history.
    Abandoned,
    /// A value the dispatch has to carry is not a legal HTTP header value —
    /// a task, queue or namespace name with a control character in it, say.
    /// Names the header, never the value: the value is exactly the thing
    /// that could not be rendered safely.
    Header {
        /// Which header could not be built, from this module's own constants.
        name: &'static str,
    },
    /// The request could not be signed.
    ///
    /// Carries [`AuthError::retryable`](crate::http::AuthError::retryable)'s
    /// answer rather than re-deriving one: whether a credential failure is
    /// worth another attempt is the auth layer's decision, and a second
    /// decision here is a second place for the two to disagree.
    Signing {
        /// The scheme that refused, from `Signer::scheme` — a fixed literal,
        /// never interpolated from a credential endpoint's response.
        scheme: &'static str,
        /// The `AuthError`'s own message.
        reason: String,
        /// Whether signing is worth attempting again.
        retryable: bool,
    },
}

impl Refusal {
    /// Whether the job should be retried on the existing backoff.
    ///
    /// Matched exhaustively rather than through a `matches!` with a wildcard:
    /// a refusal added later has to state its retry decision here, in code,
    /// instead of inheriting a silent `false`.
    pub fn should_retry(&self) -> bool {
        match self {
            Refusal::ServerError(_)
            | Refusal::Deadline(_)
            | Refusal::Transport(_)
            | Refusal::ResponseIncomplete(_)
            | Refusal::NoBudget
            | Refusal::Abandoned => true,
            Refusal::ClientError(status) => matches!(status, 408 | 425 | 429),
            Refusal::Signing { retryable, .. } => *retryable,
            Refusal::Accepted202
            | Refusal::Redirect(_)
            | Refusal::MissingOutcome
            | Refusal::UnknownOutcome(_)
            | Refusal::SleptRefused
            | Refusal::RequestTooLarge { .. }
            | Refusal::ResponseTooLarge { .. }
            | Refusal::Header { .. } => false,
        }
    }

    /// Whether this was an execution timeout, which the job's history records
    /// separately from an ordinary failure.
    pub fn timed_out(&self) -> bool {
        matches!(self, Refusal::Deadline(_))
    }

    /// The message stored on the job. Carries the target's origin but never
    /// its credentials, and never a response body.
    pub fn message(&self, target: &str) -> String {
        match self {
            Refusal::Accepted202 => format!(
                "{ACCEPTED_NOT_SETTLED}: target {target} answered 202 Accepted; push dispatch \
                 treats an accepted-but-unsettled job as failed rather than waiting further \
                 (see #845)"
            ),
            Refusal::Redirect(status) => format!(
                "target {target} answered with a {status} redirect, which push dispatch does \
                 not follow"
            ),
            Refusal::ClientError(status) => {
                format!("target {target} answered with client error {status}")
            }
            Refusal::ServerError(status) => {
                format!("target {target} answered with server error {status}")
            }
            Refusal::MissingOutcome => {
                format!("target {target} answered 2xx but sent no {HDR_OUTCOME} header")
            }
            Refusal::UnknownOutcome(value) => {
                format!("target {target} sent an unrecognized {HDR_OUTCOME} value '{value}'")
            }
            Refusal::SleptRefused => format!(
                "target {target} answered outcome 'slept', but push dispatch has no step \
                 session to resume it in"
            ),
            Refusal::RequestTooLarge { len, cap } => {
                format!("target {target}: request of {len} bytes exceeds the {cap} byte cap")
            }
            Refusal::ResponseTooLarge { cap } => {
                format!("target {target}: response exceeded the {cap} byte cap")
            }
            Refusal::ResponseIncomplete(error) => {
                format!("target {target} answered, but the response body ended early: {error}")
            }
            Refusal::Deadline(after) => {
                format!("target {target} did not answer within {after:?}")
            }
            Refusal::Transport(error) => {
                format!("target {target} could not be reached: {error}")
            }
            Refusal::NoBudget => {
                format!("target {target}: no retry budget remained for this dispatch")
            }
            Refusal::Abandoned => format!(
                "target {target} was still working when the dispatcher's shutdown drain \
                 expired; the request was abandoned and the job will be retried"
            ),
            Refusal::Header { name } => {
                format!("target {target}: '{name}' could not be rendered as a header value")
            }
            Refusal::Signing { scheme, reason, .. } => {
                format!("target {target}: {scheme} could not sign the dispatch: {reason}")
            }
        }
    }
}

/// Read `retry`'s value as a bool: `true`/`1`, `false`/`0`, case-insensitive
/// and trimmed. `None` for anything absent or unparseable — never guessed.
fn parse_retry(retry: Option<&str>) -> Option<bool> {
    match retry?.trim().to_ascii_lowercase().as_str() {
        "true" | "1" => Some(true),
        "false" | "0" => Some(false),
        _ => None,
    }
}

/// Longest `x-flexiq-outcome` value echoed back in a [`Refusal::UnknownOutcome`].
/// Nothing upstream of `classify` bounds the header, and the value lands in
/// `job_errors` and the dead-letter queue — 64 characters is plenty to spot a
/// typo without storing an operator's misconfigured multi-kilobyte header.
const MAX_ECHOED_OUTCOME_CHARS: usize = 64;

/// Bound `value` to [`MAX_ECHOED_OUTCOME_CHARS`] characters, marking the
/// point of the cut so a truncated echo never reads as the header's full
/// value. Counts characters, not bytes, so a multi-byte UTF-8 sequence at the
/// boundary is never split.
fn bound_outcome_value(value: &str) -> String {
    if value.chars().count() <= MAX_ECHOED_OUTCOME_CHARS {
        return value.to_string();
    }
    let mut bounded: String = value.chars().take(MAX_ECHOED_OUTCOME_CHARS).collect();
    bounded.push_str("...(truncated)");
    bounded
}

/// Read a response's status and outcome header into an outcome or a refusal.
pub fn classify(
    status: u16,
    outcome: Option<&str>,
    retry: Option<&str>,
) -> Result<Outcome, Refusal> {
    // Checked before the 2xx arm below: 202 is itself in that range, but it
    // never settles a job, so it must never reach the outcome-header match.
    if status == 202 {
        return Err(Refusal::Accepted202);
    }
    if (200..300).contains(&status) {
        return match outcome.map(str::trim) {
            None => Err(Refusal::MissingOutcome),
            Some(value) => match value.to_ascii_lowercase().as_str() {
                "success" => Ok(Outcome::Success),
                "failure" => Ok(Outcome::Failure {
                    should_retry: parse_retry(retry),
                }),
                "cancelled" => Ok(Outcome::Cancelled),
                "slept" => Err(Refusal::SleptRefused),
                _ => Err(Refusal::UnknownOutcome(bound_outcome_value(value))),
            },
        };
    }
    if (300..400).contains(&status) {
        return Err(Refusal::Redirect(status));
    }
    if (400..500).contains(&status) {
        return Err(Refusal::ClientError(status));
    }
    // 5xx, and anything outside 1xx-5xx (a target must not answer with one,
    // but a refusal — not a panic — is the right response to one that does).
    Err(Refusal::ServerError(status))
}

/// Convert a frame this module just built into its settled [`JobResult`].
///
/// `into_job_result` returns `Some` for exactly `Success`, `Failure`, and
/// `Cancelled` — the only three variants this module ever builds — and
/// `None` only for the handshake and side-channel frames, none of which
/// originates here. The `expect` below rests on that invariant, not on
/// anything the caller controls.
fn settle(message: ExecutorMessage, payload: Vec<u8>) -> JobResult {
    message
        .into_job_result(payload)
        .expect("into_job_result returns None only for frames this module never builds")
}

/// Build the settled result for a target that answered.
///
/// Routed through `ExecutorMessage::into_job_result` rather than constructing
/// a `JobResult` directly: the push path and the attach path settle a job the
/// same way, and a second construction site is a second place for the two to
/// drift.
pub fn into_result(job: &Job, outcome: Outcome, body: Vec<u8>, wall_time_ns: i64) -> JobResult {
    match outcome {
        Outcome::Success => {
            // A zero-byte body is not a valid envelope — every envelope has a
            // tag byte — so "no result" and "an empty result" collapse over
            // HTTP; a target wanting a present-but-empty value sends CBOR
            // null (`02 f6`) instead of an empty body.
            let result_len = if body.is_empty() {
                None
            } else {
                Some(body.len())
            };
            settle(
                ExecutorMessage::Success {
                    job_id: job.id.clone(),
                    result_len,
                    task_name: job.task_name.clone(),
                    wall_time_ns,
                    lease: None,
                },
                body,
            )
        }
        Outcome::Failure { should_retry } => {
            // Lossy, not strict: a canonical `TaskError` JSON is valid UTF-8
            // and survives untouched, but a target's body is never allowed to
            // panic this path.
            let body_text = String::from_utf8_lossy(&body).into_owned();
            let (should_retry, error) = match should_retry {
                Some(should_retry) => (should_retry, body_text),
                None => (
                    true,
                    format!(
                        "{body_text}\n\nretried because the response carried no {HDR_RETRY} \
                         header"
                    ),
                ),
            };
            settle(
                ExecutorMessage::Failure {
                    job_id: job.id.clone(),
                    error,
                    retry_count: job.retry_count,
                    max_retries: job.max_retries,
                    task_name: job.task_name.clone(),
                    wall_time_ns,
                    should_retry,
                    timed_out: false,
                    lease: None,
                },
                Vec::new(),
            )
        }
        Outcome::Cancelled => settle(
            ExecutorMessage::Cancelled {
                job_id: job.id.clone(),
                task_name: job.task_name.clone(),
                wall_time_ns,
                lease: None,
            },
            Vec::new(),
        ),
    }
}

/// Build the settled result for a response that was not an outcome.
pub fn refusal_result(job: &Job, refusal: &Refusal, target: &str, wall_time_ns: i64) -> JobResult {
    settle(
        ExecutorMessage::Failure {
            job_id: job.id.clone(),
            error: refusal.message(target),
            retry_count: job.retry_count,
            max_retries: job.max_retries,
            task_name: job.task_name.clone(),
            wall_time_ns,
            should_retry: refusal.should_retry(),
            timed_out: refusal.timed_out(),
            lease: None,
        },
        Vec::new(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::JobStatus;
    use crate::lease::Lease;

    fn a_job() -> Job {
        Job {
            id: "job-1".into(),
            queue: "default".into(),
            task_name: "resize".into(),
            payload: Vec::new(),
            status: JobStatus::Running,
            priority: 0,
            created_at: 0,
            scheduled_at: 0,
            started_at: None,
            completed_at: None,
            retry_count: 2,
            max_retries: 5,
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

    fn dispatch_with_lease(lease: Option<Lease>) -> Dispatch {
        Dispatch {
            job: a_job(),
            disabled_middleware: Vec::new(),
            lease,
        }
    }

    #[test]
    fn the_idempotency_key_is_job_attempt_and_lease() {
        let lease = Lease::from_epoch(12345);
        let dispatch = dispatch_with_lease(Some(lease.clone()));

        let key = idempotency_key(&dispatch);

        let token = String::from_utf8_lossy(lease.as_bytes()).into_owned();
        assert_eq!(key, format!("job-1.2.{token}"));
        // The silent trap: `Lease`'s `Debug` is redacted, so a key built from
        // `{lease:?}` would read "job-1.2.<redacted>" for every job.
        assert!(!key.contains("redacted"));
        assert!(key.contains(&token));
    }

    #[test]
    fn a_leaseless_dispatch_has_a_two_field_key() {
        let dispatch = dispatch_with_lease(None);
        assert_eq!(idempotency_key(&dispatch), "job-1.2");
    }

    #[test]
    fn two_leases_are_two_keys_for_one_attempt() {
        let first = dispatch_with_lease(Some(Lease::from_epoch(1)));
        let second = dispatch_with_lease(Some(Lease::from_epoch(2)));
        assert_ne!(idempotency_key(&first), idempotency_key(&second));
    }

    #[test]
    fn a_202_is_refused_before_the_success_arm() {
        assert_eq!(
            classify(202, Some("success"), None),
            Err(Refusal::Accepted202)
        );
    }

    #[test]
    fn a_2xx_without_an_outcome_header_is_refused() {
        let error = classify(200, None, None).unwrap_err();
        assert_eq!(error, Refusal::MissingOutcome);
        assert!(error.message("http://target").contains(HDR_OUTCOME));
    }

    #[test]
    fn an_unknown_outcome_is_refused_and_echoes_what_it_saw() {
        let error = classify(200, Some("exploded"), None).unwrap_err();
        assert_eq!(error, Refusal::UnknownOutcome("exploded".to_string()));
        assert!(error.message("http://target").contains("exploded"));
    }

    #[test]
    fn an_unknown_outcome_longer_than_the_cap_is_truncated() {
        // Nothing upstream of `classify` bounds the header, and the echoed
        // value lands in `job_errors` and the dead-letter queue.
        let long_value = "x".repeat(500);
        let error = classify(200, Some(&long_value), None).unwrap_err();
        match &error {
            Refusal::UnknownOutcome(echoed) => {
                assert!(
                    echoed.chars().count() < long_value.chars().count(),
                    "a 500-character header value must not be stored whole"
                );
                assert!(echoed.contains("truncated"));
            }
            other => panic!("expected UnknownOutcome, got {other:?}"),
        }
    }

    #[test]
    fn a_slept_outcome_is_refused() {
        assert_eq!(
            classify(200, Some("slept"), None),
            Err(Refusal::SleptRefused)
        );
    }

    #[test]
    fn the_outcome_header_is_read_case_insensitively() {
        assert_eq!(classify(200, Some("SuCcEsS"), None), Ok(Outcome::Success));
        assert_eq!(
            classify(200, Some("  success  "), None),
            Ok(Outcome::Success)
        );
    }

    #[test]
    fn a_5xx_is_retryable_and_most_4xx_are_not() {
        let cases = [
            (500, true),
            (503, true),
            (400, false),
            (404, false),
            (408, true),
            (425, true),
            (429, true),
        ];
        for (status, retryable) in cases {
            let error = classify(status, Some("success"), None).unwrap_err();
            assert_eq!(
                error.should_retry(),
                retryable,
                "status {status} should_retry mismatch"
            );
        }
    }

    /// Regression: a connection that broke part-way through the response body
    /// used to arrive here as `ResponseTooLarge`, which is non-retryable — so
    /// a transient network failure permanently failed a job, under a reason
    /// that was also untrue. The two must not agree on either count.
    #[test]
    fn a_broken_response_body_is_retryable_where_an_oversized_one_is_not() {
        let broken = Refusal::ResponseIncomplete("connection closed".to_string());
        let oversized = Refusal::ResponseTooLarge { cap: 1024 };

        assert!(
            broken.should_retry(),
            "the body exceeded nothing; the connection went away"
        );
        assert!(
            !oversized.should_retry(),
            "a target that answered with too much will answer with too much again"
        );
        assert!(!broken.timed_out(), "nothing timed out");

        let message = broken.message("https://push.example.com");
        assert!(message.contains("https://push.example.com"), "{message}");
        assert!(
            !message.contains("cap"),
            "a broken read must not claim a cap was exceeded: {message}"
        );
    }

    #[test]
    fn a_redirect_is_refused_rather_than_followed() {
        assert_eq!(classify(302, None, None), Err(Refusal::Redirect(302)));
        assert!(!Refusal::Redirect(302).should_retry());
    }

    #[test]
    fn an_unparseable_retry_header_reads_as_absent() {
        assert_eq!(
            classify(200, Some("failure"), Some("maybe")),
            Ok(Outcome::Failure { should_retry: None })
        );
    }

    #[test]
    fn a_failure_without_the_retry_header_is_retried_and_says_so() {
        let job = a_job();
        let result = into_result(
            &job,
            Outcome::Failure { should_retry: None },
            b"boom".to_vec(),
            10,
        );
        match result {
            JobResult::Failure {
                should_retry,
                error,
                ..
            } => {
                assert!(should_retry);
                assert!(error.contains(HDR_RETRY));
            }
            _ => panic!("expected a failure"),
        }
    }

    #[test]
    fn a_failure_body_is_stored_verbatim() {
        let job = a_job();
        let body = br#"{"errtype":"ValueError","message":"x"}"#.to_vec();
        let result = into_result(
            &job,
            Outcome::Failure {
                should_retry: Some(false),
            },
            body.clone(),
            10,
        );
        match result {
            JobResult::Failure { error, .. } => assert_eq!(error.as_bytes(), body.as_slice()),
            _ => panic!("expected a failure"),
        }
    }

    #[test]
    fn an_empty_success_body_is_no_result_and_a_present_one_is_some() {
        let job = a_job();

        match into_result(&job, Outcome::Success, Vec::new(), 10) {
            JobResult::Success { result, .. } => assert_eq!(result, None),
            _ => panic!("expected a success"),
        }

        match into_result(&job, Outcome::Success, b"ok".to_vec(), 10) {
            JobResult::Success { result, .. } => assert_eq!(result, Some(b"ok".to_vec())),
            _ => panic!("expected a success"),
        }
    }

    #[test]
    fn the_response_never_supplies_the_task_name_or_the_attempt() {
        let mut job = a_job();
        job.task_name = "known-task".into();
        job.retry_count = 3;
        job.max_retries = 9;

        match into_result(&job, Outcome::Success, Vec::new(), 10) {
            JobResult::Success { task_name, .. } => assert_eq!(task_name, "known-task"),
            _ => panic!("expected a success"),
        }

        match into_result(
            &job,
            Outcome::Failure {
                should_retry: Some(true),
            },
            Vec::new(),
            10,
        ) {
            JobResult::Failure {
                task_name,
                retry_count,
                max_retries,
                ..
            } => {
                assert_eq!(task_name, "known-task");
                assert_eq!(retry_count, 3);
                assert_eq!(max_retries, 9);
            }
            _ => panic!("expected a failure"),
        }
    }

    #[test]
    fn a_refusal_message_names_the_status_or_cap_it_is_complaining_about() {
        // `Refusal` carries no response body at all, and neither `message`
        // nor `refusal_result` takes one as an argument — that's a property
        // of the type signature, not something a test can exercise. What
        // this checks is what the settled error carries instead: the number
        // it is complaining about, and the target.
        let job = a_job();
        let cases: [(Refusal, &str); 3] = [
            (Refusal::ServerError(503), "503"),
            (Refusal::ResponseTooLarge { cap: 1_048_576 }, "1048576"),
            (
                Refusal::RequestTooLarge {
                    len: 9_000_000,
                    cap: 8_388_608,
                },
                "9000000",
            ),
        ];
        for (refusal, needle) in cases {
            let result = refusal_result(&job, &refusal, "https://push.example.com/handler", 10);
            match result {
                JobResult::Failure { error, .. } => {
                    assert!(error.contains(needle), "{error} must name {needle}");
                    assert!(error.contains("https://push.example.com/handler"));
                }
                _ => panic!("expected a failure"),
            }
        }
    }

    #[test]
    fn an_abandoned_request_retries_without_recording_a_timeout() {
        // The job's own timeout is untouched by a scheduler shutting down, so
        // `timed_out` must stay false — a `Deadline` here would write a
        // timeout into the job's history that never happened.
        let refusal = Refusal::Abandoned;
        assert!(refusal.should_retry());
        assert!(!refusal.timed_out());
        assert!(refusal
            .message("https://push.example.com/handler")
            .contains("shutdown drain"));
    }

    #[test]
    fn a_header_refusal_names_the_header_and_never_retries() {
        // A task name that cannot be rendered as a header value does not
        // become renderable on a retry, so the job is failed outright rather
        // than dialled max_retries more times.
        let refusal = Refusal::Header { name: HDR_TASK };
        assert!(!refusal.should_retry());
        assert!(!refusal.timed_out());
        let message = refusal.message("https://push.example.com/handler");
        assert!(message.contains(HDR_TASK), "{message} must name the header");
    }

    #[test]
    fn a_signing_refusal_carries_the_auth_layers_retry_decision() {
        for retryable in [true, false] {
            let refusal = Refusal::Signing {
                scheme: "oidc",
                reason: "credential fetch failed".to_string(),
                retryable,
            };
            assert_eq!(refusal.should_retry(), retryable);
            let message = refusal.message("https://push.example.com/handler");
            assert!(message.contains("oidc"), "{message} must name the scheme");
            assert!(message.contains("credential fetch failed"));
        }
    }

    #[test]
    fn a_non_utf8_failure_body_does_not_panic() {
        let job = a_job();
        let body = vec![0xff, 0xfe, 0x00, 0xff];
        let result = into_result(
            &job,
            Outcome::Failure {
                should_retry: Some(false),
            },
            body,
            10,
        );
        assert!(matches!(result, JobResult::Failure { .. }));
    }
}
