//! What goes on the wire for one push dispatch, and how a target's answer is
//! read back.
//!
//! The request body is the job's payload verbatim — the same tagged wire
//! envelope the attach protocol carries as a `job` frame's payload,
//! unchanged. Everything else an executor would otherwise read off that frame
//! — id, attempt, task, queue, namespace, the lease, the deadline, disabled
//! middleware, metadata — travels as a header instead: HTTP already has a
//! place for it, and there is no frame to carry it in.

// `classify`, `into_result`, and `refusal_result` are not re-exported by
// `super` — only the wire constants and `idempotency_key` are — because the
// dispatcher that calls them arrives in a later commit. Until then this
// file's own tests are their only caller, which rustc's dead-code analysis
// does not treat as a live root for the non-test build.
#![allow(dead_code)]

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
/// Milliseconds until the job's own execution deadline, at dispatch time.
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
    /// The target did not answer within the configured deadline.
    Deadline(Duration),
    /// The request could not be sent, or the connection failed outright.
    Transport(String),
    /// No retry budget remained for this dispatch.
    NoBudget,
}

impl Refusal {
    /// Whether the job should be retried on the existing backoff.
    pub fn should_retry(&self) -> bool {
        matches!(
            self,
            Refusal::ServerError(_)
                | Refusal::Deadline(_)
                | Refusal::Transport(_)
                | Refusal::NoBudget
                | Refusal::ClientError(408 | 425 | 429)
        )
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
            Refusal::Deadline(after) => {
                format!("target {target} did not answer within {after:?}")
            }
            Refusal::Transport(error) => {
                format!("target {target} could not be reached: {error}")
            }
            Refusal::NoBudget => {
                format!("target {target}: no retry budget remained for this dispatch")
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
        let trimmed = outcome.map(str::trim);
        return match trimmed.map(str::to_ascii_lowercase).as_deref() {
            Some("success") => Ok(Outcome::Success),
            Some("failure") => Ok(Outcome::Failure {
                should_retry: parse_retry(retry),
            }),
            Some("cancelled") => Ok(Outcome::Cancelled),
            Some("slept") => Err(Refusal::SleptRefused),
            None => Err(Refusal::MissingOutcome),
            Some(_) => Err(Refusal::UnknownOutcome(
                trimmed
                    .expect("Some(_) above implies trimmed is Some")
                    .to_string(),
            )),
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
/// Every call site here builds `Success`, `Failure`, or `Cancelled` — the
/// three variants [`ExecutorMessage::into_job_result`] always resolves — so
/// its `None` arm (the handshake and side-channel frames) never fires.
fn settle(message: ExecutorMessage, payload: Vec<u8>) -> JobResult {
    message
        .into_job_result(payload)
        .expect("push dispatch only ever builds a frame that settles a job here")
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
    fn a_refusal_message_never_carries_a_response_body() {
        // `Refusal` carries no response body at all, and neither `message`
        // nor `refusal_result` takes one as an argument — the signature is
        // the guarantee. This checks what the settled error carries instead:
        // the number it is complaining about.
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
