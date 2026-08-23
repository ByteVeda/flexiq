//! Minting the key a step hands the *downstream* service.
//!
//! Pure: a run key read off the job, a step key from the sequence, and the
//! string that joins them. The "why" is on [`idempotency_key`], which is what
//! callers see.

use crate::job::Job;

/// Metadata key carrying the id the durable run started under.
///
/// Written by `retry_dead`, which mints a **new** job id: without it an operator
/// retrying a dead-lettered charge three days later sends a fresh key and
/// charges the customer a second time — deliberately, through the admin UI.
///
/// The double-underscore prefix marks it as the runtime's, not a user's, the
/// way `__dlq_retry_count` already does. Nothing rejects a caller who sets it
/// at enqueue, and a caller who sets the *same* value on two jobs has asked
/// their payment provider to treat both as one charge.
pub const ORIGIN_JOB_ID_KEY: &str = "__origin_job_id";

/// The durable run key of a job: the id its run began under.
///
/// `job.id` for a job that has only ever been retried in place — an ordinary
/// retry, a requeue and a `step.sleep` wake all keep the row and its id. The
/// stamped origin for one resurrected from the dead-letter queue, which is the
/// single boundary where the id changes.
pub fn run_key(job: &Job) -> String {
    origin_job_id(job.metadata.as_deref()).unwrap_or_else(|| job.id.clone())
}

/// `{run_key}:{step_key}` — the key to hand the *downstream* service.
///
/// Memoization closes the replay window, not the crash window. Between "the
/// charge succeeded" and "the step row committed" there is an instant where the
/// process can die, and the next attempt has no record that the call happened —
/// so it makes it again. Nothing on this side of the network can fix that. The
/// only fix is a key the *other* service honours, and the only key that works
/// is one this job mints the same way every time it runs:
///
/// ```text
/// 018f…c2:charge#0
/// └ run key ┘└ step ┘
/// ```
///
/// Derived from the run's identity and the step's position, and from nothing
/// else: no clock, no payload, no serializer, no codec. That is the contrast to
/// draw with the `idempotent=True` auto-key, which hashes the *serialized*
/// payload — a codec that compresses with a timestamp, or serializes a map in
/// hash order, changes that key between attempts. This one it cannot touch.
///
/// # Recipe: a Stripe-style API
///
/// Every payment API worth using takes an idempotency key on the request and
/// answers a repeat with the original response. Hand it this one and the crash
/// window closes:
///
/// ```no_run
/// # use flexiq_core::{Result, StepSession, Storage};
/// # fn charge(amount: i64, idempotency_key: &str) -> Result<Vec<u8>> { Ok(vec![]) }
/// # fn example<S: Storage>(session: &mut StepSession<S>, amount: i64) -> Result<()> {
/// // `key` is "{run_key}:charge#0" — the same string on every attempt.
/// let receipt = session.run("charge", None, |key| charge(amount, key))?;
/// # let _ = receipt;
/// # Ok(())
/// # }
/// ```
///
/// The attempt that dies after the payment API's 200 replays into the same key,
/// the API answers with the charge it already made, and the step row commits the
/// second time. One charge, whichever side of the window the process died on.
///
/// Two limits worth knowing before relying on it:
///
/// - **The key covers one run, not one order.** Two jobs enqueued for the same
///   order have two run keys and will both charge. Collapsing those is
///   `unique_key` — a different mechanism at a different level.
/// - **Downstream keys expire**, typically after 24 hours. A step that sleeps
///   past that window and then replays is a new request whatever key it sends.
pub fn idempotency_key(run_key: &str, step_key: &str) -> String {
    format!("{run_key}:{step_key}")
}

/// Stamp the metadata of a job resurrected from the dead-letter queue with the
/// run it belongs to, so its steps keep minting the keys they always have.
///
/// Preserves a usable value already there: a job dead-lettered and retried
/// twice must keep the id its *first* attempt ran under, not the id of the
/// resurrection before it. A missing or unusable one is replaced rather than
/// left — [`run_key`] would otherwise fall back to the new job id, which is
/// exactly the double charge this stamp exists to prevent.
pub(crate) fn stamp_origin_job_id(
    metadata: &mut serde_json::Map<String, serde_json::Value>,
    original_job_id: &str,
) {
    let carried = metadata
        .get(ORIGIN_JOB_ID_KEY)
        .and_then(serde_json::Value::as_str)
        .is_some_and(|origin| !origin.is_empty());
    if !carried {
        metadata.insert(
            ORIGIN_JOB_ID_KEY.to_string(),
            serde_json::Value::from(original_job_id),
        );
    }
}

/// The metadata a dead-letter row should carry, given a caller's replacement.
///
/// `move_to_dlq` lets its caller replace the job's metadata wholesale — a shed
/// marker, a retry-budget marker — which takes `__origin_job_id` with it. The
/// next `retry_dead` then stamps the *intermediate* job id, and a run already
/// resurrected once starts sending different downstream keys than it has been.
///
/// A replacement that is not a JSON object is left byte-for-byte alone, so such
/// a run does lose its origin here. `RETRY_BUDGET_EXHAUSTED` is a bare string
/// three SDK suites match on exactly; that path already discards the rest of the
/// job's metadata on the way back out, and giving it a shape is a cross-SDK
/// contract change, not a fix to make here.
pub(crate) fn carry_origin_job_id(replacement: Option<&str>, job: &Job) -> Option<String> {
    let Some(replacement) = replacement else {
        return job.metadata.clone();
    };
    let Some(origin) = origin_job_id(job.metadata.as_deref()) else {
        return Some(replacement.to_string());
    };
    let Ok(mut obj) =
        serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(replacement)
    else {
        return Some(replacement.to_string());
    };
    stamp_origin_job_id(&mut obj, &origin);
    serde_json::to_string(&serde_json::Value::Object(obj))
        .ok()
        .or_else(|| Some(replacement.to_string()))
}

/// The stamped origin id, if the metadata carries a usable one.
///
/// Anything else — absent, unparseable, not a string, blank — falls back to the
/// job's own id. A malformed value must not produce a *blank* run key: that
/// would make every job in the deployment share one key space and dedupe each
/// other's charges away.
fn origin_job_id(metadata: Option<&str>) -> Option<String> {
    let parsed: serde_json::Value = serde_json::from_str(metadata?).ok()?;
    match parsed.get(ORIGIN_JOB_ID_KEY)?.as_str()? {
        "" => None,
        origin => Some(origin.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::{now_millis, NewJob};

    /// A job as the queue would hand it to a step session, with whatever
    /// metadata a `retry_dead` left on it.
    fn job_with(id: &str, metadata: Option<&str>) -> Job {
        let mut job = NewJob {
            queue: "default".to_string(),
            task_name: "charge_card".to_string(),
            payload: vec![],
            priority: 0,
            scheduled_at: now_millis(),
            max_retries: 3,
            timeout_ms: 300_000,
            unique_key: None,
            metadata: metadata.map(str::to_string),
            notes: None,
            depends_on: vec![],
            expires_at: None,
            result_ttl_ms: None,
            namespace: None,
            debounce_key: None,
        }
        .into_job();
        job.id = id.to_string();
        job
    }

    #[test]
    fn the_key_is_the_run_and_the_step() {
        assert_eq!(idempotency_key("018f", "charge#0"), "018f:charge#0");
    }

    #[test]
    fn a_job_that_kept_its_id_is_its_own_run() {
        assert_eq!(run_key(&job_with("job-1", None)), "job-1");
        assert_eq!(
            run_key(&job_with("job-1", Some(r#"{"user_id":"u1"}"#))),
            "job-1"
        );
    }

    #[test]
    fn a_resurrected_job_keeps_the_run_it_began_as() {
        let job = job_with("job-2", Some(r#"{"__origin_job_id":"job-1"}"#));
        assert_eq!(run_key(&job), "job-1");
        assert_eq!(
            idempotency_key(&run_key(&job), "charge#0"),
            "job-1:charge#0",
            "the key an operator DLQ retry sends must be the one the first attempt sent"
        );
    }

    fn stamped(metadata: &str, original_job_id: &str) -> serde_json::Value {
        let mut obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(metadata).unwrap();
        stamp_origin_job_id(&mut obj, original_job_id);
        serde_json::Value::Object(obj)
    }

    #[test]
    fn a_resurrection_stamps_the_job_that_died() {
        let meta = stamped(r#"{"user_id":"u1"}"#, "job-1");
        assert_eq!(meta["__origin_job_id"], "job-1");
        assert_eq!(meta["user_id"], "u1", "user metadata is left alone");
    }

    #[test]
    fn a_second_resurrection_keeps_the_first_run() {
        // job-1 died, was retried as job-2, died again, retried as job-3. Every
        // one of them must send the keys job-1 sent.
        let meta = stamped(r#"{"__origin_job_id":"job-1"}"#, "job-2");
        assert_eq!(meta["__origin_job_id"], "job-1");
    }

    #[test]
    fn an_unusable_carried_origin_is_replaced() {
        for metadata in [
            r#"{"__origin_job_id":null}"#,
            r#"{"__origin_job_id":42}"#,
            r#"{"__origin_job_id":""}"#,
        ] {
            assert_eq!(
                stamped(metadata, "job-2")["__origin_job_id"],
                "job-2",
                "{metadata}"
            );
        }
    }

    #[test]
    fn replacement_dlq_metadata_still_carries_the_origin() {
        let job = job_with("job-2", Some(r#"{"__origin_job_id":"job-1"}"#));
        let carried: serde_json::Value = serde_json::from_str(
            &carry_origin_job_id(Some(r#"{"shed":"rate_limit"}"#), &job).unwrap(),
        )
        .unwrap();
        assert_eq!(carried["__origin_job_id"], "job-1");
        assert_eq!(carried["shed"], "rate_limit", "the marker is kept");
    }

    #[test]
    fn dlq_metadata_without_a_replacement_is_the_jobs_own() {
        let job = job_with("job-2", Some(r#"{"__origin_job_id":"job-1"}"#));
        assert_eq!(
            carry_origin_job_id(None, &job).as_deref(),
            Some(r#"{"__origin_job_id":"job-1"}"#)
        );
        assert_eq!(carry_origin_job_id(None, &job_with("job-1", None)), None);
    }

    #[test]
    fn a_replacement_is_untouched_when_there_is_nothing_to_carry() {
        // No origin on the job, and a bare-string marker three SDK suites match
        // on exactly — neither may be rewritten.
        let plain = job_with("job-1", None);
        assert_eq!(
            carry_origin_job_id(Some("retry_budget_exhausted"), &plain).as_deref(),
            Some("retry_budget_exhausted")
        );
        let resurrected = job_with("job-2", Some(r#"{"__origin_job_id":"job-1"}"#));
        assert_eq!(
            carry_origin_job_id(Some("retry_budget_exhausted"), &resurrected).as_deref(),
            Some("retry_budget_exhausted")
        );
    }

    #[test]
    fn an_unusable_origin_falls_back_to_the_job_id() {
        // Never to a blank run key: that would put every job in the deployment
        // in one key space, deduping each other's charges away.
        for metadata in [
            "not json",
            "[]",
            r#"{"__origin_job_id":null}"#,
            r#"{"__origin_job_id":42}"#,
            r#"{"__origin_job_id":""}"#,
        ] {
            assert_eq!(
                run_key(&job_with("job-3", Some(metadata))),
                "job-3",
                "{metadata}"
            );
        }
    }
}
