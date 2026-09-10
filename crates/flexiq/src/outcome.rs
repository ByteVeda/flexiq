//! What a task body can end with, and the failure shape the wire expects.

use flexiq_core::{StepSleep, TaskError};

/// How a task body ended when it did not return a value.
///
/// Two arms rather than one, because a `step.sleep` is not a failure: the job is
/// already rescheduled and unclaimed by the time the body unwinds, and reporting
/// it as an error would burn a retry for work that has not gone wrong. The
/// dispatcher maps each arm to a different [`flexiq_core::JobResult`].
#[derive(Debug)]
pub enum Abort {
    /// The task failed. The scheduler applies the retry policy when
    /// [`TaskError::retryable`] built it.
    Fail(TaskError),
    /// The task called `step.sleep` and this attempt is over.
    Sleep(StepSleep),
}

/// What a `#[flexiq::task]` function returns.
///
/// A caller's own error reaches [`Abort`] through [`TaskError`], which carries
/// the retryable/fatal distinction the scheduler acts on. There is deliberately
/// no blanket `From<E: std::error::Error>`: it would collide with
/// `From<TaskError>`, and it would have to guess retryability — the one bit of a
/// failure only the task author knows. End a fallible call with
/// `.map_err(TaskError::retryable)?` or `.map_err(TaskError::fatal)?`.
pub type Outcome<T> = std::result::Result<T, Abort>;

impl From<TaskError> for Abort {
    fn from(err: TaskError) -> Self {
        Abort::Fail(err)
    }
}

/// The canonical JSON a failed job records, per `BINDING_CONTRACT.md`.
///
/// Rust has no exception type to name and no stack trace to attach, so `errtype`
/// is the constant `"TaskError"` and `traceback` is null. Both fields are still
/// written: a reader in another language matches on their presence, and omitting
/// them would make a Rust failure the one shape that needs a special case.
// Temporary: the dispatcher is the only caller and does not exist yet. Remove
// this the moment `pool.rs` lands — pre-commit runs clippy over the whole
// workspace with `-D warnings`, so a `pub(crate)` helper with no consumer is a
// commit failure, not a warning.
#[allow(dead_code)]
pub(crate) fn task_error_json(err: &TaskError) -> String {
    serde_json::json!({
        "errtype": "TaskError",
        "message": err.message,
        "traceback": serde_json::Value::Null,
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_error_json_uses_the_contract_shape() {
        let err = TaskError::fatal("card declined");
        let encoded = task_error_json(&err);
        let parsed: serde_json::Value = serde_json::from_str(&encoded).expect("valid JSON");

        assert_eq!(parsed["errtype"], "TaskError");
        assert_eq!(parsed["message"], "card declined");
        assert_eq!(parsed["traceback"], serde_json::Value::Null);
    }

    #[test]
    fn a_task_error_converts_into_a_failing_abort() {
        let abort: Abort = TaskError::retryable("upstream 503").into();
        match abort {
            Abort::Fail(err) => {
                assert_eq!(err.message, "upstream 503");
                assert!(err.retryable);
            }
            Abort::Sleep(_) => panic!("expected Fail"),
        }
    }
}
