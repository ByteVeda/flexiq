//! What a shell should do with a step operation that failed.

/// What a shell should do with a step operation that failed.
///
/// The classification lives here, in the rules module, so every shell
/// acknowledges a failed step the same way. Getting it wrong in either
/// direction is expensive: retrying a permanently-bad commit burns the job's
/// whole retry budget on an error that will never change, and dead-lettering a
/// transient one throws away work over a dropped connection.
///
/// Serializable because it crosses the attached-executor wire on a `step_ack`:
/// the classification is made by the side that holds storage, and travels back
/// as itself rather than being re-derived from an error string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepFailure {
    /// The backend was unavailable. Fail the attempt and let the job's own
    /// retry policy have it.
    Retryable,
    /// The commit could never succeed — a divergence, a cap, a bad encoding, a
    /// constraint. Fail without retrying; a retry would replay the same input.
    Permanent,
    /// The attempt lost its fence. It emits **no result at all**: the job is
    /// proceeding under another owner, and failing it here would kill a run
    /// going correctly elsewhere.
    Superseded,
}

impl StepFailure {
    /// Whether the attempt should be retried.
    ///
    /// [`Superseded`](Self::Superseded) answers `false` for completeness only —
    /// a superseded attempt emits no result at all, so nothing consults this
    /// for it.
    pub const fn should_retry(self) -> bool {
        matches!(self, StepFailure::Retryable)
    }
}

/// Classify a step operation's error at the acknowledgement boundary.
pub fn classify_step_failure(error: &crate::error::QueueError) -> StepFailure {
    use crate::error::QueueError as E;

    match error {
        E::ClaimLost(_) => StepFailure::Superseded,

        // The input itself is wrong, and will be just as wrong next attempt.
        E::StepDiverged { .. }
        | E::StepSequenceDiverged(_)
        | E::StepLimitExceeded { .. }
        | E::Serialization(_)
        | E::Json(_)
        | E::Config(_)
        | E::TaskNotRegistered(_)
        | E::ContractTooOld { .. }
        // Already classified as permanent by whoever could see the real error.
        | E::StepRefused(_) => StepFailure::Permanent,

        // A violated constraint is a permanently-bad write; everything else
        // Diesel reports is the database being unreachable or busy.
        E::Storage(diesel::result::Error::DatabaseError(kind, _)) => {
            use diesel::result::DatabaseErrorKind::*;
            match kind {
                UniqueViolation | ForeignKeyViolation | NotNullViolation | CheckViolation => {
                    StepFailure::Permanent
                }
                _ => StepFailure::Retryable,
            }
        }

        _ => StepFailure::Retryable,
    }
}

/// Rebuild the error a refused step commit represents.
///
/// The inverse of [`classify_step_failure`], and the one place it lives: an ack
/// carries the message and the verdict, never the variant, so every reader of
/// one — the attached executor, and any shell relaying its frames a hop further
/// — has to turn the pair back into an error the same way. Two copies of this
/// drift, and the direction they drift in decides whether a retry happens.
///
/// A missing verdict is [`StepFailure::Retryable`]: nothing was confirmed
/// written, so a replay is safe and the job's own retry policy gets to decide.
/// A `Superseded` refusal keeps only the job id, because
/// [`QueueError::ClaimLost`] renders one itself and the attempt it names emits
/// no result for anyone to read the message on.
pub fn refusal_error(
    job_id: &str,
    message: Option<String>,
    failure: Option<StepFailure>,
) -> crate::error::QueueError {
    use crate::error::QueueError as E;

    let message = message.unwrap_or_else(|| {
        format!("a step commit for job {job_id} was refused without saying why")
    });
    match failure.unwrap_or(StepFailure::Retryable) {
        StepFailure::Superseded => E::ClaimLost(job_id.to_string()),
        StepFailure::Permanent => E::StepRefused(message),
        StepFailure::Retryable => E::Other(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::QueueError;

    #[test]
    fn a_lost_fence_contributes_nothing() {
        assert_eq!(
            classify_step_failure(&QueueError::ClaimLost("j".into())),
            StepFailure::Superseded
        );
    }

    #[test]
    fn a_bad_commit_is_never_retried() {
        let permanent = [
            QueueError::StepDiverged {
                job_id: "j".into(),
                seq: 0,
                expected: "a".into(),
                found: "b".into(),
            },
            QueueError::StepLimitExceeded {
                step_key: "render#0".into(),
                limit: "step bytes".into(),
                actual: 64,
                allowed: 8,
            },
            QueueError::Serialization("bad codec".into()),
            QueueError::Config("step name contains ':'".into()),
            QueueError::Storage(diesel::result::Error::DatabaseError(
                diesel::result::DatabaseErrorKind::UniqueViolation,
                Box::new(String::new()),
            )),
        ];
        for error in permanent {
            assert_eq!(
                classify_step_failure(&error),
                StepFailure::Permanent,
                "{error}"
            );
        }
    }

    #[test]
    fn a_refusal_round_trips_through_its_classification() {
        // The pair the ack carries is enough to rebuild an error that
        // classifies back the same way; nothing else has to cross.
        for failure in [
            StepFailure::Retryable,
            StepFailure::Permanent,
            StepFailure::Superseded,
        ] {
            let error = refusal_error("job-1", Some("refused".into()), Some(failure));
            assert_eq!(classify_step_failure(&error), failure, "{error}");
        }
    }

    #[test]
    fn a_refusal_with_no_verdict_is_retried() {
        // Nothing was confirmed written, so the safe reading is "try again".
        let error = refusal_error("job-1", None, None);
        assert_eq!(classify_step_failure(&error), StepFailure::Retryable);
        assert!(error.to_string().contains("without saying why"), "{error}");
    }

    #[test]
    fn an_unreachable_backend_is_retried() {
        let retryable = [
            QueueError::Storage(diesel::result::Error::DatabaseError(
                diesel::result::DatabaseErrorKind::SerializationFailure,
                Box::new(String::new()),
            )),
            QueueError::Storage(diesel::result::Error::BrokenTransactionManager),
            QueueError::Other("connection reset".into()),
        ];
        for error in retryable {
            assert_eq!(
                classify_step_failure(&error),
                StepFailure::Retryable,
                "{error}"
            );
        }
    }
}
