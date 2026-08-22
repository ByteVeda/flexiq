//! What a shell should do with a step operation that failed.

/// What a shell should do with a step operation that failed.
///
/// The classification lives here, in the rules module, so every shell
/// acknowledges a failed step the same way. Getting it wrong in either
/// direction is expensive: retrying a permanently-bad commit burns the job's
/// whole retry budget on an error that will never change, and dead-lettering a
/// transient one throws away work over a dropped connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
        | E::ContractTooOld { .. } => StepFailure::Permanent,

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
