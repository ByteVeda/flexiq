//! Turning a [`QueueError`] into something a client can branch on.
//!
//! Nothing existed to inherit here. The dashboard maps exactly one variant and
//! drops the rest into a 500, and all three language shells collapse the whole
//! enum into one exception carrying the `Display` string. So this is the first
//! surface on which a client can branch on something other than prose, and the
//! shape it takes is fixed:
//!
//! * a [`Code`], for a generic client or a middlebox deciding whether to retry;
//! * an `ErrorInfo` whose [`reason`](reason) is the stable identifier;
//! * typed metadata wherever a client needs a number, so nothing is ever parsed
//!   back out of a message.
//!
//! The message is for humans and logs, and may be reworded in any release.
//!
//! Two invariants are pinned by the tests at the bottom of this file: the match
//! over `QueueError` is **exhaustive**, so a new variant fails the build rather
//! than reaching the wire as `UNKNOWN`; and it agrees with
//! [`classify_step_failure`] on every arm that function names.

pub mod reason;

use std::collections::HashMap;
use std::time::Duration;

use flexiq_core::error::QueueError;
use tonic::{Code, Status};
use tonic_types::{ErrorDetails, StatusExt};

/// What a client waits before retrying a `RESOURCE_EXHAUSTED`.
///
/// A fixed hint rather than a computed one: neither a full queue nor a spent
/// rate-limit token gives the server a defensible estimate of when the caller
/// should come back, and an invented number would read as one.
const RETRY_AFTER: Duration = Duration::from_secs(1);

/// What the client is told when the cause is a storage failure.
///
/// The cause goes to the operator's log and never onto the wire: those strings
/// carry SQL, schema, connection and host detail. The dashboard already made
/// this call for its own 500s.
const SANITISED: &str = "the storage backend is unavailable";

/// One error, in the shape the wire carries it.
///
/// Built once and rendered twice — as a [`Status`] for an RPC that failed, and
/// as a `google.rpc.Status` for one item of a batch that did not. Rendering
/// both from one value is what keeps a batch item's error identical to the
/// error the same request would have produced on its own.
#[derive(Debug, Clone)]
pub struct WireError {
    code: Code,
    reason: &'static str,
    message: String,
    metadata: HashMap<String, String>,
    retry_after: Option<Duration>,
}

impl WireError {
    /// Map a core error onto its code, reason and metadata.
    ///
    /// Anything whose message is withheld is logged **here**, at the one point
    /// every wire error is built, rather than at each boundary that produces
    /// one. A batch item's failure travels a different path from an RPC's, and
    /// a rule that has to be remembered per path is a rule one path will miss —
    /// leaving a storage failure sanitised on the wire and absent from the
    /// operator's log, which is the half of the trade that makes sanitising
    /// acceptable.
    pub fn from_queue_error(error: &QueueError) -> Self {
        log_if_sanitised(error);
        let (code, reason) = classify(error);
        let mut wire = Self {
            code,
            reason,
            message: message_for(error, reason),
            metadata: HashMap::new(),
            retry_after: (code == Code::ResourceExhausted).then_some(RETRY_AFTER),
        };
        wire.metadata = metadata_for(error);
        wire
    }

    /// A request this service refuses before it reaches storage.
    ///
    /// There is no `QueueError` for a missing `body` arm or an unreadable page
    /// token, and inventing one to route through [`Self::from_queue_error`]
    /// would put a wire concern into the core's error type.
    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self {
            code: Code::InvalidArgument,
            reason: reason::INVALID_REQUEST,
            message: message.into(),
            metadata: HashMap::new(),
            retry_after: None,
        }
    }

    /// A fault of the server's own, with nothing useful to say to the caller.
    ///
    /// The cause is logged by whoever raises this; the response carries only
    /// the reason, so a client can still branch on it.
    pub fn internal() -> Self {
        Self {
            code: Code::Internal,
            reason: reason::INTERNAL,
            message: "the request failed".to_string(),
            metadata: HashMap::new(),
            retry_after: None,
        }
    }

    /// Name the batch item this error belongs to.
    ///
    /// `index` rides alongside whatever reason the item raised rather than
    /// replacing it: a client that gets `QUEUE_FULL` on a batch needs both
    /// facts, and one answer should carry them.
    #[must_use]
    pub fn at_index(mut self, index: usize) -> Self {
        self.metadata
            .insert(reason::KEY_INDEX.to_string(), index.to_string());
        self
    }

    /// The reason a client branches on. Exposed for tests and for the batch
    /// paths that inspect an error before rendering it.
    pub fn reason(&self) -> &'static str {
        self.reason
    }

    /// The gRPC code.
    pub fn code(&self) -> Code {
        self.code
    }

    /// The human message. For logs and for tests — a client branches on
    /// [`reason`](Self::reason), never on this.
    pub fn message(&self) -> &str {
        &self.message
    }

    fn details(&self) -> ErrorDetails {
        let mut details = ErrorDetails::new();
        details.set_error_info(self.reason, reason::DOMAIN, self.metadata.clone());
        if let Some(retry_after) = self.retry_after {
            details.set_retry_info(Some(retry_after));
        }
        details
    }
}

impl From<WireError> for Status {
    fn from(wire: WireError) -> Self {
        Status::with_error_details(wire.code, wire.message.clone(), wire.details())
    }
}

impl From<WireError> for tonic_types::pb::Status {
    fn from(wire: WireError) -> Self {
        use prost::Message;

        // Round-tripping through `tonic::Status` rather than assembling the
        // `Any` by hand keeps one construction path for the details, so a batch
        // item's bytes are the bytes the same failure would carry as an RPC.
        let status = Status::from(wire);
        Self::decode(status.details()).unwrap_or_else(|_| Self {
            code: status.code() as i32,
            message: status.message().to_string(),
            details: Vec::new(),
        })
    }
}

impl From<&QueueError> for WireError {
    fn from(error: &QueueError) -> Self {
        Self::from_queue_error(error)
    }
}

/// A `QueueError` as a `tonic::Status`, for a handler that has nothing to add.
pub fn from_queue_error(error: &QueueError) -> Status {
    WireError::from_queue_error(error).into()
}

/// The code and reason for one error.
///
/// Deliberately exhaustive: adding a `QueueError` variant must fail this build
/// rather than reach a client as `UNKNOWN`. The two nested matches keep a
/// wildcard because both of the types they match on are `#[non_exhaustive]`
/// upstream, and their defaults are the retryable answer.
fn classify(error: &QueueError) -> (Code, &'static str) {
    use flexiq_core::diesel::result::{DatabaseErrorKind, Error as DieselError};

    match error {
        // A row that is genuinely absent is normalised long before it becomes
        // an error — `get_job` returns an `Option`, the id-addressed paths
        // answer `false` — so a raw `NotFound` here is a query that forgot
        // `.optional()`. Retrying fails identically, and answering `NOT_FOUND`
        // would tell a caller their job does not exist when it does.
        QueueError::Storage(DieselError::NotFound) => (Code::Internal, reason::INTERNAL),
        QueueError::Storage(DieselError::DatabaseError(kind, _)) => match kind {
            DatabaseErrorKind::UniqueViolation
            | DatabaseErrorKind::ForeignKeyViolation
            | DatabaseErrorKind::NotNullViolation
            | DatabaseErrorKind::CheckViolation => (Code::Internal, reason::STORAGE_CONSTRAINT),
            _ => (Code::Unavailable, reason::STORAGE_UNAVAILABLE),
        },
        QueueError::Storage(_) | QueueError::Pool(_) => {
            (Code::Unavailable, reason::STORAGE_UNAVAILABLE)
        }
        #[cfg(feature = "redis")]
        QueueError::Redis(_) => (Code::Unavailable, reason::STORAGE_UNAVAILABLE),

        // Reached only from a server-side encode or decode: the producer door
        // never parses a client's payload, it forwards the bytes. The client-
        // caused half of this row belongs to whatever first accepts structured
        // arguments, and it answers INVALID_ARGUMENT / MALFORMED_PAYLOAD.
        QueueError::Json(_) | QueueError::Serialization(_) => (Code::Internal, reason::INTERNAL),

        QueueError::JobNotFound(_) => (Code::NotFound, reason::JOB_NOT_FOUND),
        QueueError::DependencyNotFound(_) => {
            (Code::FailedPrecondition, reason::DEPENDENCY_NOT_FOUND)
        }
        QueueError::QueueFull { .. } => (Code::ResourceExhausted, reason::QUEUE_FULL),
        QueueError::RateLimitExceeded(_) => (Code::ResourceExhausted, reason::RATE_LIMITED),
        QueueError::ContractTooOld { .. } => (Code::FailedPrecondition, reason::CONTRACT_TOO_OLD),
        QueueError::TaskNotRegistered(_) => (Code::FailedPrecondition, reason::TASK_NOT_REGISTERED),
        QueueError::Timeout(_) => (Code::DeadlineExceeded, reason::JOB_TIMEOUT),

        // FAILED_PRECONDITION and never ABORTED. ABORTED is the code a generic
        // client retries with backoff, and a lost claim is the one concurrency
        // conflict where retrying is the worst available move: the job is
        // proceeding under another owner, and a resent frame is the double
        // execution the (owner, attempt) fence exists to prevent.
        QueueError::ClaimLost(_) => (Code::FailedPrecondition, reason::CLAIM_LOST),

        QueueError::StepDiverged { .. } | QueueError::StepSequenceDiverged(_) => {
            (Code::FailedPrecondition, reason::STEP_DIVERGED)
        }
        // INVALID_ARGUMENT rather than RESOURCE_EXHAUSTED: the commit cannot
        // succeed at any later time or under any server state, which is what
        // INVALID_ARGUMENT means and what RESOURCE_EXHAUSTED would deny.
        QueueError::StepLimitExceeded { .. } => {
            (Code::InvalidArgument, reason::STEP_LIMIT_EXCEEDED)
        }
        QueueError::StepRefused(_) => (Code::FailedPrecondition, reason::STEP_REFUSED),

        QueueError::Worker(_) | QueueError::Scheduler(_) => (Code::Internal, reason::INTERNAL),
        QueueError::Config(_) => (Code::Internal, reason::SERVER_MISCONFIGURED),
        QueueError::LockNotAcquired(_) => (Code::Aborted, reason::LOCK_HELD),
        QueueError::SettingConflict(_) => (Code::Aborted, reason::SETTING_CONFLICT),
        QueueError::Other(_) => (Code::Unknown, reason::UNKNOWN),
    }
}

/// Whether this error's `Display` may be sent.
///
/// Sanitisation is by **provenance**, not by variant, which is why `Other` is
/// on this list: the Redis backend stringifies a `RedisError` into it, so a
/// boundary that switched on the variant name alone would forward exactly the
/// connection detail this exists to withhold — on the one backend whose errors
/// most often name a host.
fn is_sanitised(error: &QueueError) -> bool {
    match error {
        QueueError::Storage(_) | QueueError::Pool(_) | QueueError::Other(_) => true,
        #[cfg(feature = "redis")]
        QueueError::Redis(_) => true,
        _ => false,
    }
}

fn log_if_sanitised(error: &QueueError) {
    if is_sanitised(error) {
        log::error!("grpc: {error}");
    }
}

fn message_for(error: &QueueError, reason: &'static str) -> String {
    if is_sanitised(error) {
        // `Other` is not always a storage failure, so it does not claim to be
        // one; both answers are equally uninformative on purpose.
        return if reason == reason::UNKNOWN {
            "the request failed".to_string()
        } else {
            SANITISED.to_string()
        };
    }
    error.to_string()
}

/// The typed values a client would otherwise have to parse out of the message.
fn metadata_for(error: &QueueError) -> HashMap<String, String> {
    let mut metadata = HashMap::new();
    match error {
        QueueError::QueueFull {
            queue,
            pending,
            cap,
        } => {
            metadata.insert(reason::KEY_QUEUE.to_string(), queue.clone());
            metadata.insert(reason::KEY_PENDING.to_string(), pending.to_string());
            metadata.insert(reason::KEY_CAP.to_string(), cap.to_string());
        }
        QueueError::ContractTooOld { speaks, required } => {
            metadata.insert(reason::KEY_SPEAKS.to_string(), speaks.to_string());
            metadata.insert(reason::KEY_REQUIRED.to_string(), required.to_string());
        }
        QueueError::StepLimitExceeded {
            limit,
            actual,
            allowed,
            ..
        } => {
            metadata.insert(reason::KEY_LIMIT.to_string(), limit.clone());
            metadata.insert(reason::KEY_ACTUAL.to_string(), actual.to_string());
            metadata.insert(reason::KEY_ALLOWED.to_string(), allowed.to_string());
        }
        _ => {}
    }
    metadata
}

#[cfg(test)]
mod tests {
    use super::*;
    use flexiq_core::step::{classify_step_failure, StepFailure};

    /// Every variant, so the exhaustive match in `classify` is exercised rather
    /// than merely compiled.
    fn every_variant() -> Vec<QueueError> {
        let mut all = vec![
            QueueError::Storage(flexiq_core::diesel::result::Error::NotFound),
            QueueError::Storage(flexiq_core::diesel::result::Error::DatabaseError(
                flexiq_core::diesel::result::DatabaseErrorKind::UniqueViolation,
                Box::new("unique".to_string()),
            )),
            QueueError::Storage(flexiq_core::diesel::result::Error::DatabaseError(
                flexiq_core::diesel::result::DatabaseErrorKind::ClosedConnection,
                Box::new("closed".to_string()),
            )),
            QueueError::Storage(flexiq_core::diesel::result::Error::BrokenTransactionManager),
            QueueError::Json(serde_json::from_str::<i32>("nope").unwrap_err()),
            QueueError::JobNotFound("j".into()),
            QueueError::TaskNotRegistered("t".into()),
            QueueError::Serialization("s".into()),
            QueueError::Worker("w".into()),
            QueueError::Scheduler("s".into()),
            QueueError::RateLimitExceeded("r".into()),
            QueueError::QueueFull {
                queue: "q".into(),
                pending: 11,
                cap: 10,
            },
            QueueError::Timeout("j".into()),
            QueueError::Config("c".into()),
            QueueError::DependencyNotFound("d".into()),
            QueueError::LockNotAcquired("l".into()),
            QueueError::SettingConflict("s".into()),
            QueueError::ContractTooOld {
                speaks: 1,
                required: 2,
            },
            QueueError::ClaimLost("j".into()),
            QueueError::StepDiverged {
                job_id: "j".into(),
                seq: 1,
                expected: "a".into(),
                found: "b".into(),
            },
            QueueError::StepLimitExceeded {
                step_key: "k".into(),
                limit: "step bytes".into(),
                actual: 2,
                allowed: 1,
            },
            QueueError::StepRefused("r".into()),
            QueueError::Other("o".into()),
        ];
        // `Pool` has no public constructor that does not need a live pool, and
        // `StepSequenceDiverged` boxes a private-ish payload; both share an arm
        // with a variant already listed, so the table stays covered.
        all.shrink_to_fit();
        all
    }

    /// Codes a client may retry on the code alone.
    fn is_retryable_code(code: Code) -> bool {
        matches!(
            code,
            Code::Unavailable | Code::Aborted | Code::ResourceExhausted
        )
    }

    #[test]
    fn every_error_carries_a_reason_and_a_code() {
        for error in every_variant() {
            let wire = WireError::from_queue_error(&error);
            assert!(
                !wire.reason().is_empty() && wire.code() != Code::Ok,
                "{error} produced no usable status"
            );
        }
    }

    #[test]
    fn the_wire_agrees_with_the_step_classifier_on_the_arms_it_names() {
        // Only the arms `classify_step_failure` matches explicitly. Its
        // `_ => Retryable` fallthrough is excluded on purpose: it is a fail-safe
        // default at the step-ack boundary, where "retryable" means *this job
        // attempt may run again*, while a retryable code means *this client may
        // resend this request*. `Timeout`, `Worker`, `Scheduler` and `Other`
        // all fall through it, and none of them is a request to send again.
        let named: Vec<QueueError> = vec![
            QueueError::ClaimLost("j".into()),
            QueueError::StepDiverged {
                job_id: "j".into(),
                seq: 1,
                expected: "a".into(),
                found: "b".into(),
            },
            QueueError::StepLimitExceeded {
                step_key: "k".into(),
                limit: "step bytes".into(),
                actual: 2,
                allowed: 1,
            },
            QueueError::Serialization("s".into()),
            QueueError::Json(serde_json::from_str::<i32>("nope").unwrap_err()),
            QueueError::Config("c".into()),
            QueueError::TaskNotRegistered("t".into()),
            QueueError::ContractTooOld {
                speaks: 1,
                required: 2,
            },
            QueueError::StepRefused("r".into()),
            QueueError::Storage(flexiq_core::diesel::result::Error::DatabaseError(
                flexiq_core::diesel::result::DatabaseErrorKind::UniqueViolation,
                Box::new("unique".to_string()),
            )),
            QueueError::Storage(flexiq_core::diesel::result::Error::DatabaseError(
                flexiq_core::diesel::result::DatabaseErrorKind::ClosedConnection,
                Box::new("closed".to_string()),
            )),
        ];

        for error in named {
            let wire = WireError::from_queue_error(&error);
            match classify_step_failure(&error) {
                StepFailure::Permanent => assert!(
                    !is_retryable_code(wire.code()),
                    "{error} is permanent to the step classifier but retryable on the wire"
                ),
                StepFailure::Retryable => assert!(
                    is_retryable_code(wire.code()),
                    "{error} is retryable to the step classifier but not on the wire"
                ),
                StepFailure::Superseded => {
                    assert_eq!(wire.reason(), reason::CLAIM_LOST);
                    assert!(!is_retryable_code(wire.code()));
                }
            }
        }
    }

    #[test]
    fn a_lost_claim_is_never_in_the_retry_class() {
        // ABORTED would put it there, and a resent frame is the double
        // execution the (owner, attempt) fence exists to prevent.
        let wire = WireError::from_queue_error(&QueueError::ClaimLost("j".into()));
        assert_eq!(wire.code(), Code::FailedPrecondition);
        assert_ne!(wire.code(), Code::Aborted);
    }

    #[test]
    fn queue_full_carries_its_numbers_as_values() {
        let wire = WireError::from_queue_error(&QueueError::QueueFull {
            queue: "emails".into(),
            pending: 11,
            cap: 10,
        });
        assert_eq!(wire.metadata[reason::KEY_QUEUE], "emails");
        assert_eq!(wire.metadata[reason::KEY_PENDING], "11");
        assert_eq!(wire.metadata[reason::KEY_CAP], "10");
        assert_eq!(wire.retry_after, Some(RETRY_AFTER));
    }

    #[test]
    fn storage_detail_never_reaches_the_client() {
        let error = QueueError::Storage(flexiq_core::diesel::result::Error::DatabaseError(
            flexiq_core::diesel::result::DatabaseErrorKind::ClosedConnection,
            Box::new("host=db.internal user=flexiq".to_string()),
        ));
        let status = Status::from(WireError::from_queue_error(&error));
        assert_eq!(status.message(), SANITISED);
        assert!(!status.message().contains("db.internal"));
    }

    #[test]
    fn other_is_sanitised_too() {
        // The Redis backend stringifies connection errors into `Other`, so the
        // variant name is not what decides this.
        let error = QueueError::Other("redis://user:pw@10.0.0.4:6379 refused".into());
        let status = Status::from(WireError::from_queue_error(&error));
        assert!(!status.message().contains("10.0.0.4"));
    }

    #[test]
    fn the_error_info_survives_the_round_trip_into_a_batch_item() {
        let wire = WireError::from_queue_error(&QueueError::QueueFull {
            queue: "emails".into(),
            pending: 11,
            cap: 10,
        })
        .at_index(3);

        let rpc: tonic_types::pb::Status = wire.into();
        assert_eq!(rpc.code, Code::ResourceExhausted as i32);

        let status = Status::with_error_details(
            Code::from_i32(rpc.code),
            rpc.message.clone(),
            ErrorDetails::new(),
        );
        assert_eq!(status.code(), Code::ResourceExhausted);

        // The details are the same bytes an ordinary RPC would have carried.
        let info = tonic_types::RpcStatusExt::get_details_error_info(&rpc)
            .expect("the item error carries an ErrorInfo");
        assert_eq!(info.reason, reason::QUEUE_FULL);
        assert_eq!(info.domain, reason::DOMAIN);
        assert_eq!(info.metadata[reason::KEY_INDEX], "3");
        assert_eq!(info.metadata[reason::KEY_CAP], "10");
    }

    #[test]
    fn an_invalid_request_is_not_retryable() {
        let wire = WireError::invalid_request("no body arm set");
        assert_eq!(wire.code(), Code::InvalidArgument);
        assert_eq!(wire.reason(), reason::INVALID_REQUEST);
        assert!(!is_retryable_code(wire.code()));
    }
}
