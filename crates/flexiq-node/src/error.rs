//! Translate core errors into napi errors surfaced to JavaScript.

use flexiq_core::QueueError;
use napi::bindgen_prelude::{Error, Status};

/// Map a core [`QueueError`] onto a napi [`Error`] (a thrown JS `Error`).
pub fn to_napi_err(err: QueueError) -> Error {
    Error::new(Status::GenericFailure, err.to_string())
}

/// Map a blocking-task join failure onto a napi [`Error`].
pub fn join_to_napi_err(err: tokio::task::JoinError) -> Error {
    Error::new(
        Status::GenericFailure,
        format!("blocking storage task failed: {err}"),
    )
}

/// Build an `InvalidArg` error for caller-supplied input that fails validation
/// at the N-API boundary (negative pagination, zero pool size, bad rate limit…).
pub fn invalid_arg(message: impl Into<String>) -> Error {
    Error::new(Status::InvalidArg, message.into())
}

/// Reject a negative value for a field that must be non-negative.
pub fn non_negative(value: i64, field: &str) -> Result<i64, Error> {
    if value < 0 {
        Err(invalid_arg(format!("{field} must be >= 0, got {value}")))
    } else {
        Ok(value)
    }
}
