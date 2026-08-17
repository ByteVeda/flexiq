//! API error type and its wire mapping.
//!
//! Bodies match the SDK dashboards byte for byte (`{"error": "..."}`, and the
//! same sentinel strings for `setup_required` / `not_authenticated` /
//! `csrf_failed`) because one SPA is served by every implementation and it
//! branches on those values.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

/// Anything a route handler can fail with.
#[derive(Debug)]
pub enum ApiError {
    /// Malformed input — 400 with the reason.
    BadRequest(String),
    /// No such resource — 404 with the reason.
    NotFound(String),
    /// No valid session — 401.
    Unauthenticated,
    /// Authenticated but not permitted — 403 with a sentinel.
    Forbidden(&'static str),
    /// Session auth is enabled but no user exists yet — 503.
    SetupRequired,
    /// Auth endpoints called while auth is disabled — 404, so the SPA hides
    /// its login affordances.
    AuthDisabled,
    /// Too many failed attempts — 429, with the seconds left to wait.
    TooManyAttempts(u64),
    /// A concurrent writer won every attempt — 409, so the client knows the
    /// request was well-formed and retrying it may work.
    Conflict(String),
    /// Anything unexpected — 500 with a generic body, details go to the log.
    Internal(anyhow::Error),
}

impl ApiError {
    /// A 404 with the SDK dashboards' generic wording.
    pub fn not_found() -> Self {
        Self::NotFound("Not found".to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        // A lockout has to say how long it lasts, or a client can only poll.
        if let Self::TooManyAttempts(retry_after_seconds) = self {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                [("retry-after", retry_after_seconds.to_string())],
                Json(json!({ "error": "too_many_attempts" })),
            )
                .into_response();
        }
        let (status, message) = match self {
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
            Self::Conflict(message) => (StatusCode::CONFLICT, message),
            Self::NotFound(message) => (StatusCode::NOT_FOUND, message),
            Self::Unauthenticated => (StatusCode::UNAUTHORIZED, "not_authenticated".to_string()),
            Self::Forbidden(reason) => (StatusCode::FORBIDDEN, reason.to_string()),
            Self::SetupRequired => (
                StatusCode::SERVICE_UNAVAILABLE,
                "setup_required".to_string(),
            ),
            Self::AuthDisabled => (StatusCode::NOT_FOUND, "auth_disabled".to_string()),
            // Handled above; matched again so the arm list stays exhaustive.
            Self::TooManyAttempts(_) => (
                StatusCode::TOO_MANY_REQUESTS,
                "too_many_attempts".to_string(),
            ),
            Self::Internal(error) => {
                // The cause is for the operator's log, never the response — it
                // can carry a DSN or a query fragment.
                log::error!("dashboard request failed: {error:#}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal server error".to_string(),
                )
            }
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}

impl From<flexiq_core::QueueError> for ApiError {
    fn from(error: flexiq_core::QueueError) -> Self {
        match error {
            // Losing every compare-and-set attempt is contention, not a fault:
            // the request was well-formed and retrying it may well succeed, so
            // a 500 would tell the client the opposite of the truth.
            flexiq_core::QueueError::SettingConflict(_) => Self::Conflict(error.to_string()),
            other => Self::Internal(other.into()),
        }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        Self::Internal(error)
    }
}

/// Result alias every route handler returns.
pub type ApiResult<T> = Result<T, ApiError>;

#[cfg(test)]
mod tests {
    use super::*;

    fn status_of(error: ApiError) -> StatusCode {
        error.into_response().status()
    }

    #[test]
    fn statuses_match_the_sdk_dashboards() {
        assert_eq!(status_of(ApiError::not_found()), StatusCode::NOT_FOUND);
        assert_eq!(
            status_of(ApiError::BadRequest("bad".into())),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            status_of(ApiError::Unauthenticated),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            status_of(ApiError::Forbidden("csrf_failed")),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            status_of(ApiError::SetupRequired),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(status_of(ApiError::AuthDisabled), StatusCode::NOT_FOUND);
        assert_eq!(
            status_of(ApiError::TooManyAttempts(42)),
            StatusCode::TOO_MANY_REQUESTS
        );
    }

    #[test]
    fn a_lockout_says_how_long_to_wait() {
        let response = ApiError::TooManyAttempts(120).into_response();
        assert_eq!(
            response
                .headers()
                .get("retry-after")
                .map(|v| v.to_str().unwrap()),
            Some("120")
        );
    }
}
