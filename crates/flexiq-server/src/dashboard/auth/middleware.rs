//! The single gate every request passes through.
//!
//! Running the classification in one layer — rather than per handler — is what
//! guarantees a route added later cannot end up outside it. The resolved
//! [`RequestContext`] is attached to the request so handlers that need the
//! caller (whoami, logout, the probes) read it instead of parsing cookies
//! again.

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::config::dashboard::AuthMode;
use crate::dashboard::auth::context::{self, RequestContext};
use crate::dashboard::auth::gate;
use crate::dashboard::auth::store;
use crate::dashboard::blocking::on_storage;
use crate::dashboard::error::ApiError;
use crate::dashboard::state::SharedState;

/// Authenticate and authorize, then run the route.
///
/// Takes the state as a plain argument rather than an extractor so it can be
/// called directly from tests as well as from the middleware layer.
pub async fn gate_request(state: SharedState, mut request: Request, next: Next) -> Response {
    let path = request.uri().path().to_string();
    let method = request.method().as_str().to_string();

    if state.config.auth == AuthMode::Open {
        // With auth off the session endpoints do not exist, so the SPA hides
        // its login affordances instead of offering a form that cannot work.
        if path.starts_with("/api/auth/") && path != "/api/auth/status" {
            return ApiError::AuthDisabled.into_response();
        }
        request.extensions_mut().insert(RequestContext::default());
        return next.run(request).await;
    }

    // Copy the headers out before awaiting: an in-flight `Request` holds a
    // body that is not `Sync`, so borrowing it across an await would make this
    // future non-`Send` and it could not be a middleware at all.
    let headers = request.headers().clone();
    let context = match resolve_context(&state, &headers).await {
        Ok(context) => context,
        Err(error) => return error.into_response(),
    };

    if let Err(denial) = authorize(&state, &context, &path, &method).await {
        return denial.into_response();
    }

    request.extensions_mut().insert(context);
    next.run(request).await
}

/// Load the session named by the request's cookie, if it is still live.
async fn resolve_context(
    state: &SharedState,
    headers: &axum::http::HeaderMap,
) -> Result<RequestContext, ApiError> {
    let session = match context::session_token(headers) {
        None => None,
        Some(token) => {
            on_storage(state, move |storage| store::get_session(storage, &token)).await?
        }
    };
    Ok(RequestContext::new(headers, session))
}

/// Apply the setup, session, CSRF, and role checks in that order.
async fn authorize(
    state: &SharedState,
    context: &RequestContext,
    path: &str,
    method: &str,
) -> Result<(), ApiError> {
    let is_api = path.starts_with("/api/");

    // Before the first user exists every non-public API route reports
    // setup_required, which is what drives the SPA's setup flow.
    if is_api && !gate::is_public_path(path) && on_storage(state, store::count_users).await? == 0 {
        return Err(ApiError::SetupRequired);
    }

    // Static assets and the public endpoints need nothing further. Public
    // state-changing routes are login and setup, both CSRF-exempt by nature.
    if !is_api || gate::is_public_path(path) {
        return Ok(());
    }

    if !context.is_authenticated() {
        return Err(ApiError::Unauthenticated);
    }
    if gate::is_state_changing(method) && !gate::is_csrf_exempt(path) && !context.csrf_valid() {
        return Err(ApiError::Forbidden("csrf_failed"));
    }
    if gate::requires_admin(path, method)
        && context.role() != Some(crate::dashboard::auth::model::Role::Admin)
    {
        return Err(ApiError::Forbidden("forbidden"));
    }
    Ok(())
}
