//! Provider login: `/api/auth/providers`, `/oauth/start/{slot}`, and
//! `/oauth/callback/{slot}`.
//!
//! Unlike the rest of the API these endpoints answer with redirects, because
//! the browser — not the SPA's fetch client — is what walks the flow.

pub mod config;
pub mod providers;
pub mod state;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::dashboard::auth::oauth::providers::{OAuthError, OAuthRuntime};
use crate::dashboard::auth::{cookies, store};
use crate::dashboard::blocking::{on_storage, on_storage_api};
use crate::dashboard::error::{ApiError, ApiResult};
use crate::dashboard::query::Params;
use crate::dashboard::state::SharedState;
use crate::dashboard::stores::url_safety::is_safe_redirect;

/// Where the browser lands when a login cannot complete. The SPA reads the
/// `error` parameter and explains what happened.
const LOGIN_STATE_INVALID: &str = "/login?error=oauth_state_invalid";
const LOGIN_FAILED: &str = "/login?error=oauth_failed";
const LOGIN_DENIED: &str = "/login?error=oauth_denied";

/// The provider-login endpoints.
pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/api/auth/providers", get(list_providers))
        .route("/api/auth/oauth/start/{slot}", get(start))
        .route("/api/auth/oauth/callback/{slot}", get(callback))
}

/// `GET /api/auth/providers` — what the login screen should offer.
///
/// Always answers, so the SPA can render a password-only login without having
/// to special-case an unconfigured deployment.
pub async fn list_providers(State(state): State<SharedState>) -> Json<Value> {
    match &state.oauth {
        None => Json(json!({ "password_enabled": true, "providers": [] })),
        Some(runtime) => Json(json!({
            "password_enabled": runtime.config.password_auth_enabled,
            "providers": runtime.listing(),
        })),
    }
}

/// `GET /api/auth/oauth/start/{slot}` — mint state and bounce to the provider.
pub async fn start(
    State(state): State<SharedState>,
    Path(slot): Path<String>,
    params: Params,
) -> ApiResult<Response> {
    let runtime = require_runtime(&state)?;
    let provider = runtime.provider(&slot).map_err(not_configured)?.clone();

    // An attacker-supplied `next` is an open redirect if it is trusted; only
    // same-origin paths survive.
    let next_url = params
        .get("next")
        .filter(|candidate| is_safe_redirect(Some(candidate)))
        .unwrap_or("/")
        .to_string();

    let (token, row) = on_storage(&state, move |storage| {
        state::create(storage, &slot, &next_url)
    })
    .await?;

    let challenge = state::s256_challenge(&row.code_verifier);
    let redirect_uri = runtime.config.callback_url(&row.slot);
    let url = runtime
        .authorization_url(&provider, &token, &row.nonce, &challenge, &redirect_uri)
        .await
        .map_err(|error| {
            log::warn!(
                "building the authorize URL for '{}' failed: {error}",
                row.slot
            );
            ApiError::BadRequest("oauth_unavailable".into())
        })?;

    Ok(redirect_to(&url, HeaderMap::new()))
}

/// `GET /api/auth/oauth/callback/{slot}` — verify, create a session, land home.
///
/// Failures redirect to the login screen rather than rendering an error page:
/// the browser got here from the provider, and a JSON body would be a dead end.
pub async fn callback(
    State(state): State<SharedState>,
    Path(slot): Path<String>,
    params: Params,
) -> ApiResult<Response> {
    let runtime = require_runtime(&state)?;
    let provider = runtime.provider(&slot).map_err(not_configured)?.clone();

    if let Some(error) = params.get("error") {
        log::warn!("provider '{slot}' returned an error at callback: {error}");
        return Ok(redirect_to(LOGIN_FAILED, HeaderMap::new()));
    }
    let (Some(code), Some(token)) = (params.get("code"), params.get("state")) else {
        return Ok(redirect_to(LOGIN_STATE_INVALID, HeaderMap::new()));
    };

    let lookup = token.to_string();
    let Some(row) = on_storage(&state, move |storage| state::consume(storage, &lookup)).await?
    else {
        return Ok(redirect_to(LOGIN_STATE_INVALID, HeaderMap::new()));
    };
    // A state minted for one provider must not be redeemed at another.
    if row.slot != slot {
        return Ok(redirect_to(LOGIN_STATE_INVALID, HeaderMap::new()));
    }

    let redirect_uri = runtime.config.callback_url(&slot);
    let identity = match runtime
        .exchange_code(
            &provider,
            code,
            &row.code_verifier,
            &redirect_uri,
            &row.nonce,
        )
        .await
        .and_then(|identity| {
            runtime.check_allowlist(&provider, &identity)?;
            Ok(identity)
        }) {
        Ok(identity) => identity,
        Err(error) => {
            // The reason is for the operator's log; the user gets a category.
            log::warn!("provider login on '{slot}' failed: {error}");
            let destination = match error {
                OAuthError::Denied(_) => LOGIN_DENIED,
                OAuthError::StateInvalid => LOGIN_STATE_INVALID,
                _ => LOGIN_FAILED,
            };
            return Ok(redirect_to(destination, HeaderMap::new()));
        }
    };

    let role = runtime.role_for_new_user(&identity);
    let session = on_storage_api(&state, move |storage| {
        let user = store::upsert_provider_user(
            storage,
            &identity.slot,
            &identity.subject,
            identity.email.as_deref(),
            identity.name.as_deref(),
            role,
        )?;
        Ok(store::create_session(storage, &user)?)
    })
    .await?;

    // Abandoned flows accumulate; clearing them here keeps the sweep on the
    // path that creates them.
    if on_storage(&state, state::prune_expired).await.is_err() {
        log::warn!("pruning expired OAuth state failed");
    }

    Ok(redirect_to(
        &row.next_url,
        cookies::established(&session, state.config.secure_cookies),
    ))
}

fn require_runtime(state: &SharedState) -> ApiResult<&OAuthRuntime> {
    state
        .oauth
        .as_deref()
        .ok_or_else(|| ApiError::NotFound("oauth_not_configured".into()))
}

fn not_configured(error: OAuthError) -> ApiError {
    ApiError::NotFound(error.to_string())
}

/// A 302 with no body, plus any cookies the caller wants attached.
fn redirect_to(url: &str, mut headers: HeaderMap) -> Response {
    if let Ok(location) = url.parse() {
        headers.insert("location", location);
    }
    headers.insert("cache-control", "no-store".parse().expect("static value"));
    (StatusCode::FOUND, headers).into_response()
}
