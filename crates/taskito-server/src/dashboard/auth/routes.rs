//! Session endpoints: status, setup, login, logout, whoami, change password.

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde_json::{json, Value};

use crate::config::dashboard::AuthMode;
use crate::dashboard::auth::context::RequestContext;
use crate::dashboard::auth::model::{validate_password, validate_username, Role};
use crate::dashboard::auth::throttle::{ClientAddr, LoginThrottle};
use crate::dashboard::auth::{cookies, store};
use crate::dashboard::blocking::{on_storage, on_storage_api};
use crate::dashboard::error::{ApiError, ApiResult};
use crate::dashboard::state::SharedState;

/// `GET /api/auth/status` — public: tells the SPA which shell to render.
pub async fn status(State(state): State<SharedState>) -> ApiResult<Json<Value>> {
    let enabled = state.config.auth == AuthMode::Session;
    let setup_required = enabled && on_storage(&state, store::count_users).await? == 0;
    Ok(Json(json!({
        "auth_enabled": enabled,
        "setup_required": setup_required,
    })))
}

/// `POST /api/auth/setup` — create the first admin. Only while none exists.
pub async fn setup(
    State(state): State<SharedState>,
    Json(body): Json<Value>,
) -> ApiResult<Json<Value>> {
    let username = required_field(&body, "username")?;
    let password = required_field(&body, "password")?;
    validate_username(&username).map_err(ApiError::BadRequest)?;
    validate_password(&password).map_err(ApiError::BadRequest)?;

    let user = on_storage_api(&state, move |storage| {
        // Re-checked inside the blocking task: two concurrent setup requests
        // would otherwise both see zero users and both create an admin.
        if store::count_users(storage)? > 0 {
            return Err(ApiError::BadRequest("setup already complete".into()));
        }
        store::create_user(storage, &username, &password, Role::Admin)?
            .map_err(ApiError::BadRequest)
    })
    .await?;

    Ok(Json(json!({ "user": user.to_api_json() })))
}

/// `POST /api/auth/login` — verify credentials and start a session.
///
/// Unknown user and wrong password return the same error: distinguishing them
/// is an account-enumeration oracle.
pub async fn login(
    State(state): State<SharedState>,
    client: ClientAddr,
    Json(body): Json<Value>,
) -> ApiResult<Response> {
    let username = required_field(&body, "username")?;
    let password = required_field(&body, "password")?;

    // Keyed by client and username together: probing one account must not lock
    // its real owner out from somewhere else, and one client must not get a
    // fresh budget per username it invents.
    let throttle_key = LoginThrottle::key(client.label().as_deref(), &username);
    if let Err(wait) = state.login_throttle.check(&throttle_key) {
        return Err(ApiError::TooManyAttempts(wait.as_secs().max(1)));
    }

    let attempt = on_storage_api(&state, move |storage| {
        if store::count_users(storage)? == 0 {
            return Err(ApiError::BadRequest("setup_required".into()));
        }
        let user = store::authenticate(storage, &username, &password)?
            .ok_or_else(|| ApiError::BadRequest("invalid_credentials".into()))?;
        let session = store::create_session(storage, &user)?;
        Ok((user, session))
    })
    .await;

    let (user, session) = match attempt {
        Ok(authenticated) => {
            state.login_throttle.clear(&throttle_key);
            authenticated
        }
        Err(error) => {
            // Only a rejected credential counts; a storage fault is not the
            // caller's doing and must not lock them out.
            if matches!(&error, ApiError::BadRequest(reason) if reason == "invalid_credentials") {
                state.login_throttle.record_failure(&throttle_key);
            }
            return Err(error);
        }
    };

    // The token rides in the HttpOnly cookie and never in the body.
    let body = json!({
        "user": user.to_api_json(),
        "session": session.to_api_json(),
    });
    Ok((
        cookies::established(&session, state.config.secure_cookies),
        Json(body),
    )
        .into_response())
}

/// `POST /api/auth/logout` — invalidate the session. Idempotent.
pub async fn logout(
    State(state): State<SharedState>,
    Extension(context): Extension<RequestContext>,
) -> ApiResult<Response> {
    if let Some(session) = context.session {
        on_storage(&state, move |storage| {
            store::delete_session(storage, &session.token)
        })
        .await?;
    }
    Ok((
        cookies::cleared(state.config.secure_cookies),
        Json(json!({ "ok": true })),
    )
        .into_response())
}

/// `GET /api/auth/whoami` — the current user and their CSRF token.
pub async fn whoami(
    State(state): State<SharedState>,
    Extension(context): Extension<RequestContext>,
) -> ApiResult<Json<Value>> {
    let session = context
        .session
        .ok_or_else(|| ApiError::NotFound("not_authenticated".into()))?;

    let lookup = session.username.clone();
    let token = session.token.clone();
    let user = on_storage_api(&state, move |storage| {
        match store::get_user(storage, &lookup)? {
            Some(user) => Ok(user),
            None => {
                // The session outlived its user: invalidate rather than
                // leaving a credential that names nobody.
                store::delete_session(storage, &token)?;
                Err(ApiError::NotFound("not_authenticated".into()))
            }
        }
    })
    .await?;

    Ok(Json(json!({
        "user": user.to_api_json(),
        "csrf_token": session.csrf_token,
        "expires_at": session.expires_at,
    })))
}

/// `POST /api/auth/change-password` — requires the current password.
pub async fn change_password(
    State(state): State<SharedState>,
    Extension(context): Extension<RequestContext>,
    Json(body): Json<Value>,
) -> ApiResult<Json<Value>> {
    let session = context
        .session
        .ok_or_else(|| ApiError::BadRequest("not_authenticated".into()))?;
    let old_password = required_field(&body, "old_password")?;
    let new_password = required_field(&body, "new_password")?;
    validate_password(&new_password).map_err(ApiError::BadRequest)?;

    let revoked = on_storage_api(&state, move |storage| {
        let user = store::authenticate(storage, &session.username, &old_password)?
            .ok_or_else(|| ApiError::BadRequest("invalid_credentials".into()))?;
        store::update_password(storage, &user.username, &new_password)?
            .map_err(ApiError::BadRequest)?;
        // The usual reason to change a password is that something leaked, and a
        // session minted with the old one would otherwise keep working for its
        // full TTL. The caller's own session survives.
        Ok(store::revoke_sessions_for(
            storage,
            &user.username,
            Some(&session.token),
        )?)
    })
    .await?;

    Ok(Json(json!({ "ok": true, "sessions_revoked": revoked })))
}

fn required_field(body: &Value, field: &str) -> ApiResult<String> {
    body.get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| ApiError::BadRequest(format!("missing or empty field '{field}'")))
}
