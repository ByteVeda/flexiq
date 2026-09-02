//! The gRPC API tokens an operator mints, lists and revokes.
//!
//! Admin-only without a line of its own: `auth::gate::requires_admin` classifies
//! every mutating `/api/` path, so `POST` and `DELETE` here are behind the same
//! role check as everything else, and a viewer's create is a 403 from the one
//! middleware rather than from a check this file could forget.
//!
//! Design doc §10.7 is the boundary in the other direction: this surface is
//! *not* reachable with a gRPC token. Minting a credential is an admin action
//! behind a session, and a producer credential must never be able to mint
//! itself a wider one.

use axum::extract::{Extension, Path, State};
use axum::Json;
use serde_json::{json, Map, Value};

use flexiq_core::now_millis;

use crate::dashboard::auth::context::RequestContext;
use crate::dashboard::blocking::on_storage;
use crate::dashboard::error::{ApiError, ApiResult};
use crate::dashboard::state::SharedState;
use crate::tokens::model::{mint_namespace, NewToken};
use crate::tokens::scope::{Scope, ScopeSet};
use crate::tokens::store;

/// `GET /api/grpc-tokens` — every token, newest first.
///
/// Scoped to the namespace this process serves, because the store is one global
/// keyspace: a dashboard for one tenant must not enumerate another's
/// credentials just because they share a database.
pub async fn list(State(state): State<SharedState>) -> ApiResult<Json<Value>> {
    let namespace = state.namespace.clone();
    let tokens = on_storage(&state, move |storage| {
        store::list(storage, namespace.as_deref())
    })
    .await?;
    let now = now_millis();
    Ok(Json(Value::Array(
        tokens.iter().map(|token| token.to_api_json(now)).collect(),
    )))
}

/// `GET /api/grpc-tokens/scopes` — what a token may be granted.
///
/// Served rather than hard-coded in the SPA so that adding a scope is one
/// change: the page renders what this build actually understands.
pub async fn scopes() -> Json<Value> {
    Json(json!(Scope::ALL
        .iter()
        .map(|scope| json!({ "name": scope.as_str() }))
        .collect::<Vec<_>>()))
}

/// `POST /api/grpc-tokens` — mint one, revealing the token exactly once.
pub async fn create(
    State(state): State<SharedState>,
    Extension(context): Extension<RequestContext>,
    Json(body): Json<Value>,
) -> ApiResult<Json<Value>> {
    let body = object(&body)?;
    let name = required_string(body, "name")?;
    let scopes = parse_scopes(body)?;
    let lifetime = optional_days(body, "expires_in_days")?;

    // §5.4: the namespace is the process's own. A requested one is accepted so
    // that the refusal is explicit rather than a silent substitution, and so
    // that multi-namespace credentials stay an additive change later.
    let requested = body.get("namespace").and_then(Value::as_str);
    let namespace =
        mint_namespace(state.namespace.as_deref(), requested).map_err(ApiError::BadRequest)?;

    let minted_by = context
        .session
        .as_ref()
        .map(|session| session.username.clone());
    let request = NewToken::new(name.trim(), scopes, &namespace, lifetime, minted_by)
        .map_err(ApiError::BadRequest)?;

    let (row, plaintext) =
        on_storage(&state, move |storage| store::create(storage, request)).await?;

    let now = now_millis();
    let mut rendered = row.to_api_json(now);
    // The one response that carries it. Every later read of this row — the
    // listing above, any other client's — has only the hash to work from.
    rendered["token"] = Value::String(plaintext);
    Ok(Json(rendered))
}

/// `DELETE /api/grpc-tokens/{id}` — revoke one.
///
/// It stops working on the next RPC, with no restart: the door reads the row
/// per call rather than caching a verdict.
///
/// Scoped to this process's namespace exactly as the listing is. Without that a
/// dashboard could revoke another tenant's credential, and could tell an id
/// that exists elsewhere from one that does not exist at all — the existence
/// oracle §5.3 refuses.
pub async fn revoke(
    State(state): State<SharedState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    let key = id.clone();
    let namespace = state.namespace.clone();
    let revoked = on_storage(&state, move |storage| {
        store::revoke(storage, &key, namespace.as_deref())
    })
    .await?;
    if !revoked {
        return Err(ApiError::NotFound(format!("no gRPC token with id '{id}'")));
    }
    Ok(Json(json!({ "id": id, "revoked": true })))
}

/// A JSON object body, or a 400 that says what was sent instead.
fn object(body: &Value) -> ApiResult<&Map<String, Value>> {
    body.as_object()
        .ok_or_else(|| ApiError::BadRequest("expected a JSON object".to_string()))
}

/// A required non-empty string field.
fn required_string(body: &Map<String, Value>, field: &str) -> ApiResult<String> {
    body.get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .ok_or_else(|| ApiError::BadRequest(format!("'{field}' is required")))
}

/// The `scopes` array, refusing a name this build does not know.
///
/// Refused rather than ignored, which is the opposite of how a *stored* row is
/// read (there, an unknown name narrows). The difference is who is asking: a
/// stored row may have been written by a newer build and must still work, while
/// an operator who typed a scope that does not exist has made a mistake and
/// would otherwise receive a credential quietly weaker than they asked for.
fn parse_scopes(body: &Map<String, Value>) -> ApiResult<ScopeSet> {
    let listed = body
        .get("scopes")
        .and_then(Value::as_array)
        .ok_or_else(|| ApiError::BadRequest("'scopes' must be an array".to_string()))?;
    let mut scopes = ScopeSet::NONE;
    for entry in listed {
        let name = entry
            .as_str()
            .ok_or_else(|| ApiError::BadRequest("each scope must be a string".to_string()))?;
        let scope = Scope::parse(name).ok_or_else(|| {
            ApiError::BadRequest(format!(
                "unknown scope '{name}'. Available: {}",
                Scope::names()
            ))
        })?;
        scopes.insert(scope);
    }
    Ok(scopes)
}

/// An optional whole-number day count.
fn optional_days(body: &Map<String, Value>, field: &str) -> ApiResult<Option<i64>> {
    match body.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value.as_i64().map(Some).ok_or_else(|| {
            ApiError::BadRequest(format!("'{field}' must be a whole number of days"))
        }),
    }
}
