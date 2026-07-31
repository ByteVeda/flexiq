//! The dashboard key/value settings store.
//!
//! Reserved namespaces (`auth:`, `webhooks:`, `retention:`) hold password
//! hashes, live sessions, signing secrets, and the cleaner's published policy.
//! They are treated as **absent** here rather than forbidden, so the API never
//! confirms a protected key exists, and the reserved list comes from the core
//! so every server hides exactly the same keys.

use axum::extract::{Path, State};
use axum::Json;
use serde_json::{json, Map, Value};
use taskito_core::{is_reserved_setting_key, Storage};

use crate::dashboard::blocking::on_storage;
use crate::dashboard::error::{ApiError, ApiResult};
use crate::dashboard::state::SharedState;

const MAX_KEY_LENGTH: usize = 256;
/// Enough for any realistic dashboard config blob.
const MAX_VALUE_BYTES: usize = 64 * 1024;

/// `GET /api/settings` — every visible setting as `{key: value}`.
pub async fn list(State(state): State<SharedState>) -> ApiResult<Json<Value>> {
    let settings = on_storage(&state, |storage| storage.list_settings()).await?;
    let mut body = Map::new();
    for (key, value) in settings {
        if !is_reserved_setting_key(&key) {
            body.insert(key, Value::String(value));
        }
    }
    Ok(Json(Value::Object(body)))
}

/// `GET /api/settings/{key}`.
pub async fn get(
    State(state): State<SharedState>,
    Path(key): Path<String>,
) -> ApiResult<Json<Value>> {
    if is_reserved_setting_key(&key) {
        return Err(missing(&key));
    }
    let lookup = key.clone();
    let value = on_storage(&state, move |storage| storage.get_setting(&lookup))
        .await?
        .ok_or_else(|| missing(&key))?;
    Ok(Json(json!({ "key": key, "value": value })))
}

/// `PUT /api/settings/{key}` — body `{"value": ...}`.
pub async fn set(
    State(state): State<SharedState>,
    Path(key): Path<String>,
    Json(body): Json<Value>,
) -> ApiResult<Json<Value>> {
    validate_key(&key)?;
    let raw = body.get("value").ok_or_else(|| {
        ApiError::BadRequest("body must be a JSON object with a 'value' field".into())
    })?;

    // Any JSON value is accepted; non-strings are re-encoded compactly so
    // callers never have to stringify themselves.
    let value = match raw {
        Value::String(text) => text.clone(),
        other => serde_json::to_string(other)
            .map_err(|error| ApiError::BadRequest(format!("value is not encodable: {error}")))?,
    };
    if value.len() > MAX_VALUE_BYTES {
        return Err(ApiError::BadRequest(format!(
            "setting value exceeds {MAX_VALUE_BYTES} bytes"
        )));
    }

    let (stored_key, stored_value) = (key.clone(), value.clone());
    on_storage(&state, move |storage| {
        storage.set_setting(&stored_key, &stored_value)
    })
    .await?;
    Ok(Json(json!({ "key": key, "value": value })))
}

/// `DELETE /api/settings/{key}`.
pub async fn delete(
    State(state): State<SharedState>,
    Path(key): Path<String>,
) -> ApiResult<Json<Value>> {
    if is_reserved_setting_key(&key) {
        return Err(missing(&key));
    }
    let deleted = on_storage(&state, move |storage| storage.delete_setting(&key)).await?;
    Ok(Json(json!({ "deleted": deleted })))
}

fn missing(key: &str) -> ApiError {
    ApiError::NotFound(format!("setting '{key}' not found"))
}

fn validate_key(key: &str) -> ApiResult<()> {
    if key.is_empty() {
        return Err(ApiError::BadRequest("setting key must not be empty".into()));
    }
    if key.len() > MAX_KEY_LENGTH {
        return Err(ApiError::BadRequest(format!(
            "setting key exceeds {MAX_KEY_LENGTH} characters"
        )));
    }
    // Control characters would corrupt any log line or config file the key is
    // ever written into.
    if key.chars().any(|c| (c as u32) < 32 || c as u32 == 127) {
        return Err(ApiError::BadRequest(
            "setting key must not contain control characters".into(),
        ));
    }
    if is_reserved_setting_key(key) {
        return Err(ApiError::BadRequest("setting key is reserved".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_keys_cannot_be_written() {
        assert!(matches!(
            validate_key("auth:users"),
            Err(ApiError::BadRequest(_))
        ));
    }

    #[test]
    fn control_characters_and_empty_keys_are_rejected() {
        assert!(matches!(validate_key(""), Err(ApiError::BadRequest(_))));
        assert!(matches!(
            validate_key("bad\nkey"),
            Err(ApiError::BadRequest(_))
        ));
        assert!(matches!(
            validate_key(&"k".repeat(MAX_KEY_LENGTH + 1)),
            Err(ApiError::BadRequest(_))
        ));
    }

    #[test]
    fn an_ordinary_key_passes() {
        validate_key("dashboard:links").expect("ordinary keys are writable");
    }
}
