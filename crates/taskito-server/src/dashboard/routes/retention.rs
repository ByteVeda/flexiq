//! Retention policy: what the elected cleaner published, and what a purge
//! would delete right now.

use axum::extract::State;
use axum::Json;
use serde_json::{json, Value};
use taskito_core::now_millis;
use taskito_core::scheduler::retention::{dry_run, read_effective_retention, RetentionConfig};

use crate::dashboard::blocking::on_storage;
use crate::dashboard::error::{ApiError, ApiResult};
use crate::dashboard::state::SharedState;

/// `GET /api/retention` — the policy a cleaner reported for this namespace.
///
/// `reported` is distinct from `enabled`: no cleaner having swept yet is not
/// the same as retention being switched off, and conflating them would tell an
/// operator their history is safe when nothing has looked at it.
pub async fn published(State(state): State<SharedState>) -> ApiResult<Json<Value>> {
    let namespace = state.namespace.clone();
    let snapshot = on_storage(&state, move |storage| {
        read_effective_retention(storage, namespace.as_deref())
    })
    .await?;

    let body = match snapshot {
        Some(snapshot) => {
            let mut value = serde_json::to_value(&snapshot)
                .map_err(|error| ApiError::Internal(error.into()))?;
            if let Some(object) = value.as_object_mut() {
                object.insert("reported".into(), json!(true));
            }
            value
        }
        None => json!({
            "reported": false,
            "enabled": false,
            "defaulted": false,
            "namespace": Value::Null,
            "reported_at": Value::Null,
            "windows": serde_json::to_value(RetentionConfig::default())
                .map_err(|error| ApiError::Internal(error.into()))?,
        }),
    };
    Ok(Json(body))
}

/// `GET /api/retention/dry-run` — counts, computed live, deleting nothing.
pub async fn preview(State(state): State<SharedState>) -> ApiResult<Json<Value>> {
    let namespace = state.namespace.clone();
    // Preview what *this* process would apply: maintenance off means an empty
    // policy, which keeps everything.
    let windows = (!state.maintenance).then(RetentionConfig::default);

    let preview = on_storage(&state, move |storage| {
        dry_run(
            storage,
            windows.as_ref(),
            None,
            namespace.as_deref(),
            now_millis(),
        )
    })
    .await?;

    Ok(Json(
        serde_json::to_value(&preview).map_err(|error| ApiError::Internal(error.into()))?,
    ))
}
