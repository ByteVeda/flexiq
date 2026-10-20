//! Retention policy: what the elected cleaner published, what a purge would
//! delete right now, and the dashboard's override of the code config.

use axum::extract::State;
use axum::Json;
use flexiq_core::now_millis;
use flexiq_core::scheduler::retention::{
    dry_run, dry_run_applied, read_effective_retention, RetentionConfig,
};
use flexiq_core::scheduler::retention_override::{
    clear_retention_override, read_retention_override, write_retention_override, RetentionOverride,
};
use serde_json::{json, Value};

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
            let mut value = to_value(&snapshot)?;
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
            "windows": to_value(&RetentionConfig::default())?,
            "source": "code",
            "code_windows": Value::Null,
        }),
    };
    Ok(Json(body))
}

/// `GET /api/retention/dry-run` — counts, computed live, deleting nothing.
pub async fn preview(State(state): State<SharedState>) -> ApiResult<Json<Value>> {
    let namespace = state.namespace.clone();
    let maintenance = state.maintenance;

    let preview = on_storage(&state, move |storage| {
        // Preview what *this* process would apply: maintenance off keeps
        // everything and ignores an override, as its sweep does.
        if maintenance {
            dry_run_applied(storage, None, None, namespace.as_deref(), now_millis())
        } else {
            dry_run(
                storage,
                Some(&RetentionConfig::default()),
                None,
                namespace.as_deref(),
                now_millis(),
            )
        }
    })
    .await?;

    Ok(Json(to_value(&preview)?))
}

/// `GET /api/retention/override` — the stored override, or the `code` default
/// when none was saved. Applied by the cleaner on its next sweep.
pub async fn get_override(State(state): State<SharedState>) -> ApiResult<Json<Value>> {
    let namespace = state.namespace.clone();
    let document = on_storage(&state, move |storage| {
        read_retention_override(storage, namespace.as_deref())
    })
    .await?
    .unwrap_or_default();
    Ok(Json(to_value(&document)?))
}

/// `PUT /api/retention/override` — choose code or dashboard windows, and store
/// the dashboard's. Validated before anything is written.
pub async fn put_override(
    State(state): State<SharedState>,
    body: String,
) -> ApiResult<Json<Value>> {
    let document =
        RetentionOverride::from_request_json(&body, now_millis()).map_err(ApiError::BadRequest)?;
    let namespace = state.namespace.clone();
    let stored = document.clone();
    on_storage(&state, move |storage| {
        write_retention_override(storage, namespace.as_deref(), &stored)
    })
    .await?;
    Ok(Json(to_value(&document)?))
}

/// `DELETE /api/retention/override` — forget the override; the code config
/// applies from the next sweep.
pub async fn delete_override(State(state): State<SharedState>) -> ApiResult<Json<Value>> {
    let namespace = state.namespace.clone();
    let deleted = on_storage(&state, move |storage| {
        clear_retention_override(storage, namespace.as_deref())
    })
    .await?;
    Ok(Json(json!({ "deleted": deleted })))
}

fn to_value<T: serde::Serialize>(value: &T) -> ApiResult<Value> {
    serde_json::to_value(value).map_err(|error| ApiError::Internal(error.into()))
}
