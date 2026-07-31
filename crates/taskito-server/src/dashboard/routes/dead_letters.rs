//! Dead-letter listing.

use axum::extract::{Path, State};
use axum::Json;
use serde_json::{json, Value};
use taskito_core::{now_millis, Storage};

use crate::dashboard::blocking::on_storage;
use crate::dashboard::dto;
use crate::dashboard::error::ApiResult;
use crate::dashboard::query::Params;
use crate::dashboard::state::SharedState;

/// `GET /api/dead-letters` — newest first, paginated.
pub async fn list(State(state): State<SharedState>, params: Params) -> ApiResult<Json<Value>> {
    let limit = params.int("limit", 20)?;
    let offset = params.int("offset", 0)?;
    let entries = on_storage(&state, move |storage| storage.list_dead(limit, offset)).await?;
    Ok(Json(Value::Array(
        entries.iter().map(dto::dead_job).collect(),
    )))
}

/// `POST /api/dead-letters/purge` — delete every entry.
///
/// The SDK dashboards purge with an age of zero, i.e. everything; the cutoff
/// is computed here so the semantics stay identical.
pub async fn purge(State(state): State<SharedState>) -> ApiResult<Json<Value>> {
    let purged = on_storage(&state, |storage| storage.purge_dead(now_millis())).await?;
    Ok(Json(json!({ "purged": purged })))
}

/// `POST /api/dead-letters/{dead_id}/retry` — re-enqueue one entry.
pub async fn retry(
    State(state): State<SharedState>,
    Path(dead_id): Path<String>,
) -> ApiResult<Json<Value>> {
    let new_job_id = on_storage(&state, move |storage| storage.retry_dead(&dead_id)).await?;
    Ok(Json(json!({ "new_job_id": new_job_id })))
}

/// `DELETE /api/dead-letters/{dead_id}` — drop one entry without retrying.
pub async fn delete(
    State(state): State<SharedState>,
    Path(dead_id): Path<String>,
) -> ApiResult<Json<Value>> {
    let deleted = on_storage(&state, move |storage| storage.delete_dead(&dead_id)).await?;
    Ok(Json(json!({ "deleted": deleted })))
}
