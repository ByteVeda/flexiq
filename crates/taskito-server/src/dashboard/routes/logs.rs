//! Cross-job log search.

use axum::extract::State;
use axum::Json;
use serde_json::Value;
use taskito_core::{now_millis, Storage};

use crate::dashboard::blocking::on_storage;
use crate::dashboard::dto;
use crate::dashboard::error::ApiResult;
use crate::dashboard::query::Params;
use crate::dashboard::state::SharedState;

/// `GET /api/logs` — recent log lines, filtered by task and level.
pub async fn query(State(state): State<SharedState>, params: Params) -> ApiResult<Json<Value>> {
    let task_name = params.get("task").map(str::to_string);
    let level = params.get("level").map(str::to_string);
    let since_seconds = params.int("since", 3_600)?;
    let limit = params.int("limit", 100)?;
    let since_ms = now_millis().saturating_sub(since_seconds.saturating_mul(1_000));

    let namespace = state.namespace.clone();
    let logs = on_storage(&state, move |storage| {
        storage.query_task_logs(
            task_name.as_deref(),
            level.as_deref(),
            since_ms,
            limit,
            namespace.as_deref(),
        )
    })
    .await?;

    Ok(Json(Value::Array(logs.iter().map(dto::task_log).collect())))
}
