//! Per-task middleware toggles.
//!
//! The middleware objects themselves live in the app's process, so this server
//! can neither enumerate them nor validate a name against a chain. What it can
//! do is own the disable list workers consult on every invocation — which is
//! the part an operator actually toggles. `GET /api/middleware` therefore
//! reports what is disabled, not what exists.

use axum::extract::{Path, State};
use axum::Json;
use serde_json::{json, Value};

use crate::dashboard::blocking::on_storage;
use crate::dashboard::error::{ApiError, ApiResult};
use crate::dashboard::state::SharedState;
use crate::dashboard::stores::middleware;

/// `GET /api/middleware` — every task with at least one disabled middleware.
pub async fn list(State(state): State<SharedState>) -> ApiResult<Json<Value>> {
    let disabled = on_storage(&state, middleware::list_all).await?;
    let rows: Vec<Value> = disabled
        .into_iter()
        .map(|(task, names)| json!({ "task": task, "disabled": names }))
        .collect();
    Ok(Json(Value::Array(rows)))
}

/// `GET /api/tasks/{task_name}/middleware` — the task's disabled entries.
///
/// `class_path` is null and `effective` is false throughout: only the process
/// holding the middleware chain knows those, and guessing would be worse than
/// saying nothing.
pub async fn for_task(
    State(state): State<SharedState>,
    Path(task_name): Path<String>,
) -> ApiResult<Json<Value>> {
    let lookup = task_name.clone();
    let disabled = on_storage(&state, move |storage| middleware::get_for(storage, &lookup)).await?;

    let entries: Vec<Value> = disabled
        .into_iter()
        .map(|name| {
            json!({
                "name": name,
                "class_path": Value::Null,
                "disabled": true,
                "effective": false,
            })
        })
        .collect();
    Ok(Json(json!({ "task": task_name, "middleware": entries })))
}

/// `PUT /api/tasks/{task_name}/middleware/{name}` — body `{"enabled": bool}`.
pub async fn set(
    State(state): State<SharedState>,
    Path((task_name, middleware_name)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> ApiResult<Json<Value>> {
    let enabled = body
        .get("enabled")
        .and_then(Value::as_bool)
        .ok_or_else(|| ApiError::BadRequest(r#"body must include {"enabled": bool}"#.into()))?;
    if task_name.is_empty() || middleware_name.is_empty() {
        return Err(ApiError::BadRequest(
            "task and middleware names must not be empty".into(),
        ));
    }

    let subject = task_name.clone();
    let disabled = on_storage(&state, move |storage| {
        middleware::set_disabled(storage, &subject, &middleware_name, !enabled)
    })
    .await?;
    Ok(Json(json!({ "task": task_name, "disabled": disabled })))
}

/// `DELETE /api/tasks/{task_name}/middleware` — clear every disable.
pub async fn clear(
    State(state): State<SharedState>,
    Path(task_name): Path<String>,
) -> ApiResult<Json<Value>> {
    let cleared = on_storage(&state, move |storage| {
        middleware::clear_for(storage, &task_name)
    })
    .await?;
    Ok(Json(json!({ "cleared": cleared })))
}
