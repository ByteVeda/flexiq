//! Task and queue inventory, plus the runtime overrides applied to them.
//!
//! An SDK dashboard lists tasks from its in-process registry. This process runs
//! no user code, so the inventory is assembled from what it can observe: the
//! tasks attached executors advertise, the queues that have jobs or are
//! configured, and any subject that already carries an override.

use axum::extract::{Path, State};
use axum::Json;
use serde_json::{json, Map, Value};
use taskito_core::Storage;

use crate::dashboard::blocking::{on_storage, on_storage_api};
use crate::dashboard::error::{ApiError, ApiResult};
use crate::dashboard::state::SharedState;
use crate::dashboard::stores::overrides::{self, Scope};

/// `GET /api/tasks` — every task this scheduler could dispatch, with its
/// override.
pub async fn list_tasks(State(state): State<SharedState>) -> ApiResult<Json<Value>> {
    let advertised = state
        .dispatcher
        .as_ref()
        .map(|dispatcher| {
            dispatcher
                .executors()
                .into_iter()
                .flat_map(|executor| executor.tasks)
                .collect::<std::collections::BTreeSet<String>>()
        })
        .unwrap_or_default();

    let stored = on_storage(&state, |storage| overrides::list(Scope::Task, storage)).await?;
    let mut names: std::collections::BTreeSet<String> = advertised.clone();
    names.extend(stored.iter().map(|(name, _)| name.clone()));

    let by_name: std::collections::BTreeMap<String, Map<String, Value>> =
        stored.into_iter().collect();
    let rows: Vec<Value> = names
        .into_iter()
        .map(|name| {
            let override_row = by_name
                .get(&name)
                .map(|fields| overrides::to_api_json(Scope::Task, &name, fields));
            json!({
                "name": name,
                // Decorator defaults live with the code, which this process
                // never loads; the executor advertising the task is what makes
                // it dispatchable.
                "advertised": advertised.contains(&name),
                "override": override_row,
            })
        })
        .collect();
    Ok(Json(Value::Array(rows)))
}

/// `GET /api/queues` — every known queue, with its override and pause state.
pub async fn list_queues(State(state): State<SharedState>) -> ApiResult<Json<Value>> {
    let configured = state.queues.clone();
    let (per_queue, paused, stored) = on_storage(&state, |storage| {
        Ok((
            storage.stats_all_queues()?,
            storage.list_paused_queues()?,
            overrides::list(Scope::Queue, storage)?,
        ))
    })
    .await?;

    let mut names: std::collections::BTreeSet<String> = configured.into_iter().collect();
    names.extend(per_queue.keys().cloned());
    names.extend(paused.iter().cloned());
    names.extend(stored.iter().map(|(name, _)| name.clone()));

    let by_name: std::collections::BTreeMap<String, Map<String, Value>> =
        stored.into_iter().collect();
    let rows: Vec<Value> = names
        .into_iter()
        .map(|name| {
            let stats = per_queue.get(&name);
            json!({
                "name": name,
                "paused": paused.contains(&name),
                "pending": stats.map(|stats| stats.pending).unwrap_or(0),
                "running": stats.map(|stats| stats.running).unwrap_or(0),
                "override": by_name
                    .get(&name)
                    .map(|fields| overrides::to_api_json(Scope::Queue, &name, fields)),
            })
        })
        .collect();
    Ok(Json(Value::Array(rows)))
}

/// `GET /api/tasks/{task_name}/override`.
pub async fn get_task(
    State(state): State<SharedState>,
    Path(task_name): Path<String>,
) -> ApiResult<Json<Value>> {
    get_override(state, Scope::Task, task_name, "task").await
}

/// `PUT /api/tasks/{task_name}/override`.
pub async fn put_task(
    State(state): State<SharedState>,
    Path(task_name): Path<String>,
    Json(body): Json<Value>,
) -> ApiResult<Json<Value>> {
    put_override(state, Scope::Task, task_name, body).await
}

/// `DELETE /api/tasks/{task_name}/override`.
pub async fn delete_task(
    State(state): State<SharedState>,
    Path(task_name): Path<String>,
) -> ApiResult<Json<Value>> {
    clear_override(state, Scope::Task, task_name).await
}

/// `GET /api/queues/{queue_name}/override`.
pub async fn get_queue(
    State(state): State<SharedState>,
    Path(queue_name): Path<String>,
) -> ApiResult<Json<Value>> {
    get_override(state, Scope::Queue, queue_name, "queue").await
}

/// `PUT /api/queues/{queue_name}/override`.
///
/// A `paused` change is also applied to the live pause state, which — unlike
/// the rest of an override — a running worker observes immediately.
pub async fn put_queue(
    State(state): State<SharedState>,
    Path(queue_name): Path<String>,
    Json(body): Json<Value>,
) -> ApiResult<Json<Value>> {
    let paused = body.get("paused").and_then(Value::as_bool);
    let response = put_override(state.clone(), Scope::Queue, queue_name.clone(), body).await?;
    if let Some(paused) = paused {
        on_storage(&state, move |storage| {
            if paused {
                storage.pause_queue(&queue_name)
            } else {
                storage.resume_queue(&queue_name)
            }
        })
        .await?;
    }
    Ok(response)
}

/// `DELETE /api/queues/{queue_name}/override`.
pub async fn delete_queue(
    State(state): State<SharedState>,
    Path(queue_name): Path<String>,
) -> ApiResult<Json<Value>> {
    clear_override(state, Scope::Queue, queue_name).await
}

async fn get_override(
    state: SharedState,
    scope: Scope,
    name: String,
    label: &str,
) -> ApiResult<Json<Value>> {
    let lookup = name.clone();
    let stored = on_storage(&state, move |storage| {
        overrides::get(scope, storage, &lookup)
    })
    .await?
    .ok_or_else(|| ApiError::NotFound(format!("no override set for {label} '{name}'")))?;
    Ok(Json(overrides::to_api_json(scope, &name, &stored)))
}

async fn put_override(
    state: SharedState,
    scope: Scope,
    name: String,
    body: Value,
) -> ApiResult<Json<Value>> {
    let patch = body
        .as_object()
        .ok_or_else(|| ApiError::BadRequest("body must be a JSON object".into()))?
        .clone();
    let subject = name.clone();
    let stored = on_storage_api(&state, move |storage| {
        overrides::set(scope, storage, &subject, &patch)
    })
    .await?;
    Ok(Json(overrides::to_api_json(scope, &name, &stored)))
}

async fn clear_override(state: SharedState, scope: Scope, name: String) -> ApiResult<Json<Value>> {
    let cleared = on_storage(&state, move |storage| {
        overrides::clear(scope, storage, &name)
    })
    .await?;
    Ok(Json(json!({ "cleared": cleared })))
}
