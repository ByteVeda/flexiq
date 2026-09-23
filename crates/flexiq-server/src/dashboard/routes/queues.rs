//! Queue-level views: aggregate stats, workers, breakers, pause state, and the
//! KEDA scaler payload.

use axum::extract::{Path, State};
use axum::Json;
use flexiq_core::Storage;
use serde_json::{json, Map, Value};

use crate::dashboard::blocking::on_storage;
use crate::dashboard::dto;
use crate::dashboard::error::ApiResult;
use crate::dashboard::query::Params;
use crate::dashboard::state::SharedState;

/// Queue depth KEDA scales against by default.
const TARGET_QUEUE_DEPTH: i64 = 10;

/// `GET /api/stats` — status counts across every queue.
pub async fn stats(State(state): State<SharedState>) -> ApiResult<Json<Value>> {
    let namespace = state.namespace.clone();
    let stats = on_storage(&state, move |storage| storage.stats(namespace.as_deref())).await?;
    Ok(Json(dto::queue_stats(&stats)))
}

/// `GET /api/stats/queues` — one queue when `?queue=` is given, otherwise all.
pub async fn stats_by_queue(
    State(state): State<SharedState>,
    params: Params,
) -> ApiResult<Json<Value>> {
    match params.get("queue").map(str::to_string) {
        Some(queue) => {
            let namespace = state.namespace.clone();
            let stats = on_storage(&state, move |storage| {
                storage.stats_by_queue(&queue, namespace.as_deref())
            })
            .await?;
            Ok(Json(dto::queue_stats(&stats)))
        }
        None => {
            let namespace = state.namespace.clone();
            let per_queue = on_storage(&state, move |storage| {
                storage.stats_all_queues(namespace.as_deref())
            })
            .await?;
            let mut body = Map::new();
            for (queue, stats) in per_queue {
                body.insert(queue, dto::queue_stats(&stats));
            }
            Ok(Json(Value::Object(body)))
        }
    }
}

/// `GET /api/workers`.
pub async fn workers(State(state): State<SharedState>) -> ApiResult<Json<Value>> {
    let namespace = state.namespace.clone();
    let workers = on_storage(&state, move |storage| {
        storage.list_workers(namespace.as_deref())
    })
    .await?;
    Ok(Json(Value::Array(
        workers.iter().map(dto::worker).collect(),
    )))
}

/// `GET /api/circuit-breakers`.
pub async fn circuit_breakers(State(state): State<SharedState>) -> ApiResult<Json<Value>> {
    let breakers = on_storage(&state, |storage| storage.list_circuit_breakers()).await?;
    Ok(Json(Value::Array(
        breakers.iter().map(dto::circuit_breaker).collect(),
    )))
}

/// `GET /api/queues/paused`.
pub async fn paused(State(state): State<SharedState>) -> ApiResult<Json<Value>> {
    let namespace = state.namespace.clone();
    let paused = on_storage(&state, move |storage| {
        storage.list_paused_queues(namespace.as_deref())
    })
    .await?;
    Ok(Json(json!(paused)))
}

/// `POST /api/queues/{name}/pause` — stop dispatching from a queue.
pub async fn pause(
    State(state): State<SharedState>,
    Path(queue): Path<String>,
) -> ApiResult<Json<Value>> {
    let name = queue.clone();
    let namespace = state.namespace.clone();
    on_storage(&state, move |storage| {
        storage.pause_queue(&name, namespace.as_deref())
    })
    .await?;
    Ok(Json(json!({ "paused": queue })))
}

/// `POST /api/queues/{name}/resume`.
pub async fn resume(
    State(state): State<SharedState>,
    Path(queue): Path<String>,
) -> ApiResult<Json<Value>> {
    let name = queue.clone();
    let namespace = state.namespace.clone();
    on_storage(&state, move |storage| {
        storage.resume_queue(&name, namespace.as_deref())
    })
    .await?;
    Ok(Json(json!({ "resumed": queue })))
}

/// `GET /api/scaler` — the KEDA external-scaler payload.
///
/// `totalCapacity` is the execution capacity attached executors advertise; in
/// this process there is no in-process worker pool to report instead.
///
/// Under push dispatch there is no attach dispatcher either — `state.dispatcher`
/// is `None` — so it reports `0` and omits `workerUtilization`. An autoscaler
/// cannot scale a push deployment on utilization; `metricValue` (queue depth)
/// is read from storage and is the field that stays correct under both paths.
pub async fn scaler(State(state): State<SharedState>, params: Params) -> ApiResult<Json<Value>> {
    let namespace = state.namespace.clone();
    let overall = {
        let namespace = namespace.clone();
        on_storage(&state, move |storage| storage.stats(namespace.as_deref())).await?
    };
    let workers = {
        let namespace = namespace.clone();
        on_storage(&state, move |storage| {
            storage.list_workers(namespace.as_deref())
        })
        .await?
    };
    let per_queue = on_storage(&state, move |storage| {
        storage.stats_all_queues(namespace.as_deref())
    })
    .await?;

    let total_capacity = state
        .dispatcher
        .as_ref()
        .map(|dispatcher| dispatcher.capacity().total_slots as i64)
        .unwrap_or(0);

    let mut body = Map::new();
    body.insert("metricName".into(), json!("flexiq_queue_depth"));
    body.insert("metricValue".into(), json!(overall.pending));
    body.insert("isActive".into(), json!(overall.pending > 0));
    body.insert("liveWorkers".into(), json!(workers.len()));
    body.insert("totalCapacity".into(), json!(total_capacity));
    body.insert("targetQueueDepth".into(), json!(TARGET_QUEUE_DEPTH));
    if total_capacity > 0 {
        let utilization = overall.running as f64 / total_capacity as f64;
        body.insert(
            "workerUtilization".into(),
            json!((utilization * 1_000.0).round() / 1_000.0),
        );
    }

    // A queue filter narrows the metric the autoscaler reads, and renames it so
    // two scaled objects watching different queues never collide.
    if let Some(queue) = params.get("queue") {
        let pending = per_queue.get(queue).map(|stats| stats.pending).unwrap_or(0);
        body.insert(
            "metricName".into(),
            json!(format!("flexiq_queue_depth_{queue}")),
        );
        body.insert("metricValue".into(), json!(pending));
        body.insert("isActive".into(), json!(pending > 0));
    }

    let mut breakdown = Map::new();
    for (queue, stats) in per_queue {
        breakdown.insert(
            queue,
            json!({ "pending": stats.pending, "running": stats.running }),
        );
    }
    body.insert("perQueue".into(), Value::Object(breakdown));

    Ok(Json(Value::Object(body)))
}
