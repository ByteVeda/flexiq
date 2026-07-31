//! Pub/sub topic views and subscription controls.

use axum::extract::{Path, State};
use axum::Json;
use serde_json::{json, Value};
use taskito_core::Storage;

use crate::dashboard::blocking::on_storage;
use crate::dashboard::dto;
use crate::dashboard::error::ApiResult;
use crate::dashboard::state::SharedState;

/// `GET /api/topics` — one row per topic, folded across its subscriptions.
///
/// Backlog sums pending + running so an operator sees a topic's total in-flight
/// work at a glance; `dead` sums DLQ depth across subscribers.
pub async fn list(State(state): State<SharedState>) -> ApiResult<Json<Value>> {
    let rows = on_storage(&state, |storage| storage.topic_backlog_stats()).await?;

    // Insertion-ordered so the response is stable between polls.
    let mut order = Vec::new();
    let mut totals: std::collections::HashMap<String, (i64, i64, i64)> =
        std::collections::HashMap::new();
    for row in &rows {
        let entry = totals.entry(row.topic.clone()).or_insert_with(|| {
            order.push(row.topic.clone());
            (0, 0, 0)
        });
        entry.0 += 1;
        entry.1 += row.pending + row.running;
        entry.2 += row.dead;
    }

    let topics: Vec<Value> = order
        .into_iter()
        .map(|topic| {
            let (subscriptions, backlog, dead) = totals[&topic];
            json!({
                "topic": topic,
                "subscription_count": subscriptions,
                "backlog": backlog,
                "dead": dead,
            })
        })
        .collect();
    Ok(Json(Value::Array(topics)))
}

/// `GET /api/topics/{topic}` — per-subscription rows for one topic.
pub async fn detail(
    State(state): State<SharedState>,
    Path(topic): Path<String>,
) -> ApiResult<Json<Value>> {
    let rows = on_storage(&state, |storage| storage.topic_backlog_stats()).await?;
    let matching: Vec<Value> = rows
        .iter()
        .filter(|row| row.topic == topic)
        .map(dto::subscription_stats)
        .collect();
    Ok(Json(Value::Array(matching)))
}

/// `POST /api/topics/{topic}/subscriptions/{name}/pause`.
pub async fn pause(
    State(state): State<SharedState>,
    Path((topic, name)): Path<(String, String)>,
) -> ApiResult<Json<Value>> {
    let changed = set_active(&state, topic, name, false).await?;
    Ok(Json(json!({ "paused": changed })))
}

/// `POST /api/topics/{topic}/subscriptions/{name}/resume`.
pub async fn resume(
    State(state): State<SharedState>,
    Path((topic, name)): Path<(String, String)>,
) -> ApiResult<Json<Value>> {
    let changed = set_active(&state, topic, name, true).await?;
    Ok(Json(json!({ "active": changed })))
}

/// `DELETE /api/topics/{topic}/subscriptions/{name}`.
pub async fn unsubscribe(
    State(state): State<SharedState>,
    Path((topic, name)): Path<(String, String)>,
) -> ApiResult<Json<Value>> {
    let removed = on_storage(&state, move |storage| storage.unsubscribe(&topic, &name)).await?;
    Ok(Json(json!({ "unsubscribed": removed })))
}

async fn set_active(
    state: &SharedState,
    topic: String,
    name: String,
    active: bool,
) -> ApiResult<bool> {
    on_storage(state, move |storage| {
        storage.set_subscription_active(&topic, &name, active)
    })
    .await
}
