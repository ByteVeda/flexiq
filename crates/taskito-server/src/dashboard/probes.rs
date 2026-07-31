//! Liveness, readiness, and Prometheus scrape endpoints.

use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde_json::{json, Value};
use taskito_core::Storage;

use crate::config::dashboard::AuthMode;
use crate::dashboard::auth::context::RequestContext;
use crate::dashboard::blocking::on_storage;
use crate::dashboard::error::{ApiError, ApiResult};
use crate::dashboard::security::constant_time_eq;
use crate::dashboard::state::SharedState;

/// Liveness: the process is up. Never gated — a probe that needs credentials
/// is a probe that fails during an outage.
pub async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

/// Readiness: storage answers and the worker registry is reachable.
pub async fn readiness(
    State(state): State<SharedState>,
    Extension(context): Extension<RequestContext>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    require_probe_access(&state, &headers, &context)?;

    let mut checks = serde_json::Map::new();
    let mut ready = true;

    match on_storage(&state, |storage| storage.stats()).await {
        Ok(_) => {
            checks.insert("storage".into(), json!("ok"));
        }
        Err(error) => {
            ready = false;
            checks.insert("storage".into(), json!(format!("error: {}", brief(&error))));
        }
    }

    match on_storage(&state, |storage| storage.list_workers()).await {
        Ok(workers) => {
            checks.insert(
                "workers".into(),
                json!({
                    "count": workers.len(),
                    "status": if workers.is_empty() { "none" } else { "ok" },
                }),
            );
        }
        Err(error) => {
            ready = false;
            checks.insert("workers".into(), json!(format!("error: {}", brief(&error))));
        }
    }

    Ok(Json(json!({
        "status": if ready { "ready" } else { "degraded" },
        "checks": Value::Object(checks),
    })))
}

/// Prometheus exposition derived from storage.
///
/// The SDK dashboards proxy an in-process client registry; this process runs no
/// user code and has no such registry, so it publishes what it can see — queue
/// depth, workers, and attached executor capacity.
pub async fn metrics(
    State(state): State<SharedState>,
    Extension(context): Extension<RequestContext>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    require_probe_access(&state, &headers, &context)?;

    let per_queue = on_storage(&state, |storage| storage.stats_all_queues()).await?;
    let workers = on_storage(&state, |storage| storage.list_workers()).await?;

    let mut body = String::new();
    body.push_str("# HELP taskito_jobs Jobs by queue and status.\n");
    body.push_str("# TYPE taskito_jobs gauge\n");
    let mut queues: Vec<_> = per_queue.into_iter().collect();
    queues.sort_by(|(left, _), (right, _)| left.cmp(right));
    for (queue, stats) in queues {
        for (status, count) in [
            ("pending", stats.pending),
            ("running", stats.running),
            ("completed", stats.completed),
            ("failed", stats.failed),
            ("dead", stats.dead),
            ("cancelled", stats.cancelled),
        ] {
            body.push_str(&format!(
                "taskito_jobs{{queue=\"{}\",status=\"{status}\"}} {count}\n",
                escape_label(&queue)
            ));
        }
    }

    body.push_str("# HELP taskito_workers Workers in the cluster registry.\n");
    body.push_str("# TYPE taskito_workers gauge\n");
    body.push_str(&format!("taskito_workers {}\n", workers.len()));

    if let Some(dispatcher) = &state.dispatcher {
        let capacity = dispatcher.capacity();
        body.push_str("# HELP taskito_executors Executors attached to this scheduler.\n");
        body.push_str("# TYPE taskito_executors gauge\n");
        body.push_str(&format!("taskito_executors {}\n", capacity.executors));
        body.push_str("# HELP taskito_executor_slots Execution slots advertised by executors.\n");
        body.push_str("# TYPE taskito_executor_slots gauge\n");
        body.push_str(&format!(
            "taskito_executor_slots{{state=\"total\"}} {}\n",
            capacity.total_slots
        ));
        body.push_str(&format!(
            "taskito_executor_slots{{state=\"free\"}} {}\n",
            capacity.free_slots
        ));
    }

    Ok((
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
        .into_response())
}

/// Gate `/readiness` and `/metrics`.
///
/// A configured `TASKITO_DASHBOARD_METRICS_TOKEN` is the scraper-friendly
/// credential. With no token set, open mode stays public (probes must work out
/// of the box) while session mode requires a dashboard session.
fn require_probe_access(
    state: &SharedState,
    headers: &HeaderMap,
    context: &RequestContext,
) -> ApiResult<()> {
    if let Some(token) = &state.config.metrics_token {
        let presented = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        if constant_time_eq(presented, &format!("Bearer {token}")) {
            return Ok(());
        }
    } else if state.config.auth == AuthMode::Open {
        return Ok(());
    }
    if context.is_authenticated() {
        return Ok(());
    }
    Err(ApiError::Unauthenticated)
}

/// Escape a Prometheus label value: backslash, quote, and newline are the
/// three characters the exposition format reserves.
fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// First line of an error only — storage errors can carry query fragments.
fn brief(error: &ApiError) -> String {
    match error {
        ApiError::Internal(inner) => inner.to_string(),
        other => format!("{other:?}"),
    }
}
