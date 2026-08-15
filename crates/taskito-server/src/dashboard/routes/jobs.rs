//! Job listing, detail, and per-job history.

use axum::extract::{Path, State};
use axum::Json;
use serde_json::{json, Value};
use taskito_core::Storage;

use crate::dashboard::blocking::on_storage;
use crate::dashboard::dto;
use crate::dashboard::error::{ApiError, ApiResult};
use crate::dashboard::query::Params;
use crate::dashboard::state::SharedState;

/// `GET /api/jobs` — filtered, paginated listing.
///
/// The extended filters cost a wider query, so the cheap listing is used
/// unless one of them is actually present — same split as the SDK dashboards.
pub async fn list(State(state): State<SharedState>, params: Params) -> ApiResult<Json<Value>> {
    let status = parse_status(params.get("status"))?;
    let queue = params.get("queue").map(str::to_string);
    let task_name = params.get("task").map(str::to_string);
    let metadata_like = params.get("metadata").map(str::to_string);
    let error_like = params.get("error").map(str::to_string);
    let created_after = optional_timestamp(&params, "created_after")?;
    let created_before = optional_timestamp(&params, "created_before")?;
    let limit = params.int("limit", 20)?;
    let offset = params.int("offset", 0)?;
    let namespace = state.namespace.clone();

    let wants_extended_filters = metadata_like.is_some()
        || error_like.is_some()
        || created_after.is_some()
        || created_before.is_some();

    let jobs = on_storage(&state, move |storage| {
        if wants_extended_filters {
            storage.list_jobs_filtered(
                status,
                queue.as_deref(),
                task_name.as_deref(),
                metadata_like.as_deref(),
                error_like.as_deref(),
                created_after,
                created_before,
                limit,
                offset,
                namespace.as_deref(),
            )
        } else {
            storage.list_jobs(
                status,
                queue.as_deref(),
                task_name.as_deref(),
                limit,
                offset,
                namespace.as_deref(),
            )
        }
    })
    .await?;

    Ok(Json(Value::Array(jobs.iter().map(dto::job).collect())))
}

/// `GET /api/jobs/{job_id}`.
pub async fn detail(
    State(state): State<SharedState>,
    Path(job_id): Path<String>,
) -> ApiResult<Json<Value>> {
    let namespace = state.namespace.clone();
    let job = on_storage(&state, move |storage| {
        storage.get_job(&job_id, namespace.as_deref())
    })
    .await?
    .ok_or_else(|| ApiError::NotFound("Job not found".to_string()))?;
    Ok(Json(dto::job(&job)))
}

/// `GET /api/jobs/{job_id}/errors` — one entry per failed attempt.
pub async fn errors(
    State(state): State<SharedState>,
    Path(job_id): Path<String>,
) -> ApiResult<Json<Value>> {
    let namespace = state.namespace.clone();
    let errors = on_storage(&state, move |storage| {
        storage.get_job_errors(&job_id, namespace.as_deref())
    })
    .await?;
    Ok(Json(Value::Array(
        errors.iter().map(dto::job_error).collect(),
    )))
}

/// `GET /api/jobs/{job_id}/logs`.
pub async fn logs(
    State(state): State<SharedState>,
    Path(job_id): Path<String>,
) -> ApiResult<Json<Value>> {
    let namespace = state.namespace.clone();
    let logs = on_storage(&state, move |storage| {
        storage.get_task_logs(&job_id, namespace.as_deref())
    })
    .await?;
    Ok(Json(Value::Array(logs.iter().map(dto::task_log).collect())))
}

/// `GET /api/jobs/{job_id}/replay-history`.
pub async fn replay_history(
    State(state): State<SharedState>,
    Path(job_id): Path<String>,
) -> ApiResult<Json<Value>> {
    let history = on_storage(&state, move |storage| storage.get_replay_history(&job_id)).await?;
    Ok(Json(Value::Array(
        history.iter().map(dto::replay_entry).collect(),
    )))
}

/// `GET /api/jobs/{job_id}/dag` — the job's dependency graph.
pub async fn dag(
    State(state): State<SharedState>,
    Path(job_id): Path<String>,
) -> ApiResult<Json<Value>> {
    let namespace = state.namespace.clone();
    let graph = on_storage(&state, move |storage| {
        build_dag(storage, &job_id, namespace.as_deref())
    })
    .await?;
    Ok(Json(graph))
}

/// `POST /api/jobs/{job_id}/cancel` — cancel a job before it starts.
pub async fn cancel(
    State(state): State<SharedState>,
    Path(job_id): Path<String>,
) -> ApiResult<Json<Value>> {
    let namespace = state.namespace.clone();
    let cancelled = on_storage(&state, move |storage| {
        storage.cancel_job(&job_id, namespace.as_deref())
    })
    .await?;
    Ok(Json(json!({ "cancelled": cancelled })))
}

/// `POST /api/jobs/{job_id}/replay` — re-enqueue with the same payload.
pub async fn replay(
    State(state): State<SharedState>,
    Path(job_id): Path<String>,
) -> ApiResult<Json<Value>> {
    let namespace = state.namespace.clone();
    let replay_id = on_storage(&state, move |storage| {
        replay_job(storage, &job_id, namespace.as_deref())
    })
    .await??;
    Ok(Json(json!({ "replay_job_id": replay_id })))
}

/// Enqueue a copy of `job_id` and record the pairing.
///
/// The copy deliberately drops the unique key, dependencies, and expiry: it is
/// a fresh attempt at the same work, not a resurrection of the original's
/// scheduling constraints.
fn replay_job(
    storage: &impl Storage,
    job_id: &str,
    namespace: Option<&str>,
) -> taskito_core::Result<ApiResult<String>> {
    let Some(original) = storage.get_job(job_id, namespace)? else {
        return Ok(Err(ApiError::NotFound("Job not found".to_string())));
    };

    let replay = taskito_core::NewJob {
        queue: original.queue,
        task_name: original.task_name,
        payload: original.payload,
        priority: original.priority,
        scheduled_at: taskito_core::now_millis(),
        max_retries: original.max_retries,
        timeout_ms: original.timeout_ms,
        unique_key: None,
        metadata: Some(json!({ "replayed_from": job_id }).to_string()),
        notes: original.notes,
        depends_on: vec![],
        expires_at: None,
        result_ttl_ms: original.result_ttl_ms,
        namespace: original.namespace,
        debounce_key: None,
    };
    let enqueued = storage.enqueue(replay)?;

    // Best-effort: the replay itself succeeded, and losing the audit pairing
    // must not turn that into a 500.
    if let Err(error) = storage.record_replay(
        job_id,
        &enqueued.id,
        original.result.as_deref(),
        None,
        original.error.as_deref(),
        None,
    ) {
        log::warn!("recording the replay of {job_id} failed: {error}");
    }
    Ok(Ok(enqueued.id))
}

/// Walk both directions from `root`, collecting every reachable job once.
///
/// Iterative rather than recursive: a long dependency chain is operator data,
/// and blowing the stack on it would take the whole dashboard down.
fn build_dag(
    storage: &impl Storage,
    root: &str,
    namespace: Option<&str>,
) -> taskito_core::Result<Value> {
    let mut visited = std::collections::HashSet::new();
    let mut pending = vec![root.to_string()];
    let mut nodes = Vec::new();
    let mut edges: Vec<(String, String)> = Vec::new();
    // An edge is queued as a candidate and kept only once BOTH endpoints resolved to a visible job: the edge lists are id-only, so pushing one before the adjacent node is looked up leaks a foreign job id into a scoped caller's graph even though its node is skipped.
    let mut visible = std::collections::HashSet::new();

    while let Some(job_id) = pending.pop() {
        if !visited.insert(job_id.clone()) {
            continue;
        }
        let Some(job) = storage.get_job(&job_id, namespace)? else {
            continue;
        };
        visible.insert(job_id.clone());
        nodes.push(dto::job(&job));

        for dependency in storage.get_dependencies(&job_id, namespace)? {
            edges.push((dependency.clone(), job_id.clone()));
            pending.push(dependency);
        }
        for dependent in storage.get_dependents(&job_id, namespace)? {
            edges.push((job_id.clone(), dependent.clone()));
            pending.push(dependent);
        }
    }

    let edges: Vec<Value> = edges
        .into_iter()
        .filter(|(from, to)| visible.contains(from) && visible.contains(to))
        .map(|(from, to)| json!({ "from": from, "to": to }))
        .collect();
    Ok(json!({ "nodes": nodes, "edges": edges }))
}

/// Map a status filter to its stored discriminant.
pub fn parse_status(status: Option<&str>) -> ApiResult<Option<i32>> {
    let Some(status) = status else {
        return Ok(None);
    };
    let code = match status {
        "pending" => 0,
        "running" => 1,
        "complete" | "completed" => 2,
        "failed" => 3,
        "dead" => 4,
        "cancelled" => 5,
        other => {
            return Err(ApiError::BadRequest(format!(
                "Invalid status: {other}. Use: pending, running, complete, failed, dead, cancelled"
            )))
        }
    };
    Ok(Some(code))
}

/// A millisecond timestamp filter, absent when the parameter is not set.
fn optional_timestamp(params: &Params, key: &str) -> ApiResult<Option<i64>> {
    match params.get(key) {
        None => Ok(None),
        Some(_) => Ok(Some(params.int(key, 0)?)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_names_map_to_discriminants() {
        assert_eq!(parse_status(None).expect("no filter"), None);
        assert_eq!(parse_status(Some("pending")).expect("valid"), Some(0));
        assert_eq!(parse_status(Some("completed")).expect("alias"), Some(2));
        assert!(matches!(
            parse_status(Some("nope")),
            Err(ApiError::BadRequest(_))
        ));
    }
}
