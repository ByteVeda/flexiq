//! Workflow run views, backed by the workflow store.

use axum::extract::{Path, State};
use axum::Json;
use serde_json::{json, Value};
use taskito_core::storage::cursor::{decode_cursor, next_cursor};
use taskito_workflows::{WorkflowNode, WorkflowRun, WorkflowState, WorkflowStorage};

use crate::dashboard::blocking::on_workflows;
use crate::dashboard::error::{ApiError, ApiResult};
use crate::dashboard::query::Params;
use crate::dashboard::state::SharedState;

/// Upper bound the workflow store applies to a keyset page.
const MAX_LIMIT: i64 = 500;

/// `GET /api/workflows/runs` — offset or keyset paginated.
///
/// Keyset mode is selected by `paginate=cursor` (first page, no cursor yet) or
/// by an `after` cursor for every page after it.
pub async fn list(State(state): State<SharedState>, params: Params) -> ApiResult<Json<Value>> {
    let definition_name = params.get("definition_name").map(str::to_string);
    let run_state = parse_state(params.get("state"))?;
    let limit = params.int("limit", 50)?;
    let after = params.get("after").map(str::to_string);
    let keyset = after.is_some() || params.get("paginate") == Some("cursor");

    if keyset {
        let clamped = limit.clamp(1, MAX_LIMIT);
        let runs = on_workflows(&state, move |workflows| {
            let cursor = after
                .as_deref()
                .map(decode_cursor)
                .transpose()
                .map_err(|error| ApiError::BadRequest(error.to_string()))?;
            workflows
                .list_workflow_runs_after(definition_name.as_deref(), run_state, clamped, cursor)
                .map_err(ApiError::from)
        })
        .await?;
        let cursor = next_cursor(&runs, clamped, |run| (run.created_at, &run.id));
        return Ok(Json(json!({
            "runs": runs.iter().map(run_json).collect::<Vec<_>>(),
            "limit": clamped,
            "next_cursor": cursor,
        })));
    }

    let offset = params.int("offset", 0)?;
    let runs = on_workflows(&state, move |workflows| {
        workflows
            .list_workflow_runs(definition_name.as_deref(), run_state, limit, offset)
            .map_err(ApiError::from)
    })
    .await?;
    Ok(Json(json!({
        "runs": runs.iter().map(run_json).collect::<Vec<_>>(),
        "limit": limit,
        "offset": offset,
    })))
}

/// `GET /api/workflows/runs/{run_id}` — run header plus per-node detail.
pub async fn detail(
    State(state): State<SharedState>,
    Path(run_id): Path<String>,
) -> ApiResult<Json<Value>> {
    let (run, nodes) = on_workflows(&state, move |workflows| {
        let run = workflows
            .get_workflow_run(&run_id)?
            .ok_or_else(|| ApiError::NotFound(format!("workflow run '{run_id}' not found")))?;
        let nodes = workflows.get_workflow_nodes(&run_id)?;
        Ok((run, nodes))
    })
    .await?;

    Ok(Json(json!({
        "run": run_json(&run),
        "nodes": nodes.iter().map(node_json).collect::<Vec<_>>(),
    })))
}

/// `GET /api/workflows/runs/{run_id}/dag` — the definition's DAG document.
pub async fn dag(
    State(state): State<SharedState>,
    Path(run_id): Path<String>,
) -> ApiResult<Json<Value>> {
    let dag = on_workflows(&state, move |workflows| {
        let run = workflows
            .get_workflow_run(&run_id)?
            .ok_or_else(|| ApiError::NotFound(format!("run '{run_id}' not found")))?;
        let definition = workflows
            .get_workflow_definition_by_id(&run.definition_id)?
            .ok_or_else(|| {
                ApiError::NotFound(format!("definition '{}' not found", run.definition_id))
            })?;
        // The stored DAG is already a JSON document; the client parses the
        // string itself, so it is forwarded verbatim rather than re-encoded.
        String::from_utf8(definition.dag_data)
            .map_err(|error| ApiError::Internal(anyhow::anyhow!("DAG is not valid UTF-8: {error}")))
    })
    .await?;
    Ok(Json(json!({ "dag": dag })))
}

/// `GET /api/workflows/runs/{run_id}/children` — sub-workflow runs.
pub async fn children(
    State(state): State<SharedState>,
    Path(run_id): Path<String>,
) -> ApiResult<Json<Value>> {
    let children = on_workflows(&state, move |workflows| {
        workflows
            .get_child_workflow_runs(&run_id)
            .map_err(ApiError::from)
    })
    .await?;
    Ok(Json(
        json!({ "children": children.iter().map(run_json).collect::<Vec<_>>() }),
    ))
}

fn run_json(run: &WorkflowRun) -> Value {
    json!({
        "id": run.id,
        "definition_id": run.definition_id,
        "state": run.state.as_str(),
        "params": run.params,
        "started_at": run.started_at,
        "completed_at": run.completed_at,
        "error": run.error,
        "parent_run_id": run.parent_run_id,
        "parent_node_name": run.parent_node_name,
        "created_at": run.created_at,
    })
}

fn node_json(node: &WorkflowNode) -> Value {
    json!({
        "node_name": node.node_name,
        "status": node.status.as_str(),
        "job_id": node.job_id,
        "result_hash": node.result_hash,
        "fan_out_count": node.fan_out_count,
        "started_at": node.started_at,
        "completed_at": node.completed_at,
        "error": node.error,
        "compensation_job_id": node.compensation_job_id,
        "compensation_started_at": node.compensation_started_at,
        "compensation_completed_at": node.compensation_completed_at,
        "compensation_error": node.compensation_error,
    })
}

fn parse_state(state: Option<&str>) -> ApiResult<Option<WorkflowState>> {
    match state {
        None => Ok(None),
        Some(raw) => WorkflowState::from_str_val(raw)
            .map(Some)
            .ok_or_else(|| ApiError::BadRequest(format!("invalid workflow state: {raw}"))),
    }
}
