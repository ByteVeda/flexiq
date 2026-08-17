//! JS-facing shapes for workflow runs and nodes. State enums are surfaced as
//! their lowercase string names (matching the rest of the SDK and the dashboard
//! contract).

use flexiq_workflows::{WorkflowNode, WorkflowRun};
use napi_derive::napi;

/// JS-facing view of a [`WorkflowRun`].
#[napi(object)]
pub struct JsWorkflowRun {
    pub id: String,
    pub definition_id: String,
    /// Lowercase run state: `pending`, `running`, `completed`, `failed`,
    /// `cancelled`, `completed_with_failures`, …
    pub state: String,
    pub params: Option<String>,
    pub error: Option<String>,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub created_at: i64,
    pub parent_run_id: Option<String>,
    pub parent_node_name: Option<String>,
}

/// JS-facing view of a [`WorkflowNode`] (one step of a run).
#[napi(object)]
pub struct JsWorkflowNode {
    pub run_id: String,
    pub node_name: String,
    pub job_id: Option<String>,
    /// Lowercase node status: `pending`, `ready`, `running`, `completed`,
    /// `failed`, `skipped`, …
    pub status: String,
    pub error: Option<String>,
    pub result_hash: Option<String>,
    /// Number of children a fan-out node expanded into (set on the parent).
    pub fan_out_count: Option<i32>,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    /// Saga: job id of the compensation enqueued for this node, if any.
    pub compensation_job_id: Option<String>,
    pub compensation_started_at: Option<i64>,
    pub compensation_completed_at: Option<i64>,
    pub compensation_error: Option<String>,
}

/// Result of advancing a workflow node — returned by `markWorkflowNodeResult`.
/// `finalState` is set only when the whole run reached a terminal state.
#[napi(object)]
pub struct JsWorkflowAdvance {
    pub run_id: String,
    pub node_name: String,
    pub final_state: Option<String>,
}

/// Identifies the workflow run + node a job belongs to. Returned by
/// `workflowNodeForJob` so the tracker can classify an outcome without an
/// in-memory job→run map.
#[napi(object)]
pub struct JsWorkflowNodeRef {
    pub run_id: String,
    pub node_name: String,
}

/// Outcome of a fan-out completion check. Returned by `checkFanOutCompletion`
/// only when this caller performed the parent's terminal transition.
#[napi(object)]
pub struct JsFanOutCompletion {
    /// Whether every child completed successfully.
    pub succeeded: bool,
    /// Job ids of the children, in storage order.
    pub child_job_ids: Vec<String>,
}

/// The DAG + step metadata backing a run, so the tracker can reconstruct the
/// workflow structure from storage (no submit-time registration needed).
#[napi(object)]
pub struct JsWorkflowRunPlan {
    /// `SerializableGraph` JSON (`{nodes:[{name}], edges:[{from,to,weight}]}`).
    pub dag: String,
    /// `HashMap<String, StepMetadata>` JSON, keyed by node name.
    pub step_metadata: String,
}

pub fn run_to_js(run: WorkflowRun) -> JsWorkflowRun {
    JsWorkflowRun {
        id: run.id,
        definition_id: run.definition_id,
        state: run.state.as_str().to_string(),
        params: run.params,
        error: run.error,
        started_at: run.started_at,
        completed_at: run.completed_at,
        created_at: run.created_at,
        parent_run_id: run.parent_run_id,
        parent_node_name: run.parent_node_name,
    }
}

pub fn node_to_js(node: WorkflowNode) -> JsWorkflowNode {
    JsWorkflowNode {
        run_id: node.run_id,
        node_name: node.node_name,
        job_id: node.job_id,
        status: node.status.as_str().to_string(),
        error: node.error,
        result_hash: node.result_hash,
        fan_out_count: node.fan_out_count,
        started_at: node.started_at,
        completed_at: node.completed_at,
        compensation_job_id: node.compensation_job_id,
        compensation_started_at: node.compensation_started_at,
        compensation_completed_at: node.compensation_completed_at,
        compensation_error: node.compensation_error,
    }
}
