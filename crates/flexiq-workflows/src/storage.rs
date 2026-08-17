use flexiq_core::error::Result;

use crate::{WorkflowDefinition, WorkflowNode, WorkflowNodeStatus, WorkflowRun, WorkflowState};

/// Storage operations for workflows.
///
/// Kept as a separate trait from `Storage` so that the workflow feature
/// doesn't bloat the core trait or require feature-gated methods everywhere.
pub trait WorkflowStorage: Send + Sync {
    // ── Definitions ────────────────────────────────────────────────

    fn create_workflow_definition(&self, def: &WorkflowDefinition) -> Result<()>;
    fn get_workflow_definition(
        &self,
        name: &str,
        version: Option<i32>,
    ) -> Result<Option<WorkflowDefinition>>;
    fn get_workflow_definition_by_id(&self, id: &str) -> Result<Option<WorkflowDefinition>>;

    // ── Runs ───────────────────────────────────────────────────────

    fn create_workflow_run(&self, run: &WorkflowRun) -> Result<()>;
    fn get_workflow_run(&self, run_id: &str) -> Result<Option<WorkflowRun>>;
    fn update_workflow_run_state(
        &self,
        run_id: &str,
        state: WorkflowState,
        error: Option<&str>,
    ) -> Result<()>;
    fn set_workflow_run_started(&self, run_id: &str, started_at: i64) -> Result<()>;
    fn set_workflow_run_completed(&self, run_id: &str, completed_at: i64) -> Result<()>;
    fn list_workflow_runs(
        &self,
        definition_name: Option<&str>,
        state: Option<WorkflowState>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<WorkflowRun>>;

    /// Keyset-paginated `list_workflow_runs`, ordered by `(created_at, id)`
    /// descending. `after` is the `(created_at, id)` of the previous page's
    /// last run; the caller derives the next cursor from the returned rows'
    /// last element. Stable under concurrent inserts, and O(page) at any depth
    /// on the Diesel backends.
    ///
    /// **Redis exception:** the `by_state` index is re-scored to the transition
    /// timestamp on every state change, so its order is not `created_at` and
    /// cannot be seeked. Redis applies the keyset in memory over the candidate
    /// set — correct and stable, but O(matching runs) rather than O(page).
    fn list_workflow_runs_after(
        &self,
        definition_name: Option<&str>,
        state: Option<WorkflowState>,
        limit: i64,
        after: Option<(i64, &str)>,
    ) -> Result<Vec<WorkflowRun>>;

    // ── Nodes ──────────────────────────────────────────────────────

    fn create_workflow_node(&self, node: &WorkflowNode) -> Result<()>;
    fn create_workflow_nodes_batch(&self, nodes: &[WorkflowNode]) -> Result<()>;
    fn get_workflow_node(&self, run_id: &str, node_name: &str) -> Result<Option<WorkflowNode>>;
    fn get_workflow_nodes(&self, run_id: &str) -> Result<Vec<WorkflowNode>>;
    fn update_workflow_node_status(
        &self,
        run_id: &str,
        node_name: &str,
        status: WorkflowNodeStatus,
    ) -> Result<()>;
    fn set_workflow_node_job(&self, run_id: &str, node_name: &str, job_id: &str) -> Result<()>;
    fn set_workflow_node_started(
        &self,
        run_id: &str,
        node_name: &str,
        started_at: i64,
    ) -> Result<()>;
    fn set_workflow_node_completed(
        &self,
        run_id: &str,
        node_name: &str,
        completed_at: i64,
        result_hash: Option<&str>,
    ) -> Result<()>;
    fn set_workflow_node_error(&self, run_id: &str, node_name: &str, error: &str) -> Result<()>;

    /// Return nodes whose status is `Pending` and all DAG predecessors are `Completed`.
    ///
    /// The `dag_json` parameter is the serialized DAG from the workflow definition.
    /// The implementation uses it to determine which predecessor nodes must be
    /// complete before a given node becomes ready.
    fn get_ready_workflow_nodes(&self, run_id: &str, dag_json: &str) -> Result<Vec<WorkflowNode>>;

    // ── Fan-out / Fan-in ──────────────────────────────────────────

    /// Set a node's `fan_out_count` and transition its status to `Running`.
    fn set_workflow_node_fan_out_count(
        &self,
        run_id: &str,
        node_name: &str,
        count: i32,
    ) -> Result<()>;

    /// Transition a node to `Running` and set its `started_at` timestamp.
    ///
    /// Used by the tracker to promote gate/sub-workflow nodes to running
    /// without going through fan-out bookkeeping.
    fn set_workflow_node_running(
        &self,
        run_id: &str,
        node_name: &str,
        started_at: i64,
    ) -> Result<()>;

    /// Atomically finalize a fan-out parent node if it is not already terminal.
    ///
    /// Issues a single conditional `UPDATE` that transitions the node to
    /// `Completed` (with `completed_at`) when `succeeded` is true, or `Failed`
    /// (with `error`) when false — but only when the current status is
    /// non-terminal. Returns `true` if this call performed the transition,
    /// `false` if another concurrent caller already finalized it. This is the
    /// compare-and-swap that makes fan-in expansion exactly-once even when
    /// multiple children complete on different worker threads at the same
    /// instant.
    fn finalize_fan_out_parent(
        &self,
        run_id: &str,
        node_name: &str,
        succeeded: bool,
        error: Option<&str>,
        completed_at: i64,
    ) -> Result<bool>;

    /// Return all nodes whose `node_name` starts with `prefix`.
    ///
    /// Used to find fan-out children (e.g., prefix `"process["` returns
    /// `process[0]`, `process[1]`, etc.).
    fn get_workflow_nodes_by_prefix(&self, run_id: &str, prefix: &str)
        -> Result<Vec<WorkflowNode>>;

    /// Return all child workflow runs of a parent run.
    fn get_child_workflow_runs(&self, parent_run_id: &str) -> Result<Vec<WorkflowRun>>;

    // ── Saga compensation ─────────────────────────────────────────

    /// Mark a node as `Compensating`, record the compensation job id and
    /// start timestamp. Used by the saga orchestrator when it enqueues the
    /// rollback job for a previously-completed node.
    fn set_workflow_node_compensation_job(
        &self,
        run_id: &str,
        node_name: &str,
        compensation_job_id: &str,
        started_at: i64,
    ) -> Result<()>;

    /// Mark a node as `Compensated` and record the completion timestamp.
    fn set_workflow_node_compensated(
        &self,
        run_id: &str,
        node_name: &str,
        completed_at: i64,
    ) -> Result<()>;

    /// Mark a node as `CompensationFailed`, recording the error and
    /// completion timestamp.
    fn set_workflow_node_compensation_failed(
        &self,
        run_id: &str,
        node_name: &str,
        error: &str,
        completed_at: i64,
    ) -> Result<()>;
}
