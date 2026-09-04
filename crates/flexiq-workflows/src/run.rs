use serde::{Deserialize, Serialize};

use crate::state::WorkflowState;

/// A single execution of a workflow definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRun {
    /// UUIDv7 primary key (`workflow_runs.id`), and the id every node row and
    /// job metadata blob points back at.
    pub id: String,
    /// The `WorkflowDefinition` this run walks. It names the exact graph that
    /// produced this run's jobs, so inspecting the run later is faithful.
    pub definition_id: String,
    /// Caller-supplied run parameters, stored as an opaque JSON string.
    pub params: Option<String>,
    /// Where the run is in its state machine.
    pub state: WorkflowState,
    /// Epoch-ms the run was admitted, set at submission time.
    pub started_at: Option<i64>,
    /// Epoch-ms the run reached a terminal state.
    pub completed_at: Option<i64>,
    /// Why the run failed, when it did.
    pub error: Option<String>,
    /// For sub-workflows: the parent run that spawned this one.
    pub parent_run_id: Option<String>,
    /// For sub-workflows: the node in the parent that triggered this run.
    pub parent_node_name: Option<String>,
    /// Epoch-ms the run row was written. Together with `id` it forms the
    /// keyset cursor `list_workflow_runs_after` seeks on.
    pub created_at: i64,
}
