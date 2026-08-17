use serde::{Deserialize, Serialize};

use crate::state::WorkflowState;

/// A single execution of a workflow definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRun {
    pub id: String,
    pub definition_id: String,
    pub params: Option<String>,
    pub state: WorkflowState,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub error: Option<String>,
    /// For sub-workflows: the parent run that spawned this one.
    pub parent_run_id: Option<String>,
    /// For sub-workflows: the node in the parent that triggered this run.
    pub parent_node_name: Option<String>,
    pub created_at: i64,
}
