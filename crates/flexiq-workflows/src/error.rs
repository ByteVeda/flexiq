use std::fmt;

use crate::state::WorkflowState;

#[derive(Debug)]
pub enum WorkflowError {
    /// The requested workflow definition was not found.
    DefinitionNotFound(String),
    /// The requested workflow run was not found.
    RunNotFound(String),
    /// A node with this name was not found in the workflow.
    NodeNotFound { run_id: String, node_name: String },
    /// Invalid state transition for a workflow run.
    InvalidTransition {
        from: WorkflowState,
        to: WorkflowState,
    },
    /// The DAG is structurally invalid (e.g. cycle detected).
    InvalidDag(String),
    /// The workflow definition already exists (name + version conflict).
    DuplicateDefinition { name: String, version: i32 },
}

impl fmt::Display for WorkflowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DefinitionNotFound(name) => {
                write!(f, "workflow definition not found: {name}")
            }
            Self::RunNotFound(id) => write!(f, "workflow run not found: {id}"),
            Self::NodeNotFound { run_id, node_name } => {
                write!(f, "node '{node_name}' not found in workflow run {run_id}")
            }
            Self::InvalidTransition { from, to } => {
                write!(f, "invalid workflow state transition: {from} → {to}")
            }
            Self::InvalidDag(msg) => write!(f, "invalid workflow DAG: {msg}"),
            Self::DuplicateDefinition { name, version } => {
                write!(f, "workflow definition already exists: {name} v{version}")
            }
        }
    }
}

impl std::error::Error for WorkflowError {}

impl From<WorkflowError> for flexiq_core::error::QueueError {
    fn from(e: WorkflowError) -> Self {
        flexiq_core::error::QueueError::Other(e.to_string())
    }
}
