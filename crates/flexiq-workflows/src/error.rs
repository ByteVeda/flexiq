use std::fmt;

use crate::state::WorkflowState;

/// What went wrong reading or advancing a workflow.
///
/// Crosses out of this crate as `flexiq_core::error::QueueError::Other`
/// carrying the `Display` text, so the message is the interface a caller sees.
#[derive(Debug)]
pub enum WorkflowError {
    /// The requested workflow definition was not found.
    DefinitionNotFound(String),
    /// The requested workflow run was not found.
    RunNotFound(String),
    /// A node with this name was not found in the workflow.
    NodeNotFound {
        /// The run that was searched.
        run_id: String,
        /// The node name that had no row in it.
        node_name: String,
    },
    /// Invalid state transition for a workflow run.
    InvalidTransition {
        /// The run's current state.
        from: WorkflowState,
        /// The state the caller asked to move to.
        to: WorkflowState,
    },
    /// The DAG is structurally invalid (e.g. cycle detected).
    InvalidDag(String),
    /// The workflow definition already exists (name + version conflict).
    DuplicateDefinition {
        /// The definition name that collided.
        name: String,
        /// The version already registered under that name.
        version: i32,
    },
    /// `step_metadata` failed to parse, or a node is missing from it, from
    /// `node_payloads`, or from a predecessor's job id at submit time.
    InvalidStepMetadata(String),
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
            Self::InvalidStepMetadata(msg) => write!(f, "invalid step_metadata: {msg}"),
        }
    }
}

impl std::error::Error for WorkflowError {}

impl From<WorkflowError> for flexiq_core::error::QueueError {
    fn from(e: WorkflowError) -> Self {
        flexiq_core::error::QueueError::Other(e.to_string())
    }
}
