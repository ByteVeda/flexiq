use serde::{Deserialize, Serialize};

/// Metadata for a single step in a workflow definition.
///
/// Stored alongside the DAG structure to map node names to task queue details.
/// Every optional field is `#[serde(default)]` so the JSON blob stays
/// backward-compatible as new step kinds are added (no schema migration).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StepMetadata {
    pub task_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args_template: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kwargs_template: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fan_out: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fan_in: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
    /// JSON `{timeoutMs, onTimeout, message}` marking an approval gate node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate: Option<String>,
    /// Serialized child-workflow spec marking a sub-workflow node (the tracker
    /// submits it as a child run and resolves this node when the child finalizes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub_workflow: Option<String>,
    /// Rollback task name — if the run fails, the tracker compensates this node
    /// (in reverse-dependency order) by running this task with the node's result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compensate: Option<String>,
    /// JSON `{ttlMs}` marking a cacheable node — its result is reused across runs
    /// when its task, args, and upstream results are unchanged. Opaque to the
    /// core; the shell's tracker reads it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<String>,
}

/// A persisted workflow definition: the DAG structure plus per-step metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowDefinition {
    pub id: String,
    pub name: String,
    pub version: i32,
    /// The serialized dagron DAG (JSON via `SerializableGraph`).
    pub dag_data: Vec<u8>,
    /// Per-node metadata mapping node names to task configuration.
    pub step_metadata: std::collections::HashMap<String, StepMetadata>,
    pub created_at: i64,
}
