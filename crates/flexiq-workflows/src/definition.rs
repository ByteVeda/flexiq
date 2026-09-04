use serde::{Deserialize, Serialize};

/// Metadata for a single step in a workflow definition.
///
/// Stored alongside the DAG structure to map node names to task queue details.
/// Every optional field is `#[serde(default)]` so the JSON blob stays
/// backward-compatible as new step kinds are added (no schema migration).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepMetadata {
    /// Registered task this step enqueues as its job's `task_name`.
    pub task_name: String,
    /// Queue to enqueue the step's job on. `None` takes the submission's
    /// default queue.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
    /// Base64 of the step's serialized positional args, kept so a node whose
    /// job is created at runtime (deferred, fan-out child) can still be built
    /// without the caller re-serializing them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args_template: Option<String>,
    /// Base64 of the step's serialized keyword args — the counterpart to
    /// `args_template`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kwargs_template: Option<String>,
    /// Per-step retry cap. `None` takes the submission's default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<i32>,
    /// Per-step execution timeout in milliseconds. `None` takes the
    /// submission's default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<i64>,
    /// Per-step job priority. `None` takes the submission's default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,
    /// JSON `{itemsFrom}` marking a fan-out node — at runtime the tracker
    /// expands it into one child node per item.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fan_out: Option<String>,
    /// JSON `{from}` marking a fan-in node, naming the fan-out whose children
    /// this node collects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fan_in: Option<String>,
    /// Entry condition — `on_success`, `on_failure` or `always` — deciding
    /// whether the node runs given its predecessors' outcomes.
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
    /// UUIDv7 primary key (`workflow_definitions.id`), referenced by every
    /// run's `definition_id`.
    pub id: String,
    /// Caller-chosen definition name, unique together with `version`.
    pub name: String,
    /// Definition version. A changed graph has to be submitted under a new one
    /// — reusing a version with different `dag_data` is refused.
    pub version: i32,
    /// The serialized dagron DAG (JSON via `SerializableGraph`).
    pub dag_data: Vec<u8>,
    /// Per-node metadata mapping node names to task configuration.
    pub step_metadata: std::collections::HashMap<String, StepMetadata>,
    /// Epoch-ms this definition row was first written.
    pub created_at: i64,
}
