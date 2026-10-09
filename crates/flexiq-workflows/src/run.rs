use flexiq_core::trace::TraceContext;
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
    /// W3C `traceparent` of whoever submitted the run. Every node job carries
    /// it, so each step's execute span joins the submitter's trace.
    #[serde(default)]
    pub traceparent: Option<String>,
    /// W3C `tracestate` paired with `traceparent`.
    #[serde(default)]
    pub tracestate: Option<String>,
}

impl WorkflowRun {
    /// The submitter's trace context, or `None` when absent or not valid —
    /// a stored value is re-validated so a bad row never propagates.
    pub fn trace_context(&self) -> Option<TraceContext> {
        TraceContext::from_headers(self.traceparent.as_deref(), self.tracestate.as_deref())
    }

    /// Record `trace` as this run's carrier.
    pub fn with_trace_context(mut self, trace: Option<&TraceContext>) -> Self {
        self.traceparent = trace.map(|t| t.traceparent().to_string());
        self.tracestate = trace.and_then(|t| t.tracestate().map(str::to_string));
        self
    }
}
