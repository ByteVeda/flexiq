use std::fmt;

use serde::{Deserialize, Serialize};

/// Status of a single node within a workflow run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowNodeStatus {
    /// The node exists but has not been picked up — it is either waiting on a
    /// predecessor or waiting for a caller to create its job. A node that *is*
    /// runnable is still `Pending`: readiness is a predicate over the DAG, not a
    /// stored status, and `get_ready_workflow_nodes` selects on this variant.
    Pending,
    /// Reserved, and never written. No code path persists it, so a runnable node
    /// stays `Pending`; readers that match on status group this with `Pending`.
    Ready,
    /// The node's job (or its gate/sub-workflow equivalent) is in flight.
    Running,
    /// The node's job finished successfully.
    Completed,
    /// The node's job exhausted its retries or was dead-lettered.
    Failed,
    /// The node's `condition` did not hold, so it was passed over without
    /// running.
    Skipped,
    /// An approval-gate node, parked until a caller resolves the gate or its
    /// timeout fires.
    WaitingApproval,
    /// A cacheable node whose `result_hash` was copied from an earlier run, so
    /// no job was enqueued at all.
    CacheHit,
    /// Saga compensation is in flight for this node (the original forward
    /// execution had completed, then the workflow failed elsewhere).
    Compensating,
    /// Compensation finished successfully — the side effects of the forward
    /// execution are considered rolled back.
    Compensated,
    /// Compensation itself failed. The node's side effects may be in a
    /// partially-rolled-back state and require operator attention.
    CompensationFailed,
}

impl WorkflowNodeStatus {
    /// The snake_case form written to the `status` column and to JSON.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Ready => "ready",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
            Self::WaitingApproval => "waiting_approval",
            Self::CacheHit => "cache_hit",
            Self::Compensating => "compensating",
            Self::Compensated => "compensated",
            Self::CompensationFailed => "compensation_failed",
        }
    }

    /// Parse the stored `status` string back into a variant, or `None` if the
    /// row holds a value this build doesn't know.
    pub fn from_str_val(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(Self::Pending),
            "ready" => Some(Self::Ready),
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "skipped" => Some(Self::Skipped),
            "waiting_approval" => Some(Self::WaitingApproval),
            "cache_hit" => Some(Self::CacheHit),
            "compensating" => Some(Self::Compensating),
            "compensated" => Some(Self::Compensated),
            "compensation_failed" => Some(Self::CompensationFailed),
            _ => None,
        }
    }

    /// Whether the node has reached a state that the run-level finalizer
    /// should treat as "done" (no more state transitions expected).
    ///
    /// Note: `Compensating` is NOT terminal — the saga orchestrator is still
    /// waiting on the compensation job. `Compensated` and
    /// `CompensationFailed` are.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed
                | Self::Failed
                | Self::Skipped
                | Self::CacheHit
                | Self::Compensated
                | Self::CompensationFailed
        )
    }

    /// Whether the node successfully completed its forward execution and is
    /// eligible for compensation if the run later fails.
    pub fn is_compensable(&self) -> bool {
        matches!(self, Self::Completed | Self::CacheHit)
    }
}

impl fmt::Display for WorkflowNodeStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A single node instance within a workflow run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowNode {
    /// UUIDv7 primary key (`workflow_nodes.id`).
    pub id: String,
    /// The [`WorkflowRun`](crate::WorkflowRun) this node belongs to. Unique
    /// together with `node_name`.
    pub run_id: String,
    /// The node's name in the DAG. A fan-out child is named `parent[i]`.
    pub node_name: String,
    /// The queue job carrying this node's work. `None` while the node is
    /// deferred, a cache hit, or otherwise never enqueued.
    pub job_id: Option<String>,
    /// Where the node is in its lifecycle.
    pub status: WorkflowNodeStatus,
    /// SHA-256 of the job's result bytes, recorded on completion so a later
    /// run can reuse the result instead of recomputing it. Best-effort — the
    /// event can fire before the result is written, leaving this `None`.
    pub result_hash: Option<String>,
    /// For a fan-out node, how many children it expanded into.
    pub fan_out_count: Option<i32>,
    /// Epoch-ms the node started executing.
    pub started_at: Option<i64>,
    /// Epoch-ms the node reached a terminal status.
    pub completed_at: Option<i64>,
    /// Failure message when the node's forward execution failed.
    pub error: Option<String>,
    /// Job ID of the running (or completed) compensation, when a saga has
    /// triggered rollback for this node. ``None`` outside of saga flow.
    #[serde(default)]
    pub compensation_job_id: Option<String>,
    /// Epoch-ms when the compensation job was enqueued, set together with
    /// `compensation_job_id`.
    #[serde(default)]
    pub compensation_started_at: Option<i64>,
    /// Epoch-ms when the compensation completed (success or failure).
    #[serde(default)]
    pub compensation_completed_at: Option<i64>,
    /// Error string if the compensation itself failed.
    #[serde(default)]
    pub compensation_error: Option<String>,
}
