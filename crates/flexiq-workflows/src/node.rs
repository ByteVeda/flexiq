use std::fmt;

use serde::{Deserialize, Serialize};

/// Status of a single node within a workflow run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowNodeStatus {
    Pending,
    Ready,
    Running,
    Completed,
    Failed,
    Skipped,
    WaitingApproval,
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
    pub id: String,
    pub run_id: String,
    pub node_name: String,
    pub job_id: Option<String>,
    pub status: WorkflowNodeStatus,
    pub result_hash: Option<String>,
    pub fan_out_count: Option<i32>,
    pub fan_in_data: Option<String>,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
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
