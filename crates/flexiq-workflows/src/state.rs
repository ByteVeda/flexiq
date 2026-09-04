use std::fmt;

use serde::{Deserialize, Serialize};

/// State machine for a workflow run.
///
/// Transitions:
///   Pending                 → Running
///   Running                 → Completed | CompletedWithFailures | Failed | Cancelled | Paused | Compensating
///   Paused                  → Running | Cancelled
///   CompletedWithFailures   → Compensating (only when compensate_on_continue is set)
///   Compensating            → Compensated | CompensationFailed
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowState {
    /// The run's rows are written but nothing has been admitted yet — the
    /// state a submission holds only until its nodes are placed.
    Pending,
    /// The run is in flight: at least one node is enqueued or executing.
    Running,
    /// The run is held; no further node is admitted until it resumes.
    Paused,
    /// Every node reached a successful terminal status.
    Completed,
    /// A `on_failure="continue"` run reached terminal with at least one failed
    /// node and at least one completed node. Distinct from `Completed` (all
    /// nodes succeeded) and `Failed` (a fail-fast step aborted the run).
    CompletedWithFailures,
    /// A fail-fast node failed and aborted the run.
    Failed,
    /// A caller cancelled the run before it could finish.
    Cancelled,
    /// A saga-mode run that has hit a forward failure and is rolling back
    /// previously-completed nodes via their compensation tasks.
    Compensating,
    /// All compensations succeeded — the run is fully rolled back.
    Compensated,
    /// At least one compensation failed. Partial rollback may be in effect.
    CompensationFailed,
}

impl WorkflowState {
    /// The snake_case form written to the `state` column and to JSON.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::CompletedWithFailures => "completed_with_failures",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Compensating => "compensating",
            Self::Compensated => "compensated",
            Self::CompensationFailed => "compensation_failed",
        }
    }

    /// Parse the stored `state` string back into a variant, or `None` if the
    /// row holds a value this build doesn't know.
    pub fn from_str_val(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(Self::Pending),
            "running" => Some(Self::Running),
            "paused" => Some(Self::Paused),
            "completed" => Some(Self::Completed),
            "completed_with_failures" => Some(Self::CompletedWithFailures),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            "compensating" => Some(Self::Compensating),
            "compensated" => Some(Self::Compensated),
            "compensation_failed" => Some(Self::CompensationFailed),
            _ => None,
        }
    }

    /// Whether the run has settled: no further transition is expected out of
    /// this state, except the saga path out of a failed or completed run.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed
                | Self::CompletedWithFailures
                | Self::Failed
                | Self::Cancelled
                | Self::Compensated
                | Self::CompensationFailed
        )
    }

    /// Check whether transitioning from `self` to `target` is valid.
    pub fn can_transition_to(&self, target: Self) -> bool {
        matches!(
            (self, target),
            (Self::Pending, Self::Running)
                | (Self::Running, Self::Completed)
                | (Self::Running, Self::CompletedWithFailures)
                | (Self::Running, Self::Failed)
                | (Self::Running, Self::Cancelled)
                | (Self::Running, Self::Paused)
                | (Self::Running, Self::Compensating)
                | (Self::Paused, Self::Running)
                | (Self::Paused, Self::Cancelled)
                | (Self::CompletedWithFailures, Self::Compensating)
                | (Self::Completed, Self::Compensating)
                | (Self::Failed, Self::Compensating)
                | (Self::Compensating, Self::Compensated)
                | (Self::Compensating, Self::CompensationFailed)
        )
    }
}

impl fmt::Display for WorkflowState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
