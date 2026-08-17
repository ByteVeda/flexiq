//! View model + the `DataSource` abstraction.
//!
//! Views bind to these plain structs, never to `flexiq-core` types directly,
//! so a future HTTP (`--api`) source can produce the same shapes from JSON
//! without touching the UI layer. `DbSource` (see [`db`]) is the only impl today.

pub mod db;

use anyhow::Result;

use flexiq_core::JobStatus;
use flexiq_core::QueueStats;
use flexiq_workflows::{WorkflowNodeStatus, WorkflowState};

/// Top-level tabs.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum View {
    Stats,
    Jobs,
    DeadLetters,
    Workers,
    Workflows,
}

impl View {
    pub const ALL: [View; 5] = [
        View::Stats,
        View::Jobs,
        View::DeadLetters,
        View::Workers,
        View::Workflows,
    ];

    pub fn title(self) -> &'static str {
        match self {
            View::Stats => "Stats",
            View::Jobs => "Jobs",
            View::DeadLetters => "Dead Letters",
            View::Workers => "Workers",
            View::Workflows => "Workflows",
        }
    }
}

/// One row in the jobs table (blob-free — payload/result are never shown).
#[derive(Clone)]
pub struct JobRow {
    pub id: String,
    pub task_name: String,
    pub queue: String,
    pub status: JobStatus,
    pub priority: i32,
    pub retry_count: i32,
    pub max_retries: i32,
    pub created_at: i64,
    pub scheduled_at: i64,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub error: Option<String>,
    pub cancel_requested: bool,
}

/// A dead-letter entry.
#[derive(Clone)]
pub struct DeadRow {
    pub id: String,
    pub original_job_id: String,
    pub task_name: String,
    pub queue: String,
    pub error: Option<String>,
    pub retry_count: i32,
    pub dlq_retry_count: i32,
    pub failed_at: i64,
}

/// A registered worker.
#[derive(Clone)]
pub struct WorkerView {
    pub worker_id: String,
    pub status: String,
    pub queues: String,
    pub threads: i32,
    pub last_heartbeat: i64,
    pub hostname: Option<String>,
    pub pid: Option<i32>,
}

/// A workflow run header.
#[derive(Clone)]
pub struct WorkflowRunRow {
    pub id: String,
    pub definition_id: String,
    pub state: WorkflowState,
    pub created_at: i64,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub error: Option<String>,
    pub parent_run_id: Option<String>,
}

/// A node in a workflow run's DAG, carrying its direct predecessors (edges) and
/// its depth (longest path from a root) so the detail pane can draw the graph.
#[derive(Clone)]
pub struct DagNode {
    pub name: String,
    /// `None` until the tracker materializes the node (treat as pending).
    pub status: Option<WorkflowNodeStatus>,
    pub predecessors: Vec<String>,
    pub depth: usize,
    pub error: Option<String>,
}

/// Overview counts: whole-queue totals, per-queue breakdown, and paused set.
#[derive(Clone, Default)]
pub struct StatsSnapshot {
    pub overall: QueueStats,
    pub per_queue: Vec<(String, QueueStats)>,
    pub paused: Vec<String>,
}

/// Full job detail for the detail pane.
pub struct JobDetail {
    pub row: JobRow,
    pub metadata: Option<String>,
    pub notes: Option<String>,
    pub namespace: Option<String>,
    pub errors: Vec<JobErrorEntry>,
    pub logs: Vec<LogEntry>,
}

pub struct JobErrorEntry {
    pub attempt: i32,
    pub error: String,
    pub failed_at: i64,
}

pub struct LogEntry {
    pub level: String,
    pub message: String,
    pub logged_at: i64,
}

/// Read + act on queue state. The UI depends only on this trait.
pub trait DataSource: Send {
    fn stats(&self) -> Result<StatsSnapshot>;
    fn jobs(&self, status: Option<JobStatus>, limit: i64) -> Result<Vec<JobRow>>;
    fn job_detail(&self, id: &str) -> Result<Option<JobDetail>>;
    fn dead_letters(&self, limit: i64) -> Result<Vec<DeadRow>>;
    fn workers(&self) -> Result<Vec<WorkerView>>;
    fn workflow_runs(&self, limit: i64) -> Result<Vec<WorkflowRunRow>>;
    /// The run's DAG: nodes in topological order with predecessors, depth, and
    /// live status. `definition_id` supplies the edge structure.
    fn workflow_dag(&self, run_id: &str, definition_id: &str) -> Result<Vec<DagNode>>;

    // ── Actions ──────────────────────────────────────────────────
    /// Cancel a job: try the direct `cancel_job` (Pending), then fall back to
    /// the cooperative `request_cancel` (Running). Returns whether anything was
    /// affected. Robust to the job's state changing after the UI snapshot.
    fn cancel(&self, id: &str) -> Result<bool>;
    /// Re-enqueue a job as a brand-new job; returns the new job id.
    fn replay(&self, id: &str) -> Result<String>;
    /// Requeue a dead-letter entry; returns the new job id.
    fn retry_dead(&self, dead_id: &str) -> Result<String>;
    fn delete_dead(&self, dead_id: &str) -> Result<bool>;
    /// Purge every dead-letter entry; returns the count removed.
    fn purge_dead(&self) -> Result<u64>;
    fn pause_queue(&self, queue: &str) -> Result<()>;
    fn resume_queue(&self, queue: &str) -> Result<()>;
}
