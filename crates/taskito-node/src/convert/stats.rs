//! JS-facing shapes for inspection/management results.

use napi_derive::napi;
use taskito_core::storage::records::{JobError, TaskMetric, WorkerInfo};
use taskito_core::storage::{DeadJob, QueueStats};

/// Queue job counts by status.
#[napi(object)]
pub struct JsStats {
    pub pending: i64,
    pub running: i64,
    pub completed: i64,
    pub failed: i64,
    pub dead: i64,
    pub cancelled: i64,
}

pub fn stats_to_js(stats: QueueStats) -> JsStats {
    JsStats {
        pending: stats.pending,
        running: stats.running,
        completed: stats.completed,
        failed: stats.failed,
        dead: stats.dead,
        cancelled: stats.cancelled,
    }
}

/// A dead-letter entry.
#[napi(object)]
pub struct JsDeadJob {
    pub id: String,
    pub original_job_id: String,
    pub queue: String,
    pub task_name: String,
    pub error: Option<String>,
    pub retry_count: i32,
    pub failed_at: i64,
    pub metadata: Option<String>,
    pub dlq_retry_count: i32,
}

pub fn dead_job_to_js(dead: DeadJob) -> JsDeadJob {
    JsDeadJob {
        id: dead.id,
        original_job_id: dead.original_job_id,
        queue: dead.queue,
        task_name: dead.task_name,
        error: dead.error,
        retry_count: dead.retry_count,
        failed_at: dead.failed_at,
        metadata: dead.metadata,
        dlq_retry_count: dead.dlq_retry_count,
    }
}

/// One recorded error attempt for a job.
#[napi(object)]
pub struct JsJobError {
    pub id: String,
    pub job_id: String,
    pub attempt: i32,
    pub error: String,
    pub failed_at: i64,
}

pub fn job_error_to_js(error: JobError) -> JsJobError {
    JsJobError {
        id: error.id,
        job_id: error.job_id,
        attempt: error.attempt,
        error: error.error,
        failed_at: error.failed_at,
    }
}

/// A per-execution task metric.
#[napi(object)]
pub struct JsMetric {
    pub task_name: String,
    pub job_id: String,
    pub wall_time_ns: i64,
    pub memory_bytes: i64,
    pub succeeded: bool,
    pub recorded_at: i64,
}

pub fn metric_to_js(metric: TaskMetric) -> JsMetric {
    JsMetric {
        task_name: metric.task_name,
        job_id: metric.job_id,
        wall_time_ns: metric.wall_time_ns,
        memory_bytes: metric.memory_bytes,
        succeeded: metric.succeeded,
        recorded_at: metric.recorded_at,
    }
}

/// A registered worker (heartbeat + identity).
#[napi(object)]
pub struct JsWorkerRow {
    pub worker_id: String,
    pub queues: String,
    pub status: String,
    pub last_heartbeat: i64,
    pub started_at: Option<i64>,
    pub hostname: Option<String>,
    pub pid: Option<i32>,
    pub pool_type: Option<String>,
    pub threads: i32,
    pub tags: Option<String>,
    /// JSON array of resource names the worker advertised at registration.
    pub resources: Option<String>,
    /// JSON object of per-resource health (`"healthy"`/`"unhealthy"`), written
    /// by the worker's heartbeat.
    pub resource_health: Option<String>,
    /// SDK that registered the worker, and the release of it.
    pub sdk: Option<String>,
    pub sdk_version: Option<String>,
}

pub fn worker_to_js(worker: WorkerInfo) -> JsWorkerRow {
    JsWorkerRow {
        worker_id: worker.worker_id,
        queues: worker.queues,
        status: worker.status,
        last_heartbeat: worker.last_heartbeat,
        started_at: worker.started_at,
        hostname: worker.hostname,
        pid: worker.pid,
        pool_type: worker.pool_type,
        threads: worker.threads,
        tags: worker.tags,
        resources: worker.resources,
        resource_health: worker.resource_health,
        sdk: worker.sdk,
        sdk_version: worker.sdk_version,
    }
}

/// Map a lowercase status string to the core's `i32` code (for list filtering).
pub fn status_code(status: &str) -> Option<i32> {
    match status {
        "pending" => Some(0),
        "running" => Some(1),
        "complete" | "completed" => Some(2),
        "failed" => Some(3),
        "dead" => Some(4),
        "cancelled" => Some(5),
        _ => None,
    }
}

/// One page of dead-letter entries plus the cursor for the next. See
/// [`crate::convert::JsJobPage`] for the contract.
#[napi(object)]
pub struct JsDeadJobPage {
    pub items: Vec<JsDeadJob>,
    pub next_cursor: Option<String>,
}
