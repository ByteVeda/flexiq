use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use super::records::{
    CircuitBreakerState, JobError, LockInfo, PeriodicTask, RateLimitState, ReplayEntry,
    Subscription, SubscriptionMode, TaskLogEntry, TaskMetric, Topic, TopicMessage, WorkerInfo,
    WorkerRegistration,
};
use super::schema::{
    archived_jobs, circuit_breakers, dashboard_settings, dead_letter, distributed_locks,
    execution_claims, job_dependencies, job_errors, jobs, periodic_tasks, queue_state, rate_limits,
    replay_history, task_logs, task_metrics, topic_deliveries, topic_messages, topic_subscriptions,
    topics, workers,
};

/// A row in the `jobs` table (for SELECT queries).
#[derive(Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = jobs)]
pub struct JobRow {
    pub id: String,
    pub queue: String,
    pub task_name: String,
    pub payload: Vec<u8>,
    pub status: i32,
    pub priority: i32,
    pub created_at: i64,
    pub scheduled_at: i64,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub retry_count: i32,
    pub max_retries: i32,
    pub result: Option<Vec<u8>>,
    pub error: Option<String>,
    pub timeout_ms: i64,
    pub unique_key: Option<String>,
    pub progress: Option<i32>,
    pub metadata: Option<String>,
    pub notes: Option<String>,
    pub cancel_requested: i32,
    pub expires_at: Option<i64>,
    pub result_ttl_ms: Option<i64>,
    pub namespace: Option<String>,
    pub has_deps: bool,
}

/// A row in the `jobs` table with every column EXCEPT the `payload`/`result`
/// blobs. The hot dequeue candidate scan, reap, and listing paths select this
/// so claiming one job never reads up to 100 discarded payload blobs into heap.
/// The blobs live inline in `jobs.payload`/`jobs.result` and are loaded (via the
/// full [`JobRow`]) only for the single claimed winner. Postgres TOAST and SQLite
/// overflow pages keep them off the pages a narrow select scans.
#[derive(Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = jobs)]
pub struct NarrowJobRow {
    pub id: String,
    pub queue: String,
    pub task_name: String,
    pub status: i32,
    pub priority: i32,
    pub created_at: i64,
    pub scheduled_at: i64,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub retry_count: i32,
    pub max_retries: i32,
    pub error: Option<String>,
    pub timeout_ms: i64,
    pub unique_key: Option<String>,
    pub progress: Option<i32>,
    pub metadata: Option<String>,
    pub notes: Option<String>,
    pub cancel_requested: i32,
    pub expires_at: Option<i64>,
    pub result_ttl_ms: Option<i64>,
    pub namespace: Option<String>,
    pub has_deps: bool,
}

/// Insertable struct for creating new jobs.
#[derive(Insertable, Debug)]
#[diesel(table_name = jobs)]
pub struct NewJobRow<'a> {
    pub id: &'a str,
    pub queue: &'a str,
    pub task_name: &'a str,
    pub payload: &'a [u8],
    pub status: i32,
    pub priority: i32,
    pub created_at: i64,
    pub scheduled_at: i64,
    pub retry_count: i32,
    pub max_retries: i32,
    pub timeout_ms: i64,
    pub unique_key: Option<&'a str>,
    pub metadata: Option<&'a str>,
    pub notes: Option<&'a str>,
    pub cancel_requested: i32,
    pub expires_at: Option<i64>,
    pub result_ttl_ms: Option<i64>,
    pub namespace: Option<&'a str>,
    pub has_deps: bool,
    /// Set on pub/sub deliveries (derived from `notes`) so backlog/lag stats
    /// index by subscription; `None` for ordinary jobs.
    pub topic: Option<&'a str>,
    pub subscription_name: Option<&'a str>,
}

/// A row in the `dead_letter` table.
#[derive(Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = dead_letter)]
pub struct DeadLetterRow {
    pub id: String,
    pub original_job_id: String,
    pub queue: String,
    pub task_name: String,
    pub payload: Vec<u8>,
    pub error: Option<String>,
    pub retry_count: i32,
    pub failed_at: i64,
    pub metadata: Option<String>,
    pub notes: Option<String>,
    pub priority: i32,
    pub max_retries: i32,
    pub timeout_ms: i64,
    pub result_ttl_ms: Option<i64>,
    pub namespace: Option<String>,
    pub dlq_retry_count: i32,
}

/// A `dead_letter` row without the `payload` blob. Listing paths select this so
/// paging the DLQ never drags each entry's arg blob off overflow pages/TOAST;
/// the blob is loaded (via the full [`DeadLetterRow`]) only when a single entry
/// is requeued by id.
#[derive(Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = dead_letter)]
pub struct NarrowDeadLetterRow {
    pub id: String,
    pub original_job_id: String,
    pub queue: String,
    pub task_name: String,
    pub error: Option<String>,
    pub retry_count: i32,
    pub failed_at: i64,
    pub metadata: Option<String>,
    pub notes: Option<String>,
    pub priority: i32,
    pub max_retries: i32,
    pub timeout_ms: i64,
    pub result_ttl_ms: Option<i64>,
    pub namespace: Option<String>,
    pub dlq_retry_count: i32,
}

/// Insertable struct for dead letter entries.
#[derive(Insertable, Debug)]
#[diesel(table_name = dead_letter)]
pub struct NewDeadLetterRow<'a> {
    pub id: &'a str,
    pub original_job_id: &'a str,
    pub queue: &'a str,
    pub task_name: &'a str,
    pub payload: &'a [u8],
    pub error: Option<&'a str>,
    pub retry_count: i32,
    pub failed_at: i64,
    pub metadata: Option<&'a str>,
    pub notes: Option<&'a str>,
    pub priority: i32,
    pub max_retries: i32,
    pub timeout_ms: i64,
    pub result_ttl_ms: Option<i64>,
    pub namespace: Option<&'a str>,
    pub dlq_retry_count: i32,
    pub topic: Option<&'a str>,
    pub subscription_name: Option<&'a str>,
}

/// A row in the `rate_limits` table.
#[derive(
    Queryable,
    QueryableByName,
    Selectable,
    Insertable,
    AsChangeset,
    Debug,
    Clone,
    Serialize,
    Deserialize,
)]
#[diesel(table_name = rate_limits)]
pub struct RateLimitRow {
    pub key: String,
    pub tokens: f64,
    pub max_tokens: f64,
    pub refill_rate: f64,
    pub last_refill: i64,
}

/// A row in the `periodic_tasks` table.
#[derive(Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = periodic_tasks)]
pub struct PeriodicTaskRow {
    pub name: String,
    pub task_name: String,
    pub cron_expr: String,
    pub args: Option<Vec<u8>>,
    pub kwargs: Option<Vec<u8>>,
    pub queue: String,
    pub enabled: bool,
    pub last_run: Option<i64>,
    pub next_run: i64,
    pub timezone: Option<String>,
}

/// Insertable struct for periodic tasks.
#[derive(Insertable, AsChangeset, Debug)]
#[diesel(table_name = periodic_tasks)]
pub struct NewPeriodicTaskRow<'a> {
    pub name: &'a str,
    pub task_name: &'a str,
    pub cron_expr: &'a str,
    pub args: Option<&'a [u8]>,
    pub kwargs: Option<&'a [u8]>,
    pub queue: &'a str,
    pub enabled: bool,
    pub next_run: i64,
    pub timezone: Option<&'a str>,
}

/// A row in the `job_errors` table (for SELECT queries).
#[derive(Queryable, Selectable, Debug, Clone, Serialize, Deserialize)]
#[diesel(table_name = job_errors)]
pub struct JobErrorRow {
    pub id: String,
    pub job_id: String,
    pub attempt: i32,
    pub error: String,
    pub failed_at: i64,
}

/// Insertable struct for job error entries.
#[derive(Insertable, Debug)]
#[diesel(table_name = job_errors)]
pub struct NewJobErrorRow<'a> {
    pub id: &'a str,
    pub job_id: &'a str,
    pub attempt: i32,
    pub error: &'a str,
    pub failed_at: i64,
}

/// Insertable struct for job dependency entries.
#[derive(Insertable, Debug)]
#[diesel(table_name = job_dependencies)]
pub struct NewJobDependencyRow<'a> {
    pub id: &'a str,
    pub job_id: &'a str,
    pub depends_on_job_id: &'a str,
}

// ── Task Metrics ─────────────────────────────────────────────────

#[derive(Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = task_metrics)]
pub struct TaskMetricRow {
    pub id: String,
    pub task_name: String,
    pub job_id: String,
    pub wall_time_ns: i64,
    pub memory_bytes: i64,
    pub succeeded: bool,
    pub recorded_at: i64,
    pub namespace: Option<String>,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = task_metrics)]
pub struct NewTaskMetricRow<'a> {
    pub id: &'a str,
    pub task_name: &'a str,
    pub job_id: &'a str,
    pub wall_time_ns: i64,
    pub memory_bytes: i64,
    pub succeeded: bool,
    pub recorded_at: i64,
    pub namespace: Option<&'a str>,
}

// ── Replay History ───────────────────────────────────────────────

#[derive(Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = replay_history)]
pub struct ReplayHistoryRow {
    pub id: String,
    pub original_job_id: String,
    pub replay_job_id: String,
    pub replayed_at: i64,
    pub original_result: Option<Vec<u8>>,
    pub replay_result: Option<Vec<u8>>,
    pub original_error: Option<String>,
    pub replay_error: Option<String>,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = replay_history)]
pub struct NewReplayHistoryRow<'a> {
    pub id: &'a str,
    pub original_job_id: &'a str,
    pub replay_job_id: &'a str,
    pub replayed_at: i64,
    pub original_result: Option<&'a [u8]>,
    pub replay_result: Option<&'a [u8]>,
    pub original_error: Option<&'a str>,
    pub replay_error: Option<&'a str>,
}

// ── Task Logs ────────────────────────────────────────────────────

#[derive(Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = task_logs)]
pub struct TaskLogRow {
    pub id: String,
    pub job_id: String,
    pub task_name: String,
    pub level: String,
    pub message: String,
    pub extra: Option<String>,
    pub logged_at: i64,
    pub namespace: Option<String>,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = task_logs)]
pub struct NewTaskLogRow<'a> {
    pub id: &'a str,
    pub job_id: &'a str,
    pub task_name: &'a str,
    pub level: &'a str,
    pub message: &'a str,
    pub extra: Option<&'a str>,
    pub logged_at: i64,
    pub namespace: Option<&'a str>,
}

// ── Circuit Breaker ──────────────────────────────────────────────

#[derive(Queryable, Selectable, Insertable, AsChangeset, Debug, Clone, Serialize, Deserialize)]
#[diesel(table_name = circuit_breakers)]
pub struct CircuitBreakerRow {
    pub task_name: String,
    pub state: i32,
    pub failure_count: i32,
    pub last_failure_at: Option<i64>,
    pub opened_at: Option<i64>,
    pub half_open_at: Option<i64>,
    pub threshold: i32,
    pub window_ms: i64,
    pub cooldown_ms: i64,
    #[serde(default = "default_max_probes")]
    pub half_open_max_probes: i32,
    #[serde(default = "default_success_rate")]
    pub half_open_success_rate: f64,
    #[serde(default)]
    pub half_open_probe_count: i32,
    #[serde(default)]
    pub half_open_success_count: i32,
    #[serde(default)]
    pub half_open_failure_count: i32,
}

fn default_max_probes() -> i32 {
    5
}

fn default_success_rate() -> f64 {
    0.8
}

// ── Workers ──────────────────────────────────────────────────────

#[derive(Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = workers)]
pub struct WorkerRow {
    pub worker_id: String,
    pub last_heartbeat: i64,
    pub queues: String,
    pub status: String,
    pub tags: Option<String>,
    pub resources: Option<String>,
    pub resource_health: Option<String>,
    pub threads: i32,
    pub started_at: Option<i64>,
    pub hostname: Option<String>,
    pub pid: Option<i32>,
    pub pool_type: Option<String>,
    pub sdk: Option<String>,
    pub sdk_version: Option<String>,
}

#[derive(Insertable, AsChangeset, Debug)]
#[diesel(table_name = workers)]
pub struct NewWorkerRow<'a> {
    pub worker_id: &'a str,
    pub last_heartbeat: i64,
    pub queues: &'a str,
    pub status: &'a str,
    pub tags: Option<&'a str>,
    pub resources: Option<&'a str>,
    pub resource_health: Option<&'a str>,
    pub threads: i32,
    pub started_at: Option<i64>,
    pub hostname: Option<&'a str>,
    pub pid: Option<i32>,
    pub pool_type: Option<&'a str>,
    pub sdk: Option<&'a str>,
    pub sdk_version: Option<&'a str>,
}

impl<'a> NewWorkerRow<'a> {
    /// The insertable row for a joining worker, stamped at `now`.
    ///
    /// Shared by both Diesel backends: they differ only in how they resolve the
    /// conflict on an existing `worker_id`, never in what the row contains.
    pub fn joining(registration: &WorkerRegistration<'a>, now: i64) -> Self {
        Self {
            worker_id: registration.worker_id,
            last_heartbeat: now,
            queues: registration.queues,
            status: "active",
            tags: registration.tags,
            resources: registration.resources,
            resource_health: registration.resource_health,
            threads: registration.threads,
            started_at: Some(now),
            hostname: registration.hostname,
            pid: registration.pid,
            pool_type: registration.pool_type,
            sdk: registration.sdk,
            sdk_version: registration.sdk_version,
        }
    }
}

// ── Queue State ─────────────────────────────────────────────────

#[derive(Queryable, Selectable, Insertable, AsChangeset, Debug, Clone)]
#[diesel(table_name = queue_state)]
pub struct QueueStateRow {
    pub queue_name: String,
    pub paused: bool,
    pub paused_at: Option<i64>,
}

// ── Dashboard Settings ──────────────────────────────────────────

#[derive(Queryable, Selectable, Insertable, AsChangeset, Debug, Clone)]
#[diesel(table_name = dashboard_settings)]
pub struct DashboardSettingRow {
    pub key: String,
    pub value: String,
    pub updated_at: i64,
}

// ── Topic Subscriptions ─────────────────────────────────────────

/// A row in the `topic_subscriptions` registry table.
///
/// The natural composite key is `(topic, subscription_name)`; there is no
/// surrogate id, matching the `periodic_tasks` registry precedent. A NULL
/// `owner_worker_id` marks a durable subscription that persists until an
/// explicit unsubscribe; a set owner marks an ephemeral one that is reaped
/// when its worker dies.
#[derive(Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = topic_subscriptions)]
pub struct SubscriptionRow {
    pub topic: String,
    pub subscription_name: String,
    pub task_name: String,
    pub queue: String,
    pub active: bool,
    pub durable: bool,
    pub owner_worker_id: Option<String>,
    pub created_at: i64,
    /// Per-subscription delivery settings persisted at registration so
    /// `publish_to_topic` applies them cross-process. `None` = queue default.
    pub priority: Option<i32>,
    pub max_retries: Option<i32>,
    pub timeout_ms: Option<i64>,
    /// Delivery mode: `"fanout"` (default) or `"log"`.
    pub mode: String,
    /// Log-mode read cursor (last-acked message id); `None` = unread.
    pub cursor: Option<String>,
}

/// Insertable/updatable struct for subscription registrations.
#[derive(Insertable, AsChangeset, Debug)]
#[diesel(table_name = topic_subscriptions)]
pub struct NewSubscriptionRow<'a> {
    pub topic: &'a str,
    pub subscription_name: &'a str,
    pub task_name: &'a str,
    pub queue: &'a str,
    pub active: bool,
    pub durable: bool,
    pub owner_worker_id: Option<&'a str>,
    pub created_at: i64,
    pub priority: Option<i32>,
    pub max_retries: Option<i32>,
    pub timeout_ms: Option<i64>,
    pub mode: &'a str,
}

// ── Topic Messages (log topics) ─────────────────────────────────

/// A row in the append-only `topic_messages` log. One per publish to a log
/// topic (vs one `jobs` row per subscriber in fan-out mode).
#[derive(Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = topic_messages)]
pub struct TopicMessageRow {
    pub id: String,
    pub topic: String,
    pub payload: Vec<u8>,
    pub metadata: Option<String>,
    pub notes: Option<String>,
    pub created_at: i64,
    pub expires_at: Option<i64>,
}

/// Insertable struct for a published log message.
#[derive(Insertable, Debug)]
#[diesel(table_name = topic_messages)]
pub struct NewTopicMessageRow<'a> {
    pub id: &'a str,
    pub topic: &'a str,
    pub payload: &'a [u8],
    pub metadata: Option<&'a str>,
    pub notes: Option<&'a str>,
    pub created_at: i64,
    pub expires_at: Option<i64>,
}

// ── Topics (first-class registry) ───────────────────────────────

/// A row in the `topics` registry: a declared topic and its retention window.
#[derive(Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = topics)]
pub struct TopicRow {
    pub name: String,
    pub mode: String,
    pub retention_ms: Option<i64>,
    pub created_at: i64,
}

/// Insertable struct for a declared topic.
#[derive(Insertable, Debug)]
#[diesel(table_name = topics)]
pub struct NewTopicRow<'a> {
    pub name: &'a str,
    pub mode: &'a str,
    pub retention_ms: Option<i64>,
    pub created_at: i64,
}

// ── Topic Deliveries (per-message ack) ──────────────────────────

/// Per-`(subscription, message)` delivery state for a per-message log consumer.
/// `lease_expires_at` bounds an in-flight lease (0 = available for redelivery);
/// `acked` ends the delivery; `attempts` counts (re)deliveries.
#[derive(Insertable, Debug)]
#[diesel(table_name = topic_deliveries)]
pub struct NewTopicDeliveryRow<'a> {
    pub topic: &'a str,
    pub subscription_name: &'a str,
    pub message_id: &'a str,
    pub acked: bool,
    pub attempts: i32,
    pub lease_expires_at: i64,
    pub delivered_at: i64,
}

// ── Distributed Locks ───────────────────────────────────────────

#[derive(Queryable, Selectable, QueryableByName, Debug, Clone)]
#[diesel(table_name = distributed_locks)]
pub struct LockInfoRow {
    pub lock_name: String,
    pub owner_id: String,
    pub acquired_at: i64,
    pub expires_at: i64,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = distributed_locks)]
pub struct NewLockRow<'a> {
    pub lock_name: &'a str,
    pub owner_id: &'a str,
    pub acquired_at: i64,
    pub expires_at: i64,
}

// ── Execution Claims ────────────────────────────────────────────

#[derive(Insertable, Debug)]
#[diesel(table_name = execution_claims)]
pub struct NewExecutionClaimRow<'a> {
    pub job_id: &'a str,
    pub worker_id: &'a str,
    pub claimed_at: i64,
}

// ── Archived Jobs ───────────────────────────────────────────────

#[derive(Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = archived_jobs)]
pub struct ArchivedJobRow {
    pub id: String,
    pub queue: String,
    pub task_name: String,
    pub payload: Vec<u8>,
    pub status: i32,
    pub priority: i32,
    pub created_at: i64,
    pub scheduled_at: i64,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub retry_count: i32,
    pub max_retries: i32,
    pub result: Option<Vec<u8>>,
    pub error: Option<String>,
    pub timeout_ms: i64,
    pub unique_key: Option<String>,
    pub progress: Option<i32>,
    pub metadata: Option<String>,
    pub notes: Option<String>,
    pub cancel_requested: i32,
    pub expires_at: Option<i64>,
    pub result_ttl_ms: Option<i64>,
    pub namespace: Option<String>,
}

/// An `archived_jobs` row without the `payload`/`result` blobs. Terminal-status
/// listings select this so paging the archive never reads the arg/result blobs;
/// they are loaded (via the full [`ArchivedJobRow`]) only by a `get_job` detail
/// lookup. Mirrors [`NarrowJobRow`] for the archive table.
#[derive(Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = archived_jobs)]
pub struct NarrowArchivedJobRow {
    pub id: String,
    pub queue: String,
    pub task_name: String,
    pub status: i32,
    pub priority: i32,
    pub created_at: i64,
    pub scheduled_at: i64,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub retry_count: i32,
    pub max_retries: i32,
    pub error: Option<String>,
    pub timeout_ms: i64,
    pub unique_key: Option<String>,
    pub progress: Option<i32>,
    pub metadata: Option<String>,
    pub notes: Option<String>,
    pub cancel_requested: i32,
    pub expires_at: Option<i64>,
    pub result_ttl_ms: Option<i64>,
    pub namespace: Option<String>,
}

/// Insertable struct for archived job entries.
///
/// Mirrors [`ArchivedJobRow`] with borrowed fields. The `archived_jobs` table
/// has no `has_deps` column (terminal jobs are never re-dequeued), so it is
/// absent here.
#[derive(Insertable, Debug)]
#[diesel(table_name = archived_jobs)]
pub struct NewArchivedJobRow<'a> {
    pub id: &'a str,
    pub queue: &'a str,
    pub task_name: &'a str,
    pub payload: &'a [u8],
    pub status: i32,
    pub priority: i32,
    pub created_at: i64,
    pub scheduled_at: i64,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub retry_count: i32,
    pub max_retries: i32,
    pub result: Option<&'a [u8]>,
    pub error: Option<&'a str>,
    pub timeout_ms: i64,
    pub unique_key: Option<&'a str>,
    pub progress: Option<i32>,
    pub metadata: Option<&'a str>,
    pub notes: Option<&'a str>,
    pub cancel_requested: i32,
    pub expires_at: Option<i64>,
    pub result_ttl_ms: Option<i64>,
    pub namespace: Option<&'a str>,
}

// ── Row → record conversions ─────────────────────────────────────
//
// The Diesel rows above are crate-private; the Storage trait speaks the plain
// records in `storage::records`. Diesel query sites map with `.into()` right
// after loading; insert sites build the borrowed insert rows from records.

impl From<JobErrorRow> for JobError {
    fn from(r: JobErrorRow) -> Self {
        JobError {
            id: r.id,
            job_id: r.job_id,
            attempt: r.attempt,
            error: r.error,
            failed_at: r.failed_at,
        }
    }
}

impl From<RateLimitRow> for RateLimitState {
    fn from(r: RateLimitRow) -> Self {
        RateLimitState {
            key: r.key,
            tokens: r.tokens,
            max_tokens: r.max_tokens,
            refill_rate: r.refill_rate,
            last_refill: r.last_refill,
        }
    }
}

impl From<&RateLimitState> for RateLimitRow {
    fn from(s: &RateLimitState) -> Self {
        RateLimitRow {
            key: s.key.clone(),
            tokens: s.tokens,
            max_tokens: s.max_tokens,
            refill_rate: s.refill_rate,
            last_refill: s.last_refill,
        }
    }
}

impl From<PeriodicTaskRow> for PeriodicTask {
    fn from(r: PeriodicTaskRow) -> Self {
        PeriodicTask {
            name: r.name,
            task_name: r.task_name,
            cron_expr: r.cron_expr,
            args: r.args,
            kwargs: r.kwargs,
            queue: r.queue,
            enabled: r.enabled,
            last_run: r.last_run,
            next_run: r.next_run,
            timezone: r.timezone,
        }
    }
}

impl From<SubscriptionRow> for Subscription {
    fn from(r: SubscriptionRow) -> Self {
        Subscription {
            topic: r.topic,
            subscription_name: r.subscription_name,
            task_name: r.task_name,
            queue: r.queue,
            active: r.active,
            durable: r.durable,
            owner_worker_id: r.owner_worker_id,
            created_at: r.created_at,
            priority: r.priority,
            max_retries: r.max_retries,
            timeout_ms: r.timeout_ms,
            mode: SubscriptionMode::from_wire(&r.mode),
            cursor: r.cursor,
        }
    }
}

impl From<TopicMessageRow> for TopicMessage {
    fn from(r: TopicMessageRow) -> Self {
        TopicMessage {
            id: r.id,
            topic: r.topic,
            payload: r.payload,
            metadata: r.metadata,
            notes: r.notes,
            created_at: r.created_at,
            expires_at: r.expires_at,
        }
    }
}

impl From<TopicRow> for Topic {
    fn from(r: TopicRow) -> Self {
        Topic {
            name: r.name,
            mode: SubscriptionMode::from_wire(&r.mode),
            retention_ms: r.retention_ms,
            created_at: r.created_at,
        }
    }
}

impl From<TaskMetricRow> for TaskMetric {
    fn from(r: TaskMetricRow) -> Self {
        TaskMetric {
            id: r.id,
            task_name: r.task_name,
            job_id: r.job_id,
            wall_time_ns: r.wall_time_ns,
            memory_bytes: r.memory_bytes,
            succeeded: r.succeeded,
            recorded_at: r.recorded_at,
        }
    }
}

impl From<ReplayHistoryRow> for ReplayEntry {
    fn from(r: ReplayHistoryRow) -> Self {
        ReplayEntry {
            id: r.id,
            original_job_id: r.original_job_id,
            replay_job_id: r.replay_job_id,
            replayed_at: r.replayed_at,
            original_result: r.original_result,
            replay_result: r.replay_result,
            original_error: r.original_error,
            replay_error: r.replay_error,
        }
    }
}

impl From<TaskLogRow> for TaskLogEntry {
    fn from(r: TaskLogRow) -> Self {
        TaskLogEntry {
            id: r.id,
            job_id: r.job_id,
            task_name: r.task_name,
            level: r.level,
            message: r.message,
            extra: r.extra,
            logged_at: r.logged_at,
        }
    }
}

impl From<CircuitBreakerRow> for CircuitBreakerState {
    fn from(r: CircuitBreakerRow) -> Self {
        CircuitBreakerState {
            task_name: r.task_name,
            state: r.state,
            failure_count: r.failure_count,
            last_failure_at: r.last_failure_at,
            opened_at: r.opened_at,
            half_open_at: r.half_open_at,
            threshold: r.threshold,
            window_ms: r.window_ms,
            cooldown_ms: r.cooldown_ms,
            half_open_max_probes: r.half_open_max_probes,
            half_open_success_rate: r.half_open_success_rate,
            half_open_probe_count: r.half_open_probe_count,
            half_open_success_count: r.half_open_success_count,
            half_open_failure_count: r.half_open_failure_count,
        }
    }
}

impl From<&CircuitBreakerState> for CircuitBreakerRow {
    fn from(s: &CircuitBreakerState) -> Self {
        CircuitBreakerRow {
            task_name: s.task_name.clone(),
            state: s.state,
            failure_count: s.failure_count,
            last_failure_at: s.last_failure_at,
            opened_at: s.opened_at,
            half_open_at: s.half_open_at,
            threshold: s.threshold,
            window_ms: s.window_ms,
            cooldown_ms: s.cooldown_ms,
            half_open_max_probes: s.half_open_max_probes,
            half_open_success_rate: s.half_open_success_rate,
            half_open_probe_count: s.half_open_probe_count,
            half_open_success_count: s.half_open_success_count,
            half_open_failure_count: s.half_open_failure_count,
        }
    }
}

impl From<WorkerRow> for WorkerInfo {
    fn from(r: WorkerRow) -> Self {
        WorkerInfo {
            worker_id: r.worker_id,
            last_heartbeat: r.last_heartbeat,
            queues: r.queues,
            status: r.status,
            tags: r.tags,
            resources: r.resources,
            resource_health: r.resource_health,
            threads: r.threads,
            started_at: r.started_at,
            hostname: r.hostname,
            pid: r.pid,
            pool_type: r.pool_type,
            sdk: r.sdk,
            sdk_version: r.sdk_version,
        }
    }
}

impl From<LockInfoRow> for LockInfo {
    fn from(r: LockInfoRow) -> Self {
        LockInfo {
            lock_name: r.lock_name,
            owner_id: r.owner_id,
            acquired_at: r.acquired_at,
            expires_at: r.expires_at,
        }
    }
}
