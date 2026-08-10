//! Serde views marshalled across the JNI boundary as JSON.
//!
//! Option and filter structs cross as JSON strings (decoded here); opaque job
//! payloads cross as raw `byte[]` and are never interpreted by the core.

use base64::Engine;
use serde::{Deserialize, Serialize};
use taskito_core::job::{now_millis, Job, NewJob};
use taskito_core::pubsub::{DeliveryDefaults, PublishRequest};
use taskito_core::resilience::circuit_breaker::CircuitState;
use taskito_core::storage::records::{
    CircuitBreakerState, JobError, LockInfo, PeriodicTask, ReplayEntry, Subscription, TaskLogEntry,
    TaskMetric, Topic, TopicLogStats, TopicMessage, WorkerInfo,
};
use taskito_core::storage::{DeadJob, QueueStats, SubscriptionBacklogStats};

use crate::error::BindingError;

const DEFAULT_QUEUE: &str = "default";

/// Options accepted by `NativeQueue.open`.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenOptions {
    pub backend: Option<String>,
    pub dsn: String,
    pub pool_size: Option<u32>,
    /// Postgres schema; read only by the `postgres` backend.
    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    pub schema: Option<String>,
    /// Redis key prefix; read only by the `redis` backend.
    #[cfg_attr(not(feature = "redis"), allow(dead_code))]
    pub prefix: Option<String>,
    pub namespace: Option<String>,
}

/// Per-enqueue options. Every field is optional; absent fields take core
/// defaults.
#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct EnqueueOptions {
    pub queue: Option<String>,
    pub priority: Option<i32>,
    pub max_retries: Option<i32>,
    pub timeout_ms: Option<i64>,
    pub delay_ms: Option<i64>,
    pub unique_key: Option<String>,
    pub metadata: Option<String>,
    pub namespace: Option<String>,
    pub depends_on: Option<Vec<String>>,
    /// Pre-encoded canonical JSON produced by the SDK; stored verbatim.
    pub notes: Option<String>,
}

/// Parse a JSON argument, attributing any failure to the named field.
pub fn parse_json<'a, T: Deserialize<'a>>(raw: &'a str, what: &str) -> Result<T, BindingError> {
    serde_json::from_str(raw).map_err(|e| BindingError::new(format!("invalid {what} JSON: {e}")))
}

/// Build a [`NewJob`] from an enqueue request, applying the queue's default
/// namespace when the request does not set one.
pub fn build_new_job(
    task_name: String,
    payload: Vec<u8>,
    options: EnqueueOptions,
    default_namespace: Option<&str>,
) -> NewJob {
    let delay = options.delay_ms.unwrap_or(0).max(0);
    NewJob {
        queue: options.queue.unwrap_or_else(|| DEFAULT_QUEUE.to_string()),
        task_name,
        payload,
        priority: options.priority.unwrap_or(0),
        // Saturate rather than wrap/panic on an absurd delay.
        scheduled_at: now_millis().saturating_add(delay),
        max_retries: options.max_retries.unwrap_or(0),
        timeout_ms: options.timeout_ms.unwrap_or(0),
        unique_key: options.unique_key,
        metadata: options.metadata,
        notes: options.notes,
        depends_on: options.depends_on.unwrap_or_default(),
        expires_at: None,
        result_ttl_ms: None,
        namespace: options
            .namespace
            .or_else(|| default_namespace.map(str::to_string)),
    }
}

/// Options accepted by `NativeQueue.publish`. Delivery fields left unset
/// resolve per subscriber (its own task defaults, then the queue defaults).
#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct PublishOptions {
    pub idempotency_key: Option<String>,
    pub metadata: Option<String>,
    /// Pre-encoded canonical notes JSON; the core stamps topic/subscription in.
    pub notes: Option<String>,
    pub priority: Option<i32>,
    pub delay_ms: Option<i64>,
    pub max_retries: Option<i32>,
    pub timeout_ms: Option<i64>,
    pub expires_ms: Option<i64>,
    pub result_ttl_ms: Option<i64>,
}

/// Build a core [`PublishRequest`] from a publish call. The queue-level
/// fallback is [`build_new_job`]'s zero defaults — the Java shell keeps no
/// queue-wide delivery config, so enqueue and publish resolve identically.
pub fn build_publish_request(
    topic: String,
    payload: Vec<u8>,
    options: PublishOptions,
    default_namespace: Option<&str>,
) -> PublishRequest {
    let now = now_millis();
    PublishRequest {
        topic,
        payload,
        idempotency_key: options.idempotency_key,
        metadata: options.metadata,
        notes: options.notes,
        priority: options.priority,
        // Saturate rather than wrap/panic on an absurd delay or expiry.
        scheduled_at: now.saturating_add(options.delay_ms.unwrap_or(0).max(0)),
        max_retries: options.max_retries,
        timeout_ms: options.timeout_ms,
        expires_at: options.expires_ms.map(|ms| now.saturating_add(ms.max(0))),
        result_ttl_ms: options.result_ttl_ms,
        namespace: default_namespace.map(str::to_string),
        queue_defaults: DeliveryDefaults {
            priority: 0,
            max_retries: 0,
            timeout_ms: 0,
        },
    }
}

/// Job-count snapshot returned by `NativeQueue.stats`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsView {
    pub pending: i64,
    pub running: i64,
    pub completed: i64,
    pub failed: i64,
    pub dead: i64,
    pub cancelled: i64,
}

impl From<QueueStats> for StatsView {
    fn from(s: QueueStats) -> Self {
        Self {
            pending: s.pending,
            running: s.running,
            completed: s.completed,
            failed: s.failed,
            dead: s.dead,
            cancelled: s.cancelled,
        }
    }
}

/// Java-facing view of a job. Omits the opaque `payload`/`result` blobs; the
/// Java side deserializes those itself. `status` is the lowercase wire string
/// shared across SDKs. Timestamps are Unix milliseconds.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobView<'a> {
    pub id: &'a str,
    pub queue: &'a str,
    pub task_name: &'a str,
    pub status: &'static str,
    pub priority: i32,
    pub created_at: i64,
    pub scheduled_at: i64,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub retry_count: i32,
    pub max_retries: i32,
    pub timeout_ms: i64,
    pub progress: Option<i32>,
    pub error: Option<&'a str>,
    pub unique_key: Option<&'a str>,
    pub namespace: Option<&'a str>,
    /// Opaque metadata blob (JSON the SDK sets, e.g. middleware-injected trace ids).
    pub metadata: Option<&'a str>,
    /// Structured notes as canonical JSON; the SDK parses it back into a map.
    pub notes: Option<&'a str>,
}

impl<'a> From<&'a Job> for JobView<'a> {
    fn from(j: &'a Job) -> Self {
        Self {
            id: &j.id,
            queue: &j.queue,
            task_name: &j.task_name,
            status: j.status.as_str(),
            priority: j.priority,
            created_at: j.created_at,
            scheduled_at: j.scheduled_at,
            started_at: j.started_at,
            completed_at: j.completed_at,
            retry_count: j.retry_count,
            max_retries: j.max_retries,
            timeout_ms: j.timeout_ms,
            progress: j.progress,
            error: j.error.as_deref(),
            unique_key: j.unique_key.as_deref(),
            namespace: j.namespace.as_deref(),
            metadata: j.metadata.as_deref(),
            notes: j.notes.as_deref(),
        }
    }
}

/// Java-facing view of a topic subscription. `created_at` is Unix milliseconds.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionView<'a> {
    pub topic: &'a str,
    pub subscription_name: &'a str,
    pub task_name: &'a str,
    pub queue: &'a str,
    pub active: bool,
    pub durable: bool,
    pub owner_worker_id: Option<&'a str>,
    pub created_at: i64,
}

impl<'a> From<&'a Subscription> for SubscriptionView<'a> {
    fn from(r: &'a Subscription) -> Self {
        Self {
            topic: &r.topic,
            subscription_name: &r.subscription_name,
            task_name: &r.task_name,
            queue: &r.queue,
            active: r.active,
            durable: r.durable,
            owner_worker_id: r.owner_worker_id.as_deref(),
            created_at: r.created_at,
        }
    }
}

/// Java-facing view of one log-topic message. The opaque payload crosses as a
/// base64 string (JSON can't carry raw bytes); `metadata`/`notes` stay JSON
/// strings the SDK parses back into maps. `created_at` is Unix milliseconds.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicMessageView {
    /// Message id — the cursor token passed to `ackTopicCursor`.
    pub id: String,
    /// Opaque payload, base64-encoded.
    pub payload: String,
    pub metadata: Option<String>,
    pub notes: Option<String>,
    pub created_at: i64,
}

impl From<&TopicMessage> for TopicMessageView {
    fn from(m: &TopicMessage) -> Self {
        Self {
            id: m.id.clone(),
            payload: base64::engine::general_purpose::STANDARD.encode(&m.payload),
            metadata: m.metadata.clone(),
            notes: m.notes.clone(),
            created_at: m.created_at,
        }
    }
}

/// Java-facing lag snapshot for one log subscription. `cursor` is null until the
/// first ack; `oldest_unacked_age_ms` is null once the subscription is caught up.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicLogStatsView {
    pub topic: String,
    pub subscription: String,
    pub cursor: Option<String>,
    pub lag: i64,
    pub oldest_unacked_age_ms: Option<i64>,
}

impl From<&TopicLogStats> for TopicLogStatsView {
    fn from(s: &TopicLogStats) -> Self {
        Self {
            topic: s.topic.clone(),
            subscription: s.subscription_name.clone(),
            cursor: s.cursor.clone(),
            lag: s.lag,
            oldest_unacked_age_ms: s.oldest_unacked_age_ms,
        }
    }
}

/// Java-facing backlog snapshot for one topic subscription. One entry per
/// *registered* subscription — paused and ephemeral ones included — even at zero
/// backlog. `oldest_pending_age_ms` is null when nothing is pending.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicStatsView<'a> {
    pub topic: &'a str,
    pub subscription: &'a str,
    pub task_name: &'a str,
    pub queue: &'a str,
    pub active: bool,
    pub durable: bool,
    pub pending: i64,
    pub running: i64,
    pub dead: i64,
    pub oldest_pending_age_ms: Option<i64>,
}

impl<'a> From<&'a SubscriptionBacklogStats> for TopicStatsView<'a> {
    fn from(s: &'a SubscriptionBacklogStats) -> Self {
        Self {
            topic: &s.topic,
            subscription: &s.subscription_name,
            task_name: &s.task_name,
            queue: &s.queue,
            active: s.active,
            durable: s.durable,
            pending: s.pending,
            running: s.running,
            dead: s.dead,
            oldest_pending_age_ms: s.oldest_pending_age_ms,
        }
    }
}

/// Java-facing view of a declared topic. `retention_ms` is null when the backlog
/// is kept until consumed; `created_at` is Unix milliseconds.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicView<'a> {
    pub name: &'a str,
    pub mode: &'a str,
    pub retention_ms: Option<i64>,
    pub created_at: i64,
}

impl<'a> From<&'a Topic> for TopicView<'a> {
    fn from(t: &'a Topic) -> Self {
        Self {
            name: &t.name,
            mode: t.mode.as_str(),
            retention_ms: t.retention_ms,
            created_at: t.created_at,
        }
    }
}

/// Java-facing view of a task's circuit-breaker state. `state` is the lowercase wire
/// string (`closed`/`open`/`half_open`); timestamps are Unix milliseconds.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CircuitBreakerView<'a> {
    pub task_name: &'a str,
    pub state: &'static str,
    pub failure_count: i32,
    pub threshold: i32,
    pub window_ms: i64,
    pub cooldown_ms: i64,
    pub opened_at: Option<i64>,
    pub last_failure_at: Option<i64>,
    pub half_open_max_probes: i32,
    pub half_open_success_rate: f64,
}

impl<'a> From<&'a CircuitBreakerState> for CircuitBreakerView<'a> {
    fn from(r: &'a CircuitBreakerState) -> Self {
        Self {
            task_name: &r.task_name,
            state: CircuitState::from_i32(r.state).as_str(),
            failure_count: r.failure_count,
            threshold: r.threshold,
            window_ms: r.window_ms,
            cooldown_ms: r.cooldown_ms,
            opened_at: r.opened_at,
            last_failure_at: r.last_failure_at,
            half_open_max_probes: r.half_open_max_probes,
            half_open_success_rate: r.half_open_success_rate,
        }
    }
}

/// Options accepted by `NativeQueue.runWorker`.
#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct WorkerOptions {
    pub queues: Option<Vec<String>>,
    pub channel_capacity: Option<u32>,
    pub batch_size: Option<u32>,
    /// Worker execution parallelism (Java thread-pool size). Bounds the
    /// scheduler's in-flight dispatch so it never claims more than the pool can
    /// run and starves peers sharing the DB.
    pub concurrency: Option<u32>,
    /// Per-task retry-backoff policies, registered with the scheduler at start.
    /// The core owns the retry engine; this only feeds it the backoff curve.
    pub task_configs: Option<Vec<TaskRetryConfig>>,
    /// Raw `taskito_mesh::MeshConfig` JSON. When present (and the `mesh` feature
    /// is built), the worker interposes the mesh bridge between scheduler and
    /// dispatcher; otherwise this is ignored.
    #[cfg_attr(not(feature = "mesh"), allow(dead_code))]
    pub mesh_config: Option<String>,
    /// Topic subscriptions written at worker start; ephemeral entries bind to
    /// the started worker's id.
    pub subscriptions: Option<Vec<SubscriptionSpec>>,
    /// Per-table retention windows for auto-cleanup, in seconds.
    pub retention: Option<RetentionSpec>,
    /// Per-queue config registered with the scheduler at start. Carries opt-in
    /// CoDel load shedding and dispatch order.
    pub queue_configs: Option<Vec<QueueConfigSpec>>,
    /// Opt into event-driven dispatch: an enqueue wakes the scheduler right
    /// away instead of it waiting for the next poll. Requires the library to be
    /// built with the `push-dispatch` cargo feature; otherwise accepted and
    /// ignored (polling is kept).
    pub push_dispatch: Option<bool>,
}

/// Per-queue scheduler config. Only queues with a value here are registered; an
/// entry with no positive CoDel bounds and a non-`lifo` order is a no-op.
#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct QueueConfigSpec {
    pub name: String,
    pub codel_target_ms: Option<i64>,
    pub codel_interval_ms: Option<i64>,
    pub dispatch_order: Option<String>,
}

/// Per-table retention windows in seconds. An unset field keeps that table
/// forever. `u32` seconds caps at ~136 years and cannot be negative.
#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct RetentionSpec {
    pub archived_jobs: Option<u32>,
    pub dead_letter: Option<u32>,
    pub task_logs: Option<u32>,
    pub task_metrics: Option<u32>,
    pub job_errors: Option<u32>,
}

impl RetentionSpec {
    /// Build a core [`RetentionConfig`]. An all-unset spec yields an empty
    /// config, which disables retention — deliberately distinct from *omitting*
    /// the `retention` option, which falls back to the recommended defaults.
    pub fn to_config(&self) -> taskito_core::scheduler::retention::RetentionConfig {
        let to_ms = |secs: Option<u32>| secs.map(|s| s as i64 * 1000);
        taskito_core::scheduler::retention::RetentionConfig {
            archived_jobs_ttl_ms: to_ms(self.archived_jobs),
            dead_letter_ttl_ms: to_ms(self.dead_letter),
            task_logs_ttl_ms: to_ms(self.task_logs),
            task_metrics_ttl_ms: to_ms(self.task_metrics),
            job_errors_ttl_ms: to_ms(self.job_errors),
        }
    }
}

/// One topic subscription declared by the SDK, registered at worker start.
/// Delivery settings (from the subscriber task's own config) are persisted on
/// the row so a producer-only process applies them without loading the task.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionSpec {
    pub topic: String,
    pub subscription_name: String,
    pub task_name: String,
    pub queue: String,
    pub durable: bool,
    #[serde(default)]
    pub priority: Option<i32>,
    #[serde(default)]
    pub max_retries: Option<i32>,
    #[serde(default)]
    pub timeout_ms: Option<i64>,
}

/// A task's policy: retry curve, throttling, and concurrency caps. Fields left
/// unset fall back to the core's defaults. The per-job retry *budget* travels
/// via `maxRetries` on enqueue, so it is deliberately absent here.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskRetryConfig {
    pub name: String,
    pub base_delay_ms: Option<i64>,
    pub max_delay_ms: Option<i64>,
    pub custom_delays_ms: Option<Vec<i64>>,
    /// Circuit-breaker knobs. `threshold` present ⇒ the breaker is enabled; the SDK
    /// already converts window/cooldown to milliseconds, so these are stored as-is.
    pub circuit_breaker_threshold: Option<i32>,
    pub circuit_breaker_window_ms: Option<i64>,
    pub circuit_breaker_cooldown_ms: Option<i64>,
    pub circuit_breaker_half_open_probes: Option<i32>,
    pub circuit_breaker_half_open_success_rate: Option<f64>,
    /// Rate-limit spec like `"100/m"`, `"50/s"`, `"3600/h"`.
    pub rate_limit: Option<String>,
    /// Cap on how fast this task may *retry*, across all of its jobs. Same spec
    /// as `rate_limit`; once spent, failures dead-letter instead of retrying.
    pub retry_budget: Option<String>,
    /// Cap on concurrently-running jobs of this task, across the cluster.
    pub max_concurrent: Option<i32>,
    /// Cap on this task's share of one worker's dispatch slots. In-process,
    /// unlike `max_concurrent`, which is cluster-wide and costs a DB read.
    pub max_in_flight_per_task: Option<i32>,
}

/// Filter accepted by `NativeQueue.listJobs`.
#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct JobFilter {
    pub status: Option<String>,
    pub queue: Option<String>,
    pub task: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

/// One page of a keyset-paginated listing, as the Java `Page<T>` decodes it.
/// `next_cursor` is absent on the last page.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PageView<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

/// Map a lowercase status string to the core's `i32` status code.
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

/// Periodic-task view (omits the opaque args/kwargs payloads).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PeriodicTaskView<'a> {
    pub name: &'a str,
    pub task_name: &'a str,
    pub cron_expr: &'a str,
    pub queue: &'a str,
    pub enabled: bool,
    pub last_run: Option<i64>,
    pub next_run: i64,
    pub timezone: Option<&'a str>,
}

impl<'a> From<&'a PeriodicTask> for PeriodicTaskView<'a> {
    fn from(p: &'a PeriodicTask) -> Self {
        Self {
            name: &p.name,
            task_name: &p.task_name,
            cron_expr: &p.cron_expr,
            queue: &p.queue,
            enabled: p.enabled,
            last_run: p.last_run,
            next_run: p.next_run,
            timezone: p.timezone.as_deref(),
        }
    }
}

/// Dead-letter entry view (omits the opaque payload).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeadJobView<'a> {
    pub id: &'a str,
    pub original_job_id: &'a str,
    pub queue: &'a str,
    pub task_name: &'a str,
    pub error: Option<&'a str>,
    pub retry_count: i32,
    pub failed_at: i64,
    pub metadata: Option<&'a str>,
    pub dlq_retry_count: i32,
}

impl<'a> From<&'a DeadJob> for DeadJobView<'a> {
    fn from(d: &'a DeadJob) -> Self {
        Self {
            id: &d.id,
            original_job_id: &d.original_job_id,
            queue: &d.queue,
            task_name: &d.task_name,
            error: d.error.as_deref(),
            retry_count: d.retry_count,
            failed_at: d.failed_at,
            metadata: d.metadata.as_deref(),
            dlq_retry_count: d.dlq_retry_count,
        }
    }
}

/// One recorded error attempt for a job.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobErrorView<'a> {
    pub id: &'a str,
    pub job_id: &'a str,
    pub attempt: i32,
    pub error: &'a str,
    pub failed_at: i64,
}

impl<'a> From<&'a JobError> for JobErrorView<'a> {
    fn from(e: &'a JobError) -> Self {
        Self {
            id: &e.id,
            job_id: &e.job_id,
            attempt: e.attempt,
            error: &e.error,
            failed_at: e.failed_at,
        }
    }
}

/// A per-execution task metric.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricView<'a> {
    pub task_name: &'a str,
    pub job_id: &'a str,
    pub wall_time_ns: i64,
    pub memory_bytes: i64,
    pub succeeded: bool,
    pub recorded_at: i64,
}

impl<'a> From<&'a TaskMetric> for MetricView<'a> {
    fn from(m: &'a TaskMetric) -> Self {
        Self {
            task_name: &m.task_name,
            job_id: &m.job_id,
            wall_time_ns: m.wall_time_ns,
            memory_bytes: m.memory_bytes,
            succeeded: m.succeeded,
            recorded_at: m.recorded_at,
        }
    }
}

/// A registered worker (heartbeat + identity).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerView<'a> {
    pub worker_id: &'a str,
    pub queues: &'a str,
    pub status: &'a str,
    pub last_heartbeat: i64,
    pub started_at: Option<i64>,
    pub hostname: Option<&'a str>,
    pub pid: Option<i32>,
    pub pool_type: Option<&'a str>,
    pub threads: i32,
    pub tags: Option<&'a str>,
    /// JSON array of resource names the worker advertised at registration.
    pub resources: Option<&'a str>,
    /// JSON object of per-resource health, written by the worker's heartbeat.
    pub resource_health: Option<&'a str>,
    /// SDK that registered the worker, and the release of it.
    pub sdk: Option<&'a str>,
    pub sdk_version: Option<&'a str>,
}

impl<'a> From<&'a WorkerInfo> for WorkerView<'a> {
    fn from(w: &'a WorkerInfo) -> Self {
        Self {
            worker_id: &w.worker_id,
            queues: &w.queues,
            status: &w.status,
            last_heartbeat: w.last_heartbeat,
            started_at: w.started_at,
            hostname: w.hostname.as_deref(),
            pid: w.pid,
            pool_type: w.pool_type.as_deref(),
            threads: w.threads,
            tags: w.tags.as_deref(),
            resources: w.resources.as_deref(),
            resource_health: w.resource_health.as_deref(),
            sdk: w.sdk.as_deref(),
            sdk_version: w.sdk_version.as_deref(),
        }
    }
}

/// One replay of a job (result blobs are omitted; ids + errors suffice).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplayEntryView<'a> {
    pub id: &'a str,
    pub original_job_id: &'a str,
    pub replay_job_id: &'a str,
    pub replayed_at: i64,
    pub original_error: Option<&'a str>,
    pub replay_error: Option<&'a str>,
}

impl<'a> From<&'a ReplayEntry> for ReplayEntryView<'a> {
    fn from(r: &'a ReplayEntry) -> Self {
        Self {
            id: &r.id,
            original_job_id: &r.original_job_id,
            replay_job_id: &r.replay_job_id,
            replayed_at: r.replayed_at,
            original_error: r.original_error.as_deref(),
            replay_error: r.replay_error.as_deref(),
        }
    }
}

/// A task log line.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogView<'a> {
    pub id: &'a str,
    pub job_id: &'a str,
    pub task_name: &'a str,
    pub level: &'a str,
    pub message: &'a str,
    pub extra: Option<&'a str>,
    pub logged_at: i64,
}

impl<'a> From<&'a TaskLogEntry> for LogView<'a> {
    fn from(l: &'a TaskLogEntry) -> Self {
        Self {
            id: &l.id,
            job_id: &l.job_id,
            task_name: &l.task_name,
            level: &l.level,
            message: &l.message,
            extra: l.extra.as_deref(),
            logged_at: l.logged_at,
        }
    }
}

/// Current holder of a distributed lock.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LockInfoView<'a> {
    pub lock_name: &'a str,
    pub owner_id: &'a str,
    pub acquired_at: i64,
    pub expires_at: i64,
}

impl<'a> From<&'a LockInfo> for LockInfoView<'a> {
    fn from(l: &'a LockInfo) -> Self {
        Self {
            lock_name: &l.lock_name,
            owner_id: &l.owner_id,
            acquired_at: l.acquired_at,
            expires_at: l.expires_at,
        }
    }
}

/// Serialize a view to a JSON string for return across the boundary.
pub fn to_json<T: Serialize>(value: &T) -> Result<String, BindingError> {
    serde_json::to_string(value).map_err(|e| BindingError::new(format!("serialize failed: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_spec_converts_and_deserializes() {
        let spec: RetentionSpec =
            serde_json::from_str(r#"{"archivedJobs":7,"taskLogs":3}"#).unwrap();
        let config = spec.to_config();
        assert_eq!(config.archived_jobs_ttl_ms, Some(7_000));
        assert_eq!(config.task_logs_ttl_ms, Some(3_000));
        assert_eq!(config.dead_letter_ttl_ms, None);
    }

    #[test]
    fn retention_spec_is_empty_when_no_windows() {
        // A present-but-empty spec disables retention (an empty config), rather
        // than collapsing to the omitted case that would use the defaults.
        assert!(RetentionSpec::default().to_config().is_empty());
    }
}
