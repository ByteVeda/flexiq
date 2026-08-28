//! Plain option objects passed from JavaScript. napi maps snake_case Rust
//! fields to camelCase JS keys (`maxRetries`, `timeoutMs`).

use napi::bindgen_prelude::Buffer;
use napi_derive::napi;

/// How to open a queue's storage backend.
#[napi(object)]
pub struct OpenOptions {
    /// `"sqlite"` (default), `"postgres"`, or `"redis"`. The latter two require
    /// the addon to be built with the matching cargo feature.
    pub backend: Option<String>,
    /// SQLite file path, Postgres URL, or Redis URL.
    pub dsn: String,
    /// Connection pool size (SQLite/Postgres).
    pub pool_size: Option<u32>,
    /// Postgres schema (default `"flexiq"`).
    pub schema: Option<String>,
    /// Redis key prefix.
    pub prefix: Option<String>,
    /// Optional namespace applied to enqueued jobs and the worker scheduler.
    pub namespace: Option<String>,
    /// Whether opening applies pending schema changes. Defaults to true; false
    /// gates every schema change behind an explicit `migrate()`.
    pub auto_migrate: Option<bool>,
}

/// What one explicit `migrate()` call did. Empty lists mean the database was
/// already current, which is the common case and not an error.
#[napi(object)]
pub struct MigrationSummary {
    /// Core schema versions applied by this run, in the order applied.
    pub applied: Vec<String>,
    /// Workflow schema versions applied by this run.
    pub workflow_applied: Vec<String>,
    /// Terminal jobs the one-time backlog sweep moved into `archived_jobs`.
    pub archived_jobs: i64,
    /// The backend stores no schema, so there is nothing to migrate.
    pub schemaless: bool,
}

/// Per-enqueue overrides. All optional — omitted fields fall back to defaults
/// in [`crate::convert::build_new_job`].
#[napi(object)]
#[derive(Default)]
pub struct EnqueueOptions {
    pub queue: Option<String>,
    pub priority: Option<i32>,
    pub max_retries: Option<i32>,
    pub timeout_ms: Option<i64>,
    /// Run no earlier than `now + delayMs` (delayed/scheduled job).
    pub delay_ms: Option<i64>,
    /// Dedup key — a second enqueue with the same key is a no-op while the
    /// first is pending/running (idempotency).
    pub unique_key: Option<String>,
    /// Free-form metadata string stored with the job.
    pub metadata: Option<String>,
    /// Structured notes — a pre-encoded canonical JSON object (the TS shell
    /// validates the bounds and stringifies before passing it here).
    pub notes: Option<String>,
    /// Namespace override for this job.
    pub namespace: Option<String>,
    /// Debounce key, already resolved by the shell. Its presence alone does not
    /// debounce — `debounceWindowMs` is what routes the enqueue.
    pub debounce_key: Option<String>,
    /// Collapse window in milliseconds. Set (with the two fields above and
    /// below) to route this enqueue through `Storage::enqueue_debounced`.
    pub debounce_window_ms: Option<i64>,
    /// Hard ceiling on the total delay, measured from when the window opened.
    /// Mandatory alongside `debounceWindowMs`: without it a caller who never
    /// stops enqueuing starves the job forever.
    pub debounce_max_wait_ms: Option<i64>,
    /// Overwrite the pending job's payload with this one. Omitted keeps the
    /// payload the window opened with.
    pub debounce_replace_payload: Option<bool>,
    /// The target queue's admission cap, applied inside the debounce write and
    /// only on the branch that inserts. Set for a debounced enqueue onto a
    /// capped queue and nowhere else — every other enqueue can tell it is
    /// inserting and checks the cap itself.
    pub debounce_max_pending: Option<i64>,
}

/// One entry in a batch enqueue: an opaque payload plus its own options.
#[napi(object)]
pub struct EnqueueJob {
    pub payload: Buffer,
    pub options: Option<EnqueueOptions>,
}

/// Options for [`crate::queue::JsQueue::publish`]. All optional — omitted
/// delivery settings resolve per subscriber (the subscription row's persisted
/// settings, then queue defaults) in the core.
#[napi(object)]
#[derive(Default)]
pub struct PublishOptions {
    /// Dedup key, salted per subscription in the core so republishing the
    /// same key yields no new delivery for any subscriber.
    pub idempotency_key: Option<String>,
    /// Free-form metadata string stored on every delivery.
    pub metadata: Option<String>,
    /// Pre-encoded canonical notes JSON object; the core stamps `topic` and
    /// `subscription` into it per delivery.
    pub notes: Option<String>,
    pub priority: Option<i32>,
    /// Deliver no earlier than `now + delayMs`.
    pub delay_ms: Option<i64>,
    pub max_retries: Option<i32>,
    pub timeout_ms: Option<i64>,
    /// Expire undelivered jobs at `now + expiresMs`.
    pub expires_ms: Option<i64>,
    pub result_ttl_ms: Option<i64>,
}

/// Filter for [`crate::queue::JsQueue::list_jobs`]. All fields optional.
#[napi(object)]
#[derive(Default)]
pub struct JobFilter {
    /// Lowercase status (`"pending"`, `"running"`, `"complete"`, `"failed"`, `"dead"`, `"cancelled"`).
    pub status: Option<String>,
    pub queue: Option<String>,
    pub task: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

/// Filter for [`crate::queue::JsQueue::list_jobs_filtered`]: everything
/// [`JobFilter`] matches on, plus substring and created-at-range predicates.
/// All fields optional.
#[napi(object)]
#[derive(Default)]
pub struct DetailedJobFilter {
    /// Lowercase status (`"pending"`, `"running"`, `"complete"`, `"failed"`, `"dead"`, `"cancelled"`).
    pub status: Option<String>,
    pub queue: Option<String>,
    pub task: Option<String>,
    /// Substring matched against the job's metadata.
    pub metadata_like: Option<String>,
    /// Substring matched against the job's error.
    pub error_like: Option<String>,
    /// Only jobs created at or after this Unix-millisecond timestamp.
    pub created_after: Option<i64>,
    /// Only jobs created at or before this Unix-millisecond timestamp.
    pub created_before: Option<i64>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

/// Circuit-breaker config for a task: trip after `threshold` failures within
/// `windowMs`, stay open for `cooldownMs`, then probe before fully closing.
#[napi(object)]
pub struct CircuitBreakerInput {
    pub threshold: i32,
    pub window_ms: i64,
    pub cooldown_ms: i64,
    /// Probe requests allowed while half-open (default 5).
    pub half_open_max_probes: Option<i32>,
    /// Success rate (0.0–1.0) required to close from half-open (default 0.8).
    pub half_open_success_rate: Option<f64>,
}

/// Per-task configuration registered on the scheduler before the worker runs.
#[napi(object)]
pub struct TaskConfigInput {
    pub name: String,
    pub max_retries: Option<i32>,
    pub retry_base_delay_ms: Option<i64>,
    pub retry_max_delay_ms: Option<i64>,
    pub max_concurrent: Option<i32>,
    /// Cap on this task's share of one worker's dispatch slots, so a slow task
    /// cannot occupy the whole pool. In-process, unlike `max_concurrent`.
    pub max_in_flight_per_task: Option<i32>,
    /// Rate-limit spec like `"100/m"`, `"50/s"`, `"3600/h"`.
    pub rate_limit: Option<String>,
    /// What a saturated rate limit does to this task's jobs: `"defer"` (the
    /// default) or `"drop"`, which sheds them to the dead-letter queue.
    pub on_excess: Option<String>,
    /// Cap on how fast this task may *retry*, across all of its jobs. Same spec
    /// as `rate_limit`; once spent, failures dead-letter instead of retrying.
    pub retry_budget: Option<String>,
    pub circuit_breaker: Option<CircuitBreakerInput>,
}

/// Per-queue configuration registered on the scheduler before the worker runs.
#[napi(object)]
pub struct QueueConfigInput {
    pub name: String,
    pub max_concurrent: Option<i32>,
    pub rate_limit: Option<String>,
    /// Opt-in CoDel load shedding. Both must be set and positive to enable it:
    /// `codelTargetMs` = acceptable steady-state wait; `codelIntervalMs` = window
    /// the wait must stay above target before jobs are shed to the DLQ.
    pub codel_target_ms: Option<i64>,
    pub codel_interval_ms: Option<i64>,
    /// `"lifo"` dispatches newest-first within a priority under overload;
    /// anything else (or unset) keeps the fair `"fifo"` default. Honored on
    /// SQLite/Postgres; Redis is FIFO-only.
    pub dispatch_order: Option<String>,
}

/// Opt-in mesh overlay for a worker: decentralized peer discovery (SWIM gossip)
/// plus work-stealing across nodes. Ignored unless the addon is built with the
/// `mesh` cargo feature. The DB stays the source of truth — mesh only optimizes
/// dispatch locality.
#[napi(object)]
pub struct MeshWorkerConfig {
    /// UDP gossip port. The TCP work-stealing server binds `port + 1`.
    pub port: u32,
    /// Bind address for both servers (default `"0.0.0.0"`).
    pub bind_addr: Option<String>,
    /// Seed peers to join, as `"host:gossip_port"` strings.
    pub seeds: Option<Vec<String>>,
    /// Enable work-stealing from busier peers (default true).
    pub steal: Option<bool>,
    /// Affinity weight for consistent-hash placement (default from core).
    pub affinity_weight: Option<f64>,
    /// Local deque capacity before back-pressure.
    pub local_buffer: Option<u32>,
    /// Max jobs pulled in one steal.
    pub steal_batch: Option<u32>,
    /// Deque depth at or below which this node tries to steal.
    pub steal_threshold: Option<u32>,
    /// Virtual nodes per peer on the hash ring.
    pub virtual_nodes: Option<u32>,
    /// Address advertised to peers when behind NAT (`"host:port"`).
    pub advertise_addr: Option<String>,
    /// Shared key enabling XOR gossip encryption (must match across the mesh).
    pub encryption_key: Option<String>,
    /// Per-peer steal rate limit (steals per second).
    pub steal_rate_limit: Option<u32>,
}

/// Options for a running worker. `queues` defaults to `["default"]`.
/// Per-table retention windows in seconds. An unset field keeps that table
/// forever. `u32` seconds caps at ~136 years and cannot be negative.
#[napi(object)]
#[derive(Default)]
pub struct RetentionInput {
    pub archived_jobs: Option<u32>,
    pub dead_letter: Option<u32>,
    pub task_logs: Option<u32>,
    pub task_metrics: Option<u32>,
    pub job_errors: Option<u32>,
}

impl RetentionInput {
    /// Build a core [`RetentionConfig`]. An all-unset input yields an empty
    /// config, which disables retention — deliberately distinct from *omitting*
    /// the `retention` option, which falls back to the recommended defaults.
    pub fn to_config(&self) -> flexiq_core::scheduler::retention::RetentionConfig {
        let to_ms = |secs: Option<u32>| secs.map(|s| s as i64 * 1000);
        flexiq_core::scheduler::retention::RetentionConfig {
            archived_jobs_ttl_ms: to_ms(self.archived_jobs),
            dead_letter_ttl_ms: to_ms(self.dead_letter),
            task_logs_ttl_ms: to_ms(self.task_logs),
            task_metrics_ttl_ms: to_ms(self.task_metrics),
            job_errors_ttl_ms: to_ms(self.job_errors),
        }
    }
}

#[napi(object)]
#[derive(Default)]
pub struct WorkerOptions {
    pub queues: Option<Vec<String>>,
    pub channel_capacity: Option<u32>,
    /// Jobs this worker runs at once. Unset leaves in-flight work unbounded,
    /// which is the historical behaviour — a worker then claims jobs it cannot
    /// run yet, stranding them Running and starving peers on the same database.
    pub concurrency: Option<u32>,
    /// Jobs claimed per scheduler poll (default 1).
    pub batch_size: Option<u32>,
    /// Every task name this worker has a handler for, used to fingerprint its
    /// registry on the worker row. Distinct from `task_configs`, which carries
    /// only the tasks that were given a policy — a registry read off that would
    /// be missing every task that took the defaults.
    pub tasks: Option<Vec<String>>,
    pub task_configs: Option<Vec<TaskConfigInput>>,
    pub queue_configs: Option<Vec<QueueConfigInput>>,
    /// Names of the injectable resources this worker serves, advertised on
    /// registration so inspection surfaces them alongside heartbeat health.
    pub resources: Option<Vec<String>>,
    /// Opt-in decentralized mesh overlay (requires the `mesh` build feature).
    pub mesh: Option<MeshWorkerConfig>,
    /// Per-table retention windows for auto-cleanup.
    pub retention: Option<RetentionInput>,
    /// Opt into event-driven dispatch: an enqueue wakes the scheduler right
    /// away instead of it waiting for the next poll. Requires the addon to be
    /// built with the `push-dispatch` cargo feature; otherwise accepted and
    /// ignored (polling is kept).
    pub push_dispatch: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_to_config_converts_seconds_to_ms() {
        let input = RetentionInput {
            archived_jobs: Some(7),
            task_logs: Some(3),
            ..RetentionInput::default()
        };
        let config = input.to_config();
        assert_eq!(config.archived_jobs_ttl_ms, Some(7_000));
        assert_eq!(config.task_logs_ttl_ms, Some(3_000));
        assert_eq!(config.dead_letter_ttl_ms, None);
    }

    #[test]
    fn retention_to_config_is_empty_when_no_windows() {
        // A present-but-empty input disables retention (an empty config), rather
        // than collapsing to the omitted case that would use the defaults.
        assert!(RetentionInput::default().to_config().is_empty());
    }
}
