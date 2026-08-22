pub mod cursor;
mod diesel_common;
pub mod migrate;
/// Code-first schema migrations. The files live at the crate root
/// (`crates/flexiq-core/migrations/`) but compile as part of this crate.
#[path = "../../migrations/mod.rs"]
pub(crate) mod migrations;
pub(crate) mod models;
#[cfg(feature = "push-dispatch")]
pub(crate) mod notify;
/// PostgreSQL storage backend (feature `postgres`).
#[cfg(feature = "postgres")]
pub mod postgres;
pub mod records;
/// Redis storage backend (feature `redis`).
#[cfg(feature = "redis")]
pub mod redis_backend;
pub(crate) mod schema;
/// SQLite storage backend (the default).
pub mod sqlite;
/// The [`Storage`] trait every backend implements.
pub mod traits;

pub use traits::Storage;

use crate::error::Result;
use crate::job::{Job, NewJob};

// ── Shared constants ───────────────────────────────────────────────────

/// Maximum gap between worker heartbeats before a worker is considered dead.
///
/// Used by every backend's `reap_dead_workers()` to compute the `now - 30s`
/// cutoff. Kept in one place so SQLite, Postgres, and Redis can never drift.
pub const DEAD_WORKER_THRESHOLD_MS: i64 = 30_000;

/// Ephemeral subscriptions younger than this survive the reaper even when
/// their owner is not (yet) in the live-worker set: a starting worker inserts
/// its ephemeral subscriptions before its first heartbeat registers it live,
/// and another worker's reap tick must not race that gap. Twice the dead-worker
/// threshold keeps one full failure-detection cycle of headroom.
pub const EPHEMERAL_SUBSCRIPTION_GRACE_MS: i64 = 2 * DEAD_WORKER_THRESHOLD_MS;

/// The `last_heartbeat` at or below which a worker is dead. One definition so
/// the three backends' reaps and the orphan-recovery filter can never drift on
/// the arithmetic (they already share [`DEAD_WORKER_THRESHOLD_MS`]).
pub(crate) fn dead_worker_cutoff(now: i64) -> i64 {
    now.saturating_sub(DEAD_WORKER_THRESHOLD_MS)
}

/// Cluster-wide election lock for the dead-worker reap. Not namespaced — the
/// `workers` table is cluster-global, so one worker per cluster should reap.
pub(crate) const REAPER_LOCK: &str = "flexiq:reaper";

/// Reaper lease. Longer than the 5s heartbeat cadence so the leader keeps the
/// lock across ticks, but under [`DEAD_WORKER_THRESHOLD_MS`] so a dead leader's
/// lock frees before the workers it should have reaped go stale-plus-a-cycle.
pub(crate) const REAPER_LOCK_TTL_MS: i64 = 15_000;

/// Cluster-wide election lock for the retention purge. Separate from the reaper
/// lock so a long cleanup sweep can never starve worker-death detection.
pub(crate) const RETENTION_LOCK: &str = "flexiq:retention";

/// Retention lease. Must exceed the worst-case sweep, which the per-tick batch
/// cap bounds — the first sweep after a retention default flip is the long one.
pub(crate) const RETENTION_LOCK_TTL_MS: i64 = 300_000;

/// How many pending rows a debounce write inspects before giving up on finding
/// an unclaimed one. A healthy key has exactly one; more only appear when a row
/// is mid-claim, so the scan is bounded rather than unlimited.
pub(crate) const DEBOUNCE_CANDIDATE_SCAN: i64 = 8;

/// Check the arguments of a debounced enqueue and hand back the key it
/// coalesces on.
///
/// All three rules are load-bearing. A missing key would debounce every job of
/// the task against every other — the design calls for a payload-derived key
/// like `report:{user_id}`. A non-positive window would schedule the job in the
/// past, so nothing ever coalesces. And a `max_wait_ms` below the window would
/// cap the very first insert, silently making the window meaningless; that is a
/// caller mistake worth surfacing rather than absorbing.
pub(crate) fn validated_debounce_key(
    new_job: &NewJob,
    options: &records::DebounceOptions,
) -> Result<String> {
    let key = new_job.debounce_key.as_deref().unwrap_or("");
    if key.is_empty() {
        return Err(crate::error::QueueError::Config(
            "enqueue_debounced requires a non-empty debounce_key".to_string(),
        ));
    }
    if options.window_ms <= 0 {
        return Err(crate::error::QueueError::Config(format!(
            "debounce window_ms must be positive, got {}",
            options.window_ms
        )));
    }
    if options.max_wait_ms < options.window_ms {
        return Err(crate::error::QueueError::Config(format!(
            "debounce max_wait_ms ({}) must be at least window_ms ({})",
            options.max_wait_ms, options.window_ms
        )));
    }
    Ok(key.to_string())
}

/// Hold `lock_name` as `owner_id` for another `ttl_ms`, renewing first and only
/// taking it when free. Returns whether this caller now holds it.
///
/// The renew-then-acquire order is required, not stylistic: `acquire_lock`
/// refuses a live lock even for its current owner (it only checks
/// `expires_at > now`), so an acquire-first caller would lose leadership every
/// time its own lock is still valid and thrash on each expiry.
///
/// This is best-effort load shedding, not mutual exclusion — the callers
/// (dead-worker reap, retention purge) are idempotent predicate deletes that
/// re-evaluate fresh state, so a lost lock degrades to the pre-election
/// behaviour of every worker doing the sweep, never to incorrectness.
pub(crate) fn try_lead(
    storage: &impl Storage,
    lock_name: &str,
    owner_id: &str,
    ttl_ms: i64,
) -> Result<bool> {
    if storage.extend_lock(lock_name, owner_id, ttl_ms)? {
        return Ok(true);
    }
    storage.acquire_lock(lock_name, owner_id, ttl_ms)
}

/// Reap dead workers if `owner_id` wins the cluster reaper election; a
/// non-leader gets an empty list. One elected reaper per cluster keeps the
/// registry sweep O(1) cluster-wide and fires each `WORKER_OFFLINE` once.
///
/// Reaping is opportunistic on the heartbeat cadence, so every error degrades
/// to "not this tick": a backend error is not lost leadership — it is logged
/// so a storage outage that stalls reaping stays diagnosable, then skipped.
pub fn reap_dead_workers_if_leader(storage: &impl Storage, owner_id: &str) -> Vec<String> {
    let leading = try_lead(storage, REAPER_LOCK, owner_id, REAPER_LOCK_TTL_MS).unwrap_or_else(
        |election_error| {
            log::warn!("reaper election failed: {election_error}");
            false
        },
    );
    if !leading {
        return Vec::new();
    }
    storage.reap_dead_workers().unwrap_or_else(|error| {
        log::warn!("dead-worker reap failed: {error}");
        Vec::new()
    })
}

/// Drop ephemeral subscriptions whose owning worker is gone, filtering the
/// live set by heartbeat. Returns the number removed.
///
/// With `elected_owner` set, the sweep runs only when that owner wins the
/// reaper election (the heartbeat-cadence path — one sweep per cluster);
/// a non-leader returns `Ok(0)`. `None` sweeps unconditionally (a manual
/// admin call, or a shutting-down worker reaping its own rows).
pub fn sweep_ephemeral_subscriptions(
    storage: &impl Storage,
    elected_owner: Option<&str>,
) -> Result<u64> {
    if let Some(owner) = elected_owner {
        if !try_lead(storage, REAPER_LOCK, owner, REAPER_LOCK_TTL_MS)? {
            return Ok(0);
        }
    }
    let cutoff = dead_worker_cutoff(crate::job::now_millis());
    let live = storage.list_live_worker_ids(cutoff)?;
    storage.reap_ephemeral_subscriptions(&live)
}

/// Batch-enqueue `new_jobs`, applying `unique_key` dedup to whichever entries
/// carry one — the batch equivalent of choosing [`Storage::enqueue_unique`] over
/// [`Storage::enqueue`] per job. Returns the created (or already-active) jobs in
/// input order, in one storage call either way. The Java and Node batch enqueues
/// route through here; Python still batch-inserts without dedup.
///
/// One keyed entry sends the whole batch through
/// [`Storage::enqueue_unique_batch`], which check-then-inserts per row (returning
/// the existing active job for a duplicate key) and passes keyless rows straight
/// through. On the Diesel backends that is one transaction, so a mixed batch is
/// all-or-nothing; Redis loops the single-job path per row there as it does for
/// every batch, so a mid-batch failure leaves earlier rows enqueued.
///
/// A batch with no keys at all keeps the chunked multi-row
/// [`Storage::enqueue_batch`] insert, which is the throughput path but would hit
/// the partial unique index and fail the whole batch the moment a key duplicates
/// an active job.
pub fn enqueue_batch_dedup(storage: &impl Storage, new_jobs: Vec<NewJob>) -> Result<Vec<Job>> {
    if new_jobs.iter().any(|job| job.unique_key.is_some()) {
        storage.enqueue_unique_batch(new_jobs)
    } else {
        storage.enqueue_batch(new_jobs)
    }
}

// ── Shared helper types ────────────────────────────────────────────────

/// Per-status job counts for a queue (or the whole cluster).
#[derive(Debug, Clone, Default)]
pub struct QueueStats {
    /// Jobs waiting to run.
    pub pending: i64,
    /// Jobs currently executing.
    pub running: i64,
    /// Jobs that finished successfully.
    pub completed: i64,
    /// Jobs that failed terminally.
    pub failed: i64,
    /// Jobs moved to the dead-letter queue.
    pub dead: i64,
    /// Jobs that were cancelled.
    pub cancelled: i64,
}

/// Per-table cutoffs a retention dry-run counts against. Each is the
/// `now - ttl` epoch-ms boundary the matching purge deletes below; `None` means
/// that table has no window (only its per-entry TTL is swept). Built from the
/// effective [`RetentionConfig`] via `cutoffs(now)`, mirroring the cutoffs
/// `auto_cleanup` hands to each purge.
///
/// [`RetentionConfig`]: crate::scheduler::retention::RetentionConfig
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RetentionCutoffs {
    /// Cutoff for `archived_jobs` (terminal jobs), or `None` for no window.
    pub archived_jobs: Option<i64>,
    /// Cutoff for `dead_letter`, or `None` for no window.
    pub dead_letter: Option<i64>,
    /// Cutoff for `task_logs`, or `None` for no window.
    pub task_logs: Option<i64>,
    /// Cutoff for `task_metrics`, or `None` for no window.
    pub task_metrics: Option<i64>,
    /// Cutoff for `job_errors`, or `None` for no window.
    pub job_errors: Option<i64>,
}

/// Rows a retention purge would delete per history table, as counted by a
/// read-only dry-run. Every field is the count the matching purge in
/// `auto_cleanup` would remove under the same cutoffs — nothing is deleted.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RetentionCounts {
    /// Terminal jobs (`archived_jobs`) that would be purged.
    pub archived_jobs: u64,
    /// Dead-letter entries that would be purged.
    pub dead_letter: u64,
    /// Task-log lines that would be purged.
    pub task_logs: u64,
    /// Task-metric rows that would be purged.
    pub task_metrics: u64,
    /// Per-attempt job-error rows that would be purged.
    pub job_errors: u64,
}

impl RetentionCounts {
    /// Total rows a purge would delete across every table.
    pub fn total(&self) -> u64 {
        self.archived_jobs + self.dead_letter + self.task_logs + self.task_metrics + self.job_errors
    }
}

/// Order in which same-priority jobs are dispatched from a queue. Priority
/// always dominates; this only breaks ties. `Fifo` (oldest-first) is the fair
/// default; `Lifo` (newest-first) is opt-in for freshness-sensitive workloads
/// that would rather run recent work than clear a stale backlog in order.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DispatchOrder {
    /// Oldest-first — the fair default a durable queue is expected to keep.
    #[default]
    Fifo,
    /// Newest-first — opt-in freshness lever for same-priority ties under load.
    Lifo,
}

/// Resolve a queue's dispatch order from a queue→order map, defaulting to
/// `Fifo` for any queue not explicitly set to `Lifo`.
pub(crate) fn order_for(
    orders: &std::collections::HashMap<String, DispatchOrder>,
    queue: &str,
) -> DispatchOrder {
    orders.get(queue).copied().unwrap_or_default()
}

/// Per-subscription backlog/lag snapshot for the pub/sub dashboard. One entry
/// per *registered* subscription — durable or ephemeral, active or paused —
/// even at zero backlog, so the dashboard renders the full subscriber list
/// from a single call. Counts are computed live off the delivery-attribution
/// indexes; they can never drift the way a maintained counter would.
#[derive(Debug, Clone)]
pub struct SubscriptionBacklogStats {
    /// Topic the subscription listens on.
    pub topic: String,
    /// Subscription name, unique per topic.
    pub subscription_name: String,
    /// Task enqueued for each published message.
    pub task_name: String,
    /// Queue deliveries are enqueued into.
    pub queue: String,
    /// Whether deliveries are currently enabled.
    pub active: bool,
    /// True for durable subscriptions that outlive their creator.
    pub durable: bool,
    /// Deliveries waiting to run.
    pub pending: i64,
    /// Deliveries currently executing.
    pub running: i64,
    /// Deliveries in the dead-letter queue.
    pub dead: i64,
    /// Milliseconds since the oldest still-pending delivery was created.
    /// `None` when the subscription currently has no pending backlog.
    pub oldest_pending_age_ms: Option<i64>,
}

/// Merge the four `topic_backlog_stats` aggregate queries into one row per
/// registered subscription. Seeds a zeroed entry per subscription so idle
/// subscriptions still appear, then folds in the pending/running counts,
/// oldest-pending age (converted to a millisecond age), and dead counts.
/// Shared by the Diesel and Redis backends.
pub(crate) fn merge_backlog_stats(
    subs: Vec<records::Subscription>,
    counts: Vec<(String, String, i32, i64)>,
    oldest: Vec<(String, String, Option<i64>)>,
    dead: Vec<(String, String, i64)>,
) -> Vec<SubscriptionBacklogStats> {
    use crate::job::{now_millis, JobStatus};
    use std::collections::HashMap;

    let mut by_key: HashMap<(String, String), SubscriptionBacklogStats> = subs
        .into_iter()
        .map(|s| {
            (
                (s.topic.clone(), s.subscription_name.clone()),
                SubscriptionBacklogStats {
                    topic: s.topic,
                    subscription_name: s.subscription_name,
                    task_name: s.task_name,
                    queue: s.queue,
                    active: s.active,
                    durable: s.durable,
                    pending: 0,
                    running: 0,
                    dead: 0,
                    oldest_pending_age_ms: None,
                },
            )
        })
        .collect();

    for (topic, name, status, count) in counts {
        if let Some(stats) = by_key.get_mut(&(topic, name)) {
            if status == JobStatus::Pending as i32 {
                stats.pending = count;
            } else if status == JobStatus::Running as i32 {
                stats.running = count;
            }
        }
    }

    let now = now_millis();
    for (topic, name, oldest_created) in oldest {
        if let (Some(stats), Some(created)) = (by_key.get_mut(&(topic, name)), oldest_created) {
            stats.oldest_pending_age_ms = Some((now - created).max(0));
        }
    }

    for (topic, name, count) in dead {
        if let Some(stats) = by_key.get_mut(&(topic, name)) {
            stats.dead = count;
        }
    }

    by_key.into_values().collect()
}

/// A dead-lettered job: a copy of the original with its failure context,
/// enough to re-enqueue via `retry_dead`.
#[derive(Debug, Clone)]
pub struct DeadJob {
    /// Unique id of the dead-letter entry.
    pub id: String,
    /// Id of the job that dead-lettered.
    pub original_job_id: String,
    /// Queue the job ran on.
    pub queue: String,
    /// Task name of the job.
    pub task_name: String,
    /// Serialized task arguments.
    pub payload: Vec<u8>,
    /// Final error message (canonical JSON `TaskError` when structured).
    pub error: Option<String>,
    /// Retries the job had consumed when it dead-lettered.
    pub retry_count: i32,
    /// Unix-millisecond time the job dead-lettered.
    pub failed_at: i64,
    /// Pre-encoded JSON metadata blob, if any.
    pub metadata: Option<String>,
    /// Pre-encoded JSON structured notes, if any.
    pub notes: Option<String>,
    /// Job priority (higher runs first).
    pub priority: i32,
    /// Retry cap the job was enqueued with.
    pub max_retries: i32,
    /// Execution timeout in milliseconds.
    pub timeout_ms: i64,
    /// Result retention in milliseconds. `None` = global default.
    pub result_ttl_ms: Option<i64>,
    /// Namespace of the job, if any.
    pub namespace: Option<String>,
    /// Times this entry was auto-retried out of the DLQ.
    pub dlq_retry_count: i32,
}

impl From<models::DeadLetterRow> for DeadJob {
    fn from(row: models::DeadLetterRow) -> Self {
        Self {
            id: row.id,
            original_job_id: row.original_job_id,
            queue: row.queue,
            task_name: row.task_name,
            payload: row.payload,
            error: row.error,
            retry_count: row.retry_count,
            failed_at: row.failed_at,
            metadata: row.metadata,
            notes: row.notes,
            priority: row.priority,
            max_retries: row.max_retries,
            timeout_ms: row.timeout_ms,
            result_ttl_ms: row.result_ttl_ms,
            namespace: row.namespace,
            dlq_retry_count: row.dlq_retry_count,
        }
    }
}

impl DeadJob {
    /// Build a [`DeadJob`] from a blob-free `NarrowDeadLetterRow`. Listing
    /// paths use this so paging the DLQ never loads the `payload` blob; it
    /// comes back empty and is only read when a single entry is requeued by id.
    pub fn from_narrow(row: models::NarrowDeadLetterRow) -> Self {
        Self {
            id: row.id,
            original_job_id: row.original_job_id,
            queue: row.queue,
            task_name: row.task_name,
            payload: Vec::new(),
            error: row.error,
            retry_count: row.retry_count,
            failed_at: row.failed_at,
            metadata: row.metadata,
            notes: row.notes,
            priority: row.priority,
            max_retries: row.max_retries,
            timeout_ms: row.timeout_ms,
            result_ttl_ms: row.result_ttl_ms,
            namespace: row.namespace,
            dlq_retry_count: row.dlq_retry_count,
        }
    }
}

// ── impl_storage! macro ───────────────────────────────────────────────
//
// Every concrete backend (Sqlite, Postgres, Redis) has inherent methods
// with the same signatures as the `Storage` trait.  This macro generates
// the trivial `impl Storage for $T` block that forwards every call.

macro_rules! impl_storage {
    ($type:ty) => {
        impl $crate::storage::traits::Storage for $type {
            fn enqueue(
                &self,
                new_job: $crate::job::NewJob,
            ) -> $crate::error::Result<$crate::job::Job> {
                self.enqueue(new_job)
            }
            fn enqueue_batch(
                &self,
                new_jobs: Vec<$crate::job::NewJob>,
            ) -> $crate::error::Result<Vec<$crate::job::Job>> {
                self.enqueue_batch(new_jobs)
            }
            fn enqueue_unique(
                &self,
                new_job: $crate::job::NewJob,
            ) -> $crate::error::Result<$crate::job::Job> {
                self.enqueue_unique(new_job)
            }
            fn enqueue_unique_batch(
                &self,
                new_jobs: Vec<$crate::job::NewJob>,
            ) -> $crate::error::Result<Vec<$crate::job::Job>> {
                self.enqueue_unique_batch(new_jobs)
            }
            fn enqueue_debounced(
                &self,
                new_job: $crate::job::NewJob,
                options: $crate::storage::records::DebounceOptions,
            ) -> $crate::error::Result<$crate::job::Job> {
                self.enqueue_debounced(new_job, options)
            }
            fn dequeue(
                &self,
                queue_name: &str,
                now: i64,
                namespace: Option<&str>,
            ) -> $crate::error::Result<Option<$crate::job::Job>> {
                self.dequeue(queue_name, now, namespace)
            }
            fn dequeue_from(
                &self,
                queues: &[String],
                now: i64,
                namespace: Option<&str>,
                orders: &std::collections::HashMap<String, $crate::storage::DispatchOrder>,
            ) -> $crate::error::Result<Option<$crate::job::Job>> {
                self.dequeue_from(queues, now, namespace, orders)
            }
            fn dequeue_batch(
                &self,
                queue_name: &str,
                now: i64,
                namespace: Option<&str>,
                max: usize,
            ) -> $crate::error::Result<Vec<$crate::job::Job>> {
                self.dequeue_batch(queue_name, now, namespace, max)
            }
            fn dequeue_batch_from(
                &self,
                queues: &[String],
                now: i64,
                namespace: Option<&str>,
                max: usize,
                orders: &std::collections::HashMap<String, $crate::storage::DispatchOrder>,
            ) -> $crate::error::Result<Vec<$crate::job::Job>> {
                self.dequeue_batch_from(queues, now, namespace, max, orders)
            }
            fn complete(
                &self,
                id: &str,
                result_bytes: Option<Vec<u8>>,
                namespace: Option<&str>,
            ) -> $crate::error::Result<()> {
                self.complete(id, result_bytes, namespace)
            }
            fn complete_batch(
                &self,
                completions: &[$crate::job::JobCompletion],
                namespace: Option<&str>,
            ) -> $crate::error::Result<()> {
                self.complete_batch(completions, namespace)
            }
            fn fail(&self, id: &str, error: &str) -> $crate::error::Result<()> {
                self.fail(id, error)
            }
            fn retry(
                &self,
                id: &str,
                next_scheduled_at: i64,
                namespace: Option<&str>,
            ) -> $crate::error::Result<()> {
                self.retry(id, next_scheduled_at, namespace)
            }
            fn reschedule(&self, id: &str, next_scheduled_at: i64) -> $crate::error::Result<()> {
                self.reschedule(id, next_scheduled_at)
            }
            fn requeue_stuck(&self, id: &str, now: i64) -> $crate::error::Result<bool> {
                self.requeue_stuck(id, now)
            }
            fn cancel_job(
                &self,
                id: &str,
                namespace: Option<&str>,
            ) -> $crate::error::Result<bool> {
                self.cancel_job(id, namespace)
            }
            fn request_cancel(
                &self,
                id: &str,
                namespace: Option<&str>,
            ) -> $crate::error::Result<bool> {
                self.request_cancel(id, namespace)
            }
            fn is_cancel_requested(
                &self,
                id: &str,
                namespace: Option<&str>,
            ) -> $crate::error::Result<bool> {
                self.is_cancel_requested(id, namespace)
            }
            fn mark_cancelled(
                &self,
                id: &str,
                namespace: Option<&str>,
            ) -> $crate::error::Result<()> {
                self.mark_cancelled(id, namespace)
            }
            fn cascade_cancel(
                &self,
                failed_job_id: &str,
                reason: &str,
                namespace: Option<&str>,
            ) -> $crate::error::Result<()> {
                self.cascade_cancel(failed_job_id, reason, namespace)
            }
            fn get_dependencies(
                &self,
                job_id: &str,
                namespace: Option<&str>,
            ) -> $crate::error::Result<Vec<String>> {
                self.get_dependencies(job_id, namespace)
            }
            fn get_dependents(
                &self,
                job_id: &str,
                namespace: Option<&str>,
            ) -> $crate::error::Result<Vec<String>> {
                self.get_dependents(job_id, namespace)
            }
            fn update_progress(
                &self,
                id: &str,
                progress: i32,
                namespace: Option<&str>,
            ) -> $crate::error::Result<()> {
                self.update_progress(id, progress, namespace)
            }
            fn list_jobs(
                &self,
                status: Option<i32>,
                queue_name: Option<&str>,
                task_name: Option<&str>,
                limit: i64,
                offset: i64,
                namespace: Option<&str>,
            ) -> $crate::error::Result<Vec<$crate::job::Job>> {
                self.list_jobs(status, queue_name, task_name, limit, offset, namespace)
            }
            fn list_jobs_after(
                &self,
                status: Option<i32>,
                queue_name: Option<&str>,
                task_name: Option<&str>,
                limit: i64,
                after: Option<(i64, &str)>,
                namespace: Option<&str>,
            ) -> $crate::error::Result<Vec<$crate::job::Job>> {
                self.list_jobs_after(status, queue_name, task_name, limit, after, namespace)
            }
            fn get_job(
                &self,
                id: &str,
                namespace: Option<&str>,
            ) -> $crate::error::Result<Option<$crate::job::Job>> {
                self.get_job(id, namespace)
            }
            fn stats(
                &self,
                namespace: Option<&str>,
            ) -> $crate::error::Result<$crate::storage::QueueStats> {
                self.stats(namespace)
            }
            fn count_expired_rows(
                &self,
                cutoffs: &$crate::storage::RetentionCutoffs,
                now: i64,
            ) -> $crate::error::Result<$crate::storage::RetentionCounts> {
                self.count_expired_rows(cutoffs, now)
            }
            fn purge_completed(&self, older_than_ms: i64) -> $crate::error::Result<u64> {
                self.purge_completed(older_than_ms)
            }
            fn purge_completed_with_ttl(
                &self,
                global_cutoff_ms: Option<i64>,
            ) -> $crate::error::Result<u64> {
                self.purge_completed_with_ttl(global_cutoff_ms)
            }
            fn reap_stale_jobs(
                &self,
                now: i64,
                namespace: Option<&str>,
            ) -> $crate::error::Result<Vec<$crate::job::Job>> {
                self.reap_stale_jobs(now, namespace)
            }
            fn reap_orphaned_jobs(
                &self,
                live_owner_ids: &[String],
                now: i64,
                namespace: Option<&str>,
            ) -> $crate::error::Result<Vec<($crate::job::Job, String)>> {
                self.reap_orphaned_jobs(live_owner_ids, now, namespace)
            }
            fn record_error(
                &self,
                job_id: &str,
                attempt: i32,
                error: &str,
                namespace: Option<&str>,
            ) -> $crate::error::Result<()> {
                self.record_error(job_id, attempt, error, namespace)
            }
            fn get_job_errors(
                &self,
                job_id: &str,
                namespace: Option<&str>,
            ) -> $crate::error::Result<Vec<$crate::storage::records::JobError>> {
                self.get_job_errors(job_id, namespace)
            }
            fn purge_job_errors(&self, older_than_ms: i64) -> $crate::error::Result<u64> {
                self.purge_job_errors(older_than_ms)
            }
            fn move_to_dlq(
                &self,
                job: &$crate::job::Job,
                error: &str,
                metadata: Option<&str>,
            ) -> $crate::error::Result<()> {
                self.move_to_dlq(job, error, metadata)
            }
            fn shed_to_dlq(
                &self,
                job: &$crate::job::Job,
                error: &str,
                metadata: Option<&str>,
            ) -> $crate::error::Result<()> {
                self.shed_to_dlq(job, error, metadata)
            }
            fn list_dead(
                &self,
                limit: i64,
                offset: i64,
                namespace: Option<&str>,
            ) -> $crate::error::Result<Vec<$crate::storage::DeadJob>> {
                self.list_dead(limit, offset, namespace)
            }
            fn list_dead_after(
                &self,
                limit: i64,
                after: Option<(i64, &str)>,
                namespace: Option<&str>,
            ) -> $crate::error::Result<Vec<$crate::storage::DeadJob>> {
                self.list_dead_after(limit, after, namespace)
            }
            fn list_dead_by_task(
                &self,
                task_name: &str,
                limit: i64,
                offset: i64,
                namespace: Option<&str>,
            ) -> $crate::error::Result<Vec<$crate::storage::DeadJob>> {
                self.list_dead_by_task(task_name, limit, offset, namespace)
            }
            fn purge_dead_by_task(&self, task_name: &str) -> $crate::error::Result<u64> {
                self.purge_dead_by_task(task_name)
            }
            fn retry_dead(
                &self,
                dead_id: &str,
                namespace: Option<&str>,
            ) -> $crate::error::Result<String> {
                self.retry_dead(dead_id, namespace)
            }
            fn purge_dead(&self, older_than_ms: i64) -> $crate::error::Result<u64> {
                self.purge_dead(older_than_ms)
            }
            fn delete_dead(
                &self,
                dead_id: &str,
                namespace: Option<&str>,
            ) -> $crate::error::Result<bool> {
                self.delete_dead(dead_id, namespace)
            }
            fn purge_dead_with_ttl(
                &self,
                global_cutoff_ms: Option<i64>,
            ) -> $crate::error::Result<u64> {
                self.purge_dead_with_ttl(global_cutoff_ms)
            }
            fn list_dead_for_retry(
                &self,
                cutoff_ms: i64,
                max_retries: i32,
                namespace: Option<&str>,
                queues: &[String],
                limit: i64,
            ) -> $crate::error::Result<Vec<$crate::storage::DeadJob>> {
                self.list_dead_for_retry(cutoff_ms, max_retries, namespace, queues, limit)
            }
            fn get_rate_limit(
                &self,
                key: &str,
            ) -> $crate::error::Result<Option<$crate::storage::records::RateLimitState>> {
                self.get_rate_limit(key)
            }
            fn upsert_rate_limit(
                &self,
                row: &$crate::storage::records::RateLimitState,
            ) -> $crate::error::Result<()> {
                self.upsert_rate_limit(row)
            }
            fn try_acquire_token(
                &self,
                key: &str,
                max_tokens: f64,
                refill_rate: f64,
            ) -> $crate::error::Result<bool> {
                self.try_acquire_token(key, max_tokens, refill_rate)
            }
            fn register_periodic(
                &self,
                task: &$crate::storage::records::NewPeriodicTask,
            ) -> $crate::error::Result<()> {
                self.register_periodic(task)
            }
            fn get_due_periodic(
                &self,
                now: i64,
            ) -> $crate::error::Result<Vec<$crate::storage::records::PeriodicTask>> {
                self.get_due_periodic(now)
            }
            fn update_periodic_schedule(
                &self,
                name: &str,
                last_run: i64,
                next_run: i64,
            ) -> $crate::error::Result<()> {
                self.update_periodic_schedule(name, last_run, next_run)
            }
            fn list_periodic(
                &self,
            ) -> $crate::error::Result<Vec<$crate::storage::records::PeriodicTask>> {
                self.list_periodic()
            }
            fn delete_periodic(&self, name: &str) -> $crate::error::Result<bool> {
                self.delete_periodic(name)
            }
            fn set_periodic_enabled(
                &self,
                name: &str,
                enabled: bool,
            ) -> $crate::error::Result<bool> {
                self.set_periodic_enabled(name, enabled)
            }
            fn register_subscription(
                &self,
                sub: &$crate::storage::records::NewSubscription,
            ) -> $crate::error::Result<()> {
                self.register_subscription(sub)
            }
            fn list_subscriptions_for_topic(
                &self,
                topic: &str,
            ) -> $crate::error::Result<Vec<$crate::storage::records::Subscription>> {
                self.list_subscriptions_for_topic(topic)
            }
            fn list_subscriptions(
                &self,
            ) -> $crate::error::Result<Vec<$crate::storage::records::Subscription>> {
                self.list_subscriptions()
            }
            fn unsubscribe(
                &self,
                topic: &str,
                subscription_name: &str,
            ) -> $crate::error::Result<bool> {
                self.unsubscribe(topic, subscription_name)
            }
            fn set_subscription_active(
                &self,
                topic: &str,
                subscription_name: &str,
                active: bool,
            ) -> $crate::error::Result<bool> {
                self.set_subscription_active(topic, subscription_name, active)
            }
            fn reap_ephemeral_subscriptions(
                &self,
                live_worker_ids: &[String],
            ) -> $crate::error::Result<u64> {
                self.reap_ephemeral_subscriptions(live_worker_ids)
            }
            fn topic_backlog_stats(
                &self,
            ) -> $crate::error::Result<Vec<$crate::storage::SubscriptionBacklogStats>> {
                self.topic_backlog_stats()
            }
            fn publish_message(
                &self,
                topic: &str,
                payload: &[u8],
                metadata: Option<&str>,
                notes: Option<&str>,
                expires_at: Option<i64>,
            ) -> $crate::error::Result<$crate::storage::records::TopicMessage> {
                self.publish_message(topic, payload, metadata, notes, expires_at)
            }
            fn read_topic_messages(
                &self,
                topic: &str,
                subscription_name: &str,
                limit: i64,
            ) -> $crate::error::Result<Vec<$crate::storage::records::TopicMessage>> {
                self.read_topic_messages(topic, subscription_name, limit)
            }
            fn ack_topic_cursor(
                &self,
                topic: &str,
                subscription_name: &str,
                cursor: &str,
            ) -> $crate::error::Result<bool> {
                self.ack_topic_cursor(topic, subscription_name, cursor)
            }
            fn topic_log_stats(
                &self,
            ) -> $crate::error::Result<Vec<$crate::storage::records::TopicLogStats>> {
                self.topic_log_stats()
            }
            fn purge_topic_messages(&self, now: i64, limit: i64) -> $crate::error::Result<u64> {
                self.purge_topic_messages(now, limit)
            }
            fn declare_topic(
                &self,
                name: &str,
                mode: $crate::storage::records::SubscriptionMode,
                retention_ms: Option<i64>,
            ) -> $crate::error::Result<()> {
                self.declare_topic(name, mode, retention_ms)
            }
            fn get_topic(
                &self,
                name: &str,
            ) -> $crate::error::Result<Option<$crate::storage::records::Topic>> {
                self.get_topic(name)
            }
            fn list_declared_topics(
                &self,
            ) -> $crate::error::Result<Vec<$crate::storage::records::Topic>> {
                self.list_declared_topics()
            }
            fn lease_topic_messages(
                &self,
                topic: &str,
                subscription_name: &str,
                limit: i64,
                visibility_ms: i64,
                now: i64,
            ) -> $crate::error::Result<Vec<$crate::storage::records::TopicMessage>> {
                self.lease_topic_messages(topic, subscription_name, limit, visibility_ms, now)
            }
            fn ack_message(
                &self,
                topic: &str,
                subscription_name: &str,
                message_id: &str,
            ) -> $crate::error::Result<bool> {
                self.ack_message(topic, subscription_name, message_id)
            }
            fn nack_message(
                &self,
                topic: &str,
                subscription_name: &str,
                message_id: &str,
            ) -> $crate::error::Result<bool> {
                self.nack_message(topic, subscription_name, message_id)
            }
            fn record_metric(
                &self,
                task_name: &str,
                job_id: &str,
                wall_time_ns: i64,
                memory_bytes: i64,
                succeeded: bool,
                namespace: Option<&str>,
            ) -> $crate::error::Result<()> {
                self.record_metric(
                    task_name,
                    job_id,
                    wall_time_ns,
                    memory_bytes,
                    succeeded,
                    namespace,
                )
            }
            fn get_metrics(
                &self,
                name: Option<&str>,
                since_ms: i64,
                namespace: Option<&str>,
            ) -> $crate::error::Result<Vec<$crate::storage::records::TaskMetric>> {
                self.get_metrics(name, since_ms, namespace)
            }
            fn purge_metrics(&self, older_than_ms: i64) -> $crate::error::Result<u64> {
                self.purge_metrics(older_than_ms)
            }
            fn record_replay(
                &self,
                original_job_id: &str,
                replay_job_id: &str,
                original_result: Option<&[u8]>,
                replay_result: Option<&[u8]>,
                original_error: Option<&str>,
                replay_error: Option<&str>,
            ) -> $crate::error::Result<()> {
                self.record_replay(
                    original_job_id,
                    replay_job_id,
                    original_result,
                    replay_result,
                    original_error,
                    replay_error,
                )
            }
            fn get_replay_history(
                &self,
                original_job_id: &str,
            ) -> $crate::error::Result<Vec<$crate::storage::records::ReplayEntry>> {
                self.get_replay_history(original_job_id)
            }
            fn write_task_log(
                &self,
                job_id: &str,
                task_name: &str,
                level: &str,
                message: &str,
                extra: Option<&str>,
                namespace: Option<&str>,
            ) -> $crate::error::Result<()> {
                self.write_task_log(job_id, task_name, level, message, extra, namespace)
            }
            fn get_task_logs(
                &self,
                job_id: &str,
                namespace: Option<&str>,
            ) -> $crate::error::Result<Vec<$crate::storage::records::TaskLogEntry>> {
                self.get_task_logs(job_id, namespace)
            }
            fn get_task_logs_after(
                &self,
                job_id: &str,
                after_id: Option<&str>,
                namespace: Option<&str>,
            ) -> $crate::error::Result<Vec<$crate::storage::records::TaskLogEntry>> {
                self.get_task_logs_after(job_id, after_id, namespace)
            }
            fn query_task_logs(
                &self,
                task_name: Option<&str>,
                level: Option<&str>,
                since_ms: i64,
                limit: i64,
                namespace: Option<&str>,
            ) -> $crate::error::Result<Vec<$crate::storage::records::TaskLogEntry>> {
                self.query_task_logs(task_name, level, since_ms, limit, namespace)
            }
            fn purge_task_logs(&self, older_than_ms: i64) -> $crate::error::Result<u64> {
                self.purge_task_logs(older_than_ms)
            }
            fn get_circuit_breaker(
                &self,
                task_name: &str,
            ) -> $crate::error::Result<Option<$crate::storage::records::CircuitBreakerState>> {
                self.get_circuit_breaker(task_name)
            }
            fn upsert_circuit_breaker(
                &self,
                row: &$crate::storage::records::CircuitBreakerState,
            ) -> $crate::error::Result<()> {
                self.upsert_circuit_breaker(row)
            }
            fn list_circuit_breakers(
                &self,
            ) -> $crate::error::Result<Vec<$crate::storage::records::CircuitBreakerState>> {
                self.list_circuit_breakers()
            }
            fn register_worker(
                &self,
                registration: &$crate::storage::records::WorkerRegistration<'_>,
            ) -> $crate::error::Result<()> {
                self.register_worker(registration)
            }
            fn heartbeat(
                &self,
                worker_id: &str,
                resource_health: Option<&str>,
            ) -> $crate::error::Result<()> {
                self.heartbeat(worker_id, resource_health)
            }
            fn update_worker_status(
                &self,
                worker_id: &str,
                status: $crate::storage::records::WorkerStatus,
            ) -> $crate::error::Result<()> {
                self.update_worker_status(worker_id, status)
            }
            fn list_workers(
                &self,
            ) -> $crate::error::Result<Vec<$crate::storage::records::WorkerInfo>> {
                self.list_workers()
            }
            fn list_live_worker_ids(
                &self,
                cutoff_ms: i64,
            ) -> $crate::error::Result<Vec<String>> {
                self.list_live_worker_ids(cutoff_ms)
            }
            fn reap_dead_workers(&self) -> $crate::error::Result<Vec<String>> {
                self.reap_dead_workers()
            }
            fn unregister_worker(&self, worker_id: &str) -> $crate::error::Result<()> {
                self.unregister_worker(worker_id)
            }
            fn list_claims_by_worker(&self, worker_id: &str) -> $crate::error::Result<Vec<String>> {
                self.list_claims_by_worker(worker_id)
            }
            fn pause_queue(&self, queue_name: &str) -> $crate::error::Result<()> {
                self.pause_queue(queue_name)
            }
            fn resume_queue(&self, queue_name: &str) -> $crate::error::Result<()> {
                self.resume_queue(queue_name)
            }
            fn list_paused_queues(&self) -> $crate::error::Result<Vec<String>> {
                self.list_paused_queues()
            }
            fn expire_pending_jobs(&self, now: i64) -> $crate::error::Result<u64> {
                self.expire_pending_jobs(now)
            }
            fn cancel_pending_by_queue(&self, queue: &str) -> $crate::error::Result<u64> {
                self.cancel_pending_by_queue(queue)
            }
            fn cancel_pending_by_task(&self, task_name: &str) -> $crate::error::Result<u64> {
                self.cancel_pending_by_task(task_name)
            }
            fn archive_old_jobs(&self, cutoff_ms: i64) -> $crate::error::Result<u64> {
                self.archive_old_jobs(cutoff_ms)
            }
            fn list_archived(
                &self,
                limit: i64,
                offset: i64,
                namespace: Option<&str>,
            ) -> $crate::error::Result<Vec<$crate::job::Job>> {
                self.list_archived(limit, offset, namespace)
            }
            fn list_archived_after(
                &self,
                limit: i64,
                after: Option<(i64, &str)>,
                namespace: Option<&str>,
            ) -> $crate::error::Result<Vec<$crate::job::Job>> {
                self.list_archived_after(limit, after, namespace)
            }
            fn acquire_lock(
                &self,
                lock_name: &str,
                owner_id: &str,
                ttl_ms: i64,
            ) -> $crate::error::Result<bool> {
                self.acquire_lock(lock_name, owner_id, ttl_ms)
            }
            fn release_lock(
                &self,
                lock_name: &str,
                owner_id: &str,
            ) -> $crate::error::Result<bool> {
                self.release_lock(lock_name, owner_id)
            }
            fn extend_lock(
                &self,
                lock_name: &str,
                owner_id: &str,
                ttl_ms: i64,
            ) -> $crate::error::Result<bool> {
                self.extend_lock(lock_name, owner_id, ttl_ms)
            }
            fn get_lock_info(
                &self,
                lock_name: &str,
            ) -> $crate::error::Result<Option<$crate::storage::records::LockInfo>> {
                self.get_lock_info(lock_name)
            }
            fn reap_expired_locks(&self, now: i64) -> $crate::error::Result<u64> {
                self.reap_expired_locks(now)
            }
            fn claim_execution(
                &self,
                job_id: &str,
                worker_id: &str,
            ) -> $crate::error::Result<bool> {
                self.claim_execution(job_id, worker_id)
            }
            fn claim_execution_batch(
                &self,
                job_ids: &[&str],
                worker_id: &str,
            ) -> $crate::error::Result<Vec<bool>> {
                self.claim_execution_batch(job_ids, worker_id)
            }
            fn complete_execution(
                &self,
                job_id: &str,
                namespace: Option<&str>,
            ) -> $crate::error::Result<()> {
                self.complete_execution(job_id, namespace)
            }
            fn purge_execution_claims(
                &self,
                older_than_ms: i64,
            ) -> $crate::error::Result<u64> {
                self.purge_execution_claims(older_than_ms)
            }
            fn reclaim_execution(
                &self,
                job_id: &str,
                expected_owner: &str,
                new_owner: &str,
            ) -> $crate::error::Result<bool> {
                self.reclaim_execution(job_id, expected_owner, new_owner)
            }
            fn count_running_by_task(
                &self,
                task_name: &str,
                namespace: Option<&str>,
            ) -> $crate::error::Result<i64> {
                self.count_running_by_task(task_name, namespace)
            }
            fn count_pending_by_queue(
                &self,
                queue_name: &str,
            ) -> $crate::error::Result<i64> {
                self.count_pending_by_queue(queue_name)
            }
            fn stats_by_queue(
                &self,
                queue_name: &str,
                namespace: Option<&str>,
            ) -> $crate::error::Result<$crate::storage::QueueStats> {
                self.stats_by_queue(queue_name, namespace)
            }
            fn stats_all_queues(
                &self,
                namespace: Option<&str>,
            ) -> $crate::error::Result<std::collections::HashMap<String, $crate::storage::QueueStats>>
            {
                self.stats_all_queues(namespace)
            }
            fn list_jobs_filtered(
                &self,
                status: Option<i32>,
                queue_name: Option<&str>,
                task_name: Option<&str>,
                metadata_like: Option<&str>,
                error_like: Option<&str>,
                created_after: Option<i64>,
                created_before: Option<i64>,
                limit: i64,
                offset: i64,
                namespace: Option<&str>,
            ) -> $crate::error::Result<Vec<$crate::job::Job>> {
                self.list_jobs_filtered(
                    status,
                    queue_name,
                    task_name,
                    metadata_like,
                    error_like,
                    created_after,
                    created_before,
                    limit,
                    offset,
                    namespace,
                )
            }
            fn list_jobs_filtered_after(
                &self,
                status: Option<i32>,
                queue_name: Option<&str>,
                task_name: Option<&str>,
                metadata_like: Option<&str>,
                error_like: Option<&str>,
                created_after: Option<i64>,
                created_before: Option<i64>,
                limit: i64,
                after: Option<(i64, &str)>,
                namespace: Option<&str>,
            ) -> $crate::error::Result<Vec<$crate::job::Job>> {
                self.list_jobs_filtered_after(
                    status,
                    queue_name,
                    task_name,
                    metadata_like,
                    error_like,
                    created_after,
                    created_before,
                    limit,
                    after,
                    namespace,
                )
            }
            fn get_setting(&self, key: &str) -> $crate::error::Result<Option<String>> {
                self.get_setting(key)
            }
            fn set_setting(&self, key: &str, value: &str) -> $crate::error::Result<()> {
                self.set_setting(key, value)
            }
            fn set_setting_if(
                &self,
                key: &str,
                expected: Option<&str>,
                value: &str,
            ) -> $crate::error::Result<bool> {
                self.set_setting_if(key, expected, value)
            }
            fn delete_setting(&self, key: &str) -> $crate::error::Result<bool> {
                self.delete_setting(key)
            }
            fn list_settings(
                &self,
            ) -> $crate::error::Result<std::collections::HashMap<String, String>> {
                self.list_settings()
            }
        }
    };
}

pub(crate) use impl_storage;

// ── Backend-agnostic storage wrapper ──────────────────────────────────

/// Storage backend enum that dispatches to either SQLite or PostgreSQL.
#[derive(Clone)]
pub enum StorageBackend {
    /// SQLite backend (the default).
    Sqlite(sqlite::SqliteStorage),
    /// PostgreSQL backend (feature `postgres`).
    #[cfg(feature = "postgres")]
    Postgres(postgres::PostgresStorage),
    /// Redis backend (feature `redis`).
    #[cfg(feature = "redis")]
    Redis(redis_backend::RedisStorage),
}

macro_rules! delegate {
    ($self:expr, $method:ident $(, $arg:expr)*) => {
        match $self {
            StorageBackend::Sqlite(s) => s.$method($($arg),*),
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.$method($($arg),*),
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => s.$method($($arg),*),
        }
    };
}

impl StorageBackend {
    /// Apply any pending schema changes to this backend, plus the one-time
    /// backlog sweep. Idempotent, and a successful no-op on a schemaless
    /// backend — see [`migrate::MigrationReport`].
    pub fn migrate(&self) -> crate::error::Result<migrate::MigrationReport> {
        let report = match self {
            StorageBackend::Sqlite(s) => s.migrate(),
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.migrate(),
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => s.migrate(),
        }?;
        // A gated open cannot read the contract floor — the settings table may
        // not exist yet — so the check is deferred to here, where it does. It
        // lives on this one path rather than in each shell so no shell can skip
        // it. Deliberately after the DDL: an older build only ever applies the
        // migrations it was compiled with, all of which predate the deployment
        // that outranks it.
        crate::contract::ensure_contract_supported(self)?;
        Ok(report)
    }

    /// Whether the core schema has been applied. Lets a caller tell an empty
    /// database apart from one opened without migrating, without applying
    /// anything itself.
    pub fn is_migrated(&self) -> crate::error::Result<bool> {
        match self {
            StorageBackend::Sqlite(s) => s.is_migrated(),
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.is_migrated(),
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => s.is_migrated(),
        }
    }

    /// Signal the scheduler that a ready job was enqueued, so it can dispatch
    /// immediately instead of waiting for the next poll. No-op for delayed
    /// jobs (`scheduled_at > now`) — those rely on the fallback timer — and a
    /// no-op entirely when `push-dispatch` is off.
    #[cfg(feature = "push-dispatch")]
    fn notify_if_ready(&self, scheduled_at: i64) {
        use crate::storage::notify::StorageNotifier;
        if scheduled_at > crate::job::now_millis() {
            return;
        }
        match self {
            StorageBackend::Sqlite(s) => s.notify_job_ready("", scheduled_at),
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => s.notify_job_ready("", scheduled_at),
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => s.notify_job_ready("", scheduled_at),
        }
    }
}

impl Storage for StorageBackend {
    fn enqueue(&self, new_job: NewJob) -> Result<Job> {
        let job = delegate!(self, enqueue, new_job)?;
        #[cfg(feature = "push-dispatch")]
        self.notify_if_ready(job.scheduled_at);
        Ok(job)
    }
    fn enqueue_batch(&self, new_jobs: Vec<NewJob>) -> Result<Vec<Job>> {
        let jobs = delegate!(self, enqueue_batch, new_jobs)?;
        #[cfg(feature = "push-dispatch")]
        if jobs
            .iter()
            .any(|j| j.scheduled_at <= crate::job::now_millis())
        {
            self.notify_if_ready(0);
        }
        Ok(jobs)
    }
    fn enqueue_unique(&self, new_job: NewJob) -> Result<Job> {
        let job = delegate!(self, enqueue_unique, new_job)?;
        #[cfg(feature = "push-dispatch")]
        self.notify_if_ready(job.scheduled_at);
        Ok(job)
    }
    fn enqueue_debounced(&self, new_job: NewJob, options: records::DebounceOptions) -> Result<Job> {
        let job = delegate!(self, enqueue_debounced, new_job, options)?;
        #[cfg(feature = "push-dispatch")]
        self.notify_if_ready(job.scheduled_at);
        Ok(job)
    }
    fn enqueue_unique_batch(&self, new_jobs: Vec<NewJob>) -> Result<Vec<Job>> {
        let jobs = delegate!(self, enqueue_unique_batch, new_jobs)?;
        #[cfg(feature = "push-dispatch")]
        if jobs
            .iter()
            .any(|j| j.scheduled_at <= crate::job::now_millis())
        {
            self.notify_if_ready(0);
        }
        Ok(jobs)
    }
    fn dequeue(&self, queue_name: &str, now: i64, namespace: Option<&str>) -> Result<Option<Job>> {
        delegate!(self, dequeue, queue_name, now, namespace)
    }
    fn dequeue_from(
        &self,
        queues: &[String],
        now: i64,
        namespace: Option<&str>,
        orders: &std::collections::HashMap<String, DispatchOrder>,
    ) -> Result<Option<Job>> {
        delegate!(self, dequeue_from, queues, now, namespace, orders)
    }
    fn dequeue_batch(
        &self,
        queue_name: &str,
        now: i64,
        namespace: Option<&str>,
        max: usize,
    ) -> Result<Vec<Job>> {
        delegate!(self, dequeue_batch, queue_name, now, namespace, max)
    }
    fn dequeue_batch_from(
        &self,
        queues: &[String],
        now: i64,
        namespace: Option<&str>,
        max: usize,
        orders: &std::collections::HashMap<String, DispatchOrder>,
    ) -> Result<Vec<Job>> {
        delegate!(
            self,
            dequeue_batch_from,
            queues,
            now,
            namespace,
            max,
            orders
        )
    }
    fn complete(
        &self,
        id: &str,
        result_bytes: Option<Vec<u8>>,
        namespace: Option<&str>,
    ) -> Result<()> {
        delegate!(self, complete, id, result_bytes, namespace)
    }
    fn complete_batch(
        &self,
        completions: &[crate::job::JobCompletion],
        namespace: Option<&str>,
    ) -> Result<()> {
        delegate!(self, complete_batch, completions, namespace)
    }
    fn fail(&self, id: &str, error: &str) -> Result<()> {
        delegate!(self, fail, id, error)
    }
    fn retry(&self, id: &str, next_scheduled_at: i64, namespace: Option<&str>) -> Result<()> {
        delegate!(self, retry, id, next_scheduled_at, namespace)
    }
    fn reschedule(&self, id: &str, next_scheduled_at: i64) -> Result<()> {
        delegate!(self, reschedule, id, next_scheduled_at)
    }
    fn requeue_stuck(&self, id: &str, now: i64) -> Result<bool> {
        delegate!(self, requeue_stuck, id, now)
    }
    fn cancel_job(&self, id: &str, namespace: Option<&str>) -> Result<bool> {
        delegate!(self, cancel_job, id, namespace)
    }
    fn request_cancel(&self, id: &str, namespace: Option<&str>) -> Result<bool> {
        delegate!(self, request_cancel, id, namespace)
    }
    fn is_cancel_requested(&self, id: &str, namespace: Option<&str>) -> Result<bool> {
        delegate!(self, is_cancel_requested, id, namespace)
    }
    fn mark_cancelled(&self, id: &str, namespace: Option<&str>) -> Result<()> {
        delegate!(self, mark_cancelled, id, namespace)
    }
    fn cascade_cancel(
        &self,
        failed_job_id: &str,
        reason: &str,
        namespace: Option<&str>,
    ) -> Result<()> {
        delegate!(self, cascade_cancel, failed_job_id, reason, namespace)
    }
    fn get_dependencies(&self, job_id: &str, namespace: Option<&str>) -> Result<Vec<String>> {
        delegate!(self, get_dependencies, job_id, namespace)
    }
    fn get_dependents(&self, job_id: &str, namespace: Option<&str>) -> Result<Vec<String>> {
        delegate!(self, get_dependents, job_id, namespace)
    }
    fn update_progress(&self, id: &str, progress: i32, namespace: Option<&str>) -> Result<()> {
        delegate!(self, update_progress, id, progress, namespace)
    }
    fn list_jobs(
        &self,
        status: Option<i32>,
        queue_name: Option<&str>,
        task_name: Option<&str>,
        limit: i64,
        offset: i64,
        namespace: Option<&str>,
    ) -> Result<Vec<Job>> {
        delegate!(self, list_jobs, status, queue_name, task_name, limit, offset, namespace)
    }
    fn list_jobs_after(
        &self,
        status: Option<i32>,
        queue_name: Option<&str>,
        task_name: Option<&str>,
        limit: i64,
        after: Option<(i64, &str)>,
        namespace: Option<&str>,
    ) -> Result<Vec<Job>> {
        delegate!(
            self,
            list_jobs_after,
            status,
            queue_name,
            task_name,
            limit,
            after,
            namespace
        )
    }
    fn get_job(&self, id: &str, namespace: Option<&str>) -> Result<Option<Job>> {
        delegate!(self, get_job, id, namespace)
    }
    fn stats(&self, namespace: Option<&str>) -> Result<QueueStats> {
        delegate!(self, stats, namespace)
    }
    fn count_expired_rows(&self, cutoffs: &RetentionCutoffs, now: i64) -> Result<RetentionCounts> {
        delegate!(self, count_expired_rows, cutoffs, now)
    }
    fn purge_completed(&self, older_than_ms: i64) -> Result<u64> {
        delegate!(self, purge_completed, older_than_ms)
    }
    fn purge_completed_with_ttl(&self, global_cutoff_ms: Option<i64>) -> Result<u64> {
        delegate!(self, purge_completed_with_ttl, global_cutoff_ms)
    }
    fn reap_stale_jobs(&self, now: i64, namespace: Option<&str>) -> Result<Vec<Job>> {
        delegate!(self, reap_stale_jobs, now, namespace)
    }
    fn reap_orphaned_jobs(
        &self,
        live_owner_ids: &[String],
        now: i64,
        namespace: Option<&str>,
    ) -> Result<Vec<(Job, String)>> {
        delegate!(self, reap_orphaned_jobs, live_owner_ids, now, namespace)
    }
    fn record_error(
        &self,
        job_id: &str,
        attempt: i32,
        error: &str,
        namespace: Option<&str>,
    ) -> Result<()> {
        delegate!(self, record_error, job_id, attempt, error, namespace)
    }
    fn get_job_errors(
        &self,
        job_id: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<records::JobError>> {
        delegate!(self, get_job_errors, job_id, namespace)
    }
    fn purge_job_errors(&self, older_than_ms: i64) -> Result<u64> {
        delegate!(self, purge_job_errors, older_than_ms)
    }
    fn move_to_dlq(&self, job: &Job, error: &str, metadata: Option<&str>) -> Result<()> {
        delegate!(self, move_to_dlq, job, error, metadata)
    }
    fn shed_to_dlq(&self, job: &Job, error: &str, metadata: Option<&str>) -> Result<()> {
        delegate!(self, shed_to_dlq, job, error, metadata)
    }
    fn list_dead(&self, limit: i64, offset: i64, namespace: Option<&str>) -> Result<Vec<DeadJob>> {
        delegate!(self, list_dead, limit, offset, namespace)
    }
    fn list_dead_after(
        &self,
        limit: i64,
        after: Option<(i64, &str)>,
        namespace: Option<&str>,
    ) -> Result<Vec<DeadJob>> {
        delegate!(self, list_dead_after, limit, after, namespace)
    }
    fn list_dead_by_task(
        &self,
        task_name: &str,
        limit: i64,
        offset: i64,
        namespace: Option<&str>,
    ) -> Result<Vec<DeadJob>> {
        delegate!(self, list_dead_by_task, task_name, limit, offset, namespace)
    }
    fn purge_dead_by_task(&self, task_name: &str) -> Result<u64> {
        delegate!(self, purge_dead_by_task, task_name)
    }
    fn retry_dead(&self, dead_id: &str, namespace: Option<&str>) -> Result<String> {
        delegate!(self, retry_dead, dead_id, namespace)
    }
    fn purge_dead(&self, older_than_ms: i64) -> Result<u64> {
        delegate!(self, purge_dead, older_than_ms)
    }
    fn delete_dead(&self, dead_id: &str, namespace: Option<&str>) -> Result<bool> {
        delegate!(self, delete_dead, dead_id, namespace)
    }
    fn purge_dead_with_ttl(&self, global_cutoff_ms: Option<i64>) -> Result<u64> {
        delegate!(self, purge_dead_with_ttl, global_cutoff_ms)
    }
    fn list_dead_for_retry(
        &self,
        cutoff_ms: i64,
        max_retries: i32,
        namespace: Option<&str>,
        queues: &[String],
        limit: i64,
    ) -> Result<Vec<DeadJob>> {
        delegate!(
            self,
            list_dead_for_retry,
            cutoff_ms,
            max_retries,
            namespace,
            queues,
            limit
        )
    }
    fn get_rate_limit(&self, key: &str) -> Result<Option<records::RateLimitState>> {
        delegate!(self, get_rate_limit, key)
    }
    fn upsert_rate_limit(&self, row: &records::RateLimitState) -> Result<()> {
        delegate!(self, upsert_rate_limit, row)
    }
    fn try_acquire_token(&self, key: &str, max_tokens: f64, refill_rate: f64) -> Result<bool> {
        delegate!(self, try_acquire_token, key, max_tokens, refill_rate)
    }
    fn register_periodic(&self, task: &records::NewPeriodicTask) -> Result<()> {
        delegate!(self, register_periodic, task)
    }
    fn get_due_periodic(&self, now: i64) -> Result<Vec<records::PeriodicTask>> {
        delegate!(self, get_due_periodic, now)
    }
    fn update_periodic_schedule(&self, name: &str, last_run: i64, next_run: i64) -> Result<()> {
        delegate!(self, update_periodic_schedule, name, last_run, next_run)
    }
    fn list_periodic(&self) -> Result<Vec<records::PeriodicTask>> {
        delegate!(self, list_periodic)
    }
    fn delete_periodic(&self, name: &str) -> Result<bool> {
        delegate!(self, delete_periodic, name)
    }
    fn set_periodic_enabled(&self, name: &str, enabled: bool) -> Result<bool> {
        delegate!(self, set_periodic_enabled, name, enabled)
    }
    fn register_subscription(&self, sub: &records::NewSubscription) -> Result<()> {
        delegate!(self, register_subscription, sub)
    }
    fn list_subscriptions_for_topic(&self, topic: &str) -> Result<Vec<records::Subscription>> {
        delegate!(self, list_subscriptions_for_topic, topic)
    }
    fn list_subscriptions(&self) -> Result<Vec<records::Subscription>> {
        delegate!(self, list_subscriptions)
    }
    fn unsubscribe(&self, topic: &str, subscription_name: &str) -> Result<bool> {
        delegate!(self, unsubscribe, topic, subscription_name)
    }
    fn set_subscription_active(
        &self,
        topic: &str,
        subscription_name: &str,
        active: bool,
    ) -> Result<bool> {
        delegate!(
            self,
            set_subscription_active,
            topic,
            subscription_name,
            active
        )
    }
    fn reap_ephemeral_subscriptions(&self, live_worker_ids: &[String]) -> Result<u64> {
        delegate!(self, reap_ephemeral_subscriptions, live_worker_ids)
    }
    fn topic_backlog_stats(&self) -> Result<Vec<SubscriptionBacklogStats>> {
        delegate!(self, topic_backlog_stats)
    }
    fn publish_message(
        &self,
        topic: &str,
        payload: &[u8],
        metadata: Option<&str>,
        notes: Option<&str>,
        expires_at: Option<i64>,
    ) -> Result<records::TopicMessage> {
        delegate!(
            self,
            publish_message,
            topic,
            payload,
            metadata,
            notes,
            expires_at
        )
    }
    fn read_topic_messages(
        &self,
        topic: &str,
        subscription_name: &str,
        limit: i64,
    ) -> Result<Vec<records::TopicMessage>> {
        delegate!(self, read_topic_messages, topic, subscription_name, limit)
    }
    fn ack_topic_cursor(&self, topic: &str, subscription_name: &str, cursor: &str) -> Result<bool> {
        delegate!(self, ack_topic_cursor, topic, subscription_name, cursor)
    }
    fn topic_log_stats(&self) -> Result<Vec<records::TopicLogStats>> {
        delegate!(self, topic_log_stats)
    }
    fn purge_topic_messages(&self, now: i64, limit: i64) -> Result<u64> {
        delegate!(self, purge_topic_messages, now, limit)
    }
    fn declare_topic(
        &self,
        name: &str,
        mode: records::SubscriptionMode,
        retention_ms: Option<i64>,
    ) -> Result<()> {
        delegate!(self, declare_topic, name, mode, retention_ms)
    }
    fn get_topic(&self, name: &str) -> Result<Option<records::Topic>> {
        delegate!(self, get_topic, name)
    }
    fn list_declared_topics(&self) -> Result<Vec<records::Topic>> {
        delegate!(self, list_declared_topics)
    }
    fn lease_topic_messages(
        &self,
        topic: &str,
        subscription_name: &str,
        limit: i64,
        visibility_ms: i64,
        now: i64,
    ) -> Result<Vec<records::TopicMessage>> {
        delegate!(
            self,
            lease_topic_messages,
            topic,
            subscription_name,
            limit,
            visibility_ms,
            now
        )
    }
    fn ack_message(&self, topic: &str, subscription_name: &str, message_id: &str) -> Result<bool> {
        delegate!(self, ack_message, topic, subscription_name, message_id)
    }
    fn nack_message(&self, topic: &str, subscription_name: &str, message_id: &str) -> Result<bool> {
        delegate!(self, nack_message, topic, subscription_name, message_id)
    }
    fn record_metric(
        &self,
        task_name: &str,
        job_id: &str,
        wall_time_ns: i64,
        memory_bytes: i64,
        succeeded: bool,
        namespace: Option<&str>,
    ) -> Result<()> {
        delegate!(
            self,
            record_metric,
            task_name,
            job_id,
            wall_time_ns,
            memory_bytes,
            succeeded,
            namespace
        )
    }
    fn get_metrics(
        &self,
        name: Option<&str>,
        since_ms: i64,
        namespace: Option<&str>,
    ) -> Result<Vec<records::TaskMetric>> {
        delegate!(self, get_metrics, name, since_ms, namespace)
    }
    fn purge_metrics(&self, older_than_ms: i64) -> Result<u64> {
        delegate!(self, purge_metrics, older_than_ms)
    }
    fn record_replay(
        &self,
        original_job_id: &str,
        replay_job_id: &str,
        original_result: Option<&[u8]>,
        replay_result: Option<&[u8]>,
        original_error: Option<&str>,
        replay_error: Option<&str>,
    ) -> Result<()> {
        delegate!(
            self,
            record_replay,
            original_job_id,
            replay_job_id,
            original_result,
            replay_result,
            original_error,
            replay_error
        )
    }
    fn get_replay_history(&self, original_job_id: &str) -> Result<Vec<records::ReplayEntry>> {
        delegate!(self, get_replay_history, original_job_id)
    }
    fn write_task_log(
        &self,
        job_id: &str,
        task_name: &str,
        level: &str,
        message: &str,
        extra: Option<&str>,
        namespace: Option<&str>,
    ) -> Result<()> {
        delegate!(
            self,
            write_task_log,
            job_id,
            task_name,
            level,
            message,
            extra,
            namespace
        )
    }
    fn get_task_logs(
        &self,
        job_id: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<records::TaskLogEntry>> {
        delegate!(self, get_task_logs, job_id, namespace)
    }
    fn get_task_logs_after(
        &self,
        job_id: &str,
        after_id: Option<&str>,
        namespace: Option<&str>,
    ) -> Result<Vec<records::TaskLogEntry>> {
        delegate!(self, get_task_logs_after, job_id, after_id, namespace)
    }
    fn query_task_logs(
        &self,
        task_name: Option<&str>,
        level: Option<&str>,
        since_ms: i64,
        limit: i64,
        namespace: Option<&str>,
    ) -> Result<Vec<records::TaskLogEntry>> {
        delegate!(
            self,
            query_task_logs,
            task_name,
            level,
            since_ms,
            limit,
            namespace
        )
    }
    fn purge_task_logs(&self, older_than_ms: i64) -> Result<u64> {
        delegate!(self, purge_task_logs, older_than_ms)
    }
    fn get_circuit_breaker(&self, task_name: &str) -> Result<Option<records::CircuitBreakerState>> {
        delegate!(self, get_circuit_breaker, task_name)
    }
    fn upsert_circuit_breaker(&self, row: &records::CircuitBreakerState) -> Result<()> {
        delegate!(self, upsert_circuit_breaker, row)
    }
    fn list_circuit_breakers(&self) -> Result<Vec<records::CircuitBreakerState>> {
        delegate!(self, list_circuit_breakers)
    }
    fn register_worker(&self, registration: &records::WorkerRegistration<'_>) -> Result<()> {
        delegate!(self, register_worker, registration)
    }
    fn heartbeat(&self, worker_id: &str, resource_health: Option<&str>) -> Result<()> {
        delegate!(self, heartbeat, worker_id, resource_health)
    }
    fn update_worker_status(&self, worker_id: &str, status: records::WorkerStatus) -> Result<()> {
        delegate!(self, update_worker_status, worker_id, status)
    }
    fn list_workers(&self) -> Result<Vec<records::WorkerInfo>> {
        delegate!(self, list_workers)
    }
    fn list_live_worker_ids(&self, cutoff_ms: i64) -> Result<Vec<String>> {
        delegate!(self, list_live_worker_ids, cutoff_ms)
    }
    fn reap_dead_workers(&self) -> Result<Vec<String>> {
        delegate!(self, reap_dead_workers)
    }
    fn unregister_worker(&self, worker_id: &str) -> Result<()> {
        delegate!(self, unregister_worker, worker_id)
    }
    fn list_claims_by_worker(&self, worker_id: &str) -> Result<Vec<String>> {
        delegate!(self, list_claims_by_worker, worker_id)
    }
    fn pause_queue(&self, queue_name: &str) -> Result<()> {
        delegate!(self, pause_queue, queue_name)
    }
    fn resume_queue(&self, queue_name: &str) -> Result<()> {
        delegate!(self, resume_queue, queue_name)
    }
    fn list_paused_queues(&self) -> Result<Vec<String>> {
        delegate!(self, list_paused_queues)
    }
    fn expire_pending_jobs(&self, now: i64) -> Result<u64> {
        delegate!(self, expire_pending_jobs, now)
    }
    fn cancel_pending_by_queue(&self, queue: &str) -> Result<u64> {
        delegate!(self, cancel_pending_by_queue, queue)
    }
    fn cancel_pending_by_task(&self, task_name: &str) -> Result<u64> {
        delegate!(self, cancel_pending_by_task, task_name)
    }
    fn archive_old_jobs(&self, cutoff_ms: i64) -> Result<u64> {
        delegate!(self, archive_old_jobs, cutoff_ms)
    }
    fn list_archived(&self, limit: i64, offset: i64, namespace: Option<&str>) -> Result<Vec<Job>> {
        delegate!(self, list_archived, limit, offset, namespace)
    }
    fn list_archived_after(
        &self,
        limit: i64,
        after: Option<(i64, &str)>,
        namespace: Option<&str>,
    ) -> Result<Vec<Job>> {
        delegate!(self, list_archived_after, limit, after, namespace)
    }
    fn acquire_lock(&self, lock_name: &str, owner_id: &str, ttl_ms: i64) -> Result<bool> {
        delegate!(self, acquire_lock, lock_name, owner_id, ttl_ms)
    }
    fn release_lock(&self, lock_name: &str, owner_id: &str) -> Result<bool> {
        delegate!(self, release_lock, lock_name, owner_id)
    }
    fn extend_lock(&self, lock_name: &str, owner_id: &str, ttl_ms: i64) -> Result<bool> {
        delegate!(self, extend_lock, lock_name, owner_id, ttl_ms)
    }
    fn get_lock_info(&self, lock_name: &str) -> Result<Option<records::LockInfo>> {
        delegate!(self, get_lock_info, lock_name)
    }
    fn reap_expired_locks(&self, now: i64) -> Result<u64> {
        delegate!(self, reap_expired_locks, now)
    }
    fn claim_execution(&self, job_id: &str, worker_id: &str) -> Result<bool> {
        delegate!(self, claim_execution, job_id, worker_id)
    }
    fn claim_execution_batch(&self, job_ids: &[&str], worker_id: &str) -> Result<Vec<bool>> {
        delegate!(self, claim_execution_batch, job_ids, worker_id)
    }
    fn complete_execution(&self, job_id: &str, namespace: Option<&str>) -> Result<()> {
        delegate!(self, complete_execution, job_id, namespace)
    }
    fn purge_execution_claims(&self, older_than_ms: i64) -> Result<u64> {
        delegate!(self, purge_execution_claims, older_than_ms)
    }
    fn reclaim_execution(
        &self,
        job_id: &str,
        expected_owner: &str,
        new_owner: &str,
    ) -> Result<bool> {
        delegate!(self, reclaim_execution, job_id, expected_owner, new_owner)
    }
    fn supports_steps(&self) -> bool {
        delegate!(self, supports_steps)
    }
    fn get_job_steps(
        &self,
        job_id: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<records::JobStep>> {
        delegate!(self, get_job_steps, job_id, namespace)
    }
    fn record_step_result(
        &self,
        step: &records::NewJobStep<'_>,
        owner: &str,
        attempt: i32,
        limits: &crate::step::StepLimits,
        namespace: Option<&str>,
    ) -> Result<records::StepCommit> {
        delegate!(
            self,
            record_step_result,
            step,
            owner,
            attempt,
            limits,
            namespace
        )
    }
    fn sleep_job(
        &self,
        step: &records::NewJobStep<'_>,
        owner: &str,
        attempt: i32,
        wake_at: i64,
        limits: &crate::step::StepLimits,
        namespace: Option<&str>,
    ) -> Result<records::SleepOutcome> {
        delegate!(self, sleep_job, step, owner, attempt, wake_at, limits, namespace)
    }
    fn delete_job_steps(&self, job_id: &str, namespace: Option<&str>) -> Result<u64> {
        delegate!(self, delete_job_steps, job_id, namespace)
    }
    fn count_running_by_task(&self, task_name: &str, namespace: Option<&str>) -> Result<i64> {
        delegate!(self, count_running_by_task, task_name, namespace)
    }
    fn count_pending_by_queue(&self, queue_name: &str) -> Result<i64> {
        delegate!(self, count_pending_by_queue, queue_name)
    }
    fn stats_by_queue(&self, queue_name: &str, namespace: Option<&str>) -> Result<QueueStats> {
        delegate!(self, stats_by_queue, queue_name, namespace)
    }
    fn stats_all_queues(
        &self,
        namespace: Option<&str>,
    ) -> Result<std::collections::HashMap<String, QueueStats>> {
        delegate!(self, stats_all_queues, namespace)
    }
    fn list_jobs_filtered(
        &self,
        status: Option<i32>,
        queue_name: Option<&str>,
        task_name: Option<&str>,
        metadata_like: Option<&str>,
        error_like: Option<&str>,
        created_after: Option<i64>,
        created_before: Option<i64>,
        limit: i64,
        offset: i64,
        namespace: Option<&str>,
    ) -> Result<Vec<Job>> {
        delegate!(
            self,
            list_jobs_filtered,
            status,
            queue_name,
            task_name,
            metadata_like,
            error_like,
            created_after,
            created_before,
            limit,
            offset,
            namespace
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn list_jobs_filtered_after(
        &self,
        status: Option<i32>,
        queue_name: Option<&str>,
        task_name: Option<&str>,
        metadata_like: Option<&str>,
        error_like: Option<&str>,
        created_after: Option<i64>,
        created_before: Option<i64>,
        limit: i64,
        after: Option<(i64, &str)>,
        namespace: Option<&str>,
    ) -> Result<Vec<Job>> {
        delegate!(
            self,
            list_jobs_filtered_after,
            status,
            queue_name,
            task_name,
            metadata_like,
            error_like,
            created_after,
            created_before,
            limit,
            after,
            namespace
        )
    }
    fn get_setting(&self, key: &str) -> Result<Option<String>> {
        delegate!(self, get_setting, key)
    }
    fn set_setting(&self, key: &str, value: &str) -> Result<()> {
        delegate!(self, set_setting, key, value)
    }
    fn set_setting_if(&self, key: &str, expected: Option<&str>, value: &str) -> Result<bool> {
        delegate!(self, set_setting_if, key, expected, value)
    }
    fn delete_setting(&self, key: &str) -> Result<bool> {
        delegate!(self, delete_setting, key)
    }
    fn list_settings(&self) -> Result<std::collections::HashMap<String, String>> {
        delegate!(self, list_settings)
    }
}

#[cfg(test)]
mod election_tests {
    use super::*;
    use crate::storage::sqlite::SqliteStorage;

    fn store() -> SqliteStorage {
        SqliteStorage::in_memory().unwrap()
    }

    #[test]
    fn try_lead_takes_a_free_lock() {
        let s = store();
        assert!(try_lead(&s, "l", "worker-a", 10_000).unwrap());
    }

    #[test]
    fn try_lead_renews_its_own_live_lock() {
        // The load-bearing test: `acquire_lock` refuses a live lock even for its
        // owner, so an acquire-first `try_lead` would return false on the second
        // call and hand leadership to nobody. Renew-then-acquire keeps it.
        let s = store();
        assert!(try_lead(&s, "l", "worker-a", 10_000).unwrap());
        assert!(
            try_lead(&s, "l", "worker-a", 10_000).unwrap(),
            "the current leader must keep the lock across ticks"
        );
    }

    #[test]
    fn try_lead_refuses_a_lock_held_by_another() {
        let s = store();
        assert!(try_lead(&s, "l", "worker-a", 10_000).unwrap());
        assert!(
            !try_lead(&s, "l", "worker-b", 10_000).unwrap(),
            "a live lock held by another owner is not stealable"
        );
    }

    #[test]
    fn try_lead_takes_an_expired_lock() {
        let s = store();
        assert!(try_lead(&s, "l", "worker-a", 1).unwrap());
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(
            try_lead(&s, "l", "worker-b", 10_000).unwrap(),
            "an expired lock is stealable by a new owner"
        );
    }
}
