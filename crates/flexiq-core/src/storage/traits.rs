use crate::error::{QueueError, Result};
use crate::job::{Job, NewJob};
use crate::step::StepLimits;
use crate::storage::records::{
    AttemptFence, CircuitBreakerState, DebounceOptions, JobError, JobStep, LockInfo, NewJobStep,
    NewPeriodicTask, NewSubscription, PeriodicTask, RateLimitState, ReplayEntry, SleepOutcome,
    StepCommit, Subscription, SubscriptionMode, TaskLogEntry, TaskMetric, Topic, TopicLogStats,
    TopicMessage, WorkerInfo, WorkerRegistration, WorkerStatus,
};
use crate::storage::{
    DeadJob, DispatchOrder, QueueStats, RetentionCounts, RetentionCutoffs, SubscriptionBacklogStats,
};

/// Trait abstracting the storage backend for the task queue.
///
/// Implementations: `SqliteStorage` (default), `PostgresStorage` (feature
/// `postgres`), and `RedisStorage` (feature `redis`). The trait enables
/// alternative backends and simplifies testing with mock storage.
pub trait Storage: Send + Sync + Clone {
    // ── Job operations ──────────────────────────────────────────────

    /// Insert a new job and return it.
    ///
    /// Every id in `depends_on` must name a live-or-completed job in the same
    /// namespace; anything else — missing, dead, cancelled, or belonging to
    /// another namespace — fails with `DependencyNotFound`. A cross-namespace
    /// edge is refused rather than filtered so one tenant's failure can never
    /// cascade into another's queue.
    fn enqueue(&self, new_job: NewJob) -> Result<Job>;
    /// Insert multiple jobs in a single transaction. Dependencies are validated
    /// as in [`enqueue`](Self::enqueue), except that an id may also name
    /// another job in the same batch.
    fn enqueue_batch(&self, new_jobs: Vec<NewJob>) -> Result<Vec<Job>>;
    /// Enqueue with `unique_key` deduplication: returns the existing active
    /// job when a duplicate is found instead of inserting. The match is
    /// scoped to the job's own namespace, like `depends_on` in
    /// [`enqueue`](Self::enqueue).
    fn enqueue_unique(&self, new_job: NewJob) -> Result<Job>;
    /// Batch variant of [`enqueue_unique`](Self::enqueue_unique), one transaction.
    fn enqueue_unique_batch(&self, new_jobs: Vec<NewJob>) -> Result<Vec<Job>>;
    /// [`enqueue_unique`](Self::enqueue_unique), also reporting whether the job
    /// returned was already in the unique slot (`true`) or was inserted by this
    /// call (`false`).
    ///
    /// Nothing above the backend can work this out: the job's id is generated
    /// inside the insert, so a caller has no candidate to compare the answer
    /// against, and a `created_at` comparison is wrong whenever a concurrent
    /// producer wins the slot inside the same millisecond.
    fn enqueue_unique_reporting(&self, new_job: NewJob) -> Result<(Job, bool)>;
    /// Batch variant of
    /// [`enqueue_unique_reporting`](Self::enqueue_unique_reporting), one
    /// transaction, one flag per input job in input order.
    fn enqueue_unique_batch_reporting(&self, new_jobs: Vec<NewJob>) -> Result<Vec<(Job, bool)>>;
    /// Enqueue under a debounce window: while a job carrying the same
    /// `debounce_key` is still pending and unclaimed, slide its `scheduled_at`
    /// forward instead of inserting a second job, so a burst of enqueues
    /// collapses into one run.
    ///
    /// The new deadline is `min(now + window_ms, first_seen + max_wait_ms)`,
    /// where `first_seen` is the pending job's `created_at` — no extra column.
    /// `new_job.scheduled_at` is ignored; the window decides when the job runs.
    ///
    /// The read, the status guard, and the write are one transaction. A job
    /// claimed a microsecond ago must never be pulled back to a later deadline
    /// after a worker already has it, so a claimed row is left alone and the
    /// call inserts a fresh job instead.
    ///
    /// A coalescing call changes only the deadline, plus the payload when
    /// `replace_payload` is set. Everything else on `new_job` — priority,
    /// metadata, notes, `depends_on`, `expires_at` — belongs to the job that
    /// opened the window and is discarded; the call is a vote to run again
    /// soon, not a redefinition of the run.
    ///
    /// Returns the job the enqueue landed on, whether that is the slid pending
    /// one or a newly inserted one; compare its id against the previous call's
    /// to tell coalescing from insertion. Rejects a missing or empty
    /// `debounce_key`, a non-positive window, and a `max_wait_ms` below the
    /// window with [`QueueError::Config`].
    ///
    /// `options.max_pending` is the target queue's admission cap, enforced on
    /// the inserting branch only and in the same transaction as the write —
    /// a caller checking it beforehand would refuse a burst that adds no
    /// pending row, and checking it separately reopens the race this
    /// transaction closes. Over the cap, nothing is written and the call
    /// reports [`QueueError::QueueFull`](crate::error::QueueError::QueueFull).
    ///
    /// The Diesel backends get their atomicity from the transaction; the Redis
    /// backend has none, so it decides slide-vs-insert in a Lua script and
    /// commits a slide as a document compare-and-swap. Contended enough to lose
    /// that swap repeatedly, it errors rather than inserting a second job.
    fn enqueue_debounced(&self, new_job: NewJob, options: DebounceOptions) -> Result<Job>;
    /// Atomically claim the highest-priority ready job from a queue, moving it
    /// to `Running`. `None` when no job is eligible. `namespace = None`
    /// matches only namespace-less jobs.
    fn dequeue(&self, queue_name: &str, now: i64, namespace: Option<&str>) -> Result<Option<Job>>;
    /// [`dequeue`](Self::dequeue) across several queues, checked in order. Each
    /// queue uses its dispatch order from `orders` (absent = the `Fifo` default).
    fn dequeue_from(
        &self,
        queues: &[String],
        now: i64,
        namespace: Option<&str>,
        orders: &std::collections::HashMap<String, DispatchOrder>,
    ) -> Result<Option<Job>>;
    /// Atomically claim up to `max` ready jobs from a single queue in one
    /// transaction. Returns the claimed jobs (now in `Running` state). May
    /// return fewer than `max` if the queue lacks enough eligible jobs.
    fn dequeue_batch(
        &self,
        queue_name: &str,
        now: i64,
        namespace: Option<&str>,
        max: usize,
    ) -> Result<Vec<Job>>;
    /// Claim up to `max` ready jobs across the given queues, checking each in
    /// order until the budget is exhausted. Each queue uses its dispatch order
    /// from `orders` (absent = the `Fifo` default).
    fn dequeue_batch_from(
        &self,
        queues: &[String],
        now: i64,
        namespace: Option<&str>,
        max: usize,
        orders: &std::collections::HashMap<String, DispatchOrder>,
    ) -> Result<Vec<Job>>;
    /// Mark a job completed with its result, moving it from `jobs` into
    /// `archived_jobs` in one transaction. A job in another namespace reports
    /// `JobNotFound`, like an unknown id.
    fn complete(
        &self,
        id: &str,
        result_bytes: Option<Vec<u8>>,
        namespace: Option<&str>,
    ) -> Result<()>;

    /// Persist many successful completions at once. Each entry archives the
    /// completed job, clears its execution claim, and records its metric — the
    /// Diesel backends do so in one transaction. See [`JobCompletion`].
    /// `namespace` scopes every entry, as in [`complete`](Self::complete).
    ///
    /// [`JobCompletion`]: crate::job::JobCompletion
    fn complete_batch(
        &self,
        completions: &[crate::job::JobCompletion],
        namespace: Option<&str>,
    ) -> Result<()>;
    /// Mark a job terminally failed, moving it from `jobs` into `archived_jobs`.
    fn fail(&self, id: &str, error: &str) -> Result<()>;
    /// Re-schedule a job for retry at `next_scheduled_at`, incrementing its
    /// `retry_count`. A job in another namespace reports `JobNotFound`, like
    /// an unknown id.
    fn retry(&self, id: &str, next_scheduled_at: i64, namespace: Option<&str>) -> Result<()>;
    /// Re-schedule a job back to `Pending` **without** consuming its retry
    /// budget. Used for soft-gate reschedules (rate limit, circuit breaker,
    /// concurrency cap, channel backpressure) where the job never executed,
    /// unlike [`retry`](Self::retry) which increments `retry_count`.
    ///
    /// A job in another namespace reports `JobNotFound`, like an unknown id.
    /// Scoped even though the poller only ever passes a job it just claimed:
    /// a step sleep reschedules with an id that reached the queue through task
    /// code, which is the least trusted caller there is.
    fn reschedule(&self, id: &str, next_scheduled_at: i64, namespace: Option<&str>) -> Result<()>;
    /// Force a `Running` job back to `Pending` and release its execution
    /// claim atomically, so a healthy worker can re-claim it. Preserves the
    /// retry budget (operator action, mirrors [`reschedule`](Self::reschedule))
    /// and clears any pending cancel request. Returns `false` when the job is
    /// missing or not `Running`. Only for confirmed-dead/hung workers: a
    /// still-alive owner may finish the old attempt, double-executing the job.
    fn requeue_stuck(&self, id: &str, now: i64) -> Result<bool>;
    /// Cancel a `Pending` job (archived as `Cancelled`) and cascade-cancel its
    /// dependents. Returns `false` when the job is missing or not pending.
    /// A job in another namespace reports `false`, the same answer an unknown
    /// or already-terminal id gets: a caller scoped to one namespace learns
    /// nothing about ids outside it. `None` addresses every namespace.
    fn cancel_job(&self, id: &str, namespace: Option<&str>) -> Result<bool>;
    /// Set the cancel-requested flag on a `Running` job — the task must poll
    /// for it. Returns `false` when no running job matched, which is also the
    /// answer for a job in another namespace.
    fn request_cancel(&self, id: &str, namespace: Option<&str>) -> Result<bool>;
    /// Whether cancellation has been requested for a job. A job in another
    /// namespace reports `false`, like an unknown id.
    fn is_cancel_requested(&self, id: &str, namespace: Option<&str>) -> Result<bool>;
    /// Archive a running job as `Cancelled` after it observed a cancel request.
    /// A job in another namespace is left alone.
    fn mark_cancelled(&self, id: &str, namespace: Option<&str>) -> Result<()>;
    /// Cancel every pending job that depends, directly or transitively, on
    /// `failed_job_id`.
    ///
    /// Dependents outside `namespace` are left alone. Every edge written since
    /// the boundary was enforced is intra-namespace, so this only bites on
    /// older data — but a cancel must not archive another tenant's job because
    /// of an edge it should never have been allowed to create.
    fn cascade_cancel(
        &self,
        failed_job_id: &str,
        reason: &str,
        namespace: Option<&str>,
    ) -> Result<()>;
    /// Ids of the jobs `job_id` depends on.
    ///
    /// A dependency may not cross namespaces — [`enqueue`](Self::enqueue) and
    /// its variants reject one that would — so the edge list carries the
    /// anchor job's scope. A job in another namespace reports no edges, like
    /// an unknown id.
    fn get_dependencies(&self, job_id: &str, namespace: Option<&str>) -> Result<Vec<String>>;
    /// Ids of the jobs that depend on `job_id`. Scoped by the anchor job, like
    /// [`get_dependencies`](Self::get_dependencies).
    fn get_dependents(&self, job_id: &str, namespace: Option<&str>) -> Result<Vec<String>>;
    /// Update a running job's progress (0-100). A job in another namespace is
    /// left alone.
    fn update_progress(&self, id: &str, progress: i32, namespace: Option<&str>) -> Result<()>;
    /// List jobs by filter. Rows are **blob-free** on every backend: the
    /// `payload`/`result` blobs come back empty (Diesel selects a narrow
    /// projection; Redis strips them post-load). Fetch the full job — blobs
    /// included — with [`Storage::get_job`]. The same contract holds for every
    /// listing method (`list_jobs_filtered`, `list_archived`, `list_dead*`).
    fn list_jobs(
        &self,
        status: Option<i32>,
        queue_name: Option<&str>,
        task_name: Option<&str>,
        limit: i64,
        offset: i64,
        namespace: Option<&str>,
    ) -> Result<Vec<Job>>;
    /// Keyset-paginated `list_jobs`, ordered by `(created_at, id)` descending.
    /// `after` is the `(created_at, id)` of the previous page's last row; the
    /// caller derives the next cursor from the returned rows' last element.
    /// Stable under concurrent inserts (unlike the offset form), and O(page) at
    /// any depth on the Diesel backends. Rows are blob-free like every listing.
    ///
    /// **Redis exception:** the job status indexes are plain SETs with no
    /// ordering to seek, so this applies the keyset in memory over the same
    /// candidate set the offset form loads — correct and stable, but O(matching
    /// rows) rather than O(page). Redis `list_jobs` is already O(N), so this is
    /// no worse; a seekable index requires a backfill migration of pre-existing
    /// rows. `list_archived_after` and `list_dead_after` are seekable on Redis
    /// and carry no such exception.
    fn list_jobs_after(
        &self,
        status: Option<i32>,
        queue_name: Option<&str>,
        task_name: Option<&str>,
        limit: i64,
        after: Option<(i64, &str)>,
        namespace: Option<&str>,
    ) -> Result<Vec<Job>>;
    /// Fetch a job by id, blobs included — live `jobs` first, then
    /// `archived_jobs`. A job in another namespace reads as missing.
    fn get_job(&self, id: &str, namespace: Option<&str>) -> Result<Option<Job>>;
    /// Global queue statistics: live counts from `jobs`, terminal counts from
    /// `archived_jobs`.
    /// `namespace` of `None` counts every namespace, matching `list_jobs`.
    fn stats(&self, namespace: Option<&str>) -> Result<QueueStats>;
    /// Read-only retention dry-run: count the rows each purge would delete under
    /// `cutoffs`, without deleting anything. The per-table counts mirror the
    /// purge predicates exactly — `archived_jobs`/`dead_letter` always include
    /// per-entry-TTL-expired rows (compared to `now`) plus the global window
    /// when its cutoff is set; the side tables count only when their cutoff is
    /// set. Cheap and indexed on the Diesel backends; the Redis archived/dead
    /// counts inspect their blobs like the purges they mirror.
    fn count_expired_rows(&self, cutoffs: &RetentionCutoffs, now: i64) -> Result<RetentionCounts>;
    /// Purge archived completed jobs older than the cutoff. Returns the count
    /// removed.
    fn purge_completed(&self, older_than_ms: i64) -> Result<u64>;
    /// Purge archived jobs by the global/per-entry TTL, covering every terminal
    /// status on all backends.
    fn purge_completed_with_ttl(&self, global_cutoff_ms: Option<i64>) -> Result<u64>;
    /// Running jobs that exceeded their timeout, for the scheduler to fail or
    /// retry. Scoped so a scheduler never times out another namespace's job and
    /// then records the outcome under its own.
    fn reap_stale_jobs(&self, now: i64, namespace: Option<&str>) -> Result<Vec<Job>>;
    /// Running jobs whose execution-claim owner is not in `live_owner_ids` (the
    /// worker that claimed them has died). Read-only — paired with the dead
    /// owner so the caller can atomically reclaim before requeuing. Scoped like
    /// [`Storage::reap_stale_jobs`].
    fn reap_orphaned_jobs(
        &self,
        live_owner_ids: &[String],
        now: i64,
        namespace: Option<&str>,
    ) -> Result<Vec<(Job, String)>>;
    /// Record one failed attempt's error for a job.
    fn record_error(
        &self,
        job_id: &str,
        attempt: i32,
        error: &str,
        namespace: Option<&str>,
    ) -> Result<()>;
    /// All recorded errors for a job, ordered by attempt. A job in another
    /// namespace reports no errors, like an unknown id.
    fn get_job_errors(&self, job_id: &str, namespace: Option<&str>) -> Result<Vec<JobError>>;
    /// Purge error records older than the cutoff. Returns the count removed.
    fn purge_job_errors(&self, older_than_ms: i64) -> Result<u64>;

    // ── Dead letter operations ──────────────────────────────────────

    /// Move a job to the dead-letter queue and cascade-cancel its dependents.
    /// Records an ordinary failure; a job the scheduler threw away on purpose
    /// goes through [`shed_to_dlq`](Self::shed_to_dlq) instead.
    fn move_to_dlq(&self, job: &Job, error: &str, metadata: Option<&str>) -> Result<()>;
    /// Dead-letter a job the scheduler shed rather than ran, marking the entry
    /// so [`list_dead_for_retry`](Self::list_dead_for_retry) never offers it.
    ///
    /// A separate method rather than a flag on `move_to_dlq` for two reasons:
    /// the reason prefixes that produce a shed (`codel:`, `rate_limit:`) stay
    /// the scheduler's vocabulary — storage is told "shed", never asked to
    /// parse — and `move_to_dlq` keeps the signature it published.
    ///
    /// Defaults to [`move_to_dlq`](Self::move_to_dlq) so an out-of-tree backend
    /// keeps compiling. Such a backend records the entry as an ordinary failure
    /// and leans on the sweep's reason-prefix guard, exactly as every backend
    /// did before the `shed` flag existed.
    fn shed_to_dlq(&self, job: &Job, error: &str, metadata: Option<&str>) -> Result<()> {
        self.move_to_dlq(job, error, metadata)
    }
    /// Dead-letter entries, newest first, paginated.
    /// `namespace` of `None` returns every namespace, matching `list_jobs`.
    fn list_dead(&self, limit: i64, offset: i64, namespace: Option<&str>) -> Result<Vec<DeadJob>>;
    /// Keyset-paginated `list_dead`, ordered by `(failed_at, id)` descending.
    /// See [`Storage::list_jobs_after`] for the cursor contract.
    fn list_dead_after(
        &self,
        limit: i64,
        after: Option<(i64, &str)>,
        namespace: Option<&str>,
    ) -> Result<Vec<DeadJob>>;
    /// Dead-letter entries for one task, newest first, paginated.
    fn list_dead_by_task(
        &self,
        task_name: &str,
        limit: i64,
        offset: i64,
        namespace: Option<&str>,
    ) -> Result<Vec<DeadJob>>;
    /// Delete every dead-letter entry for a task. Returns the number removed.
    fn purge_dead_by_task(&self, task_name: &str) -> Result<u64>;
    /// Re-enqueue a dead-letter entry as a fresh job, deleting the entry.
    /// Returns the new job's id; `JobNotFound` if the entry is absent.
    /// An entry in another namespace reports `JobNotFound`.
    fn retry_dead(&self, dead_id: &str, namespace: Option<&str>) -> Result<String>;
    /// Purge dead-letter entries older than the cutoff. Returns the count
    /// removed.
    fn purge_dead(&self, older_than_ms: i64) -> Result<u64>;
    /// Delete one dead-letter entry. Returns `false` when no row matched.
    /// An entry in another namespace reports `false`.
    fn delete_dead(&self, dead_id: &str, namespace: Option<&str>) -> Result<bool>;
    /// Purge dead-letter entries by the global/per-entry TTL. Returns the
    /// count removed.
    fn purge_dead_with_ttl(&self, global_cutoff_ms: Option<i64>) -> Result<u64>;
    /// Dead-letter entries eligible for automatic retry, bounded by `limit`.
    ///
    /// Entries written by [`shed_to_dlq`](Self::shed_to_dlq) are excluded: the
    /// scheduler dropped them on purpose, and letting them fill the bounded
    /// page would starve the sweep of the failures it exists to retry.
    fn list_dead_for_retry(
        &self,
        cutoff_ms: i64,
        max_retries: i32,
        namespace: Option<&str>,
        queues: &[String],
        limit: i64,
    ) -> Result<Vec<DeadJob>>;

    // ── Rate limit operations ───────────────────────────────────────

    /// Token-bucket state for a rate-limit key, if one exists.
    fn get_rate_limit(&self, key: &str) -> Result<Option<RateLimitState>>;
    /// Insert or replace a token-bucket state row.
    fn upsert_rate_limit(&self, row: &RateLimitState) -> Result<()>;
    /// Atomically refill and consume one token. Returns `false` when the
    /// bucket is empty.
    fn try_acquire_token(&self, key: &str, max_tokens: f64, refill_rate: f64) -> Result<bool>;

    // ── Periodic task operations ────────────────────────────────────

    /// Register or update a periodic task by name.
    fn register_periodic(&self, task: &NewPeriodicTask) -> Result<()>;
    /// Enabled periodic tasks whose `next_run` is due at `now`.
    fn get_due_periodic(&self, now: i64) -> Result<Vec<PeriodicTask>>;
    /// Advance a periodic task's schedule after it fires.
    fn update_periodic_schedule(&self, name: &str, last_run: i64, next_run: i64) -> Result<()>;
    /// All registered periodic tasks, enabled or paused.
    fn list_periodic(&self) -> Result<Vec<PeriodicTask>>;
    /// Remove a periodic task. Returns false if no task had that name.
    fn delete_periodic(&self, name: &str) -> Result<bool>;
    /// Pause (false) or resume (true) a periodic task by toggling `enabled`.
    /// Returns false if no task had that name.
    fn set_periodic_enabled(&self, name: &str, enabled: bool) -> Result<bool>;

    // ── Topic pub/sub ───────────────────────────────────────────────

    /// Insert or update a subscription. Idempotent on (topic, subscription_name).
    fn register_subscription(&self, sub: &NewSubscription) -> Result<()>;
    /// Active subscriptions for a topic (active = true only).
    fn list_subscriptions_for_topic(&self, topic: &str) -> Result<Vec<Subscription>>;
    /// Every registered subscription (active or paused), all topics.
    fn list_subscriptions(&self) -> Result<Vec<Subscription>>;
    /// Remove a subscription. Returns false if none matched.
    fn unsubscribe(&self, topic: &str, subscription_name: &str) -> Result<bool>;
    /// Pause/resume without removing registration. Returns false if none matched.
    fn set_subscription_active(
        &self,
        topic: &str,
        subscription_name: &str,
        active: bool,
    ) -> Result<bool>;
    /// Remove ephemeral subscriptions (owner_worker_id set) whose owner is not in
    /// `live_worker_ids`. Durable rows (owner NULL) are never touched. Returns the
    /// count removed.
    fn reap_ephemeral_subscriptions(&self, live_worker_ids: &[String]) -> Result<u64>;

    /// Backlog/lag snapshot for every registered subscription across all topics.
    /// Bounded by the pub/sub-tagged rows (partial index), never a full `jobs`
    /// table scan — safe to poll on a dashboard cadence.
    fn topic_backlog_stats(&self) -> Result<Vec<SubscriptionBacklogStats>>;

    // ── Log topics (append-once + cursor) ───────────────────────────

    /// Append one message to a log topic and return it (id generated). O(1) —
    /// independent of subscriber count, unlike fan-out delivery.
    fn publish_message(
        &self,
        topic: &str,
        payload: &[u8],
        metadata: Option<&str>,
        notes: Option<&str>,
        expires_at: Option<i64>,
    ) -> Result<TopicMessage>;
    /// Messages after a log subscription's cursor, oldest first, up to `limit`.
    /// The cursor is resolved server-side from the subscription row; the read is
    /// exclusive of the cursor. An empty result means the consumer is caught up.
    fn read_topic_messages(
        &self,
        topic: &str,
        subscription_name: &str,
        limit: i64,
    ) -> Result<Vec<TopicMessage>>;
    /// Advance a log subscription's cursor to `cursor` (a message id). Monotonic:
    /// never rewinds (a lower/equal cursor is a no-op). Returns false if no
    /// subscription matched.
    fn ack_topic_cursor(&self, topic: &str, subscription_name: &str, cursor: &str) -> Result<bool>;
    /// Lag snapshot for every log subscription: messages after the cursor and
    /// the oldest un-acked age. Fan-out subscriptions are excluded.
    fn topic_log_stats(&self) -> Result<Vec<TopicLogStats>>;
    /// Purge fully-consumed log messages: for each topic, drop messages whose id
    /// is `<=` the minimum cursor across its log subscriptions, plus any past
    /// `expires_at`. Bounded by `limit`. Returns the count removed. Caller gates
    /// this on the reaper election.
    fn purge_topic_messages(&self, now: i64, limit: i64) -> Result<u64>;

    // ── Topic registry (declared topics) ────────────────────────────

    /// Declare a topic (idempotent upsert on `name`), setting its `mode` and
    /// optional `retention_ms`. Declaring a log topic makes its publishes
    /// retained even with no subscriber (removing the late-join boundary).
    /// `mode` must be `"log"` (the only declarable mode) and `retention_ms`, if
    /// set, must be non-negative — backends reject anything else.
    fn declare_topic(
        &self,
        name: &str,
        mode: SubscriptionMode,
        retention_ms: Option<i64>,
    ) -> Result<()>;
    /// Fetch a declared topic by name, or `None` if it was never declared.
    fn get_topic(&self, name: &str) -> Result<Option<Topic>>;
    /// List every declared topic in the registry.
    fn list_declared_topics(&self) -> Result<Vec<Topic>>;

    // ── Per-message ack (opt-in, log topics) ────────────────────────

    /// Lease up to `limit` available messages of a log topic to `subscription_name`
    /// for `visibility_ms`, oldest first. "Available" = never delivered, or a
    /// prior lease that expired (`lease_expires_at <= now`) and was never acked.
    /// Each leased message's delivery row is upserted (lease extended, `attempts`
    /// bumped). Unlike the cursor read this tracks per-message state, so a nacked
    /// or timed-out message is redelivered without blocking its siblings.
    fn lease_topic_messages(
        &self,
        topic: &str,
        subscription_name: &str,
        limit: i64,
        visibility_ms: i64,
        now: i64,
    ) -> Result<Vec<TopicMessage>>;
    /// Ack one leased message — the delivery is done and never redelivered.
    /// Returns false if there was no un-acked delivery to ack.
    fn ack_message(&self, topic: &str, subscription_name: &str, message_id: &str) -> Result<bool>;
    /// Negative-ack one leased message — makes it immediately available for
    /// redelivery (vs waiting for the visibility timeout). Returns false if there
    /// was no un-acked delivery to nack.
    fn nack_message(&self, topic: &str, subscription_name: &str, message_id: &str) -> Result<bool>;

    // ── Metrics operations ──────────────────────────────────────────

    /// Record one execution measurement for a task.
    fn record_metric(
        &self,
        task_name: &str,
        job_id: &str,
        wall_time_ns: i64,
        memory_bytes: i64,
        succeeded: bool,
        namespace: Option<&str>,
    ) -> Result<()>;
    /// Metrics recorded since `since_ms` for one task, or all tasks when
    /// `name` is `None`.
    /// `namespace` of `None` returns every namespace, matching `list_jobs`.
    fn get_metrics(
        &self,
        name: Option<&str>,
        since_ms: i64,
        namespace: Option<&str>,
    ) -> Result<Vec<TaskMetric>>;
    /// Purge metric records older than the cutoff. Returns the count removed.
    fn purge_metrics(&self, older_than_ms: i64) -> Result<u64>;
    /// Record a replay of a completed job, pairing original and replay
    /// outcomes.
    fn record_replay(
        &self,
        original_job_id: &str,
        replay_job_id: &str,
        original_result: Option<&[u8]>,
        replay_result: Option<&[u8]>,
        original_error: Option<&str>,
        replay_error: Option<&str>,
    ) -> Result<()>;
    /// All replays recorded against `original_job_id`.
    fn get_replay_history(&self, original_job_id: &str) -> Result<Vec<ReplayEntry>>;

    // ── Log operations ──────────────────────────────────────────────

    /// Write one structured log line for a job. `extra` is pre-encoded JSON.
    fn write_task_log(
        &self,
        job_id: &str,
        task_name: &str,
        level: &str,
        message: &str,
        extra: Option<&str>,
        namespace: Option<&str>,
    ) -> Result<()>;
    /// All log lines for a job, in emission order. A job in another namespace
    /// reports no lines, like an unknown id.
    fn get_task_logs(&self, job_id: &str, namespace: Option<&str>) -> Result<Vec<TaskLogEntry>>;
    /// Logs for a job with id strictly after `after_id` (UUIDv7 ids are
    /// time-ordered, so the id doubles as a stream cursor). `None` = all.
    /// Scoped like [`Storage::get_task_logs`].
    fn get_task_logs_after(
        &self,
        job_id: &str,
        after_id: Option<&str>,
        namespace: Option<&str>,
    ) -> Result<Vec<TaskLogEntry>>;
    /// Log lines across jobs, filtered by task name and/or level, newest
    /// since `since_ms`, bounded by `limit`.
    fn query_task_logs(
        &self,
        task_name: Option<&str>,
        level: Option<&str>,
        since_ms: i64,
        limit: i64,
        namespace: Option<&str>,
    ) -> Result<Vec<TaskLogEntry>>;
    /// Purge log lines older than the cutoff. Returns the count removed.
    fn purge_task_logs(&self, older_than_ms: i64) -> Result<u64>;

    // ── Circuit breaker operations ──────────────────────────────────

    /// Persisted circuit-breaker state for a task, if one exists.
    fn get_circuit_breaker(&self, task_name: &str) -> Result<Option<CircuitBreakerState>>;
    /// Insert or replace a task's circuit-breaker state.
    fn upsert_circuit_breaker(&self, row: &CircuitBreakerState) -> Result<()>;
    /// Every persisted circuit-breaker state.
    fn list_circuit_breakers(&self) -> Result<Vec<CircuitBreakerState>>;

    // ── Worker operations ───────────────────────────────────────────

    /// Register a worker in the cluster registry, or update it if the id
    /// already exists.
    fn register_worker(&self, registration: &WorkerRegistration<'_>) -> Result<()>;
    /// Refresh a worker's heartbeat timestamp, optionally updating its
    /// resource-health JSON.
    fn heartbeat(&self, worker_id: &str, resource_health: Option<&str>) -> Result<()>;
    /// Set a worker's lifecycle status.
    fn update_worker_status(&self, worker_id: &str, status: WorkerStatus) -> Result<()>;
    /// Every registered worker with its heartbeat state.
    fn list_workers(&self) -> Result<Vec<WorkerInfo>>;
    /// Ids of workers whose heartbeat is at or after `cutoff_ms`. A narrow
    /// projection of [`Self::list_workers`] for callers that only need the live
    /// set and must not pay to load every worker's `resource_health` blob.
    fn list_live_worker_ids(&self, cutoff_ms: i64) -> Result<Vec<String>>;
    /// Remove workers whose heartbeat is stale past the dead-worker threshold.
    /// Returns the reaped worker ids.
    fn reap_dead_workers(&self) -> Result<Vec<String>>;
    /// Remove a worker from the registry (called on shutdown).
    fn unregister_worker(&self, worker_id: &str) -> Result<()>;
    /// Job ids currently execution-claimed by a worker.
    fn list_claims_by_worker(&self, worker_id: &str) -> Result<Vec<String>>;

    // ── Queue pause/resume ───────────────────────────────────────

    /// Pause a queue so no new jobs are dispatched from it.
    fn pause_queue(&self, queue_name: &str) -> Result<()>;
    /// Resume a paused queue.
    fn resume_queue(&self, queue_name: &str) -> Result<()>;
    /// Names of all currently paused queues.
    fn list_paused_queues(&self) -> Result<Vec<String>>;

    // ── Job expiry ───────────────────────────────────────────────

    /// Fail pending jobs whose `expires_at` has passed. Returns the count
    /// expired.
    fn expire_pending_jobs(&self, now: i64) -> Result<u64>;

    // ── Job revocation ───────────────────────────────────────────

    /// Cancel every pending job in a queue. Returns the count cancelled.
    fn cancel_pending_by_queue(&self, queue: &str) -> Result<u64>;
    /// Cancel every pending job for a task. Returns the count cancelled.
    fn cancel_pending_by_task(&self, task_name: &str) -> Result<u64>;

    // ── Job archival ─────────────────────────────────────────────

    /// Move `Complete`/`Dead`/`Cancelled` jobs older than the cutoff from
    /// `jobs` into `archived_jobs`. Returns the count archived. `Failed`
    /// jobs are archived immediately by `fail()`, never by this sweep.
    fn archive_old_jobs(&self, cutoff_ms: i64) -> Result<u64>;
    /// Archived jobs, newest first, paginated. Rows are blob-free.
    /// `namespace` of `None` returns every namespace, matching `list_jobs`.
    fn list_archived(&self, limit: i64, offset: i64, namespace: Option<&str>) -> Result<Vec<Job>>;
    /// Keyset-paginated `list_archived`, ordered by `(completed_at, id)`
    /// descending. See [`Storage::list_jobs_after`] for the cursor contract.
    fn list_archived_after(
        &self,
        limit: i64,
        after: Option<(i64, &str)>,
        namespace: Option<&str>,
    ) -> Result<Vec<Job>>;

    // ── Distributed locking ────────────────────────────────────

    /// Try to take a distributed lock for `ttl_ms`. Returns `false` when
    /// another owner (or this one) still holds an unexpired lock.
    fn acquire_lock(&self, lock_name: &str, owner_id: &str, ttl_ms: i64) -> Result<bool>;
    /// Release a lock. Returns `true` only if `owner_id` held it.
    fn release_lock(&self, lock_name: &str, owner_id: &str) -> Result<bool>;
    /// Extend a lock's TTL. Returns `true` only if `owner_id` held it.
    fn extend_lock(&self, lock_name: &str, owner_id: &str, ttl_ms: i64) -> Result<bool>;
    /// Holder and expiry of a lock, if it exists.
    fn get_lock_info(&self, lock_name: &str) -> Result<Option<LockInfo>>;
    /// Remove locks that expired before `now`. Returns the count removed.
    fn reap_expired_locks(&self, now: i64) -> Result<u64>;

    // ── Execution claims (exactly-once) ────────────────────────

    /// Claim exclusive execution of a job for `worker_id`. Returns the
    /// **epoch** the claim was won under, or `None` when a claim already
    /// exists.
    ///
    /// The epoch is the identity of this claim, and of the dispatch made under
    /// it. Without it `(owner, attempt)` cannot separate two claims that
    /// [`Storage::requeue_stuck`] produced from one attempt, and the first
    /// executor's late result authorizes over the second's. See
    /// [`crate::lease`].
    fn claim_execution(&self, job_id: &str, worker_id: &str) -> Result<Option<i64>>;
    /// Batch variant of [`Storage::claim_execution`]: attempt to claim every
    /// `job_id` for `worker_id` in as few round trips as the backend allows.
    /// Returns one result per input id, in order — the epoch if this worker won
    /// the claim, `None` if a claim already existed. Each won claim gets its
    /// **own** epoch: two jobs claimed together are still two claims.
    fn claim_execution_batch(&self, job_ids: &[&str], worker_id: &str) -> Result<Vec<Option<i64>>>;
    /// Remove the execution claim of a finished job.
    fn complete_execution(&self, job_id: &str, namespace: Option<&str>) -> Result<()>;
    /// Purge execution claims older than the cutoff. Returns the count
    /// removed.
    fn purge_execution_claims(&self, older_than_ms: i64) -> Result<u64>;
    /// Atomically transfer an existing claim from `expected_owner` to
    /// `new_owner`. Returns the transfer's **new epoch**, and only if the claim
    /// was held by `expected_owner` — the `job_id` PK serializes concurrent
    /// rescuers so exactly one wins.
    ///
    /// The epoch moves with the owner: a rescuer that kept the old one could
    /// authorize a result the dead owner's executor is still on its way to
    /// sending.
    fn reclaim_execution(
        &self,
        job_id: &str,
        expected_owner: &str,
        new_owner: &str,
    ) -> Result<Option<i64>>;

    // ── Durable inline steps ──────────────────────────────────────

    /// Whether this backend implements the step store.
    ///
    /// `false` disables the inline-step API outright. It must never degrade to
    /// "no memo recorded": a step store that fails open re-runs a charge.
    fn supports_steps(&self) -> bool {
        false
    }

    /// Every committed step for a job, ordered by `seq`.
    ///
    /// Read **once** per attempt, by the worker that just won the claim, and
    /// never per step. Unfenced for that reason: a stale read can only cost a
    /// re-run, which is what the memo would have prevented anyway.
    fn get_job_steps(&self, job_id: &str, namespace: Option<&str>) -> Result<Vec<JobStep>> {
        let _ = (job_id, namespace);
        Err(steps_unsupported())
    }

    /// Commit one step, fenced on the writer still owning the execution claim.
    ///
    /// `owner` is the worker id the writer claimed under and `attempt` the
    /// `retry_count` the job carried at claim time. Both are resolved against
    /// the live rows inside the write's own transaction: a claim naming another
    /// worker, or a job that has moved past this attempt, is
    /// [`QueueError::ClaimLost`]. A claim that is merely *absent* while the job
    /// is still `Running` at the same attempt is re-asserted rather than
    /// refused — claims are swept by age, so a long job legitimately outlives
    /// its own claim row while still being the only thing executing.
    ///
    /// `epoch` is the third part of the same fence: the identity of the claim
    /// the writer was dispatched under, which is what separates two claims one
    /// owner won at one attempt. `None` skips the comparison — a caller that
    /// holds no lease, which is what every caller was before it existed.
    ///
    /// None of the three is something the caller asserts about itself:
    /// in-process and prefork workers pass what they won the claim with, and an
    /// attached executor's comes from the scheduler's dispatch record, never
    /// off a frame.
    ///
    /// Enforces `limits` — clamped to the hard ceilings, on the encoded bytes —
    /// and rejects a `seq` that is not exactly the number of steps already
    /// committed. A byte-identical re-commit at the same position is
    /// [`StepCommit::AlreadyCommitted`], which is a success; anything else
    /// stored there is [`QueueError::StepDiverged`].
    fn record_step_result(
        &self,
        step: &NewJobStep<'_>,
        owner: &str,
        attempt: i32,
        epoch: Option<i64>,
        limits: &StepLimits,
        namespace: Option<&str>,
    ) -> Result<StepCommit> {
        let _ = (step, owner, attempt, epoch, limits, namespace);
        Err(steps_unsupported())
    }

    /// End the attempt in a sleep: commit the sleep row, release the execution
    /// claim, and reschedule the job — one atomic operation, fenced exactly as
    /// [`record_step_result`](Self::record_step_result) is.
    ///
    /// Split into three calls, a crash between the row and the reschedule
    /// leaves the job `Running` with an unreached deadline, and the stale
    /// reaper then hands it to another worker while its timeout clock still
    /// runs. One transaction has no such window.
    ///
    /// `wake_at` is a *candidate*. A sleep row already committed at this
    /// position keeps the deadline it was first given, and the reschedule
    /// targets that stored value — otherwise a duration sleep would push its
    /// own deadline further out on every replay. A sleep commits no bytes, so
    /// only `max_steps` can bite, but it is counted like any other step.
    // The arguments are the fence (`owner`, `attempt`, `epoch`) plus the
    // sleep's own three; bundling them would only hide which half is which.
    #[allow(clippy::too_many_arguments)]
    fn sleep_job(
        &self,
        step: &NewJobStep<'_>,
        owner: &str,
        attempt: i32,
        epoch: Option<i64>,
        wake_at: i64,
        limits: &StepLimits,
        namespace: Option<&str>,
    ) -> Result<SleepOutcome> {
        let _ = (step, owner, attempt, epoch, wake_at, limits, namespace);
        Err(steps_unsupported())
    }

    /// Whether a result carrying `(owner, attempt, epoch)` still speaks for
    /// this job.
    ///
    /// The same resolution [`record_step_result`](Self::record_step_result)
    /// fences writes with, exposed for the scheduler: a terminal transition
    /// applied to the wrong attempt is no longer only a wrong status — it
    /// deletes another attempt's steps.
    ///
    /// Defaults to [`AttemptFence::Authorized`], which is exactly how every
    /// caller behaved before the fence existed. This is the one gate here that
    /// must *not* fail closed: a backend that cannot evaluate it would otherwise
    /// drop every result and leave every job `Running` forever.
    fn authorize_attempt(
        &self,
        job_id: &str,
        owner: &str,
        attempt: i32,
        epoch: Option<i64>,
        namespace: Option<&str>,
    ) -> Result<AttemptFence> {
        let _ = (job_id, owner, attempt, epoch, namespace);
        Ok(AttemptFence::Authorized)
    }

    /// Drop every step row for a job. Returns the count removed.
    ///
    /// **Not** how the terminal paths clean up — they delete the rows inline,
    /// in the same transaction that moves the job, so no crash can strand a
    /// dead job's blobs. This is the explicit admin/repair entry point.
    fn delete_job_steps(&self, job_id: &str, namespace: Option<&str>) -> Result<u64> {
        let _ = (job_id, namespace);
        Err(steps_unsupported())
    }

    // ── Per-task concurrency ──────────────────────────────────────

    /// Running-job count for a task — the per-task concurrency-cap primitive.
    /// Scoped, so a job elsewhere never consumes this scheduler's budget.
    fn count_running_by_task(&self, task_name: &str, namespace: Option<&str>) -> Result<i64>;

    // ── Per-queue stats ──────────────────────────────────────────

    /// Cheap count of pending jobs on a queue — the admission-cap primitive.
    /// Single-status, unlike the full-breakdown `stats_by_queue`.
    fn count_pending_by_queue(&self, queue_name: &str) -> Result<i64>;

    /// Statistics for one queue: live counts from `jobs`, terminal counts
    /// from `archived_jobs`.
    fn stats_by_queue(&self, queue_name: &str, namespace: Option<&str>) -> Result<QueueStats>;
    /// Statistics broken down per queue name.
    fn stats_all_queues(
        &self,
        namespace: Option<&str>,
    ) -> Result<std::collections::HashMap<String, QueueStats>>;

    // ── Filtered job listing ─────────────────────────────────────

    /// `list_jobs` with extra filters (metadata/error substring, created-at
    /// range). Rows are blob-free like every listing.
    #[allow(clippy::too_many_arguments)]
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
    ) -> Result<Vec<Job>>;

    /// Keyset-paginated `list_jobs_filtered`, ordered by `(created_at, id)`
    /// descending. See [`Storage::list_jobs_after`] for the cursor contract.
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
    ) -> Result<Vec<Job>>;

    // ── Dashboard settings (key/value store) ─────────────────────

    /// Fetch a single setting value by key, or ``None`` if unset.
    fn get_setting(&self, key: &str) -> Result<Option<String>>;
    /// Insert or update a setting.
    fn set_setting(&self, key: &str, value: &str) -> Result<()>;
    /// Write a setting only if it still holds ``expected``, where ``None``
    /// means the key must be unset. Returns ``false`` when another writer got
    /// there first, so a read-modify-write caller can re-read and retry.
    ///
    /// Every backend does this in one atomic operation — the settings rows hold
    /// whole JSON documents, and a plain [`Storage::set_setting`] after a read
    /// would silently drop a concurrent edit.
    fn set_setting_if(&self, key: &str, expected: Option<&str>, value: &str) -> Result<bool>;
    /// Delete a setting. Returns ``true`` if a row was removed.
    fn delete_setting(&self, key: &str) -> Result<bool>;
    /// Return all settings as a key→value map.
    fn list_settings(&self) -> Result<std::collections::HashMap<String, String>>;
}

/// The error every step method defaults to.
///
/// Deliberately an error and not an empty result: this mirrors
/// [`Storage::shed_to_dlq`]'s defaulted shape for source compatibility but
/// inverts its semantics. A shed may safely degrade to an ordinary dead-letter;
/// a step read that degrades to "nothing recorded" silently re-executes work
/// the memo exists to skip.
fn steps_unsupported() -> QueueError {
    QueueError::Other("this storage backend does not implement the step store".to_string())
}
