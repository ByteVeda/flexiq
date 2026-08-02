pub mod codel;
mod maintenance;
mod poller;
mod result_handler;
pub mod retention;
#[cfg(feature = "push-dispatch")]
#[doc(hidden)]
pub mod wake;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use log::error;
use tokio::sync::Notify;

use crate::resilience::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use crate::resilience::dlq::DeadLetterQueue;
use crate::resilience::rate_limiter::RateLimiter;
use crate::resilience::retry::RetryPolicy;
use crate::storage::StorageBackend;

pub use crate::job::Job;

/// Configuration for the scheduler's timing and behavior.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Interval between scheduler poll cycles.
    pub poll_interval: Duration,
    /// Priority boost per second of wait time. `None` disables aging.
    /// Example: `Some(1)` boosts priority by 1 per second of wait.
    pub aging_factor: Option<i64>,
    /// Reap stale jobs every N iterations.
    pub reap_interval: u32,
    /// Check periodic tasks every N iterations.
    pub periodic_check_interval: u32,
    /// Auto-cleanup old jobs every N iterations.
    pub cleanup_interval: u32,
    /// Deprecated queue-wide result TTL in milliseconds. Superseded by
    /// [`SchedulerConfig::retention`]; kept so existing callers still work — it
    /// maps onto every table. `None` means no queue-wide auto-cleanup.
    pub result_ttl_ms: Option<i64>,
    /// Per-table retention windows. When set, wins over `result_ttl_ms`. `None`
    /// falls back to the legacy `result_ttl_ms` mapping.
    pub retention: Option<retention::RetentionConfig>,
    /// Maximum number of jobs claimed per dispatch round. `1` (the default)
    /// preserves the original one-job-per-round-trip behavior; values above
    /// `1` enable batch claiming for higher throughput.
    pub batch_size: usize,
    /// Upper bound on jobs this scheduler keeps in flight (dispatched to the
    /// worker channel but not yet finished). Set it to the worker pool's
    /// execution parallelism so a single scheduler never claims more work than
    /// its workers can run — otherwise a drain-until-empty poll would mark a
    /// deep backlog `Running` on one worker and starve peers sharing the DB.
    /// `None` (the default) leaves dispatch unbounded (legacy behavior).
    pub max_in_flight: Option<usize>,
    /// Minimum age (ms) before a DLQ entry is auto-retried. `None` disables.
    pub dlq_auto_retry_delay_ms: Option<i64>,
    /// Max DLQ auto-retries per entry before giving up.
    pub dlq_auto_retry_max: i32,
    /// Check DLQ auto-retry every N ticks.
    pub dlq_auto_retry_interval: u32,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(50),
            aging_factor: None,
            reap_interval: 100,
            periodic_check_interval: 60,
            cleanup_interval: 1200,
            result_ttl_ms: None,
            retention: None,
            batch_size: 1,
            max_in_flight: None,
            dlq_auto_retry_delay_ms: None,
            dlq_auto_retry_max: 1,
            dlq_auto_retry_interval: 60,
        }
    }
}

impl SchedulerConfig {
    /// The retention windows this config actually applies. An explicit
    /// `retention` wins (an empty one disables retention); otherwise the
    /// deprecated queue-wide `result_ttl_ms` maps onto every table, which is
    /// exactly what it used to mean. With neither set, retention falls back to
    /// the recommended defaults — history is bounded out of the box. Negative
    /// windows are dropped on the explicit and legacy paths (a negative would
    /// invert the cutoff into the future and match every row; the bindings
    /// reject them, this is defense in depth for a directly-constructed config).
    pub fn effective_retention(&self) -> retention::RetentionConfig {
        retention::resolve_effective_retention(self.retention.as_ref(), self.result_ttl_ms)
    }

    /// True when retention runs on the recommended defaults because nothing was
    /// configured — no per-table map and no legacy `result_ttl_ms` at all. The
    /// one-time cleanup announcement fires only in this case: an explicit, legacy,
    /// or set-but-invalid config is the operator's own doing and needs no warning.
    pub fn retention_is_defaulted(&self) -> bool {
        retention::is_retention_defaulted(self.retention.as_ref(), self.result_ttl_ms)
    }
}

/// Result of executing a job (sent back from worker threads).
pub enum JobResult {
    /// The task ran to completion.
    Success {
        /// Id of the completed job.
        job_id: String,
        /// Serialized result to archive.
        result: Option<Vec<u8>>,
        /// Task that ran.
        task_name: String,
        /// Wall-clock execution time in nanoseconds.
        wall_time_ns: i64,
    },
    /// The task raised an error or timed out.
    Failure {
        /// Id of the failed job.
        job_id: String,
        /// Error message (canonical JSON `TaskError` when structured).
        error: String,
        /// Retries already attempted before this failure.
        retry_count: i32,
        /// The job's retry cap.
        max_retries: i32,
        /// Task that ran.
        task_name: String,
        /// Wall-clock execution time in nanoseconds.
        wall_time_ns: i64,
        /// Whether the failure is retryable (subject to the retry policy).
        should_retry: bool,
        /// True when the failure was an execution timeout.
        timed_out: bool,
    },
    /// The task was cancelled while executing.
    Cancelled {
        /// Id of the cancelled job.
        job_id: String,
        /// Task that ran.
        task_name: String,
        /// Wall-clock execution time in nanoseconds.
        wall_time_ns: i64,
    },
}

impl JobResult {
    /// The id of the job this result is for.
    pub fn job_id(&self) -> &str {
        match self {
            JobResult::Success { job_id, .. }
            | JobResult::Failure { job_id, .. }
            | JobResult::Cancelled { job_id, .. } => job_id,
        }
    }
}

/// Outcome of processing a job result, returned to the caller (the binding)
/// for its middleware hook dispatch.
///
/// `wall_time_ns` carries the execution time the worker measured, so a shell can
/// report a job's duration without timing it again. It is 0 when nothing measured
/// the run — a job that failed before it ever executed, or one the core recovered
/// rather than a worker finishing it.
#[derive(Debug, Clone)]
pub enum ResultOutcome {
    /// Task completed successfully.
    Success {
        /// Id of the completed job.
        job_id: String,
        /// Task that ran.
        task_name: String,
        /// Wall-clock execution time in nanoseconds; 0 when not measured.
        wall_time_ns: i64,
    },
    /// Task failed and will be retried.
    Retry {
        /// Id of the failed job.
        job_id: String,
        /// Task that ran.
        task_name: String,
        /// Queue the job belongs to.
        queue: String,
        /// Error message of this failure.
        error: String,
        /// Retry count after this failure (1-based).
        retry_count: i32,
        /// True when the failure was an execution timeout.
        timed_out: bool,
        /// Wall-clock execution time in nanoseconds; 0 when not measured.
        wall_time_ns: i64,
    },
    /// Task exhausted retries and moved to the dead-letter queue.
    DeadLettered {
        /// Id of the dead-lettered job.
        job_id: String,
        /// Task that ran.
        task_name: String,
        /// Queue the job belonged to.
        queue: String,
        /// Final error message.
        error: String,
        /// True when the final failure was an execution timeout.
        timed_out: bool,
        /// Wall-clock execution time in nanoseconds; 0 when not measured.
        wall_time_ns: i64,
    },
    /// Task was cancelled during execution.
    Cancelled {
        /// Id of the cancelled job.
        job_id: String,
        /// Task that ran.
        task_name: String,
        /// Queue the job belonged to.
        queue: String,
        /// Wall-clock execution time in nanoseconds; 0 when not measured.
        wall_time_ns: i64,
    },
}

/// Per-task configuration for retry, rate limiting, and circuit breaker.
#[derive(Debug, Clone)]
pub struct TaskConfig {
    /// Retry delays and cap applied to failed runs.
    pub retry_policy: RetryPolicy,
    /// Dispatch rate limit for this task. `None` = unlimited.
    pub rate_limit: Option<crate::resilience::rate_limiter::RateLimitConfig>,
    /// Circuit-breaker settings for this task. `None` = no breaker.
    pub circuit_breaker: Option<CircuitBreakerConfig>,
    /// Cap on how fast this task may *retry*, across every job of the task.
    /// Nothing else catches sustained retry storms: the circuit breaker trips on
    /// hard failure, not on aggregate retry rate, and per-job `max_retries` is a
    /// per-job budget, not a rate. Exhausting it dead-letters instead of retrying.
    pub retry_budget: Option<crate::resilience::rate_limiter::RateLimitConfig>,
    /// Cluster-wide cap on concurrently running jobs of this task.
    pub max_concurrent: Option<i32>,
    /// Cap on this task's share of *this worker's* in-flight slots, so one slow
    /// task cannot occupy the whole pool and starve every other task. Complements
    /// `max_concurrent`, which is the cluster-wide cap and costs a database read;
    /// this one is in-process and free. `None` lets the task use the whole pool.
    pub max_in_flight_per_task: Option<usize>,
}

impl Default for TaskConfig {
    /// Default retry policy, no rate limit, breaker, retry budget, or caps.
    fn default() -> Self {
        Self {
            retry_policy: RetryPolicy::default(),
            rate_limit: None,
            circuit_breaker: None,
            retry_budget: None,
            max_concurrent: None,
            max_in_flight_per_task: None,
        }
    }
}

/// In-flight bookkeeping behind the dispatch caps: which jobs are out, and how many
/// of each task. Per-task counts are maintained alongside rather than derived so the
/// per-task gate stays O(1) — the poller consults it on every dispatch, and the map
/// is as large as `max_in_flight`.
#[derive(Default)]
struct InFlight {
    task_by_job: HashMap<String, String>,
    count_by_task: HashMap<String, usize>,
}

impl InFlight {
    fn len(&self) -> usize {
        self.task_by_job.len()
    }

    fn count_for(&self, task_name: &str) -> usize {
        self.count_by_task.get(task_name).copied().unwrap_or(0)
    }

    fn insert(&mut self, job_id: &str, task_name: &str) {
        if self
            .task_by_job
            .insert(job_id.to_string(), task_name.to_string())
            .is_none()
        {
            *self.count_by_task.entry(task_name.to_string()).or_insert(0) += 1;
        }
    }

    /// `true` if the job was tracked here (so a slot was actually freed).
    fn remove(&mut self, job_id: &str) -> bool {
        let Some(task_name) = self.task_by_job.remove(job_id) else {
            return false;
        };
        if let Some(count) = self.count_by_task.get_mut(&task_name) {
            *count -= 1;
            if *count == 0 {
                self.count_by_task.remove(&task_name);
            }
        }
        true
    }
}

/// Per-queue configuration for rate limiting and concurrency caps.
#[derive(Debug, Clone)]
pub struct QueueConfig {
    /// Dispatch rate limit for the whole queue. `None` = unlimited.
    pub rate_limit: Option<crate::resilience::rate_limiter::RateLimitConfig>,
    /// Cap on concurrently running jobs across the queue. `None` = uncapped.
    pub max_concurrent: Option<i32>,
}

/// The central scheduler that coordinates job dispatch, retries, rate limiting, and circuit breakers.
pub struct Scheduler {
    storage: StorageBackend,
    rate_limiter: RateLimiter,
    dlq: DeadLetterQueue,
    circuit_breaker: CircuitBreaker,
    task_configs: HashMap<String, TaskConfig>,
    queue_configs: HashMap<String, QueueConfig>,
    /// Opt-in per-queue CoDel tuning, kept out of `QueueConfig` so enabling it
    /// never forces every backend to thread a new field. Empty = no shedding.
    codel_configs: HashMap<String, codel::CodelConfig>,
    /// Per-queue CoDel controller state, keyed by queue name. Only touched when
    /// a queue has CoDel configured; `codel_configs.is_empty()` short-circuits
    /// the common no-CoDel case before this lock is ever taken.
    codel_states: Mutex<HashMap<String, codel::CodelState>>,
    /// Opt-in per-queue dispatch order (queue → Lifo). Absent = the fair Fifo
    /// default. Passed straight to the storage dequeue so each queue is scanned
    /// with its own tie-break direction.
    dispatch_orders: HashMap<String, crate::storage::DispatchOrder>,
    queues: Vec<String>,
    config: SchedulerConfig,
    shutdown: Arc<Notify>,
    paused_cache: Mutex<(HashSet<String>, Instant)>,
    namespace: Option<String>,
    /// Owner id recorded on execution claims. Defaults to a placeholder; the
    /// binding sets it to the process `worker_id` so dead-worker recovery can
    /// attribute orphaned claims. See [`Self::set_claim_owner`].
    claim_owner: String,
    /// Jobs dispatched to the worker channel but not yet finished, used only when
    /// `config.max_in_flight` is set. Its length is the current in-flight count;
    /// the poller stops dispatching once it hits the cap, and [`Self::handle_result`]
    /// removes a job as it finishes.
    in_flight: Mutex<InFlight>,
    /// Signalled when a finished job frees an in-flight slot, so the poll loop
    /// can refill immediately instead of waiting out its backoff — keeping
    /// throughput up despite the in-flight cap.
    dispatch_wake: Arc<Notify>,
    /// Fires the default-retention announcement exactly once per process, before
    /// the first delete. Only the elected cleaner reaches it, so in practice it
    /// is one line per cluster; a leadership change re-announces, which is
    /// informative. Per-instance (`Scheduler` is not `Clone`).
    retention_announced: std::sync::Once,
    /// Wake source for push-dispatch, installed before `run()`. Taken (moved
    /// out) once when the loop starts. `None` means the loop polls as today.
    #[cfg(feature = "push-dispatch")]
    wake_source: Mutex<Option<wake::WakeSource>>,
    /// Earliest `scheduled_at` of any delayed job seen at enqueue time, so the
    /// push loop can arm a timer for the next delayed job rather than waking
    /// immediately. Milliseconds; `i64::MAX` means "nothing scheduled".
    #[cfg(feature = "push-dispatch")]
    next_scheduled_at: std::sync::atomic::AtomicI64,
}

/// Counters for tick-based scheduling of periodic maintenance tasks.
#[derive(Default)]
struct TickCounters {
    reap: u32,
    periodic: u32,
    cleanup: u32,
    dlq_auto_retry: u32,
}

impl Scheduler {
    /// Build a scheduler over `storage`, dispatching from `queues`, optionally
    /// scoped to a tenant `namespace`.
    pub fn new(
        storage: StorageBackend,
        queues: Vec<String>,
        config: SchedulerConfig,
        namespace: Option<String>,
    ) -> Self {
        let rate_limiter = RateLimiter::new(storage.clone());
        let dlq = DeadLetterQueue::new(storage.clone());
        let circuit_breaker = CircuitBreaker::new(storage.clone());

        Self {
            storage,
            rate_limiter,
            dlq,
            circuit_breaker,
            task_configs: HashMap::new(),
            queue_configs: HashMap::new(),
            codel_configs: HashMap::new(),
            codel_states: Mutex::new(HashMap::new()),
            dispatch_orders: HashMap::new(),
            queues,
            config,
            shutdown: Arc::new(Notify::new()),
            paused_cache: Mutex::new((HashSet::new(), Instant::now())),
            namespace,
            // Instance-unique by default so two un-configured schedulers sharing
            // storage can't both win the retention election (`try_lead` renews a
            // matching owner's lease). Bindings override this with the real
            // `worker_id` via `set_claim_owner`.
            claim_owner: format!("{}-{}", poller::SCHEDULER_CLAIM_OWNER, uuid::Uuid::now_v7()),
            in_flight: Mutex::new(InFlight::default()),
            dispatch_wake: Arc::new(Notify::new()),
            retention_announced: std::sync::Once::new(),
            #[cfg(feature = "push-dispatch")]
            wake_source: Mutex::new(None),
            #[cfg(feature = "push-dispatch")]
            next_scheduled_at: std::sync::atomic::AtomicI64::new(i64::MAX),
        }
    }

    /// The storage backend this scheduler runs on.
    pub fn storage(&self) -> &StorageBackend {
        &self.storage
    }

    /// Set the execution-claim owner to this process's `worker_id`. Bindings
    /// call this right after construction so dead-worker recovery can identify
    /// which worker owns each in-flight job. Must match the id passed to
    /// `register_worker`/`heartbeat`.
    pub fn set_claim_owner(&mut self, worker_id: String) {
        self.claim_owner = worker_id;
    }

    /// The execution-claim / retention-election owner for this scheduler.
    pub fn claim_owner(&self) -> &str {
        &self.claim_owner
    }

    /// Free in-flight slots remaining before the dispatch cap, or `None` when
    /// no cap is configured (unbounded dispatch — legacy behavior).
    fn in_flight_remaining(&self) -> Option<usize> {
        self.config.max_in_flight.map(|max| {
            let used = self
                .in_flight
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .len();
            max.saturating_sub(used)
        })
    }

    /// Record a job as in flight once it is handed to the worker channel.
    /// No-op when dispatch is unbounded.
    fn track_in_flight(&self, job_id: &str, task_name: &str) {
        if self.config.max_in_flight.is_some() {
            self.in_flight
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(job_id, task_name);
        }
    }

    /// Forget a job that was tracked but never reached the worker channel
    /// (dispatch rollback). Unlike [`Self::release_in_flight`] this never wakes
    /// the poller — the slot was returned by the same thread that took it, and
    /// the send just failed, so an immediate retry would only spin.
    fn untrack_in_flight(&self, job_id: &str) {
        if self.config.max_in_flight.is_some() {
            self.in_flight
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(job_id);
        }
    }

    /// How many of this task's jobs this worker currently has in flight.
    fn task_in_flight(&self, task_name: &str) -> usize {
        self.in_flight
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .count_for(task_name)
    }

    /// Take a token from this task's retry budget. `true` when the retry may go
    /// ahead — including when no budget is configured, which is the default.
    ///
    /// Keyed per namespace so tenants sharing a database cannot spend each
    /// other's budget. The bucket is created on first use, so nothing needs
    /// registering. A storage error must not strand the job, so it fails open:
    /// the per-job `max_retries` still bounds the retry.
    fn retry_budget_allows(&self, task_name: &str) -> bool {
        let Some(config) = self
            .task_configs
            .get(task_name)
            .and_then(|c| c.retry_budget.as_ref())
        else {
            return true;
        };
        let key = format!(
            "retry::{}::{}",
            self.namespace.as_deref().unwrap_or(""),
            task_name
        );
        match self.rate_limiter.try_acquire(&key, config) {
            Ok(allowed) => allowed,
            Err(e) => {
                error!("retry budget check failed for {task_name}: {e}");
                true
            }
        }
    }

    /// Release a job's in-flight slot when it finishes and wake the poller to
    /// refill. Only ids this scheduler dispatched are tracked, so results for
    /// recovered or foreign jobs (e.g. from maintenance) are a harmless no-op.
    fn release_in_flight(&self, job_id: &str) {
        let Some(max) = self.config.max_in_flight else {
            return;
        };
        let mut in_flight = self.in_flight.lock().unwrap_or_else(|p| p.into_inner());
        // Only wake the poller when the pool was saturated — that's the only time
        // it's parked on the cap. Below the cap it's already dispatching on its
        // normal cadence, so an extra wake per completion just adds needless
        // dequeue contention with everything else touching the database.
        let was_full = in_flight.len() >= max;
        let freed = in_flight.remove(job_id);
        drop(in_flight);
        if freed && was_full {
            self.dispatch_wake.notify_one();
        }
    }

    /// Handle that stops [`Scheduler::run`] when notified.
    pub fn shutdown_handle(&self) -> Arc<Notify> {
        self.shutdown.clone()
    }

    /// `true` once every job this scheduler dispatched has had its result
    /// handled — a "drain is done" signal for a binding's shutdown loop that
    /// does not depend on the result channel disconnecting (which can hinge on
    /// foreign-object lifetimes, e.g. a Python-held sender). `false` when
    /// dispatch is unbounded: nothing is tracked, so emptiness proves nothing.
    pub fn in_flight_settled(&self) -> bool {
        self.config.max_in_flight.is_some()
            && self
                .in_flight
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .len()
                == 0
    }

    /// Set the rate-limit/concurrency policy for a queue.
    pub fn register_queue_config(&mut self, queue_name: String, config: QueueConfig) {
        self.queue_configs.insert(queue_name, config);
    }

    /// Enable opt-in CoDel load shedding on a queue. Orthogonal to
    /// [`Scheduler::register_queue_config`] so the common (no-CoDel) path never pays for it.
    pub fn register_queue_codel(&mut self, queue_name: String, config: codel::CodelConfig) {
        self.codel_configs.insert(queue_name, config);
    }

    /// Set a queue's dispatch order. `Lifo` runs newest-first within a priority
    /// under overload; the default is `Fifo`. Priority always dominates.
    pub fn register_queue_dispatch_order(
        &mut self,
        queue_name: String,
        order: crate::storage::DispatchOrder,
    ) {
        self.dispatch_orders.insert(queue_name, order);
    }

    /// Set a task's resilience policy and register its circuit breaker, if any.
    pub fn register_task(&mut self, task_name: String, config: TaskConfig) {
        if let Some(ref cb_config) = config.circuit_breaker {
            if let Err(e) = self.circuit_breaker.register(&task_name, cb_config) {
                error!("failed to register circuit breaker for {task_name}: {e}");
            }
        }
        self.task_configs.insert(task_name, config);
    }

    /// Run the scheduler loop. Polls for ready jobs and dispatches them
    /// to the worker pool via the provided channel.
    ///
    /// Uses adaptive polling: starts at `poll_interval`, backs off
    /// exponentially (up to `max_interval`, 200ms) when no jobs are found,
    /// resets immediately when a job is dispatched. Each wake drains all ready
    /// work before sleeping again.
    pub async fn run(&self, job_tx: tokio::sync::mpsc::Sender<Job>) {
        // Push-dispatch: if a wake source was configured, run the event-driven
        // loop. Otherwise (and always when the feature is off) fall through to
        // the unchanged adaptive-poll loop below.
        #[cfg(feature = "push-dispatch")]
        if let Some(wake) = self.take_wake_source() {
            self.run_push(job_tx, wake).await;
            return;
        }

        let mut counters = TickCounters::default();
        let base_interval = self.config.poll_interval;
        let max_interval = Duration::from_millis(200);
        let mut current_interval = base_interval;

        loop {
            tokio::select! {
                _ = self.shutdown.notified() => break,
                // A finished job freed an in-flight slot — refill now instead of
                // waiting out the backoff, so the cap doesn't cost throughput.
                _ = self.dispatch_wake.notified() => {}
                _ = tokio::time::sleep(current_interval) => {}
            }

            // Drain all ready work in one wake rather than dispatching a single
            // batch and sleeping again. Otherwise throughput is hard-capped at
            // `batch_size / poll_interval` (with defaults, 1 job / 50ms = 20
            // jobs/sec) no matter how deep the backlog or how many workers are
            // idle. `tick_dispatch` stops on its own when there's nothing ready
            // or the worker channel back-pressures, so this can't spin. Mirrors
            // the drain already done by the push-dispatch loop.
            let mut dispatched = false;
            while self.tick_dispatch(&job_tx) {
                dispatched = true;
            }
            // Maintenance stays on the per-wake cadence (unchanged from before).
            let had_maintenance = self.tick_maintenance(&mut counters);
            if dispatched || had_maintenance {
                current_interval = base_interval;
            } else {
                current_interval = (current_interval * 2).min(max_interval);
            }
        }
    }

    /// Test helper: one dispatch round + maintenance, like the old combined
    /// tick. Production loops drain dispatch fully instead (see [`Scheduler::run`]).
    #[cfg(test)]
    fn tick(&self, job_tx: &tokio::sync::mpsc::Sender<Job>, counters: &mut TickCounters) -> bool {
        let dispatched = self.tick_dispatch(job_tx);
        let had_maintenance = self.tick_maintenance(counters);
        dispatched || had_maintenance
    }

    /// Run one dispatch round (batch or single). Returns true if a job was
    /// dispatched. Pulled out so the push loop can run dispatch independently
    /// of the maintenance cadence.
    fn tick_dispatch(&self, job_tx: &tokio::sync::mpsc::Sender<Job>) -> bool {
        let dispatch_result = if self.config.batch_size > 1 {
            self.try_dispatch_batch(job_tx)
        } else {
            self.try_dispatch(job_tx)
        };
        match dispatch_result {
            Ok(d) => d,
            Err(e) => {
                error!("scheduler error: {e}");
                false
            }
        }
    }

    /// Advance the tick counters and run any due maintenance (reap, periodic
    /// enqueue, cleanup). Returns true if periodic enqueue ran (work that
    /// should reset the poll backoff).
    fn tick_maintenance(&self, counters: &mut TickCounters) -> bool {
        let mut had_maintenance = false;

        counters.reap += 1;
        counters.periodic += 1;
        counters.cleanup += 1;

        if counters.reap.is_multiple_of(self.config.reap_interval) {
            if let Err(e) = self.reap_stale() {
                error!("reap error: {e}");
            }
        }

        if counters
            .periodic
            .is_multiple_of(self.config.periodic_check_interval)
        {
            match self.check_periodic() {
                Ok(()) => {
                    // Periodic tasks may have been enqueued — reset polling
                    // so the next tick picks them up quickly.
                    had_maintenance = true;
                }
                Err(e) => error!("periodic check error: {e}"),
            }
        }

        if counters
            .cleanup
            .is_multiple_of(self.config.cleanup_interval)
        {
            if let Err(e) = self.auto_cleanup() {
                error!("auto-cleanup error: {e}");
            }
        }

        counters.dlq_auto_retry += 1;
        if counters
            .dlq_auto_retry
            .is_multiple_of(self.config.dlq_auto_retry_interval)
        {
            if let Err(e) = self.auto_retry_dlq() {
                error!("dlq auto-retry error: {e}");
            }
        }

        had_maintenance
    }
}

#[cfg(feature = "push-dispatch")]
impl Scheduler {
    /// Fallback dispatch interval when push-dispatch is on. A missed wake can
    /// never strand a job longer than this.
    const PUSH_FALLBACK_INTERVAL: Duration = Duration::from_secs(2);

    /// Cadence of the dedicated maintenance ticker, decoupled from dispatch.
    const PUSH_MAINTENANCE_INTERVAL: Duration = Duration::from_millis(500);

    /// Switch this scheduler from polling to event-driven dispatch, using the
    /// wake source that matches its storage backend. Call before [`Self::run`],
    /// from inside a Tokio runtime context (the Postgres and Redis sources
    /// spawn a listener task).
    ///
    /// This is the entry point binding shells call for their `push_dispatch`
    /// option. It also exists — as a logged no-op — in builds without the
    /// `push-dispatch` feature, so a shell can offer the option unconditionally
    /// and degrade to polling.
    pub fn enable_push_dispatch(&self) {
        self.set_wake_source(wake::WakeSource::for_storage(&self.storage));
    }

    /// Install the wake source the push loop should consume. Call before
    /// `run()`. With no wake source installed, `run()` keeps polling.
    pub fn set_wake_source(&self, source: wake::WakeSource) {
        let mut guard = self.wake_source.lock().unwrap_or_else(|p| p.into_inner());
        *guard = Some(source);
    }

    /// Move the installed wake source out for the run loop. Returns `None` if
    /// none was installed (the loop then polls).
    fn take_wake_source(&self) -> Option<wake::WakeSource> {
        self.wake_source
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
    }

    /// Bridge a (re)scheduled job into the push loop: wake immediately if it
    /// is ready now, otherwise arm the delayed timer. Used by the retry and
    /// periodic-enqueue paths, which run on the scheduler and already hold a
    /// storage handle.
    pub(crate) fn signal_scheduled(&self, scheduled_at: i64) {
        use crate::storage::notify::StorageNotifier;
        if scheduled_at <= crate::job::now_millis() {
            match &self.storage {
                StorageBackend::Sqlite(s) => s.notify_job_ready("", scheduled_at),
                #[cfg(feature = "postgres")]
                StorageBackend::Postgres(s) => s.notify_job_ready("", scheduled_at),
                #[cfg(feature = "redis")]
                StorageBackend::Redis(s) => s.notify_job_ready("", scheduled_at),
            }
        } else {
            self.note_scheduled_at(scheduled_at);
        }
    }

    /// Record the earliest delayed-job schedule so the loop can arm a timer.
    /// Ready jobs (`scheduled_at <= now`) wake immediately and don't update
    /// this. Safe to call from any thread.
    pub fn note_scheduled_at(&self, scheduled_at: i64) {
        use std::sync::atomic::Ordering;
        let mut current = self.next_scheduled_at.load(Ordering::Relaxed);
        while scheduled_at < current {
            match self.next_scheduled_at.compare_exchange_weak(
                current,
                scheduled_at,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }

    /// Duration until the next delayed job is due, capped at the fallback
    /// interval. Resets the tracker once the time has passed so a stale value
    /// can't pin the timer low forever.
    fn next_delayed_timer(&self) -> Duration {
        use std::sync::atomic::Ordering;
        let next = self.next_scheduled_at.load(Ordering::Relaxed);
        if next == i64::MAX {
            return Self::PUSH_FALLBACK_INTERVAL;
        }
        let now = crate::job::now_millis();
        if next <= now {
            // The delayed job is due; clear the tracker and dispatch now.
            self.next_scheduled_at.store(i64::MAX, Ordering::Relaxed);
            return Duration::ZERO;
        }
        Duration::from_millis((next - now) as u64).min(Self::PUSH_FALLBACK_INTERVAL)
    }

    /// Event-driven dispatch loop. Dispatches on a wake signal, on the
    /// fallback timer, or when a delayed job comes due. Maintenance runs on
    /// its own ticker so its cadence is independent of dispatch.
    async fn run_push(&self, job_tx: tokio::sync::mpsc::Sender<Job>, mut wake: wake::WakeSource) {
        let mut counters = TickCounters::default();
        let mut maintenance = tokio::time::interval(Self::PUSH_MAINTENANCE_INTERVAL);
        maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            let fallback = self.next_delayed_timer();
            tokio::select! {
                _ = self.shutdown.notified() => break,
                _ = wake.wait() => {
                    // Drain dispatch fully so a single wake clears the queue.
                    while self.tick_dispatch(&job_tx) {}
                }
                _ = tokio::time::sleep(fallback) => {
                    while self.tick_dispatch(&job_tx) {}
                }
                _ = maintenance.tick() => {
                    if self.tick_maintenance(&mut counters) {
                        // Periodic enqueue may have produced ready work.
                        while self.tick_dispatch(&job_tx) {}
                    }
                }
            }
        }
    }
}

#[cfg(not(feature = "push-dispatch"))]
impl Scheduler {
    /// Stand-in for the push-dispatch build's method, so binding shells can
    /// expose the option without compiling it in conditionally. Event-driven
    /// dispatch is not built here, so this logs and leaves the scheduler
    /// polling — the documented "accepted and ignored" contract.
    pub fn enable_push_dispatch(&self) {
        log::debug!(
            "push dispatch requested but this build lacks the 'push-dispatch' \
             feature; falling back to polling"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::{now_millis, JobStatus, NewJob};
    use crate::resilience::rate_limiter::RateLimitConfig;
    use crate::storage::records::NewPeriodicTask;
    use crate::storage::Storage;

    fn test_scheduler() -> Scheduler {
        let storage =
            StorageBackend::Sqlite(crate::storage::sqlite::SqliteStorage::in_memory().unwrap());
        Scheduler::new(
            storage,
            vec!["default".to_string()],
            SchedulerConfig::default(),
            None,
        )
    }

    fn make_job(task_name: &str) -> NewJob {
        NewJob {
            queue: "default".to_string(),
            task_name: task_name.to_string(),
            payload: vec![1, 2, 3],
            priority: 0,
            scheduled_at: now_millis(),
            max_retries: 3,
            timeout_ms: 300_000,
            unique_key: None,
            metadata: None,
            notes: None,
            depends_on: vec![],
            expires_at: None,
            result_ttl_ms: None,
            namespace: None,
        }
    }

    fn make_channel(
        cap: usize,
    ) -> (
        tokio::sync::mpsc::Sender<Job>,
        tokio::sync::mpsc::Receiver<Job>,
    ) {
        tokio::sync::mpsc::channel(cap)
    }

    /// Enqueue a job and dequeue it so it's in Running state.
    fn enqueue_and_run(scheduler: &Scheduler, task_name: &str) -> Job {
        let job = scheduler.storage.enqueue(make_job(task_name)).unwrap();
        let (tx, _rx) = make_channel(16);
        let mut counters = TickCounters::default();
        scheduler.tick(&tx, &mut counters);
        scheduler.storage.get_job(&job.id, None).unwrap().unwrap()
    }

    #[test]
    fn test_handle_success() {
        let scheduler = test_scheduler();
        let job = enqueue_and_run(&scheduler, "success_task");
        assert_eq!(job.status, JobStatus::Running);

        scheduler
            .handle_result(JobResult::Success {
                job_id: job.id.clone(),
                result: Some(vec![42]),
                task_name: "success_task".to_string(),
                wall_time_ns: 1_000_000,
            })
            .unwrap();

        let completed = scheduler.storage.get_job(&job.id, None).unwrap().unwrap();
        assert_eq!(completed.status, JobStatus::Complete);
        assert_eq!(completed.result, Some(vec![42]));
    }

    #[test]
    fn test_outcomes_carry_measured_wall_time() {
        // Shells report a job's duration off the outcome, so the worker's
        // measurement has to survive handle_result — on every variant, and on
        // the batch path that finalizes successes together.
        let scheduler = test_scheduler();

        let ok = enqueue_and_run(&scheduler, "timed_ok");
        let outcome = scheduler
            .handle_result(JobResult::Success {
                job_id: ok.id.clone(),
                result: None,
                task_name: "timed_ok".to_string(),
                wall_time_ns: 7_000_000,
            })
            .unwrap();
        assert!(matches!(
            outcome,
            ResultOutcome::Success {
                wall_time_ns: 7_000_000,
                ..
            }
        ));

        let retried = enqueue_and_run(&scheduler, "timed_retry");
        let outcome = scheduler
            .handle_result(JobResult::Failure {
                job_id: retried.id.clone(),
                error: "boom".to_string(),
                retry_count: 0,
                max_retries: 3,
                task_name: "timed_retry".to_string(),
                wall_time_ns: 3_500_000,
                should_retry: true,
                timed_out: false,
            })
            .unwrap();
        assert!(matches!(
            outcome,
            ResultOutcome::Retry {
                wall_time_ns: 3_500_000,
                ..
            }
        ));

        let dead = enqueue_and_run(&scheduler, "timed_dead");
        let outcome = scheduler
            .handle_result(JobResult::Failure {
                job_id: dead.id.clone(),
                error: "fatal".to_string(),
                retry_count: 0,
                max_retries: 0,
                task_name: "timed_dead".to_string(),
                wall_time_ns: 2_000_000,
                should_retry: false,
                timed_out: false,
            })
            .unwrap();
        assert!(matches!(
            outcome,
            ResultOutcome::DeadLettered {
                wall_time_ns: 2_000_000,
                ..
            }
        ));

        let cancelled = enqueue_and_run(&scheduler, "timed_cancel");
        let outcome = scheduler
            .handle_result(JobResult::Cancelled {
                job_id: cancelled.id.clone(),
                task_name: "timed_cancel".to_string(),
                wall_time_ns: 1_250_000,
            })
            .unwrap();
        assert!(matches!(
            outcome,
            ResultOutcome::Cancelled {
                wall_time_ns: 1_250_000,
                ..
            }
        ));

        let batched = enqueue_and_run(&scheduler, "timed_batch");
        let outcomes = scheduler.handle_results(vec![JobResult::Success {
            job_id: batched.id.clone(),
            result: None,
            task_name: "timed_batch".to_string(),
            wall_time_ns: 9_000_000,
        }]);
        assert!(matches!(
            outcomes[0].as_ref().unwrap(),
            ResultOutcome::Success {
                wall_time_ns: 9_000_000,
                ..
            }
        ));
    }

    #[test]
    fn test_unmeasured_result_reports_zero_wall_time() {
        // A job that failed before it ever ran has no duration to report; 0 is
        // the contract's "not measured", which shells surface as null.
        let scheduler = test_scheduler();
        let job = enqueue_and_run(&scheduler, "never_ran");
        let outcome = scheduler
            .handle_result(JobResult::Failure {
                job_id: job.id.clone(),
                error: "async submit error".to_string(),
                retry_count: 0,
                max_retries: 3,
                task_name: "never_ran".to_string(),
                wall_time_ns: 0,
                should_retry: true,
                timed_out: false,
            })
            .unwrap();
        assert!(matches!(
            outcome,
            ResultOutcome::Retry {
                wall_time_ns: 0,
                ..
            }
        ));
    }

    #[test]
    fn test_handle_results_batches_successes_in_order() {
        // S10: a drained batch of results yields one outcome per input, in
        // order. Successes are finalized together (complete_batch); a failure
        // interleaved among them still takes the per-result DLQ path.
        let scheduler = test_scheduler();
        let s0 = enqueue_and_run(&scheduler, "s0");
        let f0 = enqueue_and_run(&scheduler, "f0");
        let s1 = enqueue_and_run(&scheduler, "s1");

        let mk_success = |id: &str, task: &str| JobResult::Success {
            job_id: id.to_string(),
            result: Some(vec![1]),
            task_name: task.to_string(),
            wall_time_ns: 1,
        };
        let results = vec![
            mk_success(&s0.id, "s0"),
            JobResult::Failure {
                job_id: f0.id.clone(),
                error: "boom".to_string(),
                retry_count: 0,
                max_retries: 0,
                task_name: "f0".to_string(),
                wall_time_ns: 1,
                should_retry: false,
                timed_out: false,
            },
            mk_success(&s1.id, "s1"),
        ];

        let outcomes = scheduler.handle_results(results);
        assert_eq!(outcomes.len(), 3);
        assert!(matches!(
            outcomes[0].as_ref().unwrap(),
            ResultOutcome::Success { .. }
        ));
        assert!(matches!(
            outcomes[1].as_ref().unwrap(),
            ResultOutcome::DeadLettered { .. }
        ));
        assert!(matches!(
            outcomes[2].as_ref().unwrap(),
            ResultOutcome::Success { .. }
        ));

        // Both successes are archived Complete; their claim rows are cleared.
        for job in [&s0, &s1] {
            let done = scheduler.storage.get_job(&job.id, None).unwrap().unwrap();
            assert_eq!(done.status, JobStatus::Complete);
        }
        let claims = scheduler
            .storage
            .list_claims_by_worker(scheduler.claim_owner())
            .unwrap();
        assert!(!claims.contains(&s0.id) && !claims.contains(&s1.id));
    }

    #[test]
    fn test_handle_failure_with_retry() {
        let mut scheduler = test_scheduler();
        scheduler.register_task(
            "retry_task".to_string(),
            TaskConfig {
                retry_policy: RetryPolicy {
                    max_retries: 3,
                    base_delay_ms: 100,
                    max_delay_ms: 1000,
                    custom_delays_ms: None,
                },
                rate_limit: None,
                circuit_breaker: None,
                retry_budget: None,
                max_concurrent: None,
                max_in_flight_per_task: None,
            },
        );

        let job = enqueue_and_run(&scheduler, "retry_task");

        scheduler
            .handle_result(JobResult::Failure {
                job_id: job.id.clone(),
                error: "transient error".to_string(),
                retry_count: 0,
                max_retries: 3,
                task_name: "retry_task".to_string(),
                wall_time_ns: 500_000,
                should_retry: true,
                timed_out: false,
            })
            .unwrap();

        let retried = scheduler.storage.get_job(&job.id, None).unwrap().unwrap();
        assert_eq!(retried.status, JobStatus::Pending);
        assert_eq!(retried.retry_count, 1);
    }

    #[test]
    fn test_explicit_zero_max_retries_no_retry() {
        // The task policy would retry 3×, but an explicit per-job max_retries=0
        // (at-most-once) must override it and go straight to the DLQ.
        let mut scheduler = test_scheduler();
        scheduler.register_task(
            "noretry_task".to_string(),
            TaskConfig {
                retry_policy: RetryPolicy {
                    max_retries: 3,
                    base_delay_ms: 100,
                    max_delay_ms: 1000,
                    custom_delays_ms: None,
                },
                rate_limit: None,
                circuit_breaker: None,
                retry_budget: None,
                max_concurrent: None,
                max_in_flight_per_task: None,
            },
        );

        let job = enqueue_and_run(&scheduler, "noretry_task");

        let outcome = scheduler
            .handle_result(JobResult::Failure {
                job_id: job.id.clone(),
                error: "boom".to_string(),
                retry_count: 0,
                max_retries: 0,
                task_name: "noretry_task".to_string(),
                wall_time_ns: 500_000,
                should_retry: true,
                timed_out: false,
            })
            .unwrap();

        assert!(matches!(outcome, ResultOutcome::DeadLettered { .. }));
        let dead = scheduler.storage.list_dead(10, 0, None).unwrap();
        assert!(dead.iter().any(|d| d.original_job_id == job.id));
    }

    #[test]
    fn test_handle_failure_exhausted() {
        let scheduler = test_scheduler();
        let job = enqueue_and_run(&scheduler, "exhausted_task");

        scheduler
            .handle_result(JobResult::Failure {
                job_id: job.id.clone(),
                error: "fatal".to_string(),
                retry_count: 3,
                max_retries: 3,
                task_name: "exhausted_task".to_string(),
                wall_time_ns: 100,
                should_retry: true,
                timed_out: false,
            })
            .unwrap();

        let dead = scheduler.storage.get_job(&job.id, None).unwrap().unwrap();
        assert_eq!(dead.status, JobStatus::Dead);

        let dlq = scheduler.storage.list_dead(10, 0, None).unwrap();
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq[0].original_job_id, job.id);
    }

    #[test]
    fn test_handle_failure_no_retry() {
        let scheduler = test_scheduler();
        let job = enqueue_and_run(&scheduler, "no_retry_task");

        scheduler
            .handle_result(JobResult::Failure {
                job_id: job.id.clone(),
                error: "non-retryable".to_string(),
                retry_count: 0,
                max_retries: 3,
                task_name: "no_retry_task".to_string(),
                wall_time_ns: 100,
                should_retry: false,
                timed_out: false,
            })
            .unwrap();

        let dead = scheduler.storage.get_job(&job.id, None).unwrap().unwrap();
        assert_eq!(dead.status, JobStatus::Dead);
    }

    #[test]
    fn test_handle_cancelled() {
        let scheduler = test_scheduler();
        let job = enqueue_and_run(&scheduler, "cancel_task");

        scheduler
            .handle_result(JobResult::Cancelled {
                job_id: job.id.clone(),
                task_name: "cancel_task".to_string(),
                wall_time_ns: 100,
            })
            .unwrap();

        let cancelled = scheduler.storage.get_job(&job.id, None).unwrap().unwrap();
        assert_eq!(cancelled.status, JobStatus::Cancelled);
    }

    #[test]
    fn test_try_dispatch_rate_limited() {
        let mut scheduler = test_scheduler();
        scheduler.register_task(
            "rl_task".to_string(),
            TaskConfig {
                retry_policy: RetryPolicy::default(),
                rate_limit: Some(RateLimitConfig {
                    max_tokens: 1.0,
                    refill_rate: 0.0,
                }),
                circuit_breaker: None,
                retry_budget: None,
                max_concurrent: None,
                max_in_flight_per_task: None,
            },
        );

        // Enqueue two jobs
        scheduler.storage.enqueue(make_job("rl_task")).unwrap();
        scheduler.storage.enqueue(make_job("rl_task")).unwrap();

        let (tx, mut rx) = make_channel(16);
        let mut counters = TickCounters::default();

        // First tick should dispatch (consumes the one token)
        scheduler.tick(&tx, &mut counters);
        assert!(rx.try_recv().is_ok());

        // Second tick: job dequeued but rate limited, rescheduled
        scheduler.tick(&tx, &mut counters);
        assert!(rx.try_recv().is_err()); // nothing new dispatched

        // The second job should be back in pending with a future scheduled_at
        let jobs = scheduler
            .storage
            .list_jobs(Some(JobStatus::Pending as i32), None, None, 10, 0, None)
            .unwrap();
        assert_eq!(jobs.len(), 1);
        assert!(jobs[0].scheduled_at > now_millis());
    }

    #[test]
    fn test_try_dispatch_circuit_broken() {
        let mut scheduler = test_scheduler();
        let cb_config = crate::resilience::circuit_breaker::CircuitBreakerConfig {
            threshold: 1,
            window_ms: 60_000,
            cooldown_ms: 300_000,
            half_open_max_probes: 5,
            half_open_success_rate: 0.8,
        };
        scheduler.register_task(
            "cb_task".to_string(),
            TaskConfig {
                retry_policy: RetryPolicy::default(),
                rate_limit: None,
                circuit_breaker: Some(cb_config),
                retry_budget: None,
                max_concurrent: None,
                max_in_flight_per_task: None,
            },
        );

        // Trip the circuit breaker
        scheduler.circuit_breaker.record_failure("cb_task").unwrap();

        scheduler.storage.enqueue(make_job("cb_task")).unwrap();

        let (tx, mut rx) = make_channel(16);
        let mut counters = TickCounters::default();
        scheduler.tick(&tx, &mut counters);

        // Job should not be dispatched (circuit is open)
        assert!(rx.try_recv().is_err());

        // Job should be rescheduled
        let jobs = scheduler
            .storage
            .list_jobs(Some(JobStatus::Pending as i32), None, None, 10, 0, None)
            .unwrap();
        assert_eq!(jobs.len(), 1);
        assert!(jobs[0].scheduled_at > now_millis());
    }

    #[test]
    fn test_try_dispatch_per_task_concurrency_allows_exactly_max() {
        // Regression: with `max_concurrent = 2` we expect exactly 2 jobs to
        // be dispatched, not `max - 1`. The `dequeue_from` step transitions
        // status to `Running` before the cap check, so the count includes
        // the just-dequeued job — the gate must use `>` rather than `>=`.
        let mut scheduler = test_scheduler();
        scheduler.register_task(
            "conc_task".to_string(),
            TaskConfig {
                retry_policy: RetryPolicy::default(),
                rate_limit: None,
                circuit_breaker: None,
                retry_budget: None,
                max_concurrent: Some(2),
                max_in_flight_per_task: None,
            },
        );

        for _ in 0..3 {
            scheduler.storage.enqueue(make_job("conc_task")).unwrap();
        }

        let (tx, mut rx) = make_channel(16);
        let mut counters = TickCounters::default();

        scheduler.tick(&tx, &mut counters);
        scheduler.tick(&tx, &mut counters);
        scheduler.tick(&tx, &mut counters);

        let mut dispatched = 0;
        while rx.try_recv().is_ok() {
            dispatched += 1;
        }
        assert_eq!(
            dispatched, 2,
            "expected exactly max_concurrent jobs dispatched"
        );

        // The rejected job should be back to Pending with a future schedule
        // and its execution-claim row cleared.
        let pending = scheduler
            .storage
            .list_jobs(Some(JobStatus::Pending as i32), None, None, 10, 0, None)
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert!(pending[0].scheduled_at > now_millis());

        let claims = scheduler
            .storage
            .list_claims_by_worker(scheduler.claim_owner())
            .unwrap();
        assert_eq!(
            claims.len(),
            2,
            "rejected job's claim row should have been rolled back"
        );
        assert!(
            !claims.contains(&pending[0].id),
            "rejected job should not have a stale execution claim"
        );
    }

    #[test]
    fn test_batch_dispatch_respects_per_task_concurrency() {
        // S09: the batch path claims the whole batch in one round-trip, then
        // enforces the per-task cap with a per-batch running-count cache. With
        // `max_concurrent = 2` and a 5-job batch exactly two dispatch; the other
        // three roll back to Pending with their claim rows cleared — proving the
        // cache decrements as jobs are rejected instead of re-querying per job.
        let storage =
            StorageBackend::Sqlite(crate::storage::sqlite::SqliteStorage::in_memory().unwrap());
        let config = SchedulerConfig {
            batch_size: 8,
            ..SchedulerConfig::default()
        };
        let mut scheduler = Scheduler::new(storage, vec!["default".to_string()], config, None);
        scheduler.register_task(
            "batch_conc".to_string(),
            TaskConfig {
                retry_policy: RetryPolicy::default(),
                rate_limit: None,
                circuit_breaker: None,
                retry_budget: None,
                max_concurrent: Some(2),
                max_in_flight_per_task: None,
            },
        );

        for _ in 0..5 {
            scheduler.storage.enqueue(make_job("batch_conc")).unwrap();
        }

        let (tx, mut rx) = make_channel(16);
        let mut counters = TickCounters::default();
        scheduler.tick(&tx, &mut counters);

        let mut dispatched = 0;
        while rx.try_recv().is_ok() {
            dispatched += 1;
        }
        assert_eq!(dispatched, 2, "batch path must honor max_concurrent");

        let pending = scheduler
            .storage
            .list_jobs(Some(JobStatus::Pending as i32), None, None, 10, 0, None)
            .unwrap();
        assert_eq!(pending.len(), 3, "over-cap jobs must return to Pending");

        let claims = scheduler
            .storage
            .list_claims_by_worker(scheduler.claim_owner())
            .unwrap();
        assert_eq!(
            claims.len(),
            2,
            "rolled-back jobs must not keep a stale claim row"
        );
    }

    #[test]
    fn test_max_in_flight_bounds_dispatch_and_refills_on_completion() {
        // A scheduler capped at one in-flight job must not drain a deeper
        // backlog into its own channel (which would strand the surplus Running
        // and starve peers). Finishing a job frees the slot for exactly one more.
        let storage =
            StorageBackend::Sqlite(crate::storage::sqlite::SqliteStorage::in_memory().unwrap());
        let config = SchedulerConfig {
            max_in_flight: Some(1),
            ..SchedulerConfig::default()
        };
        let scheduler = Scheduler::new(storage, vec!["default".to_string()], config, None);
        for _ in 0..3 {
            scheduler.storage.enqueue(make_job("capped")).unwrap();
        }

        let (tx, mut rx) = make_channel(16);

        // Draining stops at the cap even though three jobs are ready.
        let mut dispatched = 0;
        while scheduler.tick_dispatch(&tx) {
            dispatched += 1;
        }
        assert_eq!(dispatched, 1, "in-flight cap limits the drain to one job");
        let job1 = rx.try_recv().expect("one job dispatched");
        assert!(
            rx.try_recv().is_err(),
            "no second job dispatched past the cap"
        );

        // Finishing the job frees its slot; the next drain admits exactly one more.
        scheduler
            .handle_result(JobResult::Success {
                job_id: job1.id.clone(),
                result: None,
                task_name: "capped".to_string(),
                wall_time_ns: 1,
            })
            .unwrap();

        let mut dispatched_after = 0;
        while scheduler.tick_dispatch(&tx) {
            dispatched_after += 1;
        }
        assert_eq!(
            dispatched_after, 1,
            "a freed slot admits exactly one more job"
        );
    }

    #[test]
    fn test_full_channel_rollback_does_not_leak_in_flight_slot() {
        // A job that fails the channel hand-off must not keep its in-flight
        // slot: a leaked entry shrinks the dispatch cap for the life of the
        // worker and keeps the drain-exit signal from ever settling.
        let storage =
            StorageBackend::Sqlite(crate::storage::sqlite::SqliteStorage::in_memory().unwrap());
        let config = SchedulerConfig {
            max_in_flight: Some(8),
            ..SchedulerConfig::default()
        };
        let scheduler = Scheduler::new(storage, vec!["default".to_string()], config, None);
        scheduler.storage.enqueue(make_job("full_chan")).unwrap();

        // Capacity-1 channel pre-filled with a sentinel so the hand-off fails.
        let (tx, _rx) = make_channel(1);
        let sentinel = scheduler.storage.enqueue(make_job("sentinel")).unwrap();
        tx.try_send(sentinel).expect("pre-fill should succeed");

        assert!(!scheduler.tick_dispatch(&tx), "hand-off must not succeed");
        assert_eq!(
            scheduler.in_flight_remaining(),
            Some(8),
            "failed hand-off must not occupy an in-flight slot"
        );
    }

    #[test]
    fn test_closed_channel_rollback_untracks_in_flight() {
        // A job tracked before a failed hand-off must be untracked by the
        // rollback, or the slot leaks and `in_flight_settled` can never turn
        // true again — every later shutdown would wait out the drain timeout.
        let storage =
            StorageBackend::Sqlite(crate::storage::sqlite::SqliteStorage::in_memory().unwrap());
        let config = SchedulerConfig {
            max_in_flight: Some(8),
            ..SchedulerConfig::default()
        };
        let scheduler = Scheduler::new(storage, vec!["default".to_string()], config, None);
        scheduler.storage.enqueue(make_job("closed_chan")).unwrap();

        let (tx, rx) = make_channel(16);
        drop(rx);
        assert!(
            !scheduler.tick_dispatch(&tx),
            "closed channel must not count as a dispatch"
        );

        assert_eq!(
            scheduler.in_flight_remaining(),
            Some(8),
            "rolled-back job must not occupy an in-flight slot"
        );
        let pending = scheduler
            .storage
            .list_jobs(Some(JobStatus::Pending as i32), None, None, 10, 0, None)
            .unwrap();
        assert_eq!(
            pending.len(),
            1,
            "job returned to Pending for another worker"
        );
    }

    #[test]
    fn test_in_flight_settled_tracks_dispatch_lifecycle() {
        // Bounded dispatch: settled when idle, unsettled while a job is out,
        // settled again once its result is handled. Unbounded dispatch tracks
        // nothing, so it must never report settled.
        let storage =
            StorageBackend::Sqlite(crate::storage::sqlite::SqliteStorage::in_memory().unwrap());
        let config = SchedulerConfig {
            max_in_flight: Some(4),
            ..SchedulerConfig::default()
        };
        let scheduler = Scheduler::new(storage, vec!["default".to_string()], config, None);
        assert!(
            scheduler.in_flight_settled(),
            "idle bounded scheduler is settled"
        );

        scheduler.storage.enqueue(make_job("settle")).unwrap();
        let (tx, mut rx) = make_channel(16);
        while scheduler.tick_dispatch(&tx) {}
        let job = rx.try_recv().expect("job dispatched");
        assert!(
            !scheduler.in_flight_settled(),
            "a dispatched job keeps the scheduler unsettled"
        );

        scheduler
            .handle_result(JobResult::Success {
                job_id: job.id.clone(),
                result: None,
                task_name: "settle".to_string(),
                wall_time_ns: 1,
            })
            .unwrap();
        assert!(
            scheduler.in_flight_settled(),
            "handled result settles the scheduler"
        );

        let unbounded = Scheduler::new(
            StorageBackend::Sqlite(crate::storage::sqlite::SqliteStorage::in_memory().unwrap()),
            vec!["default".to_string()],
            SchedulerConfig::default(),
            None,
        );
        assert!(
            !unbounded.in_flight_settled(),
            "unbounded dispatch can never prove it settled"
        );
    }

    /// Scheduler with an in-flight cap and a per-task bulkhead on `task_name`.
    fn bulkhead_scheduler(max_in_flight: usize, task_name: &str, per_task: usize) -> Scheduler {
        let storage =
            StorageBackend::Sqlite(crate::storage::sqlite::SqliteStorage::in_memory().unwrap());
        let config = SchedulerConfig {
            max_in_flight: Some(max_in_flight),
            ..SchedulerConfig::default()
        };
        let mut scheduler = Scheduler::new(storage, vec!["default".to_string()], config, None);
        scheduler.register_task(
            task_name.to_string(),
            TaskConfig {
                retry_policy: RetryPolicy::default(),
                rate_limit: None,
                circuit_breaker: None,
                retry_budget: None,
                max_concurrent: None,
                max_in_flight_per_task: Some(per_task),
            },
        );
        scheduler
    }

    #[test]
    fn test_per_task_in_flight_cap_rejects_and_reschedules() {
        // S19: a task capped at one in-flight job must not take a second slot
        // even though the pool has room. The surplus rolls back to Pending with
        // its claim cleared, rather than stranding Running.
        let scheduler = bulkhead_scheduler(8, "hog", 1);
        for _ in 0..3 {
            scheduler.storage.enqueue(make_job("hog")).unwrap();
        }

        let (tx, mut rx) = make_channel(16);
        while scheduler.tick_dispatch(&tx) {}

        let mut dispatched = 0;
        while rx.try_recv().is_ok() {
            dispatched += 1;
        }
        assert_eq!(dispatched, 1, "per-task bulkhead limits dispatch to one");

        let pending = scheduler
            .storage
            .list_jobs(Some(JobStatus::Pending as i32), None, None, 10, 0, None)
            .unwrap();
        assert_eq!(pending.len(), 2, "over-cap jobs must return to Pending");

        let claims = scheduler
            .storage
            .list_claims_by_worker(scheduler.claim_owner())
            .unwrap();
        assert_eq!(
            claims.len(),
            1,
            "rejected jobs must not keep a stale claim row"
        );
    }

    #[test]
    fn test_per_task_cap_does_not_block_other_tasks() {
        // S19's whole point: the bulkhead is scoped to the task that tripped it.
        // A saturated `hog` must not consume slots that `free` could use — the
        // starvation a global semaphore (or a blocking per-task acquire on the
        // dispatch loop) would cause.
        let scheduler = bulkhead_scheduler(8, "hog", 1);
        let (tx, mut rx) = make_channel(16);

        scheduler.storage.enqueue(make_job("hog")).unwrap();
        while scheduler.tick_dispatch(&tx) {}
        assert_eq!(scheduler.task_in_flight("hog"), 1, "hog is at its bulkhead");

        // With hog saturated, an unrelated task still dispatches up to the pool cap.
        for _ in 0..3 {
            scheduler.storage.enqueue(make_job("free")).unwrap();
        }
        while scheduler.tick_dispatch(&tx) {}

        let mut hog = 0;
        let mut free = 0;
        while let Ok(job) = rx.try_recv() {
            match job.task_name.as_str() {
                "hog" => hog += 1,
                "free" => free += 1,
                other => panic!("unexpected task {other}"),
            }
        }
        assert_eq!(hog, 1, "capped task is held to its bulkhead");
        assert_eq!(free, 3, "uncapped task dispatches past the saturated one");
    }

    #[test]
    fn test_release_in_flight_frees_per_task_slot() {
        // The per-task count must decrement as jobs finish, or the bulkhead would
        // leak slots and wedge the task at its cap forever.
        let scheduler = bulkhead_scheduler(8, "hog", 1);
        let (tx, mut rx) = make_channel(16);

        scheduler.storage.enqueue(make_job("hog")).unwrap();
        while scheduler.tick_dispatch(&tx) {}
        let first = rx.try_recv().expect("one job dispatched");
        assert_eq!(scheduler.task_in_flight("hog"), 1);

        scheduler
            .handle_result(JobResult::Success {
                job_id: first.id.clone(),
                result: None,
                task_name: "hog".to_string(),
                wall_time_ns: 1,
            })
            .unwrap();
        assert_eq!(
            scheduler.task_in_flight("hog"),
            0,
            "slot released on finish"
        );

        // The freed slot admits the next job of the same task.
        scheduler.storage.enqueue(make_job("hog")).unwrap();
        while scheduler.tick_dispatch(&tx) {}
        assert!(rx.try_recv().is_ok(), "freed slot admits the next job");
        assert_eq!(scheduler.task_in_flight("hog"), 1);
    }

    #[test]
    fn test_try_dispatch_per_task_max_one_dispatches_one() {
        // Regression: `max_concurrent = 1` must allow exactly one job to
        // run. With the pre-fix `>=` check the running-count (which already
        // includes the just-dequeued job) tripped the gate and the job was
        // rescheduled — effectively `max_concurrent = 0`.
        let mut scheduler = test_scheduler();
        scheduler.register_task(
            "single_task".to_string(),
            TaskConfig {
                retry_policy: RetryPolicy::default(),
                rate_limit: None,
                circuit_breaker: None,
                retry_budget: None,
                max_concurrent: Some(1),
                max_in_flight_per_task: None,
            },
        );

        scheduler.storage.enqueue(make_job("single_task")).unwrap();

        let (tx, mut rx) = make_channel(16);
        let mut counters = TickCounters::default();
        scheduler.tick(&tx, &mut counters);

        assert!(
            rx.try_recv().is_ok(),
            "the single allowed concurrent job must dispatch"
        );
    }

    #[test]
    fn test_dispatch_drains_all_ready_jobs_in_one_wake() {
        // Regression (S01): each poll wake drains all ready work. Before, one
        // tick dispatched a single batch (batch_size=1 → one job) then slept,
        // capping throughput at batch_size / poll_interval regardless of how
        // deep the backlog was. `run` now drains until empty per wake.
        let scheduler = test_scheduler(); // batch_size default = 1
        for _ in 0..5 {
            scheduler.storage.enqueue(make_job("bulk")).unwrap();
        }
        let (tx, mut rx) = make_channel(16);

        // A single dispatch round moves exactly one job (batch_size = 1)...
        assert!(scheduler.tick_dispatch(&tx));
        // ...but draining until empty (what `run` does) clears the rest in the
        // same wake instead of one-per-sleep.
        let mut drained = 1;
        while scheduler.tick_dispatch(&tx) {
            drained += 1;
        }
        assert_eq!(
            drained, 5,
            "drain loop must dispatch every ready job in one wake"
        );

        let mut received = 0;
        while rx.try_recv().is_ok() {
            received += 1;
        }
        assert_eq!(received, 5);
    }

    #[test]
    fn test_try_dispatch_per_queue_concurrency_allows_exactly_max() {
        // Same regression for the queue-level cap.
        let mut scheduler = test_scheduler();
        scheduler.register_queue_config(
            "default".to_string(),
            QueueConfig {
                rate_limit: None,
                max_concurrent: Some(2),
            },
        );

        for _ in 0..3 {
            scheduler.storage.enqueue(make_job("queue_capped")).unwrap();
        }

        let (tx, mut rx) = make_channel(16);
        let mut counters = TickCounters::default();
        scheduler.tick(&tx, &mut counters);
        scheduler.tick(&tx, &mut counters);
        scheduler.tick(&tx, &mut counters);

        let mut dispatched = 0;
        while rx.try_recv().is_ok() {
            dispatched += 1;
        }
        assert_eq!(dispatched, 2);

        let pending = scheduler
            .storage
            .list_jobs(Some(JobStatus::Pending as i32), None, None, 10, 0, None)
            .unwrap();
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn test_codel_sheds_stale_job_to_dlq() {
        // Drive the real dispatch shed path with controlled time. CoDel arms on
        // the first stale candidate, then sheds a later one once the sojourn has
        // stayed above target for a full interval — dead-lettering it with a
        // reserved `codel:` reason (never dropping during a transient spike).
        let mut scheduler = test_scheduler();
        scheduler.register_queue_codel(
            "default".to_string(),
            codel::CodelConfig {
                target_ms: 100,
                interval_ms: 1000,
            },
        );
        let job = scheduler.storage.enqueue(make_job("stale")).unwrap();
        let base = job.scheduled_at;

        // First stale candidate (sojourn 500 > target 100): only arms, kept.
        let kept = scheduler
            .codel_admit(vec![job.clone()], base + 500)
            .unwrap();
        assert_eq!(kept.len(), 1, "the first stale job arms but is not shed");

        // A full interval later, still stale: shed to the DLQ.
        let kept = scheduler
            .codel_admit(vec![job.clone()], base + 500 + 1000)
            .unwrap();
        assert!(kept.is_empty(), "a sustained-overload job is shed");

        let dead = scheduler.storage.list_dead(10, 0, None).unwrap();
        assert!(
            dead.iter()
                .any(|d| d.error.as_deref().is_some_and(|e| e.starts_with("codel:"))),
            "shed job is dead-lettered with a codel: reason"
        );
    }

    #[test]
    fn test_codel_leaves_uncapped_queues_untouched() {
        // With no CoDel config, the admit filter is a no-op fast path.
        let scheduler = test_scheduler();
        let job = scheduler.storage.enqueue(make_job("fresh")).unwrap();
        let ancient = job.scheduled_at + 10_000_000;
        let kept = scheduler.codel_admit(vec![job], ancient).unwrap();
        assert_eq!(kept.len(), 1, "no codel config => nothing is ever shed");
    }

    #[test]
    fn test_try_dispatch_reschedules_on_closed_channel() {
        // Regression: when the worker channel is closed (worker pool
        // shutting down) the job has already been claimed in storage —
        // dropping it without rolling back leaves it in `Running` until
        // the stale-reaper times it out, which surfaces as a *timeout*
        // in metrics. The poller must clear the claim and reset the job
        // to `Pending` so the next dispatch attempt picks it up cleanly.
        let scheduler = test_scheduler();
        let job = scheduler
            .storage
            .enqueue(make_job("ch_closed_task"))
            .unwrap();

        let (tx, rx) = make_channel(16);
        drop(rx);

        let mut counters = TickCounters::default();
        scheduler.tick(&tx, &mut counters);

        let after = scheduler.storage.get_job(&job.id, None).unwrap().unwrap();
        assert_eq!(
            after.status,
            JobStatus::Pending,
            "job must be returned to Pending when dispatch fails"
        );
        assert!(after.scheduled_at > now_millis());

        let claims = scheduler
            .storage
            .list_claims_by_worker(scheduler.claim_owner())
            .unwrap();
        assert!(
            !claims.contains(&job.id),
            "execution claim must be cleared on dispatch failure"
        );
    }

    #[test]
    fn test_try_dispatch_skips_when_channel_full() {
        // When the channel is *full* rather than closed, the poller must not
        // dequeue at all: a claim taken now could only roll back on `try_send`,
        // churning storage for nothing. The job stays Pending, untouched, and
        // dispatch resumes once the pool drains.
        let scheduler = test_scheduler();
        let job = scheduler.storage.enqueue(make_job("ch_full_task")).unwrap();

        // Capacity-1 channel pre-filled with a sentinel job.
        let (tx, mut rx) = make_channel(1);
        let sentinel = scheduler.storage.enqueue(make_job("sentinel")).unwrap();
        tx.try_send(sentinel).expect("pre-fill should succeed");

        let mut counters = TickCounters::default();
        scheduler.tick(&tx, &mut counters);

        let after = scheduler.storage.get_job(&job.id, None).unwrap().unwrap();
        assert_eq!(after.status, JobStatus::Pending);
        assert_eq!(
            after.scheduled_at, job.scheduled_at,
            "a skipped job keeps its schedule — it was never touched"
        );

        let claims = scheduler
            .storage
            .list_claims_by_worker(scheduler.claim_owner())
            .unwrap();
        assert!(!claims.contains(&job.id));

        // Draining the channel restores dispatch.
        let _ = rx.try_recv().expect("sentinel was in the channel");
        assert!(
            scheduler.tick_dispatch(&tx),
            "freed capacity resumes dispatch"
        );
    }

    #[test]
    fn test_reap_stale_jobs() {
        let mut scheduler = test_scheduler();
        scheduler.register_task(
            "stale_task".to_string(),
            TaskConfig {
                retry_policy: RetryPolicy {
                    max_retries: 3,
                    base_delay_ms: 100,
                    max_delay_ms: 1000,
                    custom_delays_ms: None,
                },
                rate_limit: None,
                circuit_breaker: None,
                retry_budget: None,
                max_concurrent: None,
                max_in_flight_per_task: None,
            },
        );

        // Create a job with a very short timeout
        let mut new_job = make_job("stale_task");
        new_job.timeout_ms = 1; // 1ms timeout
        let job = scheduler.storage.enqueue(new_job).unwrap();

        // Dequeue it (sets it to Running with started_at)
        let (tx, _rx) = make_channel(16);
        let mut counters = TickCounters::default();
        scheduler.tick(&tx, &mut counters);

        // Wait for it to "time out"
        std::thread::sleep(Duration::from_millis(10));

        // Reap should find and handle it
        scheduler.reap_stale().unwrap();

        let reaped = scheduler.storage.get_job(&job.id, None).unwrap().unwrap();
        // It should be rescheduled for retry (retry_count < max_retries)
        assert_eq!(reaped.status, JobStatus::Pending);
        assert_eq!(reaped.retry_count, 1);
    }

    fn retry_task_config(max_retries: i32) -> TaskConfig {
        TaskConfig {
            retry_policy: RetryPolicy {
                max_retries,
                base_delay_ms: 100,
                max_delay_ms: 1000,
                custom_delays_ms: None,
            },
            rate_limit: None,
            circuit_breaker: None,
            retry_budget: None,
            max_concurrent: None,
            max_in_flight_per_task: None,
        }
    }

    /// A Running job whose claim owner is a dead worker (no live heartbeat) is
    /// requeued by `recover_orphaned_jobs` without waiting for its timeout.
    #[test]
    fn test_recover_orphaned_jobs_requeues_dead_worker_job() {
        let mut scheduler = test_scheduler();
        scheduler.set_claim_owner("survivor".to_string());
        scheduler.register_task("orphan_task".to_string(), retry_task_config(3));

        let job = scheduler.storage.enqueue(make_job("orphan_task")).unwrap();
        // Move to Running and record a claim owned by a worker that never
        // heartbeats (simulating a crashed worker). No workers are registered,
        // so the live set is just the survivor → this claim is orphaned.
        scheduler
            .storage
            .dequeue("default", now_millis() + 1000, None)
            .unwrap()
            .unwrap();
        assert!(scheduler
            .storage
            .claim_execution(&job.id, "dead-worker")
            .unwrap());

        scheduler.reap_stale().unwrap();

        let recovered = scheduler.storage.get_job(&job.id, None).unwrap().unwrap();
        assert_eq!(recovered.status, JobStatus::Pending);
        assert_eq!(recovered.retry_count, 1);
        // The orphaned claim was reclaimed then cleared on requeue.
        assert!(scheduler
            .storage
            .list_claims_by_worker("dead-worker")
            .unwrap()
            .is_empty());
    }

    /// A scheduler must never orphan its OWN in-flight jobs (self-rescue guard),
    /// even before its first heartbeat row exists.
    /// A task whose retries are capped at `budget` per minute.
    fn budgeted_scheduler(task_name: &str, budget: &str) -> Scheduler {
        let mut scheduler = test_scheduler();
        let mut config = retry_task_config(5);
        config.retry_budget = Some(RateLimitConfig::parse(budget).expect("valid rate"));
        scheduler.register_task(task_name.to_string(), config);
        scheduler
    }

    /// Fail `job_id` once and report what the scheduler decided.
    fn fail_once(scheduler: &Scheduler, job_id: &str, task_name: &str) -> ResultOutcome {
        scheduler
            .handle_result(JobResult::Failure {
                job_id: job_id.to_string(),
                error: "boom".to_string(),
                retry_count: 0,
                max_retries: 5,
                task_name: task_name.to_string(),
                wall_time_ns: 1,
                should_retry: true,
                timed_out: false,
            })
            .unwrap()
    }

    #[test]
    fn test_retry_budget_dead_letters_once_exhausted() {
        // S18: the per-job max_retries is not a rate cap, so a broken dependency
        // can retry forever across jobs. Two tokens, three failures: the third
        // must dead-letter instead of retrying.
        let scheduler = budgeted_scheduler("budgeted", "2/m");

        let mut outcomes = Vec::new();
        for _ in 0..3 {
            let job = scheduler.storage.enqueue(make_job("budgeted")).unwrap();
            outcomes.push(fail_once(&scheduler, &job.id, "budgeted"));
        }

        assert!(
            matches!(outcomes[0], ResultOutcome::Retry { .. }),
            "first failure is within budget"
        );
        assert!(
            matches!(outcomes[1], ResultOutcome::Retry { .. }),
            "second failure is within budget"
        );
        assert!(
            matches!(outcomes[2], ResultOutcome::DeadLettered { .. }),
            "third failure exceeds the budget and must dead-letter, got {:?}",
            outcomes[2]
        );
    }

    #[test]
    fn test_retry_budget_marks_why_it_dead_lettered() {
        // ResultOutcome can't say why, so the DLQ metadata is what distinguishes
        // a budget kill from ordinary retry exhaustion.
        let scheduler = budgeted_scheduler("marked", "1/m");
        for _ in 0..2 {
            let job = scheduler.storage.enqueue(make_job("marked")).unwrap();
            fail_once(&scheduler, &job.id, "marked");
        }

        let dead = scheduler.storage.list_dead(10, 0, None).unwrap();
        assert_eq!(dead.len(), 1);
        assert_eq!(
            dead[0].metadata.as_deref(),
            Some(super::result_handler::RETRY_BUDGET_EXHAUSTED)
        );
    }

    #[test]
    fn test_retry_budget_is_per_task() {
        // One task draining its budget must not stop another from retrying.
        let mut scheduler = budgeted_scheduler("greedy", "1/m");
        scheduler.register_task("innocent".to_string(), retry_task_config(5));

        for _ in 0..2 {
            let job = scheduler.storage.enqueue(make_job("greedy")).unwrap();
            fail_once(&scheduler, &job.id, "greedy");
        }

        let job = scheduler.storage.enqueue(make_job("innocent")).unwrap();
        let outcome = fail_once(&scheduler, &job.id, "innocent");
        assert!(
            matches!(outcome, ResultOutcome::Retry { .. }),
            "an unbudgeted task must be unaffected, got {outcome:?}"
        );
    }

    #[test]
    fn test_no_retry_budget_configured_never_blocks() {
        // The budget is opt-in; the default must keep retrying to max_retries.
        let mut scheduler = test_scheduler();
        scheduler.register_task("unbudgeted".to_string(), retry_task_config(5));
        for _ in 0..5 {
            let job = scheduler.storage.enqueue(make_job("unbudgeted")).unwrap();
            let outcome = fail_once(&scheduler, &job.id, "unbudgeted");
            assert!(matches!(outcome, ResultOutcome::Retry { .. }));
        }
    }

    #[test]
    fn test_retry_budget_not_spent_by_jobs_that_cannot_retry() {
        // A job at its retry ceiling was never going to retry, so it must not
        // take a token — otherwise it drains the budget for its siblings.
        let scheduler = budgeted_scheduler("ceiling", "1/m");

        let exhausted = scheduler.storage.enqueue(make_job("ceiling")).unwrap();
        let outcome = scheduler
            .handle_result(JobResult::Failure {
                job_id: exhausted.id.clone(),
                error: "boom".to_string(),
                retry_count: 3,
                max_retries: 3, // already at the ceiling
                task_name: "ceiling".to_string(),
                wall_time_ns: 1,
                should_retry: true,
                timed_out: false,
            })
            .unwrap();
        assert!(matches!(outcome, ResultOutcome::DeadLettered { .. }));

        // The single token must still be there for a job that can retry.
        let fresh = scheduler.storage.enqueue(make_job("ceiling")).unwrap();
        assert!(
            matches!(
                fail_once(&scheduler, &fresh.id, "ceiling"),
                ResultOutcome::Retry { .. }
            ),
            "retry-exhausted job must not spend a budget token"
        );
    }

    #[test]
    fn test_recover_orphaned_jobs_skips_own_claims() {
        let mut scheduler = test_scheduler();
        scheduler.set_claim_owner("survivor".to_string());
        scheduler.register_task("self_task".to_string(), retry_task_config(3));

        let job = scheduler.storage.enqueue(make_job("self_task")).unwrap();
        scheduler
            .storage
            .dequeue("default", now_millis() + 1000, None)
            .unwrap()
            .unwrap();
        // Claim owned by this scheduler's own id.
        assert!(scheduler
            .storage
            .claim_execution(&job.id, "survivor")
            .unwrap());

        scheduler.reap_stale().unwrap();

        let still = scheduler.storage.get_job(&job.id, None).unwrap().unwrap();
        assert_eq!(still.status, JobStatus::Running);
        assert_eq!(still.retry_count, 0);
    }

    /// An orphan with no retries left is dead-lettered, not requeued.
    #[test]
    fn test_recover_orphaned_jobs_dead_letters_when_exhausted() {
        let mut scheduler = test_scheduler();
        scheduler.set_claim_owner("survivor".to_string());
        scheduler.register_task("exhausted_task".to_string(), retry_task_config(0));

        let mut nj = make_job("exhausted_task");
        nj.max_retries = 0;
        let job = scheduler.storage.enqueue(nj).unwrap();
        scheduler
            .storage
            .dequeue("default", now_millis() + 1000, None)
            .unwrap()
            .unwrap();
        assert!(scheduler
            .storage
            .claim_execution(&job.id, "dead-worker")
            .unwrap());

        scheduler.reap_stale().unwrap();

        let dead = scheduler.storage.list_dead(10, 0, None).unwrap();
        assert!(dead.iter().any(|d| d.original_job_id == job.id));
    }

    #[test]
    fn test_check_periodic() {
        let scheduler = test_scheduler();

        // Register a periodic task that's due now
        let now = now_millis();
        let row = NewPeriodicTask {
            name: "every_minute".to_string(),
            task_name: "periodic_task".to_string(),
            cron_expr: "* * * * * *".to_string(), // every second
            args: None,
            kwargs: None,
            queue: "default".to_string(),
            enabled: true,
            next_run: now - 1000, // due 1 second ago
            timezone: None,
        };
        scheduler.storage.register_periodic(&row).unwrap();

        scheduler.check_periodic().unwrap();

        // A job should have been enqueued
        let jobs = scheduler
            .storage
            .list_jobs(
                Some(JobStatus::Pending as i32),
                None,
                Some("periodic_task"),
                10,
                0,
                None,
            )
            .unwrap();
        assert_eq!(jobs.len(), 1);
    }

    #[test]
    fn test_auto_cleanup() {
        let storage =
            StorageBackend::Sqlite(crate::storage::sqlite::SqliteStorage::in_memory().unwrap());
        let config = SchedulerConfig {
            result_ttl_ms: Some(1), // 1ms TTL
            ..SchedulerConfig::default()
        };
        let scheduler = Scheduler::new(storage, vec!["default".to_string()], config, None);

        // Enqueue, dequeue, and complete a job
        let job = scheduler.storage.enqueue(make_job("cleanup_task")).unwrap();
        scheduler
            .storage
            .dequeue("default", now_millis() + 1000, None)
            .unwrap();
        scheduler.storage.complete(&job.id, Some(vec![1])).unwrap();

        // Wait for the TTL to expire
        std::thread::sleep(Duration::from_millis(10));

        scheduler.auto_cleanup().unwrap();

        // Job should be purged
        let fetched = scheduler.storage.get_job(&job.id, None).unwrap();
        assert!(fetched.is_none());
    }

    #[test]
    fn test_auto_cleanup_skips_when_another_holds_the_lock() {
        // A peer holds the retention lock, so this scheduler is not the elected
        // cleaner and must purge nothing even though its own TTL has expired.
        let storage =
            StorageBackend::Sqlite(crate::storage::sqlite::SqliteStorage::in_memory().unwrap());
        let config = SchedulerConfig {
            result_ttl_ms: Some(1),
            ..SchedulerConfig::default()
        };
        let scheduler = Scheduler::new(storage, vec!["default".to_string()], config, None);

        let job = scheduler.storage.enqueue(make_job("gated")).unwrap();
        scheduler
            .storage
            .dequeue("default", now_millis() + 1000, None)
            .unwrap();
        scheduler.storage.complete(&job.id, Some(vec![1])).unwrap();
        std::thread::sleep(Duration::from_millis(10));

        // A different owner takes the lock first; the scheduler's own owner
        // ("scheduler") then loses the election.
        assert!(scheduler
            .storage
            .acquire_lock(
                crate::storage::RETENTION_LOCK,
                "another-worker",
                crate::storage::RETENTION_LOCK_TTL_MS
            )
            .unwrap());

        scheduler.auto_cleanup().unwrap();

        assert!(
            scheduler.storage.get_job(&job.id, None).unwrap().is_some(),
            "a non-leader must not purge"
        );
    }

    #[test]
    fn test_auto_cleanup_publishes_the_effective_windows() {
        // The elected cleaner records what it applies so a dashboard in another
        // process can echo the live policy.
        let storage =
            StorageBackend::Sqlite(crate::storage::sqlite::SqliteStorage::in_memory().unwrap());
        let scheduler = Scheduler::new(
            storage,
            vec!["default".to_string()],
            SchedulerConfig::default(),
            Some("tenant-a".to_string()),
        );

        scheduler.auto_cleanup().unwrap();

        let published =
            retention::read_effective_retention(&scheduler.storage, Some("tenant-a")).unwrap();
        let published = published.expect("the leader must publish its windows");
        assert!(published.enabled);
        assert!(published.defaulted, "nothing was configured");
        assert_eq!(published.namespace, "tenant-a");
        assert_eq!(published.windows, retention::RetentionConfig::recommended());
        assert!(published.reported_at > 0);

        // Namespace-scoped: another tenant's dashboard sees nothing from this one.
        assert!(
            retention::read_effective_retention(&scheduler.storage, None)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_auto_cleanup_publishes_a_disabled_policy() {
        // "Retention is off" is a policy worth showing — an empty config must
        // publish as disabled, not as unreported.
        let storage =
            StorageBackend::Sqlite(crate::storage::sqlite::SqliteStorage::in_memory().unwrap());
        let config = SchedulerConfig {
            retention: Some(retention::RetentionConfig::default()),
            ..SchedulerConfig::default()
        };
        let scheduler = Scheduler::new(storage, vec!["default".to_string()], config, None);

        scheduler.auto_cleanup().unwrap();

        let published = retention::read_effective_retention(&scheduler.storage, None)
            .unwrap()
            .expect("a disabled policy is still published");
        assert!(!published.enabled);
        assert!(!published.defaulted);
        assert_eq!(published.namespace, retention::DEFAULT_NAMESPACE);
    }

    #[test]
    fn test_non_leader_does_not_publish_the_windows() {
        // Only the elected cleaner's config governs the deletes, so only it may
        // report — otherwise a peer with a different config overwrites the truth.
        let storage =
            StorageBackend::Sqlite(crate::storage::sqlite::SqliteStorage::in_memory().unwrap());
        let scheduler = Scheduler::new(
            storage,
            vec!["default".to_string()],
            SchedulerConfig::default(),
            None,
        );

        assert!(scheduler
            .storage
            .acquire_lock(
                crate::storage::RETENTION_LOCK,
                "another-worker",
                crate::storage::RETENTION_LOCK_TTL_MS
            )
            .unwrap());

        scheduler.auto_cleanup().unwrap();

        assert!(
            retention::read_effective_retention(&scheduler.storage, None)
                .unwrap()
                .is_none(),
            "a non-leader must not publish"
        );
    }

    #[test]
    fn test_archive_retention_keeps_side_tables_with_no_window() {
        // Per-table independence: archived_jobs has a window but task_logs does
        // not, so purging the archived job must NOT cascade-delete its logs —
        // they are the caller's to keep forever.
        let storage =
            StorageBackend::Sqlite(crate::storage::sqlite::SqliteStorage::in_memory().unwrap());
        let config = SchedulerConfig {
            retention: Some(retention::RetentionConfig {
                archived_jobs_ttl_ms: Some(1),
                task_logs_ttl_ms: None,
                ..retention::RetentionConfig::default()
            }),
            ..SchedulerConfig::default()
        };
        let scheduler = Scheduler::new(storage, vec!["default".to_string()], config, None);

        let job = scheduler.storage.enqueue(make_job("keeps_logs")).unwrap();
        scheduler
            .storage
            .dequeue("default", now_millis() + 1000, None)
            .unwrap();
        scheduler
            .storage
            .write_task_log(&job.id, "keeps_logs", "INFO", "msg", None, None)
            .unwrap();
        scheduler.storage.complete(&job.id, Some(vec![1])).unwrap();
        std::thread::sleep(Duration::from_millis(10));

        scheduler.auto_cleanup().unwrap();

        assert!(
            scheduler.storage.get_job(&job.id, None).unwrap().is_none(),
            "archived_jobs has a 1ms window, so the job must be purged"
        );
        assert!(
            !scheduler
                .storage
                .get_task_logs(&job.id, None)
                .unwrap()
                .is_empty(),
            "task_logs has no window, so the log must survive the archive purge"
        );
    }

    #[test]
    fn test_default_retention_is_enabled() {
        // The default flip: a config that sets neither the per-table map nor a
        // legacy result_ttl now bounds history on the recommended windows. This
        // is the single behavioral change of the default-on release.
        let config = SchedulerConfig::default();
        assert!(config.retention_is_defaulted());
        assert_eq!(
            config.effective_retention(),
            retention::RetentionConfig::recommended()
        );
        assert!(!config.effective_retention().is_empty());
    }

    #[test]
    fn test_retention_is_defaulted_only_without_config() {
        // Defaulted (announced + recommended) only when nothing is chosen.
        assert!(SchedulerConfig::default().retention_is_defaulted());
        assert!(!SchedulerConfig {
            result_ttl_ms: Some(3_600_000),
            ..SchedulerConfig::default()
        }
        .retention_is_defaulted());
        assert!(!SchedulerConfig {
            retention: Some(retention::RetentionConfig::default()),
            ..SchedulerConfig::default()
        }
        .retention_is_defaulted());
        // A negative legacy TTL is a set-but-invalid choice, not the absence of
        // one: not defaulted (no announcement), and it disables retention rather
        // than falling through to the recommended windows.
        let negative = SchedulerConfig {
            result_ttl_ms: Some(-1),
            ..SchedulerConfig::default()
        };
        assert!(!negative.retention_is_defaulted());
        assert!(negative.effective_retention().is_empty());
    }

    #[test]
    fn test_effective_retention_maps_legacy_result_ttl() {
        // The legacy knob means "every table, same window" — the mapping must be
        // lossless so existing Queue(result_ttl=...) callers keep byte-identical
        // behavior.
        let config = SchedulerConfig {
            result_ttl_ms: Some(3_600_000),
            ..SchedulerConfig::default()
        };
        assert_eq!(
            config.effective_retention(),
            retention::RetentionConfig::uniform(3_600_000)
        );
    }

    #[test]
    fn test_effective_retention_prefers_explicit_map() {
        let config = SchedulerConfig {
            result_ttl_ms: Some(3_600_000),
            retention: Some(retention::RetentionConfig {
                task_logs_ttl_ms: Some(1000),
                ..retention::RetentionConfig::default()
            }),
            ..SchedulerConfig::default()
        };
        let eff = config.effective_retention();
        assert_eq!(eff.task_logs_ttl_ms, Some(1000));
        assert_eq!(
            eff.archived_jobs_ttl_ms, None,
            "the map wins, not the legacy TTL"
        );
    }

    #[test]
    fn test_effective_retention_drops_negative_legacy_ttl() {
        let config = SchedulerConfig {
            result_ttl_ms: Some(-5_000),
            ..SchedulerConfig::default()
        };
        assert!(config.effective_retention().is_empty());
    }

    #[test]
    fn test_explicit_empty_retention_overrides_legacy_ttl() {
        // An explicitly empty retention map wins over result_ttl: the caller
        // asked for retention and set no windows, which disables them all rather
        // than falling back to the legacy queue-wide TTL.
        let config = SchedulerConfig {
            result_ttl_ms: Some(3_600_000),
            retention: Some(retention::RetentionConfig::default()),
            ..SchedulerConfig::default()
        };
        assert!(
            config.effective_retention().is_empty(),
            "explicit empty retention disables all windows, ignoring result_ttl"
        );
    }

    #[test]
    fn test_effective_retention_drops_negative_explicit_window() {
        // An explicit map is not validated by construction, so a negative field
        // must be sanitized before it can produce a future cutoff.
        let config = SchedulerConfig {
            retention: Some(retention::RetentionConfig {
                archived_jobs_ttl_ms: Some(-1),
                task_logs_ttl_ms: Some(1000),
                ..retention::RetentionConfig::default()
            }),
            ..SchedulerConfig::default()
        };
        let eff = config.effective_retention();
        assert_eq!(
            eff.archived_jobs_ttl_ms, None,
            "a negative window is dropped"
        );
        assert_eq!(eff.task_logs_ttl_ms, Some(1000), "a valid window is kept");
    }

    #[test]
    fn test_two_default_schedulers_do_not_both_lead_retention() {
        // Regression: the default claim_owner used to be the shared constant
        // "scheduler", so two un-configured schedulers on one store both renewed
        // the same lease and both ran retention. Unique default owners fix it.
        // in_memory() pins a single-connection pool, so the clone shares the DB.
        let storage = crate::storage::sqlite::SqliteStorage::in_memory().unwrap();
        let config = SchedulerConfig {
            result_ttl_ms: Some(1),
            ..SchedulerConfig::default()
        };
        let s1 = Scheduler::new(
            StorageBackend::Sqlite(storage.clone()),
            vec!["default".to_string()],
            config.clone(),
            None,
        );
        let s2 = Scheduler::new(
            StorageBackend::Sqlite(storage),
            vec!["default".to_string()],
            config,
            None,
        );

        assert_ne!(
            s1.claim_owner(),
            s2.claim_owner(),
            "each scheduler must have a distinct default owner"
        );

        let lead1 = crate::storage::try_lead(
            s1.storage(),
            crate::storage::RETENTION_LOCK,
            s1.claim_owner(),
            crate::storage::RETENTION_LOCK_TTL_MS,
        )
        .unwrap();
        let lead2 = crate::storage::try_lead(
            s2.storage(),
            crate::storage::RETENTION_LOCK,
            s2.claim_owner(),
            crate::storage::RETENTION_LOCK_TTL_MS,
        )
        .unwrap();

        assert!(lead1, "the first scheduler takes the free lease");
        assert!(
            !lead2,
            "the second must not also win — a shared owner would let it renew"
        );
    }

    #[test]
    fn test_auto_cleanup_ignores_negative_result_ttl() {
        // A negative TTL inverts the cutoff — `now.saturating_sub(-ttl)` lands in
        // the future, matching every archived row. The bindings reject it, so the
        // scheduler must never be handed one; this pins the blast radius if one
        // ever gets through.
        let storage =
            StorageBackend::Sqlite(crate::storage::sqlite::SqliteStorage::in_memory().unwrap());
        let config = SchedulerConfig {
            result_ttl_ms: Some(-5_000),
            ..SchedulerConfig::default()
        };
        let scheduler = Scheduler::new(storage, vec!["default".to_string()], config, None);

        let job = scheduler.storage.enqueue(make_job("fresh_task")).unwrap();
        scheduler
            .storage
            .dequeue("default", now_millis() + 1000, None)
            .unwrap();
        scheduler.storage.complete(&job.id, Some(vec![1])).unwrap();

        scheduler.auto_cleanup().unwrap();

        assert!(
            scheduler.storage.get_job(&job.id, None).unwrap().is_some(),
            "a negative result_ttl must not purge a job that just completed"
        );
    }

    #[test]
    fn test_auto_cleanup_purges_only_configured_tables() {
        // A per-table window must purge only its own table. Set logs to expire
        // but keep archived jobs forever, then assert the completed job survives.
        let storage =
            StorageBackend::Sqlite(crate::storage::sqlite::SqliteStorage::in_memory().unwrap());
        let config = SchedulerConfig {
            retention: Some(retention::RetentionConfig {
                task_logs_ttl_ms: Some(1),
                ..retention::RetentionConfig::default()
            }),
            ..SchedulerConfig::default()
        };
        let scheduler = Scheduler::new(storage, vec!["default".to_string()], config, None);

        let job = scheduler
            .storage
            .enqueue(make_job("keeps_archived"))
            .unwrap();
        scheduler
            .storage
            .dequeue("default", now_millis() + 1000, None)
            .unwrap();
        scheduler
            .storage
            .write_task_log(&job.id, "keeps_archived", "INFO", "msg", None, None)
            .unwrap();
        scheduler.storage.complete(&job.id, Some(vec![1])).unwrap();
        std::thread::sleep(Duration::from_millis(10));

        scheduler.auto_cleanup().unwrap();

        assert!(
            scheduler.storage.get_job(&job.id, None).unwrap().is_some(),
            "archived_jobs has no window, so the completed job must survive"
        );
        assert!(
            scheduler
                .storage
                .get_task_logs(&job.id, None)
                .unwrap()
                .is_empty(),
            "task_logs has a 1ms window, so its rows must be purged"
        );
    }

    #[test]
    fn test_auto_cleanup_per_entry_ttl_without_global() {
        // With no queue-wide result_ttl, a per-job result_ttl must still be
        // honored (the per-entry purge runs every tick).
        let storage =
            StorageBackend::Sqlite(crate::storage::sqlite::SqliteStorage::in_memory().unwrap());
        let config = SchedulerConfig {
            result_ttl_ms: None,
            ..SchedulerConfig::default()
        };
        let scheduler = Scheduler::new(storage, vec!["default".to_string()], config, None);

        let mut nj = make_job("per_entry_ttl");
        nj.result_ttl_ms = Some(1); // 1ms per-job TTL
        let job = scheduler.storage.enqueue(nj).unwrap();
        scheduler
            .storage
            .dequeue("default", now_millis() + 1000, None)
            .unwrap();
        scheduler.storage.complete(&job.id, Some(vec![1])).unwrap();

        std::thread::sleep(Duration::from_millis(10));
        scheduler.auto_cleanup().unwrap();

        assert!(
            scheduler.storage.get_job(&job.id, None).unwrap().is_none(),
            "per-job result_ttl must be purged even without a global result_ttl"
        );
    }

    #[test]
    fn test_tick_dispatches_and_maintains() {
        let storage =
            StorageBackend::Sqlite(crate::storage::sqlite::SqliteStorage::in_memory().unwrap());
        let config = SchedulerConfig {
            reap_interval: 1,
            periodic_check_interval: 1,
            cleanup_interval: 1,
            ..SchedulerConfig::default()
        };
        let scheduler = Scheduler::new(storage, vec!["default".to_string()], config, None);

        scheduler.storage.enqueue(make_job("tick_task")).unwrap();

        let (tx, mut rx) = make_channel(16);
        let mut counters = TickCounters::default();
        scheduler.tick(&tx, &mut counters);

        // Job should be dispatched
        assert!(rx.try_recv().is_ok());
        // Counters should be incremented
        assert_eq!(counters.reap, 1);
        assert_eq!(counters.periodic, 1);
        assert_eq!(counters.cleanup, 1);
    }
}

#[cfg(all(test, feature = "push-dispatch"))]
mod push_tests {
    use super::*;
    use crate::job::{now_millis, NewJob};
    use crate::storage::Storage;
    use std::time::Duration;

    fn push_scheduler() -> Scheduler {
        let storage =
            StorageBackend::Sqlite(crate::storage::sqlite::SqliteStorage::in_memory().unwrap());
        Scheduler::new(
            storage,
            vec!["default".to_string()],
            SchedulerConfig::default(),
            None,
        )
    }

    fn ready_job(task_name: &str) -> NewJob {
        NewJob {
            queue: "default".to_string(),
            task_name: task_name.to_string(),
            payload: vec![1, 2, 3],
            priority: 0,
            scheduled_at: now_millis(),
            max_retries: 3,
            timeout_ms: 300_000,
            unique_key: None,
            metadata: None,
            notes: None,
            depends_on: vec![],
            expires_at: None,
            result_ttl_ms: None,
            namespace: None,
        }
    }

    /// Enqueuing a ready job must wake the in-process Notify within 50ms.
    #[test]
    fn test_wake_fires_on_immediate_enqueue() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let scheduler = push_scheduler();
            let notify = match scheduler.storage() {
                StorageBackend::Sqlite(s) => s.notify_handle().clone(),
                _ => unreachable!("test uses sqlite"),
            };

            // Enqueue goes through the StorageBackend chokepoint, which calls
            // notify_one() for ready jobs.
            scheduler.storage().enqueue(ready_job("wake_task")).unwrap();

            // notify_one() before notified() still wakes the next waiter, so a
            // brief wait must resolve.
            let woke = tokio::time::timeout(Duration::from_millis(50), notify.notified()).await;
            assert!(
                woke.is_ok(),
                "enqueue of a ready job must wake the scheduler"
            );
        });
    }

    /// A delayed job must NOT wake immediately — the Notify stays unsignaled.
    #[test]
    fn test_delayed_job_does_not_wake() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let scheduler = push_scheduler();
            let notify = match scheduler.storage() {
                StorageBackend::Sqlite(s) => s.notify_handle().clone(),
                _ => unreachable!("test uses sqlite"),
            };

            let mut delayed = ready_job("later_task");
            delayed.scheduled_at = now_millis() + 60_000; // 1 minute out
            scheduler.storage().enqueue(delayed).unwrap();

            let woke = tokio::time::timeout(Duration::from_millis(50), notify.notified()).await;
            assert!(woke.is_err(), "a delayed job must not wake immediately");

            // It should instead arm the delayed timer.
            let timer = scheduler.next_delayed_timer();
            assert!(timer > Duration::ZERO && timer <= Scheduler::PUSH_FALLBACK_INTERVAL);
        });
    }

    /// With no wake delivered, the fallback timer still dispatches a job.
    #[test]
    fn test_fallback_poll_dispatches_without_wake() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let scheduler = push_scheduler();

            // Insert a ready job directly via the inherent SQLite method so the
            // StorageBackend notify chokepoint is bypassed — simulating a
            // missed wake. The fallback dispatch path must still pick it up.
            match scheduler.storage() {
                StorageBackend::Sqlite(s) => {
                    s.enqueue(ready_job("fallback_task")).unwrap();
                }
                _ => unreachable!("test uses sqlite"),
            }

            let (tx, mut rx) = tokio::sync::mpsc::channel(16);
            // Drive a single fallback dispatch round directly.
            while scheduler.tick_dispatch(&tx) {}

            assert!(
                rx.try_recv().is_ok(),
                "fallback dispatch must claim the job even without a wake"
            );

            let job = rx.try_recv();
            // Only one job existed.
            assert!(job.is_err());
        });
    }

    /// `enable_push_dispatch` is the one entry point every binding shell uses,
    /// so it must install a source the enqueue path actually signals. Waiting
    /// after the enqueue is sound: `notify_one` stores a permit, so the wake
    /// cannot be lost between the two.
    #[tokio::test]
    async fn test_enable_push_dispatch_wakes_on_enqueue() {
        let scheduler = push_scheduler();
        scheduler.enable_push_dispatch();
        let mut wake = scheduler
            .take_wake_source()
            .expect("enable_push_dispatch must install a wake source");

        scheduler.storage.enqueue(ready_job("wake_task")).unwrap();

        tokio::time::timeout(Duration::from_secs(5), wake.wait())
            .await
            .expect("an enqueued ready job must wake the push loop");
    }

    /// `note_scheduled_at` keeps the earliest schedule and clears once due.
    #[test]
    fn test_next_delayed_timer_tracks_earliest() {
        let scheduler = push_scheduler();
        let now = now_millis();
        scheduler.note_scheduled_at(now + 10_000);
        scheduler.note_scheduled_at(now + 5_000);
        scheduler.note_scheduled_at(now + 20_000);

        let timer = scheduler.next_delayed_timer();
        // Earliest (5s out), capped at the fallback interval (2s).
        assert!(timer <= Scheduler::PUSH_FALLBACK_INTERVAL);
        assert!(timer > Duration::ZERO);
    }
}
