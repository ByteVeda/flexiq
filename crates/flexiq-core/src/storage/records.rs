//! Plain-data records returned and accepted by the [`Storage`](super::Storage)
//! trait. ORM-free by design: the Diesel row structs live in the private
//! `models` module and convert to/from these at the query boundary, so the
//! public API never exposes backend implementation details.
//!
//! Field names mirror the storage columns on purpose — the Redis backend
//! persists some of these as JSON, so renaming a field is a wire change.

use serde::{Deserialize, Serialize};

/// One recorded failure attempt for a job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobError {
    /// Unique id of this error record.
    pub id: String,
    /// Id of the job that failed.
    pub job_id: String,
    /// 1-based attempt number that produced this error.
    pub attempt: i32,
    /// Error message (canonical JSON `TaskError` when structured).
    pub error: String,
    /// Unix-millisecond time of the failure.
    pub failed_at: i64,
}

/// Token-bucket state for a rate-limit key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitState {
    /// Rate-limit key (task or queue scoped).
    pub key: String,
    /// Tokens currently available.
    pub tokens: f64,
    /// Bucket capacity.
    pub max_tokens: f64,
    /// Tokens added per second.
    pub refill_rate: f64,
    /// Unix-millisecond time of the last refill.
    pub last_refill: i64,
}

/// A registered periodic (cron) task.
#[derive(Debug, Clone)]
pub struct PeriodicTask {
    /// Unique schedule name.
    pub name: String,
    /// Task to enqueue on each firing.
    pub task_name: String,
    /// Cron expression driving the schedule.
    pub cron_expr: String,
    /// Serialized positional arguments.
    pub args: Option<Vec<u8>>,
    /// Serialized keyword arguments.
    pub kwargs: Option<Vec<u8>>,
    /// Queue to enqueue into.
    pub queue: String,
    /// Whether the schedule is active (false = paused).
    pub enabled: bool,
    /// Unix-millisecond time of the last firing, unset until first run.
    pub last_run: Option<i64>,
    /// Unix-millisecond time of the next firing.
    pub next_run: i64,
    /// IANA timezone for cron evaluation. `None` = UTC.
    pub timezone: Option<String>,
}

/// Registration payload for a periodic task. `last_run` starts unset.
#[derive(Debug, Clone)]
pub struct NewPeriodicTask {
    /// Unique schedule name.
    pub name: String,
    /// Task to enqueue on each firing.
    pub task_name: String,
    /// Cron expression driving the schedule.
    pub cron_expr: String,
    /// Serialized positional arguments.
    pub args: Option<Vec<u8>>,
    /// Serialized keyword arguments.
    pub kwargs: Option<Vec<u8>>,
    /// Queue to enqueue into.
    pub queue: String,
    /// Whether the schedule starts active.
    pub enabled: bool,
    /// Unix-millisecond time of the first firing.
    pub next_run: i64,
    /// IANA timezone for cron evaluation. `None` = UTC.
    pub timezone: Option<String>,
}

/// A topic subscription in the pub/sub registry.
///
/// The natural composite key is `(topic, subscription_name)`. A `None`
/// `owner_worker_id` marks a durable subscription that persists until an
/// explicit unsubscribe; a set owner marks an ephemeral one that is reaped
/// when its worker dies.
#[derive(Debug, Clone)]
pub struct Subscription {
    /// Topic the subscription listens on.
    pub topic: String,
    /// Subscription name, unique per topic.
    pub subscription_name: String,
    /// Task enqueued for each published message.
    pub task_name: String,
    /// Queue deliveries are enqueued into.
    pub queue: String,
    /// Whether deliveries are currently enabled (false = paused).
    pub active: bool,
    /// True for durable subscriptions that outlive their creator.
    pub durable: bool,
    /// Owning worker id for ephemeral subscriptions; `None` = durable.
    pub owner_worker_id: Option<String>,
    /// Unix-millisecond registration time.
    pub created_at: i64,
    /// Per-subscription delivery settings persisted at registration so
    /// `publish_to_topic` applies them cross-process. `None` = queue default.
    pub priority: Option<i32>,
    /// Per-delivery retry cap. `None` = queue default.
    pub max_retries: Option<i32>,
    /// Per-delivery timeout in milliseconds. `None` = queue default.
    pub timeout_ms: Option<i64>,
    /// How this subscription is delivered to.
    pub mode: SubscriptionMode,
    /// Log-mode read cursor: the last-acked message id. `None` = unread (start
    /// from the beginning). Ignored for fan-out subscriptions.
    pub cursor: Option<String>,
}

/// Registration payload for a topic subscription.
#[derive(Debug, Clone)]
pub struct NewSubscription {
    /// Topic the subscription listens on.
    pub topic: String,
    /// Subscription name, unique per topic.
    pub subscription_name: String,
    /// Task enqueued for each published message.
    pub task_name: String,
    /// Queue deliveries are enqueued into.
    pub queue: String,
    /// Whether deliveries start enabled.
    pub active: bool,
    /// True for durable subscriptions that outlive their creator.
    pub durable: bool,
    /// Owning worker id for ephemeral subscriptions; `None` = durable.
    pub owner_worker_id: Option<String>,
    /// Unix-millisecond registration time.
    pub created_at: i64,
    /// Per-delivery priority override. `None` = queue default.
    pub priority: Option<i32>,
    /// Per-delivery retry cap. `None` = queue default.
    pub max_retries: Option<i32>,
    /// Per-delivery timeout in milliseconds. `None` = queue default.
    pub timeout_ms: Option<i64>,
    /// How this subscription is delivered to. See [`Subscription::mode`].
    pub mode: SubscriptionMode,
}

/// Lifecycle state a worker reports for itself. Stored as its lowercase wire
/// form in the `workers.status` column, so the persisted values are unchanged.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WorkerStatus {
    /// Registered and consuming — the state every worker starts in.
    #[default]
    Active,
    /// Shutting down: finishing in-flight jobs, claiming no new ones.
    Draining,
}

impl WorkerStatus {
    /// The wire form persisted in the `status` column.
    pub fn as_str(&self) -> &'static str {
        match self {
            WorkerStatus::Active => "active",
            WorkerStatus::Draining => "draining",
        }
    }

    /// Parse a persisted `status`; anything unrecognized reads as active, the
    /// value every backend writes at registration. Use [`Self::parse`] for caller
    /// input, where a typo must not pass silently.
    pub fn from_wire(wire: &str) -> Self {
        match wire {
            "draining" => WorkerStatus::Draining,
            _ => WorkerStatus::Active,
        }
    }

    /// Strictly parse caller-supplied input; `None` for anything outside the set.
    pub fn parse(wire: &str) -> Option<Self> {
        match wire {
            "active" => Some(WorkerStatus::Active),
            "draining" => Some(WorkerStatus::Draining),
            _ => None,
        }
    }
}

impl std::fmt::Display for WorkerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How a topic delivers to a subscription. Stored as its lowercase wire form in
/// the `mode` column, so the persisted values are unchanged.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SubscriptionMode {
    /// One delivery job per publish, per subscription — the default.
    #[default]
    Fanout,
    /// Append one `topic_messages` row per publish; consumers pull via cursor.
    Log,
}

impl SubscriptionMode {
    /// The wire form persisted in the `mode` column.
    pub fn as_str(&self) -> &'static str {
        match self {
            SubscriptionMode::Fanout => "fanout",
            SubscriptionMode::Log => "log",
        }
    }

    /// Parse a persisted `mode`. Anything unrecognized reads as fan-out, which is
    /// what the column's pre-enum readers did with a value that wasn't `"log"`.
    /// Use [`Self::parse`] for caller input, where a typo must not pass silently.
    pub fn from_wire(wire: &str) -> Self {
        match wire {
            "log" => SubscriptionMode::Log,
            _ => SubscriptionMode::Fanout,
        }
    }

    /// Strictly parse caller-supplied input; `None` for anything outside the set.
    pub fn parse(wire: &str) -> Option<Self> {
        match wire {
            "fanout" => Some(SubscriptionMode::Fanout),
            "log" => Some(SubscriptionMode::Log),
            _ => None,
        }
    }

    /// Whether this mode is the append-once + cursor model.
    pub fn is_log(&self) -> bool {
        matches!(self, SubscriptionMode::Log)
    }
}

impl std::fmt::Display for SubscriptionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One durable message in a log topic. Unlike fan-out delivery (one `jobs` row
/// per subscriber), a log publish writes exactly one of these and each log
/// subscription advances its own cursor over them.
#[derive(Debug, Clone)]
pub struct TopicMessage {
    /// Message id — a time-ordered token that doubles as the read cursor.
    /// Opaque to callers (UUIDv7 on Diesel backends, a stream id on Redis).
    pub id: String,
    /// Topic the message was published to.
    pub topic: String,
    /// Opaque payload bytes (same codec as fan-out `publish`).
    pub payload: Vec<u8>,
    /// Optional caller metadata (JSON).
    pub metadata: Option<String>,
    /// Optional structured notes (JSON).
    pub notes: Option<String>,
    /// Unix-millisecond publish time.
    pub created_at: i64,
    /// Optional expiry (Unix ms) — a TTL safety net for the retention sweep.
    pub expires_at: Option<i64>,
}

/// A declared topic in the first-class registry. Declaring a log topic makes
/// its publishes retained even with zero subscribers (removing the late-join
/// boundary), bounded by an optional `retention_ms` window.
#[derive(Debug, Clone)]
pub struct Topic {
    /// Topic name (primary key).
    pub name: String,
    /// Delivery mode: [`SubscriptionMode::Log`] (the only declarable mode today)
    /// opts publishes into the append-once store even without a subscriber.
    pub mode: SubscriptionMode,
    /// Retention window in milliseconds; `None` = keep until consumed/compacted.
    /// A published log row gets `expires_at = now + retention_ms` when the topic
    /// has no live log subscriber, so the retention sweep can reclaim it.
    pub retention_ms: Option<i64>,
    /// Unix-millisecond declaration time.
    pub created_at: i64,
}

impl Topic {
    /// Whether this is a log topic (publishes are retained without a subscriber).
    pub fn is_log(&self) -> bool {
        self.mode.is_log()
    }
}

/// Backlog snapshot for one log subscription: how far its cursor lags the log.
#[derive(Debug, Clone)]
pub struct TopicLogStats {
    /// Topic the subscription reads.
    pub topic: String,
    /// Subscription name.
    pub subscription_name: String,
    /// Current read cursor (last-acked id); `None` = nothing acked yet.
    pub cursor: Option<String>,
    /// Number of messages after the cursor still to be consumed.
    pub lag: i64,
    /// Age (ms) of the oldest un-acked message; `None` when fully caught up.
    pub oldest_unacked_age_ms: Option<i64>,
}

/// One execution measurement for a task.
#[derive(Debug, Clone)]
pub struct TaskMetric {
    /// Unique id of this metric record.
    pub id: String,
    /// Task that was executed.
    pub task_name: String,
    /// Job the measurement belongs to.
    pub job_id: String,
    /// Wall-clock execution time in nanoseconds.
    pub wall_time_ns: i64,
    /// Peak memory delta in bytes.
    pub memory_bytes: i64,
    /// Whether the execution succeeded.
    pub succeeded: bool,
    /// Unix-millisecond time the metric was recorded.
    pub recorded_at: i64,
}

/// One replay of a completed job, pairing original and replay outcomes.
#[derive(Debug, Clone)]
pub struct ReplayEntry {
    /// Unique id of this replay record.
    pub id: String,
    /// Id of the job that was replayed.
    pub original_job_id: String,
    /// Id of the replay job.
    pub replay_job_id: String,
    /// Unix-millisecond time of the replay.
    pub replayed_at: i64,
    /// Serialized result of the original run.
    pub original_result: Option<Vec<u8>>,
    /// Serialized result of the replay run.
    pub replay_result: Option<Vec<u8>>,
    /// Error message of the original run, if it failed.
    pub original_error: Option<String>,
    /// Error message of the replay run, if it failed.
    pub replay_error: Option<String>,
}

/// One structured log line emitted during task execution.
#[derive(Debug, Clone)]
pub struct TaskLogEntry {
    /// Unique id of this log line (UUIDv7, doubles as a stream cursor).
    pub id: String,
    /// Job the log line belongs to.
    pub job_id: String,
    /// Task that emitted the line.
    pub task_name: String,
    /// Log level (`debug`/`info`/`warning`/`error`).
    pub level: String,
    /// Log message text.
    pub message: String,
    /// Pre-encoded JSON of structured extra fields, if any.
    pub extra: Option<String>,
    /// Unix-millisecond time the line was logged.
    pub logged_at: i64,
}

/// Persisted circuit-breaker state for a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerState {
    /// Task the breaker guards.
    pub task_name: String,
    /// Current state: 0 = closed, 1 = open, 2 = half-open.
    pub state: i32,
    /// Failures observed in the current window.
    pub failure_count: i32,
    /// Unix-millisecond time of the most recent failure.
    pub last_failure_at: Option<i64>,
    /// Unix-millisecond time the breaker opened.
    pub opened_at: Option<i64>,
    /// Unix-millisecond time the breaker entered half-open.
    pub half_open_at: Option<i64>,
    /// Failure count that trips the breaker open.
    pub threshold: i32,
    /// Failure-counting window in milliseconds.
    pub window_ms: i64,
    /// Open-state cooldown in milliseconds before probing.
    pub cooldown_ms: i64,
    /// Maximum probe executions allowed while half-open.
    #[serde(default = "default_max_probes")]
    pub half_open_max_probes: i32,
    /// Probe success ratio (0.0-1.0) required to close.
    #[serde(default = "default_success_rate")]
    pub half_open_success_rate: f64,
    /// Probes dispatched in the current half-open round.
    #[serde(default)]
    pub half_open_probe_count: i32,
    /// Probes that succeeded in the current half-open round.
    #[serde(default)]
    pub half_open_success_count: i32,
    /// Probes that failed in the current half-open round.
    #[serde(default)]
    pub half_open_failure_count: i32,
}

fn default_max_probes() -> i32 {
    5
}

fn default_success_rate() -> f64 {
    0.8
}

/// How a debounced enqueue collapses a burst into one run.
///
/// Grouped into a struct rather than passed positionally for the same reason as
/// [`WorkerRegistration`]: two adjacent millisecond durations are easy to
/// transpose, and transposing these two silently inverts the semantics.
#[derive(Debug, Clone, Copy)]
pub struct DebounceOptions {
    /// How far ahead of *now* each enqueue pushes the run, in milliseconds.
    pub window_ms: i64,
    /// Hard ceiling on the total delay, measured from the pending job's
    /// `created_at`. Mandatory: without it a caller who never stops enqueuing
    /// starves the job forever, which is the classic debounce footgun.
    pub max_wait_ms: i64,
    /// Overwrite the pending job's payload with the newest one. `false` keeps
    /// the payload the window opened with.
    pub replace_payload: bool,
    /// The target queue's `max_pending` admission cap, or `None` when the queue
    /// is uncapped (then nothing is counted and no query is spent).
    ///
    /// It rides here because this is the one enqueue whose caller cannot apply
    /// it: a coalescing call adds no pending row, and slide-vs-insert is only
    /// decided inside the write. Enforced on the inserting branch alone, and
    /// with the callers' rule — refused once `pending + 1` would exceed `cap`.
    ///
    /// Still admission control, not a barrier: two enqueues under *different*
    /// debounce keys serialize on nothing (the write only locks the key it
    /// coalesces on), so both can count the same backlog and both insert. A
    /// brief overshoot under concurrent producers is accepted here for the same
    /// reason it is accepted by the producer-side check this replaces, and by
    /// the rate limiter. Making it exact would mean a per-queue lock on every
    /// debounced insert, which would serialize unrelated keys and still leave
    /// the plain enqueue path — which counts and inserts outside any shared
    /// transaction — soft.
    ///
    /// A negative cap is a caller error, not a sentinel: `None` is the uncapped
    /// case. Refused by `validated_debounce_key` before any backend sees it.
    pub max_pending: Option<i64>,
}

/// Everything a worker announces about itself when it joins the registry.
///
/// Grouped into a struct rather than passed positionally: the shells populate
/// these from unrelated sources — hostname from the OS, `sdk`/`sdk_version`
/// baked in at build time, `queues`/`threads` from user config — and a run of
/// nine same-typed `Option<&str>` arguments is easy to transpose silently.
///
/// `#[non_exhaustive]`: what a worker announces has grown twice already
/// (`0009_worker_sdk`, `0012_worker_registry_fingerprint`) and will again.
/// Build one with [`WorkerRegistration::new`] and the setters, which leave a
/// caller compiling when it does.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct WorkerRegistration<'a> {
    /// Unique worker id.
    pub worker_id: &'a str,
    /// Comma-separated queue names the worker consumes.
    pub queues: &'a str,
    /// Pre-encoded JSON list of worker tags.
    pub tags: Option<&'a str>,
    /// Pre-encoded JSON list of resource names the worker provides.
    pub resources: Option<&'a str>,
    /// Pre-encoded JSON of per-resource health.
    pub resource_health: Option<&'a str>,
    /// Worker thread count.
    pub threads: i32,
    /// Host the worker runs on.
    pub hostname: Option<&'a str>,
    /// OS process id of the worker.
    pub pid: Option<i32>,
    /// Execution pool type (e.g. `thread`, `prefork`).
    pub pool_type: Option<&'a str>,
    /// SDK registering the worker (e.g. `python`, `node`, `java`).
    pub sdk: Option<&'a str>,
    /// Release of that SDK.
    pub sdk_version: Option<&'a str>,
    /// Fingerprint of the worker's task registry, from
    /// [`crate::worker::registry_fingerprint`]. `None` from a shell that does
    /// not report one, and from a worker with nothing registered — neither is
    /// a registry that differs from its peers', so neither gets a value.
    pub registry_fingerprint: Option<&'a str>,
}

impl<'a> WorkerRegistration<'a> {
    /// A registration carrying only what every worker must state: who it is,
    /// what it consumes, and how much of it there is.
    ///
    /// Everything else is a setter, because everything else is something a
    /// shell may not know about itself — a worker that cannot see its own
    /// hostname registers without one rather than inventing a value.
    pub fn new(worker_id: &'a str, queues: &'a str, threads: i32) -> Self {
        Self {
            worker_id,
            queues,
            threads,
            ..Self::default()
        }
    }

    /// Pre-encoded JSON list of worker tags.
    pub fn tags(mut self, tags: Option<&'a str>) -> Self {
        self.tags = tags;
        self
    }

    /// Pre-encoded JSON list of resource names the worker provides.
    pub fn resources(mut self, resources: Option<&'a str>) -> Self {
        self.resources = resources;
        self
    }

    /// Pre-encoded JSON of per-resource health.
    pub fn resource_health(mut self, resource_health: Option<&'a str>) -> Self {
        self.resource_health = resource_health;
        self
    }

    /// Host the worker runs on.
    pub fn hostname(mut self, hostname: Option<&'a str>) -> Self {
        self.hostname = hostname;
        self
    }

    /// OS process id of the worker.
    pub fn pid(mut self, pid: Option<i32>) -> Self {
        self.pid = pid;
        self
    }

    /// Execution pool type (e.g. `thread`, `prefork`).
    pub fn pool_type(mut self, pool_type: Option<&'a str>) -> Self {
        self.pool_type = pool_type;
        self
    }

    /// SDK registering the worker, and the release of it. Taken together
    /// because a version without the SDK that produced it names nothing.
    pub fn sdk(mut self, sdk: Option<&'a str>, sdk_version: Option<&'a str>) -> Self {
        self.sdk = sdk;
        self.sdk_version = sdk_version;
        self
    }

    /// Fingerprint of the worker's task registry, from
    /// [`crate::worker::registry_fingerprint`]. A shell that cannot see its own
    /// registry passes `None` rather than a value it guessed: an unregistered
    /// task name is a fatal failure, so a row that overstates what a worker
    /// runs is worse than one that says nothing.
    pub fn registry_fingerprint(mut self, registry_fingerprint: Option<&'a str>) -> Self {
        self.registry_fingerprint = registry_fingerprint;
        self
    }
}

/// A registered worker as seen by the cluster registry.
///
/// `#[non_exhaustive]` for the same reason as [`WorkerRegistration`], which it
/// mirrors. Only this crate builds one; a caller reads it.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct WorkerInfo {
    /// Unique worker id.
    pub worker_id: String,
    /// Unix-millisecond time of the last heartbeat.
    pub last_heartbeat: i64,
    /// Comma-separated queue names the worker consumes.
    pub queues: String,
    /// Worker status string (e.g. `active`, `offline`).
    pub status: String,
    /// Pre-encoded JSON list of worker tags, if any.
    pub tags: Option<String>,
    /// Pre-encoded JSON list of resource names the worker provides.
    pub resources: Option<String>,
    /// Pre-encoded JSON of per-resource health, refreshed each heartbeat.
    pub resource_health: Option<String>,
    /// Worker thread count.
    pub threads: i32,
    /// Unix-millisecond time the worker started.
    pub started_at: Option<i64>,
    /// Host the worker runs on.
    pub hostname: Option<String>,
    /// OS process id of the worker.
    pub pid: Option<i32>,
    /// Execution pool type (e.g. `thread`, `prefork`).
    pub pool_type: Option<String>,
    /// SDK that registered the worker (e.g. `python`, `node`, `java`).
    pub sdk: Option<String>,
    /// Release of that SDK, so a stale worker is visible without going host by
    /// host. `None` from a shell that predates version reporting.
    pub sdk_version: Option<String>,
    /// Fingerprint of the worker's task registry, so the one worker in a fleet
    /// that discovered a different set of tasks is visible without going host
    /// by host. `None` from a shell that predates the field, and from a worker
    /// with nothing registered.
    pub registry_fingerprint: Option<String>,
}

/// Holder and expiry of a distributed lock.
#[derive(Debug, Clone)]
pub struct LockInfo {
    /// Lock name.
    pub lock_name: String,
    /// Current holder's owner id.
    pub owner_id: String,
    /// Unix-millisecond time the lock was acquired.
    pub acquired_at: i64,
    /// Unix-millisecond time the lock expires.
    pub expires_at: i64,
}

// ── Durable inline steps ─────────────────────────────────────────────

/// What a committed step row records.
///
/// Part of the replay match, not a label: a `Run` row carries no `wake_at`, so
/// reading one as a sleep would reschedule the job to a null deadline, and a
/// `Run` commit landing on a stored `Sleep` row is a divergence rather than a
/// mismatched digest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StepKind {
    /// A `step.run` checkpoint. Its `result` is the memoized value.
    Run,
    /// A `step.sleep` deadline. Complete once `now >= wake_at`.
    Sleep,
}

impl StepKind {
    /// The stored form. Pinned so a Lua script and a Diesel column agree.
    pub const fn as_str(self) -> &'static str {
        match self {
            StepKind::Run => "run",
            StepKind::Sleep => "sleep",
        }
    }

    /// Read a stored value back. An unrecognized one reads as `Run`, which is
    /// what every row written before `sleep` existed is.
    pub fn from_wire(value: &str) -> Self {
        match value {
            "sleep" => StepKind::Sleep,
            _ => StepKind::Run,
        }
    }
}

/// Whether a result still speaks for the job it names.
///
/// A `JobResult` is identified by `job_id` alone, so an unfenced
/// `handle_result` will retry, dead-letter or finalize whichever job the id
/// names — including one that orphan recovery reclaimed to another worker while
/// the original owner was merely slow rather than dead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptFence {
    /// The claim names this owner at this attempt, or is merely absent while the
    /// job is still `Running` at it — the age-sweep case, which re-asserts.
    Authorized,
    /// The claim names another worker, or the job has moved past this attempt.
    /// The only correct contribution a superseded attempt can make is none.
    Superseded,
}

/// One committed step of a job, as read back at attempt start.
///
/// There is no `status` and no `error`: a step whose closure raised is never
/// committed, so a stored `Run` row is complete by construction and a `Sleep`
/// row's completeness is `now >= wake_at`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobStep {
    /// Id of the job this step ran inside.
    pub job_id: String,
    /// Position in the job's step sequence, zero-based and gapless.
    pub seq: i32,
    /// Identity of the step within the job (`name#occurrence` or `name:key`).
    pub step_key: String,
    /// Whether this row memoizes a value or a deadline.
    pub kind: StepKind,
    /// Encoded result — post serializer, post codec. `None` for a sleep.
    pub result: Option<Vec<u8>>,
    /// Deadline this step sleeps until. `Some` only for [`StepKind::Sleep`].
    pub wake_at: Option<i64>,
    /// Unix-millisecond time the step was committed.
    pub created_at: i64,
}

/// A step about to be committed.
///
/// `owner` and `attempt` are deliberately *not* here: they fence the write and
/// are derived by the scheduler, never carried alongside the payload where a
/// caller could assert them about itself. Neither is the namespace — the row
/// denormalises it from the job the write already read to resolve its fence, so
/// the two can never disagree.
#[derive(Debug, Clone)]
pub struct NewJobStep<'a> {
    /// Id of the job this step runs inside.
    pub job_id: &'a str,
    /// Position in the job's step sequence. Must be exactly the number of
    /// steps already committed.
    pub seq: i32,
    /// Identity of the step within the job.
    pub step_key: &'a str,
    /// Whether this commits a value or a deadline.
    pub kind: StepKind,
    /// Encoded result. `None` for a sleep.
    pub result: Option<&'a [u8]>,
}

/// What a step commit did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepCommit {
    /// The row was written.
    Committed,
    /// A byte-identical row was already at this position. A retransmission
    /// gets this, and it is a success.
    AlreadyCommitted,
}

/// What a sleep did, carrying the deadline the job was actually rescheduled to.
///
/// The first commit fixes that deadline; a replay of the same `sleep("1h")`
/// keeps it rather than pushing an hour further out every time the job crashes
/// into it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SleepOutcome {
    /// The sleep row was written with the candidate deadline.
    Slept {
        /// Deadline the job was rescheduled to.
        wake_at: i64,
    },
    /// A sleep row was already committed here; its stored deadline stands.
    AlreadySleeping {
        /// Deadline the job was rescheduled to — the stored one.
        wake_at: i64,
    },
}

impl SleepOutcome {
    /// The deadline the job was rescheduled to, whichever arm this is.
    pub const fn wake_at(self) -> i64 {
        match self {
            SleepOutcome::Slept { wake_at } | SleepOutcome::AlreadySleeping { wake_at } => wake_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SubscriptionMode, WorkerStatus};

    #[test]
    fn subscription_mode_round_trips_its_stored_form() {
        for mode in [SubscriptionMode::Fanout, SubscriptionMode::Log] {
            assert_eq!(SubscriptionMode::from_wire(mode.as_str()), mode);
        }
        assert_eq!(SubscriptionMode::Fanout.as_str(), "fanout");
        assert_eq!(SubscriptionMode::Log.as_str(), "log");
        assert!(SubscriptionMode::Log.is_log());
    }

    #[test]
    fn worker_status_round_trips_its_stored_form() {
        for status in [WorkerStatus::Active, WorkerStatus::Draining] {
            assert_eq!(WorkerStatus::from_wire(status.as_str()), status);
        }
        assert_eq!(WorkerStatus::Active.as_str(), "active");
        assert_eq!(WorkerStatus::Draining.as_str(), "draining");
    }

    #[test]
    fn unrecognized_stored_values_read_as_the_default() {
        // Rows predate the enums, so a value neither wrote must not panic or
        // silently promote — it reads as what the old string compares implied.
        assert_eq!(
            SubscriptionMode::from_wire("broadcast"),
            SubscriptionMode::Fanout
        );
        assert_eq!(WorkerStatus::from_wire("paused"), WorkerStatus::Active);
    }
}
