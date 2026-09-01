#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

/// The contract level a deployment requires, and the floor that enforces it.
pub mod contract;
/// Error types: [`QueueError`] and the crate-wide [`Result`] alias.
pub mod error;
/// Core job model: [`Job`], [`JobStatus`], [`NewJob`], [`JobCompletion`].
pub mod job;
/// Periodic (cron) task scheduling helpers.
pub mod periodic;
pub mod pubsub;
/// Resilience primitives: retry policies, rate limiting, circuit breakers, DLQ.
pub mod resilience;
/// The [`Scheduler`]: job dispatch, retries, maintenance, retention.
pub mod scheduler;
/// Reserved settings-key prefixes: the namespaces the runtime owns.
pub mod settings;
/// Shared rules for durable inline steps: the limits every writer honors.
pub mod step;
/// The [`Storage`] trait, backend implementations, and shared records.
pub mod storage;
/// Native worker: task registry, dispatcher trait, worker runner.
pub mod worker;

// Primary public API — the types most consumers need. The crate root is the
// blessed import path; submodules stay public for discoverability but new code
// should prefer these re-exports.
/// Diesel, re-exported.
///
/// `QueueError::Storage` wraps a `diesel::result::Error`, so a consumer that
/// wants to tell a constraint violation from an unreachable database has to
/// name Diesel's types. Re-exporting them means it does that through the
/// version this crate is built against rather than a second one it picked.
pub use diesel;

pub use contract::{
    ensure_contract_supported, min_contract, set_min_contract, CONTRACT_VERSION,
    MIN_CONTRACT_VERSION,
};
pub use error::{QueueError, Result, StepDivergence};
pub use job::{now_millis, Job, JobCompletion, JobStatus, NewJob};
pub use resilience::circuit_breaker::{CircuitBreakerConfig, CircuitState};
pub use resilience::rate_limiter::RateLimitConfig;
pub use resilience::retry::RetryPolicy;
pub use scheduler::result_handler::RETRY_BUDGET_EXHAUSTED;
pub use scheduler::retention::{EffectiveRetention, RetentionConfig};
pub use scheduler::{
    JobResult, QueueConfig, ResultOutcome, Scheduler, SchedulerConfig, TaskConfig,
};
pub use settings::{is_reserved_setting_key, RESERVED_SETTING_PREFIXES};
pub use step::{
    classify_step_failure, idempotency_key, refusal_error, run_key, PendingStep, SleepDecision,
    StepDecision, StepFailure, StepKey, StepLimits, StepSequence, StepSession, StepSleep,
    StepStore, StorageStepSession, StorageSteps, ORIGIN_JOB_ID_KEY,
};
pub use storage::cursor::Page;
#[cfg(feature = "postgres")]
pub use storage::postgres::PostgresStorage;
pub use storage::records::{
    AttemptFence, CircuitBreakerState, JobError, JobStep, LockInfo, NewJobStep, NewPeriodicTask,
    NewSubscription, PeriodicTask, RateLimitState, ReplayEntry, SleepOutcome, StepCommit, StepKind,
    Subscription, TaskLogEntry, TaskMetric, WorkerInfo,
};
#[cfg(feature = "redis")]
pub use storage::redis_backend::RedisStorage;
pub use storage::sqlite::SqliteStorage;
pub use storage::Storage;
pub use storage::StorageBackend;
pub use storage::{DeadJob, QueueStats, SubscriptionBacklogStats};
pub use worker::registry_fingerprint;
pub use worker::{
    AttachAddress, AttachError, AttachedExecutor, Capacity, Dispatch, ExecutorClient,
    ExecutorConfig, ExecutorError, ExecutorHandle, ExecutorMessage, ExecutorSession,
    ExecutorSideChannel, ExecutorStepStore, ExecutorSteps, HelloBuilder, NativeDispatcher,
    ProtocolError, RemoteConfig, RemoteDispatcher, SchedulerMessage, Secret, SideChannel,
    StepRelay, StorageSideChannel, TaskError, TaskRegistry, TaskResult, Transport, Worker,
    WorkerDispatcher, WorkerHandle, CAP_SIDE_CHANNEL, CAP_STEPS, PROTOCOL_VERSION,
};
