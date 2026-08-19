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
/// The [`Storage`] trait, backend implementations, and shared records.
pub mod storage;
/// Native worker: task registry, dispatcher trait, worker runner.
pub mod worker;

// Primary public API — the types most consumers need. The crate root is the
// blessed import path; submodules stay public for discoverability but new code
// should prefer these re-exports.
pub use contract::{
    ensure_contract_supported, min_contract, set_min_contract, CONTRACT_VERSION,
    MIN_CONTRACT_VERSION,
};
pub use error::{QueueError, Result};
pub use job::{now_millis, Job, JobCompletion, JobStatus, NewJob};
pub use resilience::circuit_breaker::{CircuitBreakerConfig, CircuitState};
pub use resilience::rate_limiter::RateLimitConfig;
pub use resilience::retry::RetryPolicy;
pub use scheduler::retention::{EffectiveRetention, RetentionConfig};
pub use scheduler::{
    JobResult, QueueConfig, ResultOutcome, Scheduler, SchedulerConfig, TaskConfig,
};
pub use settings::{is_reserved_setting_key, RESERVED_SETTING_PREFIXES};
pub use storage::cursor::Page;
#[cfg(feature = "postgres")]
pub use storage::postgres::PostgresStorage;
pub use storage::records::{
    CircuitBreakerState, JobError, LockInfo, NewPeriodicTask, NewSubscription, PeriodicTask,
    RateLimitState, ReplayEntry, Subscription, TaskLogEntry, TaskMetric, WorkerInfo,
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
    ExecutorSideChannel, HelloBuilder, NativeDispatcher, ProtocolError, RemoteConfig,
    RemoteDispatcher, SchedulerMessage, Secret, SideChannel, StorageSideChannel, TaskError,
    TaskRegistry, TaskResult, Transport, Worker, WorkerDispatcher, WorkerHandle, CAP_SIDE_CHANNEL,
    PROTOCOL_VERSION,
};
