#![doc = include_str!("../README.md")]
// `deny`, not `warn`: rustdoc's own lints are gated in CI, but `missing_docs` is
// a rustc lint no CI flag turns on, so the crate has to gate itself or the
// coverage silently rots back down.
#![deny(missing_docs)]

/// The contract level a deployment requires, and the floor that enforces it.
pub mod contract;
/// Error types: [`QueueError`] and the crate-wide [`Result`] alias.
pub mod error;
/// Job lifecycle events sent out as CloudEvents: [`EventHub`] and its sinks.
pub mod events;
/// Outbound HTTP: the egress guard and the client every dialled URL goes through.
#[cfg(feature = "http-target")]
pub mod http;
/// Core job model: [`Job`], [`JobStatus`], [`NewJob`], [`JobCompletion`].
pub mod job;
/// The lease on one dispatch of one job: [`Lease`], [`LeaseBook`].
pub mod lease;
/// Facts about IP space, shared by every outbound guard in the workspace.
pub mod net;
/// Settings keys for task and queue runtime overrides.
pub mod overrides;
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
/// Writing the cross-SDK payload envelope: [`wire::encode_call`].
pub mod wire;
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
pub use events::{
    EventHub, EventTap, EventType, EventsConfig, EventsConfigError, JobEvent, SinkStats,
};
#[cfg(feature = "http-target")]
pub use http::auth::{AuthError, OutboundAuth, Signer, SigningRequest};
#[cfg(feature = "http-target")]
pub use http::{DispatchClient, EgressPolicy, EgressRefusal};
pub use job::{now_millis, Job, JobCompletion, JobStatus, NewJob};
pub use lease::{lease_authorizes, mint_claim_epoch, Lease, LeaseBook, MAX_LEASE_EXTENSION};
pub use overrides::{override_key, override_prefix, OverrideScope};
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
    AttemptFence, CircuitBreakerState, DebounceOptions, Dequeued, JobError, JobStep, LockInfo,
    NewJobStep, NewPeriodicTask, NewSubscription, PeriodicTask, RateLimitState, ReplayEntry,
    SettleClaimant, SettleGrant, SleepOutcome, StaleJob, StepCommit, StepKind, Subscription,
    SubscriptionMode, TaskLogEntry, TaskMetric, Topic, TopicLogStats, TopicMessage, WorkerInfo,
    WorkerRegistration, WorkerStatus,
};
#[cfg(feature = "redis")]
pub use storage::redis_backend::{RedisConnection, RedisStorage};
pub use storage::sqlite::SqliteStorage;
pub use storage::Storage;
pub use storage::StorageBackend;
pub use storage::{DeadJob, QueueStats, SubscriptionBacklogStats};
#[cfg(feature = "http-target")]
pub use worker::http_target::{
    HttpDispatchTarget, HttpTargetConfig, HttpTargetError, SettleRefused, SettledOutcome,
};
pub use worker::registry_fingerprint;
pub use worker::{
    AttachAddress, AttachError, AttachedExecutor, Capacity, Dispatch, ExecutorClient,
    ExecutorConfig, ExecutorError, ExecutorHandle, ExecutorMessage, ExecutorSession,
    ExecutorSideChannel, ExecutorStepStore, ExecutorSteps, HelloBuilder, NativeDispatcher,
    ProtocolError, RemoteConfig, RemoteDispatcher, SchedulerMessage, Secret, SideChannel,
    StepRelay, StorageSideChannel, TaskError, TaskHandler, TaskRegistry, TaskResult, Transport,
    Worker, WorkerDispatcher, WorkerHandle, CAP_SIDE_CHANNEL, CAP_STEPS, PROTOCOL_VERSION,
};
