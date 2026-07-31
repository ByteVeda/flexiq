use thiserror::Error;

/// Every error the queue can produce.
#[derive(Error, Debug)]
pub enum QueueError {
    /// A Diesel query or transaction failed.
    #[error("storage error: {0}")]
    Storage(#[from] diesel::result::Error),

    /// The r2d2 connection pool could not hand out a connection.
    #[error("connection pool error: {0}")]
    Pool(#[from] diesel::r2d2::PoolError),

    /// A Redis command or connection failed.
    #[cfg(feature = "redis")]
    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),

    /// JSON encoding or decoding failed.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// No job exists with the given id.
    #[error("job not found: {0}")]
    JobNotFound(String),

    /// A job referenced a task name with no registered handler.
    #[error("task not registered: {0}")]
    TaskNotRegistered(String),

    /// A payload or result could not be serialized/deserialized.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// A worker-side failure (spawn, dispatch, or pool error).
    #[error("worker error: {0}")]
    Worker(String),

    /// A scheduler-side failure (dispatch, maintenance, or config).
    #[error("scheduler error: {0}")]
    Scheduler(String),

    /// A queue/task rate limit rejected the operation.
    #[error("rate limit exceeded for: {0}")]
    RateLimitExceeded(String),

    /// A job exceeded its execution timeout.
    #[error("job timed out: {0}")]
    Timeout(String),

    /// Invalid or inconsistent configuration.
    #[error("config error: {0}")]
    Config(String),

    /// A job dependency id does not exist.
    #[error("dependency not found: {0}")]
    DependencyNotFound(String),

    /// A distributed lock was already held by another owner.
    #[error("lock not acquired: {0}")]
    LockNotAcquired(String),

    /// A compare-and-set write lost to a concurrent writer on every attempt.
    #[error("setting '{0}' was changed by another writer on every attempt")]
    SettingConflict(String),

    /// Any other failure that fits no specific variant.
    #[error("{0}")]
    Other(String),
}

/// Crate-wide result alias over [`QueueError`].
pub type Result<T> = std::result::Result<T, QueueError>;
