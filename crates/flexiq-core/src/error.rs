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

    /// The storage requires a newer contract level than this build speaks.
    #[error(
        "storage requires contract {required}, but this build speaks contract {speaks}:          upgrade this process, or lower the floor from one that is already current"
    )]
    ContractTooOld {
        /// The level this build implements.
        speaks: u32,
        /// The level the storage requires.
        required: u32,
    },

    /// A step write lost its fence: the job is proceeding under another owner,
    /// or has moved past the attempt the writer claimed under.
    ///
    /// The attempt that sees this must make no further contribution — it emits
    /// no result and changes no job state, because failing the job would kill a
    /// run proceeding correctly elsewhere.
    #[error("execution claim lost for job {0}")]
    ClaimLost(String),

    /// A step commit does not match what is already stored at its position:
    /// a different key, or the same key with a different kind.
    #[error(
        "step divergence on job {job_id} at position {seq}: expected '{expected}', found '{found}'"
    )]
    StepDiverged {
        /// Job whose step sequence diverged.
        job_id: String,
        /// Position the mismatch was found at.
        seq: i32,
        /// What is already stored there.
        expected: String,
        /// What the commit tried to write.
        found: String,
    },

    /// A step commit is over one of the caps. Refused, never spilled: there is
    /// nowhere to spill to, and the same database under a different key is not
    /// a spill.
    #[error("step '{step_key}' exceeds the {limit} limit: {actual} > {allowed}")]
    StepLimitExceeded {
        /// Step the commit named.
        step_key: String,
        /// Which cap was hit — `step bytes`, `total bytes`, or `step count`.
        limit: String,
        /// The value the commit would have produced.
        actual: u64,
        /// The cap it was measured against.
        allowed: u64,
    },

    /// Any other failure that fits no specific variant.
    #[error("{0}")]
    Other(String),
}

/// Crate-wide result alias over [`QueueError`].
pub type Result<T> = std::result::Result<T, QueueError>;
