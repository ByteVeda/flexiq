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

    /// An enqueue was refused because the queue is at its admission cap.
    ///
    /// Only a write that storage itself has to decide raises this — a debounced
    /// enqueue, whose slide-or-insert branch is not knowable outside the
    /// transaction. A caller that can tell it is inserting checks the cap
    /// itself and never gets here.
    ///
    /// The message is a cross-SDK contract: an SDK reads `pending` and `cap`
    /// back off it to rebuild its own typed rejection, so the two integers stay
    /// at the end where a queue name can never be mistaken for them.
    #[error("queue '{queue}' is full: {pending} pending >= max_pending {cap}")]
    QueueFull {
        /// The queue that refused the enqueue.
        queue: String,
        /// Pending rows counted on that queue, inside the write.
        pending: i64,
        /// The cap the count was measured against.
        cap: i64,
    },

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
        "step divergence on job {job_id} at position {seq}: expected {expected}, found {found}"
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

    /// The running code asked for a different step than the one recorded at
    /// that position: the step sequence changed between attempts.
    #[error("{0}")]
    StepSequenceDiverged(Box<StepDivergence>),

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

    /// A step commit was refused for a reason that will not be different next
    /// attempt, by a side of a process boundary this one cannot see.
    ///
    /// An attached executor's commits are applied by the scheduler, which sends
    /// back the message and the classification but not the error itself. This
    /// is what a permanent refusal arrives as: the message reads whole, and
    /// [`classify_step_failure`](crate::step::classify_step_failure) still
    /// answers "do not retry".
    #[error("{0}")]
    StepRefused(String),

    /// Any other failure that fits no specific variant.
    #[error("{0}")]
    Other(String),
}

/// Why one attempt's step sequence no longer matches the recorded one.
///
/// Boxed inside [`QueueError::StepSequenceDiverged`]: it is the crate's widest
/// error payload, and every `Result` in the crate would otherwise carry its
/// size. Caught in memory, against the snapshot loaded at attempt start, so it
/// fails before the closure runs rather than after it has charged a card. Both
/// sequences are in the message because the difference between them is the only
/// thing that identifies which deploy did it.
#[derive(Error, Debug)]
#[error(
    "step sequence changed for job {job_id} at position {position}\n\
     \x20 recorded: {recorded}\n\
     \x20 running:  {running}\n\
     \x20 step {position} was {expected}, now {found}\n\
     A memoized result would answer a different question than the step asking \
     for it. Drain or dead-letter this task's in-flight jobs before deploying \
     a change to its step sequence."
)]
pub struct StepDivergence {
    /// Job whose step sequence changed.
    pub job_id: String,
    /// Position the two sequences first disagree at.
    pub position: usize,
    /// The recorded sequence, around `position`.
    pub recorded: String,
    /// The sequence this attempt asked for, around `position`.
    pub running: String,
    /// What is recorded at `position`.
    pub expected: String,
    /// What this attempt asked for there.
    pub found: String,
}

/// Crate-wide result alias over [`QueueError`].
pub type Result<T> = std::result::Result<T, QueueError>;

#[cfg(test)]
mod tests {
    use super::QueueError;

    /// Every SDK reads `pending` and `cap` back off this message to rebuild its
    /// own typed rejection — there is no typed channel to use instead, since a
    /// binding's fast path may carry only a string. Changing the wording
    /// silently turns a full queue into an unrecognised failure in three
    /// shells, so it is pinned here alongside the parsers that consume it.
    #[test]
    fn queue_full_message_is_the_cross_sdk_wire() {
        let refused = QueueError::QueueFull {
            // A queue name that itself reads like the tail the parsers anchor
            // on: they match the *end* of the message, so it cannot fool them.
            queue: "is full: 9 pending >= max_pending 9".to_string(),
            pending: 4,
            cap: 4,
        };
        assert_eq!(
            refused.to_string(),
            "queue 'is full: 9 pending >= max_pending 9' is full: 4 pending >= max_pending 4"
        );
        assert!(refused.to_string().ends_with("4 pending >= max_pending 4"));
    }
}
