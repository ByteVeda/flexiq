//! Where a [`StepSession`](super::StepSession)'s writes actually land.
//!
//! A session is the *rules* — identity, the sequence, the caps, the divergence
//! check. This trait is the four operations those rules eventually perform, and
//! splitting them out is what lets one implementation of the rules serve both
//! deployment shapes: a worker holding a database connection, and an attached
//! executor holding only a socket to the scheduler that holds one.
//!
//! **The fence lives in the store, not in the session.** `(owner, attempt)` is
//! what proves a write still speaks for the live attempt (§1.4), and only the
//! side that *won the claim* knows it. A worker with storage knows it and
//! carries it here in [`StorageSteps`]; an attached executor does not, and must
//! not — an owner an executor fills in is an owner it can forge, and a forged
//! one writes straight into the live attempt's sequence. Its store therefore
//! has no owner field at all, and the scheduler supplies one from its own
//! dispatch record. The type says so, so no caller can pass the wrong thing.

use crate::error::Result;
use crate::step::StepLimits;
use crate::storage::records::{JobStep, NewJobStep, SleepOutcome, StepCommit};
use crate::storage::Storage;

/// The step operations a [`StepSession`](super::StepSession) performs.
///
/// Every method that reads or writes a step row is here and nowhere else, so a
/// new transport for durable steps is one implementation rather than a second
/// copy of the rules.
pub trait StepStore {
    /// Whether steps can be committed through this store at all.
    ///
    /// `false` refuses the inline-step API outright. It must never degrade to
    /// "no memo recorded": a step store that fails open re-runs a charge.
    fn supports_steps(&self) -> bool;

    /// Every committed step for a job, ordered by `seq`. Read **once** per
    /// attempt (§5.1).
    fn load_steps(&self, job_id: &str, namespace: Option<&str>) -> Result<Vec<JobStep>>;

    /// Commit one step, fenced on the writer still owning the execution claim.
    ///
    /// A byte-identical re-commit at the same position is
    /// [`StepCommit::AlreadyCommitted`], which is a success; anything else
    /// stored there is [`QueueError::StepDiverged`](crate::error::QueueError::StepDiverged).
    fn commit_step(
        &self,
        step: &NewJobStep<'_>,
        limits: &StepLimits,
        namespace: Option<&str>,
    ) -> Result<StepCommit>;

    /// End the attempt in a sleep: commit the sleep row, release the execution
    /// claim and reschedule the job, as one fenced operation.
    ///
    /// `wake_at` is a *candidate*: a sleep row already committed at this
    /// position keeps the deadline it was first given.
    fn commit_sleep(
        &self,
        step: &NewJobStep<'_>,
        wake_at: i64,
        limits: &StepLimits,
        namespace: Option<&str>,
    ) -> Result<SleepOutcome>;
}

/// A [`StepStore`] over storage this process can reach, fenced on the claim the
/// writer won.
///
/// `owner` is never something the running code asserts about itself: in-process
/// and prefork workers pass the id they won the claim with, and `attempt` is the
/// `retry_count` the job carried at claim time. A step written by a superseded
/// attempt is refused by the fence rather than landing in the live attempt's
/// sequence.
pub struct StorageSteps<S: Storage> {
    storage: S,
    owner: String,
    attempt: i32,
}

impl<S: Storage> StorageSteps<S> {
    /// Fence writes through `storage` on `(owner, attempt)`.
    pub fn new(storage: S, owner: impl Into<String>, attempt: i32) -> Self {
        Self {
            storage,
            owner: owner.into(),
            attempt,
        }
    }

    /// The worker id these writes are fenced on.
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// The attempt these writes are fenced on.
    pub fn attempt(&self) -> i32 {
        self.attempt
    }
}

impl<S: Storage> StepStore for StorageSteps<S> {
    fn supports_steps(&self) -> bool {
        self.storage.supports_steps()
    }

    fn load_steps(&self, job_id: &str, namespace: Option<&str>) -> Result<Vec<JobStep>> {
        self.storage.get_job_steps(job_id, namespace)
    }

    fn commit_step(
        &self,
        step: &NewJobStep<'_>,
        limits: &StepLimits,
        namespace: Option<&str>,
    ) -> Result<StepCommit> {
        self.storage
            .record_step_result(step, &self.owner, self.attempt, limits, namespace)
    }

    fn commit_sleep(
        &self,
        step: &NewJobStep<'_>,
        wake_at: i64,
        limits: &StepLimits,
        namespace: Option<&str>,
    ) -> Result<SleepOutcome> {
        self.storage
            .sleep_job(step, &self.owner, self.attempt, wake_at, limits, namespace)
    }
}
