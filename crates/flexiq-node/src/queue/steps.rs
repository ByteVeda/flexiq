//! Opening a durable-step session for one attempt.
//!
//! The only place a [`StepSession`](flexiq_core::step::StepSession) is built on
//! the Node shell, and the only place its fence inputs come from. Both are
//! resolved here rather than accepted from the caller: the owner is the worker
//! id this process claims execution under, and the attempt is checked against
//! the job row before a session is handed out.

use flexiq_core::error::QueueError;
use flexiq_core::step::{StepLimits, StepSession};
use flexiq_core::storage::Storage;
use napi::bindgen_prelude::{spawn_blocking, Result};
use napi_derive::napi;

use super::JsQueue;
use crate::error::join_to_napi_err;
use crate::steps::{step_error, JsStepSession};

#[napi]
impl JsQueue {
    /// Open the durable-step session for one attempt of `jobId`.
    ///
    /// `attempt` is the `retryCount` the job was dispatched with. It is checked
    /// against the row rather than trusted: an attempt that has been superseded
    /// — reclaimed by another worker, or retried past this one — must not write
    /// into the live attempt's sequence, and finding out here gives a clearer
    /// error than the storage fence's refusal on the first commit.
    ///
    /// Refuses when this queue holds no execution claim, which is a queue that
    /// never started a worker. An attached executor never reaches this at all:
    /// it has no channel to commit a step on, so it fails the attempt without
    /// opening a session, and never runs a step un-memoized.
    ///
    /// One owner per queue handle. Two workers started from the *same* `Queue`
    /// share this slot, so the older one's commits are refused by the storage
    /// fence as superseded — nothing is ever written under the wrong claim, but
    /// such a job fails rather than running. Give each worker its own queue
    /// handle when tasks use steps.
    #[napi]
    pub async fn open_step_session(&self, job_id: String, attempt: i32) -> Result<JsStepSession> {
        let storage = self.storage.clone();
        let namespace = self.namespace.clone();
        let owner = self
            .claim_owner
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
            .ok_or_else(|| {
                step_error(QueueError::Other(format!(
                    "job {job_id} uses durable steps, but this queue holds no execution claim to \
                     fence them on. Durable steps need a worker that reaches storage directly; \
                     an attached executor commits nothing and would re-run every step."
                )))
            })?;

        spawn_blocking(move || {
            let job = storage
                .get_job(&job_id, namespace.as_deref())
                .map_err(step_error)?
                .ok_or_else(|| {
                    step_error(QueueError::ClaimLost(format!(
                        "job {job_id} no longer exists"
                    )))
                })?;

            if job.retry_count != attempt {
                // Reported as a lost claim, which is what it is: this attempt
                // is not the one the job is on, so its writes are refused and
                // its result is dropped by the scheduler's own fence.
                return Err(step_error(QueueError::ClaimLost(format!(
                    "job {job_id} is on attempt {} and this one is {attempt}",
                    job.retry_count
                ))));
            }

            // The defaults, not a caller-supplied value: §4.2's answer to a
            // result that will not fit is to store it elsewhere and memoize the
            // handle, not to raise the cap, so there is nothing for a shell
            // knob to do yet.
            let session = StepSession::load(storage, &job, &owner, StepLimits::default())
                .map_err(step_error)?;
            Ok(JsStepSession::new(session))
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Whether this backend implements a step store at all.
    ///
    /// Exposed so the shell can answer "can this task use steps" without
    /// opening a session, which costs a job read.
    #[napi]
    pub fn supports_steps(&self) -> bool {
        self.storage.supports_steps()
    }
}
