//! Opening a durable-step session for one attempt.
//!
//! The only place a [`StepSession`](flexiq_core::step::StepSession) is built on
//! the Python shell, and the only place its fence inputs come from. Both are
//! resolved here rather than accepted from the caller: the owner is the worker
//! id this process claims execution under, and the attempt is checked against
//! the job row before a session is handed out.

use pyo3::prelude::*;

use flexiq_core::error::QueueError;
use flexiq_core::step::{StepLimits, StepSession};
use flexiq_core::storage::Storage;

use super::PyQueue;
use crate::py_step::{step_error, PyStepSession};

#[pymethods]
#[allow(clippy::useless_conversion)]
impl PyQueue {
    /// Open the durable-step session for one attempt of `job_id`.
    ///
    /// `attempt` is the `retry_count` the job was dispatched with. It is
    /// checked against the row rather than trusted: an attempt that has been
    /// superseded — reclaimed by another worker, or retried past this one —
    /// must not write into the live attempt's sequence, and finding out here
    /// gives a clearer error than the storage fence's refusal on the first
    /// commit.
    ///
    /// Refuses when this process holds no execution claim. That is an attached
    /// executor, which has no channel to commit a step on: it fails the attempt
    /// so a later one may land on a worker that can, and never runs the step
    /// un-memoized.
    #[pyo3(signature = (job_id, attempt, namespace=None))]
    pub fn open_step_session(
        &self,
        py: Python<'_>,
        job_id: &str,
        attempt: i32,
        namespace: Option<&str>,
    ) -> PyResult<PyStepSession> {
        let owner = self
            .claim_owner
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
            .ok_or_else(|| {
                step_error(
                    py,
                    QueueError::Other(format!(
                        "job {job_id} uses durable steps, but this worker holds no execution \
                         claim to fence them on. Durable steps need a worker that reaches \
                         storage directly; an attached executor commits nothing and would \
                         re-run every step."
                    )),
                )
            })?;

        let job = self
            .storage
            .get_job(job_id, namespace)
            .map_err(|error| step_error(py, error))?
            .ok_or_else(|| {
                step_error(
                    py,
                    QueueError::ClaimLost(format!("job {job_id} no longer exists")),
                )
            })?;

        if job.retry_count != attempt {
            // Reported as a lost claim, which is what it is: this attempt is
            // not the one the job is on, so its writes are refused and its
            // result is dropped by the scheduler's own fence.
            return Err(step_error(
                py,
                QueueError::ClaimLost(format!(
                    "job {job_id} is on attempt {} and this one is {attempt}",
                    job.retry_count
                )),
            ));
        }

        // The defaults, not a caller-supplied value: §4.2's answer to a result
        // that will not fit is to store it elsewhere and memoize the handle,
        // not to raise the cap, so there is nothing for a shell knob to do yet.
        let limits = StepLimits::default();
        let session = StepSession::load(self.storage.clone(), &job, &owner, limits)
            .map_err(|error| step_error(py, error))?;
        Ok(PyStepSession::new(session))
    }

    /// Whether this backend implements a step store at all.
    ///
    /// Exposed so the shell can answer "can this task use steps" without
    /// opening a session, which costs a job read.
    pub fn supports_steps(&self) -> bool {
        self.storage.supports_steps()
    }
}
