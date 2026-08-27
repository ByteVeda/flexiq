//! One running worker's door to durable steps.
//!
//! Every step write is fenced on `(owner, attempt)`, and the owner is the
//! worker id the job was *claimed* under. That belongs to the worker, not to
//! the `Queue` handle it was started from: one handle can drive several workers
//! (`app.py` documents `queue.run_worker()` in another thread or process), so a
//! single slot on the handle is overwritten by the second `run_worker` and
//! every step the first worker goes on to commit is refused as superseded —
//! nothing written under a wrong claim, but the job dies instead of running.
//!
//! So each worker carries its own handle and the shell supplies only a job id
//! and an attempt. §9.2 still holds: the owner is resolved natively — by
//! `run_worker`, or from the spawn a prefork parent made — and never asserted
//! by task code.

use pyo3::prelude::*;

use flexiq_core::error::QueueError;
use flexiq_core::step::{StepLimits, StorageStepSession};
use flexiq_core::storage::{Storage, StorageBackend};

use crate::py_step::{step_error, PyStepSession, CLAIM_OWNER_ENV};

/// The storage handle and execution claim one worker opens its steps under.
#[pyclass(name = "WorkerSteps", module = "flexiq._flexiq")]
pub struct PyWorkerSteps {
    storage: StorageBackend,
    /// The worker's own namespace, not the running job's: a scheduler only ever
    /// dispatches from the namespace it polls, and a namespace the shell passes
    /// in is one it can get wrong.
    namespace: Option<String>,
    owner: String,
}

impl PyWorkerSteps {
    pub(crate) fn new(storage: StorageBackend, namespace: Option<String>, owner: String) -> Self {
        Self {
            storage,
            namespace,
            owner,
        }
    }

    /// The handle a prefork child inherited from the pool that spawned it.
    ///
    /// The one hop the owner is allowed to travel: the parent won the claim and
    /// the spawn is private to the pair. A frame field would also reach an
    /// attached executor, and an owner an executor supplies is one it can
    /// forge. `None` when this process holds no claim, which is exactly when
    /// durable steps must refuse.
    pub(crate) fn inherited(storage: StorageBackend, namespace: Option<String>) -> Option<Self> {
        std::env::var(CLAIM_OWNER_ENV)
            .ok()
            .filter(|owner| !owner.is_empty())
            .map(|owner| Self::new(storage, namespace, owner))
    }
}

#[pymethods]
#[allow(clippy::useless_conversion)]
impl PyWorkerSteps {
    /// Open the durable-step session for one attempt of `job_id`.
    ///
    /// `attempt` is the `retry_count` the job was dispatched with. It is
    /// checked against the row rather than trusted: an attempt that has been
    /// superseded — reclaimed by another worker, or retried past this one —
    /// must not write into the live attempt's sequence, and finding out here
    /// gives a clearer error than the storage fence's refusal on the first
    /// commit.
    pub fn open_step_session(
        &self,
        py: Python<'_>,
        job_id: &str,
        attempt: i32,
    ) -> PyResult<PyStepSession> {
        // Two synchronous round trips; holding the GIL across them would stall
        // every other job running in this worker, exactly as the session's own
        // methods avoid.
        let session = py.detach(
            || -> Result<StorageStepSession<StorageBackend>, QueueError> {
                let job = self
                    .storage
                    .get_job(job_id, self.namespace.as_deref())?
                    .ok_or_else(|| {
                        QueueError::ClaimLost(format!("job {job_id} no longer exists"))
                    })?;

                if job.retry_count != attempt {
                    // Reported as a lost claim, which is what it is: this attempt is
                    // not the one the job is on, so its writes are refused and its
                    // result is dropped by the scheduler's own fence.
                    return Err(QueueError::ClaimLost(format!(
                        "job {job_id} is on attempt {} and this one is {attempt}",
                        job.retry_count
                    )));
                }

                // The defaults, not a caller-supplied value: §4.2's answer to a
                // result that will not fit is to store it elsewhere and memoize the
                // handle, not to raise the cap, so there is nothing for a shell
                // knob to do yet.
                StorageStepSession::load(
                    self.storage.clone(),
                    &job,
                    &self.owner,
                    StepLimits::default(),
                )
            },
        );

        session
            .map(PyStepSession::new)
            .map_err(|error| step_error(py, error))
    }

    /// The worker id every step opened here is fenced on.
    ///
    /// Read-only, and read from the worker rather than set by it — a caller
    /// that could name an owner could write into another worker's attempt.
    #[getter]
    pub fn owner(&self) -> &str {
        &self.owner
    }
}
