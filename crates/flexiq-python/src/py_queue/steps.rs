//! What a queue handle can answer about durable steps.
//!
//! Not much, on purpose: a session is fenced on the claim a *worker* won, so it
//! is opened on [`PyWorkerSteps`](crate::py_worker_steps::PyWorkerSteps) rather
//! than here. What is left is the capability probe, and the door a prefork child
//! resolves its inherited claim through.

use pyo3::prelude::*;

use flexiq_core::storage::Storage;

use super::PyQueue;
use crate::py_worker_steps::PyWorkerSteps;

#[pymethods]
#[allow(clippy::useless_conversion)]
impl PyQueue {
    /// Whether this backend implements a step store at all.
    ///
    /// Exposed so the shell can answer "can this task use steps" without
    /// opening a session, which costs a job read.
    pub fn supports_steps(&self) -> bool {
        self.storage.supports_steps()
    }

    /// The step handle this process was spawned with, if it holds a claim.
    ///
    /// A prefork child's only route to one: its parent won the claim and passed
    /// the owner on the spawn. `None` in every other process — a plain
    /// enqueue-side `Queue`, or an attached executor — and durable steps refuse
    /// rather than run un-memoized.
    pub fn inherited_worker_steps(&self) -> Option<PyWorkerSteps> {
        PyWorkerSteps::inherited(self.storage.clone(), self.namespace.clone())
    }
}
