//! Durable inline steps, exposed to Python.
//!
//! The rules live in `flexiq_core::step` and stay there — this module is the
//! split form of [`StepSession`] the design calls for: `begin_run` decides,
//! Python runs the closure and encodes the result with the queue's own
//! serializer and codec chain, `commit_run` stores exactly those bytes. The
//! core never sees inside them, which is what makes an encrypting codec work
//! here with no extra plumbing.
//!
//! Nothing in here reads an owner or an attempt from the running code. Both
//! are resolved by the worker that won the execution claim and handed in by
//! [`PyWorkerSteps::open_step_session`](crate::py_worker_steps::PyWorkerSteps),
//! because an owner task code can assert is an
//! owner it can forge — and a forged one writes straight into the live
//! attempt's sequence.

use std::sync::Mutex;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

use flexiq_core::error::QueueError;
use flexiq_core::step::{
    classify_step_failure, PendingStep, StepDecision, StepFailure, StepKey, StepSleep,
    StorageStepSession,
};
use flexiq_core::storage::StorageBackend;

/// Attribute every step exception carries, naming what the attempt should do.
///
/// Read by each worker path before it consults `retry_on` / `dont_retry_on`:
/// the core has already classified the failure, and a task's retry filter has
/// no opinion worth overriding it with.
pub(crate) const SHOULD_RETRY_ATTR: &str = "flexiq_should_retry";

/// Environment variable a prefork parent hands a child the claim owner in.
///
/// The one hop the owner is allowed to travel: the parent won the claim, and
/// the spawn is private to the pair. A frame field would also reach an attached
/// executor, and an owner an executor supplies is one it can get wrong.
pub(crate) const CLAIM_OWNER_ENV: &str = "FLEXIQ_CLAIM_OWNER";

/// Turn a core step error into the Python exception its class deserves,
/// stamped with the retry decision from `classify_step_failure`.
///
/// A `Superseded` failure gets its own class. The attempt still reports a
/// failure — every Python worker path owes the scheduler a result — but
/// `handle_result` fences on `(owner, attempt)` before it mutates anything, so
/// the result is dropped rather than allowed to kill a run proceeding
/// correctly under another owner.
pub(crate) fn step_error(py: Python<'_>, error: QueueError) -> PyErr {
    let failure = classify_step_failure(&error);
    let class_name = match failure {
        StepFailure::Superseded => "StepSupersededError",
        StepFailure::Permanent => match error {
            QueueError::StepLimitExceeded { .. } => "StepLimitExceededError",
            QueueError::StepDiverged { .. } | QueueError::StepSequenceDiverged(_) => {
                "StepDivergedError"
            }
            _ => "StepError",
        },
        StepFailure::Retryable => "StepUnavailableError",
    };

    let message = error.to_string();
    let built = (|| -> PyResult<PyErr> {
        let module = py.import("flexiq.steps.errors")?;
        let class = module.getattr(class_name)?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("should_retry", failure.should_retry())?;
        let instance = class.call((message.as_str(),), Some(&kwargs))?;
        Ok(PyErr::from_value(instance))
    })();

    built.unwrap_or_else(|import_error| {
        // The step modules ship with the wheel, so this is unreachable in a
        // working install. Falling back to a bare RuntimeError keeps the
        // failure legible instead of replacing it with an ImportError that
        // says nothing about the step.
        let _ = import_error;
        pyo3::exceptions::PyRuntimeError::new_err(message)
    })
}

/// Derive a step's identity under the core's rules, without a session.
///
/// For the inline path only. Test mode has no session to derive through, and
/// hand-rolling the rules in the shell would drift: an empty `key=""` is
/// refused here and was silently renumbered by occurrence before this existed,
/// so a test passed for a key a worker rejects. Same rules, one place.
#[pyfunction]
#[pyo3(signature = (name, key=None, occurrence=0))]
pub fn derive_step_key(
    py: Python<'_>,
    name: &str,
    key: Option<&str>,
    occurrence: u32,
) -> PyResult<String> {
    match key {
        Some(key) => StepKey::explicit(name, key),
        None => StepKey::derive(name, occurrence),
    }
    .map_err(|error| step_error(py, error))
}

/// What `begin_run` decided, and the token `commit_run` needs back.
///
/// The pending step is kept inside rather than handed to Python as a `(seq,
/// key)` pair a caller could invent: the position is the core's to assign, and
/// storage refuses a commit that does not match the rows it already holds.
#[pyclass(name = "StepDecision", module = "flexiq._flexiq")]
pub struct PyStepDecision {
    /// The stored bytes when this step already ran. `None` means new ground.
    memoized: Option<Vec<u8>>,
    pending: Option<PendingStep>,
    step_key: String,
    idempotency_key: String,
}

#[pymethods]
impl PyStepDecision {
    /// The step's stored result, or `None` when the closure must run.
    #[getter]
    fn memoized<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        self.memoized.as_ref().map(|bytes| PyBytes::new(py, bytes))
    }

    /// Identity of this step — `name#occurrence`, or the explicit key.
    #[getter]
    fn step_key(&self) -> &str {
        &self.step_key
    }

    /// The key to hand the downstream service for this step.
    #[getter]
    fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
}

/// Outcome of a sleep, as the shell sees it.
///
/// `elapsed` means the deadline had already passed and the attempt carries on;
/// otherwise the job is `Pending` at `wake_at` and the task body must unwind
/// now — anything it does past this point runs unclaimed and runs again on
/// wake.
#[pyclass(name = "StepSleepOutcome", module = "flexiq._flexiq")]
pub struct PyStepSleep {
    #[pyo3(get)]
    elapsed: bool,
    #[pyo3(get)]
    step_key: String,
    #[pyo3(get)]
    wake_at: i64,
}

impl PyStepSleep {
    fn from_core(sleep: StepSleep) -> Self {
        match sleep {
            StepSleep::Elapsed { step_key, wake_at } => Self {
                elapsed: true,
                step_key,
                wake_at,
            },
            StepSleep::Sleeping { step_key, wake_at } => Self {
                elapsed: false,
                step_key,
                wake_at,
            },
        }
    }
}

/// One attempt's durable steps.
///
/// Built by [`PyWorkerSteps::open_step_session`](crate::py_worker_steps::PyWorkerSteps),
/// which is the only place the owner and the attempt come from — the owner off
/// the worker that won the claim, never off the queue handle it was started
/// from. The mutex is for pyo3's `Sync` requirement, not
/// for concurrency: a session belongs to one attempt on one thread.
#[pyclass(name = "StepSession", module = "flexiq._flexiq")]
pub struct PyStepSession {
    inner: Mutex<StorageStepSession<StorageBackend>>,
}

impl PyStepSession {
    pub(crate) fn new(session: StorageStepSession<StorageBackend>) -> Self {
        Self {
            inner: Mutex::new(session),
        }
    }

    /// Run `body` against the session with the GIL released.
    ///
    /// Every one of these is a synchronous storage round trip, and holding the
    /// GIL across it would stall every other Python thread in the worker for
    /// the duration — including the ones running other jobs. A poisoned lock
    /// becomes a failure rather than a panic, which would take the worker
    /// thread with it.
    fn with<T: Send>(
        &self,
        py: Python<'_>,
        body: impl FnOnce(&mut StorageStepSession<StorageBackend>) -> Result<T, QueueError> + Send,
    ) -> PyResult<T> {
        let outcome = py.detach(|| {
            let mut guard = self
                .inner
                .lock()
                .map_err(|_| QueueError::Other("step session is poisoned".into()))?;
            body(&mut guard)
        });
        outcome.map_err(|error| step_error(py, error))
    }
}

#[pymethods]
impl PyStepSession {
    /// Decide what this step must do, without running anything.
    ///
    /// A divergence surfaces here — before the closure runs — which is the
    /// point of checking each step as it is asked for rather than at the end.
    #[pyo3(signature = (name, key=None))]
    fn begin_run(&self, py: Python<'_>, name: &str, key: Option<&str>) -> PyResult<PyStepDecision> {
        let (decision, idempotency_key) = self.with(py, |session| {
            let decision = session.begin_run(name, key)?;
            let step_key = match &decision {
                StepDecision::Memoized { step_key, .. } => step_key.clone(),
                StepDecision::Run(pending) => pending.step_key().to_string(),
            };
            let idempotency_key = session.idempotency_key(&step_key);
            Ok((decision, idempotency_key))
        })?;

        Ok(match decision {
            StepDecision::Memoized { step_key, result } => PyStepDecision {
                memoized: Some(result),
                pending: None,
                step_key,
                idempotency_key,
            },
            StepDecision::Run(pending) => PyStepDecision {
                memoized: None,
                step_key: pending.step_key().to_string(),
                pending: Some(pending),
                idempotency_key,
            },
        })
    }

    /// Commit the encoded result of the step `begin_run` handed out.
    ///
    /// `encoded` is post-serializer, post-codec: those are the bytes stored,
    /// and the bytes the caps are measured on.
    fn commit_run(
        &self,
        py: Python<'_>,
        decision: &PyStepDecision,
        encoded: &[u8],
    ) -> PyResult<()> {
        let Some(pending) = decision.pending.clone() else {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "commit_run was given a memoized decision, which has nothing to commit",
            ));
        };
        self.with(py, |session| {
            session.commit_run(&pending, encoded)?;
            Ok(())
        })
    }

    /// Sleep for `duration_ms`, ending the attempt if the deadline is ahead.
    ///
    /// The clock is read once, inside the core, and the deadline it produces is
    /// only a candidate — storage keeps whatever a sleep row at this position
    /// already holds, so a replayed `sleep("1h")` wakes at the original instant
    /// rather than an hour later every time the job crashed into it.
    #[pyo3(signature = (duration_ms, name=None, key=None))]
    fn sleep_for(
        &self,
        py: Python<'_>,
        duration_ms: i64,
        name: Option<&str>,
        key: Option<&str>,
    ) -> PyResult<PyStepSleep> {
        self.with(py, |session| session.sleep_for(name, key, duration_ms))
            .map(PyStepSleep::from_core)
    }

    /// Sleep until an absolute instant, in Unix milliseconds.
    #[pyo3(signature = (wake_at, name=None, key=None))]
    fn sleep_until(
        &self,
        py: Python<'_>,
        wake_at: i64,
        name: Option<&str>,
        key: Option<&str>,
    ) -> PyResult<PyStepSleep> {
        self.with(py, |session| session.sleep_until(name, key, wake_at))
            .map(PyStepSleep::from_core)
    }

    /// The id this durable run began under — the job's own, except across a
    /// `retry_dead`, which mints a new job for the same run.
    fn run_key(&self, py: Python<'_>) -> PyResult<String> {
        self.with(py, |session| Ok(session.run_key().to_string()))
    }

    /// The key to hand the downstream service for `step_key`.
    fn idempotency_key(&self, py: Python<'_>, step_key: &str) -> PyResult<String> {
        self.with(py, |session| Ok(session.idempotency_key(step_key)))
    }

    /// Close the attempt out, warning if the job has recorded steps this code
    /// no longer runs. Never raises: the side effects already happened.
    fn finish(&self) {
        if let Ok(session) = self.inner.lock() {
            session.finish();
        }
    }
}
