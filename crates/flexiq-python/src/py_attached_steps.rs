//! Durable steps for a task body running under an attached executor.
//!
//! An executor's task bodies run on prefork children, one process further out
//! than the socket. So a child holds neither storage nor the connection: the
//! steps its job already committed ride down the pipe on the dispatch, and each
//! new one is framed back up to the pool, which relays it to the scheduler and
//! returns the answer.
//!
//! What is local is everything that *decides* — step identity, the sequence
//! check, the caps, the divergence rule — because those belong to
//! `StepSession`, which runs wherever its store does. Only the write travels.
//!
//! **Nothing here names an owner.** The fence is the scheduler's, resolved from
//! the dispatch it recorded; an owner this side supplied would be one it could
//! forge. That is why this store has no owner field at all, and why the twin
//! that does ([`PyWorkerSteps`](crate::py_worker_steps::PyWorkerSteps)) is a
//! separate type rather than the same one with a `None`.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyBytes;

use flexiq_core::error::QueueError;
use flexiq_core::job::Job;
use flexiq_core::step::{refusal_error, StepFailure, StepLimits, StepSession, StepStore};
use flexiq_core::storage::records::{JobStep, NewJobStep, SleepOutcome, StepCommit, StepKind};
use flexiq_core::worker::protocol::{decode_step_snapshot, SchedulerMessage};

use crate::py_step::{step_error, BoxedStepSession, BoxedStepStore, PyStepSession};

/// A [`StepStore`] whose writes leave this process through a pipe.
///
/// `load_steps` answers from the snapshot the dispatch carried — there is no
/// read to perform, and no credentials to perform one with. Every commit is a
/// request/response with the pool, and the task is blocked on the answer:
/// an unconfirmed commit is indistinguishable from one that never happened.
struct PipeStepStore {
    /// The Python object that frames a commit and waits for its ack. Duck-typed
    /// on one method (`commit`) so the framing stays where the pipe is.
    relay: Py<PyAny>,
    /// The steps this job's dispatch carried, still encoded. Shared rather than
    /// copied: a snapshot runs to the step store's own ceiling, and a session is
    /// opened once per attempt for bytes nobody mutates.
    snapshot: Arc<Vec<u8>>,
}

impl PipeStepStore {
    /// Hand one commit to the relay and read its ack back.
    ///
    /// Re-acquires the GIL, which the session released before calling in. The
    /// relay parks on an event while it waits, releasing the GIL again, so the
    /// thread reading acks off the pipe can run and answer this call.
    fn commit(
        &self,
        job_id: &str,
        seq: i32,
        step_key: &str,
        kind: StepKind,
        wake_at: Option<i64>,
        result: &[u8],
    ) -> Result<Ack, QueueError> {
        Python::attach(|py| {
            let answer = self
                .relay
                .bind(py)
                .call_method1(
                    "commit",
                    (
                        job_id,
                        seq,
                        step_key,
                        kind.as_str(),
                        wake_at,
                        PyBytes::new(py, result),
                    ),
                )
                .map_err(|error| {
                    // Retryable: the pipe breaking means the answer was lost,
                    // not that the write was refused, and a replay re-runs the
                    // step under the same downstream idempotency key.
                    QueueError::Other(format!(
                        "step '{step_key}' of job {job_id} could not be committed through the \
                         executor: {}",
                        error.value(py)
                    ))
                })?;
            Ack::read(&answer)
        })
    }
}

impl StepStore for PipeStepStore {
    fn supports_steps(&self) -> bool {
        // Reaching here at all means the session opened, and
        // `PyAttachedSteps::open_step_session` refuses before that when the
        // scheduler advertised no step store.
        true
    }

    /// The snapshot the dispatch carried. `namespace` is ignored: the scheduler
    /// scoped the read when it made it, and nothing on this side can re-scope a
    /// read it did not perform.
    ///
    /// No bytes at all means no `job_steps` frame preceded the dispatch, which
    /// is a job with nothing committed — never an unknown snapshot. An encoded
    /// empty one is `[]\n`, so the two cannot be confused; and a snapshot that
    /// arrived *unreadable* still fails here rather than reading as empty,
    /// because empty is the answer that re-runs a charge.
    fn load_steps(
        &self,
        job_id: &str,
        _namespace: Option<&str>,
    ) -> Result<Vec<JobStep>, QueueError> {
        if self.snapshot.is_empty() {
            return Ok(Vec::new());
        }
        decode_step_snapshot(job_id, &self.snapshot)
    }

    /// `limits` are checked by the session before this is called, and again by
    /// the scheduler inside the write. Nothing to send: the caps are the
    /// operator's, and the operator runs the scheduler.
    fn commit_step(
        &self,
        step: &NewJobStep<'_>,
        _limits: &StepLimits,
        _namespace: Option<&str>,
    ) -> Result<StepCommit, QueueError> {
        let ack = self.commit(
            step.job_id,
            step.seq,
            step.step_key,
            StepKind::Run,
            None,
            step.result.unwrap_or(&[]),
        )?;
        if !ack.ok {
            return Err(ack.into_error(step.job_id));
        }
        Ok(if ack.already {
            StepCommit::AlreadyCommitted
        } else {
            StepCommit::Committed
        })
    }

    fn commit_sleep(
        &self,
        step: &NewJobStep<'_>,
        wake_at: i64,
        _limits: &StepLimits,
        _namespace: Option<&str>,
    ) -> Result<SleepOutcome, QueueError> {
        let ack = self.commit(
            step.job_id,
            step.seq,
            step.step_key,
            StepKind::Sleep,
            Some(wake_at),
            &[],
        )?;
        if !ack.ok {
            return Err(ack.into_error(step.job_id));
        }
        // The deadline storage settled on, which on a replay is the stored one
        // rather than the candidate this call proposed. An ack without it is a
        // broken relay, not a deadline to invent.
        let settled = ack.wake_at.ok_or_else(|| {
            QueueError::Other(format!(
                "the sleep of job {} was acknowledged without a deadline",
                step.job_id
            ))
        })?;
        Ok(if ack.already {
            SleepOutcome::AlreadySleeping { wake_at: settled }
        } else {
            SleepOutcome::Slept { wake_at: settled }
        })
    }
}

/// One `step_ack`, as the relay hands it back.
struct Ack {
    ok: bool,
    already: bool,
    wake_at: Option<i64>,
    error: Option<String>,
    failure: Option<StepFailure>,
}

impl Ack {
    /// Read the ack out of the mapping the relay returned.
    ///
    /// A malformed answer is a refusal rather than a panic, and a retryable one:
    /// whatever went wrong here, nothing confirms the write landed.
    fn read(answer: &Bound<'_, PyAny>) -> Result<Self, QueueError> {
        let field = |name: &str| answer.get_item(name).ok();
        let read = |name: &str| -> PyResult<Option<Bound<'_, PyAny>>> {
            Ok(field(name).filter(|value| !value.is_none()))
        };
        let parse = || -> PyResult<Self> {
            Ok(Self {
                ok: read("ok")?
                    .map(|v| v.extract())
                    .transpose()?
                    .unwrap_or(false),
                already: read("already")?
                    .map(|v| v.extract())
                    .transpose()?
                    .unwrap_or(false),
                wake_at: read("wake_at")?.map(|v| v.extract()).transpose()?,
                error: read("error")?.map(|v| v.extract()).transpose()?,
                failure: read("failure")?
                    .map(|v| v.extract::<String>())
                    .transpose()?
                    .as_deref()
                    .and_then(parse_failure),
            })
        };
        parse().map_err(|error| {
            QueueError::Other(format!(
                "the executor returned an unreadable step ack: {error}"
            ))
        })
    }

    /// The error a refusal represents, classified as the side holding storage
    /// classified it — never re-derived here, where the real error was invisible.
    fn into_error(self, job_id: &str) -> QueueError {
        refusal_error(job_id, self.error, self.failure)
    }
}

/// Read a `StepFailure` off the wire.
///
/// An unrecognised verdict reads as absent, which
/// [`refusal_error`] treats as retryable — the safe reading when nothing
/// confirmed the write landed.
fn parse_failure(value: &str) -> Option<StepFailure> {
    match value {
        "retryable" => Some(StepFailure::Retryable),
        "permanent" => Some(StepFailure::Permanent),
        "superseded" => Some(StepFailure::Superseded),
        _ => None,
    }
}

/// The channel one attached job opens its durable steps over.
///
/// The twin of [`PyWorkerSteps`](crate::py_worker_steps::PyWorkerSteps), for the
/// deployment where this process holds no claim and no database. Built per job,
/// because everything it needs — the job the frame described, and the snapshot
/// that rode in front of it — arrives with the dispatch.
#[pyclass(name = "AttachedSteps", module = "flexiq._flexiq")]
pub struct PyAttachedSteps {
    relay: Py<PyAny>,
    job: Job,
    snapshot: Arc<Vec<u8>>,
    supported: bool,
}

#[pymethods]
impl PyAttachedSteps {
    /// Wrap the dispatch this process is about to run.
    ///
    /// `job_frame` is the dispatch frame's own JSON, rebuilt into the same
    /// [`Job`] an executor that ran the body itself would have opened its
    /// session on — through the frame's documented inverse, so a field added to
    /// a dispatch never has to be mirrored here.
    ///
    /// `supported` is what the pool advertised in its `hello_ack`: false when
    /// the scheduler it attached to offers no step store, which refuses rather
    /// than running a step un-memoized.
    #[new]
    #[pyo3(signature = (relay, job_frame, snapshot, supported))]
    fn new(
        relay: Py<PyAny>,
        job_frame: &str,
        snapshot: Vec<u8>,
        supported: bool,
    ) -> PyResult<Self> {
        let frame: SchedulerMessage = serde_json::from_str(job_frame).map_err(|error| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "the dispatch frame could not be read back: {error}"
            ))
        })?;
        let job = frame
            .into_dispatch(Vec::new())
            .ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(
                    "durable steps need the job's own dispatch frame, not a control frame",
                )
            })?
            .job;

        Ok(Self {
            relay,
            job,
            snapshot: Arc::new(snapshot),
            supported,
        })
    }

    /// Open the durable-step session for one attempt of `job_id`.
    ///
    /// Both arguments are checked against the dispatch rather than trusted, for
    /// the same reason the storage twin re-reads the row: a session opened for
    /// the wrong job or a superseded attempt would write into a sequence that is
    /// not its own.
    pub fn open_step_session(
        &self,
        py: Python<'_>,
        job_id: &str,
        attempt: i32,
    ) -> PyResult<PyStepSession> {
        self.open(py, job_id, attempt)
            .map(PyStepSession::new)
            .map_err(|error| step_error(py, error))
    }
}

impl PyAttachedSteps {
    fn open(
        &self,
        py: Python<'_>,
        job_id: &str,
        attempt: i32,
    ) -> Result<BoxedStepSession, QueueError> {
        if !self.supported {
            // Refused here rather than left to the first commit, and named for
            // the process an operator has to look at. Retryable, so a fleet
            // mid-rollout can still place the next attempt somewhere that
            // commits.
            return Err(QueueError::Other(format!(
                "job {job_id} uses durable steps, but the scheduler this executor is attached to \
                 offers no step store"
            )));
        }
        if self.job.id != job_id || self.job.retry_count != attempt {
            return Err(QueueError::ClaimLost(format!(
                "job {job_id} attempt {attempt} is not the dispatch this executor is running \
                 (job {} attempt {})",
                self.job.id, self.job.retry_count
            )));
        }

        // The defaults, as the storage twin uses: §4.2's answer to an oversized
        // result is to store it elsewhere and memoize the handle, so there is
        // nothing for a shell knob to do. The caps that *hold* are the
        // scheduler's, inside the write's own transaction.
        StepSession::open(
            BoxedStepStore::new(PipeStepStore {
                relay: self.relay.clone_ref(py),
                snapshot: Arc::clone(&self.snapshot),
            }),
            &self.job,
            StepLimits::default(),
        )
    }
}
