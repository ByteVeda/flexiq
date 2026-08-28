//! Durable inline steps, exposed to JavaScript.
//!
//! The rules live in `flexiq_core::step` and stay there — this module is the
//! split form of [`StepSession`] the design calls for: `beginRun` decides, JS
//! runs the callback and encodes the result with the queue's own serializer
//! and codec chain, `commitRun` stores exactly those bytes. The core never
//! sees inside them, which is what makes an encrypting codec work here with no
//! extra plumbing.
//!
//! Nothing in here reads an owner from the running code. It is the id of the
//! worker that won the execution claim, read off that worker's own handle by
//! [`JsWorker::open_step_session`], because an owner task code can assert is an
//! owner it can forge — and a forged one writes straight into the live
//! attempt's sequence. An attached executor supplies none at all: the claim is
//! the scheduler's, and so is the fence (`attached_steps.rs`).
//!
//! Every storage round trip is `spawn_blocking` behind a `Promise`. Node has
//! one thread for every task on the worker, so a synchronous commit would
//! stall the other jobs' timers, their cancel polls and their I/O for the
//! duration of the write.

use std::sync::{Arc, Mutex};

use flexiq_core::error::QueueError;
use flexiq_core::step::{
    classify_step_failure, PendingStep, StepDecision, StepFailure, StepLimits, StepSession,
    StepSleep, StepStore, StorageSteps,
};
use flexiq_core::storage::Storage;
use napi::bindgen_prelude::{spawn_blocking, Buffer, Result, Status};
use napi_derive::napi;

use crate::error::join_to_napi_err;
use crate::worker::JsWorker;

/// Turn a core step error into the napi error its class deserves, stamped with
/// the retry decision from `classify_step_failure`.
///
/// napi carries only a status and a string, and neither can say "this is a
/// divergence and it must not be retried". So the reason is JSON — the shape
/// #413 already established for task errors — and `steps/errors.ts` rebuilds
/// the class and the retry verdict from it. `flexiqStep` both names the class
/// and marks the reason as a step failure; a reason that does not parse is
/// still a legible message, which is what an older shell would read.
///
/// A `Superseded` failure gets its own class. The attempt still reports a
/// failure — every worker path owes the scheduler a result — but
/// `handle_result` fences on `(owner, attempt)` before it mutates anything, so
/// the result is dropped rather than allowed to kill a run proceeding
/// correctly under another owner.
pub fn step_error(error: QueueError) -> napi::Error {
    let failure = classify_step_failure(&error);
    let kind = match failure {
        StepFailure::Superseded => "superseded",
        StepFailure::Permanent => match &error {
            QueueError::StepLimitExceeded { .. } => "limit",
            QueueError::StepDiverged { .. } | QueueError::StepSequenceDiverged(_) => "diverged",
            _ => "error",
        },
        StepFailure::Retryable => "unavailable",
    };
    let reason = serde_json::json!({
        "flexiqStep": kind,
        "message": error.to_string(),
        // The core's verdict, which outranks the task's own `retryOn` filter.
        "retryable": failure.should_retry(),
    });
    napi::Error::new(Status::GenericFailure, reason.to_string())
}

/// What `beginRun` decided.
///
/// The pending step itself is **not** here: it stays inside the session, so a
/// caller cannot hand `commitRun` a position the core did not assign. There is
/// never more than one, because the sequence refuses a second `beginRun` while
/// one is uncommitted.
#[napi(object)]
pub struct JsStepDecision {
    /// The stored bytes when this step already ran. Absent means new ground,
    /// and the callback has to run.
    pub memoized: Option<Buffer>,
    /// Identity of this step — `name#occurrence`, or the explicit key.
    pub step_key: String,
    /// The key to hand the downstream service for this step.
    pub idempotency_key: String,
}

/// Outcome of a sleep, as the shell sees it.
///
/// `elapsed` means the deadline had already passed and the attempt carries on;
/// otherwise the job is `Pending` at `wakeAt` and the task body must unwind
/// now — anything it does past this point runs unclaimed and runs again on
/// wake.
#[napi(object)]
pub struct JsStepSleepOutcome {
    pub elapsed: bool,
    pub step_key: String,
    pub wake_at: i64,
}

impl From<StepSleep> for JsStepSleepOutcome {
    fn from(sleep: StepSleep) -> Self {
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

/// One attempt's durable steps, however its writes leave this process.
///
/// The store is erased because a `#[napi]` class cannot be generic and the two
/// deployments write through different ones — storage for a worker, the
/// scheduler connection for an attached executor. Erasing it here rather than
/// duplicating the class is what keeps the split form of the session (`beginRun`
/// … `commitRun`) written once.
pub(crate) type BoxedStepSession = StepSession<Box<dyn StepStore + Send>>;

/// The session plus the one step it has handed out and not yet stored.
struct SessionState {
    session: BoxedStepSession,
    /// The step `beginRun` issued, waiting for its bytes. Cleared by
    /// `commitRun`, whether or not the write succeeded: a refused commit has
    /// already failed the attempt, and keeping the token would let a later
    /// call store bytes against a step the core has moved past.
    pending: Option<PendingStep>,
}

/// One attempt's durable steps.
///
/// Built by [`JsWorker::open_step_session`] or its attached twin, which are the
/// only places the fence comes from — the owner off the worker that won the
/// claim, or nothing at all for an executor, whose scheduler supplies both
/// halves from the dispatch it recorded. The mutex guards the hand-off
/// between the JS thread and the blocking pool, not concurrency: a session
/// belongs to one attempt, and the core refuses two steps at once.
#[napi]
pub struct JsStepSession {
    state: Arc<Mutex<SessionState>>,
}

impl JsStepSession {
    pub(crate) fn new(session: BoxedStepSession) -> Self {
        Self {
            state: Arc::new(Mutex::new(SessionState {
                session,
                pending: None,
            })),
        }
    }

    /// Run `body` against the session on the blocking pool.
    ///
    /// A poisoned lock becomes a failure rather than a panic, which would take
    /// the worker's blocking thread with it.
    async fn with<T: Send + 'static>(
        &self,
        body: impl FnOnce(&mut SessionState) -> std::result::Result<T, QueueError> + Send + 'static,
    ) -> Result<T> {
        let state = self.state.clone();
        spawn_blocking(move || {
            let outcome = match state.lock() {
                Ok(mut guard) => body(&mut guard),
                Err(_) => Err(QueueError::Other("step session is poisoned".into())),
            };
            outcome.map_err(step_error)
        })
        .await
        .map_err(join_to_napi_err)?
    }
}

#[napi]
impl JsStepSession {
    /// Decide what this step must do, without running anything.
    ///
    /// A divergence surfaces here — before the callback runs — which is the
    /// point of checking each step as it is asked for rather than at the end.
    #[napi]
    pub async fn begin_run(&self, name: String, key: Option<String>) -> Result<JsStepDecision> {
        self.with(move |state| {
            let decision = state.session.begin_run(&name, key.as_deref())?;
            let step_key = match &decision {
                StepDecision::Memoized { step_key, .. } => step_key.clone(),
                StepDecision::Run(pending) => pending.step_key().to_string(),
            };
            let idempotency_key = state.session.idempotency_key(&step_key);
            let memoized = match decision {
                StepDecision::Memoized { result, .. } => {
                    state.pending = None;
                    Some(Buffer::from(result))
                }
                StepDecision::Run(pending) => {
                    state.pending = Some(pending);
                    None
                }
            };
            Ok(JsStepDecision {
                memoized,
                step_key,
                idempotency_key,
            })
        })
        .await
    }

    /// Commit the encoded result of the step `beginRun` handed out.
    ///
    /// `encoded` is post-serializer, post-codec: those are the bytes stored,
    /// and the bytes the caps are measured on.
    #[napi]
    pub async fn commit_run(&self, encoded: Buffer) -> Result<()> {
        let bytes = encoded.to_vec();
        self.with(move |state| {
            let Some(pending) = state.pending.take() else {
                return Err(QueueError::Other(
                    "commitRun was called with no step in flight, which means the decision it \
                     belongs to was a memo hit or has already been committed"
                        .into(),
                ));
            };
            state.session.commit_run(&pending, &bytes)?;
            Ok(())
        })
        .await
    }

    /// Sleep for `durationMs`, ending the attempt if the deadline is ahead.
    ///
    /// The clock is read once, inside the core, and the deadline it produces is
    /// only a candidate — storage keeps whatever a sleep row at this position
    /// already holds, so a replayed `sleep("1h")` wakes at the original instant
    /// rather than an hour later every time the job crashed into it.
    #[napi]
    pub async fn sleep_for(
        &self,
        duration_ms: i64,
        name: Option<String>,
        key: Option<String>,
    ) -> Result<JsStepSleepOutcome> {
        self.with(move |state| {
            state
                .session
                .sleep_for(name.as_deref(), key.as_deref(), duration_ms)
        })
        .await
        .map(JsStepSleepOutcome::from)
    }

    /// Sleep until an absolute instant, in Unix milliseconds.
    #[napi]
    pub async fn sleep_until(
        &self,
        wake_at: i64,
        name: Option<String>,
        key: Option<String>,
    ) -> Result<JsStepSleepOutcome> {
        self.with(move |state| {
            state
                .session
                .sleep_until(name.as_deref(), key.as_deref(), wake_at)
        })
        .await
        .map(JsStepSleepOutcome::from)
    }

    /// The id this durable run began under — the job's own, except across a
    /// `retryDead`, which mints a new job for the same run.
    #[napi]
    pub async fn run_key(&self) -> Result<String> {
        self.with(|state| Ok(state.session.run_key().to_string()))
            .await
    }

    /// Close the attempt out, warning if the job has recorded steps this code
    /// no longer runs. Never throws: the side effects already happened.
    #[napi]
    pub fn finish(&self) {
        if let Ok(state) = self.state.lock() {
            state.session.finish();
        }
    }
}

#[napi]
impl JsWorker {
    /// Open the durable-step session for one attempt of `jobId`.
    ///
    /// On the **worker**, not the queue: the fence is `(owner, attempt)`, and
    /// the owner is the id *this* worker claims execution under. A queue-level
    /// slot would be overwritten by a second `runWorker` on the same handle, and
    /// every step the first worker went on to commit would be refused as
    /// superseded — safe, but the job would fail rather than run.
    ///
    /// `attempt` is the `retryCount` the job was dispatched with. It is checked
    /// against the row rather than trusted: an attempt that has been superseded
    /// — reclaimed by another worker, or retried past this one — must not write
    /// into the live attempt's sequence, and finding out here gives a clearer
    /// error than the storage fence's refusal on the first commit.
    ///
    /// An attached executor holds no worker handle and reaches
    /// [`JsExecutor::open_step_session`](crate::JsExecutor) instead, whose
    /// writes are fenced by the scheduler rather than here.
    #[napi]
    pub async fn open_step_session(&self, job_id: String, attempt: i32) -> Result<JsStepSession> {
        let storage = self.storage.clone();
        let namespace = self.namespace.clone();
        let owner = self.worker_id.clone();

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
            let session = StepSession::open(
                StorageSteps::new(storage, &owner, attempt),
                &job,
                StepLimits::default(),
            )
            .map_err(step_error)?;
            Ok(JsStepSession::new(session.boxed()))
        })
        .await
        .map_err(join_to_napi_err)?
    }
}
