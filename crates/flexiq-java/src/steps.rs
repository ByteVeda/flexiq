//! Durable inline steps, exposed to the JVM.
//!
//! The rules live in `flexiq_core::step` and stay there — this module is the
//! split form of [`StepSession`] the design calls for: `beginRun` decides, Java
//! runs the body and encodes the result with the queue's own serializer and
//! codec chain, `commitRun` stores exactly those bytes. The core never sees
//! inside them, which is what makes an encrypting codec work here with no extra
//! plumbing.
//!
//! Nothing in here reads an owner from the running code. It is the id of the
//! worker that won the execution claim, read off that worker's own handle by
//! [`open_step_session`], because an owner task code can assert is an owner it
//! can forge — and a forged one writes straight into the live attempt's
//! sequence.
//!
//! **The retry verdict crosses as a class.** JNI can throw any `Throwable`, so
//! `classify_step_failure`'s answer is the exception class itself and
//! `StepControlSignal.shouldRetry()` on the Java side is the whole contract.
//! (The Node shell has to encode the same verdict as JSON inside a message,
//! because napi carries only a status and a string.)

use std::sync::Mutex;

use flexiq_core::error::QueueError;
use flexiq_core::step::{
    classify_step_failure, PendingStep, StepDecision, StepFailure, StepLimits, StepSleep,
    StorageStepSession,
};
use flexiq_core::storage::{Storage, StorageBackend};
use jni::objects::{JByteArray, JClass, JObject, JString, JValue};
use jni::sys::{jint, jlong, jobject};
use jni::JNIEnv;

use crate::error::BindingError;
use crate::ffi::{guard, new_string, read_bytes, read_optional_string, read_string};
use crate::handle::{self, drop_handle, into_handle};
use crate::worker::WorkerHandle;

/// JNI names of the step signal classes. The class *is* the retry verdict, so
/// picking the wrong one here is picking the wrong verdict.
const STEP_ERROR: &str = "org/byteveda/flexiq/steps/StepError";
const STEP_DIVERGED: &str = "org/byteveda/flexiq/steps/StepDivergedError";
const STEP_LIMIT: &str = "org/byteveda/flexiq/steps/StepLimitExceededError";
const STEP_SUPERSEDED: &str = "org/byteveda/flexiq/steps/StepSupersededError";
const STEP_UNAVAILABLE: &str = "org/byteveda/flexiq/steps/StepUnavailableError";

/// The records `beginRun` and the sleeps hand back, built natively so one call
/// carries the whole answer and the memoized bytes are never re-encoded.
const STEP_DECISION: &str = "org/byteveda/flexiq/spi/StepDecision";
const STEP_SLEEP_OUTCOME: &str = "org/byteveda/flexiq/spi/StepSleepOutcome";

/// Turn a core step error into the Java exception class it deserves.
///
/// A `Superseded` failure gets its own class. The attempt still reports a
/// failure — every worker path owes the scheduler a result — but
/// `handle_result` fences on `(owner, attempt)` before it mutates anything, so
/// the result is dropped rather than allowed to kill a run proceeding correctly
/// under another owner.
pub fn step_error(error: QueueError) -> BindingError {
    let class = match classify_step_failure(&error) {
        StepFailure::Superseded => STEP_SUPERSEDED,
        StepFailure::Permanent => match &error {
            QueueError::StepLimitExceeded { .. } => STEP_LIMIT,
            QueueError::StepDiverged { .. } | QueueError::StepSequenceDiverged(_) => STEP_DIVERGED,
            _ => STEP_ERROR,
        },
        // Retryable: an unreachable backend, a missing step store, a lost
        // claim. The next attempt may land somewhere that can commit.
        StepFailure::Retryable => STEP_UNAVAILABLE,
    };
    BindingError::with_class(class, error.to_string())
}

/// The session plus the one step it has handed out and not yet stored.
struct SessionState {
    session: StorageStepSession<StorageBackend>,
    /// The step `beginRun` issued, waiting for its bytes. Cleared by
    /// `commitRun`, whether or not the write succeeded: a refused commit has
    /// already failed the attempt, and keeping the token would let a later call
    /// store bytes against a step the core has moved past.
    ///
    /// It lives here rather than in the returned `StepDecision` so a caller
    /// cannot hand `commitRun` a position the core did not assign. There is
    /// never more than one, because the sequence refuses a second `beginRun`
    /// while one is uncommitted.
    pending: Option<PendingStep>,
}

/// One attempt's durable steps, behind a `long` handle.
///
/// The mutex guards the state, not concurrency: a session belongs to one
/// attempt and the core refuses two steps at once. Every call runs on the Java
/// thread that made it — a durable step is a storage write, and this shell has
/// a pool thread per task rather than Node's single event loop.
pub struct StepSessionHandle {
    state: Mutex<SessionState>,
}

impl StepSessionHandle {
    fn new(session: StorageStepSession<StorageBackend>) -> Self {
        Self {
            state: Mutex::new(SessionState {
                session,
                pending: None,
            }),
        }
    }

    /// Run `body` against the session.
    ///
    /// A poisoned lock becomes a step failure rather than a panic: the session
    /// is unusable, and failing this attempt is the honest answer.
    fn with<T>(
        &self,
        body: impl FnOnce(&mut SessionState) -> Result<T, QueueError>,
    ) -> Result<T, BindingError> {
        match self.state.lock() {
            Ok(mut guard) => body(&mut guard).map_err(step_error),
            Err(_) => Err(step_error(QueueError::Other(
                "step session is poisoned".into(),
            ))),
        }
    }
}

/// Borrow a step-session handle.
///
/// # Safety
/// `handle` must be a live `StepSessionHandle` pointer from
/// [`open_step_session`].
unsafe fn borrow_session<'a>(handle: jlong) -> &'a StepSessionHandle {
    handle::borrow::<StepSessionHandle>(handle)
}

/// Build the `StepDecision` record `beginRun` returns.
fn new_decision<'local>(
    env: &mut JNIEnv<'local>,
    memoized: Option<Vec<u8>>,
    step_key: &str,
    idempotency_key: &str,
) -> Result<JObject<'local>, BindingError> {
    let class = find_class(env, STEP_DECISION)?;
    let bytes: JObject = match memoized {
        Some(bytes) => env
            .byte_array_from_slice(&bytes)
            .map_err(|e| BindingError::new(format!("failed to allocate byte[]: {e}")))?
            .into(),
        None => JObject::null(),
    };
    let step_key = env
        .new_string(step_key)
        .map_err(|e| BindingError::new(format!("failed to allocate Java string: {e}")))?;
    let idempotency_key = env
        .new_string(idempotency_key)
        .map_err(|e| BindingError::new(format!("failed to allocate Java string: {e}")))?;
    env.new_object(
        class,
        "([BLjava/lang/String;Ljava/lang/String;)V",
        &[
            JValue::Object(&bytes),
            JValue::Object(&step_key),
            JValue::Object(&idempotency_key),
        ],
    )
    .map_err(|e| BindingError::new(format!("failed to build StepDecision: {e}")))
}

/// Build the `StepSleepOutcome` record both sleeps return.
fn new_sleep_outcome<'local>(
    env: &mut JNIEnv<'local>,
    sleep: StepSleep,
) -> Result<JObject<'local>, BindingError> {
    let (elapsed, step_key, wake_at) = match sleep {
        StepSleep::Elapsed { step_key, wake_at } => (true, step_key, wake_at),
        StepSleep::Sleeping { step_key, wake_at } => (false, step_key, wake_at),
    };
    let class = find_class(env, STEP_SLEEP_OUTCOME)?;
    let step_key = env
        .new_string(step_key)
        .map_err(|e| BindingError::new(format!("failed to allocate Java string: {e}")))?;
    env.new_object(
        class,
        "(ZLjava/lang/String;J)V",
        &[
            JValue::Bool(u8::from(elapsed)),
            JValue::Object(&step_key),
            JValue::Long(wake_at),
        ],
    )
    .map_err(|e| BindingError::new(format!("failed to build StepSleepOutcome: {e}")))
}

fn find_class<'local>(
    env: &mut JNIEnv<'local>,
    name: &str,
) -> Result<JClass<'local>, BindingError> {
    env.find_class(name)
        .map_err(|e| BindingError::new(format!("{name} lookup failed: {e}")))
}

/// `Object beginRun(long handle, String name, String key)` — a `StepDecision`.
///
/// A divergence surfaces here, *before* the body runs, which is the point of
/// checking each step as it is asked for rather than at the end.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeStepSession_beginRun<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    name: JString<'local>,
    key: JString<'local>,
) -> jobject {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let session = unsafe { borrow_session(handle) };
        let name = read_string(env, &name)?;
        let key = read_optional_string(env, &key)?;
        let (memoized, step_key, idempotency_key) = session.with(|state| {
            let decision = state.session.begin_run(&name, key.as_deref())?;
            let step_key = match &decision {
                StepDecision::Memoized { step_key, .. } => step_key.clone(),
                StepDecision::Run(pending) => pending.step_key().to_string(),
            };
            let idempotency_key = state.session.idempotency_key(&step_key);
            let memoized = match decision {
                StepDecision::Memoized { result, .. } => {
                    state.pending = None;
                    Some(result)
                }
                StepDecision::Run(pending) => {
                    state.pending = Some(pending);
                    None
                }
            };
            Ok((memoized, step_key, idempotency_key))
        })?;
        Ok(new_decision(env, memoized, &step_key, &idempotency_key)?.into_raw())
    })
}

/// `void commitRun(long handle, byte[] encoded)`.
///
/// `encoded` is post-serializer, post-codec: those are the bytes stored, and
/// the bytes the caps are measured on.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeStepSession_commitRun<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    encoded: JByteArray<'local>,
) {
    guard(&mut env, (), |env| {
        let session = unsafe { borrow_session(handle) };
        let bytes = read_bytes(env, &encoded)?;
        session.with(|state| {
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
    })
}

/// `Object sleepFor(long handle, long durationMs, String name, String key)`.
///
/// The clock is read once, inside the core, and the deadline it produces is
/// only a candidate — storage keeps whatever a sleep row at this position
/// already holds, so a replayed one-hour sleep wakes at the original instant
/// rather than an hour later every time the job crashed into it.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeStepSession_sleepFor<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    duration_ms: jlong,
    name: JString<'local>,
    key: JString<'local>,
) -> jobject {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let session = unsafe { borrow_session(handle) };
        let name = read_optional_string(env, &name)?;
        let key = read_optional_string(env, &key)?;
        let sleep = session.with(|state| {
            state
                .session
                .sleep_for(name.as_deref(), key.as_deref(), duration_ms)
        })?;
        Ok(new_sleep_outcome(env, sleep)?.into_raw())
    })
}

/// `Object sleepUntil(long handle, long wakeAt, String name, String key)` —
/// `wakeAt` in Unix milliseconds.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeStepSession_sleepUntil<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    wake_at: jlong,
    name: JString<'local>,
    key: JString<'local>,
) -> jobject {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let session = unsafe { borrow_session(handle) };
        let name = read_optional_string(env, &name)?;
        let key = read_optional_string(env, &key)?;
        let sleep = session.with(|state| {
            state
                .session
                .sleep_until(name.as_deref(), key.as_deref(), wake_at)
        })?;
        Ok(new_sleep_outcome(env, sleep)?.into_raw())
    })
}

/// `String runKey(long handle)` — the id this durable run began under.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeStepSession_runKey<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jni::sys::jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let session = unsafe { borrow_session(handle) };
        let key = session.with(|state| Ok(state.session.run_key().to_string()))?;
        new_string(env, key)
    })
}

/// `void finish(long handle)` — close the attempt out, warning if the job has
/// recorded steps this code no longer runs. Never throws: the side effects
/// already happened.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeStepSession_finish(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    guard(&mut env, (), |_env| {
        let session = unsafe { borrow_session(handle) };
        if let Ok(state) = session.state.lock() {
            state.session.finish();
        }
        Ok(())
    })
}

/// `void close(long handle)` — reclaim the session handle.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeStepSession_close(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    guard(&mut env, (), |_env| {
        if handle != 0 {
            unsafe { drop_handle::<StepSessionHandle>(handle) };
        }
        Ok(())
    })
}

/// `long openStepSession(long workerHandle, String jobId, int attempt)`.
///
/// On the **worker**, not the queue: the fence is `(owner, attempt)`, and the
/// owner is the id *this* worker claims execution under. A queue-level slot
/// would be overwritten by a second worker on the same handle, and every step
/// the first worker went on to commit would be refused as superseded — safe,
/// but the job would fail rather than run.
///
/// `attempt` is the `retryCount` the job was dispatched with. It is checked
/// against the row rather than trusted: an attempt that has been superseded —
/// reclaimed by another worker, or retried past this one — must not write into
/// the live attempt's sequence, and finding out here gives a clearer error than
/// the storage fence's refusal on the first commit.
///
/// An attached executor never reaches this: it holds no worker handle, so it
/// has no channel to commit a step on and refuses in the shell instead.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeWorker_openStepSession<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    job_id: JString<'local>,
    attempt: jint,
) -> jlong {
    guard(&mut env, 0, |env| {
        let worker = unsafe { handle::borrow::<WorkerHandle>(handle) };
        let job_id = read_string(env, &job_id)?;
        Ok(into_handle(open_step_session(worker, &job_id, attempt)?))
    })
}

/// Load one attempt's session off `worker`'s own storage and claim id.
fn open_step_session(
    worker: &WorkerHandle,
    job_id: &str,
    attempt: jint,
) -> Result<StepSessionHandle, BindingError> {
    let job = worker
        .storage
        .get_job(job_id, worker.namespace.as_deref())
        .map_err(step_error)?
        .ok_or_else(|| {
            step_error(QueueError::ClaimLost(format!(
                "job {job_id} no longer exists"
            )))
        })?;

    if job.retry_count != attempt {
        // Reported as a lost claim, which is what it is: this attempt is not
        // the one the job is on, so its writes are refused and its result is
        // dropped by the scheduler's own fence.
        return Err(step_error(QueueError::ClaimLost(format!(
            "job {job_id} is on attempt {} and this one is {attempt}",
            job.retry_count
        ))));
    }

    // The defaults, not a caller-supplied value: §4.2's answer to a result that
    // will not fit is to store it elsewhere and memoize the handle, not to
    // raise the cap, so there is nothing for a shell knob to do yet.
    let session = StorageStepSession::load(
        worker.storage.clone(),
        &job,
        &worker.worker_id,
        StepLimits::default(),
    )
    .map_err(step_error)?;
    Ok(StepSessionHandle::new(session))
}
