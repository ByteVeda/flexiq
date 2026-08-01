//! `JavaDispatcher` — runs each job by calling back into Java.
//!
//! Implements the core [`WorkerDispatcher`] trait. The completion registry lives
//! here in Rust (the inverse of the Node shell's JS-promise bridge): for each
//! job a token + oneshot is registered, the job is submitted to Java via
//! `onJob`, and the awaiting task parks on the oneshot — no worker thread blocks.
//! Java runs the task (sync or async) and completes it through the native
//! `NativeWorker.completeJob/failJob/cancelJob` entry points (see `worker.rs`),
//! which resolve the oneshot.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crossbeam_channel::Sender;
use jni::objects::{GlobalRef, JObject, JValue};
use taskito_core::job::Job;
use taskito_core::scheduler::JobResult;
use taskito_core::worker::{CancelSignals, ExecutorSideChannel, WorkerDispatcher};
use taskito_core::StorageBackend;
use tokio::sync::oneshot;

use crate::jvm;

/// The outcome Java reports for a submitted job. A failure carries whether it
/// may be retried — the task's `retryOn` predicate runs on the Java side.
pub enum TaskOutcome {
    Success(Vec<u8>),
    Failure(String, bool),
    Cancelled,
}

/// Pending-job registry shared between the dispatcher and the Java completion
/// callbacks. Held alive by the worker handle so its pointer stays valid.
#[derive(Default)]
pub struct Registry {
    pending: Mutex<HashMap<u64, oneshot::Sender<TaskOutcome>>>,
    next_token: AtomicU64,
}

impl Registry {
    fn register(&self) -> (u64, oneshot::Receiver<TaskOutcome>) {
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(token, tx);
        (token, rx)
    }

    fn forget(&self, token: u64) {
        self.pending.lock().unwrap().remove(&token);
    }

    /// Resolve a pending job. A no-op if the token already completed or timed out.
    pub fn complete(&self, token: u64, outcome: TaskOutcome) {
        if let Some(tx) = self.pending.lock().unwrap().remove(&token) {
            let _ = tx.send(outcome);
        }
    }
}

/// Executes jobs by dispatching them to a Java `WorkerBridge` callback object.
pub struct JavaDispatcher {
    callbacks: GlobalRef,
    registry: Arc<Registry>,
    /// Where a cancel comes from: the storage flag for a worker, the scheduler's
    /// `cancel` frame for an attached executor, which has no storage at all.
    cancels: Arc<CancelSignals>,
    /// Reads the toggle list the scheduler attached to each dispatch. `None`
    /// for a worker, which has storage and reads the live list itself.
    ///
    /// Behind a lock because the handle only exists once the handshake has
    /// completed, and `run` may already be going by then.
    side_channel: Mutex<Option<ExecutorSideChannel>>,
}

impl JavaDispatcher {
    pub fn new(callbacks: GlobalRef, registry: Arc<Registry>, storage: StorageBackend) -> Self {
        Self {
            callbacks,
            registry,
            cancels: Arc::new(CancelSignals::from_storage(storage)),
            side_channel: Mutex::new(None),
        }
    }

    /// A dispatcher with no storage, for an attached executor. Cancels arrive
    /// only through [`WorkerDispatcher::notify_cancel`].
    pub fn detached(callbacks: GlobalRef, registry: Arc<Registry>) -> Self {
        Self {
            callbacks,
            registry,
            cancels: Arc::new(CancelSignals::detached()),
            side_channel: Mutex::new(None),
        }
    }

    /// Read each dispatch's toggle list from `side_channel`.
    ///
    /// Installed after the attach, which is the earliest the handle exists.
    pub fn set_side_channel(&self, side_channel: ExecutorSideChannel) {
        *self
            .side_channel
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(side_channel);
    }

    /// Middleware the scheduler resolved as disabled for `job_id`, as a JSON
    /// array — the shape the Java side already parses a stored list from.
    ///
    /// `None` for a worker, whose bridge reads the live list from storage.
    fn disabled_middleware(&self, job_id: &str) -> Option<String> {
        let disabled = self
            .side_channel
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()?
            .disabled_middleware(job_id);
        serde_json::to_string(&disabled).ok()
    }
}

#[async_trait::async_trait]
impl WorkerDispatcher for JavaDispatcher {
    async fn run(
        &self,
        mut job_rx: tokio::sync::mpsc::Receiver<Job>,
        result_tx: Sender<JobResult>,
    ) {
        while let Some(job) = job_rx.recv().await {
            let callbacks = self.callbacks.clone();
            let registry = self.registry.clone();
            let cancels = self.cancels.clone();
            let result_tx = result_tx.clone();
            // Resolved here, on the dispatch path, rather than inside the task:
            // it is the toggle list this job was dispatched with.
            let disabled = self.disabled_middleware(&job.id);
            tokio::spawn(async move {
                let job_id = job.id.clone();
                let result = run_one(&callbacks, &registry, &cancels, job, disabled).await;
                // Release the cancel record now the job has reported, so a
                // long-lived process does not accumulate ids.
                cancels.forget(&job_id);
                let _ = result_tx.send(result);
            });
        }
    }

    fn shutdown(&self) {}

    fn notify_cancel(&self, job_id: &str) {
        self.cancels.signal(job_id);
    }
}

/// Submit one job to Java, await its completion, and translate to a [`JobResult`].
async fn run_one(
    callbacks: &GlobalRef,
    registry: &Registry,
    cancels: &CancelSignals,
    job: Job,
    disabled_middleware: Option<String>,
) -> JobResult {
    let started = Instant::now();
    let (token, rx) = registry.register();

    if let Err(err) = submit_to_java(callbacks, token, &job, disabled_middleware.as_deref()) {
        registry.forget(token);
        return failure(job, err, started.elapsed().as_nanos() as i64, false, true);
    }

    // `timeout_ms <= 0` means no limit. On timeout we drop the pending entry; a
    // late completion then finds no sender and is harmlessly ignored.
    let outcome = if job.timeout_ms > 0 {
        match tokio::time::timeout(Duration::from_millis(job.timeout_ms as u64), rx).await {
            Ok(result) => result,
            Err(_) => {
                registry.forget(token);
                let wall = started.elapsed().as_nanos() as i64;
                return failure(job, "task timed out".to_string(), wall, true, true);
            }
        }
    } else {
        rx.await
    };

    let wall = started.elapsed().as_nanos() as i64;
    match outcome {
        Ok(TaskOutcome::Success(result)) => JobResult::Success {
            job_id: job.id,
            result: Some(result),
            task_name: job.task_name,
            wall_time_ns: wall,
        },
        Ok(TaskOutcome::Cancelled) => cancelled(job, wall),
        Ok(TaskOutcome::Failure(error, retryable)) => {
            // A failure on a cancel-requested job is a cancellation, not a fault.
            if cancels.is_cancelled(&job.id) {
                cancelled(job, wall)
            } else {
                failure(job, error, wall, false, retryable)
            }
        }
        // The oneshot sender dropped without completing — the Java side died.
        Err(_) => failure(
            job,
            "java task channel dropped".to_string(),
            wall,
            false,
            true,
        ),
    }
}

/// Invoke `WorkerBridge.onJob` on an attached thread. Local refs are freed when
/// the per-call attachment guard drops.
fn submit_to_java(
    callbacks: &GlobalRef,
    token: u64,
    job: &Job,
    disabled_middleware: Option<&str>,
) -> Result<(), String> {
    let vm = jvm::vm();
    let mut env = vm
        .attach_current_thread()
        .map_err(|e| format!("attach failed: {e}"))?;
    let job_id = env.new_string(&job.id).map_err(|e| e.to_string())?;
    let task_name = env.new_string(&job.task_name).map_err(|e| e.to_string())?;
    let payload = env
        .byte_array_from_slice(&job.payload)
        .map_err(|e| e.to_string())?;
    // Both nullable, and both carried rather than looked up: an executor has no
    // storage to read the job row or the toggle list from, and a worker's
    // bridge ignores them in favour of the live values.
    let metadata = optional_string(&mut env, job.metadata.as_deref())?;
    let disabled = optional_string(&mut env, disabled_middleware)?;
    // An `Err(JavaException)` leaves the exception pending on the attached
    // thread; clear it before returning so the thread isn't poisoned for the
    // next call.
    if let Err(e) = env.call_method(
        callbacks,
        "onJob",
        "(JLjava/lang/String;Ljava/lang/String;[BLjava/lang/String;Ljava/lang/String;)V",
        &[
            JValue::Long(token as i64),
            JValue::Object(&job_id),
            JValue::Object(&task_name),
            JValue::Object(&payload),
            JValue::Object(&metadata),
            JValue::Object(&disabled),
        ],
    ) {
        let _ = env.exception_clear();
        return Err(e.to_string());
    }
    if env.exception_check().unwrap_or(false) {
        let _ = env.exception_clear();
        return Err("WorkerBridge.onJob threw".to_string());
    }
    Ok(())
}

/// A Java `String`, or `null` when there is nothing to pass.
///
/// `JObject::null()` is what an absent value has to be: an empty string would
/// read on the Java side as "present but empty", which is a different answer.
fn optional_string<'local>(
    env: &mut jni::JNIEnv<'local>,
    value: Option<&str>,
) -> Result<JObject<'local>, String> {
    match value {
        Some(value) => Ok(JObject::from(
            env.new_string(value).map_err(|e| e.to_string())?,
        )),
        None => Ok(JObject::null()),
    }
}

fn cancelled(job: Job, wall_time_ns: i64) -> JobResult {
    JobResult::Cancelled {
        job_id: job.id,
        task_name: job.task_name,
        wall_time_ns,
    }
}

/// `should_retry` false skips the retry budget entirely and dead-letters the job.
fn failure(
    job: Job,
    error: String,
    wall_time_ns: i64,
    timed_out: bool,
    should_retry: bool,
) -> JobResult {
    JobResult::Failure {
        job_id: job.id,
        error,
        retry_count: job.retry_count,
        max_retries: job.max_retries,
        task_name: job.task_name,
        wall_time_ns,
        should_retry,
        timed_out,
    }
}
