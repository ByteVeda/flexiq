//! `NodeDispatcher` — runs each job by calling back into JavaScript.
//!
//! Implements the core [`WorkerDispatcher`] trait. For every dequeued job it
//! invokes the JS task callback via a `ThreadsafeFunction`, awaits the returned
//! `Promise<JsTaskOutcome>`, and reports a [`JobResult`] back to the scheduler. This is
//! the Node mirror of the Python shell's worker pool.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_channel::Sender;
use flexiq_core::job::Job;
use flexiq_core::scheduler::JobResult;
use flexiq_core::worker::{CancelSignals, WorkerDispatcher};
use flexiq_core::StorageBackend;
use napi::bindgen_prelude::{spawn, spawn_blocking, Buffer, Promise, Status};
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::Env;
use tokio::sync::{oneshot, Semaphore};

use crate::convert::{JsTaskInvocation, JsTaskOutcome};

/// Task callback registered from JS: `(invocation) => Promise<JsTaskOutcome>`.
/// `false` is napi's `CalleeHandled` flag — the JS side is called with the bare
/// invocation rather than a `(err, value)` pair.
pub type TaskCallback =
    ThreadsafeFunction<JsTaskInvocation, Promise<JsTaskOutcome>, JsTaskInvocation, Status, false>;

/// Executes jobs by dispatching them to a JavaScript callback.
pub struct NodeDispatcher {
    /// `Arc` because napi 3's `ThreadsafeFunction` is not `Clone` — every
    /// spawned job needs its own handle to the same callback.
    callback: Arc<TaskCallback>,
    /// Where a cancel comes from: the storage flag for a worker, the scheduler's
    /// `cancel` frame for an attached executor, which has no storage at all.
    cancels: Arc<CancelSignals>,
    /// Caps jobs running at once. Without it the loop spawns every job it is
    /// handed and immediately takes the next, so nothing bounds concurrency.
    ///
    /// The scheduler's `max_in_flight` bounds what this worker *claims*; this
    /// bounds what it *runs*, and is the only bound on the mesh path, where
    /// stolen jobs never pass through this scheduler's in-flight accounting.
    concurrency: Arc<Semaphore>,
}

impl NodeDispatcher {
    /// `concurrency` of `None` leaves execution unbounded, matching the
    /// behaviour of a worker that never set the option.
    pub fn new(
        callback: TaskCallback,
        storage: StorageBackend,
        namespace: Option<String>,
        concurrency: Option<usize>,
    ) -> Self {
        Self::with_cancels(
            callback,
            Arc::new(CancelSignals::from_storage(storage, namespace)),
            concurrency,
        )
    }

    /// A dispatcher with no storage, for an attached executor. Cancels arrive
    /// only through [`WorkerDispatcher::notify_cancel`].
    pub fn detached(callback: TaskCallback, concurrency: Option<usize>) -> Self {
        Self::with_cancels(callback, Arc::new(CancelSignals::detached()), concurrency)
    }

    /// The cancel source this dispatcher reads. An attached executor hands it to
    /// its JS side, which has no storage to poll and so cannot otherwise learn
    /// that a running job was cancelled.
    pub fn cancels(&self) -> Arc<CancelSignals> {
        self.cancels.clone()
    }

    fn with_cancels(
        callback: TaskCallback,
        cancels: Arc<CancelSignals>,
        concurrency: Option<usize>,
    ) -> Self {
        let permits = concurrency
            .map(|c| c.max(1))
            .unwrap_or(Semaphore::MAX_PERMITS);
        Self {
            callback: Arc::new(callback),
            cancels,
            concurrency: Arc::new(Semaphore::new(permits)),
        }
    }
}

#[async_trait::async_trait]
impl WorkerDispatcher for NodeDispatcher {
    async fn run(
        &self,
        mut job_rx: tokio::sync::mpsc::Receiver<Job>,
        result_tx: Sender<JobResult>,
    ) {
        // Run jobs concurrently — each invocation is independent and the JS
        // side may be async — but take a permit first. Spawning unconditionally
        // drains the job channel as fast as the scheduler fills it, so the
        // channel applies no backpressure and in-flight work is unbounded: the
        // worker claims jobs it cannot run, stranding them Running and starving
        // peers on the same database. Blocking here is what bounds it; the
        // permit rides with the job and is released when the task finishes.
        while let Some(job) = job_rx.recv().await {
            let permit = match self.concurrency.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => break,
            };
            let callback = self.callback.clone();
            let cancels = self.cancels.clone();
            let result_tx = result_tx.clone();
            spawn(async move {
                let _permit = permit;
                let job_id = job.id.clone();
                let result = run_one(&callback, &cancels, job).await;
                // Release the cancel record now the job has reported, so a
                // long-lived process does not accumulate ids.
                cancels.forget(&job_id);
                // A full bounded channel parks the sender — do it on the
                // blocking pool, never on the shared async runtime.
                match spawn_blocking(move || result_tx.send(result)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) | Err(_) => {
                        // A closed channel means the drain loop died — surface it.
                        log::error!(
                            "[flexiq-node] result channel closed; dropping outcome for {job_id}"
                        );
                    }
                }
            });
        }
    }

    fn shutdown(&self) {}

    fn notify_cancel(&self, job_id: &str) {
        self.cancels.signal(job_id);
    }
}

/// Invoke the JS task for one job and translate the outcome into a [`JobResult`].
async fn run_one(callback: &TaskCallback, cancels: &CancelSignals, mut job: Job) -> JobResult {
    let started = Instant::now();
    let invocation = JsTaskInvocation {
        id: job.id.clone(),
        task_name: job.task_name.clone(),
        // Never read from `job` again — take, don't clone (payloads can be
        // large); `job` stays whole for the later `failure(job, ...)` moves.
        payload: Buffer::from(std::mem::take(&mut job.payload)),
        attempt: job.retry_count,
        queue: job.queue.clone(),
    };

    // The callback runs on the JS thread and returns a Promise; bridge its
    // awaited value back to this async context through a oneshot. The JS view
    // is copied out (Buffer -> Vec) while still on that side.
    let (tx, rx) = oneshot::channel::<napi::Result<TaskOutcome>>();
    let dispatch = callback.call_with_return_value(
        invocation,
        ThreadsafeFunctionCallMode::NonBlocking,
        // napi 3 hands the callback a `Result`: a JS task that throws before it
        // can resolve an outcome arrives here as `Err` instead of aborting.
        move |promise: napi::Result<Promise<JsTaskOutcome>>, _env: Env| {
            spawn(async move {
                let outcome = match promise {
                    Ok(promise) => promise.await.map(TaskOutcome::from),
                    Err(err) => Err(err),
                };
                let _ = tx.send(outcome);
            });
            Ok(())
        },
    );
    // A rejected dispatch never runs the callback. When napi rejects it inside
    // the FFI call it also leaks the boxed closure, so `tx` is never dropped and
    // `rx` below would park forever on a job with no timeout. Retryable: the
    // queue is closing or saturated, not the task itself.
    if dispatch != Status::Ok {
        let wall_time_ns = started.elapsed().as_nanos() as i64;
        let reason = format!("node task dispatch rejected: {dispatch:?}");
        return failure(job, reason, wall_time_ns, false, true);
    }

    // Enforce the per-job timeout (the core stores `timeout_ms` but leaves
    // enforcement to the shell). `timeout_ms <= 0` means no limit.
    let timed = if job.timeout_ms > 0 {
        tokio::time::timeout(Duration::from_millis(job.timeout_ms as u64), rx).await
    } else {
        Ok(rx.await)
    };
    let wall_time_ns = started.elapsed().as_nanos() as i64;
    match timed {
        // A slept attempt is neither a result nor a failure, and it is matched
        // before either because it is the one ending whose state is already
        // written: the sleep row is committed and this worker's claim is gone.
        Ok(Ok(Ok(TaskOutcome {
            slept_until: Some(wake_at),
            ..
        }))) => JobResult::Slept {
            job_id: job.id,
            task_name: job.task_name,
            wake_at,
            wall_time_ns,
        },
        Ok(Ok(Ok(outcome))) => match outcome.error {
            None => JobResult::Success {
                job_id: job.id,
                result: outcome.result,
                task_name: job.task_name,
                wall_time_ns,
            },
            // A failed task that was cancel-requested is a cancellation, not a
            // failure (the JS side aborts via the cancel signal).
            Some(_) if cancels.is_cancelled(&job.id) => JobResult::Cancelled {
                job_id: job.id,
                task_name: job.task_name,
                wall_time_ns,
            },
            Some(error) => failure(job, error, wall_time_ns, false, outcome.retryable),
        },
        // The promise rejected rather than resolving an outcome: the shell threw
        // before it could shape one (an unregistered task, a payload it could
        // not decode). Retryable — another worker may well have the task.
        // `Error::to_string()` prepends the napi status ("GenericFailure, "), so
        // prefer the bare reason, which is the JS error's string form.
        Ok(Ok(Err(err))) => {
            let reason = if err.reason.is_empty() {
                err.to_string()
            } else {
                err.reason.clone()
            };
            failure(job, reason, wall_time_ns, false, true)
        }
        Ok(Err(_)) => failure(
            job,
            "node task channel dropped".to_string(),
            wall_time_ns,
            false,
            true,
        ),
        // We report the timeout, but the underlying JS promise cannot be force-
        // killed and keeps running in the background — same limitation as the
        // Python shell. Non-idempotent tasks should cooperate via the cancel
        // signal (`currentJob().signal`) and check it on long operations.
        Err(_) => failure(job, "task timed out".to_string(), wall_time_ns, true, true),
    }
}

/// One finished invocation, copied out of its JS view so it can cross threads.
struct TaskOutcome {
    result: Option<Vec<u8>>,
    error: Option<String>,
    /// Whether a failure may be retried. The shell's per-task `retryOn`
    /// predicate decides; an absent flag means retry, as before it existed.
    retryable: bool,
    /// Set when `ctx.step.sleep` ended the attempt: the committed deadline.
    slept_until: Option<i64>,
}

impl From<JsTaskOutcome> for TaskOutcome {
    fn from(outcome: JsTaskOutcome) -> Self {
        Self {
            result: outcome.result.map(|buffer| buffer.to_vec()),
            error: outcome.error,
            retryable: outcome.retryable.unwrap_or(true),
            slept_until: outcome.slept_until,
        }
    }
}

/// Build a [`JobResult::Failure`] from a job and error message. `should_retry`
/// false skips the retry budget entirely and dead-letters the job.
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
