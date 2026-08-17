//! Reference [`WorkerDispatcher`]: executes registered Rust handlers.
//!
//! Mirrors the language bindings' pools — bounded concurrency via a semaphore,
//! blocking handlers on `spawn_blocking`, async handlers on the runtime — with
//! a [`TaskRegistry`] in place of a foreign-object registry. Task timeouts are
//! detected server-side by the scheduler's stale-job reap; cancellation relies
//! on the storage cancel flag, so `notify_cancel` is a no-op here.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use crossbeam_channel::Sender;
use tokio::sync::Semaphore;

use super::registry::{TaskHandler, TaskRegistry, TaskResult};
use super::WorkerDispatcher;
use crate::job::Job;
use crate::scheduler::JobResult;

/// Dispatches dequeued jobs to handlers from a [`TaskRegistry`], at most
/// `num_workers` concurrently.
pub struct NativeDispatcher {
    registry: Arc<TaskRegistry>,
    num_workers: usize,
    shutdown: AtomicBool,
}

impl NativeDispatcher {
    /// Build a dispatcher over `registry`, running at most `num_workers`
    /// handlers concurrently (minimum 1).
    pub fn new(registry: TaskRegistry, num_workers: usize) -> Self {
        Self {
            registry: Arc::new(registry),
            num_workers: num_workers.max(1),
            shutdown: AtomicBool::new(false),
        }
    }
}

/// Build the [`JobResult`] for a finished handler invocation.
fn job_result(job: &Job, outcome: TaskResult, started: Instant) -> JobResult {
    let wall_time_ns: i64 = started.elapsed().as_nanos().try_into().unwrap_or(i64::MAX);
    match outcome {
        Ok(result) => JobResult::Success {
            job_id: job.id.clone(),
            result,
            task_name: job.task_name.clone(),
            wall_time_ns,
        },
        Err(task_error) => JobResult::Failure {
            job_id: job.id.clone(),
            error: task_error.message,
            retry_count: job.retry_count,
            max_retries: job.max_retries,
            task_name: job.task_name.clone(),
            wall_time_ns,
            should_retry: task_error.retryable,
            // Timeouts are synthesized server-side by the stale-job reap.
            timed_out: false,
        },
    }
}

#[async_trait]
impl WorkerDispatcher for NativeDispatcher {
    async fn run(
        &self,
        mut job_rx: tokio::sync::mpsc::Receiver<Job>,
        result_tx: Sender<JobResult>,
    ) {
        let semaphore = Arc::new(Semaphore::new(self.num_workers));

        while let Some(job) = job_rx.recv().await {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => break, // Semaphore closed.
            };

            let handler = match self.registry.get(&job.task_name) {
                Some(handler) => handler.clone(),
                None => {
                    // Fatal, not retryable: no amount of retrying makes an
                    // unregistered task runnable on this worker.
                    let error = super::registry::TaskError::fatal(format!(
                        "task not registered: {}",
                        job.task_name
                    ));
                    let _ = result_tx.send(job_result(&job, Err(error), Instant::now()));
                    continue;
                }
            };

            let tx = result_tx.clone();
            match handler {
                TaskHandler::Sync(run) => {
                    tokio::task::spawn_blocking(move || {
                        let _permit = permit; // Hold the slot until done.
                        let started = Instant::now();
                        let outcome = run(&job);
                        let _ = tx.send(job_result(&job, outcome, started));
                    });
                }
                TaskHandler::Async(make_future) => {
                    tokio::spawn(async move {
                        let _permit = permit;
                        let started = Instant::now();
                        let outcome = make_future(job.clone()).await;
                        let _ = tx.send(job_result(&job, outcome, started));
                    });
                }
            }
        }
    }

    fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}
