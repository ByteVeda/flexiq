use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use crossbeam_channel::Sender;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::sync::Semaphore;

use flexiq_core::job::Job;
use flexiq_core::scheduler::JobResult;
use flexiq_core::worker::WorkerDispatcher;

use super::permit::PyJobPermit;
use super::task_executor::execute_sync_task;
use crate::py_worker_steps::PyWorkerSteps;

/// Dual-dispatch worker pool: async tasks run natively on a Python event loop,
/// sync tasks use `spawn_blocking`. Each branch is bounded by its own semaphore —
/// `num_workers` blocking threads, `async_concurrency` coroutines — so neither can
/// starve the other out of a shared budget.
pub struct NativeAsyncPool {
    num_workers: usize,
    async_concurrency: usize,
    task_registry: Arc<Py<PyAny>>,
    retry_filters: Arc<Py<PyAny>>,
    async_executor: Arc<Py<PyAny>>,
    /// This worker's step handle. Only the blocking branch carries it per job —
    /// the executor was constructed with its own copy for the coroutines.
    worker_steps: Arc<Py<PyWorkerSteps>>,
    shutdown: AtomicBool,
}

impl NativeAsyncPool {
    pub fn new(
        num_workers: usize,
        async_concurrency: usize,
        task_registry: Arc<Py<PyAny>>,
        retry_filters: Arc<Py<PyAny>>,
        async_executor: Arc<Py<PyAny>>,
        worker_steps: Arc<Py<PyWorkerSteps>>,
    ) -> Self {
        Self {
            num_workers,
            async_concurrency,
            task_registry,
            retry_filters,
            async_executor,
            worker_steps,
            shutdown: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl WorkerDispatcher for NativeAsyncPool {
    async fn run(
        &self,
        mut job_rx: tokio::sync::mpsc::Receiver<Job>,
        result_tx: Sender<JobResult>,
    ) {
        let sync_semaphore = Arc::new(Semaphore::new(self.num_workers));
        let async_semaphore = Arc::new(Semaphore::new(self.async_concurrency));

        while let Some(job) = job_rx.recv().await {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            let is_async = Python::attach(|py| {
                let registry = self.task_registry.bind(py);
                if let Ok(dict) = registry.cast::<pyo3::types::PyDict>() {
                    if let Ok(Some(wrapper)) = dict.get_item(&job.task_name) {
                        return wrapper
                            .getattr("_flexiq_is_async")
                            .and_then(|v| v.extract::<bool>())
                            .unwrap_or(false);
                    }
                }
                false
            });

            if is_async {
                // Take the slot before submitting, not inside the coroutine: dispatch
                // returns as soon as the job is queued, so without this the loop would
                // pull jobs from `job_rx` without bound and pile up Running-but-not-yet-
                // executing work. Blocking here is the point — it is what bounds the
                // in-flight count. The permit rides with the coroutine and is released
                // when it finishes (or is collected).
                let permit = match async_semaphore.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => break,
                };

                // Submit to the Python async executor — brief GIL hold, non-blocking
                let executor = self.async_executor.clone();
                Python::attach(|py| {
                    let exec = executor.bind(py);
                    let payload = PyBytes::new(py, &job.payload);
                    // Either failure drops the permit handle, returning the slot.
                    let submitted = Py::new(py, PyJobPermit::new(permit)).and_then(|handle| {
                        exec.call_method1(
                            "submit_job",
                            (
                                &job.id,
                                &job.task_name,
                                payload,
                                job.retry_count,
                                job.max_retries,
                                &job.queue,
                                handle,
                            ),
                        )
                        .map(|_| ())
                    });
                    if let Err(e) = submitted {
                        log::error!(
                            "[flexiq] Failed to submit async task {}[{}]: {e}",
                            job.task_name,
                            job.id
                        );
                        let _ = result_tx.send(JobResult::Failure {
                            job_id: job.id.clone(),
                            error: format!("async submit error: {e}"),
                            retry_count: job.retry_count,
                            max_retries: job.max_retries,
                            task_name: job.task_name.clone(),
                            wall_time_ns: 0,
                            should_retry: true,
                            timed_out: false,
                        });
                    }
                });
            } else {
                // Sync path: spawn_blocking bounded by semaphore
                let permit = match sync_semaphore.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => break,
                };

                let registry = self.task_registry.clone();
                let filters = self.retry_filters.clone();
                let steps = self.worker_steps.clone();
                let tx = result_tx.clone();

                tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    execute_sync_task(&registry, &filters, &steps, &job, &tx);
                });
            }
        }
    }

    fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }
}
