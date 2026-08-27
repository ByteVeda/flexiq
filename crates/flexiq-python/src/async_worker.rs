use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use crossbeam_channel::Sender;
use pyo3::prelude::*;
use tokio::sync::Semaphore;

use flexiq_core::job::Job;
use flexiq_core::scheduler::JobResult;
use flexiq_core::worker::WorkerDispatcher;

use crate::py_worker::{execute_task, job_result_from_error};
use crate::py_worker_steps::PyWorkerSteps;

/// Async worker pool that dispatches jobs via tokio::spawn_blocking.
/// All GIL acquisition happens inside spawn_blocking — never in async context.
pub struct AsyncWorkerPool {
    num_workers: usize,
    task_registry: Arc<Py<PyAny>>,
    retry_filters: Arc<Py<PyAny>>,
    /// This worker's step handle, handed to every task it runs — the claim a
    /// durable step is fenced on is the one this worker won.
    worker_steps: Arc<Py<PyWorkerSteps>>,
    shutdown: AtomicBool,
}

impl AsyncWorkerPool {
    pub fn new(
        num_workers: usize,
        task_registry: Arc<Py<PyAny>>,
        retry_filters: Arc<Py<PyAny>>,
        worker_steps: Arc<Py<PyWorkerSteps>>,
    ) -> Self {
        Self {
            num_workers,
            task_registry,
            retry_filters,
            worker_steps,
            shutdown: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl WorkerDispatcher for AsyncWorkerPool {
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
                Ok(p) => p,
                Err(_) => break, // Semaphore closed
            };

            let registry = self.task_registry.clone();
            let filters = self.retry_filters.clone();
            let steps = self.worker_steps.clone();
            let tx = result_tx.clone();

            tokio::task::spawn_blocking(move || {
                let _permit = permit; // Hold until task completes

                let job_id = job.id.clone();
                let task_name = job.task_name.clone();

                let start = std::time::Instant::now();
                log::info!("[flexiq] Task {task_name}[{job_id}] received");

                let result = Python::attach(|py| -> PyResult<Option<Vec<u8>>> {
                    execute_task(py, &registry, &steps, &job)
                });

                let wall_time_ns: i64 = start.elapsed().as_nanos().try_into().unwrap_or(i64::MAX);

                let job_result = match result {
                    Ok(result_bytes) => {
                        let secs = start.elapsed().as_secs_f64();
                        log::info!("[flexiq] Task {task_name}[{job_id}] succeeded in {secs:.3}s");
                        JobResult::Success {
                            job_id,
                            result: result_bytes,
                            task_name,
                            wall_time_ns,
                        }
                    }
                    Err(e) => job_result_from_error(&e, &filters, &job, wall_time_ns),
                };

                let _ = tx.send(job_result);
            });
        }
    }

    fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }
}
