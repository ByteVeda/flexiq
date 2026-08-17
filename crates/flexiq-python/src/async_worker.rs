use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use crossbeam_channel::Sender;
use pyo3::prelude::*;
use tokio::sync::Semaphore;

use flexiq_core::job::Job;
use flexiq_core::scheduler::JobResult;
use flexiq_core::worker::WorkerDispatcher;

use crate::py_worker::{
    check_should_retry, execute_task, format_python_error, get_exception_class_name,
    is_cancelled_error,
};

/// Async worker pool that dispatches jobs via tokio::spawn_blocking.
/// All GIL acquisition happens inside spawn_blocking — never in async context.
pub struct AsyncWorkerPool {
    num_workers: usize,
    task_registry: Arc<Py<PyAny>>,
    retry_filters: Arc<Py<PyAny>>,
    shutdown: AtomicBool,
}

impl AsyncWorkerPool {
    pub fn new(
        num_workers: usize,
        task_registry: Arc<Py<PyAny>>,
        retry_filters: Arc<Py<PyAny>>,
    ) -> Self {
        Self {
            num_workers,
            task_registry,
            retry_filters,
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
            let tx = result_tx.clone();

            tokio::task::spawn_blocking(move || {
                let _permit = permit; // Hold until task completes

                let job_id = job.id.clone();
                let task_name = job.task_name.clone();
                let retry_count = job.retry_count;
                let max_retries = job.max_retries;

                let start = std::time::Instant::now();
                log::info!("[flexiq] Task {task_name}[{job_id}] received");

                let result = Python::attach(|py| -> PyResult<Option<Vec<u8>>> {
                    execute_task(py, &registry, &job)
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
                    Err(e) => {
                        // Single GIL acquisition: extract the error info and the
                        // retry decision together instead of taking the GIL twice.
                        let (error_msg, is_cancelled, should_retry) = Python::attach(|py| {
                            let msg = format_python_error(py, &e);
                            let cancelled = is_cancelled_error(py, &e);
                            let retry = if cancelled {
                                false
                            } else {
                                let class_name = get_exception_class_name(py, &e);
                                check_should_retry(py, &filters, &task_name, &class_name, &e)
                            };
                            (msg, cancelled, retry)
                        });

                        if is_cancelled {
                            JobResult::Cancelled {
                                job_id,
                                task_name,
                                wall_time_ns,
                            }
                        } else {
                            log::error!("[flexiq] Task {task_name}[{job_id}] failed: {error_msg}");
                            JobResult::Failure {
                                job_id,
                                error: error_msg,
                                retry_count,
                                max_retries,
                                task_name,
                                wall_time_ns,
                                should_retry,
                                timed_out: false,
                            }
                        }
                    }
                };

                let _ = tx.send(job_result);
            });
        }
    }

    fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }
}
