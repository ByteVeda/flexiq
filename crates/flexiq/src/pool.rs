//! The worker, and the pool that carries the fence.
//!
//! Core ships a reference pool, [`flexiq_core::NativeDispatcher`], and this is
//! not it. That one hands a handler a `&Job` and nothing else, and it overrides
//! neither `set_claim_owner` nor `set_lease_book` — both are default no-ops on
//! the trait, so the `(owner, attempt, epoch)` the scheduler mints for every
//! dispatch is dropped before any handler could see it. A durable step is
//! written under that fence, so a pool that discards it cannot open a step
//! session at all.
//!
//! This pool keeps both, and is otherwise the same shape: a semaphore bounding
//! concurrency, handlers on `spawn_blocking`, an unregistered task failed
//! fatally rather than retried.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use crossbeam_channel::Sender;
use flexiq_core::{
    Job, JobResult, LeaseBook, QueueError, Result, SchedulerConfig, StorageBackend, TaskConfig,
    TaskError, Worker, WorkerDispatcher, WorkerHandle,
};
use tokio::sync::Semaphore;

use crate::outcome::task_error_json;
use crate::{Abort, Outcome, StepHandle, Task};

/// One registered task's body, erased of its argument types.
type Handler = Arc<dyn Fn(&Job, &mut StepHandle) -> Outcome<Option<Vec<u8>>> + Send + Sync>;

/// Builds a worker over the tasks registered on it.
///
/// Not called `Worker`: core exports a type by that name through this crate's
/// glob re-export, and shadowing it would silently retype `flexiq::Worker` for
/// anyone already using it.
pub struct WorkerBuilder {
    storage: StorageBackend,
    namespace: Option<String>,
    handlers: HashMap<String, Handler>,
    configs: Vec<(String, TaskConfig)>,
    queues: Vec<String>,
    num_workers: usize,
    worker_id: Option<String>,
    scheduler_config: Option<SchedulerConfig>,
    duplicate: Option<String>,
}

impl WorkerBuilder {
    /// Start a builder over an open backend.
    pub(crate) fn new(storage: StorageBackend, namespace: Option<String>) -> Self {
        Self {
            storage,
            namespace,
            handlers: HashMap::new(),
            configs: Vec::new(),
            queues: vec!["default".to_string()],
            num_workers: 4,
            worker_id: None,
            scheduler_config: None,
            duplicate: None,
        }
    }

    /// Register a task this worker can run.
    ///
    /// A name registered twice is remembered and refused by [`spawn`](Self::spawn)
    /// rather than here, so the whole registration reads as one chain and the
    /// error arrives with the rest of the startup checks.
    pub fn register<T: Task>(mut self) -> Self {
        let handler: Handler = Arc::new(T::run_encoded);
        if self.handlers.insert(T::NAME.to_string(), handler).is_some() {
            self.duplicate.get_or_insert_with(|| T::NAME.to_string());
        }
        self.configs.push((T::NAME.to_string(), T::config()));
        self
    }

    /// Pull from these queues instead of `default`.
    pub fn queues(mut self, queues: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.queues = queues.into_iter().map(Into::into).collect();
        self
    }

    /// Run at most this many handlers at once.
    pub fn num_workers(mut self, num_workers: usize) -> Self {
        self.num_workers = num_workers.max(1);
        self
    }

    /// Name this worker, instead of taking a generated id.
    pub fn worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.worker_id = Some(worker_id.into());
        self
    }

    /// Tune the scheduler this worker runs.
    pub fn scheduler_config(mut self, config: SchedulerConfig) -> Self {
        self.scheduler_config = Some(config);
        self
    }

    /// Register this process, start the scheduler and the pool, and return a
    /// handle whose `shutdown` drains and unregisters.
    pub fn spawn(self) -> Result<WorkerHandle> {
        if let Some(name) = self.duplicate {
            return Err(QueueError::Other(format!(
                "task `{name}` was registered twice on one worker: one of the two bodies \
                 would never run"
            )));
        }

        let dispatcher = Arc::new(ShellDispatcher::new(self.handlers, self.num_workers));

        let mut worker = Worker::new(self.storage)
            .queues(self.queues)
            .num_workers(self.num_workers)
            // "rust" is this pool's reported type, beside the shells' own.
            .dispatcher("rust", dispatcher);

        if let Some(namespace) = self.namespace {
            worker = worker.namespace(namespace);
        }
        if let Some(worker_id) = self.worker_id {
            worker = worker.worker_id(worker_id);
        }
        if let Some(config) = self.scheduler_config {
            worker = worker.scheduler_config(config);
        }
        for (name, config) in self.configs {
            worker = worker.task_config(name, config);
        }

        worker.spawn()
    }
}

/// The pool the shell runs its handlers on.
struct ShellDispatcher {
    handlers: Arc<HashMap<String, Handler>>,
    num_workers: usize,
    shutdown: AtomicBool,
    /// The claim owner, handed over by the scheduler at startup. Core's own
    /// pool throws this away; a step session is fenced on it.
    owner: Mutex<String>,
    /// The lease book, likewise. `LeaseBook::current` is where a dispatch's
    /// epoch comes from.
    leases: Mutex<Option<Arc<LeaseBook>>>,
}

impl ShellDispatcher {
    fn new(handlers: HashMap<String, Handler>, num_workers: usize) -> Self {
        Self {
            handlers: Arc::new(handlers),
            num_workers: num_workers.max(1),
            shutdown: AtomicBool::new(false),
            owner: Mutex::new(String::new()),
            leases: Mutex::new(None),
        }
    }

    /// The fence this dispatch runs under.
    ///
    /// Unused until a step session needs it, and read here rather than at the
    /// step call site because the lease is a property of the dispatch.
    #[allow(dead_code)]
    fn fence(&self, job: &Job) -> (String, i32, Option<i64>) {
        let owner = self.owner.lock().expect("owner lock").clone();
        let epoch = self
            .leases
            .lock()
            .expect("lease lock")
            .as_ref()
            .and_then(|book| book.current(&job.id))
            .and_then(|lease| lease.epoch());
        (owner, job.retry_count, epoch)
    }
}

/// Build the [`JobResult`] for a finished handler invocation.
///
/// The one place three outcomes become three arms. `Slept` in particular must
/// not be reported as a success or a failure: the job is already `Pending` at
/// its wake time with its claim released, and the scheduler skips its fence for
/// exactly that reason.
fn job_result(job: &Job, outcome: Outcome<Option<Vec<u8>>>, started: Instant) -> JobResult {
    let wall_time_ns: i64 = started.elapsed().as_nanos().try_into().unwrap_or(i64::MAX);
    match outcome {
        Ok(result) => JobResult::Success {
            job_id: job.id.clone(),
            result,
            task_name: job.task_name.clone(),
            wall_time_ns,
        },
        Err(Abort::Fail(err)) => JobResult::Failure {
            job_id: job.id.clone(),
            // The contract's shape, not the bare message core's pool stores.
            error: task_error_json(&err),
            retry_count: job.retry_count,
            max_retries: job.max_retries,
            task_name: job.task_name.clone(),
            wall_time_ns,
            should_retry: err.retryable,
            // Timeouts are synthesized server-side by the stale-job reap.
            timed_out: false,
        },
        Err(Abort::Sleep(sleep)) => JobResult::Slept {
            job_id: job.id.clone(),
            task_name: job.task_name.clone(),
            wake_at: wake_at(&sleep),
            wall_time_ns,
        },
    }
}

/// When a sleeping attempt asked to be woken.
fn wake_at(sleep: &flexiq_core::StepSleep) -> i64 {
    match sleep {
        flexiq_core::StepSleep::Sleeping { wake_at, .. }
        | flexiq_core::StepSleep::Elapsed { wake_at, .. } => *wake_at,
    }
}

#[async_trait]
impl WorkerDispatcher for ShellDispatcher {
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

            let handler = match self.handlers.get(&job.task_name) {
                Some(handler) => handler.clone(),
                None => {
                    // Fatal, not retryable: no amount of retrying makes an
                    // unregistered task runnable on this worker.
                    let error = TaskError::fatal(format!(
                        "task not registered on this worker: {}",
                        job.task_name
                    ));
                    let _ =
                        result_tx.send(job_result(&job, Err(Abort::Fail(error)), Instant::now()));
                    continue;
                }
            };

            let tx = result_tx.clone();
            // Every handler is synchronous, so every handler runs here. An
            // async task is refused by the macro rather than silently blocking
            // a runtime thread.
            tokio::task::spawn_blocking(move || {
                let _permit = permit; // Hold the slot until the body returns.
                let started = Instant::now();
                let mut step = StepHandle::detached();
                let outcome = handler(&job, &mut step);
                let _ = tx.send(job_result(&job, outcome, started));
            });
        }
    }

    fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    fn set_claim_owner(&self, owner: &str) {
        *self.owner.lock().expect("owner lock") = owner.to_string();
    }

    fn set_lease_book(&self, leases: Arc<LeaseBook>) {
        *self.leases.lock().expect("lease lock") = Some(leases);
    }
}
