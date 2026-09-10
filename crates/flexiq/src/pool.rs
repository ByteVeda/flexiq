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
use crate::{Abort, Outcome, Task};

/// One registered task's body, erased of its argument types.
type Handler = Arc<dyn Fn(&Job) -> Outcome<Option<Vec<u8>>> + Send + Sync>;

/// One scheduled task's registration, erased the same way.
type PeriodicRegistration = Box<dyn FnOnce(&StorageBackend) -> Result<()> + Send>;

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
    periodics: Vec<PeriodicRegistration>,
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
            periodics: Vec::new(),
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
        if let Some(spec) = T::periodic() {
            // Boxed as a closure so the builder does not have to carry `T`.
            let register: PeriodicRegistration =
                Box::new(move |storage| crate::cron::register::<T>(storage, &spec));
            self.periodics.push(register);
        }
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

        // Before the scheduler starts, so a periodic that is already due is
        // found on the first tick rather than one interval late.
        for register in self.periodics {
            register(&self.storage)?;
        }

        let dispatcher = Arc::new(ShellDispatcher::new(
            self.handlers,
            self.storage.clone(),
            self.num_workers,
        ));

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
    /// Held so a dispatch can open its own step session; the scheduler hands
    /// this pool jobs, not a way back to storage.
    storage: StorageBackend,
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
    fn new(
        handlers: HashMap<String, Handler>,
        storage: StorageBackend,
        num_workers: usize,
    ) -> Self {
        Self {
            handlers: Arc::new(handlers),
            storage,
            num_workers: num_workers.max(1),
            shutdown: AtomicBool::new(false),
            owner: Mutex::new(String::new()),
            leases: Mutex::new(None),
        }
    }

    /// The fence this dispatch runs under.
    ///
    /// Read here rather than at the step call site because a lease is a
    /// property of the dispatch, not of the step. The epoch is the term core's
    /// own pool cannot supply, having thrown the lease book away.
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
            let (owner, _attempt, epoch) = self.fence(&job);
            let storage = self.storage.clone();

            // Every handler is synchronous, so every handler runs here. An
            // async task is refused by the macro rather than silently blocking
            // a runtime thread.
            tokio::task::spawn_blocking(move || {
                let _permit = permit; // Hold the slot until the body returns.
                let started = Instant::now();

                // A session that cannot be opened is not fatal to the job: a
                // task that never calls `current_step` does not need one, and
                // one that does gets a message naming the failure.
                match crate::steps::open(&storage, &job, &owner, epoch) {
                    Ok(session) => crate::steps::install(session),
                    Err(e) => log::debug!("no step session for job {}: {e}", job.id),
                }

                let outcome = handler(&job);

                // Blocking threads are reused, so the session has to come back
                // off this one however the body ended.
                if let Some(session) = crate::steps::take() {
                    session.finish();
                }
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

#[cfg(test)]
mod tests {
    use flexiq_core::{Lease, NewJob, SqliteStorage};

    use super::*;

    fn dispatcher() -> ShellDispatcher {
        let storage = StorageBackend::Sqlite(SqliteStorage::in_memory().expect("opens"));
        ShellDispatcher::new(HashMap::new(), storage, 1)
    }

    fn job() -> Job {
        NewJob {
            queue: "default".into(),
            task_name: "charge".into(),
            payload: Vec::new(),
            priority: 0,
            scheduled_at: 0,
            max_retries: 3,
            timeout_ms: 1_000,
            unique_key: None,
            metadata: None,
            notes: None,
            depends_on: Vec::new(),
            expires_at: None,
            result_ttl_ms: None,
            namespace: None,
            debounce_key: None,
        }
        .into_job()
    }

    /// The reason this pool exists.
    ///
    /// `NativeDispatcher` leaves `set_claim_owner` and `set_lease_book` as the
    /// trait's default no-ops, so the fence the scheduler minted is gone by the
    /// time a handler runs. Every other shell fences a step on `(owner,
    /// attempt)` and leaves the epoch unset for want of somewhere to keep the
    /// book; this asserts all three terms survive.
    #[test]
    fn the_dispatcher_keeps_the_whole_fence() {
        let dispatcher = dispatcher();
        let job = job();

        let leases = Arc::new(LeaseBook::default());
        leases.issue(&job.id, Lease::from_epoch(4_242));
        dispatcher.set_claim_owner("rust-worker-1");
        dispatcher.set_lease_book(leases);

        let (owner, attempt, epoch) = dispatcher.fence(&job);
        assert_eq!(owner, "rust-worker-1");
        assert_eq!(attempt, job.retry_count);
        assert_eq!(epoch, Some(4_242));
    }

    /// A job the book has no lease for still fences on the first two terms,
    /// rather than failing to open a session at all.
    #[test]
    fn a_job_with_no_lease_still_has_an_owner_and_an_attempt() {
        let dispatcher = dispatcher();
        let job = job();
        dispatcher.set_claim_owner("rust-worker-1");
        dispatcher.set_lease_book(Arc::new(LeaseBook::default()));

        let (owner, _attempt, epoch) = dispatcher.fence(&job);
        assert_eq!(owner, "rust-worker-1");
        assert_eq!(epoch, None);
    }
}
