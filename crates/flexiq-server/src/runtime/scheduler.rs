//! The scheduler half of the server: a `Worker` whose dispatcher is the
//! `RemoteDispatcher` fed by attached executors.
//!
//! It starts lazily, on the first successful attach. Starting eagerly would
//! claim jobs no attached executor advertises, and every one of them would fail
//! retryably once `placement_timeout` elapsed — a retry storm against an idle
//! deployment. Once started it stays up: an executor that detaches leaves its
//! in-flight jobs to the dead-owner reaper, exactly as a crashed worker does.

use std::sync::Mutex;

use anyhow::{Context, Result};
use flexiq_core::scheduler::retention::RetentionConfig;
use flexiq_core::{RemoteDispatcher, SchedulerConfig, StorageBackend, Worker, WorkerHandle};

/// Pool type reported to the worker registry, so `queue.workers()` shows what
/// is actually running the jobs.
const POOL_TYPE: &str = "remote";

/// What the supervisor needs to build its `Worker` on demand.
pub struct SchedulerSettings {
    /// Queues the scheduler consumes.
    pub queues: Vec<String>,
    /// Tenant namespace, when scoped.
    pub namespace: Option<String>,
    /// Dispatch concurrency override. `None` sizes it from advertised capacity.
    pub workers: Option<usize>,
    /// Whether this process runs retention and cleanup.
    pub maintenance: bool,
}

/// Owns the scheduler's lifecycle: start-once, shutdown-once.
pub struct SchedulerSupervisor {
    storage: StorageBackend,
    dispatcher: RemoteDispatcher,
    settings: SchedulerSettings,
    handle: Mutex<Option<WorkerHandle>>,
}

impl SchedulerSupervisor {
    /// Build a supervisor that has not started its worker yet.
    pub fn new(
        storage: StorageBackend,
        dispatcher: RemoteDispatcher,
        settings: SchedulerSettings,
    ) -> Self {
        Self {
            storage,
            dispatcher,
            settings,
            handle: Mutex::new(None),
        }
    }

    /// Start the scheduler if it is not running. Safe to call on every attach.
    pub fn ensure_started(&self) -> Result<()> {
        let mut handle = self.handle.lock().unwrap_or_else(|e| e.into_inner());
        if handle.is_some() {
            return Ok(());
        }
        let worker = self
            .build_worker()
            .spawn()
            .context("scheduler failed to start")?;
        log::info!(
            "[flexiq] scheduler {} started on queues [{}]",
            worker.worker_id(),
            self.settings.queues.join(", ")
        );
        *handle = Some(worker);
        Ok(())
    }

    /// Whether the scheduler is running.
    pub fn is_running(&self) -> bool {
        self.handle
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
    }

    /// Drain in-flight work and unregister. Blocks until the worker's threads
    /// have exited; a no-op when the scheduler never started.
    pub fn shutdown(&self) {
        let taken = self.handle.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(worker) = taken {
            if let Err(error) = worker.shutdown() {
                log::warn!("scheduler shutdown reported an error: {error}");
            }
        }
    }

    fn build_worker(&self) -> Worker {
        let num_workers = self.settings.workers.unwrap_or_else(|| {
            // Sized from what is attached right now: `max_in_flight` derives
            // from this, so it must not exceed the slots executors advertise.
            self.dispatcher.capacity().total_slots.max(1) as usize
        });

        let mut scheduler_config = SchedulerConfig::default();
        if !self.settings.maintenance {
            // An empty retention config is the documented "keep everything"
            // switch; dead-worker reaping stays on because in-flight recovery
            // depends on it.
            scheduler_config.retention = Some(RetentionConfig::default());
        }

        let mut worker = Worker::new(self.storage.clone())
            .queues(self.settings.queues.clone())
            .num_workers(num_workers)
            .scheduler_config(scheduler_config)
            .dispatcher(POOL_TYPE, std::sync::Arc::new(self.dispatcher.clone()));
        if let Some(namespace) = &self.settings.namespace {
            worker = worker.namespace(namespace.clone());
        }
        worker
    }
}
