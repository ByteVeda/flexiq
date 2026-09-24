//! The scheduler half of the server: a `Worker` whose dispatcher is whichever
//! [`DispatchPath`] this deployment configured.
//!
//! Under attach it starts lazily, on the first successful attach. Starting
//! eagerly would claim jobs no attached executor advertises, and every one of
//! them would fail retryably once `placement_timeout` elapsed — a retry storm
//! against an idle deployment. Under push there is no peer to wait for, so it
//! starts at boot; see [`DispatchPath::starts_eagerly`]. Once started it stays
//! up: an executor that detaches leaves its in-flight jobs to the dead-owner
//! reaper, exactly as a crashed worker does.

use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use flexiq_core::scheduler::retention::RetentionConfig;
#[cfg(feature = "http-target")]
use flexiq_core::HttpDispatchTarget;
use flexiq_core::{
    RemoteDispatcher, SchedulerConfig, StorageBackend, Worker, WorkerDispatcher, WorkerHandle,
};

/// Pool type the attach path reports to the worker registry, so
/// `queue.workers()` shows what is actually running the jobs.
const ATTACH_POOL_TYPE: &str = "remote";

/// Pool type the push path reports. Distinct from [`ATTACH_POOL_TYPE`] on
/// purpose: the two paths fail in different places, and an operator reading
/// `queue.workers()` should not have to guess which one this process runs.
#[cfg(feature = "http-target")]
const PUSH_POOL_TYPE: &str = "http-push";

/// How this deployment gets jobs to the code that runs them.
///
/// `Worker` holds exactly one dispatcher, so this is a choice, not a set. The
/// config layer already refuses a deployment that asks for both.
pub enum DispatchPath {
    /// Executors dial in and hold the connection.
    Attach(RemoteDispatcher),
    /// The scheduler dials out, once per job.
    ///
    /// `Arc`, not `Box`: [`Self::as_worker_dispatcher`] has to hand `Worker`
    /// an *owned* `Arc<dyn WorkerDispatcher>`, and `HttpDispatchTarget` is
    /// deliberately not `Clone` — it owns the guarded client, the slot
    /// semaphore and the lease book every attempt fences on, none of which may
    /// be duplicated. One refcounted target keeps this variant pointer-sized
    /// too, which is what a `Box` would have been for.
    #[cfg(feature = "http-target")]
    Push(Arc<HttpDispatchTarget>),
}

impl DispatchPath {
    /// What the worker registry reports, so `queue.workers()` names what is
    /// actually running the jobs.
    pub fn pool_type(&self) -> &'static str {
        match self {
            DispatchPath::Attach(_) => ATTACH_POOL_TYPE,
            #[cfg(feature = "http-target")]
            DispatchPath::Push(_) => PUSH_POOL_TYPE,
        }
    }

    /// Slots to size `num_workers` — and therefore `max_in_flight` — from.
    ///
    /// Attach reads what is attached right now; push reads the configured
    /// capacity, because a push target sends no `hello` and announces no slots
    /// of its own.
    pub fn total_slots(&self) -> u32 {
        match self {
            DispatchPath::Attach(dispatcher) => dispatcher.capacity().total_slots,
            #[cfg(feature = "http-target")]
            DispatchPath::Push(target) => target.capacity().total_slots,
        }
    }

    /// Whether the scheduler should start at boot rather than on first attach.
    ///
    /// The laziness this answers `false` for is about *waiting for a peer*:
    /// an attach deployment that claimed jobs before an executor connected
    /// would fail every one of them retryably once `placement_timeout`
    /// elapsed. A push target is there by configuration — there is no peer to
    /// wait for and no attach that would ever start it — so it answers `true`
    /// and `runtime::run` calls [`SchedulerSupervisor::ensure_started`] at
    /// boot.
    pub fn starts_eagerly(&self) -> bool {
        match self {
            DispatchPath::Attach(_) => false,
            #[cfg(feature = "http-target")]
            DispatchPath::Push(_) => true,
        }
    }

    /// The dispatcher `Worker` runs jobs through.
    pub fn as_worker_dispatcher(&self) -> Arc<dyn WorkerDispatcher> {
        match self {
            // Cloned, not shared: `RemoteDispatcher`'s clone is a handle onto
            // one registry, which is how the listener thread and the scheduler
            // already hold the same dispatcher.
            DispatchPath::Attach(dispatcher) => Arc::new(dispatcher.clone()),
            #[cfg(feature = "http-target")]
            DispatchPath::Push(target) => target.clone(),
        }
    }
}

/// What the supervisor needs to build its `Worker` on demand.
pub struct SchedulerSettings {
    /// Queues the scheduler consumes.
    pub queues: Vec<String>,
    /// Tenant namespace, when scoped.
    pub namespace: Option<String>,
    /// Dispatch concurrency override. `None` sizes it from
    /// [`DispatchPath::total_slots`].
    pub workers: Option<usize>,
    /// Whether this process runs retention and cleanup.
    pub maintenance: bool,
    /// Wake-on-enqueue choice; `None` keeps the backend default.
    pub push_dispatch: Option<bool>,
}

/// Owns the scheduler's lifecycle: start-once, shutdown-once.
pub struct SchedulerSupervisor {
    storage: StorageBackend,
    path: DispatchPath,
    settings: SchedulerSettings,
    handle: Mutex<Option<WorkerHandle>>,
}

impl SchedulerSupervisor {
    /// Build a supervisor that has not started its worker yet.
    pub fn new(storage: StorageBackend, path: DispatchPath, settings: SchedulerSettings) -> Self {
        Self {
            storage,
            path,
            settings,
            handle: Mutex::new(None),
        }
    }

    /// Start the scheduler if it is not running. Safe to call on every attach,
    /// and called once at boot on a path that [`DispatchPath::starts_eagerly`].
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
    ///
    /// Routed through [`WorkerHandle::shutdown`] rather than by dropping the
    /// handle, and **that ordering is load-bearing**: it notifies the
    /// scheduler, calls `dispatcher.shutdown()`, and only then joins the
    /// worker's threads. A dispatcher settles results on a *bounded* channel,
    /// so a send parks whenever nothing is draining, and the drain thread
    /// exits only once the dispatcher's `run` has already returned. Stopping
    /// or joining the drain ahead of `run` would park the last in-flight
    /// settlement for good. Do not invert it.
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
            // Sized from the slots the configured path actually has —
            // advertised by attached executors, or configured on the push
            // target. `max_in_flight` derives from this, so it must not exceed
            // them.
            self.path.total_slots().max(1) as usize
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
            .dispatcher(self.path.pool_type(), self.path.as_worker_dispatcher());
        if let Some(namespace) = &self.settings.namespace {
            worker = worker.namespace(namespace.clone());
        }
        if let Some(enabled) = self.settings.push_dispatch {
            worker = worker.push_dispatch(enabled);
        }
        worker
    }
}
