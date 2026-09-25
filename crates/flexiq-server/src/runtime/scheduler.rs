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
    EventHub, RemoteDispatcher, SchedulerConfig, StorageBackend, Worker, WorkerDispatcher,
    WorkerHandle,
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
    /// Where the scheduler's job lifecycle events go. `None` emits none.
    /// Shut down by `runtime::run`, after the worker, never by the worker.
    pub events: Option<Arc<EventHub>>,
}

/// One scheduler this process runs: a dispatch path and the queues it serves.
///
/// A process has one lane unless it pushes to more than one target. Each
/// target then gets a lane of its own — its own `Worker`, claiming only its
/// own queues, sized from its own capacity — because a `Worker` holds exactly
/// one dispatcher, and one scheduler feeding several targets would claim jobs
/// for a target that has no free slot while another sits idle.
pub struct Lane {
    /// Names the lane in logs. `None` for the single-target and attach shapes,
    /// which have nothing to tell apart.
    pub name: Option<String>,
    /// How this lane's jobs reach the code that runs them.
    pub path: DispatchPath,
    /// What its `Worker` is built from.
    pub settings: SchedulerSettings,
}

/// Owns the scheduler's lifecycle: start-once, shutdown-once.
pub struct SchedulerSupervisor {
    storage: StorageBackend,
    lanes: Vec<Lane>,
    handles: Mutex<Option<Vec<WorkerHandle>>>,
}

impl SchedulerSupervisor {
    /// Build a supervisor over one dispatch path that has not started its
    /// worker yet.
    pub fn new(storage: StorageBackend, path: DispatchPath, settings: SchedulerSettings) -> Self {
        Self::with_lanes(
            storage,
            vec![Lane {
                name: None,
                path,
                settings,
            }],
        )
    }

    /// Build a supervisor that runs one `Worker` per lane, none started yet.
    pub fn with_lanes(storage: StorageBackend, lanes: Vec<Lane>) -> Self {
        Self {
            storage,
            lanes,
            handles: Mutex::new(None),
        }
    }

    /// Start the scheduler if it is not running. Safe to call on every attach,
    /// and called once at boot on a path that [`DispatchPath::starts_eagerly`].
    ///
    /// Every lane starts, or none does: a lane that fails shuts the ones
    /// already started back down, so a half-started process never reads as
    /// running.
    pub fn ensure_started(&self) -> Result<()> {
        let mut handles = self.handles.lock().unwrap_or_else(|e| e.into_inner());
        if handles.is_some() {
            return Ok(());
        }
        let mut started = Vec::with_capacity(self.lanes.len());
        for (index, lane) in self.lanes.iter().enumerate() {
            let spawned = self
                .build_worker(lane, index == 0)
                .spawn()
                .with_context(|| match &lane.name {
                    Some(name) => format!("scheduler for push target {name} failed to start"),
                    None => "scheduler failed to start".to_string(),
                });
            let worker = match spawned {
                Ok(worker) => worker,
                Err(error) => {
                    shutdown_all(started);
                    return Err(error);
                }
            };
            match &lane.name {
                Some(name) => log::info!(
                    "[flexiq] scheduler {} started for push target {name} on queues [{}]",
                    worker.worker_id(),
                    lane.settings.queues.join(", ")
                ),
                None => log::info!(
                    "[flexiq] scheduler {} started on queues [{}]",
                    worker.worker_id(),
                    lane.settings.queues.join(", ")
                ),
            }
            started.push(worker);
        }
        *handles = Some(started);
        Ok(())
    }

    /// Whether the scheduler is running.
    pub fn is_running(&self) -> bool {
        self.handles
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
        let taken = self
            .handles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(workers) = taken {
            shutdown_all(workers);
        }
    }

    /// Build one lane's `Worker`. `first` is whether it is the process's
    /// first lane, which alone runs retention: several lanes on one database
    /// would otherwise sweep the same rows once each.
    fn build_worker(&self, lane: &Lane, first: bool) -> Worker {
        let settings = &lane.settings;
        let num_workers = settings.workers.unwrap_or_else(|| {
            // Sized from the slots the configured path actually has —
            // advertised by attached executors, or configured on the push
            // target. `max_in_flight` derives from this, so it must not exceed
            // them.
            lane.path.total_slots().max(1) as usize
        });

        let mut scheduler_config = SchedulerConfig::default();
        if !settings.maintenance || !first {
            // An empty retention config is the documented "keep everything"
            // switch; dead-worker reaping stays on because in-flight recovery
            // depends on it.
            scheduler_config.retention = Some(RetentionConfig::default());
        }

        let mut worker = Worker::new(self.storage.clone())
            .queues(settings.queues.clone())
            .num_workers(num_workers)
            .scheduler_config(scheduler_config)
            .dispatcher(lane.path.pool_type(), lane.path.as_worker_dispatcher());
        if let Some(namespace) = &settings.namespace {
            worker = worker.namespace(namespace.clone());
        }
        if let Some(enabled) = settings.push_dispatch {
            worker = worker.push_dispatch(enabled);
        }
        if let Some(hub) = &settings.events {
            worker = worker.events(Arc::clone(hub));
        }
        super::overrides::apply(worker, &self.storage, settings.namespace.as_deref())
    }
}

/// Drain every lane's worker at once, returning when the last has stopped.
///
/// Concurrently, not in turn: each push lane's drain runs to twice its
/// `FLEXIQ_PUSH_TARGET_DRAIN`, and the grace period a deployment gives the
/// process is sized for one drain, not one per target.
fn shutdown_all(workers: Vec<WorkerHandle>) {
    std::thread::scope(|scope| {
        for worker in workers {
            scope.spawn(move || {
                if let Err(error) = worker.shutdown() {
                    log::warn!("scheduler shutdown reported an error: {error}");
                }
            });
        }
    });
}
