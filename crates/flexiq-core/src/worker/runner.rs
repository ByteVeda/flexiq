//! Turn-key worker: wires a [`Scheduler`], a [`WorkerDispatcher`], the result
//! drain loop, and the heartbeat/reap cadence into one `Worker::spawn()` call —
//! the zero-to-executed-task path for a Rust consumer.
//!
//! Mirrors the orchestration the language bindings hand-roll: a dedicated
//! tokio runtime drives `Scheduler::run` and `WorkerDispatcher::run`, a drain
//! thread feeds `JobResult`s back into `Scheduler::handle_results`, and a
//! heartbeat thread keeps the worker registered and runs the elected reaps.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::error::Result;
use crate::scheduler::{QueueConfig, ResultOutcome, Scheduler, SchedulerConfig, TaskConfig};
use crate::storage::records::WorkerRegistration;
use crate::storage::{
    reap_dead_workers_if_leader, sweep_ephemeral_subscriptions, Storage, StorageBackend,
};

use super::dispatcher::NativeDispatcher;
use super::fingerprint::registry_fingerprint;
use super::registry::{TaskRegistry, TaskResult};
use super::WorkerDispatcher;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const DRAIN_POLL: Duration = Duration::from_millis(100);

/// Callback invoked with each processed [`ResultOutcome`] (success, retry,
/// dead-letter, cancel) — the native analogue of a binding's middleware hooks.
pub type OutcomeCallback = Arc<dyn Fn(&ResultOutcome) + Send + Sync>;

/// Builder for a running worker. Register handlers, then [`Worker::spawn`].
pub struct Worker {
    storage: StorageBackend,
    registry: TaskRegistry,
    queues: Vec<String>,
    num_workers: usize,
    namespace: Option<String>,
    scheduler_config: SchedulerConfig,
    task_configs: Vec<(String, TaskConfig)>,
    queue_configs: Vec<(String, QueueConfig)>,
    worker_id: Option<String>,
    on_outcome: Option<OutcomeCallback>,
    dispatcher: Option<(String, Arc<dyn WorkerDispatcher>)>,
}

impl Worker {
    /// A worker builder over `storage` with default settings.
    pub fn new(storage: StorageBackend) -> Self {
        Self {
            storage,
            registry: TaskRegistry::new(),
            queues: vec!["default".to_string()],
            num_workers: 4,
            namespace: None,
            scheduler_config: SchedulerConfig::default(),
            task_configs: Vec::new(),
            queue_configs: Vec::new(),
            worker_id: None,
            on_outcome: None,
            dispatcher: None,
        }
    }

    /// Queues this worker consumes (default: `["default"]`).
    pub fn queues(mut self, queues: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.queues = queues.into_iter().map(Into::into).collect();
        self
    }

    /// Maximum concurrently executing tasks (default: 4).
    pub fn num_workers(mut self, num_workers: usize) -> Self {
        self.num_workers = num_workers.max(1);
        self
    }

    /// Tenant namespace this worker is scoped to (default: none).
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Override the scheduler configuration. `max_in_flight` is capped to the
    /// worker pool size at spawn when unset.
    pub fn scheduler_config(mut self, config: SchedulerConfig) -> Self {
        self.scheduler_config = config;
        self
    }

    /// Per-task resilience policy (retry, rate limit, circuit breaker).
    pub fn task_config(mut self, task_name: impl Into<String>, config: TaskConfig) -> Self {
        self.task_configs.push((task_name.into(), config));
        self
    }

    /// Per-queue policy (rate limit, concurrency).
    pub fn queue_config(mut self, queue_name: impl Into<String>, config: QueueConfig) -> Self {
        self.queue_configs.push((queue_name.into(), config));
        self
    }

    /// Explicit worker id (default: `rust-worker-<uuid7>`).
    pub fn worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.worker_id = Some(worker_id.into());
        self
    }

    /// Observe every processed outcome (the middleware-hook analogue).
    pub fn on_outcome(mut self, callback: impl Fn(&ResultOutcome) + Send + Sync + 'static) -> Self {
        self.on_outcome = Some(Arc::new(callback));
        self
    }

    /// Run tasks on `dispatcher` instead of the built-in native pool — e.g. a
    /// [`RemoteDispatcher`](super::RemoteDispatcher) feeding attached
    /// executors. Registered handlers are then unused, and `num_workers`
    /// should match the dispatcher's own concurrency so `max_in_flight` bounds
    /// dispatch correctly.
    ///
    /// `pool_type` is what the worker registry reports, so it must describe the
    /// pool that is actually running.
    pub fn dispatcher(
        mut self,
        pool_type: impl Into<String>,
        dispatcher: Arc<dyn WorkerDispatcher>,
    ) -> Self {
        self.dispatcher = Some((pool_type.into(), dispatcher));
        self
    }

    /// Register a blocking handler. See [`TaskRegistry::register`].
    pub fn register(
        mut self,
        task_name: impl Into<String>,
        handler: impl Fn(&crate::job::Job) -> TaskResult + Send + Sync + 'static,
    ) -> Self {
        self.registry.register(task_name, handler);
        self
    }

    /// Register an async handler. See [`TaskRegistry::register_async`].
    pub fn register_async<F, Fut>(mut self, task_name: impl Into<String>, handler: F) -> Self
    where
        F: Fn(crate::job::Job) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = TaskResult> + Send + 'static,
    {
        self.registry.register_async(task_name, handler);
        self
    }

    /// Register this worker and start the scheduler, dispatcher, result-drain
    /// loop, and heartbeat. Returns a handle; call [`WorkerHandle::shutdown`]
    /// to drain and stop.
    pub fn spawn(self) -> Result<WorkerHandle> {
        let Worker {
            storage,
            registry,
            queues,
            num_workers,
            namespace,
            mut scheduler_config,
            task_configs,
            queue_configs,
            worker_id,
            on_outcome,
            dispatcher,
        } = self;

        let worker_id =
            worker_id.unwrap_or_else(|| format!("rust-worker-{}", uuid::Uuid::now_v7()));
        // Taken before the registry moves into the dispatcher. A caller that
        // supplies its own pool — the language shells, and the server's remote
        // dispatcher — keeps its handlers on its own side, so this registry is
        // empty and fingerprints as `None`. That is the honest answer: the row
        // must not claim a registry this process cannot see.
        let fingerprint = registry_fingerprint(registry.task_names());
        let (pool_type, dispatcher): (String, Arc<dyn WorkerDispatcher>) = dispatcher
            .unwrap_or_else(|| {
                (
                    "native".to_string(),
                    Arc::new(NativeDispatcher::new(registry, num_workers)),
                )
            });

        storage.register_worker(&WorkerRegistration {
            worker_id: &worker_id,
            queues: &queues.join(","),
            threads: num_workers as i32,
            pid: Some(std::process::id() as i32),
            pool_type: Some(&pool_type),
            sdk: Some("rust"),
            sdk_version: Some(env!("CARGO_PKG_VERSION")),
            registry_fingerprint: fingerprint.as_deref(),
            ..Default::default()
        })?;

        // Bound dispatch to the pool size so this scheduler never claims more
        // than its workers can run; also makes `in_flight_settled` meaningful.
        if scheduler_config.max_in_flight.is_none() {
            scheduler_config.max_in_flight = Some(num_workers);
        }

        let mut scheduler = Scheduler::new(storage.clone(), queues, scheduler_config, namespace);
        scheduler.set_claim_owner(worker_id.clone());
        for (task_name, config) in task_configs {
            scheduler.register_task(task_name, config);
        }
        for (queue_name, config) in queue_configs {
            scheduler.register_queue_config(queue_name, config);
        }
        let scheduler = Arc::new(scheduler);
        let shutdown = scheduler.shutdown_handle();

        let (job_tx, job_rx) = tokio::sync::mpsc::channel(num_workers * 2);
        let (result_tx, result_rx) = crossbeam_channel::bounded(num_workers * 2);

        let runtime_done = Arc::new(AtomicBool::new(false));

        // Runtime thread: scheduler dispatch + task execution. `result_tx`
        // moves in, so the drain side disconnects once execution is done.
        let runtime_thread = {
            let scheduler = scheduler.clone();
            let dispatcher = dispatcher.clone();
            let runtime_done = runtime_done.clone();
            thread::Builder::new()
                .name(format!("{worker_id}-runtime"))
                .spawn(move || {
                    let runtime = tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(2)
                        .enable_all()
                        .build()
                        .expect("tokio runtime construction cannot fail with these settings");
                    runtime.block_on(async move {
                        let scheduler_task = tokio::spawn({
                            let scheduler = scheduler.clone();
                            async move { scheduler.run(job_tx).await }
                        });
                        let dispatch_task =
                            tokio::spawn(async move { dispatcher.run(job_rx, result_tx).await });
                        let _ = tokio::join!(scheduler_task, dispatch_task);
                    });
                    runtime_done.store(true, Ordering::Release);
                })
                .map_err(spawn_error)?
        };

        // Drain thread: feed results back into the scheduler and surface
        // outcomes. Exits when execution has finished and every dispatched
        // job's result has been handled.
        let drain_thread = {
            let scheduler = scheduler.clone();
            let runtime_done = runtime_done.clone();
            thread::Builder::new()
                .name(format!("{worker_id}-drain"))
                .spawn(move || loop {
                    match result_rx.recv_timeout(DRAIN_POLL) {
                        Ok(first) => {
                            let mut batch = vec![first];
                            while let Ok(more) = result_rx.try_recv() {
                                batch.push(more);
                            }
                            for handled in scheduler.handle_results(batch) {
                                match handled {
                                    Ok(outcome) => {
                                        if let Some(callback) = &on_outcome {
                                            callback(&outcome);
                                        }
                                    }
                                    Err(handling_error) => {
                                        log::error!("result handling failed: {handling_error}");
                                    }
                                }
                            }
                        }
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                            if runtime_done.load(Ordering::Acquire) && scheduler.in_flight_settled()
                            {
                                break;
                            }
                        }
                        Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                    }
                })
                .map_err(spawn_error)?
        };

        // Heartbeat thread: liveness + the elected cluster reaps. The stop
        // sender doubles as the stop signal — dropping it ends the loop.
        let (stop_tx, stop_rx) = std_mpsc::channel::<()>();
        let heartbeat_thread = {
            let storage = storage.clone();
            let worker_id = worker_id.clone();
            thread::Builder::new()
                .name(format!("{worker_id}-heartbeat"))
                .spawn(move || {
                    while let Err(std_mpsc::RecvTimeoutError::Timeout) =
                        stop_rx.recv_timeout(HEARTBEAT_INTERVAL)
                    {
                        if let Err(heartbeat_error) = storage.heartbeat(&worker_id, None) {
                            log::warn!("worker heartbeat failed: {heartbeat_error}");
                        }
                        reap_dead_workers_if_leader(&storage, &worker_id);
                        if let Err(sweep_error) =
                            sweep_ephemeral_subscriptions(&storage, Some(&worker_id))
                        {
                            log::warn!("ephemeral subscription reap failed: {sweep_error}");
                        }
                    }
                })
                .map_err(spawn_error)?
        };

        Ok(WorkerHandle {
            worker_id,
            storage,
            shutdown,
            dispatcher,
            stop_tx: Some(stop_tx),
            threads: vec![runtime_thread, drain_thread, heartbeat_thread],
        })
    }
}

fn spawn_error(io_error: std::io::Error) -> crate::error::QueueError {
    crate::error::QueueError::Worker(format!("failed to spawn worker thread: {io_error}"))
}

/// Handle to a running [`Worker`]. Dropping it without calling
/// [`WorkerHandle::shutdown`] leaves the worker running detached.
pub struct WorkerHandle {
    worker_id: String,
    storage: StorageBackend,
    shutdown: Arc<tokio::sync::Notify>,
    dispatcher: Arc<dyn WorkerDispatcher>,
    stop_tx: Option<std_mpsc::Sender<()>>,
    threads: Vec<thread::JoinHandle<()>>,
}

impl WorkerHandle {
    /// Id this worker registered under.
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Stop dispatching, drain in-flight work, stop the heartbeat, and
    /// unregister the worker. Blocks until every thread has exited.
    pub fn shutdown(mut self) -> Result<()> {
        self.shutdown.notify_one();
        self.dispatcher.shutdown();
        // Dropping the stop sender ends the heartbeat loop on its next wake.
        self.stop_tx.take();
        for thread in self.threads.drain(..) {
            if thread.join().is_err() {
                log::error!("worker thread panicked during shutdown");
            }
        }
        self.storage.unregister_worker(&self.worker_id)?;
        // This worker's own ephemeral subscriptions are now dead-owned; reap
        // immediately instead of waiting for a peer's heartbeat tick.
        if let Err(sweep_error) = sweep_ephemeral_subscriptions(&self.storage, None) {
            log::warn!("shutdown subscription sweep failed: {sweep_error}");
        }
        Ok(())
    }
}
