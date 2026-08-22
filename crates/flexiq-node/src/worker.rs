//! Worker wiring — scheduler, dispatcher, result-drain, and worker lifecycle
//! (registration + unregistration) so the worker shows up in the dashboard.
//! Heartbeats are driven from JS via [`crate::queue::JsQueue::worker_heartbeat`]
//! so they can carry resource health without a second writer flapping the column.

use std::sync::Arc;

use flexiq_core::storage::records::WorkerRegistration;
use flexiq_core::worker::{registry_fingerprint, WorkerDispatcher};
use flexiq_core::{Scheduler, SchedulerConfig, Storage, StorageBackend};
use napi::bindgen_prelude::{spawn, spawn_blocking, within_runtime_if_available, Result};
use napi::threadsafe_function::ThreadsafeFunctionCallMode;
use napi_derive::napi;
use tokio::sync::Notify;

use crate::config::WorkerOptions;
use crate::convert::outcome_to_js;
use crate::dispatcher::{NodeDispatcher, TaskCallback};
#[cfg(feature = "mesh")]
use crate::error::invalid_arg;
use crate::queue::OutcomeCallback;

const DEFAULT_QUEUE: &str = "default";
const DEFAULT_CHANNEL_CAPACITY: usize = 128;

/// Handle to a running worker. Hold it for the worker's lifetime; call
/// [`JsWorker::stop`] to shut it down.
#[napi]
pub struct JsWorker {
    worker_id: String,
    shutdown: Arc<Notify>,
    lifecycle_stop: Arc<Notify>,
    #[cfg(feature = "mesh")]
    mesh_shutdown: Arc<Notify>,
}

#[napi]
impl JsWorker {
    /// The id this worker registered (and claims executions) under. The JS
    /// shell heartbeats with it via [`crate::queue::JsQueue::worker_heartbeat`].
    #[napi(getter)]
    pub fn id(&self) -> String {
        self.worker_id.clone()
    }

    /// Stop the worker: the scheduler stops dispatching, the lifecycle task
    /// unregisters, mesh gossip/steal tasks shut down, and the background
    /// tasks exit once in-flight results drain.
    #[napi]
    pub fn stop(&self) {
        // `notify_one` stores a permit if no waiter is parked yet, so the signal
        // is never lost between loop iterations.
        self.shutdown.notify_one();
        self.lifecycle_stop.notify_one();
        #[cfg(feature = "mesh")]
        self.mesh_shutdown.notify_one();
    }
}

/// Start a worker over `storage` that runs `callback` for each dequeued job.
/// Fails fast if any task/queue config is invalid (e.g. a malformed rate limit).
pub fn start_worker(
    storage: StorageBackend,
    namespace: Option<String>,
    options: WorkerOptions,
    callback: TaskCallback,
    outcome_callback: OutcomeCallback,
) -> Result<JsWorker> {
    let queues = options
        .queues
        .unwrap_or_else(|| vec![DEFAULT_QUEUE.to_string()]);
    // Validate the mesh port up front: the steal server binds `port + 1`, and
    // both narrow to u16, so anything outside 1..=65534 wraps to a wrong port.
    #[cfg(feature = "mesh")]
    if let Some(mesh_cfg) = options.mesh.as_ref() {
        if mesh_cfg.port < 1 || mesh_cfg.port > 65534 {
            return Err(invalid_arg(format!(
                "mesh port must be in 1..=65534 (got {})",
                mesh_cfg.port
            )));
        }
    }
    #[cfg(feature = "mesh")]
    let mesh_shutdown = Arc::new(Notify::new());
    // Clamp to >= 1: a zero-capacity Tokio/crossbeam channel panics on creation.
    let capacity = options
        .channel_capacity
        .map(|c| (c as usize).max(1))
        .unwrap_or(DEFAULT_CHANNEL_CAPACITY);

    // Jobs this worker runs at once. Separate from `channel_capacity`, which
    // buffers hand-offs and advertises the worker's capacity — matching Java,
    // where the two are distinct options.
    let concurrency = options.concurrency.map(|c| (c as usize).max(1));

    let mut config = SchedulerConfig::default();
    if let Some(batch) = options.batch_size {
        config.batch_size = batch.max(1) as usize;
    }
    // Present (even empty) → an explicit config: an empty one disables retention.
    // Absent → leave `None`, so the core applies the recommended defaults.
    if let Some(retention) = &options.retention {
        config.retention = Some(retention.to_config());
    }
    // Bound in-flight work to what this worker will actually run, so it never
    // claims more and strands the surplus Running while peers sharing the
    // database skip it. Left unbounded when unset, so an existing worker does
    // not silently acquire a cap it never asked for.
    config.max_in_flight = concurrency;

    // The dispatcher reads cancel flags, and the lifecycle loop registers/heartbeats
    // — both need their own storage handle before `storage` moves into the scheduler.
    let dispatcher_storage = storage.clone();
    let dispatcher_namespace = namespace.clone();
    let lifecycle_storage = storage.clone();
    let queues_csv = queues.join(",");
    let worker_id = format!("node-{}", uuid::Uuid::now_v7());
    // Mesh gossip advertises the served queues; capture them before `queues`
    // moves into the scheduler.
    #[cfg(feature = "mesh")]
    let mesh_queues = queues.clone();

    // Per-task/queue config must be registered before the scheduler is shared
    // (register_* take &mut self).
    let mut scheduler = Scheduler::new(storage, queues, config, namespace);
    // Claim execution under this worker's id so dead-worker recovery can
    // attribute orphaned jobs (matches the register_worker id below).
    scheduler.set_claim_owner(worker_id.clone());
    for input in options.task_configs.iter().flatten() {
        scheduler.register_task(input.name.clone(), crate::convert::task_config(input)?);
    }
    for input in options.queue_configs.iter().flatten() {
        if let Some(codel) = crate::convert::queue_codel(input) {
            scheduler.register_queue_codel(input.name.clone(), codel);
        }
        if input.dispatch_order.as_deref() == Some("lifo") {
            scheduler.register_queue_dispatch_order(
                input.name.clone(),
                flexiq_core::storage::DispatchOrder::Lifo,
            );
        }
        scheduler.register_queue_config(input.name.clone(), crate::convert::queue_config(input)?);
    }
    let scheduler = Arc::new(scheduler);
    // Push-dispatch: swap polling for enqueue-driven wakeups before any loop
    // below takes the source. We are on the JS thread here, so enter the napi
    // runtime — the Postgres and Redis wake sources spawn a listener task.
    // Without the `push-dispatch` build feature this logs and keeps polling.
    if options.push_dispatch.unwrap_or(false) {
        within_runtime_if_available(|| scheduler.enable_push_dispatch());
    }
    let shutdown = scheduler.shutdown_handle();

    let (job_tx, job_rx) = tokio::sync::mpsc::channel(capacity);
    let (result_tx, result_rx) = crossbeam_channel::bounded(capacity);

    // Worker lifecycle: register (advertising resource names), unregister on
    // stop. Heartbeats come from JS so they carry current resource health.
    let lifecycle_stop = Arc::new(Notify::new());
    spawn_worker_lifecycle(
        lifecycle_storage,
        worker_id.clone(),
        queues_csv,
        resources_json(options.resources.as_deref()),
        capacity,
        // The handler names JS registered. Fingerprinted rather than stored, so
        // the registry row stays one comparable value however many tasks a
        // worker serves.
        registry_fingerprint(options.tasks.iter().flatten()),
        lifecycle_stop.clone(),
    );

    // Scheduler loop: poll storage, dispatch ready jobs onto `job_tx`. With the
    // mesh feature built and a mesh config supplied, route the scheduler's output
    // through the mesh bridge (affinity-sorted local deque + work-stealing); the
    // DB stays the source of truth, so the plain path is otherwise identical.
    #[cfg(feature = "mesh")]
    {
        if let Some(mesh_cfg) = options.mesh.as_ref() {
            let mesh_node = Arc::new(flexiq_mesh::MeshNode::new(
                worker_id.clone(),
                build_mesh_config(mesh_cfg),
            ));
            // Both spawns call `tokio::spawn` internally, and we are on the JS
            // thread, which carries no ambient runtime context — enter the napi
            // runtime first or the spawn panics and aborts the process.
            let (gossip, steal_server) = within_runtime_if_available(|| {
                (
                    mesh_node.spawn_gossip(mesh_queues, capacity as u16),
                    mesh_node.spawn_steal_server(),
                )
            });
            let bridge_scheduler = scheduler.clone();
            let bridge_node = mesh_node.clone();
            spawn(async move {
                run_mesh_bridge(bridge_scheduler, bridge_node, job_tx).await;
            });
            // On stop, signal the mesh node so gossip + steal-server tasks exit
            // instead of leaking for the process lifetime.
            let mesh_stop = mesh_shutdown.clone();
            let stop_node = mesh_node.clone();
            spawn(async move {
                mesh_stop.notified().await;
                stop_node.request_shutdown();
            });
            // Keep the gossip + steal-server tasks alive for the worker's lifetime.
            spawn(async move {
                let _ = tokio::join!(gossip, steal_server);
            });
        } else {
            let scheduler_run = scheduler.clone();
            spawn(async move {
                scheduler_run.run(job_tx).await;
            });
        }
    }
    #[cfg(not(feature = "mesh"))]
    {
        let scheduler_run = scheduler.clone();
        spawn(async move {
            scheduler_run.run(job_tx).await;
        });
    }

    // Dispatcher loop: execute each job in JS, report results on `result_tx`.
    let dispatcher = NodeDispatcher::new(
        callback,
        dispatcher_storage,
        dispatcher_namespace,
        concurrency,
    );
    spawn(async move {
        dispatcher.run(job_rx, result_tx).await;
    });

    // Result-drain loop: apply outcomes to storage. crossbeam `recv` is
    // blocking, so it runs on a blocking thread; it exits when every result
    // sender has dropped (i.e. after the dispatcher and all in-flight jobs end).
    let scheduler_results = scheduler;
    spawn_blocking(move || {
        while let Ok(first) = result_rx.recv() {
            // Finalize everything already queued in one batched transaction
            // rather than one per wake.
            //
            // Capped explicitly: the channel bounds how many results can sit in
            // it at once, not how many this loop drains — senders refill slots
            // while `try_recv` runs, so an unbounded drain could swallow a whole
            // backlog into one Vec and stall finalization behind it.
            let mut batch = vec![first];
            while batch.len() < capacity {
                match result_rx.try_recv() {
                    Ok(more) => batch.push(more),
                    Err(_) => break,
                }
            }

            // A panicking batch must not kill the drain loop — a dead loop
            // silently drops every later outcome. The batch widens a panic's
            // reach from one outcome to N, which is acceptable: the realistic
            // failure is a batch write error, and `handle_results` already
            // falls back to finalizing those jobs one at a time.
            let handled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                scheduler_results.handle_results(batch)
            }));
            match handled {
                // Surface each outcome to JS so the shell can emit events and run
                // middleware (the events layer). `handle_results` returns one
                // outcome per input, in order, so each job is reported once.
                Ok(outcomes) => {
                    for outcome in outcomes {
                        match outcome {
                            Ok(outcome) => {
                                // A superseded result maps to nothing: the job
                                // is proceeding under another owner and this
                                // attempt wrote no state to report.
                                if let Some(js) = outcome_to_js(&outcome) {
                                    outcome_callback
                                        .call(js, ThreadsafeFunctionCallMode::NonBlocking);
                                }
                            }
                            Err(err) => {
                                log::error!("[flexiq-node] result handling error: {err}")
                            }
                        }
                    }
                }
                Err(_) => log::error!("[flexiq-node] result handling panicked; outcomes dropped"),
            }
        }
    });

    Ok(JsWorker {
        worker_id,
        shutdown,
        lifecycle_stop,
        #[cfg(feature = "mesh")]
        mesh_shutdown,
    })
}

/// Serialize advertised resource names to the cross-SDK wire shape: a sorted
/// JSON array, or `None` when the worker registered no resources.
fn resources_json(resources: Option<&[String]>) -> Option<String> {
    let names = resources.filter(|names| !names.is_empty())?;
    let mut sorted = names.to_vec();
    sorted.sort();
    serde_json::to_string(&sorted).ok()
}

/// Register this worker, then unregister once `stop` is signalled.
#[allow(clippy::too_many_arguments)]
fn spawn_worker_lifecycle(
    storage: StorageBackend,
    worker_id: String,
    queues_csv: String,
    resources: Option<String>,
    capacity: usize,
    registry_fingerprint: Option<String>,
    stop: Arc<Notify>,
) {
    let hostname = gethostname::gethostname().to_string_lossy().to_string();
    let pid = std::process::id() as i32;
    // Lifecycle calls are blocking storage I/O → run each on the blocking
    // pool, off the JS thread and the shared async runtime. Failures are
    // logged: a silent one leaves the worker invisible in the dashboard.
    spawn(async move {
        let reg_storage = storage.clone();
        let reg_id = worker_id.clone();
        match spawn_blocking(move || {
            reg_storage.register_worker(
                &WorkerRegistration::new(&reg_id, &queues_csv, capacity as i32)
                    .resources(resources.as_deref())
                    .hostname(Some(&hostname))
                    .pid(Some(pid))
                    .pool_type(Some("node"))
                    // The addon's version, which the package is published from.
                    .sdk(Some("node"), Some(env!("CARGO_PKG_VERSION")))
                    .registry_fingerprint(registry_fingerprint.as_deref()),
            )
        })
        .await
        {
            Ok(Err(err)) => log::warn!("[flexiq-node] worker registration failed: {err}"),
            Err(err) => log::warn!("[flexiq-node] worker registration task failed: {err}"),
            Ok(Ok(_)) => {}
        }

        stop.notified().await;

        match spawn_blocking(move || storage.unregister_worker(&worker_id)).await {
            Ok(Err(err)) => log::warn!("[flexiq-node] worker unregister failed: {err}"),
            Err(err) => log::warn!("[flexiq-node] worker unregister task failed: {err}"),
            Ok(Ok(_)) => {}
        }
    });
}

/// Mesh-aware scheduler bridge: the scheduler emits jobs into an intermediate
/// channel, which this loop pushes into the mesh local deque (affinity-sorted),
/// then drains the deque to the dispatcher channel and steals from peers when idle.
#[cfg(feature = "mesh")]
async fn run_mesh_bridge(
    scheduler: Arc<Scheduler>,
    mesh_node: Arc<flexiq_mesh::MeshNode>,
    job_tx: tokio::sync::mpsc::Sender<flexiq_core::job::Job>,
) {
    let (mesh_tx, mut mesh_rx) = tokio::sync::mpsc::channel::<flexiq_core::job::Job>(64);

    let sched = scheduler.clone();
    let sched_task = spawn(async move {
        sched.run(mesh_tx).await;
    });

    loop {
        // Drain the local deque to the dispatcher first.
        while let Some(job) = mesh_node.pop_local() {
            if job_tx.send(job).await.is_err() {
                let _ = sched_task.await;
                return;
            }
        }

        // Steal from busier peers when the deque is low and stealing is enabled.
        if mesh_node.should_steal() {
            mesh_node.try_steal().await;
            while let Some(job) = mesh_node.pop_local() {
                if job_tx.send(job).await.is_err() {
                    let _ = sched_task.await;
                    return;
                }
            }
        }

        // Wait for the scheduler to produce more jobs, batching what's ready.
        match mesh_rx.recv().await {
            Some(job) => {
                let mut batch = vec![job];
                while let Ok(extra) = mesh_rx.try_recv() {
                    batch.push(extra);
                }
                mesh_node.prefetch(batch);
            }
            None => break,
        }
    }

    // Scheduler stopped — drain whatever remains in the deque.
    while let Some(job) = mesh_node.pop_local() {
        if job_tx.send(job).await.is_err() {
            break;
        }
    }
    let _ = sched_task.await;
}

/// Translate the JS-facing [`crate::config::MeshWorkerConfig`] into the core
/// mesh config, leaving unspecified fields at their crate defaults.
#[cfg(feature = "mesh")]
#[allow(clippy::field_reassign_with_default)]
fn build_mesh_config(input: &crate::config::MeshWorkerConfig) -> flexiq_mesh::MeshConfig {
    let mut config = flexiq_mesh::MeshConfig::default();
    config.gossip_port = input.port as u16;
    config.steal_port = input.port.saturating_add(1) as u16;
    if let Some(ref addr) = input.bind_addr {
        config.bind_addr = addr.clone();
    }
    if let Some(ref seeds) = input.seeds {
        config.seeds = seeds.clone();
    }
    if let Some(enabled) = input.steal {
        config.enable_stealing = enabled;
    }
    if let Some(weight) = input.affinity_weight {
        config.affinity_weight = weight;
    }
    if let Some(buffer) = input.local_buffer {
        config.local_buffer_capacity = buffer as usize;
    }
    if let Some(batch) = input.steal_batch {
        config.max_steal_batch = batch as usize;
    }
    if let Some(threshold) = input.steal_threshold {
        config.steal_threshold = threshold as usize;
    }
    if let Some(vnodes) = input.virtual_nodes {
        config.virtual_nodes = vnodes as usize;
    }
    if let Some(ref addr) = input.advertise_addr {
        config.advertise_addr = Some(addr.clone());
    }
    if let Some(ref key) = input.encryption_key {
        config.encryption_key = Some(key.clone());
    }
    if let Some(rate) = input.steal_rate_limit {
        config.steal_rate_limit = rate;
    }
    config
}
