//! Worker wiring and the `NativeWorker` / `NativeQueue.runWorker` entry points.
//!
//! Mirrors the Node shell's `start_worker`: a scheduler loop feeds jobs to the
//! [`JavaDispatcher`], a result-drain loop applies outcomes and surfaces them to
//! Java for events/middleware, and a lifecycle loop registers + heartbeats the
//! worker. The completion registry (see [`crate::dispatcher`]) lets Java report
//! each job's outcome back.

use std::sync::Arc;
use std::time::Duration;

use flexiq_core::resilience::circuit_breaker::CircuitBreakerConfig;
use flexiq_core::resilience::rate_limiter::RateLimitConfig;
use flexiq_core::resilience::retry::RetryPolicy;
use flexiq_core::scheduler::codel::CodelConfig;
use flexiq_core::scheduler::shed::OnExcess;
use flexiq_core::scheduler::{ResultOutcome, TaskConfig};
use flexiq_core::worker::WorkerDispatcher;
use flexiq_core::{Scheduler, SchedulerConfig, Storage, StorageBackend};
use jni::objects::{GlobalRef, JByteArray, JClass, JObject, JString, JValue};
use jni::sys::{jboolean, jlong, JNI_FALSE};
use jni::JNIEnv;
use tokio::sync::Notify;

use flexiq_core::job::now_millis;
use flexiq_core::storage::records::{NewSubscription, WorkerRegistration};

use crate::backend::QueueHandle;
use crate::convert::{
    parse_json, QueueConfigSpec, SubscriptionSpec, TaskRetryConfig, WorkerOptions,
};
use crate::dispatcher::{JavaDispatcher, Registry, TaskOutcome};
use crate::ffi::{guard, read_bytes, read_string};
use crate::handle::{self, drop_handle, into_handle};
use crate::jvm;

const DEFAULT_QUEUE: &str = "default";
const DEFAULT_CHANNEL_CAPACITY: usize = 128;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// A running worker. The tokio runtime and the completion registry are held here
/// for the worker's lifetime; the registry's pointer (this handle) stays valid
/// until [`close`] runs. Fields drop in declaration order: the runtime stops
/// first (joining tasks), then the registry's last reference is released.
pub struct WorkerHandle {
    _runtime: tokio::runtime::Runtime,
    registry: Arc<Registry>,
    shutdown: Arc<Notify>,
    heartbeat_stop: Arc<Notify>,
    /// The mesh node, when mesh scheduling is enabled. Held so `stop` can signal
    /// its gossip + steal-server tasks and `meshClusterInfo` can read its state.
    #[cfg(feature = "mesh")]
    mesh: Option<Arc<flexiq_mesh::MeshNode>>,
}

/// Build and start a worker over `storage`, calling back into the Java
/// `WorkerBridge` (`callbacks`) for each job.
fn start_worker(
    storage: StorageBackend,
    namespace: Option<String>,
    mut options: WorkerOptions,
    callbacks: GlobalRef,
) -> Result<WorkerHandle, crate::error::BindingError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| crate::error::BindingError::new(format!("failed to start runtime: {e}")))?;

    let queues = options
        .queues
        .unwrap_or_else(|| vec![DEFAULT_QUEUE.to_string()]);
    // Mesh gossip advertises the served queues; capture before `queues` moves.
    #[cfg(feature = "mesh")]
    let mesh_queues = queues.clone();
    let capacity = options
        .channel_capacity
        .map(|c| (c as usize).max(1))
        .unwrap_or(DEFAULT_CHANNEL_CAPACITY);
    let mut config = SchedulerConfig::default();
    if let Some(batch) = options.batch_size {
        config.batch_size = batch.max(1) as usize;
    }
    // Present (even empty) → an explicit config: an empty one disables retention.
    // Absent → leave `None`, so the core applies the recommended defaults.
    if let Some(retention) = &options.retention {
        config.retention = Some(retention.to_config());
    }
    // Bound in-flight work to the worker's execution parallelism so a
    // drain-until-empty poll can't claim more than the pool runs and starve
    // peer workers sharing the database.
    if let Some(concurrency) = options.concurrency {
        config.max_in_flight = Some((concurrency.max(1)) as usize);
    }

    let registry = Arc::new(Registry::default());
    let dispatcher_storage = storage.clone();
    let dispatcher_namespace = namespace.clone();
    let lifecycle_storage = storage.clone();
    let queues_csv = queues.join(",");
    let worker_id = format!("java-{}", uuid::Uuid::now_v7());
    // The mesh node id is this worker's id; capture before `worker_id` moves.
    #[cfg(feature = "mesh")]
    let mesh_worker_id = worker_id.clone();

    // Validate every task policy before writing any persistent state: this is
    // the last fallible step that has no side effects, and returning Err past
    // the writes below would leave a worker row and its subscriptions behind
    // with no handle to run the lifecycle loop that cleans them up.
    let task_policies = build_task_policies(options.task_configs)?;

    // Create the live worker row before its ephemeral subscriptions exist:
    // the reaper only spares owned rows whose owner is registered, so this
    // ordering (plus the core's registration grace window) keeps a concurrent
    // reap from racing the start. Then write topic subscriptions before the
    // scheduler starts, so its first poll can already see deliveries. Either
    // failure aborts the start — a silently missing subscription would drop
    // deliveries.
    register_live_worker(&lifecycle_storage, &worker_id, &queues_csv, capacity)?;
    register_subscriptions(&storage, &worker_id, options.subscriptions.take())?;

    let mut scheduler = Scheduler::new(storage, queues, config, namespace);
    // Claim execution under this worker's id so dead-worker recovery can
    // attribute orphaned jobs (matches the worker id registered above).
    scheduler.set_claim_owner(worker_id.clone());
    for (name, policy) in task_policies {
        scheduler.register_task(name, policy);
    }
    for spec in options.queue_configs.take().unwrap_or_default() {
        if let Some(codel) = queue_codel_from_spec(&spec) {
            scheduler.register_queue_codel(spec.name.clone(), codel);
        }
        if spec.dispatch_order.as_deref() == Some("lifo") {
            scheduler.register_queue_dispatch_order(
                spec.name,
                flexiq_core::storage::DispatchOrder::Lifo,
            );
        }
    }
    let scheduler = Arc::new(scheduler);
    // Push-dispatch: swap polling for enqueue-driven wakeups before any loop
    // below takes the source. We are on the JNI thread here, so enter the
    // runtime — the Postgres and Redis wake sources spawn a listener task.
    // Without the `push-dispatch` build feature this logs and keeps polling.
    if options.push_dispatch.unwrap_or(false) {
        let _guard = runtime.enter();
        scheduler.enable_push_dispatch();
    }
    let shutdown = scheduler.shutdown_handle();
    let heartbeat_stop = Arc::new(Notify::new());

    let (job_tx, job_rx) = tokio::sync::mpsc::channel(capacity);
    // Unbounded so the dispatcher's `result_tx.send` (which runs inside a Tokio
    // task) never blocks a runtime worker when the drain loop is momentarily
    // slow. In-flight jobs are already bounded by the job channel + scheduler,
    // so the result queue can't grow without bound.
    let (result_tx, result_rx) = crossbeam_channel::unbounded();

    // Scheduler loop: poll storage and dispatch ready jobs. With mesh enabled and
    // a config supplied, route the scheduler's output through the mesh bridge
    // (affinity-sorted local deque + work-stealing) instead of straight to the
    // dispatcher; the DB stays the source of truth, so the plain path is identical.
    let scheduler_run = scheduler.clone();
    #[cfg(feature = "mesh")]
    let mesh = match options.mesh_config.as_deref() {
        Some(config_json) => Some(crate::mesh::spawn_mesh(
            &runtime,
            scheduler_run,
            job_tx,
            config_json,
            mesh_worker_id,
            mesh_queues,
            capacity.min(u16::MAX as usize) as u16,
        )),
        None => {
            runtime.spawn(async move {
                scheduler_run.run(job_tx).await;
            });
            None
        }
    };
    #[cfg(not(feature = "mesh"))]
    runtime.spawn(async move {
        scheduler_run.run(job_tx).await;
    });

    // Dispatcher loop: submit each job to Java, report results.
    let dispatcher = JavaDispatcher::new(
        callbacks.clone(),
        registry.clone(),
        dispatcher_storage,
        dispatcher_namespace,
    );
    runtime.spawn(async move {
        dispatcher.run(job_rx, result_tx).await;
    });

    // Result-drain loop: apply outcomes and surface them to Java. crossbeam
    // `recv` is blocking, so it runs on a blocking thread; it exits when the
    // result sender has dropped (dispatcher done).
    let drain_scheduler = scheduler;
    let drain_callbacks = callbacks;
    runtime.spawn_blocking(move || {
        drain_results(result_rx, drain_scheduler, drain_callbacks, capacity)
    });

    // Lifecycle loop: heartbeat until stopped, then unregister.
    spawn_lifecycle(
        &runtime,
        lifecycle_storage,
        worker_id,
        heartbeat_stop.clone(),
    );

    Ok(WorkerHandle {
        _runtime: runtime,
        registry,
        shutdown,
        heartbeat_stop,
        #[cfg(feature = "mesh")]
        mesh,
    })
}

/// Create this worker's registry row. Runs before the worker's ephemeral
/// subscriptions are written, and a failure aborts the start: those rows bind
/// to this id, so a missing registry row would leave them reap-able as soon
/// as the registration grace window lapses.
fn register_live_worker(
    storage: &StorageBackend,
    worker_id: &str,
    queues_csv: &str,
    capacity: usize,
) -> Result<(), crate::error::BindingError> {
    let hostname = gethostname::gethostname().to_string_lossy().to_string();
    let pid = std::process::id() as i32;
    storage.register_worker(&WorkerRegistration {
        worker_id,
        queues: queues_csv,
        threads: capacity as i32,
        hostname: Some(&hostname),
        pid: Some(pid),
        pool_type: Some("java"),
        sdk: Some("java"),
        // The native library's version, which the jar is published alongside.
        sdk_version: Some(env!("CARGO_PKG_VERSION")),
        ..Default::default()
    })?;
    Ok(())
}

/// Register the worker's declared topic subscriptions. Durable rows carry no
/// owner; ephemeral rows bind to this worker's id and are reaped once the
/// worker leaves the registry. `active` is an insert default — the core upsert
/// preserves an existing row's paused state across a restart.
fn register_subscriptions(
    storage: &StorageBackend,
    worker_id: &str,
    specs: Option<Vec<SubscriptionSpec>>,
) -> Result<(), crate::error::BindingError> {
    let Some(specs) = specs else {
        return Ok(());
    };
    let created_at = now_millis();
    for spec in &specs {
        let row = NewSubscription {
            topic: spec.topic.clone(),
            subscription_name: spec.subscription_name.clone(),
            task_name: spec.task_name.clone(),
            queue: spec.queue.clone(),
            active: true,
            durable: spec.durable,
            owner_worker_id: (!spec.durable).then(|| worker_id.to_string()),
            created_at,
            priority: spec.priority,
            max_retries: spec.max_retries,
            timeout_ms: spec.timeout_ms,
            // Fan-out by default; the log-mode param is threaded in a later step.
            mode: flexiq_core::storage::records::SubscriptionMode::Fanout,
        };
        storage.register_subscription(&row)?;
    }
    Ok(())
}

/// Build each task's policy — retry curve, throttling, concurrency caps — from
/// its wire config. Unset fields keep the core's defaults; the per-job retry
/// budget still resolves from the job's `max_retries`.
///
/// Fails on a malformed rate spec rather than registering the task without it:
/// silently dropping a throttle the caller asked for is worse than not starting.
/// Pure, so the caller can validate before writing any persistent worker state
/// and a rejected config leaves nothing behind.
fn build_task_policies(
    configs: Option<Vec<TaskRetryConfig>>,
) -> Result<Vec<(String, TaskConfig)>, crate::error::BindingError> {
    let Some(configs) = configs else {
        return Ok(Vec::new());
    };
    let mut built = Vec::with_capacity(configs.len());
    for config in configs {
        let mut retry_policy = RetryPolicy::default();
        if let Some(custom) = config.custom_delays_ms {
            // Explicit per-attempt delays are honored exactly. The core derives
            // its jitter and post-exhaustion fallback from base_delay_ms, so
            // zeroing it leaves the listed delays jitter-free; once the list is
            // spent, further retries fire immediately (callers list enough).
            retry_policy.base_delay_ms = 0;
            retry_policy.max_delay_ms = 0;
            retry_policy.custom_delays_ms = Some(custom);
        } else {
            if let Some(base) = config.base_delay_ms {
                retry_policy.base_delay_ms = base;
            }
            if let Some(max) = config.max_delay_ms {
                retry_policy.max_delay_ms = max;
            }
        }
        // A present threshold enables the breaker; window/cooldown already arrive in ms
        // from the SDK, so they pass through unscaled (defaults mirror the core's).
        let circuit_breaker =
            config
                .circuit_breaker_threshold
                .map(|threshold| CircuitBreakerConfig {
                    threshold,
                    window_ms: config.circuit_breaker_window_ms.unwrap_or(60_000),
                    cooldown_ms: config.circuit_breaker_cooldown_ms.unwrap_or(300_000),
                    half_open_max_probes: config.circuit_breaker_half_open_probes.unwrap_or(5),
                    half_open_success_rate: config
                        .circuit_breaker_half_open_success_rate
                        .unwrap_or(0.8),
                });
        let rate_limit = parse_rate_spec("rateLimit", &config.name, config.rate_limit.as_deref())?;
        let retry_budget =
            parse_rate_spec("retryBudget", &config.name, config.retry_budget.as_deref())?;
        let on_excess = parse_on_excess(&config.name, config.on_excess.as_deref())?;
        built.push((
            config.name,
            TaskConfig {
                retry_policy,
                rate_limit,
                on_excess,
                circuit_breaker,
                retry_budget,
                max_concurrent: config.max_concurrent,
                max_in_flight_per_task: config.max_in_flight_per_task.map(|n| n.max(1) as usize),
            },
        ));
    }
    Ok(built)
}

/// Build the per-queue CoDel configs. Only queues with positive bounds are
/// registered; anything else is dropped so an empty spec is a harmless no-op.
/// Extract a valid CoDel config from a queue spec: both bounds set and positive,
/// else `None` (the queue keeps default dispatch, no shedding).
fn queue_codel_from_spec(spec: &QueueConfigSpec) -> Option<CodelConfig> {
    match (spec.codel_target_ms, spec.codel_interval_ms) {
        (Some(target_ms), Some(interval_ms)) if target_ms > 0 && interval_ms > 0 => {
            Some(CodelConfig {
                target_ms,
                interval_ms,
            })
        }
        _ => None,
    }
}

/// Parse an optional rate spec, naming the offending task and option so a typo
/// is actionable. Several options share this `"100/m"` grammar.
fn parse_rate_spec(
    field: &str,
    task: &str,
    spec: Option<&str>,
) -> Result<Option<RateLimitConfig>, crate::error::BindingError> {
    match spec {
        Some(s) => RateLimitConfig::parse(s).map(Some).ok_or_else(|| {
            crate::error::BindingError::new(format!(
                "invalid {field} '{s}' on task '{task}' (expected e.g. '100/m')"
            ))
        }),
        None => Ok(None),
    }
}

/// Parse an optional `onExcess` spec. Unset means the default, `defer`; an
/// unrecognized value is a config error rather than a silent fall back to
/// keeping the job, which a caller who asked to shed would never notice.
fn parse_on_excess(task: &str, spec: Option<&str>) -> Result<OnExcess, crate::error::BindingError> {
    match spec {
        Some(s) => OnExcess::parse(s).ok_or_else(|| {
            crate::error::BindingError::new(format!(
                "invalid onExcess '{s}' on task '{task}' (expected 'defer' or 'drop')"
            ))
        }),
        None => Ok(OnExcess::default()),
    }
}

/// Drain results: finalize each batch in one transaction, then invoke
/// `WorkerBridge.onOutcome` per job.
///
/// `max_batch` bounds one drain. Unlike the other shells, this result channel is
/// unbounded (so the dispatcher never blocks a runtime worker), so nothing else
/// would cap how much a single drain swallows.
fn drain_results(
    result_rx: crossbeam_channel::Receiver<flexiq_core::scheduler::JobResult>,
    scheduler: Arc<Scheduler>,
    callbacks: GlobalRef,
    max_batch: usize,
) {
    let vm = jvm::vm();
    let mut env = match vm.attach_current_thread() {
        Ok(env) => env,
        Err(e) => {
            log::error!("[flexiq-java] drain attach failed: {e}");
            return;
        }
    };
    while let Ok(first) = result_rx.recv() {
        // Finalize everything already queued in one transaction rather than one
        // per wake.
        let mut batch = vec![first];
        while batch.len() < max_batch {
            match result_rx.try_recv() {
                Ok(more) => batch.push(more),
                Err(_) => break,
            }
        }

        // One outcome per input, in order, so each job is reported exactly once.
        for outcome in scheduler.handle_results(batch) {
            let outcome = match outcome {
                Ok(outcome) => outcome,
                Err(e) => {
                    log::error!("[flexiq-java] result handling error: {e}");
                    continue;
                }
            };
            // Each outcome allocates several JNI locals on this long-lived attached
            // env; scope them in a frame so a busy worker can't exhaust the
            // local-reference table over the loop's lifetime. The frame stays per
            // outcome — sized for one `call_on_outcome`, it would overflow if it
            // wrapped the whole batch.
            let framed = env.with_local_frame(16, |env| {
                if let Err(e) = call_on_outcome(env, &callbacks, &outcome) {
                    log::error!("[flexiq-java] onOutcome failed: {e}");
                }
                Ok::<(), jni::errors::Error>(())
            });
            if let Err(e) = framed {
                log::error!("[flexiq-java] drain local frame failed: {e}");
            }
        }
    }
}

/// Invoke `WorkerBridge.onOutcome` for one finished job.
fn call_on_outcome(
    env: &mut JNIEnv,
    callbacks: &GlobalRef,
    outcome: &ResultOutcome,
) -> Result<(), String> {
    let (kind, job_id, task_name, error, retry_count, timed_out, wall_time_ns) = describe(outcome);
    let kind_s = env.new_string(kind).map_err(|e| e.to_string())?;
    let job_s = env.new_string(job_id).map_err(|e| e.to_string())?;
    let task_s = env.new_string(task_name).map_err(|e| e.to_string())?;
    let error_obj: JObject = match error {
        Some(e) => env.new_string(e).map_err(|x| x.to_string())?.into(),
        None => JObject::null(),
    };
    // Clear a pending exception on the `Err(JavaException)` path before
    // returning, so a thrown listener can't poison the reused env.
    if let Err(e) = env.call_method(
        callbacks,
        "onOutcome",
        "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;IZJ)V",
        &[
            JValue::Object(&kind_s),
            JValue::Object(&job_s),
            JValue::Object(&task_s),
            JValue::Object(&error_obj),
            JValue::Int(retry_count),
            JValue::Bool(u8::from(timed_out)),
            JValue::Long(wall_time_ns),
        ],
    ) {
        let _ = env.exception_clear();
        return Err(e.to_string());
    }
    if env.exception_check().unwrap_or(false) {
        let _ = env.exception_clear();
        return Err("WorkerBridge.onOutcome threw".to_string());
    }
    Ok(())
}

/// Flatten a [`ResultOutcome`] into the fields `onOutcome` receives. `retry_count`
/// is -1 when not applicable; `wall_time_ns` is 0 when the run wasn't measured.
fn describe(outcome: &ResultOutcome) -> (&str, &str, &str, Option<&str>, i32, bool, i64) {
    match outcome {
        ResultOutcome::Success {
            job_id,
            task_name,
            wall_time_ns,
        } => ("success", job_id, task_name, None, -1, false, *wall_time_ns),
        ResultOutcome::Retry {
            job_id,
            task_name,
            error,
            retry_count,
            timed_out,
            wall_time_ns,
            ..
        } => (
            "retry",
            job_id,
            task_name,
            Some(error),
            *retry_count,
            *timed_out,
            *wall_time_ns,
        ),
        ResultOutcome::DeadLettered {
            job_id,
            task_name,
            error,
            timed_out,
            wall_time_ns,
            ..
        } => (
            "dead",
            job_id,
            task_name,
            Some(error),
            -1,
            *timed_out,
            *wall_time_ns,
        ),
        ResultOutcome::Cancelled {
            job_id,
            task_name,
            wall_time_ns,
            ..
        } => (
            "cancelled",
            job_id,
            task_name,
            None,
            -1,
            false,
            *wall_time_ns,
        ),
    }
}

/// Heartbeat the already-registered worker every 5s until stopped, then
/// unregister it. Registration itself happens synchronously at start (see
/// [`register_live_worker`]) so ephemeral subscriptions never precede it.
fn spawn_lifecycle(
    runtime: &tokio::runtime::Runtime,
    storage: StorageBackend,
    worker_id: String,
    stop: Arc<Notify>,
) {
    runtime.spawn(async move {
        loop {
            tokio::select! {
                _ = stop.notified() => break,
                _ = tokio::time::sleep(HEARTBEAT_INTERVAL) => {
                    if let Err(e) = storage.heartbeat(&worker_id, None) {
                        log::warn!("[flexiq-java] worker heartbeat failed: {e}");
                    }
                    // Elect a single reaper: without this, every worker's 5s
                    // sweep scans the whole registry, O(N) per cluster. Dead-
                    // worker reap first so a WORKER_OFFLINE peer sees the pruned
                    // registry; the ephemeral sweep re-checks the same election.
                    flexiq_core::storage::reap_dead_workers_if_leader(&storage, &worker_id);
                    if let Err(e) = flexiq_core::storage::sweep_ephemeral_subscriptions(
                        &storage,
                        Some(&worker_id),
                    ) {
                        log::warn!("[flexiq-java] ephemeral subscription reap failed: {e}");
                    }
                }
            }
        }
        if let Err(e) = storage.unregister_worker(&worker_id) {
            log::warn!("[flexiq-java] worker unregister failed: {e}");
        }
        // This worker just left the registry, so its own ephemeral subscriptions
        // are now dead-owned; reap immediately instead of waiting for a peer.
        if let Err(e) = crate::backend::reap_ephemeral_subscriptions(&storage) {
            log::warn!("[flexiq-java] ephemeral subscription reap failed: {e}");
        }
    });
}

/// Borrow a worker handle.
///
/// # Safety
/// `handle` must be a live `WorkerHandle` pointer from [`start_worker`].
unsafe fn borrow_worker<'a>(handle: jlong) -> &'a WorkerHandle {
    handle::borrow::<WorkerHandle>(handle)
}

/// `long runWorker(long queueHandle, Object bridge, String optionsJson)` — start
/// a worker; returns its handle. `bridge` is a Java `WorkerBridge`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_runWorker<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    queue_handle: jlong,
    bridge: JObject<'local>,
    options_json: JString<'local>,
) -> jlong {
    guard(&mut env, 0, |env| {
        let queue = unsafe { handle::borrow::<QueueHandle>(queue_handle) };
        let raw = read_string(env, &options_json)?;
        let options: WorkerOptions = parse_json(&raw, "worker options")?;
        let callbacks = env
            .new_global_ref(&bridge)
            .map_err(|e| crate::error::BindingError::new(format!("global ref failed: {e}")))?;
        let worker = start_worker(
            queue.storage.clone(),
            queue.namespace.clone(),
            options,
            callbacks,
        )?;
        Ok(into_handle(worker))
    })
}

/// `void completeJob(long workerHandle, long token, byte[] result)`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeWorker_completeJob<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    token: jlong,
    result: JByteArray<'local>,
) {
    guard(&mut env, (), |env| {
        let worker = unsafe { borrow_worker(handle) };
        let bytes = read_bytes(env, &result)?;
        worker
            .registry
            .complete(token as u64, TaskOutcome::Success(bytes));
        Ok(())
    })
}

/// `void failJob(long workerHandle, long token, String error, boolean retryable)`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeWorker_failJob<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    token: jlong,
    error: JString<'local>,
    retryable: jboolean,
) {
    guard(&mut env, (), |env| {
        let worker = unsafe { borrow_worker(handle) };
        let message = read_string(env, &error)?;
        worker.registry.complete(
            token as u64,
            TaskOutcome::Failure(message, retryable != JNI_FALSE),
        );
        Ok(())
    })
}

/// `void cancelJob(long workerHandle, long token)`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeWorker_cancelJob(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    token: jlong,
) {
    guard(&mut env, (), |_env| {
        let worker = unsafe { borrow_worker(handle) };
        worker
            .registry
            .complete(token as u64, TaskOutcome::Cancelled);
        Ok(())
    })
}

/// `void stop(long workerHandle)` — stop the scheduler and heartbeat loops.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeWorker_stop(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    guard(&mut env, (), |_env| {
        let worker = unsafe { borrow_worker(handle) };
        worker.shutdown.notify_one();
        worker.heartbeat_stop.notify_one();
        // Signal the mesh node so its gossip + steal-server tasks exit instead of
        // lingering until the runtime drops.
        #[cfg(feature = "mesh")]
        if let Some(mesh) = &worker.mesh {
            mesh.request_shutdown();
        }
        Ok(())
    })
}

/// `String meshClusterInfo(long workerHandle)` — a JSON `ClusterInfo` snapshot,
/// or `null` when this worker is not mesh-enabled.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeWorker_meshClusterInfo<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jni::sys::jstring {
    guard(&mut env, std::ptr::null_mut(), |_env| {
        let worker = unsafe { borrow_worker(handle) };
        #[cfg(feature = "mesh")]
        if let Some(mesh) = &worker.mesh {
            let json = serde_json::to_string(&mesh.cluster_info()).map_err(|e| {
                crate::error::BindingError::new(format!("failed to encode cluster info: {e}"))
            })?;
            return crate::ffi::new_string(_env, json);
        }
        let _ = worker;
        Ok(std::ptr::null_mut())
    })
}

/// `void close(long workerHandle)` — stop the runtime and reclaim the handle.
/// The Java side drains its handler executor before calling this, so no
/// `completeJob`/`failJob` can still borrow the handle being freed.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeWorker_close<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    // Dropping the handle stops the runtime (joining tasks); route through
    // `guard` so a panic in that teardown can't unwind across the FFI boundary.
    guard(&mut env, (), |_env| {
        if handle != 0 {
            unsafe { drop_handle::<WorkerHandle>(handle) };
        }
        Ok(())
    })
}
