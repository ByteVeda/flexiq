use std::sync::atomic::Ordering;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use pyo3::BoundObject;

use flexiq_core::resilience::circuit_breaker::CircuitBreakerConfig;
use flexiq_core::resilience::rate_limiter::RateLimitConfig;
use flexiq_core::resilience::retry::RetryPolicy;
use flexiq_core::scheduler::shed::OnExcess;
use flexiq_core::scheduler::{JobResult, ResultOutcome, Scheduler, SchedulerConfig, TaskConfig};
use flexiq_core::storage::records::{WorkerRegistration, WorkerStatus};
use flexiq_core::storage::Storage;
use flexiq_core::worker::registry_fingerprint;

use super::PyQueue;
#[cfg(not(feature = "native-async"))]
use crate::async_worker::AsyncWorkerPool;
use crate::py_config::PyTaskConfig;
use crate::py_worker_steps::PyWorkerSteps;

/// Parse the optional per-queue CoDel config from a `queue_configs` entry. Both
/// `codel_target_ms` and `codel_interval_ms` must be present and positive for
/// shedding to be enabled on the queue.
fn parse_codel(cfg: &serde_json::Value) -> Option<flexiq_core::scheduler::codel::CodelConfig> {
    let target_ms = cfg.get("codel_target_ms").and_then(|v| v.as_i64())?;
    let interval_ms = cfg.get("codel_interval_ms").and_then(|v| v.as_i64())?;
    if target_ms <= 0 || interval_ms <= 0 {
        return None;
    }
    Some(flexiq_core::scheduler::codel::CodelConfig {
        target_ms,
        interval_ms,
    })
}

/// Mesh-aware scheduler bridge: receives jobs from the scheduler's
/// intermediate channel, pushes them into the local deque with affinity
/// sorting, then drains the deque to the real dispatcher channel.
#[cfg(feature = "mesh")]
async fn run_mesh_bridge(
    scheduler: Arc<flexiq_core::scheduler::Scheduler>,
    mesh_node: Arc<flexiq_mesh::MeshNode>,
    job_tx: tokio::sync::mpsc::Sender<flexiq_core::job::Job>,
) {
    let (mesh_tx, mut mesh_rx) = tokio::sync::mpsc::channel::<flexiq_core::job::Job>(64);

    let sched = scheduler.clone();
    let sched_task = tokio::spawn(async move {
        sched.run(mesh_tx).await;
    });

    loop {
        // Drain local deque to dispatcher first
        while let Some(job) = mesh_node.pop_local() {
            if job_tx.send(job).await.is_err() {
                let _ = sched_task.await;
                return;
            }
        }

        // Try stealing when deque is low and stealing is enabled
        if mesh_node.should_steal() {
            mesh_node.try_steal().await;
            // Drain any stolen jobs
            while let Some(job) = mesh_node.pop_local() {
                if job_tx.send(job).await.is_err() {
                    let _ = sched_task.await;
                    return;
                }
            }
        }

        // Wait for scheduler to produce jobs
        match mesh_rx.recv().await {
            Some(job) => {
                let mut batch = vec![job];
                while let Ok(j) = mesh_rx.try_recv() {
                    batch.push(j);
                }
                mesh_node.prefetch(batch);
            }
            None => break,
        }
    }

    // Drain remaining deque
    while let Some(job) = mesh_node.pop_local() {
        if job_tx.send(job).await.is_err() {
            break;
        }
    }

    let _ = sched_task.await;
}

/// Nanoseconds to whole milliseconds, dropping the core's "not measured" 0 so a
/// subscriber never reads an unmeasured run as an instant one.
fn duration_ms(wall_time_ns: i64) -> Option<i64> {
    (wall_time_ns > 0).then_some(wall_time_ns / 1_000_000)
}

/// Parse a `"100/m"`-style rate, rejecting a value the core cannot read.
///
/// Both `rate_limit` and `retry_budget` take this syntax, and both silently lose
/// their limit if the value does not parse — a typo would look like it took
/// effect while the task ran unthrottled. Fail at worker start instead.
fn parse_rate(
    value: Option<&String>,
    field: &str,
    task_name: &str,
) -> PyResult<Option<RateLimitConfig>> {
    let Some(raw) = value else {
        return Ok(None);
    };
    let parsed = RateLimitConfig::parse(raw).ok_or_else(|| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "invalid {field} {raw:?} for task {task_name}: expected a rate like \"100/m\""
        ))
    })?;
    Ok(Some(parsed))
}

/// Parse the `on_excess` spec a task registered with. Unset means the default,
/// `defer`; anything unrecognized is a registration-time error rather than a
/// silent fall back to keeping the job.
fn parse_on_excess(value: Option<&String>, task_name: &str) -> PyResult<OnExcess> {
    let Some(raw) = value else {
        return Ok(OnExcess::default());
    };
    OnExcess::parse(raw).ok_or_else(|| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "invalid on_excess {raw:?} for task {task_name}: expected \"defer\" or \"drop\""
        ))
    })
}

/// Dispatch a ResultOutcome to Python middleware hooks and events.
///
/// Called with the GIL held after `handle_result()` returns.
fn dispatch_outcome(py: Python<'_>, outcome: &ResultOutcome) {
    let result = (|| -> PyResult<()> {
        let context_mod = py.import("flexiq.context")?;
        let queue_ref = context_mod.getattr("_queue_ref")?;
        if queue_ref.is_none() {
            return Ok(());
        }

        match outcome {
            ResultOutcome::Retry {
                job_id,
                task_name,
                queue,
                error,
                retry_count,
                timed_out,
                wall_time_ns,
            } => {
                // Emit JOB_RETRYING event
                let events_mod = py.import("flexiq.events")?;
                let event_type = events_mod.getattr("EventType")?.getattr("JOB_RETRYING")?;
                let payload = PyDict::new(py);
                payload.set_item("job_id", job_id)?;
                payload.set_item("task_name", task_name)?;
                payload.set_item("error", error)?;
                payload.set_item("retry_count", retry_count)?;
                payload.set_item("duration_ms", duration_ms(*wall_time_ns))?;
                queue_ref.call_method1("_emit_event", (event_type, payload))?;

                // Call on_timeout middleware if this was a timeout
                if *timed_out {
                    let ctx = build_lightweight_ctx(py, job_id, task_name, queue)?;
                    call_middleware_hook(py, &queue_ref, task_name, "on_timeout", (ctx,))?;
                }

                // Call on_retry middleware
                let ctx = build_lightweight_ctx(py, job_id, task_name, queue)?;
                let error_obj: Py<PyAny> = pyo3::exceptions::PyRuntimeError::new_err(error.clone())
                    .into_value(py)
                    .into_any();
                call_middleware_hook(
                    py,
                    &queue_ref,
                    task_name,
                    "on_retry",
                    (ctx, error_obj, *retry_count),
                )?;
            }
            ResultOutcome::DeadLettered {
                job_id,
                task_name,
                queue,
                error,
                timed_out,
                wall_time_ns,
            } => {
                // Emit JOB_DEAD event
                let events_mod = py.import("flexiq.events")?;
                let event_type = events_mod.getattr("EventType")?.getattr("JOB_DEAD")?;
                let payload = PyDict::new(py);
                payload.set_item("job_id", job_id)?;
                payload.set_item("task_name", task_name)?;
                payload.set_item("error", error)?;
                payload.set_item("duration_ms", duration_ms(*wall_time_ns))?;
                queue_ref.call_method1("_emit_event", (event_type, payload))?;

                // Call on_timeout middleware if this was a timeout
                if *timed_out {
                    let ctx = build_lightweight_ctx(py, job_id, task_name, queue)?;
                    call_middleware_hook(py, &queue_ref, task_name, "on_timeout", (ctx,))?;
                }

                // Call on_dead_letter middleware
                let ctx = build_lightweight_ctx(py, job_id, task_name, queue)?;
                let error_obj: Py<PyAny> = pyo3::exceptions::PyRuntimeError::new_err(error.clone())
                    .into_value(py)
                    .into_any();
                call_middleware_hook(
                    py,
                    &queue_ref,
                    task_name,
                    "on_dead_letter",
                    (ctx, error_obj),
                )?;
            }
            ResultOutcome::Cancelled {
                job_id,
                task_name,
                queue,
                wall_time_ns,
            } => {
                // Emit JOB_CANCELLED event
                let events_mod = py.import("flexiq.events")?;
                let event_type = events_mod.getattr("EventType")?.getattr("JOB_CANCELLED")?;
                let payload = PyDict::new(py);
                payload.set_item("job_id", job_id)?;
                payload.set_item("task_name", task_name)?;
                payload.set_item("duration_ms", duration_ms(*wall_time_ns))?;
                queue_ref.call_method1("_emit_event", (event_type, payload))?;

                // Call on_cancel middleware
                let ctx = build_lightweight_ctx(py, job_id, task_name, queue)?;
                call_middleware_hook(py, &queue_ref, task_name, "on_cancel", (ctx,))?;
            }
            ResultOutcome::Success { .. } => {
                // Success events are already emitted in _wrap_task
            }
            ResultOutcome::Superseded { .. } => {
                // The job is proceeding under another owner and this attempt
                // wrote no state, so there is nothing to hook or emit.
            }
            ResultOutcome::Slept { .. } => {
                // Handled where the attempt ended, not here: `run_lifecycle`
                // pairs the sleep with `on_sleep` and emits `job.sleeping`
                // while the job context is still live, the same way it owns
                // the success event. Doing it again here would double both.
            }
            // The taxonomy is `#[non_exhaustive]`; an outcome this build does
            // not know is one it cannot hook correctly, so it emits nothing.
            _ => {}
        }
        Ok(())
    })();

    if let Err(e) = result {
        log::error!("[flexiq] middleware dispatch error: {e}");
    }
}

/// Build a lightweight JobContext-like object for middleware hooks called
/// outside of task execution (retry/dlq/cancel outcomes).
fn build_lightweight_ctx<'py>(
    py: Python<'py>,
    job_id: &str,
    task_name: &str,
    queue_name: &str,
) -> PyResult<Bound<'py, pyo3::PyAny>> {
    let types_mod = py.import("types")?;
    let ns = types_mod.call_method1("SimpleNamespace", ())?;
    ns.setattr("id", job_id)?;
    ns.setattr("task_name", task_name)?;
    ns.setattr("queue_name", queue_name)?;
    ns.setattr("retry_count", 0)?;
    Ok(ns)
}

/// Call a middleware hook on all middleware in the chain for a given task.
fn call_middleware_hook<'py, A>(
    py: Python<'py>,
    queue_ref: &Bound<'py, pyo3::PyAny>,
    task_name: &str,
    hook_name: &str,
    args: A,
) -> PyResult<()>
where
    A: IntoPyObject<'py, Target = PyTuple>,
    A::Error: Into<PyErr>,
{
    let chain = queue_ref.call_method1("_get_middleware_chain", (task_name,))?;
    let args_tuple = args.into_pyobject(py).map_err(Into::into)?.into_bound();
    for mw in chain.try_iter()? {
        let mw = mw?;
        if let Err(e) = mw.call_method(hook_name, args_tuple.clone(), None) {
            let logging = py.import("logging")?;
            let logger = logging.call_method1("getLogger", ("flexiq",))?;
            logger.call_method1("warning", (format!("middleware {hook_name}() error: {e}"),))?;
        }
    }
    Ok(())
}

/// Fingerprint of the registry Python actually dispatches from.
///
/// Taken from the very dict the dispatcher looks task names up in, so the
/// worker's registry row cannot advertise a task set this worker will not run.
/// `None` when the object is not a dict — only reachable by calling the native
/// method directly, and a wrong fingerprint would be worse than none.
fn registry_fingerprint_of(py: Python<'_>, task_registry: &Py<PyAny>) -> Option<String> {
    let registry = task_registry.bind(py).cast::<PyDict>().ok()?;
    registry_fingerprint(
        registry
            .keys()
            .iter()
            .filter_map(|name| name.extract::<String>().ok()),
    )
}

#[pymethods]
#[allow(clippy::useless_conversion)]
impl PyQueue {
    /// Run the worker loop. This blocks until interrupted.
    ///
    /// The heartbeat is now driven from Python (see `worker_heartbeat`),
    /// so the internal Rust heartbeat thread is removed.
    #[pyo3(signature = (
        task_registry,
        task_configs,
        queues=None,
        drain_timeout_secs=None,
        tags=None,
        worker_id=None,
        resources=None,
        threads=1,
        async_concurrency=100,
        queue_configs=None,
        pool=None,
        app_path=None,
        mesh_config=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn run_worker(
        &self,
        py: Python<'_>,
        task_registry: Py<PyAny>,
        task_configs: Vec<PyTaskConfig>,
        queues: Option<Vec<String>>,
        drain_timeout_secs: Option<u64>,
        tags: Option<String>,
        worker_id: Option<String>,
        resources: Option<String>,
        threads: i32,
        #[allow(unused_variables)] async_concurrency: i32,
        queue_configs: Option<String>,
        pool: Option<String>,
        app_path: Option<String>,
        #[allow(unused_variables)] mesh_config: Option<String>,
    ) -> PyResult<()> {
        // Reset shutdown flag for this run
        self.shutdown_flag.store(false, Ordering::SeqCst);

        let queues = queues.unwrap_or_else(|| vec!["default".to_string()]);
        let queues_str = queues.join(",");

        // Clamp once for every consumer: 0 would leave `asyncio.Semaphore(0)` holding
        // every native async job forever, and a negative raises inside `start()`.
        #[allow(unused_variables)]
        let async_concurrency = async_concurrency.max(1) as usize;

        let use_prefork = pool.as_deref() == Some("prefork");

        // Bound in-flight work to what this worker can actually run, so it never
        // claims more and starves peers sharing the DB.
        //
        // With the native pool, sync and async work draw on separate budgets:
        // blocking tasks are bounded by `num_workers` threads, coroutines by the
        // executor's `async_concurrency` semaphore — so the cap is their sum.
        // Everywhere else — non-native builds, and prefork, which builds no
        // executor — every task is bounded by `num_workers`; adding
        // `async_concurrency` there would claim jobs nothing can execute — they
        // would sit Running in the channel waiting for a worker slot.
        #[cfg(feature = "native-async")]
        let max_in_flight = if use_prefork {
            self.num_workers
        } else {
            self.num_workers + async_concurrency
        };
        #[cfg(not(feature = "native-async"))]
        let max_in_flight = self.num_workers;

        let scheduler_config = SchedulerConfig {
            poll_interval: std::time::Duration::from_millis(self.scheduler_poll_interval_ms),
            reap_interval: self.scheduler_reap_interval,
            cleanup_interval: self.scheduler_cleanup_interval,
            result_ttl_ms: self.result_ttl_ms,
            retention: self.retention.clone(),
            batch_size: self.scheduler_batch_size,
            max_in_flight: Some(max_in_flight),
            dlq_auto_retry_delay_ms: self.dlq_auto_retry_delay_ms,
            dlq_auto_retry_max: self.dlq_auto_retry_max,
            ..SchedulerConfig::default()
        };
        // Resolve the worker id up front so the scheduler claims execution under
        // it — dead-worker recovery attributes orphaned claims by this id, which
        // must match the `register_worker`/`heartbeat` id below.
        let worker_id = worker_id.unwrap_or_else(|| uuid::Uuid::now_v7().to_string());
        let mut scheduler = Scheduler::new(
            self.storage.clone(),
            queues,
            scheduler_config,
            self.namespace.clone(),
        );
        scheduler.set_claim_owner(worker_id.clone());
        // This worker's own step handle, fenced on the id it claims execution
        // under. It belongs to the worker rather than to `self`, because one
        // `Queue` may run several workers and a shared slot would leave all but
        // the last of them committing steps under a claim they do not hold.
        let worker_steps = Arc::new(Py::new(
            py,
            PyWorkerSteps::new(
                self.storage.clone(),
                self.namespace.clone(),
                worker_id.clone(),
            ),
        )?);

        // Build retry filters dict from the Queue's _task_retry_filters
        let retry_filters = PyDict::new(py).into_any();
        // Get the Queue's retry filters from the app module
        let app_queue_ref = {
            let context_mod = py.import("flexiq.context")?;
            context_mod.getattr("_queue_ref")?
        };
        if !app_queue_ref.is_none() {
            if let Ok(filters) = app_queue_ref.getattr("_task_retry_filters") {
                let filters_dict: &Bound<'_, PyDict> = filters.cast()?;
                let out_dict: &Bound<'_, PyDict> = retry_filters.cast()?;
                for (key, val) in filters_dict.iter() {
                    out_dict.set_item(key, val)?;
                }
            }
        }

        for tc in &task_configs {
            let custom_delays_ms = tc.retry_delays.as_ref().map(|delays| {
                delays
                    .iter()
                    .map(|d| {
                        if !d.is_finite() || *d < 0.0 {
                            0i64
                        } else {
                            (d.min(i64::MAX as f64 / 1000.0) * 1000.0) as i64
                        }
                    })
                    .collect()
            });
            let base_delay_ms = if !tc.retry_backoff.is_finite() || tc.retry_backoff < 0.0 {
                0i64
            } else {
                (tc.retry_backoff.min(i64::MAX as f64 / 1000.0) * 1000.0) as i64
            };
            let max_delay_ms = tc
                .max_retry_delay
                .map(|s| s.saturating_mul(1000))
                .unwrap_or(300_000);
            let retry_policy = RetryPolicy {
                max_retries: tc.max_retries,
                base_delay_ms,
                max_delay_ms,
                custom_delays_ms,
            };
            let rate_limit = parse_rate(tc.rate_limit.as_ref(), "rate_limit", &tc.name)?;
            let retry_budget = parse_rate(tc.retry_budget.as_ref(), "retry_budget", &tc.name)?;
            let circuit_breaker =
                tc.circuit_breaker_threshold
                    .map(|threshold| CircuitBreakerConfig {
                        threshold,
                        window_ms: tc.circuit_breaker_window.unwrap_or(60) * 1000,
                        cooldown_ms: tc.circuit_breaker_cooldown.unwrap_or(300) * 1000,
                        half_open_max_probes: tc.circuit_breaker_half_open_probes.unwrap_or(5),
                        half_open_success_rate: tc
                            .circuit_breaker_half_open_success_rate
                            .unwrap_or(0.8),
                    });
            scheduler.register_task(
                tc.name.clone(),
                TaskConfig {
                    retry_policy,
                    rate_limit,
                    on_excess: parse_on_excess(tc.on_excess.as_ref(), &tc.name)?,
                    circuit_breaker,
                    retry_budget,
                    max_concurrent: tc.max_concurrent,
                    max_in_flight_per_task: tc.max_in_flight_per_task.map(|n| n.max(1) as usize),
                },
            );
        }

        // Register queue-level rate limits and concurrency caps
        if let Some(ref qc_json) = queue_configs {
            if let Ok(map) = serde_json::from_str::<
                std::collections::HashMap<String, serde_json::Value>,
            >(qc_json)
            {
                for (queue_name, cfg) in map {
                    let rate_limit = cfg
                        .get("rate_limit")
                        .and_then(|v| v.as_str())
                        .and_then(RateLimitConfig::parse);
                    let max_concurrent = cfg
                        .get("max_concurrent")
                        .and_then(|v| v.as_i64())
                        .map(|v| v as i32);
                    if let Some(codel) = parse_codel(&cfg) {
                        scheduler.register_queue_codel(queue_name.clone(), codel);
                    }
                    if let Some("lifo") = cfg.get("dispatch_order").and_then(|v| v.as_str()) {
                        scheduler.register_queue_dispatch_order(
                            queue_name.clone(),
                            flexiq_core::storage::DispatchOrder::Lifo,
                        );
                    }
                    scheduler.register_queue_config(
                        queue_name,
                        flexiq_core::scheduler::QueueConfig {
                            rate_limit,
                            max_concurrent,
                        },
                    );
                }
            }
        }

        let shutdown = scheduler.shutdown_handle();

        // Headroom over the in-flight cap, so the cheap pre-claim gate binds before
        // the channel does: the channel-full path claims and then rolls back, which
        // churns storage where the gate does not.
        let (job_tx, job_rx) = tokio::sync::mpsc::channel(self.num_workers * 2);
        let (result_tx, result_rx) = crossbeam_channel::bounded(self.num_workers * 2);

        // Read before the registry moves into the `Arc` the pools share.
        let fingerprint = registry_fingerprint_of(py, &task_registry);

        let registry_arc = Arc::new(task_registry);
        let filters_arc: Arc<Py<PyAny>> = Arc::new(retry_filters.into());

        let scheduler_arc = Arc::new(scheduler);
        let scheduler_for_dispatch = scheduler_arc.clone();

        // Register the worker under the id resolved above (shared with the
        // scheduler's claim owner).
        let hostname = gethostname::gethostname().to_string_lossy().to_string();
        let pid = std::process::id() as i32;
        let _ = self.storage.register_worker(
            &WorkerRegistration::new(&worker_id, &queues_str, threads)
                .tags(tags.as_deref())
                .resources(resources.as_deref())
                .hostname(Some(&hostname))
                .pid(Some(pid))
                .pool_type(pool.as_deref())
                // The native module's version, which maturin builds from the
                // same workspace version the wheel carries.
                .sdk(Some("python"), Some(env!("CARGO_PKG_VERSION")))
                .registry_fingerprint(fingerprint.as_deref()),
        );

        // Build the dispatcher up front for the prefork case so we can install
        // it on the queue before the run loop starts — request_cancel relies on
        // the install to deliver out-of-band cancel signals to child processes.
        //
        // For in-process pools (native-async, classic async) `notify_cancel` is
        // a no-op — running tasks observe cancellation via the storage flag —
        // so we don't install the dispatcher on `self`.
        let num_workers = self.num_workers;

        // Held on this thread so shutdown can stop the executor's event loop
        // once the drain completes. Safe to keep here: the drain exits when
        // in-flight work settles, not when the result channel disconnects, so
        // this reference cannot deadlock shutdown.
        #[cfg(feature = "native-async")]
        let mut async_executor_for_shutdown: Option<Arc<Py<PyAny>>> = None;

        let dispatcher_for_run: Arc<dyn flexiq_core::worker::WorkerDispatcher> = if use_prefork {
            let pool_arc: Arc<dyn flexiq_core::worker::WorkerDispatcher> =
                Arc::new(crate::prefork::PreforkPool::new(
                    num_workers,
                    app_path.unwrap_or_default(),
                    Some(worker_id.clone()),
                    // Nothing to relay: these children reach storage and open
                    // their own sessions under the claim inherited from here.
                    false,
                ));
            self.set_dispatcher(Some(pool_arc.clone()));
            pool_arc
        } else {
            #[cfg(feature = "native-async")]
            {
                // The executor is built only for the pool that uses it —
                // prefork must not hold a PyResultSender (a result-channel
                // clone) it would never send on.
                let sender = crate::native_async::PyResultSender::new(result_tx.clone());
                let steps_for_executor = worker_steps.clone();
                let async_executor = Python::attach(|py| -> PyResult<Arc<Py<PyAny>>> {
                    let sender_obj = pyo3::Py::new(py, sender)?;
                    let mod_ = py.import("flexiq.async_support.executor")?;
                    let cls = mod_.getattr("AsyncTaskExecutor")?;
                    let context_mod = py.import("flexiq.context")?;
                    let queue_ref = context_mod.getattr("_queue_ref")?;
                    let executor = cls.call1((
                        sender_obj,
                        registry_arc.clone_ref(py),
                        queue_ref,
                        async_concurrency,
                        steps_for_executor.clone_ref(py),
                    ))?;
                    executor.call_method0("start")?;
                    Ok(Arc::new(executor.unbind()))
                })?;
                async_executor_for_shutdown = Some(async_executor.clone());
                let pool_arc: Arc<dyn flexiq_core::worker::WorkerDispatcher> =
                    Arc::new(crate::native_async::NativeAsyncPool::new(
                        num_workers,
                        async_concurrency,
                        registry_arc.clone(),
                        filters_arc.clone(),
                        async_executor,
                        worker_steps.clone(),
                    ));
                pool_arc
            }
            #[cfg(not(feature = "native-async"))]
            {
                let pool_arc: Arc<dyn flexiq_core::worker::WorkerDispatcher> =
                    Arc::new(AsyncWorkerPool::new(
                        num_workers,
                        registry_arc.clone(),
                        filters_arc.clone(),
                        worker_steps.clone(),
                    ));
                pool_arc
            }
        };

        #[cfg(feature = "mesh")]
        let mesh_worker_id = worker_id.clone();

        // Mesh-stolen jobs enter the channel through the bridge, bypassing
        // `finish_dispatch` — the scheduler's in-flight tracking never sees
        // them, so the settled-work drain exit below is only sound without
        // mesh.
        #[cfg(feature = "mesh")]
        let mesh_enabled = mesh_config.is_some();
        #[cfg(not(feature = "mesh"))]
        let mesh_enabled = false;

        // Captured for the wake-source install inside the runtime below.
        let push_dispatch_enabled = self.push_dispatch;

        // Move result_tx into the runtime — don't keep a copy in the main thread
        // so result_rx disconnects when all workers are done.
        let runtime_handle = std::thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2) // Scheduler + pool coordinator
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    log::error!("flexiq: failed to build tokio runtime: {e}");
                    return;
                }
            };

            rt.block_on(async {
                // Push-dispatch: install the wake source before the scheduler
                // loop takes it. Inside the runtime because the Postgres and
                // Redis sources spawn a listener task. Without the
                // `push-dispatch` feature this logs and keeps polling, so
                // `push_dispatch=True` is accepted and ignored.
                if push_dispatch_enabled {
                    scheduler_for_dispatch.enable_push_dispatch();
                }

                // When mesh is enabled, interpose a local deque between
                // scheduler and dispatcher for affinity-sorted prefetch,
                // and spawn the SWIM gossip loop for peer discovery.
                #[cfg(feature = "mesh")]
                let scheduler_task = {
                    if let Some(ref cfg_json) = mesh_config {
                        let mesh_cfg: flexiq_mesh::MeshConfig =
                            serde_json::from_str(cfg_json).unwrap_or_default();
                        let mesh_node =
                            Arc::new(flexiq_mesh::MeshNode::new(mesh_worker_id, mesh_cfg));
                        log::info!(
                            "[flexiq] mesh scheduling enabled (local_buffer={})",
                            mesh_node.config().local_buffer_capacity,
                        );

                        let gossip_queues: Vec<String> =
                            queues_str.split(',').map(|s| s.to_string()).collect();
                        let gossip_handle =
                            mesh_node.spawn_gossip(gossip_queues, num_workers as u16);
                        let steal_handle = mesh_node.spawn_steal_server();

                        let mesh_for_bridge = mesh_node.clone();
                        let bridge_handle = tokio::spawn(async move {
                            run_mesh_bridge(scheduler_for_dispatch, mesh_for_bridge, job_tx).await;
                        });

                        tokio::spawn(async move {
                            let _ = tokio::join!(bridge_handle, gossip_handle, steal_handle);
                        })
                    } else {
                        tokio::spawn(async move {
                            scheduler_for_dispatch.run(job_tx).await;
                        })
                    }
                };
                #[cfg(not(feature = "mesh"))]
                let scheduler_task = tokio::spawn(async move {
                    scheduler_for_dispatch.run(job_tx).await;
                });

                let worker_task = tokio::spawn(async move {
                    dispatcher_for_run.run(job_rx, result_tx).await;
                });

                let _ = tokio::join!(scheduler_task, worker_task);
            });
        });

        let scheduler_for_results = scheduler_arc.clone();
        let flag = self.shutdown_flag.clone();

        // Poll action enum for communicating between GIL-released and
        // GIL-held sections of the result loop.
        enum PollAction {
            Shutdown,
            Result(JobResult),
            Continue,
            Done,
        }

        let drain_timeout = std::time::Duration::from_secs(drain_timeout_secs.unwrap_or(30));

        // Refcount-independent "drain is done" signal. Waiting for the result
        // channel to *disconnect* requires every sender clone to drop, and the
        // async executor's PyResultSender is owned by a Python object — a failed
        // task's traceback can pin it (any hook that retains the exception
        // reaches the frame that owns the sender), stalling shutdown for the
        // full drain timeout. Instead: the runtime thread finishing proves the
        // scheduler returned (no dispatch in progress) and the pool drained its
        // queue (the runtime's drop flushes running blocking tasks), and a
        // settled in-flight map proves every dispatched job's result was
        // handled — this loop is the only consumer, so nothing can still be
        // pending.
        let work_settled = || {
            !mesh_enabled
                && runtime_handle.is_finished()
                && scheduler_for_results.in_flight_settled()
        };

        loop {
            // Release GIL for one iteration of result polling
            let action = py.detach(|| {
                if flag.load(Ordering::SeqCst) {
                    return PollAction::Shutdown;
                }
                match result_rx.recv_timeout(std::time::Duration::from_millis(100)) {
                    Ok(result) => PollAction::Result(result),
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => PollAction::Continue,
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => PollAction::Done,
                }
            });

            // Re-acquire GIL briefly to let Python signal handlers run
            py.check_signals()?;

            match action {
                PollAction::Shutdown => {
                    // Stop the scheduler from dispatching new jobs
                    py.detach(|| shutdown.notify_one());

                    // Drain remaining results with a timeout
                    let drain_start = std::time::Instant::now();
                    while drain_start.elapsed() < drain_timeout {
                        let drain_action = py.detach(|| {
                            match result_rx.recv_timeout(std::time::Duration::from_millis(100)) {
                                Ok(result) => PollAction::Result(result),
                                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                                    PollAction::Continue
                                }
                                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                                    PollAction::Done
                                }
                            }
                        });

                        // Allow signal handlers to run during drain too
                        py.check_signals()?;

                        match drain_action {
                            PollAction::Result(result) => {
                                let outcomes = py.detach(|| {
                                    let mut batch = vec![result];
                                    while let Ok(more) = result_rx.try_recv() {
                                        batch.push(more);
                                    }
                                    scheduler_for_results.handle_results(batch)
                                });
                                for outcome in &outcomes {
                                    match outcome {
                                        Ok(o) => dispatch_outcome(py, o),
                                        Err(e) => {
                                            log::error!("[flexiq] result handling error: {e}")
                                        }
                                    }
                                }
                            }
                            PollAction::Continue => {
                                if work_settled() {
                                    break;
                                }
                                continue;
                            }
                            PollAction::Done => break,
                            PollAction::Shutdown => unreachable!(),
                        }
                    }
                    break;
                }
                PollAction::Result(result) => {
                    // Drain every result already queued and finalize them in one
                    // batched transaction instead of one-per-wake. The channel is
                    // bounded, so the drain is naturally capped.
                    let outcomes = py.detach(|| {
                        let mut batch = vec![result];
                        while let Ok(more) = result_rx.try_recv() {
                            batch.push(more);
                        }
                        scheduler_for_results.handle_results(batch)
                    });
                    for outcome in &outcomes {
                        match outcome {
                            Ok(o) => dispatch_outcome(py, o),
                            Err(e) => log::error!("[flexiq] result handling error: {e}"),
                        }
                    }
                }
                PollAction::Continue => {
                    // Without this, a runtime that died with all work settled
                    // would leave the loop polling a channel that can never
                    // disconnect (the executor still holds a sender).
                    if work_settled() {
                        break;
                    }
                    continue;
                }
                PollAction::Done => break,
            }
        }

        let _ = runtime_handle.join();

        // Stop the async executor's event loop and drop its result sender.
        // Best effort — shutdown must finish even if the interpreter is
        // tearing down. Without this the daemon thread and the sender live
        // until the executor object is garbage collected.
        #[cfg(feature = "native-async")]
        if let Some(executor) = async_executor_for_shutdown {
            if let Err(e) = executor.call_method0(py, "stop") {
                log::warn!("[flexiq] async executor stop failed: {e}");
            }
        }

        // Clear the dispatcher reference so post-shutdown cancel requests
        // become no-ops instead of forwarding to a torn-down pool.
        self.set_dispatcher(None);

        // Unregister worker on shutdown
        let _ = self.storage.unregister_worker(&worker_id);

        Ok(())
    }

    /// Update the heartbeat for a running worker. Called from Python every 5s.
    /// Returns a list of worker IDs that were reaped as dead.
    ///
    /// Only the elected reaper sweeps: every worker running this every 5s makes
    /// the dead-worker scan O(N) per cluster, and each returns the same dead ids
    /// so a `WORKER_OFFLINE` webhook fires N times per death. A non-leader
    /// returns an empty list and emits nothing.
    #[pyo3(signature = (worker_id, resource_health=None))]
    pub fn worker_heartbeat(
        &self,
        worker_id: &str,
        resource_health: Option<&str>,
    ) -> PyResult<Vec<String>> {
        self.storage
            .heartbeat(worker_id, resource_health)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Ok(flexiq_core::storage::reap_dead_workers_if_leader(
            &self.storage,
            worker_id,
        ))
    }

    /// Update the status of a worker. An unrecognized status is a caller error —
    /// coercing it would silently un-drain a worker.
    pub fn set_worker_status(&self, worker_id: &str, status: &str) -> PyResult<()> {
        let parsed = WorkerStatus::parse(status).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "unknown worker status '{status}' (expected 'active' or 'draining')"
            ))
        })?;
        self.storage
            .update_worker_status(worker_id, parsed)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }
}
