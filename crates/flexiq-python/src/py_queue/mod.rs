// pyo3's #[pymethods] macro generates Into<PyErr> conversions that trigger this lint
#![allow(clippy::useless_conversion)]

mod inspection;
mod pubsub;
mod steps;
mod worker;
#[cfg(feature = "workflows")]
mod workflow_ops;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use pyo3::prelude::*;
use pyo3::types::PyDict;

use flexiq_core::job::{now_millis, NewJob};
use flexiq_core::periodic::next_cron_time;
use flexiq_core::scheduler::retention::RetentionConfig;
#[cfg(feature = "postgres")]
use flexiq_core::storage::postgres::PostgresStorage;
use flexiq_core::storage::records::{DebounceOptions, NewPeriodicTask};
#[cfg(feature = "redis")]
use flexiq_core::storage::redis_backend::RedisStorage;
use flexiq_core::storage::sqlite::SqliteStorage;
use flexiq_core::storage::{Storage, StorageBackend};
use flexiq_core::worker::WorkerDispatcher;

use crate::py_job::PyJob;
use crate::py_step::CLAIM_OWNER_ENV;

pub(crate) use flexiq_core::storage::cursor::next_cursor;

/// The core queue engine exposed to Python.
#[pyclass]
pub struct PyQueue {
    pub(crate) storage: StorageBackend,
    pub(crate) db_path: String,
    pub(crate) num_workers: usize,
    pub(crate) default_retry: i32,
    pub(crate) default_timeout: i64,
    pub(crate) default_priority: i32,
    pub(crate) shutdown_flag: Arc<AtomicBool>,
    pub(crate) result_ttl_ms: Option<i64>,
    pub(crate) retention: Option<RetentionConfig>,
    pub(crate) scheduler_poll_interval_ms: u64,
    pub(crate) scheduler_reap_interval: u32,
    pub(crate) scheduler_cleanup_interval: u32,
    pub(crate) scheduler_batch_size: usize,
    pub(crate) dlq_auto_retry_delay_ms: Option<i64>,
    pub(crate) dlq_auto_retry_max: i32,
    pub(crate) namespace: Option<String>,
    /// Opt-in event-driven dispatch. Honored only when the crate is built with
    /// the `push-dispatch` cargo feature; otherwise accepted and ignored.
    pub(crate) push_dispatch: bool,
    /// Whether opening applies schema changes. When false the workflow store is
    /// built unmigrated too, so *no* path applies DDL until `migrate()` runs.
    /// Only the workflow path reads it, hence the cfg.
    #[cfg(feature = "workflows")]
    pub(crate) auto_migrate: bool,
    /// Active worker dispatcher, set while `run_worker` is executing. Used by
    /// `request_cancel` to deliver a side-channel signal to pools that run
    /// tasks out-of-process (prefork). For in-process pools the trait's
    /// default no-op makes this a free notification.
    pub(crate) dispatcher: Arc<Mutex<Option<Arc<dyn WorkerDispatcher>>>>,
    /// Worker id the execution claim was won under, which is what every step
    /// write is fenced on. Never read off task code or a frame: an in-process
    /// worker sets it when `run_worker` resolves the id, and a prefork child
    /// inherits it through the environment its parent spawned it with. Absent
    /// means this process holds no claim — an attached executor, or a plain
    /// enqueue-side `Queue` — and durable steps refuse rather than degrade.
    pub(crate) claim_owner: Arc<Mutex<Option<String>>>,
    /// Cached workflow storage handle. Lazily initialized on first workflow API
    /// call; migrations run exactly once per `PyQueue` instance instead of
    /// per-call.
    #[cfg(feature = "workflows")]
    pub(crate) workflow_storage: std::sync::OnceLock<flexiq_workflows::WorkflowStorageBackend>,
}

/// Build a per-table [`RetentionConfig`] from a `{table: seconds}` map. An
/// absent map is `None` (fall back to the legacy `result_ttl`); an explicitly
/// empty map is `Some(default)` — the caller asked for retention and set no
/// windows, which must disable them all, not fall back. Each value is validated
/// non-negative and converted to milliseconds; an unknown table name is
/// rejected so a typo never silently disables a window.
fn build_retention_config(
    retention: Option<std::collections::HashMap<String, i64>>,
) -> PyResult<Option<RetentionConfig>> {
    let Some(map) = retention else {
        return Ok(None);
    };
    if map.is_empty() {
        return Ok(Some(RetentionConfig::default()));
    }

    let to_ms = |secs: i64| -> PyResult<i64> {
        if secs < 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "retention windows must be non-negative",
            ));
        }
        secs.checked_mul(1000).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err("retention window too large, would overflow")
        })
    };

    let mut config = RetentionConfig::default();
    for (table, secs) in map {
        let ms = to_ms(secs)?;
        match table.as_str() {
            "archived_jobs" => config.archived_jobs_ttl_ms = Some(ms),
            "dead_letter" => config.dead_letter_ttl_ms = Some(ms),
            "task_logs" => config.task_logs_ttl_ms = Some(ms),
            "task_metrics" => config.task_metrics_ttl_ms = Some(ms),
            "job_errors" => config.job_errors_ttl_ms = Some(ms),
            other => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "unknown retention table '{other}'"
                )))
            }
        }
    }
    Ok(Some(config))
}

#[pymethods]
#[allow(
    clippy::too_many_arguments,
    clippy::useless_conversion,
    unused_variables
)]
impl PyQueue {
    #[new]
    #[pyo3(signature = (db_path=".flexiq/flexiq.db", workers=0, default_retry=3, default_timeout=300, default_priority=0, result_ttl=None, backend="sqlite", db_url=None, schema="flexiq", pool_size=None, scheduler_poll_interval_ms=50, scheduler_reap_interval=100, scheduler_cleanup_interval=1200, scheduler_batch_size=1, namespace=None, push_dispatch=false, dlq_auto_retry_delay=None, dlq_auto_retry_max=1, retention=None, auto_migrate=true))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        py: Python<'_>,
        db_path: &str,
        workers: usize,
        default_retry: i32,
        default_timeout: i64,
        default_priority: i32,
        result_ttl: Option<i64>,
        backend: &str,
        db_url: Option<&str>,
        schema: &str,
        pool_size: Option<u32>,
        scheduler_poll_interval_ms: u64,
        scheduler_reap_interval: u32,
        scheduler_cleanup_interval: u32,
        scheduler_batch_size: usize,
        namespace: Option<String>,
        push_dispatch: bool,
        dlq_auto_retry_delay: Option<i64>,
        dlq_auto_retry_max: i32,
        retention: Option<std::collections::HashMap<String, i64>>,
        auto_migrate: bool,
    ) -> PyResult<Self> {
        // A negative TTL inverts the cleanup cutoff: `now.saturating_sub(-ttl)`
        // lands in the future, so every archived job matches and auto-cleanup
        // deletes the whole history.
        if let Some(ttl) = result_ttl {
            if ttl < 0 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "result_ttl must be non-negative",
                ));
            }
        }
        if let Some(delay) = dlq_auto_retry_delay {
            if delay < 0 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "dlq_auto_retry_delay must be non-negative",
                ));
            }
        }
        if dlq_auto_retry_max < 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "dlq_auto_retry_max must be non-negative",
            ));
        }

        let result_ttl_ms = result_ttl
            .map(|s| {
                s.checked_mul(1000).ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err("result_ttl too large, would overflow")
                })
            })
            .transpose()?;

        let retention = build_retention_config(retention)?;

        let dlq_auto_retry_delay_ms = dlq_auto_retry_delay
            .map(|s| {
                s.checked_mul(1000).ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err(
                        "dlq_auto_retry_delay too large, would overflow",
                    )
                })
            })
            .transpose()?;

        // Storage init blocks on connection-pool builders that may emit
        // `log::*` records from worker threads. With the pyo3-log bridge
        // active, those records need the GIL to deliver to Python — so we
        // must release it here to avoid a deadlock.
        let storage = py.detach(|| -> PyResult<StorageBackend> {
            match backend {
                "sqlite" => {
                    // `pool_size` stays a Postgres knob here, as it always has
                    // been — this arm only gains the auto-migrate branch.
                    const SQLITE_POOL: u32 = 8;
                    let s = if auto_migrate {
                        SqliteStorage::with_pool_size(db_path, SQLITE_POOL)
                    } else {
                        SqliteStorage::unmigrated(db_path, SQLITE_POOL)
                    }
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                    Ok(StorageBackend::Sqlite(s))
                }
                #[cfg(feature = "postgres")]
                "postgres" | "postgresql" => {
                    let url = db_url.ok_or_else(|| {
                        pyo3::exceptions::PyValueError::new_err(
                            "db_url is required for postgres backend",
                        )
                    })?;
                    let pool = pool_size.unwrap_or(10);
                    let s = if auto_migrate {
                        PostgresStorage::with_schema_and_pool_size(url, schema, pool)
                    } else {
                        PostgresStorage::unmigrated(url, schema, pool)
                    }
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                    Ok(StorageBackend::Postgres(s))
                }
                #[cfg(feature = "redis")]
                "redis" => {
                    let url = db_url.ok_or_else(|| {
                        pyo3::exceptions::PyValueError::new_err(
                            "db_url is required for redis backend",
                        )
                    })?;
                    let s = RedisStorage::new(url)
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                    Ok(StorageBackend::Redis(s))
                }
                _ => {
                    #[allow(unused_mut, clippy::useless_vec)]
                    let mut available = vec!["sqlite"];
                    #[cfg(feature = "postgres")]
                    available.push("postgres");
                    #[cfg(feature = "redis")]
                    available.push("redis");
                    Err(pyo3::exceptions::PyValueError::new_err(format!(
                        "Unknown backend: '{backend}'. Available backends: {}.",
                        available.join(", ")
                    )))
                }
            }
        })?;

        // Every process that joins a deployment passes through this one open,
        // so the contract floor is checked here rather than in the SDK. The one
        // storage that cannot answer is one whose schema was never applied —
        // there is no floor recorded and nothing to read it from — and that
        // storage is unusable until `migrate()` runs, which checks it then.
        py.detach(|| -> flexiq_core::Result<()> {
            if auto_migrate || storage.is_migrated()? {
                flexiq_core::ensure_contract_supported(&storage)?;
            }
            Ok(())
        })
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        let num_workers = if workers == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        } else {
            workers
        };

        Ok(Self {
            retention,
            storage,
            db_path: db_path.to_string(),
            num_workers,
            default_retry,
            default_timeout,
            default_priority,
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            result_ttl_ms,
            scheduler_poll_interval_ms,
            scheduler_reap_interval,
            scheduler_cleanup_interval,
            scheduler_batch_size: scheduler_batch_size.max(1),
            dlq_auto_retry_delay_ms,
            dlq_auto_retry_max,
            namespace,
            push_dispatch,
            #[cfg(feature = "workflows")]
            auto_migrate,
            dispatcher: Arc::new(Mutex::new(None)),
            claim_owner: Arc::new(Mutex::new(inherited_claim_owner())),
            #[cfg(feature = "workflows")]
            workflow_storage: std::sync::OnceLock::new(),
        })
    }

    /// Signal the worker to shut down gracefully.
    pub fn request_shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::SeqCst);
    }

    /// Enqueue a job.
    ///
    /// The three `debounce_*` window arguments travel together: supplying all
    /// of them routes the write through
    /// [`Storage::enqueue_debounced`](flexiq_core::storage::Storage::enqueue_debounced)
    /// instead of a plain insert, so a burst on one key collapses into a
    /// single run. Supplying only some of them is a caller error rather than a
    /// partially-applied window — an absent `max_wait_ms` in particular would
    /// mean an unbounded debounce, which starves the job.
    #[pyo3(signature = (task_name, payload, queue="default", priority=None, delay_seconds=None, max_retries=None, timeout=None, unique_key=None, metadata=None, notes=None, depends_on=None, expires=None, result_ttl=None, debounce_key=None, debounce_window_ms=None, debounce_max_wait_ms=None, debounce_replace_payload=false))]
    #[allow(clippy::too_many_arguments)]
    pub fn enqueue(
        &self,
        task_name: &str,
        payload: Vec<u8>,
        queue: &str,
        priority: Option<i32>,
        delay_seconds: Option<f64>,
        max_retries: Option<i32>,
        timeout: Option<i64>,
        unique_key: Option<String>,
        metadata: Option<String>,
        notes: Option<String>,
        depends_on: Option<Vec<String>>,
        expires: Option<f64>,
        result_ttl: Option<i64>,
        debounce_key: Option<String>,
        debounce_window_ms: Option<i64>,
        debounce_max_wait_ms: Option<i64>,
        debounce_replace_payload: bool,
    ) -> PyResult<PyJob> {
        let debounce = match (
            debounce_key.as_deref(),
            debounce_window_ms,
            debounce_max_wait_ms,
        ) {
            (None, None, None) if !debounce_replace_payload => None,
            (Some(_), Some(window_ms), Some(max_wait_ms)) => Some(DebounceOptions {
                window_ms,
                max_wait_ms,
                replace_payload: debounce_replace_payload,
            }),
            // `replace_payload` only means anything to a debounced write, so on
            // its own it is a window the caller thinks they configured and did
            // not. Its own arm because the message names the missing three.
            (None, None, None) => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "debounce_replace_payload requires debounce_key, debounce_window_ms \
                     and debounce_max_wait_ms",
                ))
            }
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "debounce_key, debounce_window_ms and debounce_max_wait_ms must be \
                     supplied together",
                ))
            }
        };
        let now = now_millis();
        let scheduled_at = match delay_seconds {
            Some(d) => {
                if !d.is_finite() || d < 0.0 {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "delay_seconds must be a finite non-negative number",
                    ));
                }
                if d > i64::MAX as f64 / 1000.0 {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "delay_seconds too large",
                    ));
                }
                let delay_ms = (d * 1000.0) as i64;
                now.checked_add(delay_ms).ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err(
                        "delay_seconds too large, would overflow",
                    )
                })?
            }
            None => now,
        };

        let expires_at = match expires {
            Some(e) => {
                if !e.is_finite() || e < 0.0 {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "expires must be a finite non-negative number",
                    ));
                }
                if e > i64::MAX as f64 / 1000.0 {
                    return Err(pyo3::exceptions::PyValueError::new_err("expires too large"));
                }
                let expires_ms = (e * 1000.0) as i64;
                Some(now.checked_add(expires_ms).ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err("expires too large, would overflow")
                })?)
            }
            None => None,
        };
        let result_ttl_ms = match result_ttl {
            Some(s) => Some(s.checked_mul(1000).ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err("result_ttl too large, would overflow")
            })?),
            None => None,
        };

        let timeout_ms = timeout
            .unwrap_or(self.default_timeout)
            .checked_mul(1000)
            .ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err("timeout too large, would overflow")
            })?;

        let new_job = NewJob {
            queue: queue.to_string(),
            task_name: task_name.to_string(),
            payload,
            priority: priority.unwrap_or(self.default_priority),
            scheduled_at,
            max_retries: max_retries.unwrap_or(self.default_retry),
            timeout_ms,
            unique_key: unique_key.clone(),
            metadata,
            notes,
            depends_on: depends_on.unwrap_or_default(),
            expires_at,
            result_ttl_ms,
            namespace: self.namespace.clone(),
            debounce_key,
        };

        let job = match debounce {
            Some(options) => self.storage.enqueue_debounced(new_job, options),
            None if unique_key.is_some() => self.storage.enqueue_unique(new_job),
            None => self.storage.enqueue(new_job),
        };

        let job = job.map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyJob::from(job))
    }

    /// Enqueue multiple jobs in a single transaction.
    #[pyo3(signature = (task_names, payloads, queues=None, priorities=None, max_retries_list=None, timeouts=None, delay_seconds_list=None, unique_keys=None, metadata_list=None, notes_list=None, expires_list=None, result_ttl_list=None))]
    #[allow(clippy::too_many_arguments)]
    pub fn enqueue_batch(
        &self,
        task_names: Vec<String>,
        payloads: Vec<Vec<u8>>,
        queues: Option<Vec<String>>,
        priorities: Option<Vec<Option<i32>>>,
        max_retries_list: Option<Vec<Option<i32>>>,
        timeouts: Option<Vec<Option<i64>>>,
        delay_seconds_list: Option<Vec<Option<f64>>>,
        unique_keys: Option<Vec<Option<String>>>,
        metadata_list: Option<Vec<Option<String>>>,
        notes_list: Option<Vec<Option<String>>>,
        expires_list: Option<Vec<Option<f64>>>,
        result_ttl_list: Option<Vec<Option<i64>>>,
    ) -> PyResult<Vec<PyJob>> {
        let count = task_names.len();
        if payloads.len() != count {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "task_names and payloads must have the same length",
            ));
        }

        let now = now_millis();
        let mut new_jobs = Vec::with_capacity(count);

        for i in 0..count {
            let delay = delay_seconds_list
                .as_ref()
                .and_then(|d| d.get(i).copied().flatten());
            let scheduled_at = match delay {
                Some(d) if d.is_finite() && d >= 0.0 => {
                    let delay_ms = (d * 1000.0) as i64;
                    now.saturating_add(delay_ms)
                }
                _ => now,
            };

            let expires_at = expires_list
                .as_ref()
                .and_then(|e| e.get(i).copied().flatten())
                .and_then(|e| {
                    if e.is_finite() && e >= 0.0 {
                        let ms = (e * 1000.0) as i64;
                        Some(now.saturating_add(ms))
                    } else {
                        None
                    }
                });

            let result_ttl_ms = result_ttl_list
                .as_ref()
                .and_then(|r| r.get(i).copied().flatten())
                .map(|s| s.saturating_mul(1000));

            new_jobs.push(NewJob {
                queue: queues.as_ref().map_or("default".to_string(), |q| {
                    q.get(i).cloned().unwrap_or_else(|| "default".to_string())
                }),
                task_name: task_names[i].clone(),
                payload: payloads[i].clone(),
                priority: priorities
                    .as_ref()
                    .and_then(|p| p.get(i).copied().flatten())
                    .unwrap_or(self.default_priority),
                scheduled_at,
                max_retries: max_retries_list
                    .as_ref()
                    .and_then(|r| r.get(i).copied().flatten())
                    .unwrap_or(self.default_retry),
                timeout_ms: {
                    let t = timeouts
                        .as_ref()
                        .and_then(|t| t.get(i).copied().flatten())
                        .unwrap_or(self.default_timeout);
                    t.checked_mul(1000).ok_or_else(|| {
                        pyo3::exceptions::PyValueError::new_err("timeout too large, would overflow")
                    })?
                },
                unique_key: unique_keys
                    .as_ref()
                    .and_then(|u| u.get(i).cloned().flatten()),
                metadata: metadata_list
                    .as_ref()
                    .and_then(|m| m.get(i).cloned().flatten()),
                notes: notes_list
                    .as_ref()
                    .and_then(|n| n.get(i).cloned().flatten()),
                depends_on: vec![],
                expires_at,
                result_ttl_ms,
                namespace: self.namespace.clone(),
                debounce_key: None,
            });
        }

        let jobs = self
            .storage
            .enqueue_batch(new_jobs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Ok(jobs.into_iter().map(PyJob::from).collect())
    }

    /// Get a job by ID.
    pub fn get_job(&self, job_id: &str) -> PyResult<Option<PyJob>> {
        let job = self
            .storage
            .get_job(job_id, self.namespace.as_deref())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(job.map(PyJob::from))
    }

    /// Cancel a pending job. Returns True if cancelled, False if not pending.
    pub fn cancel_job(&self, job_id: &str) -> PyResult<bool> {
        self.storage
            .cancel_job(job_id, self.namespace.as_deref())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Request cancellation of a running job. Returns True if cancel was requested.
    ///
    /// Sets the storage cancel flag and, if a worker pool is currently running,
    /// notifies it via a side channel so out-of-process pools (prefork) can
    /// observe the cancel without polling storage.
    pub fn request_cancel(&self, job_id: &str) -> PyResult<bool> {
        let requested = self
            .storage
            .request_cancel(job_id, self.namespace.as_deref())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        if requested {
            self.notify_dispatcher_cancel(job_id);
        }
        Ok(requested)
    }

    /// Check if cancellation has been requested for a job.
    pub fn is_cancel_requested(&self, job_id: &str) -> PyResult<bool> {
        self.storage
            .is_cancel_requested(job_id, self.namespace.as_deref())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Get the IDs of jobs that a given job depends on.
    pub fn get_dependencies(&self, job_id: &str) -> PyResult<Vec<String>> {
        self.storage
            .get_dependencies(job_id, self.namespace.as_deref())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Get the IDs of jobs that depend on a given job.
    pub fn get_dependents(&self, job_id: &str) -> PyResult<Vec<String>> {
        self.storage
            .get_dependents(job_id, self.namespace.as_deref())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Update progress for a running job (0-100).
    pub fn update_progress(&self, job_id: &str, progress: i32) -> PyResult<()> {
        if !(0..=100).contains(&progress) {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "progress must be between 0 and 100",
            ));
        }
        self.storage
            .update_progress(job_id, progress, self.namespace.as_deref())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Purge completed jobs older than given seconds ago. Returns count deleted.
    pub fn purge_completed(&self, older_than_seconds: i64) -> PyResult<u64> {
        let cutoff = now_millis().saturating_sub(older_than_seconds.saturating_mul(1000));
        self.storage
            .purge_completed(cutoff)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Get queue statistics.
    pub fn stats(&self) -> PyResult<Py<PyAny>> {
        let stats = self
            .storage
            .stats(self.namespace.as_deref())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Python::attach(|py| {
            let dict = PyDict::new(py);
            dict.set_item("pending", stats.pending)?;
            dict.set_item("running", stats.running)?;
            dict.set_item("completed", stats.completed)?;
            dict.set_item("failed", stats.failed)?;
            dict.set_item("dead", stats.dead)?;
            dict.set_item("cancelled", stats.cancelled)?;
            Ok(dict.into())
        })
    }

    /// Get queue statistics for a specific queue.
    pub fn stats_by_queue(&self, queue_name: &str) -> PyResult<Py<PyAny>> {
        let stats = self
            .storage
            .stats_by_queue(queue_name, self.namespace.as_deref())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Python::attach(|py| {
            let dict = PyDict::new(py);
            dict.set_item("pending", stats.pending)?;
            dict.set_item("running", stats.running)?;
            dict.set_item("completed", stats.completed)?;
            dict.set_item("failed", stats.failed)?;
            dict.set_item("dead", stats.dead)?;
            dict.set_item("cancelled", stats.cancelled)?;
            Ok(dict.into())
        })
    }

    /// Count pending jobs on a queue — the lean primitive behind the
    /// `max_pending` admission cap (avoids the full `stats_by_queue` breakdown).
    pub fn count_pending_by_queue(&self, queue_name: &str) -> PyResult<i64> {
        self.storage
            .count_pending_by_queue(queue_name)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Get queue statistics broken down by queue name.
    pub fn stats_all_queues(&self) -> PyResult<Py<PyAny>> {
        let all = self
            .storage
            .stats_all_queues(self.namespace.as_deref())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Python::attach(|py| {
            let outer = PyDict::new(py);
            for (queue, stats) in all {
                let dict = PyDict::new(py);
                dict.set_item("pending", stats.pending)?;
                dict.set_item("running", stats.running)?;
                dict.set_item("completed", stats.completed)?;
                dict.set_item("failed", stats.failed)?;
                dict.set_item("dead", stats.dead)?;
                dict.set_item("cancelled", stats.cancelled)?;
                outer.set_item(queue, dict)?;
            }
            Ok(outer.into())
        })
    }

    /// Pause a queue (no jobs will be dispatched from it).
    pub fn pause_queue(&self, queue_name: &str) -> PyResult<()> {
        self.storage
            .pause_queue(queue_name)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Resume a paused queue.
    pub fn resume_queue(&self, queue_name: &str) -> PyResult<()> {
        self.storage
            .resume_queue(queue_name)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// List paused queues.
    pub fn list_paused_queues(&self) -> PyResult<Vec<String>> {
        self.storage
            .list_paused_queues()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Cancel all pending jobs in a queue. Returns count cancelled.
    pub fn purge_queue(&self, queue_name: &str) -> PyResult<u64> {
        self.storage
            .cancel_pending_by_queue(queue_name)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Get a single dashboard setting value, or ``None`` if unset.
    pub fn get_setting(&self, key: &str) -> PyResult<Option<String>> {
        self.storage
            .get_setting(key)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Set a dashboard setting (insert or update).
    pub fn set_setting(&self, key: &str, value: &str) -> PyResult<()> {
        self.storage
            .set_setting(key, value)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Set a dashboard setting only if it still holds ``expected``, where
    /// ``None`` means the key must be unset. Returns ``False`` when another
    /// writer got there first, so a read-modify-write caller can re-read and
    /// retry instead of overwriting the edit it never saw.
    #[pyo3(signature = (key, expected, value))]
    pub fn set_setting_if(&self, key: &str, expected: Option<&str>, value: &str) -> PyResult<bool> {
        self.storage
            .set_setting_if(key, expected, value)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Apply any pending schema changes and report what ran, as a dict:
    /// ``applied``, ``workflow_applied``, ``archived_jobs``, ``schemaless``.
    /// Idempotent — a current database reports empty lists.
    pub fn migrate(&self, py: Python<'_>) -> PyResult<pyo3::Py<pyo3::types::PyDict>> {
        let report = py
            .detach(|| self.storage.migrate())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        #[cfg(feature = "workflows")]
        let workflow_applied = workflow_ops::migrate_workflow_storage(self, py)?;
        #[cfg(not(feature = "workflows"))]
        let workflow_applied: Vec<String> = Vec::new();

        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("applied", report.applied)?;
        dict.set_item("workflow_applied", workflow_applied)?;
        dict.set_item("archived_jobs", report.archived_jobs)?;
        dict.set_item("schemaless", report.schemaless)?;
        Ok(dict.unbind())
    }

    /// Whether the core schema has been applied. A catalog read, never DDL, so
    /// a queue that gates its own migrations can still tell an empty database
    /// apart from one another process already migrated.
    pub fn is_migrated(&self, py: Python<'_>) -> PyResult<bool> {
        py.detach(|| self.storage.is_migrated())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// The lowest contract level a process may speak and still open this
    /// storage. See ``BINDING_CONTRACT.md``.
    pub fn min_contract(&self) -> PyResult<u32> {
        flexiq_core::min_contract(&self.storage)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Raise or lower that floor. Rejects a level this build does not itself
    /// speak, which would lock the caller out of its own storage.
    pub fn set_min_contract(&self, level: u32) -> PyResult<()> {
        flexiq_core::set_min_contract(&self.storage, level)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    /// Delete a dashboard setting. Returns ``True`` if the key existed.
    pub fn delete_setting(&self, key: &str) -> PyResult<bool> {
        self.storage
            .delete_setting(key)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// The retention windows the elected cleaner last published for this
    /// queue's namespace, as a JSON document, or ``None`` if no worker has
    /// swept yet. See ``BINDING_CONTRACT.md``.
    pub fn effective_retention(&self) -> PyResult<Option<String>> {
        flexiq_core::scheduler::retention::read_effective_retention_json(
            &self.storage,
            self.namespace.as_deref(),
        )
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Count what a retention purge would delete right now, without deleting
    /// anything. Returns the preview as a JSON document. See
    /// ``BINDING_CONTRACT.md``.
    ///
    /// With no `retention` argument the preview uses this queue's configured (or
    /// default-recommended) windows. Passing candidate windows previews those
    /// instead — so an operator can size a window before setting it, without
    /// reconfiguring a worker. An explicit empty map previews a disabled policy.
    #[pyo3(signature = (retention=None))]
    pub fn dry_run_retention(
        &self,
        py: Python<'_>,
        retention: Option<std::collections::HashMap<String, i64>>,
    ) -> PyResult<String> {
        // Candidate windows override the queue's own config and the legacy TTL,
        // so the preview reflects exactly what was asked about; `None` falls back
        // to what a worker would actually apply for this queue.
        let (config, result_ttl_ms) = match retention {
            Some(map) => (build_retention_config(Some(map))?, None),
            None => (self.retention.clone(), self.result_ttl_ms),
        };
        let storage = &self.storage;
        let namespace = self.namespace.as_deref();
        // Release the GIL: the counts scan every history table.
        py.detach(|| {
            flexiq_core::scheduler::retention::dry_run_json(
                storage,
                config.as_ref(),
                result_ttl_ms,
                namespace,
                flexiq_core::job::now_millis(),
            )
        })
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Return all dashboard settings as a ``{key: value}`` dict.
    pub fn list_settings(&self) -> PyResult<std::collections::HashMap<String, String>> {
        self.storage
            .list_settings()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Cancel all pending jobs for a task name. Returns count cancelled.
    pub fn revoke_task(&self, task_name: &str) -> PyResult<u64> {
        self.storage
            .cancel_pending_by_task(task_name)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Archive completed/dead/cancelled jobs older than cutoff (seconds ago).
    pub fn archive_old_jobs(&self, older_than_seconds: i64) -> PyResult<u64> {
        let cutoff = now_millis().saturating_sub(older_than_seconds.saturating_mul(1000));
        self.storage
            .archive_old_jobs(cutoff)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// List archived jobs with pagination.
    #[pyo3(signature = (limit=50, offset=0))]
    pub fn list_archived(&self, limit: i64, offset: i64) -> PyResult<Vec<PyJob>> {
        if limit < 0 || offset < 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "limit and offset must be non-negative",
            ));
        }
        let jobs = self
            .storage
            .list_archived(limit, offset, self.namespace.as_deref())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(jobs.into_iter().map(PyJob::from).collect())
    }

    /// Keyset-paginated `list_archived`, ordered by completed time. Returns
    /// `(jobs, next_cursor)`; pass `next_cursor` back as `after`.
    #[pyo3(signature = (limit=50, after=None))]
    pub fn list_archived_after(
        &self,
        limit: i64,
        after: Option<&str>,
    ) -> PyResult<(Vec<PyJob>, Option<String>)> {
        if limit < 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "limit must be non-negative",
            ));
        }
        let cursor = after
            .map(flexiq_core::storage::cursor::decode_cursor)
            .transpose()
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        let jobs = self
            .storage
            .list_archived_after(limit, cursor, self.namespace.as_deref())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        // Archived rows always have `completed_at`; fall back to created_at.
        let next = next_cursor(&jobs, limit, |j| {
            (j.completed_at.unwrap_or(j.created_at), &j.id)
        });
        Ok((jobs.into_iter().map(PyJob::from).collect(), next))
    }

    /// Register a periodic task schedule.
    #[pyo3(signature = (name, task_name, cron_expr, args=None, kwargs=None, queue="default", timezone=None))]
    pub fn register_periodic(
        &self,
        name: &str,
        task_name: &str,
        cron_expr: &str,
        args: Option<Vec<u8>>,
        kwargs: Option<Vec<u8>>,
        queue: &str,
        timezone: Option<&str>,
    ) -> PyResult<()> {
        use flexiq_core::periodic::next_cron_time_tz;

        let now = now_millis();
        let next_run = if let Some(tz) = timezone {
            next_cron_time_tz(cron_expr, now, tz)
        } else {
            next_cron_time(cron_expr, now)
        }
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

        let row = NewPeriodicTask {
            name: name.to_string(),
            task_name: task_name.to_string(),
            cron_expr: cron_expr.to_string(),
            args,
            kwargs,
            queue: queue.to_string(),
            enabled: true,
            next_run,
            timezone: timezone.map(str::to_string),
        };

        self.storage
            .register_periodic(&row)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// List all registered periodic tasks, enabled or paused. Omits the opaque
    /// args/kwargs payloads; `last_run`/`next_run` are Unix milliseconds.
    pub fn list_periodic(&self) -> PyResult<Vec<Py<PyAny>>> {
        let rows = self
            .storage
            .list_periodic()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Python::attach(|py| {
            let mut result = Vec::with_capacity(rows.len());
            for row in rows {
                let dict = PyDict::new(py);
                dict.set_item("name", row.name)?;
                dict.set_item("task_name", row.task_name)?;
                dict.set_item("cron_expr", row.cron_expr)?;
                dict.set_item("queue", row.queue)?;
                dict.set_item("enabled", row.enabled)?;
                dict.set_item("last_run", row.last_run)?;
                dict.set_item("next_run", row.next_run)?;
                dict.set_item("timezone", row.timezone)?;
                result.push(dict.into());
            }
            Ok(result)
        })
    }

    /// Remove a periodic task. Returns false if no task had that name.
    pub fn delete_periodic(&self, name: &str) -> PyResult<bool> {
        self.storage
            .delete_periodic(name)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Pause a periodic task so it stops firing. Returns false if no task had that name.
    pub fn pause_periodic(&self, name: &str) -> PyResult<bool> {
        self.storage
            .set_periodic_enabled(name, false)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Resume a paused periodic task. Returns false if no task had that name.
    pub fn resume_periodic(&self, name: &str) -> PyResult<bool> {
        self.storage
            .set_periodic_enabled(name, true)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    pub fn __repr__(&self) -> String {
        format!(
            "PyQueue(db_path='{}', workers={})",
            self.db_path, self.num_workers
        )
    }
}

/// The claim owner this process was spawned with, if any.
///
/// Set by a prefork parent on the child it spawns, which is the one hop where
/// the owner may travel: the parent won the claim and the pipe is private to
/// the pair. It is deliberately **not** a field on the dispatch frame — that
/// frame also crosses a socket to an attached executor, and an owner an
/// executor fills in is an owner it can forge.
fn inherited_claim_owner() -> Option<String> {
    std::env::var(CLAIM_OWNER_ENV)
        .ok()
        .filter(|owner| !owner.is_empty())
}

impl PyQueue {
    /// Record the worker id the scheduler claims execution under, so a step
    /// write from a task running here is fenced on the same owner.
    pub(crate) fn set_claim_owner(&self, owner: Option<String>) {
        if let Ok(mut guard) = self.claim_owner.lock() {
            *guard = owner;
        }
    }

    /// Install the active worker dispatcher. Called by `run_worker` before
    /// the dispatch loop starts so `request_cancel` can deliver out-of-band
    /// cancel signals to the running pool. Pass `None` on shutdown.
    pub(crate) fn set_dispatcher(&self, dispatcher: Option<Arc<dyn WorkerDispatcher>>) {
        if let Ok(mut guard) = self.dispatcher.lock() {
            *guard = dispatcher;
        }
    }

    fn notify_dispatcher_cancel(&self, job_id: &str) {
        let dispatcher = match self.dispatcher.lock() {
            Ok(guard) => guard.clone(),
            Err(_) => return,
        };
        if let Some(d) = dispatcher {
            d.notify_cancel(job_id);
        }
    }
}
