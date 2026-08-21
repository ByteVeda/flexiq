use pyo3::prelude::*;
use pyo3::types::PyDict;

use flexiq_core::job::{now_millis, NewJob};
use flexiq_core::storage::Storage;

use super::{next_cursor, PyQueue};
use crate::py_job::PyJob;

#[pymethods]
#[allow(clippy::useless_conversion)]
impl PyQueue {
    /// List jobs with optional filters and pagination.
    #[pyo3(signature = (status=None, queue=None, task_name=None, limit=50, offset=0, namespace=None))]
    pub fn list_jobs(
        &self,
        status: Option<&str>,
        queue: Option<&str>,
        task_name: Option<&str>,
        limit: i64,
        offset: i64,
        namespace: Option<&str>,
    ) -> PyResult<Vec<PyJob>> {
        if limit < 0 || offset < 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "limit and offset must be non-negative",
            ));
        }
        let status_int = parse_status(status)?;

        let jobs = self
            .storage
            .list_jobs(status_int, queue, task_name, limit, offset, namespace)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Ok(jobs.into_iter().map(PyJob::from).collect())
    }

    /// Keyset-paginated `list_jobs`. Returns `(jobs, next_cursor)`; pass
    /// `next_cursor` back as `after` for the following page (`None` when the
    /// page was the last). O(page) at any depth, stable under inserts.
    #[pyo3(signature = (status=None, queue=None, task_name=None, limit=50, after=None, namespace=None))]
    pub fn list_jobs_after(
        &self,
        status: Option<&str>,
        queue: Option<&str>,
        task_name: Option<&str>,
        limit: i64,
        after: Option<&str>,
        namespace: Option<&str>,
    ) -> PyResult<(Vec<PyJob>, Option<String>)> {
        if limit < 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "limit must be non-negative",
            ));
        }
        let status_int = parse_status(status)?;
        let cursor = decode_after(after)?;

        let jobs = self
            .storage
            .list_jobs_after(status_int, queue, task_name, limit, cursor, namespace)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        let next = next_cursor(&jobs, limit, |j| (j.created_at, &j.id));
        Ok((jobs.into_iter().map(PyJob::from).collect(), next))
    }

    /// List jobs with extended filters.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (status=None, queue=None, task_name=None, metadata_like=None, error_like=None, created_after=None, created_before=None, limit=50, offset=0, namespace=None))]
    pub fn list_jobs_filtered(
        &self,
        status: Option<&str>,
        queue: Option<&str>,
        task_name: Option<&str>,
        metadata_like: Option<&str>,
        error_like: Option<&str>,
        created_after: Option<i64>,
        created_before: Option<i64>,
        limit: i64,
        offset: i64,
        namespace: Option<&str>,
    ) -> PyResult<Vec<PyJob>> {
        if limit < 0 || offset < 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "limit and offset must be non-negative",
            ));
        }
        let status_int = parse_status(status)?;

        let jobs = self
            .storage
            .list_jobs_filtered(
                status_int,
                queue,
                task_name,
                metadata_like,
                error_like,
                created_after,
                created_before,
                limit,
                offset,
                namespace,
            )
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Ok(jobs.into_iter().map(PyJob::from).collect())
    }

    /// Keyset-paginated `list_jobs_filtered`. Returns `(jobs, next_cursor)`.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (status=None, queue=None, task_name=None, metadata_like=None, error_like=None, created_after=None, created_before=None, limit=50, after=None, namespace=None))]
    pub fn list_jobs_filtered_after(
        &self,
        status: Option<&str>,
        queue: Option<&str>,
        task_name: Option<&str>,
        metadata_like: Option<&str>,
        error_like: Option<&str>,
        created_after: Option<i64>,
        created_before: Option<i64>,
        limit: i64,
        after: Option<&str>,
        namespace: Option<&str>,
    ) -> PyResult<(Vec<PyJob>, Option<String>)> {
        if limit < 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "limit must be non-negative",
            ));
        }
        let status_int = parse_status(status)?;
        let cursor = decode_after(after)?;

        let jobs = self
            .storage
            .list_jobs_filtered_after(
                status_int,
                queue,
                task_name,
                metadata_like,
                error_like,
                created_after,
                created_before,
                limit,
                cursor,
                namespace,
            )
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        let next = next_cursor(&jobs, limit, |j| (j.created_at, &j.id));
        Ok((jobs.into_iter().map(PyJob::from).collect(), next))
    }

    /// List dead letter queue entries.
    #[pyo3(signature = (limit=10, offset=0))]
    pub fn dead_letters(&self, limit: i64, offset: i64) -> PyResult<Vec<Py<PyAny>>> {
        if limit < 0 || offset < 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "limit and offset must be non-negative",
            ));
        }
        let dead = self
            .storage
            .list_dead(limit, offset, self.namespace.as_deref())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Python::attach(|py| {
            let mut result = Vec::with_capacity(dead.len());
            for d in dead {
                let dict = PyDict::new(py);
                dict.set_item("id", d.id)?;
                dict.set_item("original_job_id", d.original_job_id)?;
                dict.set_item("queue", d.queue)?;
                dict.set_item("task_name", d.task_name)?;
                dict.set_item("error", d.error)?;
                dict.set_item("retry_count", d.retry_count)?;
                dict.set_item("failed_at", d.failed_at)?;
                dict.set_item("metadata", d.metadata)?;
                dict.set_item("dlq_retry_count", d.dlq_retry_count)?;
                result.push(dict.into());
            }
            Ok(result)
        })
    }

    /// Keyset-paginated `dead_letters`. Returns `(entries, next_cursor)`.
    #[pyo3(signature = (limit=10, after=None))]
    pub fn dead_letters_after(
        &self,
        limit: i64,
        after: Option<&str>,
    ) -> PyResult<(Vec<Py<PyAny>>, Option<String>)> {
        if limit < 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "limit must be non-negative",
            ));
        }
        let cursor = decode_after(after)?;
        let dead = self
            .storage
            .list_dead_after(limit, cursor, self.namespace.as_deref())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        let next = next_cursor(&dead, limit, |d| (d.failed_at, &d.id));

        Python::attach(|py| {
            let mut result = Vec::with_capacity(dead.len());
            for d in dead {
                let dict = PyDict::new(py);
                dict.set_item("id", d.id)?;
                dict.set_item("original_job_id", d.original_job_id)?;
                dict.set_item("queue", d.queue)?;
                dict.set_item("task_name", d.task_name)?;
                dict.set_item("error", d.error)?;
                dict.set_item("retry_count", d.retry_count)?;
                dict.set_item("failed_at", d.failed_at)?;
                dict.set_item("metadata", d.metadata)?;
                dict.set_item("dlq_retry_count", d.dlq_retry_count)?;
                result.push(dict.into());
            }
            Ok((result, next))
        })
    }

    /// List dead letter queue entries for a single task, newest first.
    #[pyo3(signature = (task_name, limit=10, offset=0))]
    pub fn dead_letters_by_task(
        &self,
        task_name: &str,
        limit: i64,
        offset: i64,
    ) -> PyResult<Vec<Py<PyAny>>> {
        if limit < 0 || offset < 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "limit and offset must be non-negative",
            ));
        }
        let dead = self
            .storage
            .list_dead_by_task(task_name, limit, offset, self.namespace.as_deref())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Python::attach(|py| {
            let mut result = Vec::with_capacity(dead.len());
            for d in dead {
                let dict = PyDict::new(py);
                dict.set_item("id", d.id)?;
                dict.set_item("original_job_id", d.original_job_id)?;
                dict.set_item("queue", d.queue)?;
                dict.set_item("task_name", d.task_name)?;
                dict.set_item("error", d.error)?;
                dict.set_item("retry_count", d.retry_count)?;
                dict.set_item("failed_at", d.failed_at)?;
                dict.set_item("metadata", d.metadata)?;
                dict.set_item("dlq_retry_count", d.dlq_retry_count)?;
                result.push(dict.into());
            }
            Ok(result)
        })
    }

    /// Purge every dead letter entry for a task. Returns the count removed.
    pub fn purge_dead_by_task(&self, task_name: &str) -> PyResult<u64> {
        self.storage
            .purge_dead_by_task(task_name)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Re-enqueue a dead letter job. Returns new job ID.
    pub fn retry_dead(&self, dead_id: &str) -> PyResult<String> {
        self.storage
            .retry_dead(dead_id, self.namespace.as_deref())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Force a stuck Running job back to Pending, releasing its execution
    /// claim. Returns false when the job is missing or not Running.
    pub fn requeue_job(&self, job_id: &str) -> PyResult<bool> {
        self.storage
            .requeue_stuck(job_id, now_millis())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Purge dead letter entries older than given seconds ago.
    pub fn purge_dead(&self, older_than_seconds: i64) -> PyResult<u64> {
        let cutoff = now_millis().saturating_sub(older_than_seconds.saturating_mul(1000));
        self.storage
            .purge_dead(cutoff)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Delete a single dead letter entry without re-enqueueing.
    pub fn delete_dead(&self, dead_id: &str) -> PyResult<bool> {
        self.storage
            .delete_dead(dead_id, self.namespace.as_deref())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Get error history for a job.
    pub fn get_job_errors(&self, job_id: &str) -> PyResult<Vec<Py<PyAny>>> {
        let errors = self
            .storage
            .get_job_errors(job_id, self.namespace.as_deref())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Python::attach(|py| {
            let mut result = Vec::with_capacity(errors.len());
            for err in errors {
                let dict = PyDict::new(py);
                dict.set_item("id", err.id)?;
                dict.set_item("job_id", err.job_id)?;
                dict.set_item("attempt", err.attempt)?;
                dict.set_item("error", err.error)?;
                dict.set_item("failed_at", err.failed_at)?;
                result.push(dict.into());
            }
            Ok(result)
        })
    }

    // ── Metrics API ────────────────────────────────────────────────

    /// Get raw metric records for a task (or all tasks).
    #[pyo3(signature = (task_name=None, since_seconds=3600))]
    pub fn get_metrics(
        &self,
        task_name: Option<&str>,
        since_seconds: i64,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let since_ms = now_millis().saturating_sub(since_seconds.saturating_mul(1000));
        let rows = self
            .storage
            .get_metrics(task_name, since_ms, self.namespace.as_deref())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Python::attach(|py| {
            let mut result = Vec::with_capacity(rows.len());
            for r in rows {
                let dict = PyDict::new(py);
                dict.set_item("id", r.id)?;
                dict.set_item("task_name", r.task_name)?;
                dict.set_item("job_id", r.job_id)?;
                dict.set_item("wall_time_ns", r.wall_time_ns)?;
                dict.set_item("memory_bytes", r.memory_bytes)?;
                dict.set_item("succeeded", r.succeeded)?;
                dict.set_item("recorded_at", r.recorded_at)?;
                result.push(dict.into());
            }
            Ok(result)
        })
    }

    // ── Replay API ───────────────────────────────────────────────

    /// Create a replay of an existing job (re-enqueue with same payload).
    pub fn replay_job(&self, job_id: &str) -> PyResult<String> {
        let original = self
            .storage
            .get_job(job_id, self.namespace.as_deref())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
            .ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(format!("Job not found: {job_id}"))
            })?;

        let new_job = NewJob {
            queue: original.queue,
            task_name: original.task_name,
            payload: original.payload,
            priority: original.priority,
            scheduled_at: now_millis(),
            max_retries: original.max_retries,
            timeout_ms: original.timeout_ms,
            unique_key: None,
            metadata: Some(format!("{{\"replayed_from\":\"{job_id}\"}}")),
            notes: original.notes,
            depends_on: vec![],
            expires_at: None,
            result_ttl_ms: original.result_ttl_ms,
            namespace: original.namespace,
            debounce_key: None,
        };

        let job = self
            .storage
            .enqueue(new_job)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        let _ = self.storage.record_replay(
            job_id,
            &job.id,
            original.result.as_deref(),
            None,
            original.error.as_deref(),
            None,
        );

        Ok(job.id)
    }

    /// Get replay history for a job.
    pub fn get_replay_history(&self, job_id: &str) -> PyResult<Vec<Py<PyAny>>> {
        let rows = self
            .storage
            .get_replay_history(job_id)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Python::attach(|py| {
            let mut result = Vec::with_capacity(rows.len());
            for r in rows {
                let dict = PyDict::new(py);
                dict.set_item("id", r.id)?;
                dict.set_item("original_job_id", r.original_job_id)?;
                dict.set_item("replay_job_id", r.replay_job_id)?;
                dict.set_item("replayed_at", r.replayed_at)?;
                dict.set_item("original_error", r.original_error)?;
                dict.set_item("replay_error", r.replay_error)?;
                result.push(dict.into());
            }
            Ok(result)
        })
    }

    // ── Task Logs API ────────────────────────────────────────────

    /// Write a structured log entry for a task.
    #[pyo3(signature = (job_id, task_name, level, message, extra=None))]
    pub fn write_task_log(
        &self,
        job_id: &str,
        task_name: &str,
        level: &str,
        message: &str,
        extra: Option<&str>,
    ) -> PyResult<()> {
        self.storage
            .write_task_log(
                job_id,
                task_name,
                level,
                message,
                extra,
                self.namespace.as_deref(),
            )
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Get logs for a specific job.
    pub fn get_task_logs(&self, job_id: &str) -> PyResult<Vec<Py<PyAny>>> {
        let rows = self
            .storage
            .get_task_logs(job_id, self.namespace.as_deref())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Python::attach(|py| {
            let mut result = Vec::with_capacity(rows.len());
            for r in rows {
                let dict = PyDict::new(py);
                dict.set_item("id", r.id)?;
                dict.set_item("job_id", r.job_id)?;
                dict.set_item("task_name", r.task_name)?;
                dict.set_item("level", r.level)?;
                dict.set_item("message", r.message)?;
                dict.set_item("extra", r.extra)?;
                dict.set_item("logged_at", r.logged_at)?;
                result.push(dict.into());
            }
            Ok(result)
        })
    }

    /// Query logs with filters.
    #[pyo3(signature = (task_name=None, level=None, since_seconds=3600, limit=100))]
    pub fn query_task_logs(
        &self,
        task_name: Option<&str>,
        level: Option<&str>,
        since_seconds: i64,
        limit: i64,
    ) -> PyResult<Vec<Py<PyAny>>> {
        if limit < 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "limit must be non-negative",
            ));
        }
        let since_ms = now_millis().saturating_sub(since_seconds.saturating_mul(1000));
        let rows = self
            .storage
            .query_task_logs(task_name, level, since_ms, limit, self.namespace.as_deref())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Python::attach(|py| {
            let mut result = Vec::with_capacity(rows.len());
            for r in rows {
                let dict = PyDict::new(py);
                dict.set_item("id", r.id)?;
                dict.set_item("job_id", r.job_id)?;
                dict.set_item("task_name", r.task_name)?;
                dict.set_item("level", r.level)?;
                dict.set_item("message", r.message)?;
                dict.set_item("extra", r.extra)?;
                dict.set_item("logged_at", r.logged_at)?;
                result.push(dict.into());
            }
            Ok(result)
        })
    }

    // ── Circuit Breaker API ──────────────────────────────────────

    /// List all circuit breaker states.
    pub fn list_circuit_breakers(&self) -> PyResult<Vec<Py<PyAny>>> {
        let rows = self
            .storage
            .list_circuit_breakers()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Python::attach(|py| {
            let mut result = Vec::with_capacity(rows.len());
            for r in rows {
                let dict = PyDict::new(py);
                let state_str = match r.state {
                    1 => "open",
                    2 => "half_open",
                    _ => "closed",
                };
                dict.set_item("task_name", r.task_name)?;
                dict.set_item("state", state_str)?;
                dict.set_item("failure_count", r.failure_count)?;
                dict.set_item("last_failure_at", r.last_failure_at)?;
                dict.set_item("opened_at", r.opened_at)?;
                dict.set_item("threshold", r.threshold)?;
                dict.set_item("window_ms", r.window_ms)?;
                dict.set_item("cooldown_ms", r.cooldown_ms)?;
                result.push(dict.into());
            }
            Ok(result)
        })
    }

    // ── Worker API ───────────────────────────────────────────────

    /// List all registered workers.
    pub fn list_workers(&self) -> PyResult<Vec<Py<PyAny>>> {
        let rows = self
            .storage
            .list_workers()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Python::attach(|py| {
            let mut result = Vec::with_capacity(rows.len());
            for r in rows {
                let dict = PyDict::new(py);
                dict.set_item("worker_id", r.worker_id)?;
                dict.set_item("last_heartbeat", r.last_heartbeat)?;
                dict.set_item("queues", r.queues)?;
                dict.set_item("status", r.status)?;
                dict.set_item("tags", r.tags)?;
                dict.set_item("resources", r.resources)?;
                dict.set_item("resource_health", r.resource_health)?;
                dict.set_item("threads", r.threads)?;
                dict.set_item("started_at", r.started_at)?;
                dict.set_item("hostname", r.hostname)?;
                dict.set_item("pid", r.pid)?;
                dict.set_item("pool_type", r.pool_type)?;
                dict.set_item("sdk", r.sdk)?;
                dict.set_item("sdk_version", r.sdk_version)?;
                dict.set_item("registry_fingerprint", r.registry_fingerprint)?;
                result.push(dict.into());
            }
            Ok(result)
        })
    }

    // ── Distributed Locking API ──────────────────────────────────

    /// Acquire a distributed lock. Returns True if acquired.
    #[pyo3(signature = (lock_name, owner_id, ttl_ms=30000))]
    pub fn acquire_lock(&self, lock_name: &str, owner_id: &str, ttl_ms: i64) -> PyResult<bool> {
        if ttl_ms <= 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "ttl_ms must be positive",
            ));
        }
        self.storage
            .acquire_lock(lock_name, owner_id, ttl_ms)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Release a distributed lock. Returns True if released.
    pub fn release_lock(&self, lock_name: &str, owner_id: &str) -> PyResult<bool> {
        self.storage
            .release_lock(lock_name, owner_id)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Extend a lock's TTL. Returns True if extended.
    #[pyo3(signature = (lock_name, owner_id, ttl_ms=30000))]
    pub fn extend_lock(&self, lock_name: &str, owner_id: &str, ttl_ms: i64) -> PyResult<bool> {
        if ttl_ms <= 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "ttl_ms must be positive",
            ));
        }
        self.storage
            .extend_lock(lock_name, owner_id, ttl_ms)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Get info about a lock. Returns dict or None.
    pub fn get_lock_info(&self, lock_name: &str) -> PyResult<Option<Py<PyAny>>> {
        let info = self
            .storage
            .get_lock_info(lock_name)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        match info {
            Some(lock) => Python::attach(|py| {
                let dict = PyDict::new(py);
                dict.set_item("lock_name", lock.lock_name)?;
                dict.set_item("owner_id", lock.owner_id)?;
                dict.set_item("acquired_at", lock.acquired_at)?;
                dict.set_item("expires_at", lock.expires_at)?;
                Ok(Some(dict.into()))
            }),
            None => Ok(None),
        }
    }
}

/// Decode an opaque `after` cursor string into a `(sort_key, id)` keyset bound.
fn decode_after(after: Option<&str>) -> PyResult<Option<(i64, &str)>> {
    after
        .map(flexiq_core::storage::cursor::decode_cursor)
        .transpose()
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

fn parse_status(status: Option<&str>) -> PyResult<Option<i32>> {
    match status {
        Some(s) => Ok(Some(match s {
            "pending" => 0,
            "running" => 1,
            "complete" | "completed" => 2,
            "failed" => 3,
            "dead" => 4,
            "cancelled" => 5,
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Invalid status: {s}. Use: pending, running, complete, failed, dead, cancelled"
                )))
            }
        })),
        None => Ok(None),
    }
}
