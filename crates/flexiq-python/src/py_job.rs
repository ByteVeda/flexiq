use pyo3::prelude::*;

use flexiq_core::job::{Job, JobStatus};

/// Python-visible handle to a queued job.
#[pyclass(from_py_object)]
#[derive(Debug, Clone)]
pub struct PyJob {
    #[pyo3(get)]
    pub id: String,
    #[pyo3(get)]
    pub queue: String,
    #[pyo3(get)]
    pub task_name: String,
    #[pyo3(get)]
    pub priority: i32,
    #[pyo3(get)]
    pub retry_count: i32,
    #[pyo3(get)]
    pub max_retries: i32,
    #[pyo3(get)]
    pub created_at: i64,
    #[pyo3(get)]
    pub scheduled_at: i64,
    #[pyo3(get)]
    pub started_at: Option<i64>,
    #[pyo3(get)]
    pub completed_at: Option<i64>,
    #[pyo3(get)]
    pub error: Option<String>,
    #[pyo3(get)]
    pub timeout_ms: i64,
    #[pyo3(get)]
    pub unique_key: Option<String>,
    #[pyo3(get)]
    pub progress: Option<i32>,
    #[pyo3(get)]
    pub metadata: Option<String>,
    /// Pre-encoded canonical JSON for structured notes (≤ 15 fields).
    /// Python wraps this with `json.loads` via `JobResult.notes`.
    #[pyo3(get)]
    pub notes: Option<String>,
    #[pyo3(get)]
    pub namespace: Option<String>,

    status_val: i32,
    payload_bytes: Vec<u8>,
    result_bytes: Option<Vec<u8>>,
}

#[pymethods]
impl PyJob {
    #[getter]
    pub fn status(&self) -> &str {
        JobStatus::from_i32(self.status_val)
            .unwrap_or(JobStatus::Pending)
            .as_str()
    }

    /// The serialized ``(args, kwargs)`` payload that was enqueued for this
    /// job. Use the queue's per-task serializer to decode.
    #[getter]
    pub fn payload_bytes(&self) -> &[u8] {
        &self.payload_bytes
    }

    #[getter]
    pub fn result_bytes(&self) -> Option<&[u8]> {
        self.result_bytes.as_deref()
    }

    pub fn __repr__(&self) -> String {
        format!(
            "PyJob(id={}, task={}, status={}, priority={})",
            self.id,
            self.task_name,
            self.status(),
            self.priority
        )
    }
}

impl From<Job> for PyJob {
    fn from(job: Job) -> Self {
        Self {
            id: job.id,
            queue: job.queue,
            task_name: job.task_name,
            priority: job.priority,
            retry_count: job.retry_count,
            max_retries: job.max_retries,
            created_at: job.created_at,
            scheduled_at: job.scheduled_at,
            started_at: job.started_at,
            completed_at: job.completed_at,
            error: job.error,
            timeout_ms: job.timeout_ms,
            unique_key: job.unique_key,
            progress: job.progress,
            metadata: job.metadata,
            notes: job.notes,
            namespace: job.namespace,
            status_val: job.status as i32,
            payload_bytes: job.payload,
            result_bytes: job.result,
        }
    }
}
