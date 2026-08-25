use crossbeam_channel::{Sender, TrySendError};
use pyo3::prelude::*;

use flexiq_core::scheduler::JobResult;

/// Python-accessible sender for reporting task results back to the Rust scheduler.
///
/// The `AsyncTaskExecutor` on the Python side calls `try_report_success`,
/// `try_report_failure`, or `try_report_cancelled` to push `JobResult` values into the
/// same crossbeam channel that sync tasks use.
///
/// Async callers must use the `try_report_*` variants. The blocking `report_*` methods
/// park the caller when the channel is full while still holding the GIL — fatal on the
/// executor's single event-loop thread, because the result drain loop re-acquires the GIL
/// each iteration and could never drain the channel that the sender is waiting on.
#[pyclass]
pub struct PyResultSender {
    tx: Sender<JobResult>,
}

impl PyResultSender {
    pub fn new(tx: Sender<JobResult>) -> Self {
        Self { tx }
    }

    /// `true` when the result was handed off (or the channel is gone, so retrying is
    /// futile); `false` only when the channel is full and the caller should back off.
    fn try_hand_off(&self, result: JobResult) -> bool {
        !matches!(self.tx.try_send(result), Err(TrySendError::Full(_)))
    }
}

#[pymethods]
impl PyResultSender {
    /// Report a successful task execution. `false` means "channel full, retry".
    #[pyo3(signature = (job_id, task_name, result, wall_time_ns))]
    fn try_report_success(
        &self,
        job_id: String,
        task_name: String,
        result: Option<Vec<u8>>,
        wall_time_ns: i64,
    ) -> bool {
        self.try_hand_off(JobResult::Success {
            job_id,
            result,
            task_name,
            wall_time_ns,
        })
    }

    /// Report a failed task execution. `false` means "channel full, retry".
    #[pyo3(signature = (job_id, task_name, error, retry_count, max_retries, wall_time_ns, should_retry))]
    #[allow(clippy::too_many_arguments)]
    fn try_report_failure(
        &self,
        job_id: String,
        task_name: String,
        error: String,
        retry_count: i32,
        max_retries: i32,
        wall_time_ns: i64,
        should_retry: bool,
    ) -> bool {
        self.try_hand_off(JobResult::Failure {
            job_id,
            error,
            retry_count,
            max_retries,
            task_name,
            wall_time_ns,
            should_retry,
            timed_out: false,
        })
    }

    /// Report a cancelled task. `false` means "channel full, retry".
    #[pyo3(signature = (job_id, task_name, wall_time_ns))]
    fn try_report_cancelled(&self, job_id: String, task_name: String, wall_time_ns: i64) -> bool {
        self.try_hand_off(JobResult::Cancelled {
            job_id,
            task_name,
            wall_time_ns,
        })
    }

    /// Report an attempt that ended in a `step.sleep`.
    ///
    /// Not a success and not a failure: by the time this is sent the sleep row
    /// is committed, the claim released and the job already `Pending` at
    /// `wake_at`. `wake_at` is the deadline storage settled on, which on a
    /// replay is not the one this attempt proposed.
    #[pyo3(signature = (job_id, task_name, wake_at, wall_time_ns))]
    fn try_report_slept(
        &self,
        job_id: String,
        task_name: String,
        wake_at: i64,
        wall_time_ns: i64,
    ) -> bool {
        self.try_hand_off(JobResult::Slept {
            job_id,
            task_name,
            wake_at,
            wall_time_ns,
        })
    }
}
