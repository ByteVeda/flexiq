//! PyO3 entry points for the saga orchestrator (Python-side).
//!
//! The orchestrator lives in `sdks/python/flexiq/workflows/saga/orchestrator.py`
//! and drives reverse-order compensation. It needs to:
//!
//! - Transition the workflow run's state to `Compensating`, then to one
//!   of `Compensated` / `CompensationFailed` when the saga finishes.
//! - Mark individual nodes as `Compensating` (with the comp job id),
//!   `Compensated`, or `CompensationFailed`.
//!
//! Each of those operations maps directly to a `WorkflowStorage` trait
//! method added in the saga-foundations PR; this file exposes them as
//! Python-callable `PyQueue` methods.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use flexiq_workflows::{WorkflowState, WorkflowStorage};

use crate::py_queue::workflow_ops::workflow_storage;
use crate::py_queue::PyQueue;

#[pymethods]
impl PyQueue {
    /// Set a workflow run's state to `Compensating`.
    pub fn set_workflow_run_compensating(&self, py: Python<'_>, run_id: &str) -> PyResult<()> {
        let wf = workflow_storage(self)?;
        let rid = run_id.to_string();
        py.detach(|| wf.update_workflow_run_state(&rid, WorkflowState::Compensating, None))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Set a workflow run's state to `Compensated` and stamp `completed_at`.
    pub fn set_workflow_run_compensated(
        &self,
        py: Python<'_>,
        run_id: &str,
        completed_at: i64,
    ) -> PyResult<()> {
        let wf = workflow_storage(self)?;
        let rid = run_id.to_string();
        py.detach(|| {
            wf.update_workflow_run_state(&rid, WorkflowState::Compensated, None)?;
            wf.set_workflow_run_completed(&rid, completed_at)?;
            Ok::<_, flexiq_core::error::QueueError>(())
        })
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Set a workflow run's state to `CompensationFailed` and stamp
    /// `completed_at`. Optionally records an error string on the run.
    #[pyo3(signature = (run_id, completed_at, error=None))]
    pub fn set_workflow_run_compensation_failed(
        &self,
        py: Python<'_>,
        run_id: &str,
        completed_at: i64,
        error: Option<String>,
    ) -> PyResult<()> {
        let wf = workflow_storage(self)?;
        let rid = run_id.to_string();
        py.detach(|| {
            wf.update_workflow_run_state(
                &rid,
                WorkflowState::CompensationFailed,
                error.as_deref(),
            )?;
            wf.set_workflow_run_completed(&rid, completed_at)?;
            Ok::<_, flexiq_core::error::QueueError>(())
        })
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Mark a node as `Compensating`, recording the compensation job id
    /// and start timestamp.
    pub fn set_workflow_node_compensation_job(
        &self,
        py: Python<'_>,
        run_id: &str,
        node_name: &str,
        compensation_job_id: &str,
        started_at: i64,
    ) -> PyResult<()> {
        let wf = workflow_storage(self)?;
        let rid = run_id.to_string();
        let nn = node_name.to_string();
        let cjid = compensation_job_id.to_string();
        py.detach(|| wf.set_workflow_node_compensation_job(&rid, &nn, &cjid, started_at))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Mark a node as `Compensated`.
    pub fn set_workflow_node_compensated(
        &self,
        py: Python<'_>,
        run_id: &str,
        node_name: &str,
        completed_at: i64,
    ) -> PyResult<()> {
        let wf = workflow_storage(self)?;
        let rid = run_id.to_string();
        let nn = node_name.to_string();
        py.detach(|| wf.set_workflow_node_compensated(&rid, &nn, completed_at))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Mark a node as `CompensationFailed`, recording the error and
    /// completion timestamp.
    pub fn set_workflow_node_compensation_failed(
        &self,
        py: Python<'_>,
        run_id: &str,
        node_name: &str,
        error: &str,
        completed_at: i64,
    ) -> PyResult<()> {
        let wf = workflow_storage(self)?;
        let rid = run_id.to_string();
        let nn = node_name.to_string();
        let err = error.to_string();
        py.detach(|| wf.set_workflow_node_compensation_failed(&rid, &nn, &err, completed_at))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
}
