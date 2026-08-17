#![allow(clippy::useless_conversion)]
//! Python bindings for the `flexiq-workflows` crate.
//!
//! Compiled only when the `workflows` feature is enabled. Exposes three
//! `#[pyclass]` types:
//!
//! * [`PyWorkflowBuilder`] — construct a DAG from Python and serialize it
//!   for storage.
//! * [`PyWorkflowHandle`] — opaque handle returned from `PyQueue::submit_workflow`
//!   carrying the run id.
//! * [`PyWorkflowRunStatus`] — snapshot of a workflow run, returned by
//!   `PyQueue::get_workflow_run_status`.

use std::collections::HashMap;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use flexiq_workflows::dagron_core::DAG;
use flexiq_workflows::StepMetadata;

/// Builder for a workflow DAG.
///
/// Construct in Python, add steps, then call `serialize()` to produce
/// `(dag_json_bytes, step_metadata_json)` for submission.
#[pyclass]
pub struct PyWorkflowBuilder {
    dag: DAG<()>,
    step_metadata: HashMap<String, StepMetadata>,
    step_order: Vec<String>,
}

#[pymethods]
impl PyWorkflowBuilder {
    #[new]
    fn new() -> Self {
        Self {
            dag: DAG::new(),
            step_metadata: HashMap::new(),
            step_order: Vec::new(),
        }
    }

    /// Add a step to the workflow.
    ///
    /// `after` lists the names of predecessor steps that must complete before
    /// this step runs. All predecessors must already have been added.
    #[pyo3(signature = (
        name, task_name, after=None, queue=None, max_retries=None,
        timeout_ms=None, priority=None, args_template=None, kwargs_template=None,
        fan_out=None, fan_in=None, condition=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn add_step(
        &mut self,
        name: &str,
        task_name: &str,
        after: Option<Vec<String>>,
        queue: Option<String>,
        max_retries: Option<i32>,
        timeout_ms: Option<i64>,
        priority: Option<i32>,
        args_template: Option<String>,
        kwargs_template: Option<String>,
        fan_out: Option<String>,
        fan_in: Option<String>,
        condition: Option<String>,
    ) -> PyResult<()> {
        self.dag
            .add_node(name.to_string(), ())
            .map_err(|e| PyValueError::new_err(format!("add_node failed: {e}")))?;

        if let Some(preds) = after {
            for pred in preds {
                self.dag
                    .add_edge(&pred, name, None, None)
                    .map_err(|e| PyValueError::new_err(format!("add_edge failed: {e}")))?;
            }
        }

        self.step_metadata.insert(
            name.to_string(),
            StepMetadata {
                task_name: task_name.to_string(),
                queue,
                args_template,
                kwargs_template,
                max_retries,
                timeout_ms,
                priority,
                fan_out,
                fan_in,
                condition,
                ..Default::default()
            },
        );
        self.step_order.push(name.to_string());
        Ok(())
    }

    /// Return the number of steps added.
    fn step_count(&self) -> usize {
        self.step_order.len()
    }

    /// Return the names of steps in insertion order.
    fn step_names(&self) -> Vec<String> {
        self.step_order.clone()
    }

    /// Serialize the DAG and its step metadata for storage.
    ///
    /// Returns `(dag_bytes, step_metadata_json)` where:
    ///   * `dag_bytes` is the UTF-8 encoded JSON of the dagron
    ///     `SerializableGraph` (no payloads).
    ///   * `step_metadata_json` is the JSON-encoded
    ///     `HashMap<String, StepMetadata>`.
    fn serialize(&self) -> PyResult<(Vec<u8>, String)> {
        let dag_json = self
            .dag
            .to_json(|_| None)
            .map_err(|e| PyRuntimeError::new_err(format!("DAG to_json failed: {e}")))?;
        let meta_json = serde_json::to_string(&self.step_metadata)
            .map_err(|e| PyRuntimeError::new_err(format!("step_metadata serialize failed: {e}")))?;
        Ok((dag_json.into_bytes(), meta_json))
    }
}

/// Opaque handle returned from `PyQueue::submit_workflow`.
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct PyWorkflowHandle {
    #[pyo3(get)]
    pub run_id: String,
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub definition_id: String,
}

#[pymethods]
impl PyWorkflowHandle {
    fn __repr__(&self) -> String {
        format!(
            "PyWorkflowHandle(run_id='{}', name='{}')",
            self.run_id, self.name
        )
    }
}

/// Snapshot of a workflow run's state and per-node status.
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct PyWorkflowRunStatus {
    #[pyo3(get)]
    pub run_id: String,
    #[pyo3(get)]
    pub state: String,
    #[pyo3(get)]
    pub started_at: Option<i64>,
    #[pyo3(get)]
    pub completed_at: Option<i64>,
    #[pyo3(get)]
    pub error: Option<String>,
    pub nodes: Vec<(String, String, Option<String>, Option<String>)>,
}

#[pymethods]
impl PyWorkflowRunStatus {
    /// Return per-node status as a dict keyed by node name.
    ///
    /// Each value is a dict with keys `status`, `job_id`, and `error`.
    fn node_statuses<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let out = PyDict::new(py);
        for (name, status, job_id, error) in &self.nodes {
            let entry = PyDict::new(py);
            entry.set_item("status", status)?;
            entry.set_item("job_id", job_id.clone())?;
            entry.set_item("error", error.clone())?;
            out.set_item(name, entry)?;
        }
        Ok(out)
    }

    fn __repr__(&self) -> String {
        format!(
            "PyWorkflowRunStatus(run_id='{}', state='{}', nodes={})",
            self.run_id,
            self.state,
            self.nodes.len()
        )
    }
}

/// One row of the dashboard ``/api/workflows/runs`` listing.
///
/// Mirrors the core ``WorkflowRun`` struct but uses ``String`` for the
/// state (so the wire shape stays stable when the enum gets new variants)
/// and exposes only the fields the dashboard actually renders.
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct PyWorkflowRun {
    #[pyo3(get)]
    pub id: String,
    #[pyo3(get)]
    pub definition_id: String,
    #[pyo3(get)]
    pub state: String,
    #[pyo3(get)]
    pub params: Option<String>,
    #[pyo3(get)]
    pub started_at: Option<i64>,
    #[pyo3(get)]
    pub completed_at: Option<i64>,
    #[pyo3(get)]
    pub error: Option<String>,
    #[pyo3(get)]
    pub parent_run_id: Option<String>,
    #[pyo3(get)]
    pub parent_node_name: Option<String>,
    #[pyo3(get)]
    pub created_at: i64,
}

impl From<::flexiq_workflows::WorkflowRun> for PyWorkflowRun {
    fn from(r: ::flexiq_workflows::WorkflowRun) -> Self {
        Self {
            id: r.id,
            definition_id: r.definition_id,
            state: r.state.as_str().to_string(),
            params: r.params,
            started_at: r.started_at,
            completed_at: r.completed_at,
            error: r.error,
            parent_run_id: r.parent_run_id,
            parent_node_name: r.parent_node_name,
            created_at: r.created_at,
        }
    }
}

#[pymethods]
impl PyWorkflowRun {
    fn __repr__(&self) -> String {
        format!("PyWorkflowRun(id='{}', state='{}')", self.id, self.state)
    }
}

/// One node entry for the dashboard ``/api/workflows/runs/{run_id}`` detail view.
///
/// Includes compensation columns (added in 0.13.0) so the dashboard can show
/// "this node was compensated at ts, comp job xxx" without an extra query.
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct PyWorkflowRunNode {
    #[pyo3(get)]
    pub node_name: String,
    #[pyo3(get)]
    pub status: String,
    #[pyo3(get)]
    pub job_id: Option<String>,
    #[pyo3(get)]
    pub result_hash: Option<String>,
    #[pyo3(get)]
    pub fan_out_count: Option<i32>,
    #[pyo3(get)]
    pub started_at: Option<i64>,
    #[pyo3(get)]
    pub completed_at: Option<i64>,
    #[pyo3(get)]
    pub error: Option<String>,
    #[pyo3(get)]
    pub compensation_job_id: Option<String>,
    #[pyo3(get)]
    pub compensation_started_at: Option<i64>,
    #[pyo3(get)]
    pub compensation_completed_at: Option<i64>,
    #[pyo3(get)]
    pub compensation_error: Option<String>,
}

impl From<::flexiq_workflows::WorkflowNode> for PyWorkflowRunNode {
    fn from(n: ::flexiq_workflows::WorkflowNode) -> Self {
        Self {
            node_name: n.node_name,
            status: n.status.as_str().to_string(),
            job_id: n.job_id,
            result_hash: n.result_hash,
            fan_out_count: n.fan_out_count,
            started_at: n.started_at,
            completed_at: n.completed_at,
            error: n.error,
            compensation_job_id: n.compensation_job_id,
            compensation_started_at: n.compensation_started_at,
            compensation_completed_at: n.compensation_completed_at,
            compensation_error: n.compensation_error,
        }
    }
}

#[pymethods]
impl PyWorkflowRunNode {
    fn __repr__(&self) -> String {
        format!(
            "PyWorkflowRunNode(node_name='{}', status='{}')",
            self.node_name, self.status
        )
    }
}
