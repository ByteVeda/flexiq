//! Read-only queries: run status, base run node data, definition DAG.

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use flexiq_core::error::Result as CoreResult;
use flexiq_workflows::{WorkflowState, WorkflowStorage};

use crate::py_queue::workflow_ops::{status_to_py, workflow_storage};
use crate::py_queue::PyQueue;
use crate::py_workflow::{PyWorkflowRun, PyWorkflowRunNode, PyWorkflowRunStatus};

#[pymethods]
impl PyQueue {
    /// Fetch a snapshot of a workflow run's state and per-node status.
    pub fn get_workflow_run_status(
        &self,
        py: Python<'_>,
        run_id: &str,
    ) -> PyResult<PyWorkflowRunStatus> {
        let wf_storage = workflow_storage(self)?;
        let run_id_owned = run_id.to_string();

        let result: CoreResult<Option<PyWorkflowRunStatus>> = py.detach(|| {
            let run = match wf_storage.get_workflow_run(&run_id_owned)? {
                Some(r) => r,
                None => return Ok(None),
            };
            let nodes = wf_storage.get_workflow_nodes(&run_id_owned)?;
            let node_rows = nodes
                .into_iter()
                .map(|n| {
                    (
                        n.node_name,
                        n.status.as_str().to_string(),
                        n.job_id,
                        n.error,
                    )
                })
                .collect();
            Ok(Some(PyWorkflowRunStatus {
                run_id: run.id,
                state: status_to_py(run.state),
                started_at: run.started_at,
                completed_at: run.completed_at,
                error: run.error,
                nodes: node_rows,
            }))
        });

        result
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
            .ok_or_else(|| PyValueError::new_err(format!("workflow run '{run_id}' not found")))
    }

    /// Fetch node data from a prior run for incremental caching.
    ///
    /// Returns a list of ``(node_name, status, result_hash)`` tuples.
    pub fn get_base_run_node_data(
        &self,
        py: Python<'_>,
        base_run_id: &str,
    ) -> PyResult<Vec<(String, String, Option<String>)>> {
        let wf_storage = workflow_storage(self)?;
        let base_run_id_owned = base_run_id.to_string();

        let result: CoreResult<Vec<(String, String, Option<String>)>> = py.detach(|| {
            let nodes = wf_storage.get_workflow_nodes(&base_run_id_owned)?;
            Ok(nodes
                .into_iter()
                .map(|n| (n.node_name, n.status.as_str().to_string(), n.result_hash))
                .collect())
        });

        result.map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// List workflow runs, optionally filtered by definition name and / or state.
    ///
    /// Used by the dashboard ``/api/workflows/runs`` endpoint. Caller-supplied
    /// ``limit`` is clamped to ``[1, 500]`` and ``offset`` floored at 0 so the
    /// endpoint can't be abused for unbounded scans.
    #[pyo3(signature = (definition_name=None, state=None, limit=50, offset=0))]
    pub fn list_workflow_runs(
        &self,
        py: Python<'_>,
        definition_name: Option<&str>,
        state: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> PyResult<Vec<PyWorkflowRun>> {
        let wf_storage = workflow_storage(self)?;
        let limit = limit.clamp(1, 500);
        let offset = offset.max(0);
        let definition_owned = definition_name.map(|s| s.to_string());
        let state_parsed = match state {
            Some(s) => Some(
                WorkflowState::from_str_val(s)
                    .ok_or_else(|| PyValueError::new_err(format!("invalid workflow state: {s}")))?,
            ),
            None => None,
        };

        let result: CoreResult<Vec<PyWorkflowRun>> = py.detach(|| {
            let runs = wf_storage.list_workflow_runs(
                definition_owned.as_deref(),
                state_parsed,
                limit,
                offset,
            )?;
            Ok(runs.into_iter().map(PyWorkflowRun::from).collect())
        });

        result.map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Keyset-paginated `list_workflow_runs`. Returns `(runs, next_cursor)`;
    /// pass `next_cursor` back as `after`. `limit` is clamped to `[1, 500]`.
    #[pyo3(signature = (definition_name=None, state=None, limit=50, after=None))]
    pub fn list_workflow_runs_after(
        &self,
        py: Python<'_>,
        definition_name: Option<&str>,
        state: Option<&str>,
        limit: i64,
        after: Option<&str>,
    ) -> PyResult<(Vec<PyWorkflowRun>, Option<String>)> {
        let wf_storage = workflow_storage(self)?;
        let limit = limit.clamp(1, 500);
        let definition_owned = definition_name.map(|s| s.to_string());
        let state_parsed = match state {
            Some(s) => Some(
                WorkflowState::from_str_val(s)
                    .ok_or_else(|| PyValueError::new_err(format!("invalid workflow state: {s}")))?,
            ),
            None => None,
        };
        let cursor = after
            .map(flexiq_core::storage::cursor::decode_cursor)
            .transpose()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        let result: CoreResult<(Vec<PyWorkflowRun>, Option<String>)> = py.detach(|| {
            let runs = wf_storage.list_workflow_runs_after(
                definition_owned.as_deref(),
                state_parsed,
                limit,
                cursor,
            )?;
            let next = crate::py_queue::next_cursor(&runs, limit, |r| (r.created_at, &r.id));
            Ok((runs.into_iter().map(PyWorkflowRun::from).collect(), next))
        });

        result.map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Fetch a workflow run with full per-node detail including compensation fields.
    ///
    /// Used by the dashboard ``/api/workflows/runs/{run_id}`` endpoint. Unlike
    /// ``get_workflow_run_status`` (which returns a flat tuple representation),
    /// this exposes compensation timestamps and error strings for every node.
    pub fn get_workflow_run_detail(
        &self,
        py: Python<'_>,
        run_id: &str,
    ) -> PyResult<(PyWorkflowRun, Vec<PyWorkflowRunNode>)> {
        let wf_storage = workflow_storage(self)?;
        let run_id_owned = run_id.to_string();

        let result: CoreResult<Option<(PyWorkflowRun, Vec<PyWorkflowRunNode>)>> = py.detach(|| {
            let run = match wf_storage.get_workflow_run(&run_id_owned)? {
                Some(r) => r,
                None => return Ok(None),
            };
            let nodes = wf_storage.get_workflow_nodes(&run_id_owned)?;
            let node_rows = nodes.into_iter().map(PyWorkflowRunNode::from).collect();
            Ok(Some((PyWorkflowRun::from(run), node_rows)))
        });

        result
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
            .ok_or_else(|| PyValueError::new_err(format!("workflow run '{run_id}' not found")))
    }

    /// List sub-workflow runs spawned by a parent run.
    pub fn get_child_workflow_runs(
        &self,
        py: Python<'_>,
        parent_run_id: &str,
    ) -> PyResult<Vec<PyWorkflowRun>> {
        let wf_storage = workflow_storage(self)?;
        let parent_owned = parent_run_id.to_string();

        let result: CoreResult<Vec<PyWorkflowRun>> = py.detach(|| {
            let runs = wf_storage.get_child_workflow_runs(&parent_owned)?;
            Ok(runs.into_iter().map(PyWorkflowRun::from).collect())
        });

        result.map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Return the DAG JSON bytes for a workflow run's definition.
    ///
    /// Used by the Python visualization layer to render diagrams.
    pub fn get_workflow_definition_dag(&self, py: Python<'_>, run_id: &str) -> PyResult<Vec<u8>> {
        let wf_storage = workflow_storage(self)?;
        let run_id_owned = run_id.to_string();

        enum Outcome {
            RunMissing,
            DefinitionMissing(String),
            Found(Vec<u8>),
        }

        let outcome: CoreResult<Outcome> = py.detach(|| {
            let run = match wf_storage.get_workflow_run(&run_id_owned)? {
                Some(r) => r,
                None => return Ok(Outcome::RunMissing),
            };
            match wf_storage.get_workflow_definition_by_id(&run.definition_id)? {
                Some(def) => Ok(Outcome::Found(def.dag_data)),
                None => Ok(Outcome::DefinitionMissing(run.definition_id)),
            }
        });

        match outcome.map_err(|e| PyRuntimeError::new_err(e.to_string()))? {
            Outcome::Found(data) => Ok(data),
            Outcome::RunMissing => Err(PyValueError::new_err(format!("run '{run_id}' not found"))),
            Outcome::DefinitionMissing(def_id) => Err(PyRuntimeError::new_err(format!(
                "definition '{def_id}' not found"
            ))),
        }
    }
}
