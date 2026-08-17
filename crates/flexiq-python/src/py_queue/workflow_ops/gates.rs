//! Approval gate resolution.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use flexiq_core::error::Result as CoreResult;
use flexiq_core::job::now_millis;
use flexiq_workflows::WorkflowStorage;

use crate::py_queue::workflow_ops::workflow_storage;
use crate::py_queue::PyQueue;

#[pymethods]
impl PyQueue {
    /// Approve or reject an approval gate node.
    ///
    /// Approved gates transition to `Completed`; rejected gates to `Failed`.
    #[pyo3(signature = (run_id, node_name, approved, error=None))]
    pub fn resolve_workflow_gate(
        &self,
        py: Python<'_>,
        run_id: &str,
        node_name: &str,
        approved: bool,
        error: Option<String>,
    ) -> PyResult<()> {
        let wf_storage = workflow_storage(self)?;
        let run_id_owned = run_id.to_string();
        let node_name_owned = node_name.to_string();

        let result: CoreResult<()> = py.detach(|| {
            let now = now_millis();
            if approved {
                wf_storage.set_workflow_node_completed(
                    &run_id_owned,
                    &node_name_owned,
                    now,
                    None,
                )?;
            } else {
                let err_msg = error.unwrap_or_else(|| "rejected".to_string());
                wf_storage.set_workflow_node_error(&run_id_owned, &node_name_owned, &err_msg)?;
            }
            Ok(())
        });

        result.map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
}
