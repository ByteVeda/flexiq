//! Fan-out expansion, deferred job creation, fan-out completion check.

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use taskito_core::error::Result as CoreResult;
use taskito_core::job::{now_millis, NewJob};
use taskito_core::storage::Storage;
use taskito_workflows::{WorkflowNode, WorkflowNodeStatus, WorkflowStorage};

use crate::py_queue::workflow_ops::{build_metadata_json, require_visible_run, workflow_storage};
use crate::py_queue::PyQueue;

/// Maximum number of children a single fan-out may expand into. Guards against
/// a task returning an enormous list and flooding storage + memory in one
/// transaction.
const MAX_FAN_OUT: usize = 10_000;

#[pymethods]
impl PyQueue {
    /// Expand a fan-out node into N child nodes + jobs.
    ///
    /// Creates one `WorkflowNode` and one job per child. Sets the parent
    /// node's `fan_out_count` and transitions it to `Running`. If the
    /// children list is empty (fan-out over empty result), the parent is
    /// marked `Completed` immediately.
    #[pyo3(signature = (
        run_id, parent_node_name, child_names, child_payloads,
        task_name, queue, max_retries, timeout_ms, priority
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn expand_fan_out(
        &self,
        py: Python<'_>,
        run_id: &str,
        parent_node_name: &str,
        child_names: Vec<String>,
        child_payloads: Vec<Vec<u8>>,
        task_name: &str,
        queue: &str,
        max_retries: i32,
        timeout_ms: i64,
        priority: i32,
    ) -> PyResult<Vec<String>> {
        if child_names.len() != child_payloads.len() {
            return Err(PyValueError::new_err(
                "child_names and child_payloads must have the same length",
            ));
        }
        if child_names.len() > MAX_FAN_OUT {
            return Err(PyValueError::new_err(format!(
                "fan-out of {} children exceeds the limit of {MAX_FAN_OUT}",
                child_names.len()
            )));
        }

        let wf_storage = workflow_storage(self)?;
        let run_id_owned = run_id.to_string();
        let parent_name_owned = parent_node_name.to_string();
        let task_name_owned = task_name.to_string();
        let queue_owned = queue.to_string();

        let result: CoreResult<Vec<String>> = py.detach(|| {
            require_visible_run(&wf_storage, &run_id_owned)?;
            let now = now_millis();
            let count = child_names.len() as i32;

            // Empty fan-out: mark parent completed immediately.
            if count == 0 {
                wf_storage.set_workflow_node_fan_out_count(&run_id_owned, &parent_name_owned, 0)?;
                wf_storage.set_workflow_node_completed(
                    &run_id_owned,
                    &parent_name_owned,
                    now,
                    None,
                )?;
                return Ok(Vec::new());
            }

            // Enqueue all child jobs first, then atomically batch-insert the
            // matching workflow_nodes. The batch insert wraps every row in one
            // transaction, so a crash partway through node creation no longer
            // leaves the run with half-tracked children.
            let mut child_job_ids = Vec::with_capacity(child_names.len());
            let mut wf_nodes = Vec::with_capacity(child_names.len());
            for (child_name, payload) in child_names.iter().zip(child_payloads.into_iter()) {
                let new_job = NewJob {
                    queue: queue_owned.clone(),
                    task_name: task_name_owned.clone(),
                    payload,
                    priority,
                    scheduled_at: now,
                    max_retries,
                    timeout_ms,
                    unique_key: None,
                    metadata: Some(build_metadata_json(&run_id_owned, child_name)),
                    notes: None,
                    depends_on: vec![],
                    expires_at: None,
                    result_ttl_ms: self.result_ttl_ms,
                    namespace: self.namespace.clone(),
                    debounce_key: None,
                };
                let job = self.storage.enqueue(new_job)?;
                child_job_ids.push(job.id.clone());

                wf_nodes.push(WorkflowNode {
                    id: uuid::Uuid::now_v7().to_string(),
                    run_id: run_id_owned.clone(),
                    node_name: child_name.clone(),
                    job_id: Some(job.id),
                    status: WorkflowNodeStatus::Pending,
                    result_hash: None,
                    fan_out_count: None,
                    fan_in_data: None,
                    started_at: None,
                    completed_at: None,
                    error: None,
                    compensation_job_id: None,
                    compensation_started_at: None,
                    compensation_completed_at: None,
                    compensation_error: None,
                });
            }

            wf_storage.create_workflow_nodes_batch(&wf_nodes)?;
            wf_storage.set_workflow_node_fan_out_count(&run_id_owned, &parent_name_owned, count)?;
            Ok(child_job_ids)
        });

        result.map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Create a job for a deferred workflow node.
    ///
    /// Used after fan-in collects results, or for static nodes downstream of
    /// dynamic nodes whose predecessors are now all complete.
    #[pyo3(signature = (run_id, node_name, payload, task_name, queue, max_retries, timeout_ms, priority))]
    #[allow(clippy::too_many_arguments)]
    pub fn create_deferred_job(
        &self,
        py: Python<'_>,
        run_id: &str,
        node_name: &str,
        payload: Vec<u8>,
        task_name: &str,
        queue: &str,
        max_retries: i32,
        timeout_ms: i64,
        priority: i32,
    ) -> PyResult<String> {
        let wf_storage = workflow_storage(self)?;
        let run_id_owned = run_id.to_string();
        let node_name_owned = node_name.to_string();
        let task_name_owned = task_name.to_string();
        let queue_owned = queue.to_string();

        let result: CoreResult<String> = py.detach(|| {
            require_visible_run(&wf_storage, &run_id_owned)?;
            let now = now_millis();
            let new_job = NewJob {
                queue: queue_owned,
                task_name: task_name_owned,
                payload,
                priority,
                scheduled_at: now,
                max_retries,
                timeout_ms,
                unique_key: None,
                metadata: Some(build_metadata_json(&run_id_owned, &node_name_owned)),
                notes: None,
                depends_on: vec![],
                expires_at: None,
                result_ttl_ms: self.result_ttl_ms,
                namespace: self.namespace.clone(),
                debounce_key: None,
            };
            let job = self.storage.enqueue(new_job)?;
            wf_storage.set_workflow_node_job(&run_id_owned, &node_name_owned, &job.id)?;
            Ok(job.id)
        });

        result.map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Check whether all fan-out children of a parent node are terminal.
    ///
    /// When all children are terminal, performs an atomic compare-and-swap on
    /// the parent node's status to finalize it exactly once, even across
    /// concurrent callers. Returns `Some((all_succeeded, child_job_ids))` if
    /// this caller performed the transition, `None` otherwise (either not all
    /// children are done yet, or another concurrent caller already finalized).
    pub fn check_fan_out_completion(
        &self,
        py: Python<'_>,
        run_id: &str,
        parent_node_name: &str,
    ) -> PyResult<Option<(bool, Vec<String>)>> {
        let wf_storage = workflow_storage(self)?;
        let run_id_owned = run_id.to_string();
        let parent_name_owned = parent_node_name.to_string();

        let result: CoreResult<Option<(bool, Vec<String>)>> = py.detach(|| {
            let prefix = format!("{parent_name_owned}[");
            let children = wf_storage.get_workflow_nodes_by_prefix(&run_id_owned, &prefix)?;

            if children.is_empty() || !children.iter().all(|n| n.status.is_terminal()) {
                return Ok(None);
            }

            let any_failed = children
                .iter()
                .any(|n| n.status == WorkflowNodeStatus::Failed);
            let child_job_ids: Vec<String> =
                children.iter().filter_map(|n| n.job_id.clone()).collect();

            let transitioned = wf_storage.finalize_fan_out_parent(
                &run_id_owned,
                &parent_name_owned,
                !any_failed,
                if any_failed {
                    Some("fan-out child failed")
                } else {
                    None
                },
                now_millis(),
            )?;
            if !transitioned {
                return Ok(None);
            }

            Ok(Some((!any_failed, child_job_ids)))
        });

        result.map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::py_queue::workflow_ops::test_helpers::*;

    /// Mismatched lengths surface as a `PyValueError`. Caller-side bug, not a
    /// silent partial expansion.
    #[test]
    fn expand_fan_out_rejects_mismatched_lengths() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let queue = make_test_pyqueue();
            let wf = wf_storage(&queue);
            let run_id = seed_run(wf);
            seed_node(wf, &run_id, "parent", WorkflowNodeStatus::Pending, None);

            let result = queue.expand_fan_out(
                py,
                &run_id,
                "parent",
                vec!["child[0]".to_string()],
                vec![vec![1u8, 2, 3], vec![4u8, 5, 6]],
                "task_child",
                "default",
                3,
                300_000,
                0,
            );
            let err = match result {
                Err(e) => e,
                Ok(_) => panic!("expected PyValueError for length mismatch"),
            };
            assert!(err.to_string().contains("same length"));
        });
    }

    /// Empty child list short-circuits to a `Completed` parent — there is
    /// nothing to wait on. No child jobs are enqueued.
    #[test]
    fn expand_fan_out_completes_parent_immediately_on_empty_children() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let queue = make_test_pyqueue();
            let wf = wf_storage(&queue);
            let run_id = seed_run(wf);
            seed_node(wf, &run_id, "parent", WorkflowNodeStatus::Pending, None);

            let ids = queue
                .expand_fan_out(
                    py,
                    &run_id,
                    "parent",
                    Vec::new(),
                    Vec::new(),
                    "task_child",
                    "default",
                    3,
                    300_000,
                    0,
                )
                .unwrap();
            assert!(ids.is_empty());

            let parent = fetch_node(wf, &run_id, "parent");
            assert_eq!(parent.status, WorkflowNodeStatus::Completed);
            assert_eq!(parent.fan_out_count, Some(0));
            assert!(parent.completed_at.is_some());
        });
    }

    /// Non-empty fan-out creates a child node and job per entry, sets the
    /// parent's `fan_out_count`, and returns the child job ids in order.
    #[test]
    fn expand_fan_out_creates_children_and_records_count() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let queue = make_test_pyqueue();
            let wf = wf_storage(&queue);
            let run_id = seed_run(wf);
            seed_node(wf, &run_id, "parent", WorkflowNodeStatus::Pending, None);

            let child_names = vec!["parent[0]".to_string(), "parent[1]".to_string()];
            let child_payloads = vec![vec![1u8], vec![2u8]];

            let ids = queue
                .expand_fan_out(
                    py,
                    &run_id,
                    "parent",
                    child_names.clone(),
                    child_payloads,
                    "task_child",
                    "default",
                    3,
                    300_000,
                    0,
                )
                .unwrap();
            assert_eq!(ids.len(), 2);

            let nodes = wf.get_workflow_nodes(&run_id).unwrap();
            for name in &child_names {
                let node = nodes
                    .iter()
                    .find(|n| &n.node_name == name)
                    .unwrap_or_else(|| panic!("child node {name} missing"));
                assert_eq!(node.status, WorkflowNodeStatus::Pending);
                assert!(node.job_id.is_some());
            }

            // The parent's fan_out_count is updated; the parent stays in its
            // prior status because the tracker is responsible for promoting it
            // separately (mirrors the production path).
            let parent = fetch_node(wf, &run_id, "parent");
            assert_eq!(parent.fan_out_count, Some(2));
        });
    }

    /// `create_deferred_job` enqueues a job and binds it to the named node.
    #[test]
    fn create_deferred_job_enqueues_and_attaches_to_node() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let queue = make_test_pyqueue();
            let wf = wf_storage(&queue);
            let run_id = seed_run(wf);
            seed_node(wf, &run_id, "deferred", WorkflowNodeStatus::Pending, None);

            let job_id = queue
                .create_deferred_job(
                    py,
                    &run_id,
                    "deferred",
                    vec![9u8, 9, 9],
                    "task_deferred",
                    "default",
                    3,
                    300_000,
                    0,
                )
                .unwrap();

            let job = queue.storage.get_job(&job_id, None).unwrap().unwrap();
            assert_eq!(job.task_name, "task_deferred");
            assert!(job
                .metadata
                .as_deref()
                .unwrap_or("")
                .contains("workflow_run_id"));

            let node = fetch_node(wf, &run_id, "deferred");
            assert_eq!(node.job_id.as_deref(), Some(job_id.as_str()));
        });
    }

    /// `check_fan_out_completion` returns `None` while any child is still
    /// non-terminal.
    #[test]
    fn check_fan_out_completion_returns_none_while_children_running() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let queue = make_test_pyqueue();
            let wf = wf_storage(&queue);
            let run_id = seed_run(wf);
            seed_node(wf, &run_id, "parent", WorkflowNodeStatus::Running, None);
            seed_node(
                wf,
                &run_id,
                "parent[0]",
                WorkflowNodeStatus::Completed,
                None,
            );
            seed_node(wf, &run_id, "parent[1]", WorkflowNodeStatus::Running, None);

            let outcome = queue
                .check_fan_out_completion(py, &run_id, "parent")
                .unwrap();
            assert!(outcome.is_none());
        });
    }

    /// All-success children finalize the parent as `Completed` exactly once
    /// and return `(true, child_job_ids)`.
    #[test]
    fn check_fan_out_completion_finalizes_parent_on_all_success() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let queue = make_test_pyqueue();
            let wf = wf_storage(&queue);
            let run_id = seed_run(wf);
            seed_node(wf, &run_id, "parent", WorkflowNodeStatus::Running, None);
            let j0 = enqueue_test_job(&queue.storage, "task_child_0");
            let j1 = enqueue_test_job(&queue.storage, "task_child_1");
            seed_node(
                wf,
                &run_id,
                "parent[0]",
                WorkflowNodeStatus::Completed,
                Some(j0.clone()),
            );
            seed_node(
                wf,
                &run_id,
                "parent[1]",
                WorkflowNodeStatus::Completed,
                Some(j1.clone()),
            );

            let outcome = queue
                .check_fan_out_completion(py, &run_id, "parent")
                .unwrap()
                .expect("first caller transitions the parent");
            assert!(outcome.0, "all-success path returns true");
            let mut ids = outcome.1;
            ids.sort();
            let mut expected = vec![j0, j1];
            expected.sort();
            assert_eq!(ids, expected);

            // The second caller observes the parent already finalized and
            // returns `None` rather than double-firing the success path.
            let again = queue
                .check_fan_out_completion(py, &run_id, "parent")
                .unwrap();
            assert!(again.is_none());
        });
    }

    /// Any failed child finalizes the parent as `Failed` and reports
    /// `(false, child_job_ids)`.
    #[test]
    fn check_fan_out_completion_finalizes_parent_as_failed_on_any_failure() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let queue = make_test_pyqueue();
            let wf = wf_storage(&queue);
            let run_id = seed_run(wf);
            seed_node(wf, &run_id, "parent", WorkflowNodeStatus::Running, None);
            seed_node(
                wf,
                &run_id,
                "parent[0]",
                WorkflowNodeStatus::Completed,
                Some(enqueue_test_job(&queue.storage, "task_child_0")),
            );
            seed_node(
                wf,
                &run_id,
                "parent[1]",
                WorkflowNodeStatus::Failed,
                Some(enqueue_test_job(&queue.storage, "task_child_1")),
            );

            let outcome = queue
                .check_fan_out_completion(py, &run_id, "parent")
                .unwrap()
                .expect("first caller transitions the parent");
            assert!(!outcome.0, "failure path returns false");

            let parent = fetch_node(wf, &run_id, "parent");
            assert_eq!(parent.status, WorkflowNodeStatus::Failed);
        });
    }

    /// A fan-out parent with no children yet (e.g. expansion not run) leaves
    /// `check_fan_out_completion` returning `None` rather than promoting the
    /// parent on a zero-child set.
    #[test]
    fn check_fan_out_completion_is_noop_when_no_children_exist() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let queue = make_test_pyqueue();
            let wf = wf_storage(&queue);
            let run_id = seed_run(wf);
            seed_node(wf, &run_id, "parent", WorkflowNodeStatus::Running, None);

            let outcome = queue
                .check_fan_out_completion(py, &run_id, "parent")
                .unwrap();
            assert!(outcome.is_none());

            let parent = fetch_node(wf, &run_id, "parent");
            assert_eq!(parent.status, WorkflowNodeStatus::Running);
        });
    }
}
