//! Workflow run lifecycle: submit, cancel, finalize-if-terminal.

use std::collections::{HashMap, HashSet};

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use flexiq_core::error::Result as CoreResult;
use flexiq_core::job::now_millis;
use flexiq_workflows::lifecycle::{parse_step_metadata, SubmitStaticWorkflowRequest};
use flexiq_workflows::{WorkflowNodeStatus, WorkflowState, WorkflowStorage};

use crate::py_queue::workflow_ops::{cascade_skip_pending_nodes, workflow_storage};
use crate::py_queue::PyQueue;
use crate::py_workflow::PyWorkflowHandle;

#[pymethods]
impl PyQueue {
    /// Submit a workflow for execution.
    ///
    /// Creates (or reuses) a `WorkflowDefinition` with the given name + version,
    /// inserts a `WorkflowRun`, pre-enqueues all step jobs in topological order
    /// with `depends_on` chains so flexiq's existing scheduler runs them in the
    /// correct order. Nodes listed in `deferred_node_names` get a
    /// `WorkflowNode` only (no job) — their jobs are created at runtime by the
    /// Python tracker (fan-out / fan-in orchestration).
    ///
    /// Returns a `PyWorkflowHandle` carrying the run id.
    #[pyo3(signature = (
        name, version, dag_bytes, step_metadata_json, node_payloads,
        queue_default="default", params_json=None, deferred_node_names=None,
        parent_run_id=None, parent_node_name=None, cache_hit_nodes=None
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn submit_workflow(
        &self,
        name: &str,
        version: i32,
        dag_bytes: Vec<u8>,
        step_metadata_json: &str,
        node_payloads: HashMap<String, Vec<u8>>,
        queue_default: &str,
        params_json: Option<String>,
        deferred_node_names: Option<Vec<String>>,
        parent_run_id: Option<String>,
        parent_node_name: Option<String>,
        cache_hit_nodes: Option<HashMap<String, String>>,
    ) -> PyResult<PyWorkflowHandle> {
        let wf_storage = workflow_storage(self)?;
        let step_metadata = parse_step_metadata(step_metadata_json)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        let handle = flexiq_workflows::lifecycle::submit_workflow(
            &self.storage,
            &wf_storage,
            SubmitStaticWorkflowRequest {
                name: name.to_string(),
                version,
                dag_bytes,
                step_metadata,
                node_payloads,
                queue_default: queue_default.to_string(),
                params_json,
                deferred_node_names: deferred_node_names
                    .unwrap_or_default()
                    .into_iter()
                    .collect(),
                cache_hit_nodes: cache_hit_nodes.unwrap_or_default(),
                parent_run_id,
                parent_node_name,
                default_timeout_ms: self.default_timeout * 1000,
                default_priority: self.default_priority,
                default_max_retries: self.default_retry,
                result_ttl_ms: self.result_ttl_ms,
                namespace: self.namespace.clone(),
            },
        )
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        Ok(PyWorkflowHandle {
            run_id: handle.run_id,
            name: name.to_string(),
            definition_id: handle.definition_id,
        })
    }

    /// Cancel a workflow run and all of its sub-workflow descendants.
    ///
    /// Marks each visited run `Cancelled`, skips pending/ready nodes, and
    /// cancels their underlying jobs. Traversal is iterative with a visited
    /// set so that any accidental cycle in `parent_run_id` links terminates
    /// safely instead of recursing. Nodes already running are left alone
    /// (consistent with flexiq's existing cancel semantics).
    pub fn cancel_workflow_run(&self, py: Python<'_>, run_id: &str) -> PyResult<()> {
        let wf_storage = workflow_storage(self)?;
        let root = run_id.to_string();

        let result: CoreResult<()> = py.detach(|| {
            let mut visited: HashSet<String> = HashSet::new();
            let mut stack: Vec<String> = vec![root];
            let now = now_millis();

            while let Some(rid) = stack.pop() {
                if !visited.insert(rid.clone()) {
                    continue;
                }

                let nodes = wf_storage.get_workflow_nodes(&rid)?;
                cascade_skip_pending_nodes(
                    &self.storage,
                    &wf_storage,
                    &rid,
                    &nodes,
                    self.namespace.as_deref(),
                )?;

                wf_storage.update_workflow_run_state(&rid, WorkflowState::Cancelled, None)?;
                wf_storage.set_workflow_run_completed(&rid, now)?;

                match wf_storage.get_child_workflow_runs(&rid) {
                    Ok(children) => {
                        for child in children {
                            if !child.state.is_terminal() && !visited.contains(&child.id) {
                                stack.push(child.id);
                            }
                        }
                    }
                    Err(e) => {
                        log::warn!(
                            "[flexiq] get_child_workflow_runs({}) failed during cancel: {}",
                            rid,
                            e
                        );
                    }
                }
            }

            Ok(())
        });

        result.map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Override a run that finalized as `Failed` to `CompletedWithFailures`.
    ///
    /// Used by the Python tracker for `on_failure="continue"` runs that
    /// finished with a mix of completed and failed nodes when the workflow
    /// was constructed with `compensate_on_continue=True`. The Python
    /// tracker calls this after `mark_workflow_node_result` returns
    /// `Failed`, BEFORE handing off to the saga orchestrator.
    pub fn set_workflow_run_completed_with_failures(
        &self,
        py: Python<'_>,
        run_id: &str,
    ) -> PyResult<()> {
        let wf = workflow_storage(self)?;
        let rid = run_id.to_string();
        py.detach(|| wf.update_workflow_run_state(&rid, WorkflowState::CompletedWithFailures, None))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Check whether all workflow nodes are terminal and finalize the run.
    ///
    /// Called by the Python tracker after updating the fan-out parent status
    /// (e.g., after a failed fan-out). If all nodes are terminal, transitions
    /// the run to `Completed` or `Failed` and returns the final state string.
    /// Returns `None` if not all nodes are terminal yet.
    pub fn finalize_run_if_terminal(
        &self,
        py: Python<'_>,
        run_id: &str,
    ) -> PyResult<Option<String>> {
        let wf_storage = workflow_storage(self)?;
        let run_id_owned = run_id.to_string();

        let result: CoreResult<Option<String>> = py.detach(|| {
            let nodes = wf_storage.get_workflow_nodes(&run_id_owned)?;
            if !nodes.iter().all(|n| n.status.is_terminal()) {
                return Ok(None);
            }

            let any_failed = nodes.iter().any(|n| n.status == WorkflowNodeStatus::Failed);
            let final_state = if any_failed {
                WorkflowState::Failed
            } else {
                WorkflowState::Completed
            };

            let now = now_millis();
            wf_storage.update_workflow_run_state(
                &run_id_owned,
                final_state,
                if final_state == WorkflowState::Failed {
                    Some("fan-out child failed")
                } else {
                    None
                },
            )?;
            wf_storage.set_workflow_run_completed(&run_id_owned, now)?;
            Ok(Some(final_state.as_str().to_string()))
        });

        result.map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::py_queue::workflow_ops::test_helpers::*;
    use flexiq_core::storage::Storage;

    /// Submitting a 2-node linear DAG inserts a node per step, enqueues a job
    /// per step, and threads the predecessor's job id through the successor's
    /// `depends_on` chain.
    #[test]
    fn submit_workflow_links_linear_dag_via_depends_on() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|_py| {
            let queue = make_test_pyqueue();
            let dag = make_dag_bytes(&["a", "b"], &[("a", "b")]);
            let metadata = make_step_metadata_json(&[("a", "task_a"), ("b", "task_b")]);
            let payloads = make_node_payloads(&["a", "b"]);

            let handle = queue
                .submit_workflow(
                    "linear", 1, dag, &metadata, payloads, "default", None, None, None, None, None,
                )
                .unwrap();

            let wf = wf_storage(&queue);
            let mut nodes = wf.get_workflow_nodes(&handle.run_id).unwrap();
            nodes.sort_by(|x, y| x.node_name.cmp(&y.node_name));
            assert_eq!(nodes.len(), 2);
            assert_eq!(nodes[0].node_name, "a");
            assert_eq!(nodes[1].node_name, "b");

            let a_job_id = nodes[0].job_id.clone().expect("node a has a job");
            let b_job_id = nodes[1].job_id.clone().expect("node b has a job");

            let b_deps = queue.storage.get_dependencies(&b_job_id, None).unwrap();
            assert_eq!(b_deps, vec![a_job_id]);

            let run = wf.get_workflow_run(&handle.run_id).unwrap().unwrap();
            assert_eq!(run.state, WorkflowState::Running);
        });
    }

    /// Nodes listed in `deferred_node_names` get a `WorkflowNode` but no job;
    /// downstream successors omit the deferred predecessor from `depends_on`.
    #[test]
    fn submit_workflow_skips_job_creation_for_deferred_nodes() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|_py| {
            let queue = make_test_pyqueue();
            let dag = make_dag_bytes(&["a", "b"], &[("a", "b")]);
            let metadata = make_step_metadata_json(&[("a", "task_a"), ("b", "task_b")]);
            let payloads = make_node_payloads(&["a", "b"]);

            let handle = queue
                .submit_workflow(
                    "deferred",
                    1,
                    dag,
                    &metadata,
                    payloads,
                    "default",
                    None,
                    Some(vec!["a".to_string()]),
                    None,
                    None,
                    None,
                )
                .unwrap();

            let wf = wf_storage(&queue);
            let nodes = wf.get_workflow_nodes(&handle.run_id).unwrap();
            let a = nodes.iter().find(|n| n.node_name == "a").unwrap();
            let b = nodes.iter().find(|n| n.node_name == "b").unwrap();

            assert!(a.job_id.is_none(), "deferred node must not enqueue a job");
            assert_eq!(a.status, WorkflowNodeStatus::Pending);

            let b_job_id = b.job_id.clone().expect("non-deferred node enqueues a job");
            let b_deps = queue.storage.get_dependencies(&b_job_id, None).unwrap();
            assert!(
                b_deps.is_empty(),
                "successor must drop the deferred predecessor from depends_on"
            );
        });
    }

    /// Cache-hit nodes copy a `result_hash` from a previous run, skip job
    /// creation, and land in `CacheHit` state with a `completed_at` timestamp.
    /// The incremental layer only marks leaf-style nodes (no successors that
    /// need a real job id) as cache hits, so the test mirrors that invariant.
    #[test]
    fn submit_workflow_marks_cache_hit_nodes_terminal_without_job() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|_py| {
            let queue = make_test_pyqueue();
            let dag = make_dag_bytes(&["a"], &[]);
            let metadata = make_step_metadata_json(&[("a", "task_a")]);
            let payloads = make_node_payloads(&["a"]);
            let mut cache = HashMap::new();
            cache.insert("a".to_string(), "hash-of-a".to_string());

            let handle = queue
                .submit_workflow(
                    "cached",
                    1,
                    dag,
                    &metadata,
                    payloads,
                    "default",
                    None,
                    None,
                    None,
                    None,
                    Some(cache),
                )
                .unwrap();

            let wf = wf_storage(&queue);
            let nodes = wf.get_workflow_nodes(&handle.run_id).unwrap();
            let a = nodes.iter().find(|n| n.node_name == "a").unwrap();

            assert!(a.job_id.is_none());
            assert_eq!(a.status, WorkflowNodeStatus::CacheHit);
            assert_eq!(a.result_hash.as_deref(), Some("hash-of-a"));
            assert!(a.completed_at.is_some());
        });
    }

    /// A reused `(name, version)` returns the same definition id rather than
    /// inserting a duplicate row.
    #[test]
    fn submit_workflow_reuses_existing_definition_by_name_and_version() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|_py| {
            let queue = make_test_pyqueue();
            let dag = make_dag_bytes(&["a"], &[]);
            let metadata = make_step_metadata_json(&[("a", "task_a")]);

            let first = queue
                .submit_workflow(
                    "reused",
                    1,
                    dag.clone(),
                    &metadata,
                    make_node_payloads(&["a"]),
                    "default",
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .unwrap();
            let second = queue
                .submit_workflow(
                    "reused",
                    1,
                    dag,
                    &metadata,
                    make_node_payloads(&["a"]),
                    "default",
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .unwrap();

            assert_eq!(first.definition_id, second.definition_id);
            assert_ne!(first.run_id, second.run_id);
        });
    }

    /// Missing `step_metadata` for a non-deferred, non-cached node is a
    /// `PyValueError` — callers must supply per-step task config.
    #[test]
    fn submit_workflow_rejects_missing_step_metadata() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|_py| {
            let queue = make_test_pyqueue();
            let dag = make_dag_bytes(&["a", "b"], &[("a", "b")]);
            // step_metadata only describes 'a' — 'b' is intentionally missing.
            let metadata = make_step_metadata_json(&[("a", "task_a")]);
            let payloads = make_node_payloads(&["a", "b"]);

            let result = queue.submit_workflow(
                "broken", 1, dag, &metadata, payloads, "default", None, None, None, None, None,
            );
            let err = match result {
                Err(e) => e,
                Ok(_) => panic!("expected PyValueError for missing step_metadata"),
            };
            assert!(err.to_string().contains("missing from step_metadata"));
        });
    }

    /// Cancelling a run marks the run `Cancelled`, skips pending/ready nodes,
    /// and cancels their underlying jobs.
    #[test]
    fn cancel_workflow_run_cancels_pending_jobs_and_marks_terminal() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let queue = make_test_pyqueue();
            let wf = wf_storage(&queue);
            let run_id = seed_run(wf);
            let job_id = enqueue_test_job(&queue.storage, "task_pending");
            seed_node(
                wf,
                &run_id,
                "p",
                WorkflowNodeStatus::Pending,
                Some(job_id.clone()),
            );

            queue.cancel_workflow_run(py, &run_id).unwrap();

            let run = wf.get_workflow_run(&run_id).unwrap().unwrap();
            assert_eq!(run.state, WorkflowState::Cancelled);
            assert!(run.completed_at.is_some());
            assert_eq!(
                fetch_node(wf, &run_id, "p").status,
                WorkflowNodeStatus::Skipped,
            );
            let job = queue.storage.get_job(&job_id, None).unwrap().unwrap();
            assert_eq!(job.status.wire_name(), "Cancelled");
        });
    }

    /// `finalize_run_if_terminal` returns `None` while any node is still
    /// non-terminal and does not transition the run.
    #[test]
    fn finalize_run_if_terminal_is_noop_when_nodes_pending() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let queue = make_test_pyqueue();
            let wf = wf_storage(&queue);
            let run_id = seed_run(wf);
            seed_node(wf, &run_id, "p", WorkflowNodeStatus::Pending, None);
            seed_node(wf, &run_id, "done", WorkflowNodeStatus::Completed, None);

            let outcome = queue.finalize_run_if_terminal(py, &run_id).unwrap();
            assert!(outcome.is_none());

            let run = wf.get_workflow_run(&run_id).unwrap().unwrap();
            assert_eq!(run.state, WorkflowState::Running);
        });
    }

    /// All-success terminal nodes promote the run to `Completed`.
    #[test]
    fn finalize_run_if_terminal_promotes_run_to_completed_on_all_success() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let queue = make_test_pyqueue();
            let wf = wf_storage(&queue);
            let run_id = seed_run(wf);
            seed_node(wf, &run_id, "a", WorkflowNodeStatus::Completed, None);
            seed_node(wf, &run_id, "b", WorkflowNodeStatus::Completed, None);

            let outcome = queue.finalize_run_if_terminal(py, &run_id).unwrap();
            assert_eq!(outcome.as_deref(), Some("completed"));

            let run = wf.get_workflow_run(&run_id).unwrap().unwrap();
            assert_eq!(run.state, WorkflowState::Completed);
            assert!(run.completed_at.is_some());
        });
    }

    /// Any failed terminal node demotes the run to `Failed`.
    #[test]
    fn finalize_run_if_terminal_promotes_run_to_failed_on_any_failure() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let queue = make_test_pyqueue();
            let wf = wf_storage(&queue);
            let run_id = seed_run(wf);
            seed_node(wf, &run_id, "a", WorkflowNodeStatus::Completed, None);
            seed_node(wf, &run_id, "b", WorkflowNodeStatus::Failed, None);

            let outcome = queue.finalize_run_if_terminal(py, &run_id).unwrap();
            assert_eq!(outcome.as_deref(), Some("failed"));

            let run = wf.get_workflow_run(&run_id).unwrap().unwrap();
            assert_eq!(run.state, WorkflowState::Failed);
        });
    }

    /// `set_workflow_run_completed_with_failures` flips an already-failed run
    /// to `CompletedWithFailures` for `on_failure="continue"` semantics.
    #[test]
    fn set_workflow_run_completed_with_failures_overrides_failed_state() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let queue = make_test_pyqueue();
            let wf = wf_storage(&queue);
            let run_id = seed_run(wf);
            wf.update_workflow_run_state(&run_id, WorkflowState::Failed, Some("partial"))
                .unwrap();

            queue
                .set_workflow_run_completed_with_failures(py, &run_id)
                .unwrap();

            let run = wf.get_workflow_run(&run_id).unwrap().unwrap();
            assert_eq!(run.state, WorkflowState::CompletedWithFailures);
        });
    }
}
