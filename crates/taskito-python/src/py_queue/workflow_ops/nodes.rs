//! Node-level mutations: mark result, transition status, fail, skip.

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use taskito_core::error::Result as CoreResult;
use taskito_core::job::now_millis;
use taskito_core::storage::Storage;
use taskito_workflows::{WorkflowNodeStatus, WorkflowState, WorkflowStorage};

use crate::py_queue::workflow_ops::{cascade_skip_pending_nodes, workflow_storage};
use crate::py_queue::PyQueue;

#[pymethods]
impl PyQueue {
    /// Record the terminal outcome of a workflow node's job.
    ///
    /// Called by the Python workflow tracker in response to
    /// `JOB_COMPLETED`/`JOB_FAILED`/`JOB_DEAD`/`JOB_CANCELLED` events.
    /// On failure, walks remaining pending nodes in the run and skips them
    /// (fail-fast semantics) unless ``skip_cascade`` is true. When the run
    /// transitions to a terminal state, returns
    /// `(run_id, node_name, final_state_str)`. Otherwise returns
    /// `(run_id, node_name, None)` so the tracker can decide whether to
    /// trigger fan-out expansion or fan-in collection.
    ///
    /// Set ``skip_cascade=True`` for tracker-managed runs (those with
    /// conditions or ``on_failure="continue"``) so the Python tracker can
    /// handle selective skip/create decisions.
    #[pyo3(signature = (job_id, succeeded, error=None, skip_cascade=false, result_hash=None))]
    pub fn mark_workflow_node_result(
        &self,
        py: Python<'_>,
        job_id: &str,
        succeeded: bool,
        error: Option<String>,
        skip_cascade: bool,
        result_hash: Option<String>,
    ) -> PyResult<Option<(String, String, Option<String>)>> {
        let wf_storage = workflow_storage(self)?;
        let job_id_owned = job_id.to_string();

        enum Outcome {
            NotFound,
            NoWorkflowMetadata,
            Settled {
                run_id: String,
                node_name: String,
                final_state: Option<String>,
            },
        }

        let outcome: CoreResult<Outcome> = py.detach(|| {
            let job = match self
                .storage
                .get_job(&job_id_owned, self.namespace.as_deref())?
            {
                Some(j) => j,
                None => return Ok(Outcome::NotFound),
            };

            let metadata_json = match &job.metadata {
                Some(m) => m,
                None => return Ok(Outcome::NoWorkflowMetadata),
            };
            let parsed: serde_json::Value = match serde_json::from_str(metadata_json) {
                Ok(v) => v,
                Err(_) => return Ok(Outcome::NoWorkflowMetadata),
            };
            let run_id = match parsed.get("workflow_run_id").and_then(|v| v.as_str()) {
                Some(id) => id.to_string(),
                None => return Ok(Outcome::NoWorkflowMetadata),
            };
            let node_name = match parsed.get("workflow_node_name").and_then(|v| v.as_str()) {
                Some(n) => n.to_string(),
                None => return Ok(Outcome::NoWorkflowMetadata),
            };

            let now = now_millis();
            if succeeded {
                wf_storage.set_workflow_node_completed(
                    &run_id,
                    &node_name,
                    now,
                    result_hash.as_deref(),
                )?;
            } else {
                let err_msg = error.clone().unwrap_or_else(|| "failed".to_string());
                wf_storage.set_workflow_node_error(&run_id, &node_name, &err_msg)?;
            }

            // Fail-fast: cascade failure to pending/ready nodes. Skipped when the
            // Python tracker manages cascade (conditions / continue mode).
            if !succeeded && !skip_cascade {
                let nodes = wf_storage.get_workflow_nodes(&run_id)?;
                cascade_skip_pending_nodes(
                    &self.storage,
                    &wf_storage,
                    &run_id,
                    &nodes,
                    self.namespace.as_deref(),
                )?;
            }

            // Note: fan-out parent status is NOT updated here. The tracker calls
            // `check_fan_out_completion` which uses a CAS to finalize exactly once.

            let nodes = wf_storage.get_workflow_nodes(&run_id)?;
            let all_terminal = nodes.iter().all(|n| n.status.is_terminal());
            if !all_terminal {
                return Ok(Outcome::Settled {
                    run_id,
                    node_name,
                    final_state: None,
                });
            }

            let any_failed = nodes.iter().any(|n| n.status == WorkflowNodeStatus::Failed);
            let final_state = if any_failed || !succeeded {
                WorkflowState::Failed
            } else {
                WorkflowState::Completed
            };

            wf_storage.update_workflow_run_state(
                &run_id,
                final_state,
                if final_state == WorkflowState::Failed {
                    error.as_deref()
                } else {
                    None
                },
            )?;
            wf_storage.set_workflow_run_completed(&run_id, now)?;

            Ok(Outcome::Settled {
                run_id,
                node_name,
                final_state: Some(final_state.as_str().to_string()),
            })
        });

        match outcome.map_err(|e| PyRuntimeError::new_err(e.to_string()))? {
            Outcome::NotFound => Err(PyValueError::new_err(format!("job '{job_id}' not found"))),
            Outcome::NoWorkflowMetadata => Ok(None),
            Outcome::Settled {
                run_id,
                node_name,
                final_state,
            } => Ok(Some((run_id, node_name, final_state))),
        }
    }

    /// Transition a workflow node to `WaitingApproval` status.
    ///
    /// Used by the Python tracker when a gate node becomes evaluable.
    pub fn set_workflow_node_waiting_approval(
        &self,
        py: Python<'_>,
        run_id: &str,
        node_name: &str,
    ) -> PyResult<()> {
        let wf_storage = workflow_storage(self)?;
        let run_id_owned = run_id.to_string();
        let node_name_owned = node_name.to_string();

        py.detach(|| {
            wf_storage.update_workflow_node_status(
                &run_id_owned,
                &node_name_owned,
                WorkflowNodeStatus::WaitingApproval,
            )
        })
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Transition a workflow node to `Running` with a `started_at` timestamp.
    ///
    /// Used by the Python tracker to promote sub-workflow parent nodes after
    /// the child workflow has been successfully compiled and submitted. This
    /// is the clean counterpart to the old "waiting-approval → skip → running"
    /// dance that could leave nodes permanently skipped on compile failure.
    pub fn set_workflow_node_running(
        &self,
        py: Python<'_>,
        run_id: &str,
        node_name: &str,
    ) -> PyResult<()> {
        let wf_storage = workflow_storage(self)?;
        let run_id_owned = run_id.to_string();
        let node_name_owned = node_name.to_string();

        py.detach(|| {
            wf_storage.set_workflow_node_running(&run_id_owned, &node_name_owned, now_millis())
        })
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Set a node's fan_out_count and transition to Running.
    pub fn set_workflow_node_fan_out_count(
        &self,
        py: Python<'_>,
        run_id: &str,
        node_name: &str,
        count: i32,
    ) -> PyResult<()> {
        let wf_storage = workflow_storage(self)?;
        let run_id_owned = run_id.to_string();
        let node_name_owned = node_name.to_string();

        py.detach(|| {
            wf_storage.set_workflow_node_fan_out_count(&run_id_owned, &node_name_owned, count)
        })
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Mark a workflow node as `Failed` with an error message.
    ///
    /// Used by the Python tracker when sub-workflow compilation or submission
    /// fails — the parent node needs a terminal state so the outer run can
    /// finalize instead of hanging.
    pub fn fail_workflow_node(
        &self,
        py: Python<'_>,
        run_id: &str,
        node_name: &str,
        error: &str,
    ) -> PyResult<()> {
        let wf_storage = workflow_storage(self)?;
        let run_id_owned = run_id.to_string();
        let node_name_owned = node_name.to_string();
        let error_owned = error.to_string();

        py.detach(|| {
            wf_storage.set_workflow_node_error(&run_id_owned, &node_name_owned, &error_owned)
        })
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Mark a single workflow node as `Skipped` and cancel its job.
    ///
    /// Used by the Python tracker for condition-based skip propagation.
    /// Cancel-job failures are logged but do not abort the skip — the node's
    /// terminal status is more important than best-effort job cancellation.
    pub fn skip_workflow_node(
        &self,
        py: Python<'_>,
        run_id: &str,
        node_name: &str,
    ) -> PyResult<()> {
        let wf_storage = workflow_storage(self)?;
        let run_id_owned = run_id.to_string();
        let node_name_owned = node_name.to_string();

        let result: CoreResult<()> = py.detach(|| {
            let node = wf_storage.get_workflow_node(&run_id_owned, &node_name_owned)?;
            if let Some(node) = node {
                if let Some(job_id) = &node.job_id {
                    if let Err(e) = self.storage.cancel_job(job_id, self.namespace.as_deref()) {
                        log::warn!(
                            "[taskito] cancel_job({}) failed while skipping node '{}' in run {}: {}",
                            job_id,
                            node_name_owned,
                            run_id_owned,
                            e
                        );
                    }
                }
                wf_storage.update_workflow_node_status(
                    &run_id_owned,
                    &node_name_owned,
                    WorkflowNodeStatus::Skipped,
                )?;
            }
            Ok(())
        });

        result.map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::py_queue::workflow_ops::test_helpers::*;

    /// Mark a node successful when there is no matching workflow metadata on
    /// the job. The method returns `None` (signalling a non-workflow job) and
    /// leaves the run unchanged.
    #[test]
    fn mark_workflow_node_result_returns_none_for_non_workflow_job() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let queue = make_test_pyqueue();
            let job_id = enqueue_test_job(&queue.storage, "standalone_task");

            let outcome = queue
                .mark_workflow_node_result(py, &job_id, true, None, false, None)
                .unwrap();
            assert!(outcome.is_none());
        });
    }

    /// Missing job id surfaces as a `PyValueError` rather than a silent no-op.
    #[test]
    fn mark_workflow_node_result_errors_on_unknown_job_id() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let queue = make_test_pyqueue();
            let result =
                queue.mark_workflow_node_result(py, "no-such-job", true, None, false, None);
            let err = match result {
                Err(e) => e,
                Ok(_) => panic!("expected PyValueError for missing job"),
            };
            assert!(err.to_string().contains("not found"));
        });
    }

    /// Marking the last terminal node successful transitions the run to
    /// `Completed` and reports the final state back to the caller.
    #[test]
    fn mark_workflow_node_result_finalizes_run_on_last_success() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let queue = make_test_pyqueue();
            let wf = wf_storage(&queue);
            let run_id = seed_run(wf);
            let job_id = enqueue_workflow_job(&queue.storage, &run_id, "leaf");
            seed_node(
                wf,
                &run_id,
                "leaf",
                WorkflowNodeStatus::Running,
                Some(job_id.clone()),
            );

            let outcome = queue
                .mark_workflow_node_result(py, &job_id, true, None, false, None)
                .unwrap()
                .unwrap();
            assert_eq!(outcome.0, run_id);
            assert_eq!(outcome.1, "leaf");
            assert_eq!(outcome.2.as_deref(), Some("completed"));

            let run = wf.get_workflow_run(&run_id).unwrap().unwrap();
            assert_eq!(run.state, WorkflowState::Completed);
        });
    }

    /// On failure (without `skip_cascade`), siblings in `Pending`/`Ready`
    /// status are skipped and the run finalizes as `Failed`.
    #[test]
    fn mark_workflow_node_result_cascades_failure_to_pending_siblings() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let queue = make_test_pyqueue();
            let wf = wf_storage(&queue);
            let run_id = seed_run(wf);
            let failing_job = enqueue_workflow_job(&queue.storage, &run_id, "fail");
            let pending_job = enqueue_test_job(&queue.storage, "task_pending");
            seed_node(
                wf,
                &run_id,
                "fail",
                WorkflowNodeStatus::Running,
                Some(failing_job.clone()),
            );
            seed_node(
                wf,
                &run_id,
                "pending",
                WorkflowNodeStatus::Pending,
                Some(pending_job.clone()),
            );

            let outcome = queue
                .mark_workflow_node_result(
                    py,
                    &failing_job,
                    false,
                    Some("boom".to_string()),
                    false,
                    None,
                )
                .unwrap()
                .unwrap();
            assert_eq!(outcome.2.as_deref(), Some("failed"));

            assert_eq!(
                fetch_node(wf, &run_id, "pending").status,
                WorkflowNodeStatus::Skipped,
            );
            let pending = queue.storage.get_job(&pending_job, None).unwrap().unwrap();
            assert_eq!(pending.status.wire_name(), "Cancelled");
        });
    }

    /// `skip_cascade=true` records the failure but leaves pending siblings
    /// alone — the tracker handles the cascade for conditional/continue runs.
    #[test]
    fn mark_workflow_node_result_skips_cascade_when_requested() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let queue = make_test_pyqueue();
            let wf = wf_storage(&queue);
            let run_id = seed_run(wf);
            let failing_job = enqueue_workflow_job(&queue.storage, &run_id, "fail");
            let pending_job = enqueue_test_job(&queue.storage, "task_pending");
            seed_node(
                wf,
                &run_id,
                "fail",
                WorkflowNodeStatus::Running,
                Some(failing_job.clone()),
            );
            seed_node(
                wf,
                &run_id,
                "pending",
                WorkflowNodeStatus::Pending,
                Some(pending_job.clone()),
            );

            queue
                .mark_workflow_node_result(py, &failing_job, false, None, true, None)
                .unwrap();

            assert_eq!(
                fetch_node(wf, &run_id, "pending").status,
                WorkflowNodeStatus::Pending,
                "skip_cascade must leave pending siblings untouched",
            );
            let pending = queue.storage.get_job(&pending_job, None).unwrap().unwrap();
            assert_ne!(pending.status.wire_name(), "Cancelled");
        });
    }

    /// Success carries the `result_hash` through to the persisted node so the
    /// incremental layer can resolve future cache hits.
    #[test]
    fn mark_workflow_node_result_persists_result_hash_on_success() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let queue = make_test_pyqueue();
            let wf = wf_storage(&queue);
            let run_id = seed_run(wf);
            let job_id = enqueue_workflow_job(&queue.storage, &run_id, "hashed");
            seed_node(
                wf,
                &run_id,
                "hashed",
                WorkflowNodeStatus::Running,
                Some(job_id.clone()),
            );

            queue
                .mark_workflow_node_result(
                    py,
                    &job_id,
                    true,
                    None,
                    false,
                    Some("sha-of-result".to_string()),
                )
                .unwrap();

            let node = fetch_node(wf, &run_id, "hashed");
            assert_eq!(node.status, WorkflowNodeStatus::Completed);
            assert_eq!(node.result_hash.as_deref(), Some("sha-of-result"));
        });
    }

    /// Gate hand-off: the node flips to `WaitingApproval` and stays there
    /// until externally resolved.
    #[test]
    fn set_workflow_node_waiting_approval_transitions_node() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let queue = make_test_pyqueue();
            let wf = wf_storage(&queue);
            let run_id = seed_run(wf);
            seed_node(wf, &run_id, "gate", WorkflowNodeStatus::Pending, None);

            queue
                .set_workflow_node_waiting_approval(py, &run_id, "gate")
                .unwrap();

            assert_eq!(
                fetch_node(wf, &run_id, "gate").status,
                WorkflowNodeStatus::WaitingApproval,
            );
        });
    }

    /// Sub-workflow parent promotion: the node flips to `Running` and gets a
    /// non-null `started_at`.
    #[test]
    fn set_workflow_node_running_stamps_started_at() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let queue = make_test_pyqueue();
            let wf = wf_storage(&queue);
            let run_id = seed_run(wf);
            seed_node(wf, &run_id, "child", WorkflowNodeStatus::Pending, None);

            queue
                .set_workflow_node_running(py, &run_id, "child")
                .unwrap();

            let node = fetch_node(wf, &run_id, "child");
            assert_eq!(node.status, WorkflowNodeStatus::Running);
            assert!(node.started_at.is_some());
        });
    }

    /// `set_workflow_node_fan_out_count` records the count and transitions
    /// the parent to `Running`, gating downstream finalization on the count.
    #[test]
    fn set_workflow_node_fan_out_count_records_count_and_promotes() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let queue = make_test_pyqueue();
            let wf = wf_storage(&queue);
            let run_id = seed_run(wf);
            seed_node(wf, &run_id, "parent", WorkflowNodeStatus::Pending, None);

            queue
                .set_workflow_node_fan_out_count(py, &run_id, "parent", 5)
                .unwrap();

            let node = fetch_node(wf, &run_id, "parent");
            assert_eq!(node.fan_out_count, Some(5));
            assert_eq!(node.status, WorkflowNodeStatus::Running);
        });
    }

    /// `fail_workflow_node` records the error message on the node and flips
    /// the status to `Failed` without touching siblings.
    #[test]
    fn fail_workflow_node_marks_failed_with_error() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let queue = make_test_pyqueue();
            let wf = wf_storage(&queue);
            let run_id = seed_run(wf);
            seed_node(wf, &run_id, "broken", WorkflowNodeStatus::Pending, None);
            seed_node(wf, &run_id, "sibling", WorkflowNodeStatus::Pending, None);

            queue
                .fail_workflow_node(py, &run_id, "broken", "compile error")
                .unwrap();

            let broken = fetch_node(wf, &run_id, "broken");
            assert_eq!(broken.status, WorkflowNodeStatus::Failed);
            assert_eq!(broken.error.as_deref(), Some("compile error"));
            assert_eq!(
                fetch_node(wf, &run_id, "sibling").status,
                WorkflowNodeStatus::Pending,
            );
        });
    }

    /// `skip_workflow_node` cancels the backing job (if any) and marks the
    /// node `Skipped`.
    #[test]
    fn skip_workflow_node_cancels_job_and_marks_skipped() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let queue = make_test_pyqueue();
            let wf = wf_storage(&queue);
            let run_id = seed_run(wf);
            let job_id = enqueue_test_job(&queue.storage, "task_to_skip");
            seed_node(
                wf,
                &run_id,
                "skipped",
                WorkflowNodeStatus::Pending,
                Some(job_id.clone()),
            );

            queue.skip_workflow_node(py, &run_id, "skipped").unwrap();

            assert_eq!(
                fetch_node(wf, &run_id, "skipped").status,
                WorkflowNodeStatus::Skipped,
            );
            let job = queue.storage.get_job(&job_id, None).unwrap().unwrap();
            assert_eq!(job.status.wire_name(), "Cancelled");
        });
    }

    /// Skipping an unknown node name is a no-op rather than an error — the
    /// tracker may race the storage and we accept that.
    #[test]
    fn skip_workflow_node_is_noop_for_unknown_node() {
        pyo3::Python::initialize();
        pyo3::Python::attach(|py| {
            let queue = make_test_pyqueue();
            let wf = wf_storage(&queue);
            let run_id = seed_run(wf);

            queue
                .skip_workflow_node(py, &run_id, "ghost")
                .expect("missing node is a no-op, not an error");
            assert!(wf.get_workflow_nodes(&run_id).unwrap().is_empty());
        });
    }
}
