//! Workflow operations on `PyQueue`.
//!
//! Compiled only when the `workflows` feature is enabled. Each submodule
//! holds a partial `#[pymethods]` impl block (enabled by pyo3's
//! `multiple-pymethods` feature) grouped by concern: lifecycle, node
//! mutations, fan-out/fan-in, gates, and read-only queries. Helpers shared
//! across the submodules live in this file.

mod fan_out;
mod gates;
mod lifecycle;
mod nodes;
mod queries;
mod saga;

use std::collections::HashMap;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use taskito_core::error::Result as CoreResult;
use taskito_core::storage::{Storage, StorageBackend};
#[cfg(feature = "postgres")]
use taskito_workflows::WorkflowPostgresStorage;
#[cfg(feature = "redis")]
use taskito_workflows::WorkflowRedisStorage;
use taskito_workflows::{
    StepMetadata, WorkflowNode, WorkflowNodeStatus, WorkflowSqliteStorage, WorkflowState,
    WorkflowStorage, WorkflowStorageBackend,
};

use crate::py_queue::PyQueue;

/// Return the queue's cached workflow storage, initializing it on first use.
///
/// Migrations run on first construction only; subsequent calls are a cheap
/// `OnceLock::get()`. Callers receive a cloned handle — every variant of
/// `WorkflowStorageBackend` wraps a pool handle so clones share the same
/// connection pool.
pub(super) fn workflow_storage(queue: &PyQueue) -> PyResult<WorkflowStorageBackend> {
    if let Some(wf) = queue.workflow_storage.get() {
        return Ok(wf.clone());
    }
    let wf = match &queue.storage {
        StorageBackend::Sqlite(s) => WorkflowSqliteStorage::new(s.clone(), queue.namespace.clone())
            .map(WorkflowStorageBackend::Sqlite)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
        #[cfg(feature = "postgres")]
        StorageBackend::Postgres(s) => {
            WorkflowPostgresStorage::new(s.clone(), queue.namespace.clone())
                .map(WorkflowStorageBackend::Postgres)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
        }
        #[cfg(feature = "redis")]
        StorageBackend::Redis(s) => WorkflowRedisStorage::new(s.clone(), queue.namespace.clone())
            .map(WorkflowStorageBackend::Redis)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
    };
    // If another thread raced us to initialize, our value is ignored — either
    // handle is equivalent because the underlying pool is shared.
    let _ = queue.workflow_storage.set(wf.clone());
    Ok(wf)
}

pub(super) fn parse_step_metadata(json: &str) -> PyResult<HashMap<String, StepMetadata>> {
    serde_json::from_str(json)
        .map_err(|e| PyValueError::new_err(format!("invalid step_metadata JSON: {e}")))
}

/// Build a job-metadata JSON blob that carries workflow routing info.
///
/// Uses `serde_json` to guarantee proper escaping of node names containing
/// backslashes, control characters, or Unicode — hand-rolled escaping previously
/// produced invalid JSON for such inputs.
pub(super) fn build_metadata_json(run_id: &str, node_name: &str) -> String {
    serde_json::json!({
        "workflow_run_id": run_id,
        "workflow_node_name": node_name,
    })
    .to_string()
}

pub(super) fn status_to_py(status: WorkflowState) -> String {
    status.as_str().to_string()
}

/// Mark every pending/ready node in a run as skipped and cancel its job.
///
/// Best-effort: per-node failures are logged but do not abort the sweep.
pub(super) fn cascade_skip_pending_nodes(
    storage: &StorageBackend,
    wf_storage: &WorkflowStorageBackend,
    run_id: &str,
    nodes: &[WorkflowNode],
    namespace: Option<&str>,
) -> CoreResult<()> {
    for node in nodes {
        if !matches!(
            node.status,
            WorkflowNodeStatus::Pending | WorkflowNodeStatus::Ready
        ) {
            continue;
        }
        if let Some(job_id) = &node.job_id {
            if let Err(e) = storage.cancel_job(job_id, namespace) {
                log::warn!(
                    "[taskito] cancel_job({}) failed during cascade skip for run {}: {}",
                    job_id,
                    run_id,
                    e
                );
            }
        }
        if let Err(e) = wf_storage.update_workflow_node_status(
            run_id,
            &node.node_name,
            WorkflowNodeStatus::Skipped,
        ) {
            log::warn!(
                "[taskito] skip node '{}' failed for run {}: {}",
                node.node_name,
                run_id,
                e
            );
        }
    }
    Ok(())
}

#[cfg(test)]
pub(super) mod test_helpers;

#[cfg(test)]
mod tests {
    use super::test_helpers::*;
    use super::*;

    #[test]
    fn build_metadata_json_round_trips_special_characters() {
        let json = build_metadata_json("run-1", "node\\with\"quotes\nand\ttabs");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["workflow_run_id"], "run-1");
        assert_eq!(v["workflow_node_name"], "node\\with\"quotes\nand\ttabs");
    }

    #[test]
    fn build_metadata_json_preserves_unicode_node_names() {
        let json = build_metadata_json("run-2", "ノード/ステップ");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["workflow_node_name"], "ノード/ステップ");
    }

    #[test]
    fn parse_step_metadata_round_trips_minimal_payload() {
        let json = r#"{
            "extract": {
                "task_name": "task_extract",
                "queue": null,
                "args_template": null,
                "kwargs_template": null,
                "max_retries": null,
                "timeout_ms": null,
                "priority": null,
                "fan_out": null,
                "fan_in": null,
                "condition": null
            }
        }"#;
        let map = parse_step_metadata(json).unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map["extract"].task_name, "task_extract");
    }

    #[test]
    fn parse_step_metadata_rejects_invalid_json() {
        // PyValueError construction needs a live Python interpreter.
        pyo3::Python::initialize();
        let err = parse_step_metadata("not-json").unwrap_err();
        assert!(err.to_string().contains("invalid step_metadata JSON"));
    }

    #[test]
    fn status_to_py_returns_canonical_strings() {
        assert_eq!(status_to_py(WorkflowState::Pending), "pending");
        assert_eq!(status_to_py(WorkflowState::Running), "running");
        assert_eq!(status_to_py(WorkflowState::Completed), "completed");
        assert_eq!(status_to_py(WorkflowState::Failed), "failed");
        assert_eq!(status_to_py(WorkflowState::Cancelled), "cancelled");
    }

    #[test]
    fn cascade_skip_skips_pending_and_ready_only() {
        let (storage, wf_storage) = make_storages();
        let run_id = seed_run(&wf_storage);
        seed_node(&wf_storage, &run_id, "p", WorkflowNodeStatus::Pending, None);
        seed_node(&wf_storage, &run_id, "r", WorkflowNodeStatus::Ready, None);
        seed_node(
            &wf_storage,
            &run_id,
            "running",
            WorkflowNodeStatus::Running,
            None,
        );
        seed_node(
            &wf_storage,
            &run_id,
            "done",
            WorkflowNodeStatus::Completed,
            None,
        );

        let nodes = wf_storage.get_workflow_nodes(&run_id).unwrap();
        cascade_skip_pending_nodes(&storage, &wf_storage, &run_id, &nodes, None).unwrap();

        assert_eq!(
            fetch_node(&wf_storage, &run_id, "p").status,
            WorkflowNodeStatus::Skipped,
        );
        assert_eq!(
            fetch_node(&wf_storage, &run_id, "r").status,
            WorkflowNodeStatus::Skipped,
        );
        assert_eq!(
            fetch_node(&wf_storage, &run_id, "running").status,
            WorkflowNodeStatus::Running,
        );
        assert_eq!(
            fetch_node(&wf_storage, &run_id, "done").status,
            WorkflowNodeStatus::Completed,
        );
    }

    #[test]
    fn cascade_skip_cancels_pending_node_jobs() {
        let (storage, wf_storage) = make_storages();
        let run_id = seed_run(&wf_storage);

        let pending_job_id = enqueue_test_job(&storage, "task_pending");
        let running_job_id = enqueue_test_job(&storage, "task_running");
        seed_node(
            &wf_storage,
            &run_id,
            "p",
            WorkflowNodeStatus::Pending,
            Some(pending_job_id.clone()),
        );
        seed_node(
            &wf_storage,
            &run_id,
            "running",
            WorkflowNodeStatus::Running,
            Some(running_job_id.clone()),
        );

        let nodes = wf_storage.get_workflow_nodes(&run_id).unwrap();
        cascade_skip_pending_nodes(&storage, &wf_storage, &run_id, &nodes, None).unwrap();

        let pending_job = storage.get_job(&pending_job_id, None).unwrap().unwrap();
        assert_eq!(pending_job.status.wire_name(), "Cancelled");

        let running_job = storage.get_job(&running_job_id, None).unwrap().unwrap();
        assert_ne!(running_job.status.wire_name(), "Cancelled");
    }

    #[test]
    fn cascade_skip_is_a_noop_for_empty_node_slice() {
        let (storage, wf_storage) = make_storages();
        let run_id = seed_run(&wf_storage);
        cascade_skip_pending_nodes(&storage, &wf_storage, &run_id, &[], None).unwrap();
        assert!(wf_storage.get_workflow_nodes(&run_id).unwrap().is_empty());
    }
}
