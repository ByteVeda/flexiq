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

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use flexiq_core::error::Result as CoreResult;
use flexiq_core::storage::{Storage, StorageBackend};
#[cfg(feature = "postgres")]
use flexiq_workflows::WorkflowPostgresStorage;
#[cfg(feature = "redis")]
use flexiq_workflows::WorkflowRedisStorage;
use flexiq_workflows::{
    WorkflowNode, WorkflowNodeStatus, WorkflowSqliteStorage, WorkflowState, WorkflowStorage,
    WorkflowStorageBackend,
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
        // A queue opened with `auto_migrate=False` gates every schema change
        // behind `migrate()`, including the workflow tables — otherwise the
        // first workflow call would quietly apply DDL the operator withheld.
        StorageBackend::Sqlite(s) => if queue.auto_migrate {
            WorkflowSqliteStorage::new(s.clone(), queue.namespace.clone())
        } else {
            WorkflowSqliteStorage::unmigrated(s.clone(), queue.namespace.clone())
        }
        .map(WorkflowStorageBackend::Sqlite)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
        #[cfg(feature = "postgres")]
        StorageBackend::Postgres(s) => if queue.auto_migrate {
            WorkflowPostgresStorage::new(s.clone(), queue.namespace.clone())
        } else {
            WorkflowPostgresStorage::unmigrated(s.clone(), queue.namespace.clone())
        }
        .map(WorkflowStorageBackend::Postgres)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
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

/// Apply pending workflow schema changes and report the versions applied.
///
/// Built unmigrated first so the versions this call applies are visible in the
/// return value rather than swallowed by the constructor.
pub(crate) fn migrate_workflow_storage(queue: &PyQueue, py: Python<'_>) -> PyResult<Vec<String>> {
    let wf = match &queue.storage {
        StorageBackend::Sqlite(s) => {
            WorkflowSqliteStorage::unmigrated(s.clone(), queue.namespace.clone())
                .map(WorkflowStorageBackend::Sqlite)
        }
        #[cfg(feature = "postgres")]
        StorageBackend::Postgres(s) => {
            WorkflowPostgresStorage::unmigrated(s.clone(), queue.namespace.clone())
                .map(WorkflowStorageBackend::Postgres)
        }
        #[cfg(feature = "redis")]
        StorageBackend::Redis(s) => WorkflowRedisStorage::new(s.clone(), queue.namespace.clone())
            .map(WorkflowStorageBackend::Redis),
    }
    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    let applied = py
        .detach(|| wf.migrate())
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let _ = queue.workflow_storage.set(wf);
    Ok(applied)
}

/// Refuse a `run_id` this queue's namespace cannot see.
///
/// The fan-out and deferred paths enqueue the job *before* binding it to its
/// node. A scoped bind against a foreign run has no effect, which would leave
/// the job running untracked — so refuse before anything is enqueued.
pub(super) fn require_visible_run(wf: &WorkflowStorageBackend, run_id: &str) -> CoreResult<()> {
    if wf.get_workflow_run(run_id)?.is_none() {
        return Err(flexiq_core::error::QueueError::Other(format!(
            "workflow run not found: {run_id}"
        )));
    }
    Ok(())
}

/// Re-exported rather than duplicated: `flexiq-workflows` is the one place
/// this logic lives now (`crates/flexiq-workflows/src/lifecycle.rs`), shared
/// with `flexiq-server`'s `SubmitWorkflow` handler. `fan_out.rs` is the
/// remaining caller here.
pub(super) use flexiq_workflows::lifecycle::build_metadata_json;

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
                    "[flexiq] cancel_job({}) failed during cascade skip for run {}: {}",
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
                "[flexiq] skip node '{}' failed for run {}: {}",
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

    // build_metadata_json's and parse_step_metadata's own tests moved with
    // them to crates/flexiq-workflows/src/lifecycle.rs.

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
