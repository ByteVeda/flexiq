//! Test helpers shared across `workflow_ops` submodule test blocks.
//!
//! Provides:
//! * `make_test_pyqueue` — builds a `PyQueue` backed by an in-memory SQLite
//!   storage with sane defaults, suitable for direct invocation of
//!   `#[pymethods]` from Rust.
//! * `make_storages` — bare storage pair (no `PyQueue`) for helper-level
//!   tests that only exercise the free functions.
//! * Seed helpers for definitions, runs, nodes, and pre-enqueued jobs.
//! * DAG / step-metadata constructors that mirror what the Python builder
//!   sends to `submit_workflow`.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use serde_json::json;

use taskito_core::job::{now_millis, NewJob};
use taskito_core::storage::sqlite::SqliteStorage;
use taskito_core::storage::{Storage, StorageBackend};
use taskito_workflows::{
    StepMetadata, WorkflowDefinition, WorkflowNode, WorkflowNodeStatus, WorkflowRun,
    WorkflowSqliteStorage, WorkflowState, WorkflowStorage, WorkflowStorageBackend,
};

use crate::py_queue::PyQueue;

/// Construct an in-memory SQLite storage pair (job storage + workflow storage)
/// sharing a single underlying connection pool.
pub(crate) fn make_storages() -> (StorageBackend, WorkflowStorageBackend) {
    let sql = SqliteStorage::in_memory().unwrap();
    let backend = StorageBackend::Sqlite(sql.clone());
    let wf = WorkflowSqliteStorage::new(sql, None).unwrap();
    (backend, WorkflowStorageBackend::Sqlite(wf))
}

/// Construct a `PyQueue` wired to in-memory SQLite, with workflow storage
/// pre-initialized. Suitable for invoking `#[pymethods]` directly in Rust
/// tests — no Python construction, just a struct literal.
pub(crate) fn make_test_pyqueue() -> PyQueue {
    let (storage, wf_backend) = make_storages();
    let workflow_storage = std::sync::OnceLock::new();
    workflow_storage.set(wf_backend).ok();
    PyQueue {
        storage,
        db_path: ":memory:".to_string(),
        num_workers: 1,
        default_retry: 3,
        default_timeout: 300,
        default_priority: 0,
        shutdown_flag: Arc::new(AtomicBool::new(false)),
        result_ttl_ms: None,
        retention: None,
        scheduler_poll_interval_ms: 50,
        scheduler_reap_interval: 100,
        scheduler_cleanup_interval: 1200,
        scheduler_batch_size: 1,
        dlq_auto_retry_delay_ms: None,
        dlq_auto_retry_max: 1,
        auto_migrate: true,
        namespace: None,
        push_dispatch: false,
        dispatcher: Arc::new(Mutex::new(None)),
        workflow_storage,
    }
}

/// Resolve the workflow storage handle from a test `PyQueue`. Panics if
/// `make_test_pyqueue` was not used to seed the `OnceLock` (it is).
pub(crate) fn wf_storage(queue: &PyQueue) -> &WorkflowStorageBackend {
    queue
        .workflow_storage
        .get()
        .expect("workflow storage seeded by make_test_pyqueue")
}

/// Enqueue a throwaway job and return its id. Useful for seeding nodes that
/// reference a real job, so that cancel-job assertions can observe the
/// transition.
pub(crate) fn enqueue_test_job(storage: &StorageBackend, task_name: &str) -> String {
    storage
        .enqueue(NewJob {
            queue: "default".to_string(),
            task_name: task_name.to_string(),
            payload: vec![1, 2, 3],
            priority: 0,
            scheduled_at: now_millis(),
            max_retries: 3,
            timeout_ms: 300_000,
            unique_key: None,
            metadata: None,
            notes: None,
            depends_on: vec![],
            expires_at: None,
            result_ttl_ms: None,
            namespace: None,
        })
        .unwrap()
        .id
}

/// Enqueue a job whose metadata carries the workflow routing blob the
/// tracker uses to associate `JOB_*` events with a node. Returns the job id.
pub(crate) fn enqueue_workflow_job(
    storage: &StorageBackend,
    run_id: &str,
    node_name: &str,
) -> String {
    let metadata = json!({
        "workflow_run_id": run_id,
        "workflow_node_name": node_name,
    })
    .to_string();
    storage
        .enqueue(NewJob {
            queue: "default".to_string(),
            task_name: format!("task_{node_name}"),
            payload: vec![1, 2, 3],
            priority: 0,
            scheduled_at: now_millis(),
            max_retries: 3,
            timeout_ms: 300_000,
            unique_key: None,
            metadata: Some(metadata),
            notes: None,
            depends_on: vec![],
            expires_at: None,
            result_ttl_ms: None,
            namespace: None,
        })
        .unwrap()
        .id
}

/// Seed a workflow definition + a `Running` run and return the run id.
pub(crate) fn seed_run(wf_storage: &WorkflowStorageBackend) -> String {
    let now = now_millis();
    let definition = WorkflowDefinition {
        id: uuid::Uuid::now_v7().to_string(),
        name: "audit_pipeline".to_string(),
        version: 1,
        dag_data: vec![],
        step_metadata: HashMap::new(),
        created_at: now,
    };
    wf_storage.create_workflow_definition(&definition).unwrap();

    let run = WorkflowRun {
        id: uuid::Uuid::now_v7().to_string(),
        definition_id: definition.id,
        params: None,
        state: WorkflowState::Running,
        started_at: Some(now),
        completed_at: None,
        error: None,
        parent_run_id: None,
        parent_node_name: None,
        created_at: now,
    };
    let run_id = run.id.clone();
    wf_storage.create_workflow_run(&run).unwrap();
    run_id
}

/// Insert a node row directly via the workflow storage.
pub(crate) fn seed_node(
    wf_storage: &WorkflowStorageBackend,
    run_id: &str,
    node_name: &str,
    status: WorkflowNodeStatus,
    job_id: Option<String>,
) -> WorkflowNode {
    let node = WorkflowNode {
        id: uuid::Uuid::now_v7().to_string(),
        run_id: run_id.to_string(),
        node_name: node_name.to_string(),
        job_id,
        status,
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
    };
    wf_storage.create_workflow_node(&node).unwrap();
    node
}

/// Look up a node by name and unwrap. Panics if missing — tests want the
/// short-circuit on missing nodes.
pub(crate) fn fetch_node(
    wf_storage: &WorkflowStorageBackend,
    run_id: &str,
    node_name: &str,
) -> WorkflowNode {
    wf_storage
        .get_workflow_nodes(run_id)
        .unwrap()
        .into_iter()
        .find(|n| n.node_name == node_name)
        .unwrap()
}

/// Serialize a `SerializableGraph`-compatible DAG payload from a list of
/// nodes and `(from, to)` edges. Mirrors what the Python builder emits.
pub(crate) fn make_dag_bytes(nodes: &[&str], edges: &[(&str, &str)]) -> Vec<u8> {
    let nodes_json: Vec<_> = nodes.iter().map(|n| json!({"name": n})).collect();
    let edges_json: Vec<_> = edges
        .iter()
        .map(|(from, to)| json!({"from": from, "to": to, "weight": 1.0}))
        .collect();
    serde_json::to_vec(&json!({"nodes": nodes_json, "edges": edges_json})).unwrap()
}

/// Build a `step_metadata` JSON blob mapping each `(node_name, task_name)`
/// pair to a minimal `StepMetadata`. Mirrors what the Python builder emits.
pub(crate) fn make_step_metadata_json(steps: &[(&str, &str)]) -> String {
    let map: HashMap<String, StepMetadata> = steps
        .iter()
        .map(|(node, task)| {
            (
                (*node).to_string(),
                StepMetadata {
                    task_name: (*task).to_string(),
                    ..Default::default()
                },
            )
        })
        .collect();
    serde_json::to_string(&map).unwrap()
}

/// Build `node_payloads` keyed by node name with throwaway bytes.
pub(crate) fn make_node_payloads(nodes: &[&str]) -> HashMap<String, Vec<u8>> {
    nodes
        .iter()
        .map(|n| ((*n).to_string(), vec![0u8; 4]))
        .collect()
}
