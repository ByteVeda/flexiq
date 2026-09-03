//! Submitting a statically-sequenceable workflow: pre-enqueue every step as a
//! `Job` with a `depends_on` chain mirroring the graph's edges, so the core
//! scheduler runs them in order with no runtime tracker involved.
//!
//! Pyo3- and napi-free on purpose: `flexiq-python`'s `submit_workflow`
//! `#[pymethods]` and `flexiq-server`'s `SubmitWorkflow` gRPC handler both call
//! [`submit_workflow`] directly rather than each carrying their own copy.

use std::collections::{HashMap, HashSet};

use flexiq_core::error::Result;
use flexiq_core::job::{now_millis, NewJob};
use flexiq_core::storage::{Storage, StorageBackend};

use crate::error::WorkflowError;
use crate::{
    topological_order, StepMetadata, WorkflowDefinition, WorkflowNode, WorkflowNodeStatus,
    WorkflowRun, WorkflowState, WorkflowStorage, WorkflowStorageBackend,
};

/// Everything [`submit_workflow`] needs beyond the two storage handles.
///
/// `deferred_node_names` and `cache_hit_nodes` stay even though `SubmitWorkflow`
/// (the gRPC RPC) always passes both empty — it refuses a graph needing either
/// before this function is ever called (see the RPC's own validation) — because
/// `flexiq-python`'s existing dynamic-workflow callers (fan-out expansion,
/// sub-workflow submission) still need them.
pub struct SubmitStaticWorkflowRequest {
    pub name: String,
    pub version: i32,
    pub dag_bytes: Vec<u8>,
    pub step_metadata: HashMap<String, StepMetadata>,
    pub node_payloads: HashMap<String, Vec<u8>>,
    pub queue_default: String,
    pub params_json: Option<String>,
    pub deferred_node_names: HashSet<String>,
    pub cache_hit_nodes: HashMap<String, String>,
    pub parent_run_id: Option<String>,
    pub parent_node_name: Option<String>,
    /// A node with no `timeout_ms` of its own takes this. Milliseconds — the
    /// caller converts whatever unit it stores this in, once, at the call
    /// site, rather than this function guessing at one.
    pub default_timeout_ms: i64,
    pub default_priority: i32,
    pub default_max_retries: i32,
    pub result_ttl_ms: Option<i64>,
    pub namespace: Option<String>,
}

/// What a successful submission produced.
#[derive(Debug)]
pub struct WorkflowRunHandle {
    pub run_id: String,
    pub definition_id: String,
}

/// Parse a `step_metadata` JSON blob into the map [`submit_workflow`] takes.
pub fn parse_step_metadata(json: &str) -> Result<HashMap<String, StepMetadata>> {
    serde_json::from_str(json)
        .map_err(|e| WorkflowError::InvalidStepMetadata(format!("invalid JSON: {e}")).into())
}

/// Build a job-metadata JSON blob carrying workflow routing info.
///
/// `serde_json` guarantees proper escaping of node names containing
/// backslashes, control characters or Unicode.
pub fn build_metadata_json(run_id: &str, node_name: &str) -> String {
    serde_json::json!({
        "workflow_run_id": run_id,
        "workflow_node_name": node_name,
    })
    .to_string()
}

/// Submit a workflow for static execution.
///
/// Creates (or reuses) a `WorkflowDefinition` with the given name + version,
/// inserts a `WorkflowRun`, pre-enqueues all non-deferred, non-cache-hit step
/// jobs in topological order with `depends_on` chains so the core scheduler
/// runs them in the correct order. Nodes listed in `deferred_node_names` get a
/// `WorkflowNode` only (no job) — their jobs are created at runtime by a
/// caller's own tracker.
pub fn submit_workflow(
    storage: &StorageBackend,
    wf_storage: &WorkflowStorageBackend,
    request: SubmitStaticWorkflowRequest,
) -> Result<WorkflowRunHandle> {
    let ordered = topological_order(&request.dag_bytes)?;

    let definition_id =
        match wf_storage.get_workflow_definition(&request.name, Some(request.version))? {
            Some(existing) => existing.id,
            None => {
                let def = WorkflowDefinition {
                    id: uuid::Uuid::now_v7().to_string(),
                    name: request.name.clone(),
                    version: request.version,
                    dag_data: request.dag_bytes.clone(),
                    step_metadata: request.step_metadata.clone(),
                    created_at: now_millis(),
                };
                let def_id = def.id.clone();
                wf_storage.create_workflow_definition(&def)?;
                def_id
            }
        };

    let run_id = uuid::Uuid::now_v7().to_string();
    let now = now_millis();
    let run = WorkflowRun {
        id: run_id.clone(),
        definition_id: definition_id.clone(),
        params: request.params_json,
        state: WorkflowState::Pending,
        started_at: Some(now),
        completed_at: None,
        error: None,
        parent_run_id: request.parent_run_id,
        parent_node_name: request.parent_node_name,
        created_at: now,
    };
    wf_storage.create_workflow_run(&run)?;

    let mut job_ids: HashMap<String, String> = HashMap::new();
    for topo in &ordered {
        // Cache-hit nodes: copy result_hash from a previous run, no job.
        if let Some(rh) = request.cache_hit_nodes.get(&topo.name) {
            let wf_node = WorkflowNode {
                id: uuid::Uuid::now_v7().to_string(),
                run_id: run_id.clone(),
                node_name: topo.name.clone(),
                job_id: None,
                status: WorkflowNodeStatus::CacheHit,
                result_hash: Some(rh.clone()),
                fan_out_count: None,
                fan_in_data: None,
                started_at: None,
                completed_at: Some(now),
                error: None,
                compensation_job_id: None,
                compensation_started_at: None,
                compensation_completed_at: None,
                compensation_error: None,
            };
            wf_storage.create_workflow_node(&wf_node)?;
            continue;
        }

        // Deferred nodes: WorkflowNode only, no job.
        if request.deferred_node_names.contains(&topo.name) {
            let wf_node = WorkflowNode {
                id: uuid::Uuid::now_v7().to_string(),
                run_id: run_id.clone(),
                node_name: topo.name.clone(),
                job_id: None,
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
            };
            wf_storage.create_workflow_node(&wf_node)?;
            continue;
        }

        let meta = request.step_metadata.get(&topo.name).ok_or_else(|| {
            WorkflowError::InvalidStepMetadata(format!(
                "step '{}' missing from step_metadata",
                topo.name
            ))
        })?;
        let payload = request
            .node_payloads
            .get(&topo.name)
            .cloned()
            .ok_or_else(|| {
                WorkflowError::InvalidStepMetadata(format!(
                    "step '{}' missing from node_payloads",
                    topo.name
                ))
            })?;

        // Only resolve depends_on for non-deferred predecessors.
        let depends_on: Vec<String> = topo
            .predecessors
            .iter()
            .filter(|p| !request.deferred_node_names.contains(*p))
            .map(|p| {
                job_ids.get(p).cloned().ok_or_else(|| {
                    WorkflowError::InvalidStepMetadata(format!(
                        "predecessor '{}' of step '{}' has no job id",
                        p, topo.name
                    ))
                    .into()
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let timeout_ms = meta.timeout_ms.unwrap_or(request.default_timeout_ms);
        let new_job = NewJob {
            queue: meta
                .queue
                .clone()
                .unwrap_or_else(|| request.queue_default.clone()),
            task_name: meta.task_name.clone(),
            payload,
            priority: meta.priority.unwrap_or(request.default_priority),
            scheduled_at: now,
            max_retries: meta.max_retries.unwrap_or(request.default_max_retries),
            timeout_ms,
            unique_key: None,
            metadata: Some(build_metadata_json(&run_id, &topo.name)),
            notes: None,
            depends_on,
            expires_at: None,
            result_ttl_ms: request.result_ttl_ms,
            namespace: request.namespace.clone(),
            debounce_key: None,
        };

        let job = storage.enqueue(new_job)?;
        job_ids.insert(topo.name.clone(), job.id.clone());

        let wf_node = WorkflowNode {
            id: uuid::Uuid::now_v7().to_string(),
            run_id: run_id.clone(),
            node_name: topo.name.clone(),
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
        };
        wf_storage.create_workflow_node(&wf_node)?;
    }

    wf_storage.update_workflow_run_state(&run_id, WorkflowState::Running, None)?;

    Ok(WorkflowRunHandle {
        run_id,
        definition_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlite_store::WorkflowSqliteStorage;
    use flexiq_core::storage::sqlite::SqliteStorage;

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
                "condition": null,
                "gate": null,
                "sub_workflow": null,
                "compensate": null,
                "cache": null
            }
        }"#;
        let map = parse_step_metadata(json).unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map["extract"].task_name, "task_extract");
    }

    #[test]
    fn parse_step_metadata_rejects_invalid_json() {
        let err = parse_step_metadata("not-json").unwrap_err();
        assert!(err.to_string().contains("invalid step_metadata"));
    }

    fn make_storages() -> (StorageBackend, WorkflowStorageBackend) {
        let base = SqliteStorage::in_memory().unwrap();
        let storage = StorageBackend::Sqlite(base.clone());
        let wf = WorkflowStorageBackend::Sqlite(WorkflowSqliteStorage::new(base, None).unwrap());
        (storage, wf)
    }

    fn dag_bytes(nodes: &[&str], edges: &[(&str, &str)]) -> Vec<u8> {
        let nodes_json: Vec<_> = nodes
            .iter()
            .map(|n| serde_json::json!({"name": n}))
            .collect();
        let edges_json: Vec<_> = edges
            .iter()
            .map(|(f, t)| serde_json::json!({"from": f, "to": t, "weight": 1.0}))
            .collect();
        serde_json::to_vec(&serde_json::json!({"nodes": nodes_json, "edges": edges_json})).unwrap()
    }

    fn step_metadata_json(steps: &[(&str, &str)]) -> String {
        let map: HashMap<_, _> = steps
            .iter()
            .map(|(name, task)| {
                (
                    name.to_string(),
                    serde_json::json!({
                        "task_name": task, "queue": null, "args_template": null,
                        "kwargs_template": null, "max_retries": null, "timeout_ms": null,
                        "priority": null, "fan_out": null, "fan_in": null, "condition": null,
                        "gate": null, "sub_workflow": null, "compensate": null, "cache": null,
                    }),
                )
            })
            .collect();
        serde_json::to_string(&map).unwrap()
    }

    fn node_payloads(names: &[&str]) -> HashMap<String, Vec<u8>> {
        names
            .iter()
            .map(|n| (n.to_string(), vec![1, 2, 3]))
            .collect()
    }

    fn base_request(
        name: &str,
        dag: Vec<u8>,
        metadata: &str,
        payloads: HashMap<String, Vec<u8>>,
    ) -> SubmitStaticWorkflowRequest {
        SubmitStaticWorkflowRequest {
            name: name.to_string(),
            version: 1,
            dag_bytes: dag,
            step_metadata: parse_step_metadata(metadata).unwrap(),
            node_payloads: payloads,
            queue_default: "default".to_string(),
            params_json: None,
            deferred_node_names: HashSet::new(),
            cache_hit_nodes: HashMap::new(),
            parent_run_id: None,
            parent_node_name: None,
            default_timeout_ms: 300_000,
            default_priority: 0,
            default_max_retries: 0,
            result_ttl_ms: None,
            namespace: None,
        }
    }

    /// Submitting a 2-node linear DAG inserts a node per step, enqueues a job
    /// per step, and threads the predecessor's job id through the successor's
    /// `depends_on` chain.
    #[test]
    fn submit_workflow_links_linear_dag_via_depends_on() {
        let (storage, wf) = make_storages();
        let dag = dag_bytes(&["a", "b"], &[("a", "b")]);
        let metadata = step_metadata_json(&[("a", "task_a"), ("b", "task_b")]);
        let request = base_request("linear", dag, &metadata, node_payloads(&["a", "b"]));

        let handle = submit_workflow(&storage, &wf, request).unwrap();

        let mut nodes = wf.get_workflow_nodes(&handle.run_id).unwrap();
        nodes.sort_by(|x, y| x.node_name.cmp(&y.node_name));
        assert_eq!(nodes.len(), 2);
        let a_job_id = nodes[0].job_id.clone().expect("node a has a job");
        let b_job_id = nodes[1].job_id.clone().expect("node b has a job");
        let b_deps = storage.get_dependencies(&b_job_id, None).unwrap();
        assert_eq!(b_deps, vec![a_job_id]);

        let run = wf.get_workflow_run(&handle.run_id).unwrap().unwrap();
        assert_eq!(run.state, WorkflowState::Running);
    }

    /// Nodes listed in `deferred_node_names` get a `WorkflowNode` but no job;
    /// downstream successors omit the deferred predecessor from `depends_on`.
    #[test]
    fn submit_workflow_skips_job_creation_for_deferred_nodes() {
        let (storage, wf) = make_storages();
        let dag = dag_bytes(&["a", "b"], &[("a", "b")]);
        let metadata = step_metadata_json(&[("a", "task_a"), ("b", "task_b")]);
        let mut request = base_request("deferred", dag, &metadata, node_payloads(&["a", "b"]));
        request.deferred_node_names.insert("a".to_string());

        let handle = submit_workflow(&storage, &wf, request).unwrap();

        let nodes = wf.get_workflow_nodes(&handle.run_id).unwrap();
        let a = nodes.iter().find(|n| n.node_name == "a").unwrap();
        let b = nodes.iter().find(|n| n.node_name == "b").unwrap();
        assert!(a.job_id.is_none(), "deferred node must not enqueue a job");
        assert_eq!(a.status, WorkflowNodeStatus::Pending);
        let b_job_id = b.job_id.clone().expect("non-deferred node enqueues a job");
        assert!(storage
            .get_dependencies(&b_job_id, None)
            .unwrap()
            .is_empty());
    }

    /// Cache-hit nodes copy a `result_hash` from a previous run, skip job
    /// creation, and land in `CacheHit` state with a `completed_at` timestamp.
    #[test]
    fn submit_workflow_marks_cache_hit_nodes_terminal_without_job() {
        let (storage, wf) = make_storages();
        let dag = dag_bytes(&["a"], &[]);
        let metadata = step_metadata_json(&[("a", "task_a")]);
        let mut request = base_request("cached", dag, &metadata, node_payloads(&["a"]));
        request
            .cache_hit_nodes
            .insert("a".to_string(), "hash-of-a".to_string());

        let handle = submit_workflow(&storage, &wf, request).unwrap();

        let nodes = wf.get_workflow_nodes(&handle.run_id).unwrap();
        let a = nodes.iter().find(|n| n.node_name == "a").unwrap();
        assert!(a.job_id.is_none());
        assert_eq!(a.status, WorkflowNodeStatus::CacheHit);
        assert_eq!(a.result_hash.as_deref(), Some("hash-of-a"));
        assert!(a.completed_at.is_some());
    }

    /// A reused `(name, version)` returns the same definition id rather than
    /// inserting a duplicate row.
    #[test]
    fn submit_workflow_reuses_existing_definition_by_name_and_version() {
        let (storage, wf) = make_storages();
        let dag = dag_bytes(&["a"], &[]);
        let metadata = step_metadata_json(&[("a", "task_a")]);

        let first = submit_workflow(
            &storage,
            &wf,
            base_request("reused", dag.clone(), &metadata, node_payloads(&["a"])),
        )
        .unwrap();
        let second = submit_workflow(
            &storage,
            &wf,
            base_request("reused", dag, &metadata, node_payloads(&["a"])),
        )
        .unwrap();

        assert_eq!(first.definition_id, second.definition_id);
        assert_ne!(first.run_id, second.run_id);
    }

    /// Missing `step_metadata` for a non-deferred, non-cached node is refused.
    #[test]
    fn submit_workflow_rejects_missing_step_metadata() {
        let (storage, wf) = make_storages();
        let dag = dag_bytes(&["a", "b"], &[("a", "b")]);
        // step_metadata only describes 'a' — 'b' is intentionally missing.
        let metadata = step_metadata_json(&[("a", "task_a")]);
        let request = base_request("broken", dag, &metadata, node_payloads(&["a", "b"]));

        let err = submit_workflow(&storage, &wf, request).unwrap_err();
        assert!(err.to_string().contains("missing from step_metadata"));
    }
}
