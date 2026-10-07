//! `SubmitWorkflow` and `GetWorkflowRun`.
//!
//! `SubmitWorkflow` executes only statically-sequenceable graphs (D26 of
//! `tasks/specs/2026-09-01-flexiq-v1-proto-design.md`): a node setting `gate`,
//! `cache`, `fan_out`, `fan_in` or `sub_workflow` is refused before anything is
//! written, because nothing reachable from this process advances a dynamic
//! construct outside a live SDK tracker. A graph that clears the check gets
//! exactly what `PyQueue::submit_workflow`'s static path produces today — see
//! `flexiq_workflows::lifecycle::submit_workflow`, the one implementation both
//! call.

use std::collections::HashMap;

use flexiq_core::job::Job;
use flexiq_core::storage::Storage;
use flexiq_core::StorageBackend;
use flexiq_workflows::lifecycle::{self, SubmitStaticWorkflowRequest, SubmitWorkflowError};
use flexiq_workflows::{
    StepMetadata, WorkflowNode, WorkflowRun, WorkflowStorage, WorkflowStorageBackend,
};
use tonic::{Response, Status};

use super::convert::{self, DEFAULT_QUEUE, DEFAULT_TIMEOUT_MS};
use super::structured;
use super::Scoped;
use crate::grpc::audit::TargetKind;
use crate::grpc::blocking::on_storage_and_workflows;
use crate::grpc::pb;
use crate::grpc::status::WireError;

/// A caller mistake (`InvalidStepMetadata`) reads `INVALID_ARGUMENT`, the same
/// code every other request-shape refusal on this door uses; a storage
/// failure keeps the ordinary `QueueError` classification.
impl From<SubmitWorkflowError> for WireError {
    fn from(err: SubmitWorkflowError) -> Self {
        match err {
            SubmitWorkflowError::InvalidStepMetadata(msg) => WireError::invalid_request(msg),
            SubmitWorkflowError::Storage(err) => WireError::from_queue_error(&err),
        }
    }
}

/// A node with no `priority` of its own takes this — the same zero value
/// `EnqueueOptions.priority` defaults to.
const DEFAULT_PRIORITY: i32 = 0;
/// A node with no `max_retries` of its own takes this — the same default
/// `EnqueueOptions.max_retries` has.
const DEFAULT_MAX_RETRIES: i32 = 0;

pub(crate) async fn submit_workflow(
    scoped: &Scoped<'_>,
    request: pb::SubmitWorkflowRequest,
) -> Result<Response<pb::SubmitWorkflowResponse>, Status> {
    let graph = request
        .graph
        .ok_or_else(|| WireError::invalid_request("graph is required"))?;
    refuse_dynamic_constructs(&graph)?;
    admit_graph(scoped, &graph)?;
    let (dag_bytes, step_metadata, node_payloads) = compile_graph(graph)?;

    let namespace = scoped.namespace().to_string();
    let submit_request = SubmitStaticWorkflowRequest {
        name: request.name,
        version: 1,
        dag_bytes,
        step_metadata,
        node_payloads,
        queue_default: DEFAULT_QUEUE.to_string(),
        params_json: request.params_json,
        deferred_node_names: Default::default(),
        cache_hit_nodes: Default::default(),
        parent_run_id: None,
        parent_node_name: None,
        default_timeout_ms: DEFAULT_TIMEOUT_MS,
        default_priority: DEFAULT_PRIORITY,
        default_max_retries: DEFAULT_MAX_RETRIES,
        result_ttl_ms: None,
        namespace: Some(namespace),
        enqueued_by: Some(scoped.token_id().to_string()),
    };

    let handle = on_storage_and_workflows(
        scoped.storage(),
        scoped.workflows(),
        move |storage, workflows| lifecycle::submit_workflow(storage, workflows, submit_request),
    )
    .await?;
    scoped.audit(TargetKind::WorkflowRun, handle.run_id.clone());

    Ok(Response::new(pb::SubmitWorkflowResponse {
        run_id: handle.run_id,
    }))
}

pub(crate) async fn get_workflow_run(
    scoped: &Scoped<'_>,
    request: pb::GetWorkflowRunRequest,
) -> Result<Response<pb::GetWorkflowRunResponse>, Status> {
    let run_id = request.run_id.clone();
    scoped.audit(TargetKind::WorkflowRun, run_id.clone());
    let namespace = scoped.namespace().to_string();
    // Only a narrowed caller pays for reading the nodes' jobs back.
    let narrowed = !scoped.reaches(None, None);
    let (run, nodes, jobs) = on_storage_and_workflows(
        scoped.storage(),
        scoped.workflows(),
        move |storage, workflows| {
            read_run(storage, workflows, &run_id, &namespace, narrowed)
                .map_err(|error| WireError::from_queue_error(&error))
        },
    )
    .await?;

    let not_found = || Status::not_found(format!("workflow run '{}' not found", request.run_id));
    let run = run.ok_or_else(not_found)?;
    if narrowed && !run_within_grants(scoped, &nodes, &jobs) {
        return Err(not_found());
    }

    Ok(Response::new(pb::GetWorkflowRunResponse {
        run: Some(convert::workflow_run_to_wire(run)),
        nodes: nodes
            .into_iter()
            .map(convert::workflow_node_to_wire)
            .collect(),
    }))
}

/// A run, its nodes, and — when `with_jobs` — every job those nodes name.
fn read_run(
    storage: &StorageBackend,
    workflows: &WorkflowStorageBackend,
    run_id: &str,
    namespace: &str,
    with_jobs: bool,
) -> flexiq_core::Result<(Option<WorkflowRun>, Vec<WorkflowNode>, Vec<Job>)> {
    let Some(run) = workflows.get_workflow_run(run_id)? else {
        return Ok((None, Vec::new(), Vec::new()));
    };
    let nodes = workflows.get_workflow_nodes(run_id)?;
    let jobs = if with_jobs {
        let ids: Vec<&str> = nodes.iter().flat_map(node_job_ids).collect();
        storage.get_jobs_by_ids(&ids, Some(namespace))?
    } else {
        Vec::new()
    };
    Ok((Some(run), nodes, jobs))
}

/// Whether the caller's grants reach every job the run's nodes name (#990).
///
/// The node rows carry no queue or task, and the definition's metadata is
/// shared by every run of that name and version, so the jobs are the only
/// record of where the work landed. Fails closed: a node with no job (deferred,
/// a cache hit) or whose job retention removed has no queue to check, and a run
/// with no nodes has nothing to vouch for it.
fn run_within_grants(scoped: &Scoped<'_>, nodes: &[WorkflowNode], jobs: &[Job]) -> bool {
    let jobs: HashMap<&str, &Job> = jobs.iter().map(|job| (job.id.as_str(), job)).collect();
    !nodes.is_empty()
        && nodes.iter().all(|node| {
            node.job_id.is_some()
                && node_job_ids(node).all(|id| {
                    jobs.get(id)
                        .is_some_and(|job| scoped.reaches(Some(&job.queue), Some(&job.task_name)))
                })
        })
}

/// The forward job and, under a saga's rollback, the compensation job.
fn node_job_ids(node: &WorkflowNode) -> impl Iterator<Item = &str> {
    node.job_id
        .iter()
        .chain(node.compensation_job_id.iter())
        .map(String::as_str)
}

/// Refuse the whole call if any node sets a construct nothing can advance yet.
///
/// Checked before anything is written to storage: a partially-written refusal
/// would leave a definition or run row behind for a submission the caller was
/// told did not happen.
fn refuse_dynamic_constructs(graph: &pb::WorkflowGraph) -> Result<(), Status> {
    for node in &graph.node_configs {
        let field = if node.gate.is_some() {
            "gate"
        } else if node.cache.is_some() {
            "cache"
        } else if node.fan_out.is_some() {
            "fan_out"
        } else if node.fan_in.is_some() {
            "fan_in"
        } else if node.sub_workflow.is_some() {
            "sub_workflow"
        } else {
            continue;
        };
        return Err(WireError::workflow_construct_unsupported(&node.name, field).into());
    }
    Ok(())
}

/// Refuse the whole graph if any node would enqueue onto a queue or task the
/// caller's grants do not reach, naming the first such node (#990).
///
/// Checked before anything is written, on the queue the node's job lands in:
/// an empty or absent queue is `default` by then, as it is for `Enqueue`.
fn admit_graph(scoped: &Scoped<'_>, graph: &pb::WorkflowGraph) -> Result<(), WireError> {
    for node in &graph.node_configs {
        let queue = node
            .queue
            .as_deref()
            .filter(|queue| !queue.is_empty())
            .unwrap_or(DEFAULT_QUEUE);
        scoped
            .require(Some(queue), Some(&node.task_name))
            .and_then(|()| admit_compensation(scoped, node))
            .map_err(|error| error.at_node(&node.name))?;
    }
    Ok(())
}

/// A rollback task is enqueued later, by whichever worker runs the saga, on a
/// queue that worker picks rather than the graph. With no queue to check, the
/// grants must reach the task on every queue.
fn admit_compensation(scoped: &Scoped<'_>, node: &pb::WorkflowNodeConfig) -> Result<(), WireError> {
    match node.compensate.as_deref() {
        Some(task) => scoped.require(None, Some(task)),
        None => Ok(()),
    }
}

/// A compiled graph: the bare DAG's JSON bytes, a name-keyed `StepMetadata`
/// map, and a name-keyed map of each node's encoded payload.
type CompiledGraph = (
    Vec<u8>,
    HashMap<String, StepMetadata>,
    HashMap<String, Vec<u8>>,
);

/// Compile a `WorkflowGraph` into the shape `lifecycle::submit_workflow`
/// takes: a bare graph's JSON bytes, matching what
/// `dagron_core::SerializableGraph` produces, and a name-keyed `StepMetadata`
/// map. Never the reverse — this service never holds or forwards
/// `dag_data` bytes (§7.6).
fn compile_graph(graph: pb::WorkflowGraph) -> Result<CompiledGraph, WireError> {
    let dag = serde_json::json!({
        "nodes": graph.nodes.iter().map(|n| serde_json::json!({"name": n.name})).collect::<Vec<_>>(),
        "edges": graph.edges.iter().map(|e| serde_json::json!({
            "from": e.from, "to": e.to, "weight": 1.0,
        })).collect::<Vec<_>>(),
    });
    let dag_bytes = serde_json::to_vec(&dag)
        .map_err(|e| WireError::invalid_request(format!("graph did not encode: {e}")))?;

    let mut step_metadata = HashMap::with_capacity(graph.node_configs.len());
    let mut node_payloads = HashMap::with_capacity(graph.node_configs.len());
    for node in graph.node_configs {
        let payload = match node.body {
            Some(pb::workflow_node_config::Body::Raw(bytes)) => bytes,
            Some(pb::workflow_node_config::Body::Structured(args)) => structured::encode(args)?,
            None => {
                return Err(WireError::invalid_request(format!(
                    "node '{}' has no body arm set; send raw = \"\" for a node with no payload",
                    node.name
                )))
            }
        };
        node_payloads.insert(node.name.clone(), payload);

        step_metadata.insert(
            node.name.clone(),
            StepMetadata {
                task_name: node.task_name,
                // The wire's "empty means default"; left as `Some("")`, the job
                // would land in a queue named `""` instead.
                queue: node.queue.filter(|queue| !queue.is_empty()),
                args_template: None,
                kwargs_template: None,
                max_retries: node.max_retries,
                timeout_ms: node.timeout.as_ref().map(convert::millis_from_duration),
                priority: node.priority,
                fan_out: None,
                fan_in: None,
                condition: condition_to_str(node.condition),
                gate: None,
                sub_workflow: None,
                compensate: node.compensate,
                cache: None,
            },
        );
    }

    Ok((dag_bytes, step_metadata, node_payloads))
}

/// `EdgeCondition` as `StepMetadata.condition`'s string convention. Only the
/// three static values exist on the wire (a `callable` predicate is code, not
/// data, and is not representable here) — `_UNSPECIFIED` and `ALWAYS` both
/// mean "no filter", which `StepMetadata.condition = None` already expresses.
fn condition_to_str(condition: i32) -> Option<String> {
    match pb::EdgeCondition::try_from(condition).unwrap_or(pb::EdgeCondition::Unspecified) {
        pb::EdgeCondition::Unspecified | pb::EdgeCondition::Always => None,
        pb::EdgeCondition::OnSuccess => Some("on_success".to_string()),
        pb::EdgeCondition::OnFailure => Some("on_failure".to_string()),
    }
}
