//! JNI entry points backing `org.byteveda.taskito.internal.NativeWorkflows`.
//!
//! Compiled only with the `workflows` feature. Static DAG steps are pre-enqueued
//! with a `depends_on` chain so the core scheduler runs them in topological
//! order; the worker-side `WorkflowTracker` records each node's terminal outcome
//! and drives the run forward. Deferred steps (fan-out / fan-in, and anything
//! downstream of them) carry a node row but no job at submit; the tracker
//! expands a fan-out into per-item child jobs and collects them into a fan-in
//! job at runtime via [`expand_fan_out`] / [`create_deferred_job`]. Approval
//! gates, conditional nodes, and sub-workflows are also deferred: the tracker
//! parks a gate node ([`set_workflow_node_waiting_approval`]) until resolved,
//! skips a node whose condition is false ([`skip_workflow_node`]), and submits a
//! sub-workflow as a child run (`submitWorkflow` with a parent link), resolving
//! the parent node when the child run finalizes.

use std::collections::{HashMap, HashSet};

use jni::objects::{JByteArray, JClass, JObjectArray, JString};
use jni::sys::{jboolean, jint, jlong, jobjectArray, jstring, JNI_FALSE};
use jni::JNIEnv;
use serde::{Deserialize, Serialize};
use taskito_core::job::{now_millis, NewJob};
use taskito_core::Storage;
use taskito_workflows::dagron_core::DAG;
use taskito_workflows::{
    topological_order, StepMetadata, WorkflowDefinition, WorkflowNode, WorkflowNodeStatus,
    WorkflowRun, WorkflowState, WorkflowStorage, WorkflowStorageBackend,
};
use uuid::Uuid;

use crate::backend::QueueHandle;
use crate::convert::{parse_json, to_json};
use crate::error::BindingError;
use crate::ffi::{
    guard, new_string, new_string_array, read_bytes, read_bytes_array, read_optional_string,
    read_string, read_string_array,
};

const DEFAULT_QUEUE: &str = "default";
const DEFAULT_MAX_RETRIES: i32 = 3;
const DEFAULT_TIMEOUT_MS: i64 = 300_000;
/// Largest number of children one fan-out may expand into — guards against a
/// producer returning an enormous list and flooding storage in one batch.
const MAX_FAN_OUT: usize = 10_000;

/// One step as described by the Java builder. Edges come from `after`.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StepSpec {
    name: String,
    task_name: String,
    #[serde(default)]
    after: Vec<String>,
    queue: Option<String>,
    max_retries: Option<i32>,
    timeout_ms: Option<i64>,
    priority: Option<i32>,
    /// Fan-out strategy (e.g. "each") — this node expands per predecessor result item.
    fan_out: Option<String>,
    /// Fan-in strategy (e.g. "all") — this node collects its fan-out children's results.
    fan_in: Option<String>,
    /// Condition controlling whether this node runs: "on_success" / "on_failure"
    /// / "always", or "callable" when the tracker holds a Java predicate.
    condition: Option<String>,
    /// JSON `{timeoutMs, onTimeout, message}` marking an approval gate node.
    gate: Option<String>,
    /// Serialized child-workflow spec marking a sub-workflow node.
    sub_workflow: Option<String>,
    /// Rollback task name — the saga runs it to compensate this node.
    compensate: Option<String>,
    /// JSON `{ttlMs}` marking a cacheable node whose result the tracker may reuse.
    cache: Option<String>,
}

/// Combined run + node snapshot returned by `getWorkflowStatus`. Timestamps are
/// Unix milliseconds; `state`/`status` are the lowercase wire strings.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkflowStatusView<'a> {
    run_id: &'a str,
    state: &'static str,
    started_at: Option<i64>,
    completed_at: Option<i64>,
    error: Option<&'a str>,
    nodes: Vec<NodeView<'a>>,
}

/// One node within a run.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeView<'a> {
    node_name: &'a str,
    status: &'static str,
    job_id: Option<&'a str>,
    result_hash: Option<&'a str>,
    fan_out_count: Option<i32>,
    started_at: Option<i64>,
    completed_at: Option<i64>,
    error: Option<&'a str>,
    compensation_job_id: Option<&'a str>,
    compensation_started_at: Option<i64>,
    compensation_completed_at: Option<i64>,
    compensation_error: Option<&'a str>,
}

impl<'a> From<&'a WorkflowNode> for NodeView<'a> {
    fn from(n: &'a WorkflowNode) -> Self {
        Self {
            node_name: &n.node_name,
            status: n.status.as_str(),
            job_id: n.job_id.as_deref(),
            result_hash: n.result_hash.as_deref(),
            fan_out_count: n.fan_out_count,
            started_at: n.started_at,
            completed_at: n.completed_at,
            error: n.error.as_deref(),
            compensation_job_id: n.compensation_job_id.as_deref(),
            compensation_started_at: n.compensation_started_at,
            compensation_completed_at: n.compensation_completed_at,
            compensation_error: n.compensation_error.as_deref(),
        }
    }
}

/// A workflow run summary (no node detail — `getWorkflowStatus` provides that).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkflowRunView<'a> {
    id: &'a str,
    definition_id: &'a str,
    state: &'static str,
    params: Option<&'a str>,
    error: Option<&'a str>,
    started_at: Option<i64>,
    completed_at: Option<i64>,
    created_at: i64,
    parent_run_id: Option<&'a str>,
    parent_node_name: Option<&'a str>,
}

impl<'a> From<&'a WorkflowRun> for WorkflowRunView<'a> {
    fn from(r: &'a WorkflowRun) -> Self {
        Self {
            id: &r.id,
            definition_id: &r.definition_id,
            state: r.state.as_str(),
            params: r.params.as_deref(),
            error: r.error.as_deref(),
            started_at: r.started_at,
            completed_at: r.completed_at,
            created_at: r.created_at,
            parent_run_id: r.parent_run_id.as_deref(),
            parent_node_name: r.parent_node_name.as_deref(),
        }
    }
}

/// Borrow the queue behind a handle.
///
/// # Safety
/// `handle` must be a live `QueueHandle` pointer (see [`crate::handle`]).
unsafe fn borrow_queue<'a>(handle: jlong) -> &'a QueueHandle {
    crate::handle::borrow::<QueueHandle>(handle)
}

/// `String submitWorkflow(long, String name, int version, String stepsJson,
/// String[] payloadNames, byte[][] payloads, String queueDefault,
/// String paramsJson, String[] deferredNames, String parentRunId,
/// String parentNodeName)` — record a run and pre-enqueue a job per static step.
/// `parentRunId`/`parentNodeName` link a sub-workflow child to its parent node
/// (both null for a top-level run). Returns the run id.
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_submitWorkflow<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    name: JString<'local>,
    version: jint,
    steps_json: JString<'local>,
    payload_names: JObjectArray<'local>,
    payloads: JObjectArray<'local>,
    queue_default: JString<'local>,
    params_json: JString<'local>,
    deferred_names: JObjectArray<'local>,
    parent_run_id: JString<'local>,
    parent_node_name: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let name = read_string(env, &name)?;
        let steps_raw = read_string(env, &steps_json)?;
        let names = read_string_array(env, &payload_names)?;
        let payload_bytes = read_bytes_array(env, &payloads)?;
        let queue_default = read_optional_string(env, &queue_default)?;
        let params = read_optional_string(env, &params_json)?;
        let deferred = read_string_array(env, &deferred_names)?;
        let parent_run = read_optional_string(env, &parent_run_id)?;
        let parent_node = read_optional_string(env, &parent_node_name)?;
        let run_id = submit(
            queue,
            name,
            version,
            &steps_raw,
            names,
            payload_bytes,
            queue_default,
            params,
            deferred,
            parent_run,
            parent_node,
        )?;
        new_string(env, run_id)
    })
}

#[allow(clippy::too_many_arguments)]
fn submit(
    queue: &QueueHandle,
    name: String,
    version: i32,
    steps_json: &str,
    payload_names: Vec<String>,
    payloads: Vec<Vec<u8>>,
    queue_default: Option<String>,
    params_json: Option<String>,
    deferred_names: Vec<String>,
    parent_run_id: Option<String>,
    parent_node_name: Option<String>,
) -> Result<String, BindingError> {
    if payload_names.len() != payloads.len() {
        return Err(BindingError::new(
            "payloadNames and payloads must have equal length",
        ));
    }
    let wf = queue.workflow_store()?;
    let specs: Vec<StepSpec> = parse_json(steps_json, "workflow steps")?;

    // Add every node first, then the edges, so steps may be declared in any
    // order (a step's `after` need not have been declared before it).
    let mut dag: DAG<()> = DAG::new();
    for spec in &specs {
        dag.add_node(spec.name.clone(), ()).map_err(|e| {
            BindingError::new(format!("workflow add_node('{}') failed: {e}", spec.name))
        })?;
    }
    for spec in &specs {
        for pred in &spec.after {
            dag.add_edge(pred, &spec.name, None, None).map_err(|e| {
                BindingError::new(format!(
                    "workflow add_edge('{pred}' -> '{}') failed: {e}",
                    spec.name
                ))
            })?;
        }
    }
    let dag_bytes = dag
        .to_json(|_| None)
        .map_err(|e| BindingError::new(format!("workflow DAG serialize failed: {e}")))?
        .into_bytes();

    let step_meta: HashMap<String, StepMetadata> = specs
        .iter()
        .map(|s| {
            (
                s.name.clone(),
                StepMetadata {
                    task_name: s.task_name.clone(),
                    queue: s.queue.clone(),
                    max_retries: s.max_retries,
                    timeout_ms: s.timeout_ms,
                    priority: s.priority,
                    fan_out: s.fan_out.clone(),
                    fan_in: s.fan_in.clone(),
                    condition: s.condition.clone(),
                    gate: s.gate.clone(),
                    sub_workflow: s.sub_workflow.clone(),
                    compensate: s.compensate.clone(),
                    cache: s.cache.clone(),
                    ..Default::default()
                },
            )
        })
        .collect();

    let node_payloads: HashMap<String, Vec<u8>> = payload_names.into_iter().zip(payloads).collect();
    let deferred: HashSet<String> = deferred_names.into_iter().collect();
    let queue_default = queue_default.unwrap_or_else(|| DEFAULT_QUEUE.to_string());
    let ordered = topological_order(&dag_bytes)?;

    // Reuse a stored definition only when its DAG matches; otherwise the run
    // would execute one topology while `definition_id` points at another.
    let definition_id = match wf.get_workflow_definition(&name, Some(version))? {
        Some(existing) => {
            if existing.dag_data != dag_bytes {
                return Err(BindingError::new(format!(
                    "workflow '{name}' v{version} already exists with a different DAG; bump the version"
                )));
            }
            existing.id
        }
        None => {
            let def = WorkflowDefinition {
                id: Uuid::now_v7().to_string(),
                name: name.clone(),
                version,
                dag_data: dag_bytes.clone(),
                step_metadata: step_meta.clone(),
                created_at: now_millis(),
            };
            let id = def.id.clone();
            wf.create_workflow_definition(&def)?;
            id
        }
    };

    let run_id = Uuid::now_v7().to_string();
    let now = now_millis();
    wf.create_workflow_run(&WorkflowRun {
        id: run_id.clone(),
        definition_id,
        params: params_json,
        state: WorkflowState::Pending,
        started_at: Some(now),
        completed_at: None,
        error: None,
        parent_run_id,
        parent_node_name,
        created_at: now,
    })?;

    let mut job_ids: HashMap<String, String> = HashMap::new();
    for topo in &ordered {
        let meta = step_meta
            .get(&topo.name)
            .ok_or_else(|| BindingError::new(format!("step '{}' missing metadata", topo.name)))?;

        // Deferred node: tracked but not enqueued; the tracker creates its job
        // once predecessors settle. (Not produced by the Java builder yet.)
        if deferred.contains(&topo.name) {
            wf.create_workflow_node(&new_node(&run_id, &topo.name, None))?;
            continue;
        }

        let payload = node_payloads
            .get(&topo.name)
            .cloned()
            .ok_or_else(|| BindingError::new(format!("step '{}' missing payload", topo.name)))?;
        // Only static predecessors gate this job; a deferred one has no job to
        // wait on, so the tracker enqueues nodes downstream of it.
        let depends_on = topo
            .predecessors
            .iter()
            .filter(|p| !deferred.contains(*p))
            .map(|p| {
                job_ids.get(p).cloned().ok_or_else(|| {
                    BindingError::new(format!("predecessor '{p}' of '{}' has no job", topo.name))
                })
            })
            .collect::<Result<Vec<String>, BindingError>>()?;

        let job = queue.storage.enqueue(NewJob {
            queue: meta.queue.clone().unwrap_or_else(|| queue_default.clone()),
            task_name: meta.task_name.clone(),
            payload,
            priority: meta.priority.unwrap_or(0),
            scheduled_at: now,
            max_retries: meta.max_retries.unwrap_or(DEFAULT_MAX_RETRIES),
            timeout_ms: meta.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS),
            unique_key: None,
            metadata: Some(workflow_metadata_json(&run_id, &topo.name)),
            notes: None,
            depends_on,
            expires_at: None,
            result_ttl_ms: None,
            namespace: queue.namespace.clone(),
        })?;
        job_ids.insert(topo.name.clone(), job.id.clone());
        wf.create_workflow_node(&new_node(&run_id, &topo.name, Some(job.id)))?;
    }

    wf.update_workflow_run_state(&run_id, WorkflowState::Running, None)?;
    Ok(run_id)
}

/// `String markWorkflowNodeResult(long, String jobId, boolean succeeded,
/// String error, boolean skipCascade)` — record a node's terminal outcome.
/// Returns the run's final state when it reached terminal, else `null` (also for
/// non-workflow jobs).
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_markWorkflowNodeResult<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    job_id: JString<'local>,
    succeeded: jboolean,
    error: JString<'local>,
    skip_cascade: jboolean,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let job_id = read_string(env, &job_id)?;
        let error = read_optional_string(env, &error)?;
        match mark_node_result(
            queue,
            &job_id,
            succeeded != JNI_FALSE,
            error,
            skip_cascade != JNI_FALSE,
        )? {
            Some(state) => new_string(env, state),
            None => Ok(std::ptr::null_mut()),
        }
    })
}

fn mark_node_result(
    queue: &QueueHandle,
    job_id: &str,
    succeeded: bool,
    error: Option<String>,
    skip_cascade: bool,
) -> Result<Option<String>, BindingError> {
    let wf = queue.workflow_store()?;
    let job = match queue.storage.get_job(job_id, queue.namespace.as_deref())? {
        Some(j) => j,
        None => return Ok(None),
    };
    let (run_id, node_name) = match parse_workflow_metadata(job.metadata.as_deref()) {
        Some(pair) => pair,
        None => return Ok(None),
    };

    let now = now_millis();
    if succeeded {
        wf.set_workflow_node_completed(&run_id, &node_name, now, None)?;
    } else {
        let msg = error.clone().unwrap_or_else(|| "failed".to_string());
        wf.set_workflow_node_error(&run_id, &node_name, &msg)?;
        if !skip_cascade {
            let nodes = wf.get_workflow_nodes(&run_id)?;
            cascade_skip_pending(queue, &wf, &run_id, &nodes);
        }
    }

    // Tracker-managed run: it owns cascade + finalization. Not used by the
    // current Java tracker (static DAGs), kept for forward compatibility.
    if skip_cascade {
        return Ok(None);
    }

    let nodes = wf.get_workflow_nodes(&run_id)?;
    if !nodes.iter().all(|n| n.status.is_terminal()) {
        return Ok(None);
    }
    let any_failed = nodes.iter().any(|n| n.status == WorkflowNodeStatus::Failed);
    let final_state = if any_failed || !succeeded {
        WorkflowState::Failed
    } else {
        WorkflowState::Completed
    };
    wf.update_workflow_run_state(
        &run_id,
        final_state,
        if final_state == WorkflowState::Failed {
            error.as_deref()
        } else {
            None
        },
    )?;
    wf.set_workflow_run_completed(&run_id, now)?;
    Ok(Some(final_state.as_str().to_string()))
}

/// `String getWorkflowStatus(long, String runId)` — a JSON run + node snapshot,
/// or `null` if no such run exists.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_getWorkflowStatus<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    run_id: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let run_id = read_string(env, &run_id)?;
        let wf = queue.workflow_store()?;
        let run = match wf.get_workflow_run(&run_id)? {
            Some(r) => r,
            None => return Ok(std::ptr::null_mut()),
        };
        let nodes = wf.get_workflow_nodes(&run_id)?;
        let view = WorkflowStatusView {
            run_id: &run.id,
            state: run.state.as_str(),
            started_at: run.started_at,
            completed_at: run.completed_at,
            error: run.error.as_deref(),
            nodes: nodes.iter().map(NodeView::from).collect(),
        };
        new_string(env, to_json(&view)?)
    })
}

/// `void cancelWorkflowRun(long, String runId)` — skip every pending node
/// (cancelling its job), request cancel on running nodes, and mark the run
/// `cancelled`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_cancelWorkflowRun<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    run_id: JString<'local>,
) {
    guard(&mut env, (), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let run_id = read_string(env, &run_id)?;
        let wf = queue.workflow_store()?;
        for node in &wf.get_workflow_nodes(&run_id)? {
            match node.status {
                WorkflowNodeStatus::Pending | WorkflowNodeStatus::Ready => {
                    if let Some(job_id) = &node.job_id {
                        let _ = queue.storage.cancel_job(job_id, queue.namespace.as_deref());
                    }
                    wf.update_workflow_node_status(
                        &run_id,
                        &node.node_name,
                        WorkflowNodeStatus::Skipped,
                    )?;
                }
                WorkflowNodeStatus::Running => {
                    if let Some(job_id) = &node.job_id {
                        let _ = queue
                            .storage
                            .request_cancel(job_id, queue.namespace.as_deref());
                    }
                }
                _ => {}
            }
        }
        wf.update_workflow_run_state(&run_id, WorkflowState::Cancelled, None)?;
        wf.set_workflow_run_completed(&run_id, now_millis())?;
        Ok(())
    })
}

/// One node of a run's plan: its predecessors plus the step metadata the tracker
/// needs to enqueue deferred (fan-out / fan-in) work.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PlanNodeView<'a> {
    name: &'a str,
    predecessors: &'a [String],
    task_name: Option<&'a str>,
    queue: Option<&'a str>,
    max_retries: Option<i32>,
    timeout_ms: Option<i64>,
    priority: Option<i32>,
    fan_out: Option<&'a str>,
    fan_in: Option<&'a str>,
    condition: Option<&'a str>,
    gate: Option<&'a str>,
    sub_workflow: Option<&'a str>,
    compensate: Option<&'a str>,
    cache: Option<&'a str>,
}

/// The run + node a job belongs to.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeRefView<'a> {
    run_id: &'a str,
    node_name: &'a str,
}

/// Outcome of a settled fan-out parent.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FanOutCompletionView {
    succeeded: bool,
    child_job_ids: Vec<String>,
}

/// `String getWorkflowPlan(long, String runId)` — the run's nodes with their
/// predecessors and step metadata (JSON array), or `null` if the run is absent.
/// The tracker inverts predecessors into successors to route deferred work.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_getWorkflowPlan<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    run_id: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let run_id = read_string(env, &run_id)?;
        let wf = queue.workflow_store()?;
        let run = match wf.get_workflow_run(&run_id)? {
            Some(r) => r,
            None => return Ok(std::ptr::null_mut()),
        };
        let def = match wf.get_workflow_definition_by_id(&run.definition_id)? {
            Some(d) => d,
            None => return Ok(std::ptr::null_mut()),
        };
        let ordered = topological_order(&def.dag_data)?;
        let views: Vec<PlanNodeView> = ordered
            .iter()
            .map(|node| {
                let meta = def.step_metadata.get(&node.name);
                PlanNodeView {
                    name: &node.name,
                    predecessors: &node.predecessors,
                    task_name: meta.map(|m| m.task_name.as_str()),
                    queue: meta.and_then(|m| m.queue.as_deref()),
                    max_retries: meta.and_then(|m| m.max_retries),
                    timeout_ms: meta.and_then(|m| m.timeout_ms),
                    priority: meta.and_then(|m| m.priority),
                    fan_out: meta.and_then(|m| m.fan_out.as_deref()),
                    fan_in: meta.and_then(|m| m.fan_in.as_deref()),
                    condition: meta.and_then(|m| m.condition.as_deref()),
                    gate: meta.and_then(|m| m.gate.as_deref()),
                    sub_workflow: meta.and_then(|m| m.sub_workflow.as_deref()),
                    compensate: meta.and_then(|m| m.compensate.as_deref()),
                    cache: meta.and_then(|m| m.cache.as_deref()),
                }
            })
            .collect();
        new_string(env, to_json(&views)?)
    })
}

/// `String workflowNameForRun(long, String runId)` — the run's definition name,
/// or `null` if the run (or its definition) is absent. Lets the tracker resolve a
/// run back to a registered `Workflow` so it can supply a deferred node's payload.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_workflowNameForRun<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    run_id: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let run_id = read_string(env, &run_id)?;
        let wf = queue.workflow_store()?;
        let run = match wf.get_workflow_run(&run_id)? {
            Some(r) => r,
            None => return Ok(std::ptr::null_mut()),
        };
        match wf.get_workflow_definition_by_id(&run.definition_id)? {
            Some(def) => new_string(env, def.name),
            None => Ok(std::ptr::null_mut()),
        }
    })
}

/// `String workflowNodeForJob(long, String jobId)` — the run + node a job
/// belongs to (JSON), or `null` for a non-workflow job. Lets the tracker classify
/// any worker outcome without an in-memory job→run map.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_workflowNodeForJob<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    job_id: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let job_id = read_string(env, &job_id)?;
        let job = match queue.storage.get_job(&job_id, queue.namespace.as_deref())? {
            Some(j) => j,
            None => return Ok(std::ptr::null_mut()),
        };
        match parse_workflow_metadata(job.metadata.as_deref()) {
            Some((run_id, node_name)) => new_string(
                env,
                to_json(&NodeRefView {
                    run_id: &run_id,
                    node_name: &node_name,
                })?,
            ),
            None => Ok(std::ptr::null_mut()),
        }
    })
}

/// `String[] expandFanOut(long, String runId, String parentNode, String[]
/// childNames, byte[][] childPayloads, String taskName, String queue,
/// int maxRetries, long timeoutMs, int priority)` — expand a fan-out parent into
/// one child node + job per item. Returns the child job ids.
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_expandFanOut<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    run_id: JString<'local>,
    parent_node: JString<'local>,
    child_names: JObjectArray<'local>,
    child_payloads: JObjectArray<'local>,
    task_name: JString<'local>,
    queue: JString<'local>,
    max_retries: jint,
    timeout_ms: jlong,
    priority: jint,
) -> jobjectArray {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let q = unsafe { borrow_queue(handle) };
        let run_id = read_string(env, &run_id)?;
        let parent = read_string(env, &parent_node)?;
        let names = read_string_array(env, &child_names)?;
        let payloads = read_bytes_array(env, &child_payloads)?;
        let task = read_string(env, &task_name)?;
        let queue_name = read_string(env, &queue)?;
        let ids = expand_fan_out(
            q,
            &run_id,
            &parent,
            names,
            payloads,
            &task,
            &queue_name,
            max_retries,
            timeout_ms,
            priority,
        )?;
        new_string_array(env, &ids)
    })
}

/// Refuse a `run_id` this queue's namespace cannot see.
///
/// The fan-out and deferred paths enqueue the job *before* binding it to its
/// node. A scoped bind against a foreign run has no effect, which would leave
/// the job running untracked — so refuse before anything is enqueued.
fn require_visible_run(wf: &WorkflowStorageBackend, run_id: &str) -> Result<(), BindingError> {
    match wf.get_workflow_run(run_id)? {
        Some(_) => Ok(()),
        None => Err(BindingError::new(format!(
            "workflow run not found: {run_id}"
        ))),
    }
}

#[allow(clippy::too_many_arguments)]
fn expand_fan_out(
    queue: &QueueHandle,
    run_id: &str,
    parent: &str,
    child_names: Vec<String>,
    child_payloads: Vec<Vec<u8>>,
    task_name: &str,
    queue_name: &str,
    max_retries: i32,
    timeout_ms: i64,
    priority: i32,
) -> Result<Vec<String>, BindingError> {
    if child_names.len() != child_payloads.len() {
        return Err(BindingError::new(
            "childNames and childPayloads must have equal length",
        ));
    }
    if child_names.len() > MAX_FAN_OUT {
        return Err(BindingError::new(format!(
            "fan-out of {} children exceeds the limit of {MAX_FAN_OUT}",
            child_names.len()
        )));
    }
    let wf = queue.workflow_store()?;
    require_visible_run(&wf, run_id)?;
    let now = now_millis();
    let count = child_names.len() as i32;
    if count == 0 {
        wf.set_workflow_node_fan_out_count(run_id, parent, 0)?;
        wf.set_workflow_node_completed(run_id, parent, now, None)?;
        return Ok(Vec::new());
    }

    // Enqueue every child job first, then batch-insert their nodes in one
    // transaction so a crash partway never leaves half-tracked children.
    let mut child_job_ids = Vec::with_capacity(child_names.len());
    let mut nodes = Vec::with_capacity(child_names.len());
    for (child_name, payload) in child_names.iter().zip(child_payloads) {
        let job = queue.storage.enqueue(NewJob {
            queue: queue_name.to_string(),
            task_name: task_name.to_string(),
            payload,
            priority,
            scheduled_at: now,
            max_retries,
            timeout_ms,
            unique_key: None,
            metadata: Some(workflow_metadata_json(run_id, child_name)),
            notes: None,
            depends_on: vec![],
            expires_at: None,
            result_ttl_ms: None,
            namespace: queue.namespace.clone(),
        })?;
        child_job_ids.push(job.id.clone());
        nodes.push(new_node(run_id, child_name, Some(job.id)));
    }
    // Children are enqueued; if node creation fails, cancel them so they don't
    // run untracked outside the workflow.
    if let Err(err) = wf.create_workflow_nodes_batch(&nodes) {
        for id in &child_job_ids {
            let _ = queue.storage.cancel_job(id, queue.namespace.as_deref());
        }
        return Err(err.into());
    }
    wf.set_workflow_node_fan_out_count(run_id, parent, count)?;
    Ok(child_job_ids)
}

/// `String checkFanOutCompletion(long, String runId, String parentNode)` — when
/// every fan-out child is terminal, atomically finalize the parent (once) and
/// return `{succeeded, childJobIds}` (JSON); otherwise `null`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_checkFanOutCompletion<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    run_id: JString<'local>,
    parent_node: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let run_id = read_string(env, &run_id)?;
        let parent = read_string(env, &parent_node)?;
        let wf = queue.workflow_store()?;
        let prefix = format!("{parent}[");
        let mut children = wf.get_workflow_nodes_by_prefix(&run_id, &prefix)?;
        if children.is_empty() || !children.iter().all(|n| n.status.is_terminal()) {
            return Ok(std::ptr::null_mut());
        }
        // Order children by their item index (`parent[i]`) so the fan-in list
        // matches the producer's list order, not storage-return order.
        children.sort_by_key(|n| fan_out_child_index(&n.node_name));
        let any_failed = children
            .iter()
            .any(|n| n.status == WorkflowNodeStatus::Failed);
        let child_job_ids: Vec<String> = children.iter().filter_map(|n| n.job_id.clone()).collect();
        let transitioned = wf.finalize_fan_out_parent(
            &run_id,
            &parent,
            !any_failed,
            if any_failed {
                Some("fan-out child failed")
            } else {
                None
            },
            now_millis(),
        )?;
        if !transitioned {
            return Ok(std::ptr::null_mut());
        }
        new_string(
            env,
            to_json(&FanOutCompletionView {
                succeeded: !any_failed,
                child_job_ids,
            })?,
        )
    })
}

/// `String createDeferredJob(long, String runId, String nodeName, byte[] payload,
/// String taskName, String queue, int maxRetries, long timeoutMs, int priority)`
/// — enqueue a job for a deferred node (e.g. the fan-in collector) and bind it to
/// that node. Returns the new job id.
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_createDeferredJob<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    run_id: JString<'local>,
    node_name: JString<'local>,
    payload: JByteArray<'local>,
    task_name: JString<'local>,
    queue: JString<'local>,
    max_retries: jint,
    timeout_ms: jlong,
    priority: jint,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let q = unsafe { borrow_queue(handle) };
        let run_id = read_string(env, &run_id)?;
        let node_name = read_string(env, &node_name)?;
        let bytes = read_bytes(env, &payload)?;
        let task = read_string(env, &task_name)?;
        let queue_name = read_string(env, &queue)?;
        let wf = q.workflow_store()?;
        require_visible_run(&wf, &run_id)?;
        let job = q.storage.enqueue(NewJob {
            queue: queue_name,
            task_name: task,
            payload: bytes,
            priority,
            scheduled_at: now_millis(),
            max_retries,
            timeout_ms,
            unique_key: None,
            metadata: Some(workflow_metadata_json(&run_id, &node_name)),
            notes: None,
            depends_on: vec![],
            expires_at: None,
            result_ttl_ms: None,
            namespace: q.namespace.clone(),
        })?;
        // Cancel the job if binding fails, so it can't run untracked.
        if let Err(err) = wf.set_workflow_node_job(&run_id, &node_name, &job.id) {
            let _ = q.storage.cancel_job(&job.id, q.namespace.as_deref());
            return Err(err.into());
        }
        new_string(env, job.id)
    })
}

/// `void cascadeSkipPending(long, String runId)` — fail-fast: skip every
/// still-pending node of a run and cancel its job.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_cascadeSkipPending<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    run_id: JString<'local>,
) {
    guard(&mut env, (), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let run_id = read_string(env, &run_id)?;
        let wf = queue.workflow_store()?;
        let nodes = wf.get_workflow_nodes(&run_id)?;
        cascade_skip_pending(queue, &wf, &run_id, &nodes);
        Ok(())
    })
}

/// `String finalizeRunIfTerminal(long, String runId)` — if every node is
/// terminal, set the run `completed` (or `failed`) and return that state; else
/// `null`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_finalizeRunIfTerminal<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    run_id: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let run_id = read_string(env, &run_id)?;
        let wf = queue.workflow_store()?;
        let nodes = wf.get_workflow_nodes(&run_id)?;
        if nodes.is_empty() || !nodes.iter().all(|n| n.status.is_terminal()) {
            return Ok(std::ptr::null_mut());
        }
        let any_failed = nodes.iter().any(|n| n.status == WorkflowNodeStatus::Failed);
        let final_state = if any_failed {
            WorkflowState::Failed
        } else {
            WorkflowState::Completed
        };
        wf.update_workflow_run_state(
            &run_id,
            final_state,
            if any_failed {
                Some("a workflow node failed")
            } else {
                None
            },
        )?;
        wf.set_workflow_run_completed(&run_id, now_millis())?;
        new_string(env, final_state.as_str().to_string())
    })
}

/// `void setWorkflowNodeWaitingApproval(long, String runId, String nodeName)` —
/// park an approval-gate node until it is resolved.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_setWorkflowNodeWaitingApproval<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    run_id: JString<'local>,
    node_name: JString<'local>,
) {
    guard(&mut env, (), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let run_id = read_string(env, &run_id)?;
        let node_name = read_string(env, &node_name)?;
        queue.workflow_store()?.update_workflow_node_status(
            &run_id,
            &node_name,
            WorkflowNodeStatus::WaitingApproval,
        )?;
        Ok(())
    })
}

/// `void resolveWorkflowGate(long, String runId, String nodeName,
/// boolean approved, String error)` — settle a parked gate (or a sub-workflow
/// parent) as completed when approved, else failed with `error`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_resolveWorkflowGate<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    run_id: JString<'local>,
    node_name: JString<'local>,
    approved: jboolean,
    error: JString<'local>,
) {
    guard(&mut env, (), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let run_id = read_string(env, &run_id)?;
        let node_name = read_string(env, &node_name)?;
        let error = read_optional_string(env, &error)?;
        let wf = queue.workflow_store()?;
        if approved != JNI_FALSE {
            wf.set_workflow_node_completed(&run_id, &node_name, now_millis(), None)?;
        } else {
            let msg = error.unwrap_or_else(|| "gate rejected".to_string());
            wf.set_workflow_node_error(&run_id, &node_name, &msg)?;
        }
        Ok(())
    })
}

/// `void setWorkflowNodeRunning(long, String runId, String nodeName)` — promote a
/// gate / sub-workflow node to running without fan-out bookkeeping.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_setWorkflowNodeRunning<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    run_id: JString<'local>,
    node_name: JString<'local>,
) {
    guard(&mut env, (), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let run_id = read_string(env, &run_id)?;
        let node_name = read_string(env, &node_name)?;
        queue
            .workflow_store()?
            .set_workflow_node_running(&run_id, &node_name, now_millis())?;
        Ok(())
    })
}

/// `void failWorkflowNode(long, String runId, String nodeName, String error)` —
/// mark a node failed (e.g. a sub-workflow whose child could not be submitted).
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_failWorkflowNode<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    run_id: JString<'local>,
    node_name: JString<'local>,
    error: JString<'local>,
) {
    guard(&mut env, (), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let run_id = read_string(env, &run_id)?;
        let node_name = read_string(env, &node_name)?;
        let error = read_optional_string(env, &error)?.unwrap_or_else(|| "failed".to_string());
        queue
            .workflow_store()?
            .set_workflow_node_error(&run_id, &node_name, &error)?;
        Ok(())
    })
}

/// `void skipWorkflowNode(long, String runId, String nodeName)` — mark a node
/// skipped (its condition evaluated false) and cancel any bound job.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_skipWorkflowNode<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    run_id: JString<'local>,
    node_name: JString<'local>,
) {
    guard(&mut env, (), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let run_id = read_string(env, &run_id)?;
        let node_name = read_string(env, &node_name)?;
        let wf = queue.workflow_store()?;
        if let Some(node) = wf.get_workflow_node(&run_id, &node_name)? {
            if let Some(job_id) = &node.job_id {
                // Surface a storage failure rather than skipping while the bound job runs.
                queue
                    .storage
                    .cancel_job(job_id, queue.namespace.as_deref())?;
            }
        }
        wf.update_workflow_node_status(&run_id, &node_name, WorkflowNodeStatus::Skipped)?;
        Ok(())
    })
}

/// `void setWorkflowNodeCacheHit(long, String runId, String nodeName)` — mark a
/// node as a cache hit (terminal, treated as completed) without running it.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_setWorkflowNodeCacheHit<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    run_id: JString<'local>,
    node_name: JString<'local>,
) {
    guard(&mut env, (), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let run_id = read_string(env, &run_id)?;
        let node_name = read_string(env, &node_name)?;
        queue.workflow_store()?.update_workflow_node_status(
            &run_id,
            &node_name,
            WorkflowNodeStatus::CacheHit,
        )?;
        Ok(())
    })
}

/// `void setWorkflowRunCompensating(long, String runId)` — mark a run as
/// compensating (the saga is rolling back completed nodes).
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_setWorkflowRunCompensating<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    run_id: JString<'local>,
) {
    guard(&mut env, (), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let run_id = read_string(env, &run_id)?;
        queue.workflow_store()?.update_workflow_run_state(
            &run_id,
            WorkflowState::Compensating,
            None,
        )?;
        Ok(())
    })
}

/// `void setWorkflowRunCompensated(long, String runId, long completedAt)` — the
/// saga rolled everything back successfully.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_setWorkflowRunCompensated<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    run_id: JString<'local>,
    completed_at: jlong,
) {
    guard(&mut env, (), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let run_id = read_string(env, &run_id)?;
        let wf = queue.workflow_store()?;
        wf.update_workflow_run_state(&run_id, WorkflowState::Compensated, None)?;
        wf.set_workflow_run_completed(&run_id, completed_at)?;
        Ok(())
    })
}

/// `void setWorkflowRunCompensationFailed(long, String runId, long completedAt,
/// String error)` — a compensation job itself failed; rollback is incomplete.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_setWorkflowRunCompensationFailed<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    run_id: JString<'local>,
    completed_at: jlong,
    error: JString<'local>,
) {
    guard(&mut env, (), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let run_id = read_string(env, &run_id)?;
        let error = read_optional_string(env, &error)?;
        let wf = queue.workflow_store()?;
        wf.update_workflow_run_state(&run_id, WorkflowState::CompensationFailed, error.as_deref())?;
        wf.set_workflow_run_completed(&run_id, completed_at)?;
        Ok(())
    })
}

/// `void setWorkflowRunCompletedWithFailures(long, String runId, long completedAt)`
/// — the run finished with some failed nodes (on-failure=continue).
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_setWorkflowRunCompletedWithFailures<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    run_id: JString<'local>,
    completed_at: jlong,
) {
    guard(&mut env, (), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let run_id = read_string(env, &run_id)?;
        let wf = queue.workflow_store()?;
        wf.update_workflow_run_state(&run_id, WorkflowState::CompletedWithFailures, None)?;
        wf.set_workflow_run_completed(&run_id, completed_at)?;
        Ok(())
    })
}

/// `void setWorkflowNodeCompensationJob(long, String runId, String nodeName,
/// String compensationJobId, long startedAt)` — bind a node's rollback job.
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_setWorkflowNodeCompensationJob<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    run_id: JString<'local>,
    node_name: JString<'local>,
    compensation_job_id: JString<'local>,
    started_at: jlong,
) {
    guard(&mut env, (), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let run_id = read_string(env, &run_id)?;
        let node_name = read_string(env, &node_name)?;
        let comp_job_id = read_string(env, &compensation_job_id)?;
        queue.workflow_store()?.set_workflow_node_compensation_job(
            &run_id,
            &node_name,
            &comp_job_id,
            started_at,
        )?;
        Ok(())
    })
}

/// `void setWorkflowNodeCompensated(long, String runId, String nodeName,
/// long completedAt)` — a node's rollback succeeded.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_setWorkflowNodeCompensated<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    run_id: JString<'local>,
    node_name: JString<'local>,
    completed_at: jlong,
) {
    guard(&mut env, (), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let run_id = read_string(env, &run_id)?;
        let node_name = read_string(env, &node_name)?;
        queue
            .workflow_store()?
            .set_workflow_node_compensated(&run_id, &node_name, completed_at)?;
        Ok(())
    })
}

/// `void setWorkflowNodeCompensationFailed(long, String runId, String nodeName,
/// String error, long completedAt)` — a node's rollback failed.
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_setWorkflowNodeCompensationFailed<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    run_id: JString<'local>,
    node_name: JString<'local>,
    error: JString<'local>,
    completed_at: jlong,
) {
    guard(&mut env, (), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let run_id = read_string(env, &run_id)?;
        let node_name = read_string(env, &node_name)?;
        let error =
            read_optional_string(env, &error)?.unwrap_or_else(|| "compensation failed".to_string());
        queue
            .workflow_store()?
            .set_workflow_node_compensation_failed(&run_id, &node_name, &error, completed_at)?;
        Ok(())
    })
}

/// A fresh `Pending` node. `job_id` is `None` for deferred nodes.
fn new_node(run_id: &str, node_name: &str, job_id: Option<String>) -> WorkflowNode {
    WorkflowNode {
        id: Uuid::now_v7().to_string(),
        run_id: run_id.to_string(),
        node_name: node_name.to_string(),
        job_id,
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
    }
}

/// Job-metadata blob linking a job back to its workflow node.
fn workflow_metadata_json(run_id: &str, node_name: &str) -> String {
    serde_json::json!({
        "workflow_run_id": run_id,
        "workflow_node_name": node_name,
    })
    .to_string()
}

/// Item index `i` parsed from a fan-out child name `parent[i]`. Unparseable names
/// sort last so they never silently reorder valid children.
fn fan_out_child_index(name: &str) -> u64 {
    name.rsplit_once('[')
        .and_then(|(_, rest)| rest.strip_suffix(']'))
        .and_then(|idx| idx.parse().ok())
        .unwrap_or(u64::MAX)
}

/// Parse `{workflow_run_id, workflow_node_name}` from a job's metadata. Returns
/// `None` for compensation jobs so they never advance the forward run.
fn parse_workflow_metadata(metadata: Option<&str>) -> Option<(String, String)> {
    let parsed: serde_json::Value = serde_json::from_str(metadata?).ok()?;
    if parsed
        .get("compensation")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
    {
        return None;
    }
    let run_id = parsed.get("workflow_run_id")?.as_str()?.to_string();
    let node_name = parsed.get("workflow_node_name")?.as_str()?.to_string();
    Some((run_id, node_name))
}

/// Fail-fast: mark every still-pending node skipped and cancel its job.
/// Best-effort — per-node failures are logged, not propagated.
fn cascade_skip_pending(
    queue: &QueueHandle,
    wf: &WorkflowStorageBackend,
    run_id: &str,
    nodes: &[WorkflowNode],
) {
    for node in nodes {
        if !matches!(
            node.status,
            WorkflowNodeStatus::Pending | WorkflowNodeStatus::Ready
        ) {
            continue;
        }
        if let Some(job_id) = &node.job_id {
            if let Err(e) = queue.storage.cancel_job(job_id, queue.namespace.as_deref()) {
                log::warn!("[taskito-java] cancel_job({job_id}) during workflow cascade: {e}");
            }
        }
        if let Err(e) =
            wf.update_workflow_node_status(run_id, &node.node_name, WorkflowNodeStatus::Skipped)
        {
            log::warn!("[taskito-java] skip node '{}' failed: {e}", node.node_name);
        }
    }
}

/// `String listWorkflowRuns(long, String definitionNameOrNull, String stateOrNull,
/// long limit, long offset)` — run summaries as a JSON array (dashboard read).
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_listWorkflowRuns<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    definition_name: JString<'local>,
    state: JString<'local>,
    limit: jlong,
    offset: jlong,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let definition_name = read_optional_string(env, &definition_name)?;
        let state_raw = read_optional_string(env, &state)?;
        let state = match state_raw.as_deref() {
            Some(s) => Some(
                WorkflowState::from_str_val(s)
                    .ok_or_else(|| BindingError::new(format!("unknown workflow state '{s}'")))?,
            ),
            None => None,
        };
        let wf = queue.workflow_store()?;
        let runs = wf.list_workflow_runs(
            definition_name.as_deref(),
            state,
            limit.max(0),
            offset.max(0),
        )?;
        let views: Vec<WorkflowRunView> = runs.iter().map(WorkflowRunView::from).collect();
        new_string(env, to_json(&views)?)
    })
}

/// `String getWorkflowRun(long, String runId)` — a single run summary, or
/// `null` if the run no longer exists.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_getWorkflowRun<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    run_id: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let run_id = read_string(env, &run_id)?;
        let wf = queue.workflow_store()?;
        match wf.get_workflow_run(&run_id)? {
            Some(run) => new_string(env, to_json(&WorkflowRunView::from(&run))?),
            None => Ok(std::ptr::null_mut()),
        }
    })
}

/// `String getWorkflowChildren(long, String runId)` — sub-workflow runs spawned
/// by a run, as a JSON array of run summaries.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_getWorkflowChildren<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    run_id: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let run_id = read_string(env, &run_id)?;
        let wf = queue.workflow_store()?;
        let runs = wf.get_child_workflow_runs(&run_id)?;
        let views: Vec<WorkflowRunView> = runs.iter().map(WorkflowRunView::from).collect();
        new_string(env, to_json(&views)?)
    })
}

/// `String getWorkflowDag(long, String runId)` — the serialized DAG JSON backing
/// a run, or `null` if the run or its definition no longer exists.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeWorkflows_getWorkflowDag<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    run_id: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let run_id = read_string(env, &run_id)?;
        let wf = queue.workflow_store()?;
        let Some(run) = wf.get_workflow_run(&run_id)? else {
            return Ok(std::ptr::null_mut());
        };
        let Some(def) = wf.get_workflow_definition_by_id(&run.definition_id)? else {
            return Ok(std::ptr::null_mut());
        };
        let dag = String::from_utf8(def.dag_data)
            .map_err(|e| BindingError::new(format!("invalid DAG utf8: {e}")))?;
        new_string(env, dag)
    })
}
