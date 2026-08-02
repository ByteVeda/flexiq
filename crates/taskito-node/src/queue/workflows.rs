//! Workflow submission, advancement, and queries.
//!
//! Static DAG steps are pre-enqueued with a `depends_on` chain so the core
//! scheduler runs them in topological order. Deferred steps — fan-out / fan-in,
//! conditioned steps, gates, and sub-workflows — are submitted without a job;
//! the worker-side tracker (see `sdks/node/src/workflows/tracker.ts`) expands,
//! resolves, and enqueues them at runtime via the primitives below. On failure
//! the tracker can drive saga compensation, rolling nodes back in reverse order.

use std::collections::{HashMap, HashSet};

use napi::bindgen_prelude::{Buffer, Error, Result};
use napi_derive::napi;
use taskito_core::job::{now_millis, NewJob};
use taskito_core::{Storage, StorageBackend};
use taskito_workflows::{
    topological_order, StepMetadata, WorkflowDefinition, WorkflowNode, WorkflowNodeStatus,
    WorkflowRun, WorkflowSqliteStorage, WorkflowState, WorkflowStorage, WorkflowStorageBackend,
};

use super::JsQueue;
use crate::convert::{
    node_to_js, run_to_js, JsFanOutCompletion, JsWorkflowAdvance, JsWorkflowNode,
    JsWorkflowNodeRef, JsWorkflowRun, JsWorkflowRunPlan,
};
use crate::error::{invalid_arg, to_napi_err};

/// Largest number of children a single fan-out may expand into — guards against
/// a producer returning an enormous list and flooding storage in one batch.
const MAX_FAN_OUT: usize = 10_000;

const DEFAULT_QUEUE: &str = "default";
const DEFAULT_MAX_RETRIES: i32 = 3;
const DEFAULT_TIMEOUT_MS: i64 = 300_000;
const DEFAULT_LIST_LIMIT: i64 = 50;

#[napi]
impl JsQueue {
    /// Submit a workflow run from a serialized DAG plus per-step metadata and
    /// payloads. Pre-enqueues a job per step with a `depends_on` chain and
    /// records a `WorkflowRun` + a `WorkflowNode` per step. Returns the run id.
    ///
    /// `deferred_node_names` lists steps the tracker enqueues on demand at
    /// runtime (fan-out parents, fan-in collectors, and anything downstream of
    /// them). A deferred node gets a `Pending` node row but **no job**, and is
    /// excluded from its successors' `depends_on` so the static scheduler never
    /// blocks on a job that will not exist until expansion.
    #[napi]
    #[allow(clippy::too_many_arguments)]
    pub fn submit_workflow(
        &self,
        name: String,
        version: i32,
        dag_bytes: Buffer,
        step_metadata_json: String,
        node_payloads: HashMap<String, Buffer>,
        queue_default: Option<String>,
        params_json: Option<String>,
        deferred_node_names: Option<Vec<String>>,
        parent_run_id: Option<String>,
        parent_node_name: Option<String>,
    ) -> Result<String> {
        let wf = self.workflow_store()?;
        let dag = dag_bytes.to_vec();
        let step_meta: HashMap<String, StepMetadata> =
            serde_json::from_str(&step_metadata_json).map_err(|e| reason(e.to_string()))?;
        let ordered = topological_order(&dag).map_err(to_napi_err)?;
        let queue_default = queue_default.unwrap_or_else(|| DEFAULT_QUEUE.to_string());
        let deferred: HashSet<String> = deferred_node_names
            .unwrap_or_default()
            .into_iter()
            .collect();

        let definition_id = match wf
            .get_workflow_definition(&name, Some(version))
            .map_err(to_napi_err)?
        {
            // Reuse the stored id only when the DAG matches; otherwise the run
            // would execute one topology while `definition_id` points at another,
            // breaking run-plan reconstruction. Force a version bump instead.
            Some(existing) => {
                if existing.dag_data != dag {
                    return Err(invalid_arg(format!(
                        "workflow '{name}' v{version} already exists with a different DAG; bump the version"
                    )));
                }
                existing.id
            }
            None => {
                let def = WorkflowDefinition {
                    id: uuid::Uuid::now_v7().to_string(),
                    name: name.clone(),
                    version,
                    dag_data: dag.clone(),
                    step_metadata: step_meta.clone(),
                    created_at: now_millis(),
                };
                let id = def.id.clone();
                wf.create_workflow_definition(&def).map_err(to_napi_err)?;
                id
            }
        };

        let run_id = uuid::Uuid::now_v7().to_string();
        let now = now_millis();
        let run = WorkflowRun {
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
        };
        wf.create_workflow_run(&run).map_err(to_napi_err)?;

        let mut job_ids: HashMap<String, String> = HashMap::new();
        for topo in &ordered {
            let meta = step_meta.get(&topo.name).ok_or_else(|| {
                reason(format!("step '{}' missing from step_metadata", topo.name))
            })?;

            // Deferred node: tracked but not enqueued. The tracker creates its
            // job (or expands it) once its predecessors settle.
            if deferred.contains(&topo.name) {
                wf.create_workflow_node(&new_workflow_node(&run_id, &topo.name, None))
                    .map_err(to_napi_err)?;
                continue;
            }

            let payload = node_payloads
                .get(&topo.name)
                .map(|b| b.to_vec())
                .ok_or_else(|| {
                    reason(format!("step '{}' missing from node_payloads", topo.name))
                })?;
            // Only static predecessors gate this job; deferred ones have no job
            // to wait on, so the tracker is responsible for enqueuing nodes that
            // sit downstream of them.
            let depends_on = topo
                .predecessors
                .iter()
                .filter(|p| !deferred.contains(*p))
                .map(|p| {
                    job_ids.get(p).cloned().ok_or_else(|| {
                        reason(format!(
                            "predecessor '{}' of step '{}' has no job id",
                            p, topo.name
                        ))
                    })
                })
                .collect::<Result<Vec<String>>>()?;

            let new_job = NewJob {
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
                namespace: self.namespace.clone(),
            };
            let job = self.storage.enqueue(new_job).map_err(to_napi_err)?;
            job_ids.insert(topo.name.clone(), job.id.clone());

            wf.create_workflow_node(&new_workflow_node(&run_id, &topo.name, Some(job.id)))
                .map_err(to_napi_err)?;
        }

        wf.update_workflow_run_state(&run_id, WorkflowState::Running, None)
            .map_err(to_napi_err)?;
        Ok(run_id)
    }

    /// Record the terminal outcome of a workflow node's job. A no-op (returns
    /// `null`) for non-workflow jobs. On failure, cascades a fail-fast skip to
    /// the run's remaining pending nodes. When the run reaches a terminal state,
    /// the returned advance carries `finalState`.
    ///
    /// `skip_cascade` is set by the tracker for runs it manages (those with
    /// deferred/fan-out nodes): the node's status is recorded but cascade and
    /// run finalization are left to the tracker, which drives them after
    /// expanding/aggregating. For plain DAGs it is false and behaviour is
    /// unchanged.
    #[napi]
    pub fn mark_workflow_node_result(
        &self,
        job_id: String,
        succeeded: bool,
        error: Option<String>,
        skip_cascade: Option<bool>,
    ) -> Result<Option<JsWorkflowAdvance>> {
        let skip_cascade = skip_cascade.unwrap_or(false);
        let wf = self.workflow_store()?;
        let job = match self
            .storage
            .get_job(&job_id, self.namespace.as_deref())
            .map_err(to_napi_err)?
        {
            Some(j) => j,
            None => return Ok(None),
        };
        let (run_id, node_name) = match parse_workflow_metadata(job.metadata.as_deref()) {
            Some(pair) => pair,
            None => return Ok(None),
        };

        let now = now_millis();
        if succeeded {
            wf.set_workflow_node_completed(&run_id, &node_name, now, None)
                .map_err(to_napi_err)?;
        } else {
            let msg = error.clone().unwrap_or_else(|| "failed".to_string());
            wf.set_workflow_node_error(&run_id, &node_name, &msg)
                .map_err(to_napi_err)?;
            if !skip_cascade {
                let nodes = wf.get_workflow_nodes(&run_id).map_err(to_napi_err)?;
                cascade_skip_pending(
                    &self.storage,
                    &wf,
                    &run_id,
                    &nodes,
                    self.namespace.as_deref(),
                );
            }
        }

        // Tracker-managed run: it owns cascade + finalization (it may still need
        // to expand a fan-out or enqueue a deferred successor first).
        if skip_cascade {
            return Ok(Some(JsWorkflowAdvance {
                run_id,
                node_name,
                final_state: None,
            }));
        }

        let nodes = wf.get_workflow_nodes(&run_id).map_err(to_napi_err)?;
        if !nodes.iter().all(|n| n.status.is_terminal()) {
            return Ok(Some(JsWorkflowAdvance {
                run_id,
                node_name,
                final_state: None,
            }));
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
        )
        .map_err(to_napi_err)?;
        wf.set_workflow_run_completed(&run_id, now)
            .map_err(to_napi_err)?;

        Ok(Some(JsWorkflowAdvance {
            run_id,
            node_name,
            final_state: Some(final_state.as_str().to_string()),
        }))
    }

    /// Fetch a workflow run by id, or `null` if no such run exists.
    #[napi]
    pub fn get_workflow_run(&self, run_id: String) -> Result<Option<JsWorkflowRun>> {
        let wf = self.workflow_store()?;
        Ok(wf
            .get_workflow_run(&run_id)
            .map_err(to_napi_err)?
            .map(run_to_js))
    }

    /// List the nodes (steps) of a workflow run.
    #[napi]
    pub fn get_workflow_nodes(&self, run_id: String) -> Result<Vec<JsWorkflowNode>> {
        let wf = self.workflow_store()?;
        Ok(wf
            .get_workflow_nodes(&run_id)
            .map_err(to_napi_err)?
            .into_iter()
            .map(node_to_js)
            .collect())
    }

    /// List workflow runs, optionally filtered by definition name and/or state.
    #[napi]
    pub fn list_workflow_runs(
        &self,
        definition_name: Option<String>,
        state: Option<String>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<JsWorkflowRun>> {
        let wf = self.workflow_store()?;
        let state = match state {
            Some(s) => Some(
                WorkflowState::from_str_val(&s)
                    .ok_or_else(|| reason(format!("unknown workflow state '{s}'")))?,
            ),
            None => None,
        };
        let runs = wf
            .list_workflow_runs(
                definition_name.as_deref(),
                state,
                limit.unwrap_or(DEFAULT_LIST_LIMIT),
                offset.unwrap_or(0),
            )
            .map_err(to_napi_err)?;
        Ok(runs.into_iter().map(run_to_js).collect())
    }

    /// The serialized DAG (a `SerializableGraph` JSON string) backing a run, or
    /// `null` if the run or its definition no longer exists.
    #[napi]
    pub fn get_workflow_dag(&self, run_id: String) -> Result<Option<String>> {
        let wf = self.workflow_store()?;
        let Some(run) = wf.get_workflow_run(&run_id).map_err(to_napi_err)? else {
            return Ok(None);
        };
        let Some(def) = wf
            .get_workflow_definition_by_id(&run.definition_id)
            .map_err(to_napi_err)?
        else {
            return Ok(None);
        };
        String::from_utf8(def.dag_data)
            .map(Some)
            .map_err(|e| reason(e.to_string()))
    }

    /// Child (sub-workflow) runs spawned by a run. Always empty for runs
    /// submitted by the Node SDK, which does not yet create sub-workflows.
    #[napi]
    pub fn get_workflow_children(&self, run_id: String) -> Result<Vec<JsWorkflowRun>> {
        let wf = self.workflow_store()?;
        Ok(wf
            .get_child_workflow_runs(&run_id)
            .map_err(to_napi_err)?
            .into_iter()
            .map(run_to_js)
            .collect())
    }

    /// The run + node a job belongs to, or `null` for a non-workflow job. Lets
    /// the tracker classify any worker outcome without an in-memory job→run map.
    #[napi]
    pub fn workflow_node_for_job(&self, job_id: String) -> Result<Option<JsWorkflowNodeRef>> {
        let Some(job) = self
            .storage
            .get_job(&job_id, self.namespace.as_deref())
            .map_err(to_napi_err)?
        else {
            return Ok(None);
        };
        Ok(parse_workflow_metadata(job.metadata.as_deref())
            .map(|(run_id, node_name)| JsWorkflowNodeRef { run_id, node_name }))
    }

    /// The DAG + step metadata backing a run, so the tracker can reconstruct
    /// the workflow structure from storage. `null` if the run or its definition
    /// no longer exists.
    #[napi]
    pub fn get_workflow_run_plan(&self, run_id: String) -> Result<Option<JsWorkflowRunPlan>> {
        let wf = self.workflow_store()?;
        let Some(run) = wf.get_workflow_run(&run_id).map_err(to_napi_err)? else {
            return Ok(None);
        };
        let Some(def) = wf
            .get_workflow_definition_by_id(&run.definition_id)
            .map_err(to_napi_err)?
        else {
            return Ok(None);
        };
        let dag = String::from_utf8(def.dag_data).map_err(|e| reason(e.to_string()))?;
        let step_metadata =
            serde_json::to_string(&def.step_metadata).map_err(|e| reason(e.to_string()))?;
        Ok(Some(JsWorkflowRunPlan { dag, step_metadata }))
    }

    /// Expand a fan-out parent into one child node + job per item. Sets the
    /// parent's `fan_out_count` and transitions it to `Running`; an empty item
    /// list completes the parent immediately. Returns the child job ids.
    #[napi]
    #[allow(clippy::too_many_arguments)]
    pub fn expand_fan_out(
        &self,
        run_id: String,
        parent_node_name: String,
        child_names: Vec<String>,
        child_payloads: Vec<Buffer>,
        task_name: String,
        queue: String,
        max_retries: i32,
        timeout_ms: i64,
        priority: i32,
    ) -> Result<Vec<String>> {
        if child_names.len() != child_payloads.len() {
            return Err(reason(
                "child_names and child_payloads must have equal length",
            ));
        }
        if child_names.len() > MAX_FAN_OUT {
            return Err(reason(format!(
                "fan-out of {} children exceeds the limit of {MAX_FAN_OUT}",
                child_names.len()
            )));
        }
        let wf = self.workflow_store()?;
        require_visible_run(&wf, &run_id)?;
        let now = now_millis();
        let count = child_names.len() as i32;

        if count == 0 {
            wf.set_workflow_node_fan_out_count(&run_id, &parent_node_name, 0)
                .map_err(to_napi_err)?;
            wf.set_workflow_node_completed(&run_id, &parent_node_name, now, None)
                .map_err(to_napi_err)?;
            return Ok(Vec::new());
        }

        // Enqueue every child job first, then batch-insert their nodes in one
        // transaction so a crash partway through never leaves half-tracked
        // children.
        let mut child_job_ids = Vec::with_capacity(child_names.len());
        let mut nodes = Vec::with_capacity(child_names.len());
        for (child_name, payload) in child_names.iter().zip(child_payloads) {
            let new_job = NewJob {
                queue: queue.clone(),
                task_name: task_name.clone(),
                payload: payload.to_vec(),
                priority,
                scheduled_at: now,
                max_retries,
                timeout_ms,
                unique_key: None,
                metadata: Some(workflow_metadata_json(&run_id, child_name)),
                notes: None,
                depends_on: vec![],
                expires_at: None,
                result_ttl_ms: None,
                namespace: self.namespace.clone(),
            };
            let job = self.storage.enqueue(new_job).map_err(to_napi_err)?;
            child_job_ids.push(job.id.clone());
            nodes.push(new_workflow_node(&run_id, child_name, Some(job.id)));
        }

        // Child jobs are already enqueued; if node creation fails, cancel them
        // so they don't run untracked (orphaned) outside the workflow.
        if let Err(err) = wf.create_workflow_nodes_batch(&nodes).map_err(to_napi_err) {
            for id in &child_job_ids {
                let _ = self.storage.cancel_job(id, self.namespace.as_deref());
            }
            return Err(err);
        }
        wf.set_workflow_node_fan_out_count(&run_id, &parent_node_name, count)
            .map_err(to_napi_err)?;
        Ok(child_job_ids)
    }

    /// Enqueue a job for a deferred node and bind it to that node. Used for the
    /// fan-in collector and for static nodes downstream of a deferred one whose
    /// predecessors are now all terminal. Returns the new job id.
    #[napi]
    #[allow(clippy::too_many_arguments)]
    pub fn create_deferred_job(
        &self,
        run_id: String,
        node_name: String,
        payload: Buffer,
        task_name: String,
        queue: String,
        max_retries: i32,
        timeout_ms: i64,
        priority: i32,
    ) -> Result<String> {
        let wf = self.workflow_store()?;
        require_visible_run(&wf, &run_id)?;
        let new_job = NewJob {
            queue,
            task_name,
            payload: payload.to_vec(),
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
            namespace: self.namespace.clone(),
        };
        let job = self.storage.enqueue(new_job).map_err(to_napi_err)?;
        // Cancel the job if we can't bind it to its node, so it doesn't run
        // unbound (the tracker would never see its outcome).
        if let Err(err) = wf
            .set_workflow_node_job(&run_id, &node_name, &job.id)
            .map_err(to_napi_err)
        {
            let _ = self.storage.cancel_job(&job.id, self.namespace.as_deref());
            return Err(err);
        }
        Ok(job.id)
    }

    /// Check whether all fan-out children of a parent are terminal. When they
    /// are, atomically finalizes the parent exactly once (compare-and-swap) and
    /// returns the outcome; otherwise — or if another caller already finalized —
    /// returns `null`.
    #[napi]
    pub fn check_fan_out_completion(
        &self,
        run_id: String,
        parent_node_name: String,
    ) -> Result<Option<JsFanOutCompletion>> {
        let wf = self.workflow_store()?;
        let prefix = format!("{parent_node_name}[");
        let children = wf
            .get_workflow_nodes_by_prefix(&run_id, &prefix)
            .map_err(to_napi_err)?;

        if children.is_empty() || !children.iter().all(|n| n.status.is_terminal()) {
            return Ok(None);
        }

        let any_failed = children
            .iter()
            .any(|n| n.status == WorkflowNodeStatus::Failed);
        let child_job_ids: Vec<String> = children.iter().filter_map(|n| n.job_id.clone()).collect();

        let transitioned = wf
            .finalize_fan_out_parent(
                &run_id,
                &parent_node_name,
                !any_failed,
                if any_failed {
                    Some("fan-out child failed")
                } else {
                    None
                },
                now_millis(),
            )
            .map_err(to_napi_err)?;
        if !transitioned {
            return Ok(None);
        }
        Ok(Some(JsFanOutCompletion {
            succeeded: !any_failed,
            child_job_ids,
        }))
    }

    /// Skip a single node whose condition evaluated false: cancel any backing
    /// job (best-effort) and mark it `Skipped`. The tracker propagates the skip
    /// to the node's successors so a not-taken branch settles cleanly.
    #[napi]
    pub fn skip_workflow_node(&self, run_id: String, node_name: String) -> Result<()> {
        let wf = self.workflow_store()?;
        if let Some(node) = wf
            .get_workflow_node(&run_id, &node_name)
            .map_err(to_napi_err)?
        {
            if let Some(job_id) = &node.job_id {
                if let Err(e) = self.storage.cancel_job(job_id, self.namespace.as_deref()) {
                    log::warn!("[taskito-node] cancel_job({job_id}) during skip: {e}");
                }
            }
        }
        wf.update_workflow_node_status(&run_id, &node_name, WorkflowNodeStatus::Skipped)
            .map_err(to_napi_err)?;
        Ok(())
    }

    /// Park a gate node at `WaitingApproval`. The run pauses here until
    /// `resolve_workflow_gate` approves or rejects it (or a tracker timeout does).
    #[napi]
    pub fn set_workflow_node_waiting_approval(
        &self,
        run_id: String,
        node_name: String,
    ) -> Result<()> {
        let wf = self.workflow_store()?;
        wf.update_workflow_node_status(&run_id, &node_name, WorkflowNodeStatus::WaitingApproval)
            .map_err(to_napi_err)?;
        Ok(())
    }

    /// Resolve a waiting gate: approve → mark the node `Completed`; reject →
    /// mark it `Failed` with `error`. The tracker then advances or skips the
    /// gate's successors. Also used to resolve a parent node when its
    /// sub-workflow child finalizes.
    #[napi]
    pub fn resolve_workflow_gate(
        &self,
        run_id: String,
        node_name: String,
        approved: bool,
        error: Option<String>,
    ) -> Result<()> {
        let wf = self.workflow_store()?;
        if approved {
            wf.set_workflow_node_completed(&run_id, &node_name, now_millis(), None)
                .map_err(to_napi_err)?;
        } else {
            let msg = error.unwrap_or_else(|| "rejected".to_string());
            wf.set_workflow_node_error(&run_id, &node_name, &msg)
                .map_err(to_napi_err)?;
        }
        Ok(())
    }

    /// Promote a node to `Running` — used when its sub-workflow child has been
    /// submitted, so the parent node reflects in-flight work until the child
    /// finalizes and resolves it.
    #[napi]
    pub fn set_workflow_node_running(&self, run_id: String, node_name: String) -> Result<()> {
        let wf = self.workflow_store()?;
        wf.set_workflow_node_running(&run_id, &node_name, now_millis())
            .map_err(to_napi_err)?;
        Ok(())
    }

    /// Set a run's state (and optional error + completion timestamp). Used by the
    /// saga tracker to transition through `compensating` → `compensated` /
    /// `compensation_failed`.
    #[napi]
    pub fn set_workflow_run_state(
        &self,
        run_id: String,
        state: String,
        error: Option<String>,
        completed_at: Option<i64>,
    ) -> Result<()> {
        let wf = self.workflow_store()?;
        let state = WorkflowState::from_str_val(&state)
            .ok_or_else(|| reason(format!("unknown workflow state '{state}'")))?;
        wf.update_workflow_run_state(&run_id, state, error.as_deref())
            .map_err(to_napi_err)?;
        if let Some(ts) = completed_at {
            wf.set_workflow_run_completed(&run_id, ts)
                .map_err(to_napi_err)?;
        }
        Ok(())
    }

    /// Mark a node's compensation succeeded (status `Compensated`).
    #[napi]
    pub fn set_workflow_node_compensated(
        &self,
        run_id: String,
        node_name: String,
        completed_at: i64,
    ) -> Result<()> {
        let wf = self.workflow_store()?;
        wf.set_workflow_node_compensated(&run_id, &node_name, completed_at)
            .map_err(to_napi_err)?;
        Ok(())
    }

    /// Mark a node's compensation failed (status `CompensationFailed`).
    #[napi]
    pub fn set_workflow_node_compensation_failed(
        &self,
        run_id: String,
        node_name: String,
        error: String,
        completed_at: i64,
    ) -> Result<()> {
        let wf = self.workflow_store()?;
        wf.set_workflow_node_compensation_failed(&run_id, &node_name, &error, completed_at)
            .map_err(to_napi_err)?;
        Ok(())
    }

    /// Enqueue a node's rollback (compensation) job and bind it to the node
    /// (status `Compensating`). The job carries a compensation marker so its
    /// outcome routes to saga handling, and a dedup `unique_key` so concurrent
    /// workers never double-compensate the same node. Returns the job id.
    #[napi]
    #[allow(clippy::too_many_arguments)]
    pub fn enqueue_compensation(
        &self,
        run_id: String,
        node_name: String,
        payload: Buffer,
        task_name: String,
        queue: String,
        max_retries: i32,
        timeout_ms: i64,
        priority: i32,
    ) -> Result<String> {
        let wf = self.workflow_store()?;
        require_visible_run(&wf, &run_id)?;
        let now = now_millis();
        let new_job = NewJob {
            queue,
            task_name,
            payload: payload.to_vec(),
            priority,
            scheduled_at: now,
            max_retries,
            timeout_ms,
            unique_key: Some(format!("compensation:{run_id}:{node_name}")),
            metadata: Some(compensation_metadata_json(&run_id, &node_name)),
            notes: None,
            depends_on: vec![],
            expires_at: None,
            result_ttl_ms: None,
            namespace: self.namespace.clone(),
        };
        let job = self.storage.enqueue(new_job).map_err(to_napi_err)?;
        // Cancel the job if binding fails, so a compensation job can't run
        // untracked by the saga state machine (mirrors create_deferred_job).
        if let Err(err) = wf
            .set_workflow_node_compensation_job(&run_id, &node_name, &job.id, now)
            .map_err(to_napi_err)
        {
            let _ = self.storage.cancel_job(&job.id, self.namespace.as_deref());
            return Err(err);
        }
        Ok(job.id)
    }

    /// The run + node a compensation job belongs to, or `null` if `job_id` is not
    /// a compensation job. The tracker checks this before normal node handling so
    /// a rollback outcome advances the saga rather than the forward run.
    #[napi]
    pub fn compensation_node_for_job(&self, job_id: String) -> Result<Option<JsWorkflowNodeRef>> {
        let Some(job) = self
            .storage
            .get_job(&job_id, self.namespace.as_deref())
            .map_err(to_napi_err)?
        else {
            return Ok(None);
        };
        Ok(parse_compensation_metadata(job.metadata.as_deref())
            .map(|(run_id, node_name)| JsWorkflowNodeRef { run_id, node_name }))
    }

    /// Fail-fast cascade: skip every still-pending node of a run and cancel its
    /// job. Called by the tracker when a managed run's node fails.
    #[napi]
    pub fn cascade_skip_pending(&self, run_id: String) -> Result<()> {
        let wf = self.workflow_store()?;
        let nodes = wf.get_workflow_nodes(&run_id).map_err(to_napi_err)?;
        cascade_skip_pending(
            &self.storage,
            &wf,
            &run_id,
            &nodes,
            self.namespace.as_deref(),
        );
        Ok(())
    }

    /// Finalize a run if every node has reached a terminal state: set it
    /// `Completed`, or `Failed` if any node failed. Returns the final state, or
    /// `null` if the run still has work in flight.
    #[napi]
    pub fn finalize_run_if_terminal(&self, run_id: String) -> Result<Option<String>> {
        let wf = self.workflow_store()?;
        let nodes = wf.get_workflow_nodes(&run_id).map_err(to_napi_err)?;
        if nodes.is_empty() || !nodes.iter().all(|n| n.status.is_terminal()) {
            return Ok(None);
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
        )
        .map_err(to_napi_err)?;
        wf.set_workflow_run_completed(&run_id, now_millis())
            .map_err(to_napi_err)?;
        Ok(Some(final_state.as_str().to_string()))
    }

    /// Lazily build the workflow storage from this queue's backend (runs the
    /// workflow migrations on first use). Not exported to JS.
    fn workflow_store(&self) -> Result<WorkflowStorageBackend> {
        if let Some(wf) = self.workflow_storage.get() {
            return Ok(wf.clone());
        }
        let wf = build_workflow_storage(&self.storage, self.namespace.clone())?;
        // If another thread raced us, either handle wraps the same pool.
        let _ = self.workflow_storage.set(wf.clone());
        Ok(wf)
    }
}

/// Refuse a `run_id` this queue's namespace cannot see.
///
/// The fan-out, deferred and compensation paths enqueue the job *before*
/// binding it to its node. A scoped bind against a foreign run has no effect,
/// which would leave the job running untracked — so refuse before anything is
/// enqueued rather than after.
fn require_visible_run(wf: &WorkflowStorageBackend, run_id: &str) -> Result<()> {
    match wf.get_workflow_run(run_id).map_err(to_napi_err)? {
        Some(_) => Ok(()),
        None => Err(reason(format!("workflow run not found: {run_id}"))),
    }
}

/// Construct the workflow storage matching the queue's core backend.
fn build_workflow_storage(
    storage: &StorageBackend,
    namespace: Option<String>,
) -> Result<WorkflowStorageBackend> {
    let wf = match storage {
        StorageBackend::Sqlite(s) => WorkflowSqliteStorage::new(s.clone(), namespace)
            .map(WorkflowStorageBackend::Sqlite)
            .map_err(to_napi_err)?,
        #[cfg(feature = "postgres")]
        StorageBackend::Postgres(s) => {
            taskito_workflows::WorkflowPostgresStorage::new(s.clone(), namespace)
                .map(WorkflowStorageBackend::Postgres)
                .map_err(to_napi_err)?
        }
        #[cfg(feature = "redis")]
        StorageBackend::Redis(s) => {
            taskito_workflows::WorkflowRedisStorage::new(s.clone(), namespace)
                .map(WorkflowStorageBackend::Redis)
                .map_err(to_napi_err)?
        }
    };
    Ok(wf)
}

/// Build a fresh `Pending` workflow node. `job_id` is `None` for deferred nodes
/// (fan-out/fan-in/downstream) that the tracker enqueues later.
fn new_workflow_node(run_id: &str, node_name: &str, job_id: Option<String>) -> WorkflowNode {
    WorkflowNode {
        id: uuid::Uuid::now_v7().to_string(),
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

/// Job-metadata blob that links a job back to its workflow node.
fn workflow_metadata_json(run_id: &str, node_name: &str) -> String {
    serde_json::json!({
        "workflow_run_id": run_id,
        "workflow_node_name": node_name,
    })
    .to_string()
}

/// Like `workflow_metadata_json` but flags a node's rollback (compensation) job.
fn compensation_metadata_json(run_id: &str, node_name: &str) -> String {
    serde_json::json!({
        "workflow_run_id": run_id,
        "workflow_node_name": node_name,
        "compensation": true,
    })
    .to_string()
}

/// Parse `{workflow_run_id, workflow_node_name}` from a job's metadata blob.
/// Returns `None` for compensation jobs so they never advance the forward run.
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

/// Parse a compensation job's `{workflow_run_id, workflow_node_name}` — `None`
/// unless the metadata carries `compensation: true`.
fn parse_compensation_metadata(metadata: Option<&str>) -> Option<(String, String)> {
    let parsed: serde_json::Value = serde_json::from_str(metadata?).ok()?;
    if parsed
        .get("compensation")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
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
    storage: &StorageBackend,
    wf: &WorkflowStorageBackend,
    run_id: &str,
    nodes: &[WorkflowNode],
    namespace: Option<&str>,
) {
    for node in nodes {
        if !matches!(
            node.status,
            WorkflowNodeStatus::Pending | WorkflowNodeStatus::Ready
        ) {
            continue;
        }
        if let Some(job_id) = &node.job_id {
            if let Err(e) = storage.cancel_job(job_id, namespace) {
                log::warn!("[taskito-node] cancel_job({job_id}) during workflow cascade: {e}");
            }
        }
        if let Err(e) =
            wf.update_workflow_node_status(run_id, &node.node_name, WorkflowNodeStatus::Skipped)
        {
            log::warn!("[taskito-node] skip node '{}' failed: {e}", node.node_name);
        }
    }
}

/// A JS error carrying a plain message (for validation / serde failures).
fn reason(message: impl Into<String>) -> Error {
    Error::new(napi::Status::GenericFailure, message.into())
}
