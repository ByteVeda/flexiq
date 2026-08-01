//! Direct-storage `DataSource` — reads and mutates through the public
//! `taskito-core` `Storage` trait (and `taskito-workflows` `WorkflowStorage`)
//! on a connected [`Backend`]. No server, no PyO3.

use std::collections::HashMap;

use anyhow::{anyhow, Result};

use taskito_core::now_millis;
use taskito_core::{Job, JobStatus, NewJob, Storage};
use taskito_workflows::{topological_order, TopologicalNode, WorkflowNodeStatus, WorkflowStorage};

use super::{
    DagNode, DataSource, DeadRow, JobDetail, JobErrorEntry, JobRow, LogEntry, StatsSnapshot,
    WorkerView, WorkflowRunRow,
};
use crate::backend::Backend;

pub struct DbSource {
    be: Backend,
}

impl DbSource {
    pub fn new(be: Backend) -> Self {
        Self { be }
    }
}

impl DataSource for DbSource {
    fn stats(&self) -> Result<StatsSnapshot> {
        let overall = self.be.storage.stats(None)?;
        let mut per_queue: Vec<_> = self
            .be
            .storage
            .stats_all_queues(None)?
            .into_iter()
            .collect();
        per_queue.sort_by(|a, b| a.0.cmp(&b.0));
        let paused = self.be.storage.list_paused_queues()?;
        Ok(StatsSnapshot {
            overall,
            per_queue,
            paused,
        })
    }

    fn jobs(&self, status: Option<JobStatus>, limit: i64) -> Result<Vec<JobRow>> {
        let status = status.map(|s| s as i32);
        let jobs = self
            .be
            .storage
            .list_jobs(status, None, None, limit, 0, None)?;
        Ok(jobs.into_iter().map(job_to_row).collect())
    }

    fn job_detail(&self, id: &str) -> Result<Option<JobDetail>> {
        let Some(job) = self.be.storage.get_job(id)? else {
            return Ok(None);
        };
        let errors = self
            .be
            .storage
            .get_job_errors(id)?
            .into_iter()
            .map(|e| JobErrorEntry {
                attempt: e.attempt,
                error: e.error,
                failed_at: e.failed_at,
            })
            .collect();
        let logs = self
            .be
            .storage
            .get_task_logs(id)?
            .into_iter()
            .map(|l| LogEntry {
                level: l.level,
                message: l.message,
                logged_at: l.logged_at,
            })
            .collect();
        Ok(Some(JobDetail {
            metadata: job.metadata.clone(),
            notes: job.notes.clone(),
            namespace: job.namespace.clone(),
            row: job_to_row(job),
            errors,
            logs,
        }))
    }

    fn dead_letters(&self, limit: i64) -> Result<Vec<DeadRow>> {
        let dead = self.be.storage.list_dead(limit, 0, None)?;
        Ok(dead
            .into_iter()
            .map(|d| DeadRow {
                id: d.id,
                original_job_id: d.original_job_id,
                task_name: d.task_name,
                queue: d.queue,
                error: d.error,
                retry_count: d.retry_count,
                dlq_retry_count: d.dlq_retry_count,
                failed_at: d.failed_at,
            })
            .collect())
    }

    fn workers(&self) -> Result<Vec<WorkerView>> {
        let workers = self.be.storage.list_workers()?;
        Ok(workers
            .into_iter()
            .map(|w| WorkerView {
                worker_id: w.worker_id,
                status: w.status,
                queues: w.queues,
                threads: w.threads,
                last_heartbeat: w.last_heartbeat,
                hostname: w.hostname,
                pid: w.pid,
            })
            .collect())
    }

    fn workflow_runs(&self, limit: i64) -> Result<Vec<WorkflowRunRow>> {
        let runs = self.be.workflows.list_workflow_runs(None, None, limit, 0)?;
        Ok(runs
            .into_iter()
            .map(|r| WorkflowRunRow {
                id: r.id,
                definition_id: r.definition_id,
                state: r.state,
                created_at: r.created_at,
                started_at: r.started_at,
                completed_at: r.completed_at,
                error: r.error,
                parent_run_id: r.parent_run_id,
            })
            .collect())
    }

    fn workflow_dag(&self, run_id: &str, definition_id: &str) -> Result<Vec<DagNode>> {
        // Live per-node status/error, keyed by node name.
        let statuses: NodeStatuses = self
            .be
            .workflows
            .get_workflow_nodes(run_id)?
            .into_iter()
            .map(|n| (n.node_name, (Some(n.status), n.error)))
            .collect();

        // Edge structure comes from the definition's DAG, in topological order.
        let def = self
            .be
            .workflows
            .get_workflow_definition_by_id(definition_id)?
            .ok_or_else(|| anyhow!("workflow definition {definition_id} not found"))?;
        let topo = topological_order(&def.dag_data)?;

        Ok(assemble_dag(topo, &statuses))
    }

    fn cancel(&self, id: &str) -> Result<bool> {
        // Try the Pending path; if the job isn't Pending (e.g. it started
        // running since the UI snapshot), fall back to the cooperative Running
        // path. Never relies on stale UI state to choose.
        if self.be.storage.cancel_job(id, None)? {
            return Ok(true);
        }
        Ok(self.be.storage.request_cancel(id)?)
    }

    fn replay(&self, id: &str) -> Result<String> {
        // No single `replay` on the trait — mirror the PyO3 composition:
        // read the original, enqueue a fresh independent job, record the audit.
        let job = self
            .be
            .storage
            .get_job(id)?
            .ok_or_else(|| anyhow!("job {id} not found"))?;
        let original_error = job.error.clone();
        // id is a UUID, so it needs no JSON escaping.
        let metadata = format!("{{\"replayed_from\":\"{id}\"}}");
        let new_job = NewJob {
            queue: job.queue,
            task_name: job.task_name,
            payload: job.payload,
            priority: job.priority,
            scheduled_at: now_millis(),
            max_retries: job.max_retries,
            timeout_ms: job.timeout_ms,
            unique_key: None,
            metadata: Some(metadata),
            notes: None,
            depends_on: Vec::new(),
            expires_at: None,
            result_ttl_ms: job.result_ttl_ms,
            namespace: job.namespace,
        };
        let created = self.be.storage.enqueue(new_job)?;
        // Audit is best-effort: the job already exists, so a failure here must
        // not report the replay as failed (which would also risk a duplicate on
        // retry). Core has no single transactional replay to make this atomic.
        let _ = self.be.storage.record_replay(
            id,
            &created.id,
            None,
            None,
            original_error.as_deref(),
            None,
        );
        Ok(created.id)
    }

    fn retry_dead(&self, dead_id: &str) -> Result<String> {
        Ok(self.be.storage.retry_dead(dead_id, None)?)
    }

    fn delete_dead(&self, dead_id: &str) -> Result<bool> {
        Ok(self.be.storage.delete_dead(dead_id, None)?)
    }

    fn purge_dead(&self) -> Result<u64> {
        // Max cutoff → every entry, including same-millisecond or clock-skewed
        // future timestamps a `now`-based cutoff would miss.
        Ok(self.be.storage.purge_dead(i64::MAX)?)
    }

    fn pause_queue(&self, queue: &str) -> Result<()> {
        Ok(self.be.storage.pause_queue(queue)?)
    }

    fn resume_queue(&self, queue: &str) -> Result<()> {
        Ok(self.be.storage.resume_queue(queue)?)
    }
}

/// Per-node `(status, error)` keyed by node name. `status` is `None` until the
/// tracker materializes the node (i.e. it is still effectively pending).
type NodeStatuses = HashMap<String, (Option<WorkflowNodeStatus>, Option<String>)>;

/// Merge topologically-ordered DAG nodes with their live status and compute each
/// node's depth (longest path from a root). Pure — no storage — so it is unit
/// testable. Predecessors always precede a node in topological order, so their
/// depth is known by the time we reach it.
fn assemble_dag(topo: Vec<TopologicalNode>, statuses: &NodeStatuses) -> Vec<DagNode> {
    let mut depth: HashMap<String, usize> = HashMap::new();
    let mut out = Vec::with_capacity(topo.len());
    for tn in topo {
        let d = tn
            .predecessors
            .iter()
            .map(|p| depth.get(p).copied().unwrap_or(0) + 1)
            .max()
            .unwrap_or(0);
        depth.insert(tn.name.clone(), d);
        let (status, error) = statuses.get(&tn.name).cloned().unwrap_or((None, None));
        out.push(DagNode {
            status,
            error,
            predecessors: tn.predecessors,
            depth: d,
            name: tn.name,
        });
    }
    out
}

fn job_to_row(job: Job) -> JobRow {
    JobRow {
        id: job.id,
        task_name: job.task_name,
        queue: job.queue,
        status: job.status,
        priority: job.priority,
        retry_count: job.retry_count,
        max_retries: job.max_retries,
        created_at: job.created_at,
        scheduled_at: job.scheduled_at,
        started_at: job.started_at,
        completed_at: job.completed_at,
        error: job.error,
        cancel_requested: job.cancel_requested,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed() -> DbSource {
        let be = crate::backend::open(":memory:").expect("open in-memory");
        DbSource::new(be)
    }

    fn enqueue(src: &DbSource, task: &str, queue: &str) -> Job {
        src.be
            .storage
            .enqueue(NewJob {
                queue: queue.to_string(),
                task_name: task.to_string(),
                payload: Vec::new(),
                priority: 0,
                scheduled_at: now_millis(),
                max_retries: 3,
                timeout_ms: 30_000,
                unique_key: None,
                metadata: None,
                notes: None,
                depends_on: Vec::new(),
                expires_at: None,
                result_ttl_ms: None,
                namespace: None,
            })
            .expect("enqueue")
    }

    #[test]
    fn jobs_and_stats_reflect_enqueue() {
        let src = seed();
        enqueue(&src, "send_email", "default");
        enqueue(&src, "resize_image", "media");

        let jobs = src.jobs(None, 50).unwrap();
        assert_eq!(jobs.len(), 2);

        let pending = src.jobs(Some(JobStatus::Pending), 50).unwrap();
        assert_eq!(pending.len(), 2);
        let running = src.jobs(Some(JobStatus::Running), 50).unwrap();
        assert!(running.is_empty());

        let stats = src.stats().unwrap();
        assert_eq!(stats.overall.pending, 2);
        assert_eq!(stats.per_queue.len(), 2);
    }

    #[test]
    fn cancel_pending_moves_out_of_pending() {
        let src = seed();
        let job = enqueue(&src, "send_email", "default");

        assert!(src.cancel(&job.id).unwrap());
        let pending = src.jobs(Some(JobStatus::Pending), 50).unwrap();
        assert!(pending.is_empty());
    }

    #[test]
    fn replay_creates_new_pending_job() {
        let src = seed();
        let job = enqueue(&src, "send_email", "default");
        src.cancel(&job.id).unwrap();

        let new_id = src.replay(&job.id).unwrap();
        assert_ne!(new_id, job.id);

        let detail = src.job_detail(&new_id).unwrap().expect("new job exists");
        assert_eq!(detail.row.status, JobStatus::Pending);
        assert_eq!(
            detail.metadata.as_deref(),
            Some(format!("{{\"replayed_from\":\"{}\"}}", job.id).as_str())
        );
    }

    #[test]
    fn file_backed_open_persists_across_reopen() {
        let path = std::env::temp_dir().join(format!("taskito_tui_e2e_{}.db", std::process::id()));
        let url = format!("sqlite://{}", path.display());
        let _ = std::fs::remove_file(&path);

        // First handle writes (stands in for an SDK worker process).
        {
            let src = DbSource::new(crate::backend::open(&url).unwrap());
            enqueue(&src, "persisted_task", "default");
        }
        // A fresh handle on the same file reads it back — the cross-SDK path.
        let src = DbSource::new(crate::backend::open(&url).unwrap());
        let jobs = src.jobs(None, 50).unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].task_name, "persisted_task");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    fn tn(name: &str, preds: &[&str]) -> TopologicalNode {
        TopologicalNode {
            name: name.to_string(),
            predecessors: preds.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn assemble_dag_orders_by_depth_and_carries_edges() {
        // Diamond: extract → {transform_a, transform_b} → load.
        let topo = vec![
            tn("extract", &[]),
            tn("transform_a", &["extract"]),
            tn("transform_b", &["extract"]),
            tn("load", &["transform_a", "transform_b"]),
        ];
        let mut statuses: NodeStatuses = HashMap::new();
        statuses.insert(
            "extract".into(),
            (Some(WorkflowNodeStatus::Completed), None),
        );
        statuses.insert(
            "transform_a".into(),
            (Some(WorkflowNodeStatus::Running), None),
        );

        let dag = assemble_dag(topo, &statuses);

        let shape: Vec<_> = dag.iter().map(|n| (n.name.as_str(), n.depth)).collect();
        assert_eq!(
            shape,
            vec![
                ("extract", 0),
                ("transform_a", 1),
                ("transform_b", 1),
                ("load", 2),
            ]
        );
        assert_eq!(dag[3].predecessors, vec!["transform_a", "transform_b"]);
        assert_eq!(dag[0].status, Some(WorkflowNodeStatus::Completed));
        assert_eq!(dag[3].status, None); // not yet materialized → pending
    }

    #[test]
    fn pause_resume_roundtrip() {
        let src = seed();
        src.pause_queue("default").unwrap();
        assert_eq!(src.stats().unwrap().paused, vec!["default".to_string()]);
        src.resume_queue("default").unwrap();
        assert!(src.stats().unwrap().paused.is_empty());
    }
}
