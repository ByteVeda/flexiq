//! Read-only inspection methods on `JsQueue`. These scan/aggregate over
//! storage, so each is async and offloads the blocking I/O to the blocking
//! pool instead of stalling the JS event loop for the DB round-trip.

use std::collections::{HashMap, HashSet};

use napi::bindgen_prelude::{spawn_blocking, Result};
use napi_derive::napi;
use taskito_core::Storage;

use super::JsQueue;
use crate::config::{DetailedJobFilter, JobFilter};
use crate::convert::{
    circuit_breaker_to_js, job_error_to_js, job_to_js, log_to_js, metric_to_js, replay_to_js,
    stats_to_js, status_code, worker_to_js, JsCircuitBreaker, JsDagEdge, JsJob, JsJobDag,
    JsJobError, JsJobPage, JsMetric, JsReplayEntry, JsStats, JsTaskLog, JsWorkerRow,
};
use crate::error::{invalid_arg, join_to_napi_err, non_negative, to_napi_err};

const DEFAULT_LIMIT: i64 = 50;

/// Resolve a lowercase status name to its code. An unrecognized status would
/// otherwise silently widen the result set to every job, so a typo fails loudly.
fn parse_status_filter(status: Option<&str>) -> Result<Option<i32>> {
    match status {
        Some(s) => status_code(s)
            .map(Some)
            .ok_or_else(|| invalid_arg(format!("unknown status filter '{s}'"))),
        None => Ok(None),
    }
}

/// Reject an offset on a keyset call. The two ways of paging do not compose —
/// a cursor already says where to resume — so honouring one and ignoring the
/// other would quietly return a different page than the caller asked for.
fn reject_offset(offset: Option<i64>) -> Result<()> {
    match offset {
        Some(o) if o != 0 => Err(invalid_arg(
            "offset is not supported with cursor pagination; pass the page's nextCursor as `after`",
        )),
        _ => Ok(()),
    }
}

/// Decode a caller-supplied page cursor. A malformed one is a bad request, not
/// a reason to silently restart from the first page.
fn parse_cursor(after: Option<&str>) -> Result<Option<(i64, &str)>> {
    after
        .map(taskito_core::storage::cursor::decode_cursor)
        .transpose()
        .map_err(to_napi_err)
}

/// Wrap a page of jobs with the cursor for the next, keyed on `sort_key`.
/// Archived rows always carry `completed_at`; the fallback keeps the key
/// non-null for a row written before it was set.
fn job_page(jobs: Vec<taskito_core::job::Job>, limit: i64, by_completed_at: bool) -> JsJobPage {
    let next_cursor = taskito_core::storage::cursor::next_cursor(&jobs, limit, |j| {
        let key = if by_completed_at {
            j.completed_at.unwrap_or(j.created_at)
        } else {
            j.created_at
        };
        (key, &j.id)
    });
    JsJobPage {
        items: jobs.into_iter().map(job_to_js).collect(),
        next_cursor,
    }
}

#[napi]
impl JsQueue {
    /// Job counts by status across all queues.
    #[napi]
    pub async fn stats(&self) -> Result<JsStats> {
        let storage = self.storage.clone();
        let namespace = self.namespace.clone();
        spawn_blocking(move || {
            storage
                .stats(namespace.as_deref())
                .map(stats_to_js)
                .map_err(to_napi_err)
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Job counts by status for a single queue.
    #[napi]
    pub async fn stats_by_queue(&self, queue: String) -> Result<JsStats> {
        let storage = self.storage.clone();
        let namespace = self.namespace.clone();
        spawn_blocking(move || {
            storage
                .stats_by_queue(&queue, namespace.as_deref())
                .map(stats_to_js)
                .map_err(to_napi_err)
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Job counts by status, keyed by queue name.
    #[napi]
    pub async fn stats_all_queues(&self) -> Result<HashMap<String, JsStats>> {
        let storage = self.storage.clone();
        let namespace = self.namespace.clone();
        spawn_blocking(move || {
            let all = storage
                .stats_all_queues(namespace.as_deref())
                .map_err(to_napi_err)?;
            Ok(all.into_iter().map(|(k, v)| (k, stats_to_js(v))).collect())
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// List jobs, optionally filtered by status/queue/task and paginated.
    #[napi]
    pub async fn list_jobs(&self, filter: Option<JobFilter>) -> Result<Vec<JsJob>> {
        let filter = filter.unwrap_or_default();
        let status = parse_status_filter(filter.status.as_deref())?;
        let limit = non_negative(filter.limit.unwrap_or(DEFAULT_LIMIT), "limit")?;
        let offset = non_negative(filter.offset.unwrap_or(0), "offset")?;
        let storage = self.storage.clone();
        let namespace = self.namespace.clone();
        spawn_blocking(move || {
            let jobs = storage
                .list_jobs(
                    status,
                    filter.queue.as_deref(),
                    filter.task.as_deref(),
                    limit,
                    offset,
                    namespace.as_deref(),
                )
                .map_err(to_napi_err)?;
            Ok(jobs.into_iter().map(job_to_js).collect())
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// List jobs on the wider filter: everything [`JsQueue::list_jobs`] matches
    /// on, plus metadata/error substrings and a created-at range.
    #[napi]
    pub async fn list_jobs_filtered(
        &self,
        filter: Option<DetailedJobFilter>,
    ) -> Result<Vec<JsJob>> {
        let filter = filter.unwrap_or_default();
        let status = parse_status_filter(filter.status.as_deref())?;
        let limit = non_negative(filter.limit.unwrap_or(DEFAULT_LIMIT), "limit")?;
        let offset = non_negative(filter.offset.unwrap_or(0), "offset")?;
        let storage = self.storage.clone();
        let namespace = self.namespace.clone();
        spawn_blocking(move || {
            let jobs = storage
                .list_jobs_filtered(
                    status,
                    filter.queue.as_deref(),
                    filter.task.as_deref(),
                    filter.metadata_like.as_deref(),
                    filter.error_like.as_deref(),
                    filter.created_after,
                    filter.created_before,
                    limit,
                    offset,
                    namespace.as_deref(),
                )
                .map_err(to_napi_err)?;
            Ok(jobs.into_iter().map(job_to_js).collect())
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// List archived (completed, moved out of the live table) jobs, newest first.
    #[napi]
    pub async fn list_archived(
        &self,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<JsJob>> {
        let limit = non_negative(limit.unwrap_or(DEFAULT_LIMIT), "limit")?;
        let offset = non_negative(offset.unwrap_or(0), "offset")?;
        let storage = self.storage.clone();
        let namespace = self.namespace.clone();
        spawn_blocking(move || {
            let jobs = storage
                .list_archived(limit, offset, namespace.as_deref())
                .map_err(to_napi_err)?;
            Ok(jobs.into_iter().map(job_to_js).collect())
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Keyset-paginated [`JsQueue::list_jobs`], ordered by created time. Pass a
    /// page's `nextCursor` back as `after`; `null` means the last page.
    ///
    /// O(page) at any depth on SQLite/Postgres. On Redis the status indexes are
    /// not seekable, so the keyset is applied in memory — correct, but O(rows
    /// matching the filter) rather than O(page).
    #[napi]
    pub async fn list_jobs_after(
        &self,
        filter: Option<JobFilter>,
        after: Option<String>,
    ) -> Result<JsJobPage> {
        let filter = filter.unwrap_or_default();
        let status = parse_status_filter(filter.status.as_deref())?;
        reject_offset(filter.offset)?;
        let limit = non_negative(filter.limit.unwrap_or(DEFAULT_LIMIT), "limit")?;
        let storage = self.storage.clone();
        let namespace = self.namespace.clone();
        spawn_blocking(move || {
            let cursor = parse_cursor(after.as_deref())?;
            let jobs = storage
                .list_jobs_after(
                    status,
                    filter.queue.as_deref(),
                    filter.task.as_deref(),
                    limit,
                    cursor,
                    namespace.as_deref(),
                )
                .map_err(to_napi_err)?;
            Ok(job_page(jobs, limit, false))
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Keyset-paginated [`JsQueue::list_jobs_filtered`], ordered by created
    /// time. See [`JsQueue::list_jobs_after`] for the cursor contract.
    #[napi]
    pub async fn list_jobs_filtered_after(
        &self,
        filter: Option<DetailedJobFilter>,
        after: Option<String>,
    ) -> Result<JsJobPage> {
        let filter = filter.unwrap_or_default();
        let status = parse_status_filter(filter.status.as_deref())?;
        reject_offset(filter.offset)?;
        let limit = non_negative(filter.limit.unwrap_or(DEFAULT_LIMIT), "limit")?;
        let storage = self.storage.clone();
        let namespace = self.namespace.clone();
        spawn_blocking(move || {
            let cursor = parse_cursor(after.as_deref())?;
            let jobs = storage
                .list_jobs_filtered_after(
                    status,
                    filter.queue.as_deref(),
                    filter.task.as_deref(),
                    filter.metadata_like.as_deref(),
                    filter.error_like.as_deref(),
                    filter.created_after,
                    filter.created_before,
                    limit,
                    cursor,
                    namespace.as_deref(),
                )
                .map_err(to_napi_err)?;
            Ok(job_page(jobs, limit, false))
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Keyset-paginated [`JsQueue::list_archived`], ordered by completed time.
    /// See [`JsQueue::list_jobs_after`] for the cursor contract.
    #[napi]
    pub async fn list_archived_after(
        &self,
        limit: Option<i64>,
        after: Option<String>,
    ) -> Result<JsJobPage> {
        let limit = non_negative(limit.unwrap_or(DEFAULT_LIMIT), "limit")?;
        let storage = self.storage.clone();
        let namespace = self.namespace.clone();
        spawn_blocking(move || {
            let cursor = parse_cursor(after.as_deref())?;
            let jobs = storage
                .list_archived_after(limit, cursor, namespace.as_deref())
                .map_err(to_napi_err)?;
            Ok(job_page(jobs, limit, true))
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Error history for a job (one entry per failed attempt).
    #[napi]
    pub async fn get_job_errors(&self, job_id: String) -> Result<Vec<JsJobError>> {
        let storage = self.storage.clone();
        let namespace = self.namespace.clone();
        spawn_blocking(move || {
            let errors = storage
                .get_job_errors(&job_id, namespace.as_deref())
                .map_err(to_napi_err)?;
            Ok(errors.into_iter().map(job_error_to_js).collect())
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Per-execution task metrics within the last `since_ms` milliseconds.
    #[napi]
    pub async fn get_metrics(&self, task: Option<String>, since_ms: i64) -> Result<Vec<JsMetric>> {
        let storage = self.storage.clone();
        let namespace = self.namespace.clone();
        spawn_blocking(move || {
            let metrics = storage
                .get_metrics(task.as_deref(), since_ms, namespace.as_deref())
                .map_err(to_napi_err)?;
            Ok(metrics.into_iter().map(metric_to_js).collect())
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Task logs across jobs, filtered by task/level, newest first.
    /// `since_ms` is a Unix-ms lower bound on `logged_at`.
    #[napi]
    pub async fn query_task_logs(
        &self,
        task_name: Option<String>,
        level: Option<String>,
        since_ms: i64,
        limit: i64,
    ) -> Result<Vec<JsTaskLog>> {
        non_negative(limit, "limit")?;
        let storage = self.storage.clone();
        let namespace = self.namespace.clone();
        spawn_blocking(move || {
            let rows = storage
                .query_task_logs(
                    task_name.as_deref(),
                    level.as_deref(),
                    since_ms,
                    limit,
                    namespace.as_deref(),
                )
                .map_err(to_napi_err)?;
            Ok(rows.into_iter().map(log_to_js).collect())
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Circuit-breaker state for every task that has one.
    #[napi]
    pub async fn list_circuit_breakers(&self) -> Result<Vec<JsCircuitBreaker>> {
        let storage = self.storage.clone();
        spawn_blocking(move || {
            let rows = storage.list_circuit_breakers().map_err(to_napi_err)?;
            Ok(rows.into_iter().map(circuit_breaker_to_js).collect())
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Replays recorded for a job, newest first.
    #[napi]
    pub async fn get_replay_history(&self, job_id: String) -> Result<Vec<JsReplayEntry>> {
        let storage = self.storage.clone();
        spawn_blocking(move || {
            let rows = storage.get_replay_history(&job_id).map_err(to_napi_err)?;
            Ok(rows.into_iter().map(replay_to_js).collect())
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// The dependency DAG reachable from `job_id`: full job rows as nodes
    /// plus `dependency -> dependent` edges, walked in both directions.
    #[napi]
    pub async fn job_dag(&self, job_id: String) -> Result<JsJobDag> {
        let storage = self.storage.clone();
        let namespace = self.namespace.clone();
        spawn_blocking(move || {
            let mut visited: HashSet<String> = HashSet::new();
            // Both endpoints of an edge report it (dependencies from one
            // side, dependents from the other) — dedupe by (from, to).
            let mut seen_edges: HashSet<(String, String)> = HashSet::new();
            let mut nodes = Vec::new();
            let mut edges = Vec::new();
            // An edge is queued as a candidate and kept only once BOTH endpoints resolved to a visible job: the edge lists are id-only, so pushing one before the adjacent node is looked up leaks a foreign job id into a scoped caller's graph even though its node is skipped.
            let mut visible: HashSet<String> = HashSet::new();
            let mut pending = vec![job_id];
            while let Some(current) = pending.pop() {
                if !visited.insert(current.clone()) {
                    continue;
                }
                let Some(job) = storage
                    .get_job(&current, namespace.as_deref())
                    .map_err(to_napi_err)?
                else {
                    continue;
                };
                visible.insert(current.clone());
                nodes.push(job_to_js(job));
                for dep_id in storage
                    .get_dependencies(&current, namespace.as_deref())
                    .map_err(to_napi_err)?
                {
                    if seen_edges.insert((dep_id.clone(), current.clone())) {
                        edges.push(JsDagEdge {
                            from: dep_id.clone(),
                            to: current.clone(),
                        });
                    }
                    pending.push(dep_id);
                }
                for dep_id in storage
                    .get_dependents(&current, namespace.as_deref())
                    .map_err(to_napi_err)?
                {
                    if seen_edges.insert((current.clone(), dep_id.clone())) {
                        edges.push(JsDagEdge {
                            from: current.clone(),
                            to: dep_id.clone(),
                        });
                    }
                    pending.push(dep_id);
                }
            }
            edges.retain(|edge: &JsDagEdge| {
                visible.contains(&edge.from) && visible.contains(&edge.to)
            });
            Ok(JsJobDag { nodes, edges })
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// List registered workers (heartbeat + identity).
    #[napi]
    pub async fn list_workers(&self) -> Result<Vec<JsWorkerRow>> {
        let storage = self.storage.clone();
        spawn_blocking(move || {
            let workers = storage.list_workers().map_err(to_napi_err)?;
            Ok(workers.into_iter().map(worker_to_js).collect())
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Record a heartbeat for a running worker, carrying its current resource
    /// health as a JSON object (`null` clears it). Called from the JS shell
    /// every 5s. Returns the ids of dead workers reaped as a side effect.
    ///
    /// Only the elected reaper sweeps: every worker scanning for dead workers
    /// every 5s is O(N) per cluster, and each returns the same dead ids so a
    /// `WORKER_OFFLINE` event fires N times per death. A non-leader reaps
    /// nothing and returns an empty list.
    #[napi]
    pub async fn worker_heartbeat(
        &self,
        worker_id: String,
        resource_health: Option<String>,
    ) -> Result<Vec<String>> {
        let storage = self.storage.clone();
        spawn_blocking(move || {
            storage
                .heartbeat(&worker_id, resource_health.as_deref())
                .map_err(to_napi_err)?;
            // Reaping is opportunistic — a failure must not fail the heartbeat.
            Ok(taskito_core::storage::reap_dead_workers_if_leader(
                &storage, &worker_id,
            ))
        })
        .await
        .map_err(join_to_napi_err)?
    }
}
