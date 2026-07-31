//! Read-only queries: get_job, list_jobs, list_jobs_filtered, stats.

use redis::Commands;

use crate::error::Result;
use crate::job::{Job, JobStatus};
use crate::storage::redis_backend::{map_err, strip_list_blobs, RedisStorage};
use crate::storage::QueueStats;

impl RedisStorage {
    /// List jobs by filter. Rows are blob-free — fetch the full job with
    /// [`get_job`](Self::get_job).
    pub fn list_jobs(
        &self,
        status: Option<i32>,
        queue_name: Option<&str>,
        task_name: Option<&str>,
        limit: i64,
        offset: i64,
        namespace: Option<&str>,
    ) -> Result<Vec<Job>> {
        let mut conn = self.conn()?;

        // Load the candidate jobs from the table(s) that can hold the requested
        // status: terminal statuses live in the archive, live statuses in the
        // active sets, and an unfiltered status spans both.
        let mut jobs: Vec<Job> = match status {
            Some(s) if Self::is_terminal_status(s) => {
                let key = self.key(&["archived", "status", &s.to_string()]);
                let ids: Vec<String> = conn.smembers(&key).map_err(map_err)?;
                self.load_archived_jobs(&mut conn, &ids)?
            }
            Some(s) => {
                let key = self.key(&["jobs", "status", &s.to_string()]);
                let ids: Vec<String> = conn.smembers(&key).map_err(map_err)?;
                self.load_live_jobs(&mut conn, &ids)?
            }
            None => {
                let live_key = self.key(&["jobs", "all"]);
                let live_ids: Vec<String> = conn.zrange(&live_key, 0, -1).map_err(map_err)?;
                let mut all = self.load_live_jobs(&mut conn, &live_ids)?;

                let archived_key = self.key(&["archived", "all"]);
                let archived_ids: Vec<String> =
                    conn.zrange(&archived_key, 0, -1).map_err(map_err)?;
                all.extend(self.load_archived_jobs(&mut conn, &archived_ids)?);
                all
            }
        };

        // Apply the remaining filters in memory.
        jobs.retain(|job| {
            queue_name.is_none_or(|q| job.queue == q)
                && task_name.is_none_or(|t| job.task_name == t)
                && namespace.is_none_or(|ns| job.namespace.as_deref() == Some(ns))
        });

        jobs.sort_by_key(|j| std::cmp::Reverse(j.created_at));

        let start = (offset.max(0) as usize).min(jobs.len());
        let end = start.saturating_add(limit.max(0) as usize).min(jobs.len());
        Ok(jobs[start..end].to_vec())
    }

    /// Keyset-paginated `list_jobs`. The live/archived status indexes are plain
    /// SETs (and the None-merge spans two differently-scored ZSETs), so there is
    /// no ZSET to seek — this loads the same candidate set `list_jobs` does and
    /// applies the `(created_at, id) < cursor` keyset in memory. Correct and
    /// stable across inserts; it does not add the Big-O win the Diesel backends
    /// get (Redis `list_jobs` is already O(N) regardless of pagination).
    pub fn list_jobs_after(
        &self,
        status: Option<i32>,
        queue_name: Option<&str>,
        task_name: Option<&str>,
        limit: i64,
        after: Option<(i64, &str)>,
        namespace: Option<&str>,
    ) -> Result<Vec<Job>> {
        let mut conn = self.conn()?;
        let mut jobs = self.load_status_candidates(&mut conn, status)?;
        jobs.retain(|job| {
            // Re-check the status against the hydrated job: the index is read
            // before each job, so a job can change status in between.
            status.is_none_or(|s| job.status as i32 == s)
                && queue_name.is_none_or(|q| job.queue == q)
                && task_name.is_none_or(|t| job.task_name == t)
                && namespace.is_none_or(|ns| job.namespace.as_deref() == Some(ns))
        });
        Ok(keyset_take(jobs, after, limit))
    }

    /// Load the candidate jobs for a status filter (terminal → archive, live →
    /// active sets, None → both), shared by the keyset listing paths.
    fn load_status_candidates(
        &self,
        conn: &mut redis::Connection,
        status: Option<i32>,
    ) -> Result<Vec<Job>> {
        match status {
            Some(s) if Self::is_terminal_status(s) => {
                let key = self.key(&["archived", "status", &s.to_string()]);
                let ids: Vec<String> = conn.smembers(&key).map_err(map_err)?;
                self.load_archived_jobs(conn, &ids)
            }
            Some(s) => {
                let key = self.key(&["jobs", "status", &s.to_string()]);
                let ids: Vec<String> = conn.smembers(&key).map_err(map_err)?;
                self.load_live_jobs(conn, &ids)
            }
            None => {
                let live_key = self.key(&["jobs", "all"]);
                let live_ids: Vec<String> = conn.zrange(&live_key, 0, -1).map_err(map_err)?;
                let mut all = self.load_live_jobs(conn, &live_ids)?;
                let archived_key = self.key(&["archived", "all"]);
                let archived_ids: Vec<String> =
                    conn.zrange(&archived_key, 0, -1).map_err(map_err)?;
                all.extend(self.load_archived_jobs(conn, &archived_ids)?);
                Ok(all)
            }
        }
    }

    /// True when `status` is a terminal status whose jobs live in the archive.
    fn is_terminal_status(status: i32) -> bool {
        matches!(
            JobStatus::from_i32(status),
            Some(JobStatus::Complete)
                | Some(JobStatus::Failed)
                | Some(JobStatus::Dead)
                | Some(JobStatus::Cancelled)
        )
    }

    fn load_live_jobs(&self, conn: &mut redis::Connection, ids: &[String]) -> Result<Vec<Job>> {
        let mut jobs = Vec::new();
        for id in ids {
            if let Some(mut job) = self.load_job(conn, id)? {
                strip_list_blobs(&mut job);
                jobs.push(job);
            }
        }
        Ok(jobs)
    }

    fn load_archived_jobs(&self, conn: &mut redis::Connection, ids: &[String]) -> Result<Vec<Job>> {
        let mut jobs = Vec::new();
        for id in ids {
            if let Some(mut job) = self.load_archived_job(conn, id)? {
                strip_list_blobs(&mut job);
                jobs.push(job);
            }
        }
        Ok(jobs)
    }

    /// Fetch a job by id, blobs included — live jobs first, then the archive.
    pub fn get_job(&self, id: &str) -> Result<Option<Job>> {
        let mut conn = self.conn()?;
        match self.load_job(&mut conn, id)? {
            Some(job) => Ok(Some(job)),
            None => self.load_archived_job(&mut conn, id),
        }
    }

    /// Global queue statistics: live counts plus terminal counts from the
    /// archive. `namespace` of `None` counts every namespace, matching
    /// `list_jobs`.
    pub fn stats(&self, namespace: Option<&str>) -> Result<QueueStats> {
        if let Some(scope) = namespace {
            let mut conn = self.conn()?;
            let mut stats = QueueStats::default();
            for job in self.jobs_in_namespace(&mut conn, scope)? {
                Self::tally(&mut stats, job.status);
            }
            return Ok(stats);
        }

        let mut conn = self.conn()?;
        let mut stats = QueueStats::default();

        // Pending/Running stay in the live `jobs:status:<n>` sets; terminal
        // jobs are archived, so their counts come from `archived:status:<n>`.
        let live_pending = self.key(&["jobs", "status", "0"]);
        let live_running = self.key(&["jobs", "status", "1"]);
        stats.pending = conn.scard(&live_pending).map_err(map_err)?;
        stats.running = conn.scard(&live_running).map_err(map_err)?;

        for (status_int, field) in [
            (2, &mut stats.completed),
            (3, &mut stats.failed),
            (4, &mut stats.dead),
            (5, &mut stats.cancelled),
        ] {
            let key = self.key(&["archived", "status", &status_int.to_string()]);
            let count: i64 = conn.scard(&key).map_err(map_err)?;
            *field = count;
        }

        Ok(stats)
    }

    /// `SINTERCARD` of a membership set with a per-status set — the count of
    /// jobs that are both in `membership_key` and in the given status, computed
    /// server-side without transferring or deserializing any job. Redis starts
    /// from the smaller set, so e.g. intersecting against the (small) running
    /// set stays cheap regardless of how large the membership set has grown.
    fn count_in_status(
        &self,
        conn: &mut redis::Connection,
        membership_key: &str,
        status: JobStatus,
    ) -> Result<i64> {
        let status_key = self.key(&["jobs", "status", &(status as i32).to_string()]);
        redis::cmd("SINTERCARD")
            .arg(2)
            .arg(membership_key)
            .arg(&status_key)
            .query::<i64>(conn)
            .map_err(map_err)
    }

    /// `SINTERCARD` of a per-queue archived set with a terminal status set —
    /// the count of archived jobs for that queue in the given status, computed
    /// server-side without transferring any job.
    fn count_in_archived_status(
        &self,
        conn: &mut redis::Connection,
        membership_key: &str,
        status: JobStatus,
    ) -> Result<i64> {
        let status_key = self.key(&["archived", "status", &(status as i32).to_string()]);
        redis::cmd("SINTERCARD")
            .arg(2)
            .arg(membership_key)
            .arg(&status_key)
            .query::<i64>(conn)
            .map_err(map_err)
    }

    /// Full breakdown for one queue: live pending/running from `jobs:by_queue`,
    /// terminal counts from `archived:by_queue` — all `SINTERCARD`, bounded by
    /// the status count rather than the job population.
    fn queue_stats(&self, conn: &mut redis::Connection, queue_name: &str) -> Result<QueueStats> {
        let by_queue = self.key(&["jobs", "by_queue", queue_name]);
        let archived_by_queue = self.key(&["archived", "by_queue", queue_name]);
        Ok(QueueStats {
            pending: self.count_in_status(conn, &by_queue, JobStatus::Pending)?,
            running: self.count_in_status(conn, &by_queue, JobStatus::Running)?,
            completed: self.count_in_archived_status(
                conn,
                &archived_by_queue,
                JobStatus::Complete,
            )?,
            failed: self.count_in_archived_status(conn, &archived_by_queue, JobStatus::Failed)?,
            dead: self.count_in_archived_status(conn, &archived_by_queue, JobStatus::Dead)?,
            cancelled: self.count_in_archived_status(
                conn,
                &archived_by_queue,
                JobStatus::Cancelled,
            )?,
        })
    }

    /// Count running jobs for a specific task name (for per-task concurrency limiting).
    pub fn count_running_by_task(&self, task_name: &str) -> Result<i64> {
        let mut conn = self.conn()?;
        let by_task_key = self.key(&["jobs", "by_task", task_name]);
        self.count_in_status(&mut conn, &by_task_key, JobStatus::Running)
    }

    /// Count pending jobs on a queue (for the `max_pending` admission cap).
    pub fn count_pending_by_queue(&self, queue_name: &str) -> Result<i64> {
        let mut conn = self.conn()?;
        let by_queue_key = self.key(&["jobs", "by_queue", queue_name]);
        self.count_in_status(&mut conn, &by_queue_key, JobStatus::Pending)
    }

    /// Statistics for one queue: live counts plus archived terminal counts.
    pub fn stats_by_queue(&self, queue_name: &str, namespace: Option<&str>) -> Result<QueueStats> {
        let mut conn = self.conn()?;
        let Some(scope) = namespace else {
            return self.queue_stats(&mut conn, queue_name);
        };

        let mut stats = QueueStats::default();
        for job in self.jobs_in_namespace(&mut conn, scope)? {
            if job.queue == queue_name {
                Self::tally(&mut stats, job.status);
            }
        }
        Ok(stats)
    }

    /// Every live and archived job in `namespace`, blob-free.
    ///
    /// Redis has no per-namespace index, so a scoped count walks the same
    /// candidate set `list_jobs` does — O(N), like every other Redis listing,
    /// and correct for rows written before namespaces were used.
    fn jobs_in_namespace(&self, conn: &mut redis::Connection, namespace: &str) -> Result<Vec<Job>> {
        let live_key = self.key(&["jobs", "all"]);
        let live_ids: Vec<String> = conn.zrange(&live_key, 0, -1).map_err(map_err)?;
        let mut jobs = self.load_live_jobs(conn, &live_ids)?;

        let archived_key = self.key(&["archived", "all"]);
        let archived_ids: Vec<String> = conn.zrange(&archived_key, 0, -1).map_err(map_err)?;
        jobs.extend(self.load_archived_jobs(conn, &archived_ids)?);

        jobs.retain(|job| job.namespace.as_deref() == Some(namespace));
        Ok(jobs)
    }

    /// Add one job to the matching `QueueStats` field.
    fn tally(stats: &mut QueueStats, status: JobStatus) {
        let field = match status {
            JobStatus::Pending => &mut stats.pending,
            JobStatus::Running => &mut stats.running,
            JobStatus::Complete => &mut stats.completed,
            JobStatus::Failed => &mut stats.failed,
            JobStatus::Dead => &mut stats.dead,
            JobStatus::Cancelled => &mut stats.cancelled,
        };
        *field += 1;
    }

    /// Statistics broken down per queue name.
    pub fn stats_all_queues(
        &self,
        namespace: Option<&str>,
    ) -> Result<std::collections::HashMap<String, QueueStats>> {
        let mut conn = self.conn()?;

        if let Some(scope) = namespace {
            let mut map = std::collections::HashMap::<String, QueueStats>::new();
            for job in self.jobs_in_namespace(&mut conn, scope)? {
                Self::tally(map.entry(job.queue.clone()).or_default(), job.status);
            }
            return Ok(map);
        }

        // Enumerate queues by scanning the live and archived per-queue membership
        // keys (one per queue with at least one job — empty sets are auto-removed
        // by Redis). A queue with only terminal jobs lives solely in the archived
        // set, so both prefixes must be scanned. Bounded by the number of queues,
        // not the job population.
        let mut queues: Vec<String> = Vec::new();
        for prefix in [["jobs", "by_queue"], ["archived", "by_queue"]] {
            let pattern = self.key(&[prefix[0], prefix[1], "*"]);
            let strip = self.key(&[prefix[0], prefix[1], ""]);
            let mut cursor = 0u64;
            loop {
                let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(&pattern)
                    .arg("COUNT")
                    .arg(100)
                    .query(&mut conn)
                    .map_err(map_err)?;
                for key in batch {
                    if let Some(name) = key.strip_prefix(&strip) {
                        queues.push(name.to_string());
                    }
                }
                cursor = next;
                if cursor == 0 {
                    break;
                }
            }
        }
        // SCAN may return duplicates, and a queue can appear in both sets.
        queues.sort_unstable();
        queues.dedup();

        let mut map = std::collections::HashMap::<String, QueueStats>::new();
        for queue in queues {
            let stats = self.queue_stats(&mut conn, &queue)?;
            map.insert(queue, stats);
        }
        Ok(map)
    }

    #[allow(clippy::too_many_arguments)]
    /// `list_jobs` with extra filters (metadata/error substring, created-at
    /// range). Rows are blob-free like every listing.
    pub fn list_jobs_filtered(
        &self,
        status: Option<i32>,
        queue_name: Option<&str>,
        task_name: Option<&str>,
        metadata_like: Option<&str>,
        error_like: Option<&str>,
        created_after: Option<i64>,
        created_before: Option<i64>,
        limit: i64,
        offset: i64,
        namespace: Option<&str>,
    ) -> Result<Vec<Job>> {
        let mut conn = self.conn()?;

        // Source candidates from the table(s) that can hold the requested
        // status: terminal statuses live in the archive, live statuses in the
        // active sets, and an unfiltered status spans both.
        let candidates: Vec<Job> = match status {
            Some(s) if Self::is_terminal_status(s) => {
                let key = self.key(&["archived", "status", &s.to_string()]);
                let ids: Vec<String> = conn.smembers(&key).map_err(map_err)?;
                self.load_archived_jobs(&mut conn, &ids)?
            }
            Some(s) => {
                let key = self.key(&["jobs", "status", &s.to_string()]);
                let ids: Vec<String> = conn.smembers(&key).map_err(map_err)?;
                self.load_live_jobs(&mut conn, &ids)?
            }
            None => {
                let live_key = self.key(&["jobs", "all"]);
                let live_ids: Vec<String> = conn.zrange(&live_key, 0, -1).map_err(map_err)?;
                let mut all = self.load_live_jobs(&mut conn, &live_ids)?;

                let archived_key = self.key(&["archived", "all"]);
                let archived_ids: Vec<String> =
                    conn.zrange(&archived_key, 0, -1).map_err(map_err)?;
                all.extend(self.load_archived_jobs(&mut conn, &archived_ids)?);
                all
            }
        };

        let mut jobs = Vec::new();
        for job in candidates {
            {
                if let Some(s) = status {
                    if job.status as i32 != s {
                        continue;
                    }
                }
                if let Some(q) = queue_name {
                    if job.queue != q {
                        continue;
                    }
                }
                if let Some(t) = task_name {
                    if job.task_name != t {
                        continue;
                    }
                }
                if let Some(m) = metadata_like {
                    match &job.metadata {
                        Some(meta) if meta.contains(m) => {}
                        _ => continue,
                    }
                }
                if let Some(e) = error_like {
                    match &job.error {
                        Some(err) if err.contains(e) => {}
                        _ => continue,
                    }
                }
                if let Some(after) = created_after {
                    if job.created_at < after {
                        continue;
                    }
                }
                if let Some(before) = created_before {
                    if job.created_at > before {
                        continue;
                    }
                }
                if let Some(ns) = namespace {
                    if job.namespace.as_deref() != Some(ns) {
                        continue;
                    }
                }
                jobs.push(job);
            }
        }

        jobs.sort_by_key(|j| std::cmp::Reverse(j.created_at));

        let start = (offset.max(0) as usize).min(jobs.len());
        let end = start.saturating_add(limit.max(0) as usize).min(jobs.len());
        Ok(jobs[start..end].to_vec())
    }

    /// Keyset-paginated `list_jobs_filtered` — same in-memory keyset approach
    /// and caveats as [`Self::list_jobs_after`].
    #[allow(clippy::too_many_arguments)]
    pub fn list_jobs_filtered_after(
        &self,
        status: Option<i32>,
        queue_name: Option<&str>,
        task_name: Option<&str>,
        metadata_like: Option<&str>,
        error_like: Option<&str>,
        created_after: Option<i64>,
        created_before: Option<i64>,
        limit: i64,
        after: Option<(i64, &str)>,
        namespace: Option<&str>,
    ) -> Result<Vec<Job>> {
        let mut conn = self.conn()?;
        let candidates = self.load_status_candidates(&mut conn, status)?;

        let mut jobs = Vec::new();
        for job in candidates {
            if status.is_some_and(|s| job.status as i32 != s) {
                continue;
            }
            if queue_name.is_some_and(|q| job.queue != q) {
                continue;
            }
            if task_name.is_some_and(|t| job.task_name != t) {
                continue;
            }
            if let Some(m) = metadata_like {
                if !job.metadata.as_deref().is_some_and(|meta| meta.contains(m)) {
                    continue;
                }
            }
            if let Some(e) = error_like {
                if !job.error.as_deref().is_some_and(|err| err.contains(e)) {
                    continue;
                }
            }
            if created_after.is_some_and(|a| job.created_at < a) {
                continue;
            }
            if created_before.is_some_and(|b| job.created_at > b) {
                continue;
            }
            if namespace.is_some_and(|ns| job.namespace.as_deref() != Some(ns)) {
                continue;
            }
            jobs.push(job);
        }

        Ok(keyset_take(jobs, after, limit))
    }
}

/// Sort `jobs` by `(created_at, id)` descending, keep only rows strictly after
/// the `after` cursor in that order, and take the first `limit`. The in-memory
/// equivalent of the Diesel `(created_at, id) < cursor` keyset.
fn keyset_take(mut jobs: Vec<Job>, after: Option<(i64, &str)>, limit: i64) -> Vec<Job> {
    jobs.sort_by(|a, b| (b.created_at, &b.id).cmp(&(a.created_at, &a.id)));
    jobs.into_iter()
        .filter(|j| match after {
            Some((cursor_created_at, cursor_id)) => {
                (j.created_at, j.id.as_str()) < (cursor_created_at, cursor_id)
            }
            None => true,
        })
        .take(limit.max(0) as usize)
        .collect()
}
