//! Job state mutations: complete, fail, retry, cancel, cascade-cancel,
//! dependency lookups, and progress updates.

use redis::Commands;

use super::dequeue_score;
use crate::error::{QueueError, Result};
use crate::job::{now_millis, JobStatus};
use crate::storage::redis_backend::{map_err, RedisStorage};

/// Lua: write the precomputed job JSON only if the job is still live (its
/// `job:<id>` key exists). Guards `update_progress` so a concurrent archive
/// (complete/fail/reap) can't be clobbered into a resurrected orphan key that
/// belongs to no index. Returns 1 on write, 0 if the job is already gone.
const SET_IF_LIVE: &str = r#"
    if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
    redis.call('SET', KEYS[1], ARGV[1])
    return 1
"#;

/// Lua: mark a job cancel-requested (write JSON + add to the cancel set) only
/// if it is still live-Running (member of `jobs:status:1`). Prevents resurrecting
/// a job that was archived concurrently. Returns 1 if applied, 0 otherwise.
const REQUEST_CANCEL_IF_RUNNING: &str = r#"
    if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 0 then return 0 end
    redis.call('SET', KEYS[1], ARGV[2])
    redis.call('SADD', KEYS[3], ARGV[1])
    return 1
"#;

/// Lua: move a job from Running back to Pending and release its execution
/// claim, all-or-nothing. Guarded by Running-set membership (never decodes
/// `job.status` in Lua) so a job archived between the Rust read and this
/// script is a no-op. Also drops any pending cancel request so the fresh
/// attempt isn't insta-cancelled, and clears the claim key + its time index
/// so a healthy worker can re-claim. Returns 1 if applied, 0 otherwise.
///
/// KEYS[8] (the job's debounce index, present only when it carries a debounce
/// key) re-admits the job to its window, since it is pending again. ARGV[4]
/// carries its `created_at` score.
const REQUEUE_STUCK_IF_RUNNING: &str = r#"
    if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 0 then return 0 end
    redis.call('SET', KEYS[1], ARGV[2])
    redis.call('SREM', KEYS[2], ARGV[1])
    redis.call('SADD', KEYS[3], ARGV[1])
    redis.call('ZADD', KEYS[4], ARGV[3], ARGV[1])
    redis.call('SREM', KEYS[5], ARGV[1])
    redis.call('DEL', KEYS[6])
    redis.call('ZREM', KEYS[7], ARGV[1])
    if KEYS[8] then
        redis.call('ZADD', KEYS[8], ARGV[4], ARGV[1])
    end
    return 1
"#;

impl RedisStorage {
    /// Mark a job completed with its result, moving it into the archive.
    ///
    /// A job in another namespace reports `JobNotFound`, like an unknown id.
    pub fn complete(
        &self,
        id: &str,
        result_bytes: Option<Vec<u8>>,
        namespace: Option<&str>,
    ) -> Result<()> {
        let mut conn = self.conn()?;
        let mut job = self.get_job_required_in(id, namespace)?;

        if job.status != JobStatus::Running {
            return Err(QueueError::JobNotFound(id.to_string()));
        }

        let old_status = job.status;
        job.status = JobStatus::Complete;
        job.completed_at = Some(now_millis());
        job.result = result_bytes;
        self.archive_job_immediately(&mut conn, &job, old_status)?;

        // Release the unique-key pointer only if it still points at THIS job —
        // a concurrent enqueue_unique reusing the key after the archive removed
        // `job:<id>` may have already repointed it to a new job, whose dedup
        // lock an unconditional DEL would clobber.
        if let Some(ref uk) = job.unique_key {
            self.release_unique_key(&mut conn, uk, id)?;
        }

        Ok(())
    }

    /// Persist many successful completions. Redis has no cross-key transaction
    /// to coalesce (each job's archive spans several keys), so this loops the
    /// single-job path — the batching win is in the Diesel backends. If any job
    /// is not `Running`, its `complete` errors and the caller falls back.
    pub fn complete_batch(
        &self,
        completions: &[crate::job::JobCompletion],
        namespace: Option<&str>,
    ) -> Result<()> {
        for c in completions {
            // Read before the archive move so the job's own namespace is still
            // reachable — an unscoped caller has no other source for it, and
            // the metric must be filed under the job's tenant either way.
            let job_namespace = self
                .get_job(&c.job_id, namespace)?
                .and_then(|job| job.namespace);
            self.complete(&c.job_id, c.result.clone(), namespace)?;
            self.complete_execution(&c.job_id, namespace)?;
            self.record_metric(
                &c.task_name,
                &c.job_id,
                c.wall_time_ns,
                0,
                true,
                job_namespace.as_deref(),
            )?;
        }
        Ok(())
    }

    /// Mark a job terminally failed, moving it into the archive.
    pub fn fail(&self, id: &str, error: &str) -> Result<()> {
        let mut conn = self.conn()?;
        let mut job = self.get_job_required(id)?;

        if job.status != JobStatus::Running {
            return Err(QueueError::JobNotFound(id.to_string()));
        }

        let old_status = job.status;
        job.status = JobStatus::Failed;
        job.completed_at = Some(now_millis());
        job.error = Some(error.to_string());
        self.archive_job_immediately(&mut conn, &job, old_status)?;

        Ok(())
    }

    /// Re-schedule a job for retry at `next_scheduled_at` (Unix milliseconds),
    /// incrementing its `retry_count`.
    ///
    /// A job in another namespace reports `JobNotFound`, like an unknown id.
    pub fn retry(&self, id: &str, next_scheduled_at: i64, namespace: Option<&str>) -> Result<()> {
        let mut conn = self.conn()?;
        let mut job = self.get_job_required_in(id, namespace)?;
        let old_status = job.status;

        job.status = JobStatus::Pending;
        job.scheduled_at = next_scheduled_at;
        job.retry_count = job.retry_count.saturating_add(1);
        job.started_at = None;
        job.completed_at = None;
        job.error = None;

        // Atomic status-move + pending-zset insert, plus the claim revocation
        // that has to share the transaction with the `retry_count` bump.
        self.requeue_pending(&mut conn, &job, old_status, true)?;

        Ok(())
    }

    /// Re-schedule a job back to `Pending` without consuming retry budget.
    /// Mirrors [`retry`](Self::retry) for soft-gate reschedules where the job
    /// never executed, so `retry_count` must be preserved.
    pub fn reschedule(&self, id: &str, next_scheduled_at: i64) -> Result<()> {
        let mut conn = self.conn()?;
        let mut job = self.get_job_required(id)?;
        let old_status = job.status;

        job.status = JobStatus::Pending;
        job.scheduled_at = next_scheduled_at;
        job.started_at = None;
        job.completed_at = None;
        job.error = None;

        // Atomic status-move + pending-zset insert (see `requeue_pending`). No
        // claim to revoke: a soft-gate reschedule is a job that never ran.
        self.requeue_pending(&mut conn, &job, old_status, false)?;

        Ok(())
    }

    /// Force a `Running` job back to `Pending` and release its execution
    /// claim atomically. Preserves retry budget (no `retry_count` bump) and
    /// clears any pending cancel request. Returns `false` when the job is
    /// missing or not `Running`.
    pub fn requeue_stuck(&self, id: &str, now: i64) -> Result<bool> {
        let mut conn = self.conn()?;
        let mut job = match self.get_job(id, None)? {
            Some(j) => j,
            None => return Ok(false),
        };
        if job.status != JobStatus::Running {
            return Ok(false);
        }

        job.status = JobStatus::Pending;
        job.scheduled_at = now;
        job.started_at = None;
        job.completed_at = None;
        job.error = None;
        job.cancel_requested = false;

        let job_json = serde_json::to_string(&job)?;
        let job_key = self.key(&["job", id]);
        let running_key = self.key(&["jobs", "status", &(JobStatus::Running as i32).to_string()]);
        let pending_key = self.key(&["jobs", "status", &(JobStatus::Pending as i32).to_string()]);
        let queue_key = self.key(&["queue", &job.queue, "pending"]);
        let cancel_set = self.key(&["jobs", "cancel_requested"]);
        let claim_key = self.key(&["exec_claim", id]);
        let claim_index_key = self.key(&["exec_claims", "by_time"]);
        let score = dequeue_score(job.priority, job.scheduled_at);

        let requeue_script = redis::Script::new(REQUEUE_STUCK_IF_RUNNING);
        let mut invocation = requeue_script.prepare_invoke();
        invocation
            .key(&job_key)
            .key(&running_key)
            .key(&pending_key)
            .key(&queue_key)
            .key(&cancel_set)
            .key(&claim_key)
            .key(&claim_index_key);
        if let Some(debounce_key) = self.job_debounce_index_key(&job) {
            invocation.key(debounce_key);
        }
        let applied: i32 = invocation
            .arg(id)
            .arg(&job_json)
            .arg(score)
            .arg(job.created_at)
            .invoke(&mut conn)
            .map_err(map_err)?;

        if applied == 1 {
            // Best-effort backlog reindex Running→Pending; a follow-up rather
            // than folded into the correctness-critical requeue-stuck script
            // (see `reindex_pubsub_best_effort`).
            self.reindex_pubsub_best_effort(&mut conn, &job, JobStatus::Pending);
        }
        Ok(applied == 1)
    }

    /// Cancel a `Pending` job (archived as `Cancelled`) and cascade-cancel its
    /// dependents. Returns `false` when the job is missing or not pending.
    ///
    /// A job in another namespace also reports `false`, so a scoped caller
    /// learns nothing about ids outside its own namespace.
    pub fn cancel_job(&self, id: &str, namespace: Option<&str>) -> Result<bool> {
        let mut conn = self.conn()?;
        let job = match self.get_job(id, namespace)? {
            Some(j) => j,
            None => return Ok(false),
        };

        if job.status != JobStatus::Pending {
            return Ok(false);
        }

        let mut job = job;
        let old_status = job.status;
        job.status = JobStatus::Cancelled;
        job.completed_at = Some(now_millis());

        // `archive_job_immediately` removes the job from the per-queue pending
        // zset as part of its atomic live→archive move (old_status == Pending).
        self.archive_job_immediately(&mut conn, &job, old_status)?;

        // Cascade cancel dependents
        drop(conn);
        self.cascade_cancel(id, "dependency cancelled", namespace)?;

        Ok(true)
    }

    /// Set the cancel-requested flag on a `Running` job — the task must poll
    /// for it. Returns `false` when no running job matched, which is also the
    /// answer for a job in another namespace.
    pub fn request_cancel(&self, id: &str, namespace: Option<&str>) -> Result<bool> {
        let mut conn = self.conn()?;
        let mut job = match self.get_job(id, namespace)? {
            Some(j) => j,
            None => return Ok(false),
        };

        if job.status != JobStatus::Running {
            return Ok(false);
        }

        job.cancel_requested = true;
        let job_json = serde_json::to_string(&job)?;
        let job_key = self.key(&["job", id]);
        let running_key = self.key(&["jobs", "status", &(JobStatus::Running as i32).to_string()]);
        let cancel_set = self.key(&["jobs", "cancel_requested"]);

        // Guarded write: only mark the job if it is still live-Running. If it was
        // archived between the read above and here, the script is a no-op (returns
        // 0) and we report "not cancellable" rather than resurrecting it.
        let applied: i32 = redis::Script::new(REQUEST_CANCEL_IF_RUNNING)
            .key(&job_key)
            .key(&running_key)
            .key(&cancel_set)
            .arg(id)
            .arg(&job_json)
            .invoke(&mut conn)
            .map_err(map_err)?;

        Ok(applied == 1)
    }

    /// Whether cancellation has been requested for a job. A job in another
    /// namespace reports `false`, like an unknown id.
    pub fn is_cancel_requested(&self, id: &str, namespace: Option<&str>) -> Result<bool> {
        if namespace.is_some() && self.get_job(id, namespace)?.is_none() {
            return Ok(false);
        }
        let mut conn = self.conn()?;
        let cancel_set = self.key(&["jobs", "cancel_requested"]);
        let is_member: bool = conn.sismember(&cancel_set, id).map_err(map_err)?;
        Ok(is_member)
    }

    /// Archive a live job as `Cancelled` after a cancel request was observed.
    /// A job in another namespace is left alone.
    pub fn mark_cancelled(&self, id: &str, namespace: Option<&str>) -> Result<()> {
        let mut conn = self.conn()?;
        let mut job = self.get_job_required(id)?;
        if namespace.is_some_and(|scope| job.namespace.as_deref() != Some(scope)) {
            return Ok(());
        }
        let old_status = job.status;

        job.status = JobStatus::Cancelled;
        job.completed_at = Some(now_millis());
        job.error = Some("cancelled by request".to_string());
        self.archive_job_immediately(&mut conn, &job, old_status)?;

        // Clean up cancel request
        let cancel_set = self.key(&["jobs", "cancel_requested"]);
        conn.srem::<_, _, ()>(&cancel_set, id).map_err(map_err)?;

        Ok(())
    }

    /// Cancel every pending job that depends, directly or transitively, on
    /// `failed_job_id`.
    ///
    /// Dependents outside `namespace` are left alone. Every edge written since
    /// the boundary was enforced is intra-namespace, so this only bites on older
    /// data — but a cancel must not archive another tenant's job because of an
    /// edge it should never have had.
    pub fn cascade_cancel(
        &self,
        failed_job_id: &str,
        reason: &str,
        namespace: Option<&str>,
    ) -> Result<()> {
        let now = now_millis();

        let mut queue: Vec<String> = vec![failed_job_id.to_string()];
        let mut visited = std::collections::HashSet::new();
        visited.insert(failed_job_id.to_string());
        let mut idx = 0;

        while idx < queue.len() {
            let current_id = queue[idx].clone();
            idx += 1;

            // Unscoped walk: the cancel itself is filtered per job below, and
            // the root is already archived by the time this runs, so gating
            // the traversal on the anchor's visibility would cut it short.
            let dependents = self.get_dependents(&current_id, None)?;
            for dep_id in dependents {
                if visited.insert(dep_id.clone()) {
                    queue.push(dep_id);
                }
            }
        }

        // Remove the original job
        if !queue.is_empty() {
            queue.remove(0);
        }

        let error_msg = format!("{reason}: {failed_job_id}");
        for dep_id in &queue {
            if let Some(mut job) = self.get_job(dep_id, namespace)? {
                if job.status == JobStatus::Pending {
                    let mut conn = self.conn()?;
                    let old_status = job.status;
                    job.status = JobStatus::Cancelled;
                    job.completed_at = Some(now);
                    job.error = Some(error_msg.clone());

                    // `archive_job_immediately` removes the job from the
                    // per-queue pending zset as part of its atomic move.
                    self.archive_job_immediately(&mut conn, &job, old_status)?;
                }
            }
        }

        Ok(())
    }

    /// Ids of the jobs `job_id` depends on.
    ///
    /// The edge sets carry no namespace of their own and a dependency may not
    /// cross namespaces, so the scope comes from the anchor job: one in
    /// another namespace reports no edges, like an unknown id.
    pub fn get_dependencies(&self, job_id: &str, namespace: Option<&str>) -> Result<Vec<String>> {
        if namespace.is_some() && self.get_job(job_id, namespace)?.is_none() {
            return Ok(Vec::new());
        }

        let mut conn = self.conn()?;
        let key = self.key(&["job", job_id, "depends_on"]);
        let ids: Vec<String> = conn.smembers(&key).map_err(map_err)?;
        Ok(ids)
    }

    /// Ids of the jobs that depend on `job_id`. Scoped by the anchor job, like
    /// [`get_dependencies`](Self::get_dependencies).
    pub fn get_dependents(&self, job_id: &str, namespace: Option<&str>) -> Result<Vec<String>> {
        if namespace.is_some() && self.get_job(job_id, namespace)?.is_none() {
            return Ok(Vec::new());
        }

        let mut conn = self.conn()?;
        let key = self.key(&["job", job_id, "dependents"]);
        let ids: Vec<String> = conn.smembers(&key).map_err(map_err)?;
        Ok(ids)
    }

    /// Update a live (not yet archived) job's progress (0-100).
    ///
    /// A job in another namespace reports `JobNotFound`, like an unknown id.
    pub fn update_progress(&self, id: &str, progress: i32, namespace: Option<&str>) -> Result<()> {
        if !(0..=100).contains(&progress) {
            return Err(QueueError::Other(
                "progress must be between 0 and 100".into(),
            ));
        }
        let mut conn = self.conn()?;
        let mut job = self.get_job_required(id)?;
        if namespace.is_some_and(|scope| job.namespace.as_deref() != Some(scope)) {
            return Err(QueueError::JobNotFound(id.to_string()));
        }
        job.progress = Some(progress);
        let job_json = serde_json::to_string(&job)?;
        let job_key = self.key(&["job", id]);

        // Guarded write: only update if `job:<id>` still exists. If the job was
        // archived (completed/failed/reaped) between the read and here, the plain
        // SET would otherwise recreate it as a Running orphan outside every index.
        let applied: i32 = redis::Script::new(SET_IF_LIVE)
            .key(&job_key)
            .arg(&job_json)
            .invoke(&mut conn)
            .map_err(map_err)?;
        if applied == 0 {
            // The job was archived concurrently — surface it like the read path
            // rather than reporting a silent success that wrote nothing.
            return Err(QueueError::JobNotFound(id.to_string()));
        }
        Ok(())
    }
}
