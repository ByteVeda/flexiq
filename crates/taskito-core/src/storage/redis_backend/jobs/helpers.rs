//! Internal helpers shared by other job submodules.

use redis::Commands;

use super::dequeue_score;
use crate::error::{QueueError, Result};
use crate::job::{Job, JobStatus};
use crate::storage::redis_backend::{map_err, RedisStorage};

/// Lua: release a unique-key pointer only if it still points at `ARGV[1]`.
/// A newer job may have reused the same `unique_key` after this job left the
/// live indices, so an unconditional `DEL` would clobber the new job's dedup
/// lock. Mirrors `RELEASE_LOCK_SCRIPT` in `locks.rs`.
const RELEASE_UNIQUE_IF_OWNER: &str = r#"
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        redis.call('DEL', KEYS[1])
        return 1
    end
    return 0
"#;

impl RedisStorage {
    pub(in crate::storage::redis_backend) fn load_job(
        &self,
        conn: &mut redis::Connection,
        id: &str,
    ) -> Result<Option<Job>> {
        let job_key = self.key(&["job", id]);
        let data: Option<String> = conn.get(&job_key).map_err(map_err)?;
        match data {
            Some(d) => {
                let job: Job = serde_json::from_str(&d)?;
                Ok(Some(job))
            }
            None => Ok(None),
        }
    }

    pub(in crate::storage::redis_backend) fn load_archived_job(
        &self,
        conn: &mut redis::Connection,
        id: &str,
    ) -> Result<Option<Job>> {
        let archived_key = self.key(&["archived", id]);
        let data: Option<String> = conn.get(&archived_key).map_err(map_err)?;
        match data {
            Some(d) => {
                let job: Job = serde_json::from_str(&d)?;
                Ok(Some(job))
            }
            None => Ok(None),
        }
    }

    /// Live-only required lookup for write/mutator paths. Resolves `job:<id>`
    /// directly without the archived fallback, so a mutator never operates on a
    /// terminal job that has already left the live indices (which would leave
    /// the reindex partial). Read paths must use `get_job` instead.
    pub(super) fn get_job_required(&self, id: &str) -> Result<Job> {
        let mut conn = self.conn()?;
        self.load_job(&mut conn, id)?
            .ok_or_else(|| QueueError::JobNotFound(id.to_string()))
    }

    /// [`get_job_required`](Self::get_job_required) confined to `namespace`.
    ///
    /// A job outside it reports `JobNotFound`, like an unknown id. The check
    /// rides on the load the mutation already does, so it costs no extra round
    /// trip. `None` addresses every namespace.
    pub(super) fn get_job_required_in(&self, id: &str, namespace: Option<&str>) -> Result<Job> {
        let job = self.get_job_required(id)?;
        if namespace.is_some() && job.namespace.as_deref() != namespace {
            return Err(QueueError::JobNotFound(id.to_string()));
        }
        Ok(job)
    }

    /// Save job JSON and move between status sets.
    pub(in crate::storage::redis_backend) fn save_job_and_move_status(
        &self,
        conn: &mut redis::Connection,
        job: &Job,
        old_status: JobStatus,
    ) -> Result<()> {
        let job_json = serde_json::to_string(job)?;
        let job_key = self.key(&["job", &job.id]);
        let old_status_key = self.key(&["jobs", "status", &(old_status as i32).to_string()]);
        let new_status_key = self.key(&["jobs", "status", &(job.status as i32).to_string()]);

        let pipe = &mut redis::pipe();
        pipe.set(&job_key, &job_json);
        if old_status != job.status {
            pipe.srem(&old_status_key, &job.id);
            pipe.sadd(&new_status_key, &job.id);
        }
        // Mirror the status move on the pub/sub backlog indices, keyed on the
        // job's new status (no-op for ordinary jobs). Same pipe as the move.
        self.push_pubsub_transition(pipe, job, job.status);
        pipe.query::<()>(conn).map_err(map_err)?;

        Ok(())
    }

    /// Reset a job to `Pending` and (re)insert it into the per-queue pending
    /// zset in a single MULTI/EXEC. Folding the status-set move and the zset
    /// add into one transaction means a crash can no longer strand the job as
    /// `Pending` in the status set but absent from the queue zset (where it
    /// would never be dequeued and never reaped).
    ///
    /// The caller must have already set the job's `status = Pending`,
    /// `scheduled_at`, and cleared `started_at`/`completed_at`/`error`.
    pub(in crate::storage::redis_backend) fn requeue_pending(
        &self,
        conn: &mut redis::Connection,
        job: &Job,
        old_status: JobStatus,
    ) -> Result<()> {
        let job_json = serde_json::to_string(job)?;
        let job_key = self.key(&["job", &job.id]);
        let old_status_key = self.key(&["jobs", "status", &(old_status as i32).to_string()]);
        let pending_status_key =
            self.key(&["jobs", "status", &(JobStatus::Pending as i32).to_string()]);
        let queue_key = self.key(&["queue", &job.queue, "pending"]);
        let score = dequeue_score(job.priority, job.scheduled_at);

        let pipe = &mut redis::pipe();
        pipe.atomic();
        pipe.set(&job_key, &job_json);
        if old_status != JobStatus::Pending {
            pipe.srem(&old_status_key, &job.id);
            pipe.sadd(&pending_status_key, &job.id);
        }
        pipe.zadd(&queue_key, &job.id, score);
        // Back to Pending: leave the running index and (re)enter the pending
        // backlog index. No-op for ordinary jobs; same atomic pipe.
        self.push_pubsub_transition(pipe, job, JobStatus::Pending);
        pipe.query::<()>(conn).map_err(map_err)?;

        Ok(())
    }

    /// Release a job's unique-key pointer iff it still belongs to this job, via
    /// an atomic compare-and-delete (mirrors `RELEASE_LOCK_SCRIPT`). Safe to
    /// call when a newer job may have reused the same `unique_key`.
    pub(in crate::storage::redis_backend) fn release_unique_key(
        &self,
        conn: &mut redis::Connection,
        unique_key: &str,
        job_id: &str,
    ) -> Result<()> {
        let key = self.key(&["jobs", "unique", unique_key]);
        redis::Script::new(RELEASE_UNIQUE_IF_OWNER)
            .key(&key)
            .arg(job_id)
            .invoke::<i32>(conn)
            .map_err(map_err)?;
        Ok(())
    }

    /// Append the live→archive command sequence to `pipe` (no MULTI/EXEC of its
    /// own). Removes the job from the live indices and writes the archived row.
    /// Shared by `archive_job_immediately` and `move_to_dlq` so the DLQ write and
    /// the archive can commit in one transaction. The caller must have already
    /// set the terminal `status`/`completed_at` and serialized `job_json`.
    pub(in crate::storage::redis_backend) fn push_archive_ops(
        &self,
        pipe: &mut redis::Pipeline,
        job: &Job,
        old_status: JobStatus,
        job_json: &str,
    ) {
        let completed_at = job.completed_at.unwrap_or_else(crate::job::now_millis);

        let job_key = self.key(&["job", &job.id]);
        let status_key = self.key(&["jobs", "status", &(old_status as i32).to_string()]);
        let by_queue_key = self.key(&["jobs", "by_queue", &job.queue]);
        let by_task_key = self.key(&["jobs", "by_task", &job.task_name]);
        let all_key = self.key(&["jobs", "all"]);
        let pending_key = self.key(&["queue", &job.queue, "pending"]);
        let archived_key = self.key(&["archived", &job.id]);
        let archived_status_key =
            self.key(&["archived", "status", &(job.status as i32).to_string()]);
        let archived_by_queue = self.key(&["archived", "by_queue", &job.queue]);
        let archived_all = self.key(&["archived", "all"]);
        let archived_expiry = self.key(&["archived", "expiry"]);

        // Results are ignored so callers can query the pipe as `()` (or
        // `Option<()>` inside a WATCH transaction).
        pipe.del(&job_key).ignore();
        pipe.srem(&status_key, &job.id).ignore();
        pipe.srem(&by_queue_key, &job.id).ignore();
        pipe.srem(&by_task_key, &job.id).ignore();
        pipe.zrem(&all_key, &job.id).ignore();
        // Pending jobs still sit in the per-queue pending zset; running jobs
        // were removed at dequeue, so only remove on a Pending→terminal move.
        if old_status == JobStatus::Pending {
            pipe.zrem(&pending_key, &job.id).ignore();
        }
        pipe.set(&archived_key, job_json).ignore();
        pipe.sadd(&archived_status_key, &job.id).ignore();
        pipe.sadd(&archived_by_queue, &job.id).ignore();
        pipe.zadd(&archived_all, &job.id, completed_at as f64)
            .ignore();

        // A per-entry TTL gets its own expiry index, scored by when it expires
        // (`completed_at + result_ttl_ms`). The retention purge drains this by
        // score, so a per-entry row is found without scanning the whole archive.
        if let Some(ttl) = job.result_ttl_ms {
            if let Some(expiry) = completed_at.checked_add(ttl) {
                pipe.zadd(&archived_expiry, &job.id, expiry as f64).ignore();
            }
        }

        // Mirror the terminal move on the pub/sub backlog indices (no-op for
        // ordinary jobs). `job.status` is the terminal status the caller set:
        // Complete/Failed/Cancelled leave the backlog; Dead (from `move_to_dlq`)
        // also enters the sub:dead index. Same atomic pipe as the archive move,
        // so the backlog index can never be left desynced from the archive.
        self.push_pubsub_transition(pipe, job, job.status);
    }

    /// Move a terminal job out of the live indices into the archive in one
    /// atomic pipeline (`.atomic()` MULTI/EXEC), so it is never observable in
    /// both the live and archived indices at once.
    pub(in crate::storage::redis_backend) fn archive_job_immediately(
        &self,
        conn: &mut redis::Connection,
        job: &Job,
        old_status: JobStatus,
    ) -> Result<()> {
        let job_json = serde_json::to_string(job)?;
        let pipe = &mut redis::pipe();
        pipe.atomic();
        self.push_archive_ops(pipe, job, old_status, &job_json);
        pipe.query::<()>(conn).map_err(map_err)?;

        Ok(())
    }

    /// Delete an archived job and its archive-index entries plus its dependency
    /// relations. `cascade_diagnostics` also deletes the job's errors — a full
    /// purge sets it, but the per-table retention purge leaves `job_errors` to
    /// its own window (see the Diesel `purge_archived_id_batch` split).
    pub(in crate::storage::redis_backend) fn delete_archived_job(
        &self,
        conn: &mut redis::Connection,
        job: &Job,
        cascade_diagnostics: bool,
    ) -> Result<()> {
        let pipe = &mut redis::pipe();

        let archived_key = self.key(&["archived", &job.id]);
        let archived_status_key =
            self.key(&["archived", "status", &(job.status as i32).to_string()]);
        let archived_by_queue = self.key(&["archived", "by_queue", &job.queue]);
        let archived_all = self.key(&["archived", "all"]);
        let archived_expiry = self.key(&["archived", "expiry"]);
        let deps_key = self.key(&["job", &job.id, "depends_on"]);
        let dependents_key = self.key(&["job", &job.id, "dependents"]);

        pipe.del(&archived_key);
        pipe.srem(&archived_status_key, &job.id);
        pipe.srem(&archived_by_queue, &job.id);
        pipe.zrem(&archived_all, &job.id);
        pipe.zrem(&archived_expiry, &job.id);
        pipe.del(&deps_key);
        pipe.del(&dependents_key);
        if cascade_diagnostics {
            pipe.del(self.key(&["job_errors", &job.id]));
        }
        pipe.query::<()>(conn).map_err(map_err)?;

        // Release the unique-key pointer through the atomic compare-and-delete so
        // a `enqueue_unique` that reused the key between a plain GET and DEL can't
        // have its lock clobbered.
        if let Some(ref uk) = job.unique_key {
            self.release_unique_key(conn, uk, &job.id)?;
        }

        Ok(())
    }
}
