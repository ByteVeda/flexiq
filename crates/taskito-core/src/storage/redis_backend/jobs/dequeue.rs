//! Dequeue jobs from one or more queues.

use redis::Commands;

use crate::error::Result;
use crate::job::{Job, JobStatus};
use crate::storage::redis_backend::{map_err, RedisStorage};

/// Lua: claim a candidate by flipping it Pending→Running, but only if it is
/// still a member of the pending status set (`jobs:status:0`). This is the
/// Redis equivalent of the Diesel `UPDATE ... WHERE status = Pending`
/// affected-row guard: a concurrent cancel/expire that already archived the
/// job has removed it from `jobs:status:0`, so the claim is refused (returns 0)
/// instead of resurrecting the job as a Running orphan. Redis runs the script
/// atomically, which also closes the double-claim window between schedulers.
///
/// KEYS: job_key, pending_status_set, running_status_set, pending_zset,
///       [debounce_zset] (present only for a job carrying a debounce key)
/// ARGV: job_id, running_json
const CLAIM_JOB_SCRIPT: &str = r#"
    if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 0 then return 0 end
    redis.call('SET', KEYS[1], ARGV[2])
    redis.call('SREM', KEYS[2], ARGV[1])
    redis.call('SADD', KEYS[3], ARGV[1])
    redis.call('ZREM', KEYS[4], ARGV[1])
    if KEYS[5] then
        redis.call('ZREM', KEYS[5], ARGV[1])
    end
    return 1
"#;

impl RedisStorage {
    /// Atomically claim a candidate job (Pending→Running) via [`CLAIM_JOB_SCRIPT`].
    /// The caller must have already set `job.status = Running` and `started_at`.
    /// Returns `false` if the job was concurrently cancelled/expired/claimed, so
    /// the dequeue scan should skip it.
    fn claim_pending(
        &self,
        conn: &mut redis::Connection,
        job: &Job,
        queue_key: &str,
    ) -> Result<bool> {
        let job_json = serde_json::to_string(job)?;
        let job_key = self.key(&["job", &job.id]);
        let pending_status =
            self.key(&["jobs", "status", &(JobStatus::Pending as i32).to_string()]);
        let running_status =
            self.key(&["jobs", "status", &(JobStatus::Running as i32).to_string()]);
        let claim_script = redis::Script::new(CLAIM_JOB_SCRIPT);
        let mut invocation = claim_script.prepare_invoke();
        invocation
            .key(&job_key)
            .key(&pending_status)
            .key(&running_status)
            .key(queue_key);
        // A claimed job has left the debounce window it opened: a later
        // debounced enqueue must insert a fresh job rather than slide this one
        // out from under the worker now holding it. KEYS[5] is absent (and the
        // script's guard false) for a job without a debounce key.
        if let Some(debounce_key) = self.job_debounce_index_key(job) {
            invocation.key(debounce_key);
        }
        let claimed: i32 = invocation
            .arg(&job.id)
            .arg(&job_json)
            .invoke(conn)
            .map_err(map_err)?;
        Ok(claimed == 1)
    }

    /// Atomically claim the highest-priority ready job, moving it to `Running`.
    /// Scans the queue's sorted set by score; each candidate is claimed Lua-atomically.
    pub fn dequeue(
        &self,
        queue_name: &str,
        now: i64,
        namespace: Option<&str>,
    ) -> Result<Option<Job>> {
        let mut conn = self.conn()?;
        let queue_key = self.key(&["queue", queue_name, "pending"]);

        // Get candidates ordered by score (lowest first = highest priority)
        let candidates: Vec<String> = conn
            .zrangebyscore_limit(&queue_key, "-inf", "+inf", 0, 100)
            .map_err(map_err)?;

        if candidates.is_empty() {
            return Ok(None);
        }

        // Batch-load every candidate's JSON in one MGET instead of one GET per
        // candidate.
        let job_keys: Vec<String> = candidates.iter().map(|id| self.key(&["job", id])).collect();
        let blobs: Vec<Option<String>> = conn.mget(&job_keys).map_err(map_err)?;

        for (job_id, data) in candidates.into_iter().zip(blobs) {
            let data = match data {
                Some(d) => d,
                None => {
                    // Stale entry — remove from queue
                    conn.zrem::<_, _, ()>(&queue_key, &job_id)
                        .map_err(map_err)?;
                    continue;
                }
            };

            let mut job: Job = serde_json::from_str(&data)?;

            // Must be pending and scheduled_at <= now
            if job.status != JobStatus::Pending || job.scheduled_at > now {
                continue;
            }

            // Filter by namespace: Some(ns) matches that namespace, None matches only jobs without a namespace
            if let Some(ns) = namespace {
                if job.namespace.as_deref() != Some(ns) {
                    continue;
                }
            } else if job.namespace.is_some() {
                continue;
            }

            // Skip expired jobs
            if let Some(expires_at) = job.expires_at {
                if now > expires_at {
                    job.status = JobStatus::Cancelled;
                    job.completed_at = Some(now);
                    job.error = Some("expired before execution".to_string());
                    conn.zrem::<_, _, ()>(&queue_key, &job_id)
                        .map_err(map_err)?;
                    self.archive_job_immediately(&mut conn, &job, JobStatus::Pending)?;
                    continue;
                }
            }

            // Check dependencies — only for jobs that actually have them, and
            // resolve them on the existing connection rather than opening a new
            // one per dependency.
            if job.has_deps {
                let deps_key = self.key(&["job", &job_id, "depends_on"]);
                let dep_ids: Vec<String> = conn.smembers(&deps_key).map_err(map_err)?;
                if !dep_ids.is_empty() {
                    let mut all_complete = true;
                    for dep_id in &dep_ids {
                        // A completed dep has been archived out of the live
                        // indices, so fall back to the archive before deciding
                        // the dep is unsatisfied.
                        let dep_job = match self.load_job(&mut conn, dep_id)? {
                            Some(j) => Some(j),
                            None => self.load_archived_job(&mut conn, dep_id)?,
                        };
                        match dep_job {
                            Some(dep_job) if dep_job.status == JobStatus::Complete => {}
                            _ => {
                                all_complete = false;
                                break;
                            }
                        }
                    }
                    if !all_complete {
                        continue;
                    }
                }
            }

            // Claim the job atomically; if we lost the race (cancelled / expired
            // / claimed by another scheduler) skip to the next candidate.
            job.status = JobStatus::Running;
            job.started_at = Some(now);
            if self.claim_pending(&mut conn, &job, &queue_key)? {
                // Best-effort pub/sub backlog reindex Pending→Running; a
                // follow-up rather than folded into the correctness-critical
                // claim script (see `reindex_pubsub_best_effort`).
                self.reindex_pubsub_best_effort(&mut conn, &job, JobStatus::Running);
                return Ok(Some(job));
            }
        }

        Ok(None)
    }

    /// Dequeue across queues. `orders` is accepted for cross-backend signature
    /// parity but **ignored**: Redis packs priority and `scheduled_at` into a
    /// single ZSET score, so per-priority LIFO would need a second score-inverted
    /// sorted set (a backfill migration of every pending row). Documented as an
    /// exception, like the `list_jobs_after` Redis note; Redis stays FIFO.
    pub fn dequeue_from(
        &self,
        queues: &[String],
        now: i64,
        namespace: Option<&str>,
        _orders: &std::collections::HashMap<String, crate::storage::DispatchOrder>,
    ) -> Result<Option<Job>> {
        for queue_name in queues {
            if let Some(job) = self.dequeue(queue_name, now, namespace)? {
                return Ok(Some(job));
            }
        }
        Ok(None)
    }

    /// Claim up to `max` ready jobs from a single queue. Mirrors `dequeue`
    /// (ZRANGEBYSCORE candidates + MGET + claim loop) but accumulates up to
    /// `max` jobs. Shares `dequeue`'s TOCTOU window — `claim_execution` is the
    /// external exactly-once guard.
    pub fn dequeue_batch(
        &self,
        queue_name: &str,
        now: i64,
        namespace: Option<&str>,
        max: usize,
    ) -> Result<Vec<Job>> {
        if max == 0 {
            return Ok(Vec::new());
        }

        let mut conn = self.conn()?;
        let queue_key = self.key(&["queue", queue_name, "pending"]);

        // Scan more candidates than `max` so dependency/expiry skips still
        // leave enough eligible rows to fill the batch, bounded to keep the
        // loaded set small.
        let scan_limit = (max.saturating_mul(4)).min(400) as isize;

        // Get candidates ordered by score (lowest first = highest priority)
        let candidates: Vec<String> = conn
            .zrangebyscore_limit(&queue_key, "-inf", "+inf", 0, scan_limit)
            .map_err(map_err)?;

        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        // Batch-load every candidate's JSON in one MGET instead of one GET per
        // candidate.
        let job_keys: Vec<String> = candidates.iter().map(|id| self.key(&["job", id])).collect();
        let blobs: Vec<Option<String>> = conn.mget(&job_keys).map_err(map_err)?;

        let mut claimed: Vec<Job> = Vec::with_capacity(max.min(candidates.len()));

        for (job_id, data) in candidates.into_iter().zip(blobs) {
            if claimed.len() == max {
                break;
            }

            let data = match data {
                Some(d) => d,
                None => {
                    // Stale entry — remove from queue
                    conn.zrem::<_, _, ()>(&queue_key, &job_id)
                        .map_err(map_err)?;
                    continue;
                }
            };

            let mut job: Job = serde_json::from_str(&data)?;

            // Must be pending and scheduled_at <= now
            if job.status != JobStatus::Pending || job.scheduled_at > now {
                continue;
            }

            // Filter by namespace: Some(ns) matches that namespace, None matches only jobs without a namespace
            if let Some(ns) = namespace {
                if job.namespace.as_deref() != Some(ns) {
                    continue;
                }
            } else if job.namespace.is_some() {
                continue;
            }

            // Skip expired jobs
            if let Some(expires_at) = job.expires_at {
                if now > expires_at {
                    job.status = JobStatus::Cancelled;
                    job.completed_at = Some(now);
                    job.error = Some("expired before execution".to_string());
                    self.save_job_and_move_status(&mut conn, &job, JobStatus::Pending)?;
                    conn.zrem::<_, _, ()>(&queue_key, &job_id)
                        .map_err(map_err)?;
                    continue;
                }
            }

            // Check dependencies — only for jobs that actually have them.
            if job.has_deps {
                let deps_key = self.key(&["job", &job_id, "depends_on"]);
                let dep_ids: Vec<String> = conn.smembers(&deps_key).map_err(map_err)?;
                if !dep_ids.is_empty() {
                    let mut all_complete = true;
                    for dep_id in &dep_ids {
                        // A completed dep has been archived out of the live
                        // indices, so fall back to the archive before deciding
                        // the dep is unsatisfied.
                        let dep_job = match self.load_job(&mut conn, dep_id)? {
                            Some(j) => Some(j),
                            None => self.load_archived_job(&mut conn, dep_id)?,
                        };
                        match dep_job {
                            Some(dep_job) if dep_job.status == JobStatus::Complete => {}
                            _ => {
                                all_complete = false;
                                break;
                            }
                        }
                    }
                    if !all_complete {
                        continue;
                    }
                }
            }

            // Claim the job atomically; skip candidates lost to a concurrent
            // cancel / expire / claim instead of resurrecting them.
            job.status = JobStatus::Running;
            job.started_at = Some(now);
            if self.claim_pending(&mut conn, &job, &queue_key)? {
                // Best-effort pub/sub backlog reindex Pending→Running (see the
                // single-claim `dequeue` for the rationale).
                self.reindex_pubsub_best_effort(&mut conn, &job, JobStatus::Running);
                claimed.push(job);
            }
        }

        Ok(claimed)
    }

    /// Claim up to `max` ready jobs across the given queues, checking each in
    /// order until the budget is exhausted. `orders` is accepted but ignored —
    /// see [`dequeue_from`](Self::dequeue_from) for why Redis stays FIFO.
    pub fn dequeue_batch_from(
        &self,
        queues: &[String],
        now: i64,
        namespace: Option<&str>,
        max: usize,
        _orders: &std::collections::HashMap<String, crate::storage::DispatchOrder>,
    ) -> Result<Vec<Job>> {
        let mut claimed: Vec<Job> = Vec::new();
        for queue_name in queues {
            if claimed.len() >= max {
                break;
            }
            let remaining = max - claimed.len();
            let mut batch = self.dequeue_batch(queue_name, now, namespace, remaining)?;
            claimed.append(&mut batch);
        }
        Ok(claimed)
    }
}
