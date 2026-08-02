//! Periodic maintenance: purge completed jobs, reap stale running jobs,
//! expire pending jobs, cancel by queue/task name.

use std::collections::HashSet;

use redis::Commands;

use crate::error::Result;
use crate::job::{now_millis, Job, JobStatus};
use crate::storage::redis_backend::{map_err, RedisStorage, SCAN_BATCH};
use crate::storage::{RetentionCounts, RetentionCutoffs};

impl RedisStorage {
    /// Key of the completed-archive index SET (`archived:status:2`).
    fn completed_archive_key(&self) -> String {
        self.key(&[
            "archived",
            "status",
            &(JobStatus::Complete as i32).to_string(),
        ])
    }

    /// Purge archived completed jobs older than the cutoff. Returns the count
    /// removed.
    pub fn purge_completed(&self, older_than_ms: i64) -> Result<u64> {
        self.purge_completed_scan(|job| {
            job.completed_at
                .is_some_and(|completed| completed < older_than_ms)
        })
    }

    /// Retention purge of archived jobs. Bounded, unlike the manual
    /// `purge_completed`: two ZRANGEBYSCORE drains touch only the rows that can
    /// expire, not the whole archive. Covers **every terminal status** — the
    /// archive grows with failures too — and leaves `job_errors` to its own
    /// window (`cascade_diagnostics = false`).
    pub fn purge_completed_with_ttl(&self, global_cutoff_ms: Option<i64>) -> Result<u64> {
        let now = now_millis();
        // `archived:expiry` only holds rows archived since this feature shipped.
        // Index any pre-existing per-entry-TTL rows a batch at a time, so a
        // no-global-cutoff purge can find them (the global drain below re-checks
        // per-entry TTLs, but does not run when there is no cutoff).
        self.backfill_archived_expiry()?;
        // Per-entry TTLs first: the expiry index is scored by expiry time, so a
        // score drain finds the expired rows without scanning the archive. This
        // also catches short-TTL rows too recent for the global window below.
        let mut count = self.drain_archived_expiry(now)?;
        // Then the global window: rows older than the cutoff with no per-entry
        // TTL (a per-entry row is governed by its own TTL, handled above).
        if let Some(cutoff) = global_cutoff_ms {
            count += self.drain_archived_below(cutoff, now)?;
        }
        Ok(count)
    }

    /// Read-only dry-run: count the rows each retention purge would delete under
    /// `cutoffs`, without deleting or indexing anything. Delegates to the
    /// per-table counters, each mirroring its purge's predicate.
    pub fn count_expired_rows(
        &self,
        cutoffs: &RetentionCutoffs,
        now: i64,
    ) -> Result<RetentionCounts> {
        Ok(RetentionCounts {
            archived_jobs: self.count_expired_archived(cutoffs.archived_jobs, now)?,
            dead_letter: self.count_expired_dead(cutoffs.dead_letter, now)?,
            task_logs: match cutoffs.task_logs {
                Some(cutoff) => self.count_expired_task_logs(cutoff)?,
                None => 0,
            },
            task_metrics: match cutoffs.task_metrics {
                Some(cutoff) => self.count_expired_metrics(cutoff)?,
                None => 0,
            },
            job_errors: match cutoffs.job_errors {
                Some(cutoff) => self.count_expired_job_errors(cutoff)?,
                None => 0,
            },
        })
    }

    /// Count archived jobs a purge would remove under `global_cutoff_ms`,
    /// mirroring [`Self::purge_completed_with_ttl`] without deleting.
    ///
    /// Read-only, so it does **not** backfill the expiry index: a pre-upgrade
    /// per-entry row is counted only once it falls below the global window (the
    /// same row the purge indexes lazily, one batch per tick). Post-upgrade
    /// per-entry rows are collected from `archived:expiry` and excluded from the
    /// global walk so the two drains are never double-counted.
    pub fn count_expired_archived(&self, global_cutoff_ms: Option<i64>, now: i64) -> Result<u64> {
        let mut conn = self.conn()?;
        let expiry_key = self.key(&["archived", "expiry"]);
        let all_key = self.key(&["archived", "all"]);

        // Per-entry-expired rows (indexed): every id below `now` is deletable.
        // Page the index so peak memory stays bounded; the set of per-entry
        // archived rows is small in practice.
        let mut indexed: HashSet<String> = HashSet::new();
        let mut offset = 0i64;
        loop {
            let ids: Vec<String> = redis::cmd("ZRANGEBYSCORE")
                .arg(&expiry_key)
                .arg("-inf")
                .arg(format!("({now}"))
                .arg("LIMIT")
                .arg(offset)
                .arg(SCAN_BATCH)
                .query(&mut conn)
                .map_err(map_err)?;
            if ids.is_empty() {
                break;
            }
            let drained = ids.len();
            indexed.extend(ids);
            offset += drained as i64;
            if (drained as isize) < SCAN_BATCH {
                break;
            }
        }
        let mut count = indexed.len() as u64;

        // Global window: rows below the cutoff the expiry index did not already
        // account for — no per-entry TTL, or a per-entry TTL that has itself
        // expired (a pre-upgrade row absent from the index).
        if let Some(cutoff) = global_cutoff_ms {
            let mut offset = 0i64;
            loop {
                let ids: Vec<String> = redis::cmd("ZRANGEBYSCORE")
                    .arg(&all_key)
                    .arg("-inf")
                    .arg(format!("({cutoff}"))
                    .arg("LIMIT")
                    .arg(offset)
                    .arg(SCAN_BATCH)
                    .query(&mut conn)
                    .map_err(map_err)?;
                if ids.is_empty() {
                    break;
                }
                let drained = ids.len();
                for id in &ids {
                    if indexed.contains(id) {
                        continue;
                    }
                    let Some(job) = self.load_archived_job(&mut conn, id)? else {
                        continue;
                    };
                    let deletable = match job.result_ttl_ms {
                        None => true,
                        Some(ttl) => job
                            .completed_at
                            .and_then(|c| c.checked_add(ttl))
                            .is_some_and(|expiry| expiry < now),
                    };
                    if deletable {
                        count += 1;
                    }
                }
                offset += drained as i64;
                if (drained as isize) < SCAN_BATCH {
                    break;
                }
            }
        }

        Ok(count)
    }

    /// Index one batch of pre-existing archived rows that carry a per-entry TTL
    /// but predate the `archived:expiry` index. Resumable via a stored cursor
    /// and bounded to one `ZSCAN` batch per call, so the one-time migration
    /// never blocks a maintenance tick. Sets a done marker when the scan wraps.
    fn backfill_archived_expiry(&self) -> Result<()> {
        let mut conn = self.conn()?;
        let done_key = self.key(&["archived", "expiry", "backfilled"]);
        let done: Option<String> = conn.get(&done_key).map_err(map_err)?;
        if done.is_some() {
            return Ok(());
        }

        let all_key = self.key(&["archived", "all"]);
        let expiry_key = self.key(&["archived", "expiry"]);
        let cursor_key = self.key(&["archived", "expiry", "cursor"]);
        let cursor: u64 = conn
            .get::<_, Option<u64>>(&cursor_key)
            .map_err(map_err)?
            .unwrap_or(0);

        let (next, flat): (u64, Vec<String>) = redis::cmd("ZSCAN")
            .arg(&all_key)
            .arg(cursor)
            .arg("COUNT")
            .arg(SCAN_BATCH)
            .query(&mut conn)
            .map_err(map_err)?;

        // ZSCAN returns a flat [member, score, member, score, ...] list.
        for id in flat.iter().step_by(2) {
            if let Some(job) = self.load_archived_job(&mut conn, id)? {
                if let Some(ttl) = job.result_ttl_ms {
                    if let Some(expiry) = job.completed_at.and_then(|c| c.checked_add(ttl)) {
                        conn.zadd::<_, _, _, ()>(&expiry_key, id, expiry as f64)
                            .map_err(map_err)?;
                    }
                }
            }
        }

        if next == 0 {
            conn.set::<_, _, ()>(&done_key, "1").map_err(map_err)?;
            conn.del::<_, ()>(&cursor_key).map_err(map_err)?;
        } else {
            conn.set::<_, _, ()>(&cursor_key, next).map_err(map_err)?;
        }
        Ok(())
    }

    /// Delete archived jobs whose per-entry TTL has expired, draining the
    /// `archived:expiry` index by score in bounded batches. Every returned id is
    /// past its expiry, so each is unconditionally deletable.
    fn drain_archived_expiry(&self, now: i64) -> Result<u64> {
        let mut conn = self.conn()?;
        let expiry_key = self.key(&["archived", "expiry"]);
        let mut count = 0u64;

        loop {
            let ids: Vec<String> = redis::cmd("ZRANGEBYSCORE")
                .arg(&expiry_key)
                .arg("-inf")
                .arg(format!("({now}"))
                .arg("LIMIT")
                .arg(0)
                .arg(SCAN_BATCH)
                .query(&mut conn)
                .map_err(map_err)?;
            if ids.is_empty() {
                break;
            }
            let drained = ids.len();
            for id in &ids {
                if let Some(job) = self.load_archived_job(&mut conn, id)? {
                    self.delete_archived_job(&mut conn, &job, false)?;
                    count += 1;
                } else {
                    // The row is gone but a stale index entry remains; prune it so
                    // the drain can never spin on it.
                    conn.zrem::<_, _, ()>(&expiry_key, id).map_err(map_err)?;
                }
            }
            if drained < SCAN_BATCH as usize {
                break;
            }
        }
        Ok(count)
    }

    /// Delete archived jobs older than `cutoff` that have no per-entry TTL (or a
    /// per-entry TTL already expired — a pre-upgrade row absent from the expiry
    /// index). Drains `archived:all` by score; a row not yet eligible is skipped
    /// past with an advancing offset so the drain never re-reads it.
    fn drain_archived_below(&self, cutoff: i64, now: i64) -> Result<u64> {
        let mut conn = self.conn()?;
        let all_key = self.key(&["archived", "all"]);
        let mut count = 0u64;
        let mut offset = 0i64;

        loop {
            let ids: Vec<String> = redis::cmd("ZRANGEBYSCORE")
                .arg(&all_key)
                .arg("-inf")
                .arg(format!("({cutoff}"))
                .arg("LIMIT")
                .arg(offset)
                .arg(SCAN_BATCH)
                .query(&mut conn)
                .map_err(map_err)?;
            if ids.is_empty() {
                break;
            }
            let drained = ids.len();
            for id in &ids {
                let Some(job) = self.load_archived_job(&mut conn, id)? else {
                    // Stale index entry — prune it and advance so it can't recur.
                    conn.zrem::<_, _, ()>(&all_key, id).map_err(map_err)?;
                    continue;
                };
                let expired = match job.result_ttl_ms {
                    // No per-entry TTL: the global cutoff governs, and the score
                    // range already guarantees `completed_at < cutoff`.
                    None => true,
                    // Per-entry TTL: purge only if its own window has passed; a
                    // not-yet-expired row is skipped for the expiry drain to own.
                    Some(ttl) => job
                        .completed_at
                        .and_then(|c| c.checked_add(ttl))
                        .is_some_and(|expiry| expiry < now),
                };
                if expired {
                    self.delete_archived_job(&mut conn, &job, false)?;
                    count += 1;
                } else {
                    offset += 1;
                }
            }
            if drained < SCAN_BATCH as usize {
                break;
            }
        }
        Ok(count)
    }

    /// Walk the completed-archive index in bounded SSCAN batches, deleting
    /// every job the `should_purge` predicate accepts. SSCAN bounds memory to
    /// one batch of ids (plus one loaded job at a time) rather than loading the
    /// whole completed set, and tolerates the concurrent deletes so nothing
    /// still-eligible is skipped.
    fn purge_completed_scan<F>(&self, should_purge: F) -> Result<u64>
    where
        F: Fn(&Job) -> bool,
    {
        let mut conn = self.conn()?;
        let status_key = self.completed_archive_key();
        let mut cursor: u64 = 0;
        let mut count = 0u64;

        loop {
            let (next, ids): (u64, Vec<String>) = redis::cmd("SSCAN")
                .arg(&status_key)
                .arg(cursor)
                .arg("COUNT")
                .arg(SCAN_BATCH)
                .query(&mut conn)
                .map_err(map_err)?;
            cursor = next;

            for id in &ids {
                if let Some(job) = self.load_archived_job(&mut conn, id)? {
                    if should_purge(&job) {
                        // The manual purge is a blunt "delete these jobs and all
                        // their data", so cascade the diagnostics too.
                        self.delete_archived_job(&mut conn, &job, true)?;
                        count += 1;
                    }
                }
            }

            if cursor == 0 {
                break;
            }
        }

        Ok(count)
    }

    /// Running jobs that exceeded their timeout, for the scheduler to fail or
    /// retry. Scoped so a scheduler never times out another namespace's job.
    pub fn reap_stale_jobs(&self, now: i64, namespace: Option<&str>) -> Result<Vec<Job>> {
        let mut conn = self.conn()?;
        let status_key = self.key(&["jobs", "status", &(JobStatus::Running as i32).to_string()]);
        let job_ids: Vec<String> = conn.smembers(&status_key).map_err(map_err)?;

        let mut stale = Vec::new();
        for id in &job_ids {
            if let Some(job) = self.load_job(&mut conn, id)? {
                if namespace.is_some_and(|scope| job.namespace.as_deref() != Some(scope)) {
                    continue;
                }
                if let Some(started) = job.started_at {
                    let timed_out = match started.checked_add(job.timeout_ms) {
                        Some(deadline) => deadline < now,
                        None => true,
                    };
                    if timed_out {
                        stale.push(job);
                    }
                }
            }
        }

        Ok(stale)
    }

    /// Running jobs whose execution-claim owner is not in `live_owner_ids` (its
    /// worker died). Read-only — paired with the dead owner so the scheduler can
    /// atomically reclaim before requeuing. Iterates the bounded Running set.
    pub fn reap_orphaned_jobs(
        &self,
        live_owner_ids: &[String],
        _now: i64,
        namespace: Option<&str>,
    ) -> Result<Vec<(Job, String)>> {
        if live_owner_ids.is_empty() {
            return Ok(Vec::new());
        }
        let live: std::collections::HashSet<&str> =
            live_owner_ids.iter().map(String::as_str).collect();

        let mut conn = self.conn()?;
        let status_key = self.key(&["jobs", "status", &(JobStatus::Running as i32).to_string()]);
        let job_ids: Vec<String> = conn.smembers(&status_key).map_err(map_err)?;

        let mut orphaned = Vec::new();
        for id in &job_ids {
            let claim_key = self.key(&["exec_claim", id]);
            let claim: Option<String> = conn.get(&claim_key).map_err(map_err)?;
            // Claim value is "{owner}:{ts}". The timestamp is a numeric suffix, so
            // split on the LAST ':' — the owner itself may contain ':' (e.g.
            // "host:pid"). No claim → time-based reap handles it.
            let owner = match claim.as_deref() {
                Some(v) => v.rsplit_once(':').map(|(o, _)| o).unwrap_or(v).to_string(),
                None => continue,
            };
            if live.contains(owner.as_str()) {
                continue;
            }
            if let Some(job) = self.load_job(&mut conn, id)? {
                if namespace.is_some_and(|scope| job.namespace.as_deref() != Some(scope)) {
                    continue;
                }
                if job.status == JobStatus::Running {
                    orphaned.push((job, owner));
                }
            }
        }

        Ok(orphaned)
    }

    /// Cancel pending jobs whose `expires_at` has passed, archiving them as
    /// `Cancelled`. Returns the count expired.
    pub fn expire_pending_jobs(&self, now: i64) -> Result<u64> {
        let mut conn = self.conn()?;
        let status_key = self.key(&["jobs", "status", &(JobStatus::Pending as i32).to_string()]);
        let job_ids: Vec<String> = conn.smembers(&status_key).map_err(map_err)?;

        let mut count = 0u64;
        for id in &job_ids {
            if let Some(mut job) = self.load_job(&mut conn, id)? {
                if let Some(expires_at) = job.expires_at {
                    if expires_at < now {
                        let old_status = job.status;
                        job.status = JobStatus::Cancelled;
                        job.completed_at = Some(now);
                        job.error = Some("expired".to_string());

                        let queue_key = self.key(&["queue", &job.queue, "pending"]);
                        conn.zrem::<_, _, ()>(&queue_key, &job.id)
                            .map_err(map_err)?;
                        self.archive_job_immediately(&mut conn, &job, old_status)?;
                        count += 1;
                    }
                }
            }
        }

        Ok(count)
    }

    /// Cancel every pending job in a queue. Returns the count cancelled.
    pub fn cancel_pending_by_queue(&self, queue: &str) -> Result<u64> {
        let mut conn = self.conn()?;
        let by_queue_key = self.key(&["jobs", "by_queue", queue]);
        let job_ids: Vec<String> = conn.smembers(&by_queue_key).map_err(map_err)?;
        let now = now_millis();

        let mut count = 0u64;
        for id in &job_ids {
            if let Some(mut job) = self.load_job(&mut conn, id)? {
                if job.status == JobStatus::Pending {
                    let old_status = job.status;
                    job.status = JobStatus::Cancelled;
                    job.completed_at = Some(now);
                    job.error = Some("purged".to_string());

                    let queue_key = self.key(&["queue", &job.queue, "pending"]);
                    conn.zrem::<_, _, ()>(&queue_key, &job.id)
                        .map_err(map_err)?;
                    self.archive_job_immediately(&mut conn, &job, old_status)?;
                    count += 1;
                }
            }
        }

        Ok(count)
    }

    /// Cancel every pending job for a task. Returns the count cancelled.
    pub fn cancel_pending_by_task(&self, task_name: &str) -> Result<u64> {
        let mut conn = self.conn()?;
        let by_task_key = self.key(&["jobs", "by_task", task_name]);
        let job_ids: Vec<String> = conn.smembers(&by_task_key).map_err(map_err)?;
        let now = now_millis();

        let mut count = 0u64;
        for id in &job_ids {
            if let Some(mut job) = self.load_job(&mut conn, id)? {
                if job.status == JobStatus::Pending {
                    let old_status = job.status;
                    job.status = JobStatus::Cancelled;
                    job.completed_at = Some(now);
                    job.error = Some("revoked".to_string());

                    let queue_key = self.key(&["queue", &job.queue, "pending"]);
                    conn.zrem::<_, _, ()>(&queue_key, &job.id)
                        .map_err(map_err)?;
                    self.archive_job_immediately(&mut conn, &job, old_status)?;
                    count += 1;
                }
            }
        }

        Ok(count)
    }
}
