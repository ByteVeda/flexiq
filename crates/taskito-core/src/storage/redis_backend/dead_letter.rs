use redis::Commands;
use serde::{Deserialize, Serialize};

use super::{map_err, strip_dead_blob, RedisStorage, SCAN_BATCH};
use crate::error::{QueueError, Result};
use crate::job::{now_millis, Job, JobStatus, NewJob};
use crate::storage::DeadJob;

#[derive(Serialize, Deserialize)]
struct DeadJobEntry {
    pub id: String,
    pub original_job_id: String,
    pub queue: String,
    pub task_name: String,
    pub payload: Vec<u8>,
    pub error: Option<String>,
    pub retry_count: i32,
    pub failed_at: i64,
    pub metadata: Option<String>,
    #[serde(default)]
    pub notes: Option<String>,
    pub priority: i32,
    pub max_retries: i32,
    pub timeout_ms: i64,
    pub result_ttl_ms: Option<i64>,
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default)]
    pub dlq_retry_count: i32,
}

impl From<DeadJobEntry> for DeadJob {
    fn from(e: DeadJobEntry) -> Self {
        Self {
            id: e.id,
            original_job_id: e.original_job_id,
            queue: e.queue,
            task_name: e.task_name,
            payload: e.payload,
            error: e.error,
            retry_count: e.retry_count,
            failed_at: e.failed_at,
            metadata: e.metadata,
            notes: e.notes,
            priority: e.priority,
            max_retries: e.max_retries,
            timeout_ms: e.timeout_ms,
            result_ttl_ms: e.result_ttl_ms,
            namespace: e.namespace,
            dlq_retry_count: e.dlq_retry_count,
        }
    }
}

impl RedisStorage {
    /// Move a job to the dead-letter queue and cascade-cancel its dependents.
    pub fn move_to_dlq(&self, job: &Job, error: &str, metadata: Option<&str>) -> Result<()> {
        let now = now_millis();
        let dlq_id = uuid::Uuid::now_v7().to_string();
        let mut conn = self.conn()?;

        let dlq_retry_count = job
            .metadata
            .as_deref()
            .and_then(|m| serde_json::from_str::<serde_json::Value>(m).ok())
            .and_then(|v| v.get("__dlq_retry_count")?.as_i64())
            .unwrap_or(0) as i32;

        let entry = DeadJobEntry {
            id: dlq_id.clone(),
            original_job_id: job.id.clone(),
            queue: job.queue.clone(),
            task_name: job.task_name.clone(),
            payload: job.payload.clone(),
            error: Some(error.to_string()),
            retry_count: job.retry_count,
            failed_at: now,
            // Preserve the job's own metadata so it survives the round trip;
            // an explicit `metadata` arg overrides it.
            metadata: metadata
                .map(|s| s.to_string())
                .or_else(|| job.metadata.clone()),
            notes: job.notes.clone(),
            priority: job.priority,
            max_retries: job.max_retries,
            timeout_ms: job.timeout_ms,
            result_ttl_ms: job.result_ttl_ms,
            namespace: job.namespace.clone(),
            dlq_retry_count,
        };

        let json = serde_json::to_string(&entry)?;

        let dlq_key = self.key(&["dlq", &dlq_id]);
        let dlq_all = self.key(&["dlq", "all"]);

        let mut dead_job = job.clone();
        let old_status = dead_job.status;
        dead_job.status = JobStatus::Dead;
        dead_job.error = Some(error.to_string());
        dead_job.completed_at = Some(now);
        let dead_json = serde_json::to_string(&dead_job)?;

        // Commit the DLQ entry and the live→archive move together, but only if the
        // job is still live in its expected state. A racing complete/fail or the
        // stale-job reaper may have archived it first; WATCH aborts the EXEC if
        // `job:<id>` changes between the liveness check and the commit, so we never
        // create a duplicate DLQ entry or overwrite a terminal archive.
        let job_live_key = self.key(&["job", &job.id]);
        let dead_lettered: bool =
            redis::transaction(&mut conn, &[job_live_key.as_str()], |conn, pipe| {
                if !conn.exists::<_, bool>(&job_live_key)? {
                    return Ok(Some(false));
                }
                pipe.set(&dlq_key, &json).ignore();
                pipe.zadd(&dlq_all, &dlq_id, now as f64).ignore();
                self.push_archive_ops(pipe, &dead_job, old_status, &dead_json);
                Ok(pipe.query::<Option<()>>(conn)?.map(|()| true))
            })
            .map_err(map_err)?;

        // Cascade-cancel dependents only if we actually dead-lettered the job
        // (skipped when a racer had already archived it).
        drop(conn);
        if dead_lettered {
            self.cascade_cancel(&job.id, "dependency failed")?;
        }

        Ok(())
    }

    /// Dead-letter entries, newest first, paginated. `namespace` of `None`
    /// returns every namespace, matching `list_jobs`.
    pub fn list_dead(
        &self,
        limit: i64,
        offset: i64,
        namespace: Option<&str>,
    ) -> Result<Vec<DeadJob>> {
        // Scoped: namespace is not indexed, so the set is walked newest-first
        // and paginated after the filter, like `list_dead_by_task`.
        if let Some(scope) = namespace {
            return self.list_dead_matching(limit, offset, |entry| {
                entry.namespace.as_deref() == Some(scope)
            });
        }

        let mut conn = self.conn()?;
        let dlq_all = self.key(&["dlq", "all"]);

        let ids: Vec<String> = conn
            .zrevrangebyscore_limit(
                &dlq_all,
                "+inf",
                "-inf",
                offset.max(0) as isize,
                limit.max(0) as isize,
            )
            .map_err(map_err)?;

        let mut results = Vec::new();
        for id in ids {
            let dlq_key = self.key(&["dlq", &id]);
            let data: Option<String> = conn.get(&dlq_key).map_err(map_err)?;
            if let Some(d) = data {
                let entry: DeadJobEntry = serde_json::from_str(&d)?;
                let mut dead = DeadJob::from(entry);
                strip_dead_blob(&mut dead);
                results.push(dead);
            }
        }

        Ok(results)
    }

    /// Keyset-paginated `list_dead`, ordered by `(failed_at, id)` descending.
    /// `dlq:all` is scored by `failed_at`, so the cursor maps straight onto the
    /// ZSET keyset.
    ///
    /// A namespace filter is not indexed, so the scoped form keeps advancing the
    /// cursor until the page is full — the caller still gets `limit` rows rather
    /// than a page thinned by rows it may not see.
    pub fn list_dead_after(
        &self,
        limit: i64,
        after: Option<(i64, &str)>,
        namespace: Option<&str>,
    ) -> Result<Vec<DeadJob>> {
        if limit <= 0 {
            return Ok(Vec::new());
        }
        let mut conn = self.conn()?;
        let dlq_all = self.key(&["dlq", "all"]);

        let mut results: Vec<DeadJob> = Vec::with_capacity(limit as usize);
        let mut cursor: Option<(i64, String)> =
            after.map(|(failed_at, id)| (failed_at, id.to_string()));

        loop {
            let borrowed = cursor.as_ref().map(|(at, id)| (*at, id.as_str()));
            let ids = super::zset_keyset_page(&mut conn, &dlq_all, borrowed, limit)?;
            if ids.is_empty() {
                break;
            }

            // Advanced from the last row *examined*, not the last one kept: a
            // page that filters out entirely still has to move the cursor, or
            // the walk would re-read it forever.
            let mut examined = None;
            for id in &ids {
                let dlq_key = self.key(&["dlq", id]);
                let data: Option<String> = conn.get(&dlq_key).map_err(map_err)?;
                let Some(d) = data else { continue };
                let entry: DeadJobEntry = serde_json::from_str(&d)?;
                examined = Some((entry.failed_at, entry.id.clone()));
                if namespace.is_some_and(|scope| entry.namespace.as_deref() != Some(scope)) {
                    continue;
                }
                let mut dead = DeadJob::from(entry);
                strip_dead_blob(&mut dead);
                results.push(dead);
                if results.len() as i64 == limit {
                    return Ok(results);
                }
            }

            // A whole page can be members whose blob is already gone —
            // `dlq:all` still indexes them, so nothing above set `examined`.
            // Breaking here would end the walk and report the DLQ exhausted
            // while matching entries sit below the page, so advance from the
            // ZSET member itself.
            if examined.is_none() {
                if let Some(last) = ids.last() {
                    let score: Option<f64> = conn.zscore(&dlq_all, last).map_err(map_err)?;
                    examined = score.map(|failed_at| (failed_at as i64, last.clone()));
                }
            }

            // Unscoped pages are already exactly the answer; only a filtered
            // walk needs to look past this page.
            if namespace.is_none() || (ids.len() as i64) < limit || examined.is_none() {
                break;
            }
            cursor = examined;
        }

        Ok(results)
    }

    /// Walk the DLQ newest-first, keeping entries `matches` accepts, and return
    /// one page of them.
    fn list_dead_matching(
        &self,
        limit: i64,
        offset: i64,
        matches: impl Fn(&DeadJobEntry) -> bool,
    ) -> Result<Vec<DeadJob>> {
        if limit <= 0 {
            return Ok(Vec::new());
        }
        let mut conn = self.conn()?;
        let dlq_all = self.key(&["dlq", "all"]);
        let offset = offset.max(0) as usize;
        let limit = limit as usize;

        let ids: Vec<String> = conn.zrevrange(&dlq_all, 0, -1).map_err(map_err)?;
        let mut kept = Vec::new();
        for id in ids {
            let dlq_key = self.key(&["dlq", &id]);
            let data: Option<String> = conn.get(&dlq_key).map_err(map_err)?;
            let Some(d) = data else { continue };
            let entry: DeadJobEntry = serde_json::from_str(&d)?;
            if !matches(&entry) {
                continue;
            }
            let mut dead = DeadJob::from(entry);
            strip_dead_blob(&mut dead);
            kept.push(dead);
            // Stop once this page can be served. Saturating: both are public
            // i64 inputs.
            if kept.len() >= offset.saturating_add(limit) {
                break;
            }
        }

        Ok(kept.into_iter().skip(offset).take(limit).collect())
    }

    /// Dead-letter entries for one task, newest first, paginated.
    pub fn list_dead_by_task(
        &self,
        task_name: &str,
        limit: i64,
        offset: i64,
        namespace: Option<&str>,
    ) -> Result<Vec<DeadJob>> {
        // Neither task name nor namespace is indexed by design, so the global
        // DLQ set is walked newest-first and paginated after the filter.
        self.list_dead_matching(limit, offset, |entry| {
            entry.task_name == task_name
                && namespace.is_none_or(|scope| entry.namespace.as_deref() == Some(scope))
        })
    }

    /// Delete every dead-letter entry for a task. Returns the number removed.
    pub fn purge_dead_by_task(&self, task_name: &str) -> Result<u64> {
        let mut conn = self.conn()?;
        let dlq_all = self.key(&["dlq", "all"]);

        // No per-task index by design: scan the global DLQ set and collect the
        // ids whose entry matches `task_name`.
        let ids: Vec<String> = conn.zrevrange(&dlq_all, 0, -1).map_err(map_err)?;

        // (dlq_id, notes, original_job_id) — notes + original id let the
        // sub:dead index shrink with the purge (no-op for non-pub/sub rows).
        let mut to_delete: Vec<(String, Option<String>, String)> = Vec::new();
        for id in ids {
            let dlq_key = self.key(&["dlq", &id]);
            let data: Option<String> = conn.get(&dlq_key).map_err(map_err)?;
            if let Some(d) = data {
                // Propagate (don't skip) on a corrupt entry: silently ignoring it
                // would leave a task-owned row behind and under-report the count.
                let entry: DeadJobEntry = serde_json::from_str(&d)?;
                if entry.task_name == task_name {
                    to_delete.push((id, entry.notes, entry.original_job_id));
                }
            }
        }

        if to_delete.is_empty() {
            return Ok(0);
        }

        let pipe = &mut redis::pipe();
        for (id, notes, member) in &to_delete {
            pipe.del(self.key(&["dlq", id]));
            pipe.zrem(&dlq_all, id.as_str());
            self.push_pubsub_dead_remove(pipe, notes.as_deref(), member);
        }
        pipe.query::<()>(&mut conn).map_err(map_err)?;
        Ok(to_delete.len() as u64)
    }

    /// Re-enqueue a dead-letter entry as a fresh job, deleting the entry.
    /// Returns the new job's id; `JobNotFound` if the entry is absent or lives
    /// in another namespace — a scoped caller cannot tell the two apart.
    pub fn retry_dead(&self, dead_id: &str, namespace: Option<&str>) -> Result<String> {
        let mut conn = self.conn()?;
        let dlq_key = self.key(&["dlq", dead_id]);

        let data: String = conn
            .get(&dlq_key)
            .map_err(map_err)
            .and_then(|v: Option<String>| {
                v.ok_or_else(|| QueueError::JobNotFound(dead_id.to_string()))
            })?;

        let entry: DeadJobEntry = serde_json::from_str(&data)?;
        if namespace.is_some_and(|scope| entry.namespace.as_deref() != Some(scope)) {
            return Err(QueueError::JobNotFound(dead_id.to_string()));
        }

        // Attribution + original job id for the sub:dead removal below, captured
        // before `entry`'s fields are moved into `new_job`. The re-enqueue below
        // re-attributes the fresh job into sub:pending via its carried notes.
        let dead_notes = entry.notes.clone();
        let dead_member = entry.original_job_id.clone();

        let retry_metadata = {
            let next_count = entry.dlq_retry_count + 1;
            let mut obj = entry
                .metadata
                .as_deref()
                .and_then(|m| {
                    serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(m).ok()
                })
                .unwrap_or_default();
            obj.insert(
                "__dlq_retry_count".to_string(),
                serde_json::Value::from(next_count),
            );
            Some(serde_json::to_string(&serde_json::Value::Object(obj)).unwrap_or_default())
        };

        let new_job = NewJob {
            queue: entry.queue,
            task_name: entry.task_name,
            payload: entry.payload,
            priority: entry.priority,
            scheduled_at: now_millis(),
            max_retries: entry.max_retries,
            timeout_ms: entry.timeout_ms,
            unique_key: None,
            metadata: retry_metadata,
            notes: entry.notes,
            depends_on: vec![],
            expires_at: None,
            result_ttl_ms: entry.result_ttl_ms,
            namespace: entry.namespace,
        };

        let job = self.enqueue(new_job)?;

        // Remove from DLQ, and drop it from its subscription's sub:dead index
        // so the dead count doesn't keep the retried delivery (no-op otherwise).
        let dlq_all = self.key(&["dlq", "all"]);
        let pipe = &mut redis::pipe();
        pipe.del(&dlq_key);
        pipe.zrem(&dlq_all, dead_id);
        self.push_pubsub_dead_remove(pipe, dead_notes.as_deref(), &dead_member);
        pipe.query::<()>(&mut conn).map_err(map_err)?;

        Ok(job.id)
    }

    /// Purge dead-letter entries older than the cutoff. Returns the count
    /// removed.
    pub fn purge_dead(&self, older_than_ms: i64) -> Result<u64> {
        let mut conn = self.conn()?;
        let dlq_all = self.key(&["dlq", "all"]);
        let mut total = 0u64;

        // Every id in the `-inf..=cutoff` score window is eligible, and each
        // batch is deleted before the next query, so re-reading the window
        // drains the expired rows in bounded chunks instead of loading the
        // entire below-cutoff set at once.
        loop {
            let ids: Vec<String> = conn
                .zrangebyscore_limit(&dlq_all, "-inf", older_than_ms as f64, 0, SCAN_BATCH)
                .map_err(map_err)?;
            if ids.is_empty() {
                break;
            }

            // Load blobs to attribute each dead row to its subscription so the
            // sub:dead index shrinks with the purge (no-op for non-pub/sub rows).
            let blob_keys: Vec<String> = ids.iter().map(|id| self.key(&["dlq", id])).collect();
            let blobs: Vec<Option<String>> = conn.mget(&blob_keys).map_err(map_err)?;

            let pipe = &mut redis::pipe();
            for (id, blob) in ids.iter().zip(&blobs) {
                pipe.del(self.key(&["dlq", id]));
                pipe.zrem(&dlq_all, id.as_str());
                if let Some(d) = blob {
                    if let Ok(entry) = serde_json::from_str::<DeadJobEntry>(d) {
                        self.push_pubsub_dead_remove(
                            pipe,
                            entry.notes.as_deref(),
                            &entry.original_job_id,
                        );
                    }
                }
            }
            pipe.query::<()>(&mut conn).map_err(map_err)?;

            total += ids.len() as u64;
            if (ids.len() as isize) < SCAN_BATCH {
                break;
            }
        }

        Ok(total)
    }

    /// Delete one dead-letter entry. Returns `false` when none matched.
    /// An entry in another namespace reports `false`, the same answer an
    /// unknown id gets.
    pub fn delete_dead(&self, dead_id: &str, namespace: Option<&str>) -> Result<bool> {
        let mut conn = self.conn()?;
        let dlq_key = self.key(&["dlq", dead_id]);
        let dlq_all = self.key(&["dlq", "all"]);

        // Load the row (rather than a bare EXISTS) so its subscription
        // attribution is known and the sub:dead index shrinks with the delete.
        let data: Option<String> = conn.get(&dlq_key).map_err(map_err)?;
        let Some(d) = data else {
            return Ok(false);
        };
        let entry: DeadJobEntry = serde_json::from_str(&d)?;
        if namespace.is_some_and(|scope| entry.namespace.as_deref() != Some(scope)) {
            return Ok(false);
        }

        let pipe = &mut redis::pipe();
        pipe.del(&dlq_key);
        pipe.zrem(&dlq_all, dead_id);
        self.push_pubsub_dead_remove(pipe, entry.notes.as_deref(), &entry.original_job_id);
        pipe.query::<()>(&mut conn).map_err(map_err)?;
        Ok(true)
    }

    /// Purge dead-letter entries by the global/per-entry TTL. Returns the
    /// count removed.
    pub fn purge_dead_with_ttl(&self, global_cutoff_ms: Option<i64>) -> Result<u64> {
        let mut conn = self.conn()?;
        let dlq_all = self.key(&["dlq", "all"]);
        let now = now_millis();
        let mut total = 0u64;

        // Per-entry TTL lives in the blob, so every row must be inspected — but
        // ZSCAN walks the set in bounded batches instead of loading every id at
        // once. Each batch's expired rows are deleted before the next step.
        let mut cursor: u64 = 0;
        loop {
            let (next, flat): (u64, Vec<String>) = redis::cmd("ZSCAN")
                .arg(&dlq_all)
                .arg(cursor)
                .arg("COUNT")
                .arg(SCAN_BATCH)
                .query(&mut conn)
                .map_err(map_err)?;
            cursor = next;

            // ZSCAN returns a flat [member, score, member, score, ...] list.
            let mut to_delete: Vec<(String, Option<String>, String)> = Vec::new();
            for id in flat.iter().step_by(2) {
                let dlq_key = self.key(&["dlq", id]);
                let data: Option<String> = conn.get(&dlq_key).map_err(map_err)?;
                if let Some(d) = data {
                    if let Ok(entry) = serde_json::from_str::<DeadJobEntry>(&d) {
                        // An overflowing TTL is treated as never expiring, as in
                        // the completed-job purge.
                        let expired = match entry.result_ttl_ms {
                            Some(ttl) => entry
                                .failed_at
                                .checked_add(ttl)
                                .is_some_and(|expiry| expiry <= now),
                            None => global_cutoff_ms.is_some_and(|c| entry.failed_at < c),
                        };
                        if expired {
                            to_delete.push((id.clone(), entry.notes, entry.original_job_id));
                        }
                    }
                }
            }

            if !to_delete.is_empty() {
                let pipe = &mut redis::pipe();
                for (id, notes, member) in &to_delete {
                    pipe.del(self.key(&["dlq", id]));
                    pipe.zrem(&dlq_all, id.as_str());
                    self.push_pubsub_dead_remove(pipe, notes.as_deref(), member);
                }
                pipe.query::<()>(&mut conn).map_err(map_err)?;
                total += to_delete.len() as u64;
            }

            if cursor == 0 {
                break;
            }
        }

        Ok(total)
    }

    /// Count dead-letter entries a purge would remove under `global_cutoff_ms`,
    /// mirroring [`Self::purge_dead_with_ttl`]'s per-entry/global predicate
    /// without deleting. Read-only: `ZSCAN` walks the set in bounded batches and
    /// each entry's blob is inspected exactly as the purge inspects it.
    pub fn count_expired_dead(&self, global_cutoff_ms: Option<i64>, now: i64) -> Result<u64> {
        let mut conn = self.conn()?;
        let dlq_all = self.key(&["dlq", "all"]);
        let mut total = 0u64;

        let mut cursor: u64 = 0;
        loop {
            let (next, flat): (u64, Vec<String>) = redis::cmd("ZSCAN")
                .arg(&dlq_all)
                .arg(cursor)
                .arg("COUNT")
                .arg(SCAN_BATCH)
                .query(&mut conn)
                .map_err(map_err)?;
            cursor = next;

            // ZSCAN returns a flat [member, score, member, score, ...] list.
            for id in flat.iter().step_by(2) {
                let dlq_key = self.key(&["dlq", id]);
                let data: Option<String> = conn.get(&dlq_key).map_err(map_err)?;
                if let Some(d) = data {
                    if let Ok(entry) = serde_json::from_str::<DeadJobEntry>(&d) {
                        let expired = match entry.result_ttl_ms {
                            Some(ttl) => entry
                                .failed_at
                                .checked_add(ttl)
                                .is_some_and(|expiry| expiry <= now),
                            None => global_cutoff_ms.is_some_and(|c| entry.failed_at < c),
                        };
                        if expired {
                            total += 1;
                        }
                    }
                }
            }

            if cursor == 0 {
                break;
            }
        }

        Ok(total)
    }

    /// Dead-letter entries eligible for automatic retry, bounded by `limit`.
    pub fn list_dead_for_retry(
        &self,
        cutoff_ms: i64,
        max_retries: i32,
        namespace: Option<&str>,
        queues: &[String],
        limit: i64,
    ) -> Result<Vec<DeadJob>> {
        let mut conn = self.conn()?;
        let dlq_all = self.key(&["dlq", "all"]);

        let ids: Vec<String> = conn
            .zrangebyscore(&dlq_all, "-inf", cutoff_ms as f64)
            .map_err(map_err)?;

        let mut results = Vec::new();
        for id in ids {
            if results.len() >= limit as usize {
                break;
            }
            let dlq_key = self.key(&["dlq", &id]);
            let data: Option<String> = conn.get(&dlq_key).map_err(map_err)?;
            if let Some(d) = data {
                if let Ok(entry) = serde_json::from_str::<DeadJobEntry>(&d) {
                    // Scope to the worker's own namespace + served queues, matching
                    // the Diesel backend (the poller's dequeue scoping).
                    if entry.dlq_retry_count < max_retries
                        && entry.namespace.as_deref() == namespace
                        && queues.iter().any(|q| q == &entry.queue)
                    {
                        let mut dead = DeadJob::from(entry);
                        strip_dead_blob(&mut dead);
                        results.push(dead);
                    }
                }
            }
        }

        Ok(results)
    }
}
