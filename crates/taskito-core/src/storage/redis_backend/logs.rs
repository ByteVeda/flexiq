use redis::Commands;
use serde::{Deserialize, Serialize};

use super::{map_err, RedisStorage, SCAN_BATCH};
use crate::error::Result;
use crate::job::now_millis;
use crate::storage::records::TaskLogEntry;

#[derive(Serialize, Deserialize)]
struct LogEntry {
    pub id: String,
    pub job_id: String,
    pub task_name: String,
    pub level: String,
    pub message: String,
    pub extra: Option<String>,
    pub logged_at: i64,
    /// Absent on rows written before namespaces reached the log store.
    #[serde(default)]
    pub namespace: Option<String>,
}

impl From<LogEntry> for TaskLogEntry {
    fn from(e: LogEntry) -> Self {
        Self {
            id: e.id,
            job_id: e.job_id,
            task_name: e.task_name,
            level: e.level,
            message: e.message,
            extra: e.extra,
            logged_at: e.logged_at,
        }
    }
}

impl RedisStorage {
    /// Write one structured log line for a job. `extra` is pre-encoded JSON.
    pub fn write_task_log(
        &self,
        job_id: &str,
        task_name: &str,
        level: &str,
        message: &str,
        extra: Option<&str>,
        namespace: Option<&str>,
    ) -> Result<()> {
        let mut conn = self.conn()?;
        let id = uuid::Uuid::now_v7().to_string();
        let now = now_millis();

        let entry = LogEntry {
            id: id.clone(),
            job_id: job_id.to_string(),
            task_name: task_name.to_string(),
            level: level.to_string(),
            message: message.to_string(),
            extra: extra.map(|s| s.to_string()),
            logged_at: now,
            namespace: namespace.map(str::to_string),
        };

        let json = serde_json::to_string(&entry)?;

        let log_key = self.key(&["log", &id]);
        let by_job_key = self.key(&["logs", "by_job", job_id]);
        let all_key = self.key(&["logs", "all"]);

        let pipe = &mut redis::pipe();
        pipe.set(&log_key, &json);
        pipe.zadd(&by_job_key, &id, now as f64);
        pipe.zadd(&all_key, &id, now as f64);
        pipe.query::<()>(&mut conn).map_err(map_err)?;

        Ok(())
    }

    /// All log lines for a job, in chronological (UUIDv7 id) order;
    /// same-millisecond lines have no guaranteed emission order.
    /// A job in another namespace reports no lines, like an unknown id.
    pub fn get_task_logs(
        &self,
        job_id: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<TaskLogEntry>> {
        let mut conn = self.conn()?;
        let by_job_key = self.key(&["logs", "by_job", job_id]);

        let ids: Vec<String> = conn
            .zrangebyscore(&by_job_key, "-inf", "+inf")
            .map_err(map_err)?;

        let mut rows = Vec::new();
        for id in ids {
            let log_key = self.key(&["log", &id]);
            let data: Option<String> = conn.get(&log_key).map_err(map_err)?;
            if let Some(d) = data {
                let entry: LogEntry = serde_json::from_str(&d)?;
                if namespace.is_some_and(|scope| entry.namespace.as_deref() != Some(scope)) {
                    continue;
                }
                rows.push(TaskLogEntry::from(entry));
            }
        }

        // Sort by logged_at asc
        rows.sort_by_key(|r| r.logged_at);
        Ok(rows)
    }

    /// Logs for a job with id strictly after `after_id` (cursor scan).
    /// Scoped like `get_task_logs`.
    pub fn get_task_logs_after(
        &self,
        job_id: &str,
        after_id: Option<&str>,
        namespace: Option<&str>,
    ) -> Result<Vec<TaskLogEntry>> {
        let mut conn = self.conn()?;
        let by_job_key = self.key(&["logs", "by_job", job_id]);

        // Filter ids before fetching values so old entries cost one cheap
        // ZRANGEBYSCORE, not a GET per historical log.
        let mut ids: Vec<String> = conn
            .zrangebyscore(&by_job_key, "-inf", "+inf")
            .map_err(map_err)?;
        if let Some(cursor) = after_id {
            ids.retain(|id| id.as_str() > cursor);
        }

        let mut rows = Vec::new();
        for id in ids {
            let log_key = self.key(&["log", &id]);
            let data: Option<String> = conn.get(&log_key).map_err(map_err)?;
            if let Some(d) = data {
                let entry: LogEntry = serde_json::from_str(&d)?;
                if namespace.is_some_and(|scope| entry.namespace.as_deref() != Some(scope)) {
                    continue;
                }
                rows.push(TaskLogEntry::from(entry));
            }
        }

        // UUIDv7 ids sort by creation time, so id order == time order.
        rows.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(rows)
    }

    /// Log lines across jobs, filtered by task name and/or level, newest since
    /// `since_ms` (Unix milliseconds), bounded by `limit`.
    pub fn query_task_logs(
        &self,
        task_name: Option<&str>,
        level: Option<&str>,
        since_ms: i64,
        limit: i64,
        namespace: Option<&str>,
    ) -> Result<Vec<TaskLogEntry>> {
        // A zero limit is an empty page; without this the filtered walk below
        // never hits its `rows.len() == limit` stop and scans the whole range.
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut conn = self.conn()?;
        let all_key = self.key(&["logs", "all"]);

        // Fast path: with no task/level/namespace filter the page is exactly the
        // newest `limit` ids at or after `since_ms`, so bound the fetch server-side
        // (`logs:all` is scored by logged_at) instead of loading every id since
        // the cutoff and truncating in memory.
        if task_name.is_none() && level.is_none() && namespace.is_none() {
            let ids: Vec<String> = if limit >= 0 {
                conn.zrevrangebyscore_limit(&all_key, "+inf", since_ms as f64, 0, limit as isize)
                    .map_err(map_err)?
            } else {
                conn.zrevrangebyscore(&all_key, "+inf", since_ms as f64)
                    .map_err(map_err)?
            };
            let mut rows = Vec::with_capacity(ids.len());
            for id in ids {
                let log_key = self.key(&["log", &id]);
                let data: Option<String> = conn.get(&log_key).map_err(map_err)?;
                if let Some(d) = data {
                    let entry: LogEntry = serde_json::from_str(&d)?;
                    rows.push(TaskLogEntry::from(entry));
                }
            }
            // ZREVRANGEBYSCORE already yields newest-first (logged_at desc).
            return Ok(rows);
        }

        // Filtered path: task_name/level are not indexed, so the cutoff range is
        // walked newest-first in bounded windows and filtered in memory. Each
        // window is hydrated in one pipelined round trip and the walk stops as
        // soon as `limit` rows match, so neither memory nor round trips scale
        // with the log history.
        let mut rows = Vec::new();
        let mut offset: isize = 0;
        loop {
            let ids: Vec<String> = conn
                .zrevrangebyscore_limit(&all_key, "+inf", since_ms as f64, offset, SCAN_BATCH)
                .map_err(map_err)?;
            if ids.is_empty() {
                break;
            }
            offset += ids.len() as isize;

            let log_keys: Vec<String> = ids.iter().map(|id| self.key(&["log", id])).collect();
            let mut pipe = redis::pipe();
            for key in &log_keys {
                pipe.get(key);
            }
            let entries: Vec<Option<String>> = pipe.query(&mut conn).map_err(map_err)?;

            for data in entries.into_iter().flatten() {
                let entry: LogEntry = serde_json::from_str(&data)?;

                if task_name.is_some_and(|n| entry.task_name != n) {
                    continue;
                }
                if level.is_some_and(|l| entry.level != l) {
                    continue;
                }
                if namespace.is_some_and(|ns| entry.namespace.as_deref() != Some(ns)) {
                    continue;
                }

                rows.push(TaskLogEntry::from(entry));
                if limit >= 0 && rows.len() as i64 == limit {
                    return Ok(rows);
                }
            }

            if (ids.len() as isize) < SCAN_BATCH {
                break;
            }
        }

        // ZREVRANGEBYSCORE walks newest-first, so the rows are already
        // logged_at desc.
        Ok(rows)
    }

    /// Purge log lines logged at or before the cutoff (inclusive). Returns
    /// the count removed.
    /// Count task-log lines a purge would remove older than the cutoff,
    /// mirroring [`Self::purge_task_logs`] without deleting. `logs:all` is scored
    /// by `logged_at`, so a `ZCOUNT` (inclusive of the cutoff, like the purge's
    /// `ZRANGEBYSCORE`) is precise and O(log n).
    pub fn count_expired_task_logs(&self, older_than_ms: i64) -> Result<u64> {
        let mut conn = self.conn()?;
        let all_key = self.key(&["logs", "all"]);
        let n: i64 = conn
            .zcount(&all_key, "-inf", older_than_ms as f64)
            .map_err(map_err)?;
        Ok(n as u64)
    }

    /// Purge task-log lines older than the cutoff. Returns the count removed.
    pub fn purge_task_logs(&self, older_than_ms: i64) -> Result<u64> {
        let mut conn = self.conn()?;
        let all_key = self.key(&["logs", "all"]);
        let mut total = 0u64;

        // Every id in the `-inf..=cutoff` window is expired and gets deleted, so
        // re-reading the window drains old logs in bounded batches rather than
        // loading the whole below-cutoff set in one shot.
        loop {
            let ids: Vec<String> = conn
                .zrangebyscore_limit(&all_key, "-inf", older_than_ms as f64, 0, SCAN_BATCH)
                .map_err(map_err)?;
            if ids.is_empty() {
                break;
            }

            let pipe = &mut redis::pipe();
            for id in &ids {
                let log_key = self.key(&["log", id]);
                // Load to get job_id for by_job cleanup.
                let data: Option<String> = conn.get(&log_key).map_err(map_err)?;
                if let Some(d) = data {
                    if let Ok(entry) = serde_json::from_str::<LogEntry>(&d) {
                        let by_job_key = self.key(&["logs", "by_job", &entry.job_id]);
                        pipe.zrem(&by_job_key, id);
                    }
                }
                pipe.del(&log_key);
                pipe.zrem(&all_key, id);
            }
            pipe.query::<()>(&mut conn).map_err(map_err)?;

            total += ids.len() as u64;
            if (ids.len() as isize) < SCAN_BATCH {
                break;
            }
        }

        Ok(total)
    }
}
