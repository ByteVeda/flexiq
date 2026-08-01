use redis::Commands;
use serde::{Deserialize, Serialize};

use super::{map_err, RedisStorage};
use crate::error::Result;
use crate::job::now_millis;
use crate::storage::records::{ReplayEntry, TaskMetric};

#[derive(Serialize, Deserialize)]
struct MetricEntry {
    pub id: String,
    pub task_name: String,
    pub job_id: String,
    pub wall_time_ns: i64,
    pub memory_bytes: i64,
    pub succeeded: bool,
    pub recorded_at: i64,
    /// Absent on rows written before namespaces reached the metric store.
    #[serde(default)]
    pub namespace: Option<String>,
}

impl From<MetricEntry> for TaskMetric {
    fn from(e: MetricEntry) -> Self {
        Self {
            id: e.id,
            task_name: e.task_name,
            job_id: e.job_id,
            wall_time_ns: e.wall_time_ns,
            memory_bytes: e.memory_bytes,
            succeeded: e.succeeded,
            recorded_at: e.recorded_at,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct ReplayHistoryEntry {
    pub id: String,
    pub original_job_id: String,
    pub replay_job_id: String,
    pub replayed_at: i64,
    pub original_result: Option<Vec<u8>>,
    pub replay_result: Option<Vec<u8>>,
    pub original_error: Option<String>,
    pub replay_error: Option<String>,
}

impl From<ReplayHistoryEntry> for ReplayEntry {
    fn from(e: ReplayHistoryEntry) -> Self {
        Self {
            id: e.id,
            original_job_id: e.original_job_id,
            replay_job_id: e.replay_job_id,
            replayed_at: e.replayed_at,
            original_result: e.original_result,
            replay_result: e.replay_result,
            original_error: e.original_error,
            replay_error: e.replay_error,
        }
    }
}

impl RedisStorage {
    /// Record one execution measurement for a task.
    pub fn record_metric(
        &self,
        task_name: &str,
        job_id: &str,
        wall_time_ns: i64,
        memory_bytes: i64,
        succeeded: bool,
        namespace: Option<&str>,
    ) -> Result<()> {
        let mut conn = self.conn()?;
        let id = uuid::Uuid::now_v7().to_string();
        let now = now_millis();

        let entry = MetricEntry {
            id: id.clone(),
            task_name: task_name.to_string(),
            job_id: job_id.to_string(),
            wall_time_ns,
            memory_bytes,
            succeeded,
            recorded_at: now,
            namespace: namespace.map(str::to_string),
        };

        let json = serde_json::to_string(&entry)?;

        let metric_key = self.key(&["metric", &id]);
        let all_key = self.key(&["metrics", "all"]);
        let by_task_key = self.key(&["metrics", "by_task", task_name]);

        let pipe = &mut redis::pipe();
        pipe.set(&metric_key, &json);
        pipe.zadd(&all_key, &id, now as f64);
        pipe.zadd(&by_task_key, &id, now as f64);
        pipe.query::<()>(&mut conn).map_err(map_err)?;

        Ok(())
    }

    /// Metrics recorded since `since_ms` (Unix milliseconds) for one task, or
    /// all tasks when `name` is `None`.
    pub fn get_metrics(
        &self,
        name: Option<&str>,
        since_ms: i64,
        namespace: Option<&str>,
    ) -> Result<Vec<TaskMetric>> {
        let mut conn = self.conn()?;

        let ids: Vec<String> = if let Some(n) = name {
            let by_task_key = self.key(&["metrics", "by_task", n]);
            conn.zrangebyscore(&by_task_key, since_ms as f64, "+inf")
                .map_err(map_err)?
        } else {
            let all_key = self.key(&["metrics", "all"]);
            conn.zrangebyscore(&all_key, since_ms as f64, "+inf")
                .map_err(map_err)?
        };

        let mut rows = Vec::new();
        for id in ids {
            let metric_key = self.key(&["metric", &id]);
            let data: Option<String> = conn.get(&metric_key).map_err(map_err)?;
            if let Some(d) = data {
                let entry: MetricEntry = serde_json::from_str(&d)?;
                // Not indexed by namespace, so the scope is applied in memory.
                if namespace.is_some_and(|ns| entry.namespace.as_deref() != Some(ns)) {
                    continue;
                }
                rows.push(TaskMetric::from(entry));
            }
        }

        // Sort by recorded_at desc
        rows.sort_by_key(|r| std::cmp::Reverse(r.recorded_at));
        Ok(rows)
    }

    /// Purge metric records recorded at or before the cutoff (inclusive).
    /// Returns the count removed.
    /// Count task-metric rows a purge would remove older than the cutoff,
    /// mirroring [`Self::purge_metrics`] without deleting. `metrics:all` is
    /// scored by `recorded_at`, so a `ZCOUNT` (inclusive of the cutoff, like the
    /// purge's `ZRANGEBYSCORE`) is precise and O(log n).
    pub fn count_expired_metrics(&self, older_than_ms: i64) -> Result<u64> {
        let mut conn = self.conn()?;
        let all_key = self.key(&["metrics", "all"]);
        let n: i64 = conn
            .zcount(&all_key, "-inf", older_than_ms as f64)
            .map_err(map_err)?;
        Ok(n as u64)
    }

    /// Purge task-metric rows older than the cutoff. Returns the count removed.
    pub fn purge_metrics(&self, older_than_ms: i64) -> Result<u64> {
        let mut conn = self.conn()?;
        let all_key = self.key(&["metrics", "all"]);

        let ids: Vec<String> = conn
            .zrangebyscore(&all_key, "-inf", older_than_ms as f64)
            .map_err(map_err)?;

        if ids.is_empty() {
            return Ok(0);
        }

        let pipe = &mut redis::pipe();
        for id in &ids {
            let metric_key = self.key(&["metric", id]);
            // Load to get task_name for by_task cleanup
            let data: Option<String> = conn.get(&metric_key).map_err(map_err)?;
            if let Some(d) = data {
                if let Ok(entry) = serde_json::from_str::<MetricEntry>(&d) {
                    let by_task_key = self.key(&["metrics", "by_task", &entry.task_name]);
                    pipe.zrem(&by_task_key, id);
                }
            }
            pipe.del(&metric_key);
            pipe.zrem(&all_key, id);
        }
        pipe.query::<()>(&mut conn).map_err(map_err)?;

        Ok(ids.len() as u64)
    }

    /// Record a replay of a completed job, pairing original and replay
    /// outcomes.
    pub fn record_replay(
        &self,
        original_job_id: &str,
        replay_job_id: &str,
        original_result: Option<&[u8]>,
        replay_result: Option<&[u8]>,
        original_error: Option<&str>,
        replay_error: Option<&str>,
    ) -> Result<()> {
        let mut conn = self.conn()?;
        let id = uuid::Uuid::now_v7().to_string();
        let now = now_millis();

        let entry = ReplayHistoryEntry {
            id: id.clone(),
            original_job_id: original_job_id.to_string(),
            replay_job_id: replay_job_id.to_string(),
            replayed_at: now,
            original_result: original_result.map(|r| r.to_vec()),
            replay_result: replay_result.map(|r| r.to_vec()),
            original_error: original_error.map(|s| s.to_string()),
            replay_error: replay_error.map(|s| s.to_string()),
        };

        let json = serde_json::to_string(&entry)?;

        let replay_key = self.key(&["replay", &id]);
        let by_original = self.key(&["replay", "by_original", original_job_id]);

        let pipe = &mut redis::pipe();
        pipe.set(&replay_key, &json);
        pipe.rpush(&by_original, &id);
        pipe.query::<()>(&mut conn).map_err(map_err)?;

        Ok(())
    }

    /// All replays recorded against `original_job_id`.
    pub fn get_replay_history(&self, original_job_id: &str) -> Result<Vec<ReplayEntry>> {
        let mut conn = self.conn()?;
        let by_original = self.key(&["replay", "by_original", original_job_id]);
        let ids: Vec<String> = conn.lrange(&by_original, 0, -1).map_err(map_err)?;

        let mut rows = Vec::new();
        for id in ids {
            let replay_key = self.key(&["replay", &id]);
            let data: Option<String> = conn.get(&replay_key).map_err(map_err)?;
            if let Some(d) = data {
                let entry: ReplayHistoryEntry = serde_json::from_str(&d)?;
                rows.push(ReplayEntry::from(entry));
            }
        }

        // Sort by replayed_at desc
        rows.sort_by_key(|r| std::cmp::Reverse(r.replayed_at));
        Ok(rows)
    }
}
