use redis::Commands;

use super::{map_err, RedisStorage};
use crate::error::Result;
use crate::job::now_millis;
use crate::storage::records::{WorkerInfo, WorkerRegistration};

impl RedisStorage {
    /// Register a worker in the cluster registry, or update it if the id
    /// already exists. `tags`/`resources` are pre-encoded JSON.
    pub fn register_worker(&self, registration: &WorkerRegistration<'_>) -> Result<()> {
        let mut conn = self.conn()?;
        let now = now_millis();
        let wkey = self.key(&["worker", registration.worker_id]);
        let wall = self.key(&["workers", "all"]);

        let pipe = &mut redis::pipe();
        pipe.hset(&wkey, "last_heartbeat", now);
        pipe.hset(&wkey, "queues", registration.queues);
        pipe.hset(&wkey, "status", "active");
        pipe.hset(&wkey, "tags", registration.tags.unwrap_or(""));
        pipe.hset(&wkey, "resources", registration.resources.unwrap_or(""));
        pipe.hset(
            &wkey,
            "resource_health",
            registration.resource_health.unwrap_or(""),
        );
        pipe.hset(&wkey, "threads", registration.threads);
        pipe.hset(&wkey, "started_at", now);
        pipe.hset(&wkey, "hostname", registration.hostname.unwrap_or(""));
        pipe.hset(&wkey, "pid", registration.pid.unwrap_or(0));
        pipe.hset(&wkey, "pool_type", registration.pool_type.unwrap_or(""));
        pipe.hset(&wkey, "sdk", registration.sdk.unwrap_or(""));
        pipe.hset(&wkey, "sdk_version", registration.sdk_version.unwrap_or(""));
        pipe.sadd(&wall, registration.worker_id);
        pipe.query::<()>(&mut conn).map_err(map_err)?;

        Ok(())
    }

    /// Refresh a worker's heartbeat timestamp and overwrite its
    /// resource-health JSON (`None` clears any previous value).
    pub fn heartbeat(&self, worker_id: &str, resource_health: Option<&str>) -> Result<()> {
        let mut conn = self.conn()?;
        let now = now_millis();
        let wkey = self.key(&["worker", worker_id]);

        let pipe = &mut redis::pipe();
        pipe.hset(&wkey, "last_heartbeat", now);
        pipe.hset(&wkey, "resource_health", resource_health.unwrap_or(""));
        pipe.query::<()>(&mut conn).map_err(map_err)?;

        Ok(())
    }

    /// Set a worker's status string.
    pub fn update_worker_status(
        &self,
        worker_id: &str,
        status: crate::storage::records::WorkerStatus,
    ) -> Result<()> {
        let mut conn = self.conn()?;
        let wkey = self.key(&["worker", worker_id]);

        conn.hset::<_, _, _, ()>(&wkey, "status", status.as_str())
            .map_err(map_err)?;

        Ok(())
    }

    /// Every registered worker with its heartbeat state.
    pub fn list_workers(&self) -> Result<Vec<WorkerInfo>> {
        let mut conn = self.conn()?;
        let wall = self.key(&["workers", "all"]);

        let worker_ids: Vec<String> = conn.smembers(&wall).map_err(map_err)?;

        let mut rows = Vec::new();
        for wid in worker_ids {
            let wkey = self.key(&["worker", &wid]);
            let data: std::collections::HashMap<String, String> =
                conn.hgetall(&wkey).map_err(map_err)?;

            if data.is_empty() {
                continue;
            }

            let to_opt =
                |key: &str| -> Option<String> { data.get(key).filter(|s| !s.is_empty()).cloned() };

            rows.push(WorkerInfo {
                worker_id: wid,
                last_heartbeat: data
                    .get("last_heartbeat")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0),
                queues: data
                    .get("queues")
                    .cloned()
                    .unwrap_or_else(|| "default".to_string()),
                status: data
                    .get("status")
                    .cloned()
                    .unwrap_or_else(|| "active".to_string()),
                tags: to_opt("tags"),
                resources: to_opt("resources"),
                resource_health: to_opt("resource_health"),
                threads: data
                    .get("threads")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0),
                started_at: data.get("started_at").and_then(|s| s.parse().ok()),
                hostname: to_opt("hostname"),
                pid: data
                    .get("pid")
                    .and_then(|s| s.parse().ok())
                    .filter(|&v: &i32| v != 0),
                pool_type: to_opt("pool_type"),
                sdk: to_opt("sdk"),
                sdk_version: to_opt("sdk_version"),
            });
        }

        Ok(rows)
    }

    /// Ids of workers whose heartbeat is at or after `cutoff_ms`.
    ///
    /// Pipelines the per-worker `HGET` so a live-set read costs one round trip
    /// plus one pipeline flush, not one round trip per worker.
    pub fn list_live_worker_ids(&self, cutoff_ms: i64) -> Result<Vec<String>> {
        let mut conn = self.conn()?;
        let wall = self.key(&["workers", "all"]);

        let worker_ids: Vec<String> = conn.smembers(&wall).map_err(map_err)?;
        if worker_ids.is_empty() {
            return Ok(Vec::new());
        }

        let heartbeats: Vec<Option<i64>> = self.heartbeats_pipelined(&mut conn, &worker_ids)?;

        Ok(worker_ids
            .into_iter()
            .zip(heartbeats)
            .filter_map(|(wid, hb)| match hb {
                Some(last_hb) if last_hb >= cutoff_ms => Some(wid),
                _ => None,
            })
            .collect())
    }

    /// Remove workers whose heartbeat is stale past the dead-worker threshold.
    /// Returns the reaped worker ids.
    pub fn reap_dead_workers(&self) -> Result<Vec<String>> {
        let mut conn = self.conn()?;
        let cutoff = crate::storage::dead_worker_cutoff(now_millis());
        let wall = self.key(&["workers", "all"]);

        let worker_ids: Vec<String> = conn.smembers(&wall).map_err(map_err)?;
        if worker_ids.is_empty() {
            return Ok(Vec::new());
        }

        let heartbeats = self.heartbeats_pipelined(&mut conn, &worker_ids)?;

        let mut reaped = Vec::new();
        for (wid, hb) in worker_ids.into_iter().zip(heartbeats) {
            let is_dead = match hb {
                Some(last_hb) => last_hb < cutoff,
                None => true,
            };

            if is_dead {
                let wkey = self.key(&["worker", &wid]);
                let pipe = &mut redis::pipe();
                pipe.del(&wkey);
                pipe.srem(&wall, &wid);
                pipe.query::<()>(&mut conn).map_err(map_err)?;
                reaped.push(wid);
            }
        }

        Ok(reaped)
    }

    /// Fetch every worker's `last_heartbeat` in one pipeline, preserving input
    /// order so callers can zip the result back onto the id list.
    fn heartbeats_pipelined(
        &self,
        conn: &mut redis::Connection,
        worker_ids: &[String],
    ) -> Result<Vec<Option<i64>>> {
        let mut pipe = redis::pipe();
        for wid in worker_ids {
            pipe.hget(self.key(&["worker", wid]), "last_heartbeat");
        }
        pipe.query(conn).map_err(map_err)
    }

    /// Remove a worker from the registry (called on shutdown).
    pub fn unregister_worker(&self, worker_id: &str) -> Result<()> {
        let mut conn = self.conn()?;
        let wkey = self.key(&["worker", worker_id]);
        let wall = self.key(&["workers", "all"]);

        let pipe = &mut redis::pipe();
        pipe.del(&wkey);
        pipe.srem(&wall, worker_id);
        pipe.query::<()>(&mut conn).map_err(map_err)?;

        Ok(())
    }

    /// Job ids currently execution-claimed by a worker.
    pub fn list_claims_by_worker(&self, worker_id: &str) -> Result<Vec<String>> {
        let mut conn = self.conn()?;
        let pattern = self.key(&["exec_claim", "*"]);

        let mut job_ids = Vec::new();
        let mut cursor: u64 = 0;
        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(100)
                .query(&mut conn)
                .map_err(map_err)?;

            for key in keys {
                let value: Option<String> = conn.get(&key).map_err(map_err)?;
                if let Some(val) = value {
                    // Value format is "{worker_id}:{timestamp}"
                    if val.starts_with(worker_id) && val[worker_id.len()..].starts_with(':') {
                        // Extract job_id from key: "{prefix}exec_claim:{job_id}"
                        let prefix = self.key(&["exec_claim", ""]);
                        if let Some(job_id) = key.strip_prefix(&prefix) {
                            job_ids.push(job_id.to_string());
                        }
                    }
                }
            }

            cursor = next_cursor;
            if cursor == 0 {
                break;
            }
        }

        Ok(job_ids)
    }
}
