use redis::Commands;
use serde::{Deserialize, Serialize};

use super::{map_err, RedisStorage};
use crate::error::Result;
use crate::storage::records::{NewPeriodicTask, PeriodicTask};

#[derive(Serialize, Deserialize)]
struct PeriodicEntry {
    pub name: String,
    pub task_name: String,
    pub cron_expr: String,
    pub args: Option<Vec<u8>>,
    pub kwargs: Option<Vec<u8>>,
    pub queue: String,
    pub enabled: bool,
    pub last_run: Option<i64>,
    pub next_run: i64,
    pub timezone: Option<String>,
}

impl From<PeriodicEntry> for PeriodicTask {
    fn from(e: PeriodicEntry) -> Self {
        Self {
            name: e.name,
            task_name: e.task_name,
            cron_expr: e.cron_expr,
            args: e.args,
            kwargs: e.kwargs,
            queue: e.queue,
            enabled: e.enabled,
            last_run: e.last_run,
            next_run: e.next_run,
            timezone: e.timezone,
        }
    }
}

impl RedisStorage {
    /// Register or update a periodic task by name.
    pub fn register_periodic(&self, task: &NewPeriodicTask) -> Result<()> {
        let mut conn = self.conn()?;

        let entry = PeriodicEntry {
            name: task.name.clone(),
            task_name: task.task_name.clone(),
            cron_expr: task.cron_expr.clone(),
            args: task.args.clone(),
            kwargs: task.kwargs.clone(),
            queue: task.queue.clone(),
            enabled: task.enabled,
            last_run: None,
            next_run: task.next_run,
            timezone: task.timezone.clone(),
        };

        let json = serde_json::to_string(&entry)?;

        let pkey = self.key(&["periodic", &task.name]);
        let due_key = self.key(&["periodic", "due"]);

        let pipe = &mut redis::pipe();
        pipe.set(&pkey, &json);
        if entry.enabled {
            pipe.zadd(&due_key, &task.name, task.next_run as f64);
        }
        pipe.query::<()>(&mut conn).map_err(map_err)?;

        Ok(())
    }

    /// Enabled periodic tasks whose `next_run` is due at `now` (Unix
    /// milliseconds).
    pub fn get_due_periodic(&self, now: i64) -> Result<Vec<PeriodicTask>> {
        let mut conn = self.conn()?;
        let due_key = self.key(&["periodic", "due"]);

        let names: Vec<String> = conn
            .zrangebyscore(&due_key, "-inf", now as f64)
            .map_err(map_err)?;

        let mut rows = Vec::new();
        for name in names {
            let pkey = self.key(&["periodic", &name]);
            let data: Option<String> = conn.get(&pkey).map_err(map_err)?;
            if let Some(d) = data {
                let entry: PeriodicEntry = serde_json::from_str(&d)?;
                if entry.enabled {
                    rows.push(PeriodicTask::from(entry));
                }
            }
        }

        Ok(rows)
    }

    /// Advance a periodic task's schedule after it fires.
    pub fn update_periodic_schedule(&self, name: &str, last_run: i64, next_run: i64) -> Result<()> {
        let mut conn = self.conn()?;
        let pkey = self.key(&["periodic", name]);

        let data: Option<String> = conn.get(&pkey).map_err(map_err)?;
        if let Some(d) = data {
            let mut entry: PeriodicEntry = serde_json::from_str(&d)?;
            entry.last_run = Some(last_run);
            entry.next_run = next_run;

            let json = serde_json::to_string(&entry)?;

            let due_key = self.key(&["periodic", "due"]);
            let pipe = &mut redis::pipe();
            pipe.set(&pkey, &json);
            pipe.zadd(&due_key, name, next_run as f64);
            pipe.query::<()>(&mut conn).map_err(map_err)?;
        }

        Ok(())
    }

    /// List all registered periodic tasks, enabled or paused. Scans the
    /// `periodic:*` keyspace (like `list_dead`/`list_workers`) so there is no
    /// secondary index to keep consistent — tasks registered by any version are
    /// listed.
    pub fn list_periodic(&self) -> Result<Vec<PeriodicTask>> {
        let mut conn = self.conn()?;
        let pattern = self.key(&["periodic", "*"]);
        // The due sorted-set shares the `periodic:` namespace but is not a task
        // JSON blob, so skip it (a `GET` on it would be a WRONGTYPE error).
        let due_key = self.key(&["periodic", "due"]);

        let mut rows = Vec::new();
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
                if key == due_key {
                    continue;
                }
                let data: Option<String> = conn.get(&key).map_err(map_err)?;
                if let Some(d) = data {
                    let entry: PeriodicEntry = serde_json::from_str(&d)?;
                    rows.push(PeriodicTask::from(entry));
                }
            }

            cursor = next_cursor;
            if cursor == 0 {
                break;
            }
        }

        Ok(rows)
    }

    /// Remove a periodic task. Returns false if no task had that name.
    pub fn delete_periodic(&self, name: &str) -> Result<bool> {
        let mut conn = self.conn()?;
        let pkey = self.key(&["periodic", name]);
        let due_key = self.key(&["periodic", "due"]);

        let pipe = &mut redis::pipe();
        pipe.del(&pkey);
        pipe.zrem(&due_key, name);
        let (deleted, _zrem): (i64, i64) = pipe.query(&mut conn).map_err(map_err)?;

        Ok(deleted > 0)
    }

    /// Pause (false) or resume (true) a periodic task by toggling `enabled`.
    /// Returns false if no task had that name.
    pub fn set_periodic_enabled(&self, name: &str, enabled: bool) -> Result<bool> {
        let mut conn = self.conn()?;
        let pkey = self.key(&["periodic", name]);

        let data: Option<String> = conn.get(&pkey).map_err(map_err)?;
        let Some(d) = data else {
            return Ok(false);
        };

        let mut entry: PeriodicEntry = serde_json::from_str(&d)?;
        entry.enabled = enabled;

        let json = serde_json::to_string(&entry)?;
        let due_key = self.key(&["periodic", "due"]);

        let pipe = &mut redis::pipe();
        pipe.set(&pkey, &json);
        if enabled {
            pipe.zadd(&due_key, name, entry.next_run as f64);
        } else {
            pipe.zrem(&due_key, name);
        }
        pipe.query::<()>(&mut conn).map_err(map_err)?;

        Ok(true)
    }
}
