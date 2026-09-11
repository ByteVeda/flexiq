//! The periodic-task registry.
//!
//! A schedule is identified by `(namespace, name)` (#918), so the namespace
//! rides in the key — `periodic:<ns-segment>:<name>`, using the same
//! length-prefixed [`namespace_segment`](RedisStorage::namespace_segment)
//! encoding the `unique_key` pointer got in #773. The due sorted set stays a
//! single index so an unscoped scheduler reads every tenant's schedules in one
//! call, and its members are **whole keys** rather than bare names: a member is
//! then something to `GET`, never something to parse back into a namespace and
//! a name.
//!
//! # Rows written before this
//!
//! Pre-#918 rows live at `periodic:<name>` and are orphaned by the rename, not
//! migrated — Redis has no schema to migrate, and this follows what #773 did to
//! the `unique_key` pointer. They are inert rather than merely unreachable:
//!
//! - a legacy due member is a bare name, which is not a key under the periodic
//!   root, so `get_due_periodic` skips it without a read. Nothing fires, which
//!   matters — reading one *would* find the legacy key, fire it, and then write
//!   the advance to the new key, leaving the old `next_run` in place to fire
//!   again on the next tick, forever;
//! - `list_periodic` skips any key that is not the key its own
//!   `(namespace, name)` would compute, which is exactly the set of legacy
//!   rows.
//!
//! A worker re-registers its declared schedules at startup, so a code-declared
//! periodic comes back on the first boot after the upgrade. An operator can
//! `DEL` what is left.

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
    /// Absent in documents written before #918, which decode as the default
    /// namespace — and are then skipped, because their key is the pre-#918 one.
    #[serde(default)]
    pub namespace: Option<String>,
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
            namespace: e.namespace,
        }
    }
}

impl RedisStorage {
    /// The key one schedule lives at.
    fn periodic_key(&self, namespace: Option<&str>, name: &str) -> String {
        self.key(&["periodic", &Self::namespace_segment(namespace), name])
    }

    /// The one due index, shared by every namespace. Its members are whole
    /// schedule keys.
    fn periodic_due_key(&self) -> String {
        self.key(&["periodic", "due"])
    }

    /// The prefix every schedule key starts with. A due member that does not is
    /// a pre-#918 bare name, and reading it would reach a key this module does
    /// not own.
    fn periodic_root(&self) -> String {
        self.key(&["periodic", ""])
    }

    /// Whether `key` is the key `entry`'s own identity would compute. A stored
    /// document that does not address itself was written before #918.
    fn addresses_itself(&self, key: &str, entry: &PeriodicEntry) -> bool {
        key == self.periodic_key(entry.namespace.as_deref(), &entry.name)
    }

    /// Register or update the schedule named by `(namespace, name)`.
    pub fn register_periodic(&self, task: &NewPeriodicTask) -> Result<()> {
        let mut conn = self.conn()?;

        let namespace = task.namespace.as_deref();
        let pkey = self.periodic_key(namespace, &task.name);
        let due_key = self.periodic_due_key();

        // Re-registering must not forget when the task last fired — the Diesel
        // backends leave `last_run` off the changeset for the same reason.
        let existing: Option<String> = conn.get(&pkey).map_err(map_err)?;
        let last_run = match existing {
            Some(d) => serde_json::from_str::<PeriodicEntry>(&d)?.last_run,
            None => None,
        };

        let entry = PeriodicEntry {
            name: task.name.clone(),
            task_name: task.task_name.clone(),
            cron_expr: task.cron_expr.clone(),
            args: task.args.clone(),
            kwargs: task.kwargs.clone(),
            queue: task.queue.clone(),
            enabled: task.enabled,
            last_run,
            next_run: task.next_run,
            timezone: task.timezone.clone(),
            namespace: task.namespace.clone(),
        };

        let json = serde_json::to_string(&entry)?;

        let pipe = &mut redis::pipe();
        pipe.set(&pkey, &json);
        if entry.enabled {
            pipe.zadd(&due_key, &pkey, task.next_run as f64);
        } else {
            // A re-registration that pauses the task has to pull it out of the
            // due index, or the old membership keeps firing it.
            pipe.zrem(&due_key, &pkey);
        }
        pipe.query::<()>(&mut conn).map_err(map_err)?;

        Ok(())
    }

    /// Enabled schedules due at `now` (Unix milliseconds). `namespace` is a
    /// filter, not an address: `None` reads every namespace, because a
    /// scheduler running unscoped fires every tenant's schedules.
    pub fn get_due_periodic(&self, now: i64, namespace: Option<&str>) -> Result<Vec<PeriodicTask>> {
        let mut conn = self.conn()?;
        let due_key = self.periodic_due_key();
        let root = self.periodic_root();

        let keys: Vec<String> = conn
            .zrangebyscore(&due_key, "-inf", now as f64)
            .map_err(map_err)?;

        let mut rows = Vec::new();
        for key in keys {
            if !key.starts_with(&root) {
                continue;
            }
            let data: Option<String> = conn.get(&key).map_err(map_err)?;
            if let Some(d) = data {
                let entry: PeriodicEntry = serde_json::from_str(&d)?;
                if !entry.enabled || !self.addresses_itself(&key, &entry) {
                    continue;
                }
                if namespace.is_some() && entry.namespace.as_deref() != namespace {
                    continue;
                }
                rows.push(PeriodicTask::from(entry));
            }
        }

        Ok(rows)
    }

    /// Advance a schedule after it fires.
    pub fn update_periodic_schedule(
        &self,
        name: &str,
        last_run: i64,
        next_run: i64,
        namespace: Option<&str>,
    ) -> Result<()> {
        let mut conn = self.conn()?;
        let pkey = self.periodic_key(namespace, name);

        let data: Option<String> = conn.get(&pkey).map_err(map_err)?;
        if let Some(d) = data {
            let mut entry: PeriodicEntry = serde_json::from_str(&d)?;
            entry.last_run = Some(last_run);
            entry.next_run = next_run;

            let json = serde_json::to_string(&entry)?;

            let due_key = self.periodic_due_key();
            let pipe = &mut redis::pipe();
            pipe.set(&pkey, &json);
            pipe.zadd(&due_key, &pkey, next_run as f64);
            pipe.query::<()>(&mut conn).map_err(map_err)?;
        }

        Ok(())
    }

    /// Every schedule registered in `namespace`, enabled or paused. Scans the
    /// `periodic:*` keyspace (like `list_dead`/`list_workers`) so there is no
    /// secondary index to keep consistent.
    pub fn list_periodic(&self, namespace: Option<&str>) -> Result<Vec<PeriodicTask>> {
        let mut conn = self.conn()?;
        let pattern = self.key(&["periodic", "*"]);
        // The due sorted set shares the `periodic:` namespace but is not a task
        // JSON blob, so skip it (a `GET` on it would be a WRONGTYPE error).
        let due_key = self.periodic_due_key();

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
                    if !self.addresses_itself(&key, &entry) {
                        continue;
                    }
                    if entry.namespace.as_deref() != namespace {
                        continue;
                    }
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

    /// Remove a schedule. False when `namespace` had none by that name,
    /// including when another namespace does.
    pub fn delete_periodic(&self, name: &str, namespace: Option<&str>) -> Result<bool> {
        let mut conn = self.conn()?;
        let pkey = self.periodic_key(namespace, name);
        let due_key = self.periodic_due_key();

        let pipe = &mut redis::pipe();
        pipe.del(&pkey);
        pipe.zrem(&due_key, &pkey);
        let (deleted, _zrem): (i64, i64) = pipe.query(&mut conn).map_err(map_err)?;

        Ok(deleted > 0)
    }

    /// Pause (false) or resume (true) a schedule. False when `namespace` had
    /// none by that name.
    pub fn set_periodic_enabled(
        &self,
        name: &str,
        enabled: bool,
        namespace: Option<&str>,
    ) -> Result<bool> {
        let mut conn = self.conn()?;
        let pkey = self.periodic_key(namespace, name);

        let data: Option<String> = conn.get(&pkey).map_err(map_err)?;
        let Some(d) = data else {
            return Ok(false);
        };

        let mut entry: PeriodicEntry = serde_json::from_str(&d)?;
        entry.enabled = enabled;

        let json = serde_json::to_string(&entry)?;
        let due_key = self.periodic_due_key();

        let pipe = &mut redis::pipe();
        pipe.set(&pkey, &json);
        if enabled {
            pipe.zadd(&due_key, &pkey, entry.next_run as f64);
        } else {
            pipe.zrem(&due_key, &pkey);
        }
        pipe.query::<()>(&mut conn).map_err(map_err)?;

        Ok(true)
    }
}
