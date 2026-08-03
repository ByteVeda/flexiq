//! Redis implementation of `WorkflowStorage`.
//!
//! ## Key layout (all keys are namespaced by `RedisStorage.prefix + "wf:"`)
//!
//! - `wf:def:{id}` (HASH) — definition fields (`id`, `name`, `version`,
//!   `dag_data` (binary), `step_metadata` (JSON), `created_at`).
//! - `wf:def:by_name:{name}` (ZSET, score=version, member=id) — index for
//!   `get_workflow_definition(name, version=None)` (highest version wins).
//! - `wf:run:{id}` (HASH) — run fields, including the `namespace` the owning
//!   store stamped. The run indexes below are *not* partitioned by it, so a
//!   scoped store filters after loading rather than seeking.
//! - `wf:runs:all` (ZSET, score=created_at, member=id) — unfiltered list.
//! - `wf:runs:by_state:{state}` (ZSET, score=created_at, member=id) — state filter.
//! - `wf:runs:by_def:{definition_id}` (ZSET, score=created_at, member=id).
//! - `wf:node:{run_id}:{node_name}` (HASH) — node fields.
//! - `wf:run_nodes:{run_id}` (SET, members=node_name) — enumeration index.
//! - `wf:children:{parent_run_id}` (SET, members=child_run_id) — sub-workflow
//!   index for `get_child_workflow_runs`.
//!
//! ## Atomicity
//!
//! Multi-write operations use `redis::pipe().atomic()` (a `MULTI`/`EXEC`
//! pipeline) so insertions of a run/node and its index entries are committed
//! together. `finalize_fan_out_parent` uses a Lua script (`FINALIZE_FAN_OUT`)
//! so the read-current-status-then-conditional-write happens atomically in
//! one round trip. `compute_ready_nodes` runs in Rust on the fetched nodes —
//! same backend-agnostic helper SQLite + Postgres use.

use std::collections::HashMap;

use redis::{Commands, FromRedisValue, Value};

use taskito_core::error::{QueueError, Result};
use taskito_core::storage::redis_backend::RedisStorage;

use crate::common::compute_ready_nodes;
use crate::storage::WorkflowStorage;
use crate::{
    StepMetadata, WorkflowDefinition, WorkflowNode, WorkflowNodeStatus, WorkflowRun, WorkflowState,
};

// ─── Key formatters ────────────────────────────────────────────────────────

fn k_def(prefix: &str, id: &str) -> String {
    format!("{prefix}wf:def:{id}")
}
fn k_def_by_name(prefix: &str, name: &str) -> String {
    format!("{prefix}wf:def:by_name:{name}")
}
fn k_run(prefix: &str, id: &str) -> String {
    format!("{prefix}wf:run:{id}")
}
fn k_runs_all(prefix: &str) -> String {
    format!("{prefix}wf:runs:all")
}
fn k_runs_by_state(prefix: &str, state: &str) -> String {
    format!("{prefix}wf:runs:by_state:{state}")
}
fn k_runs_by_def(prefix: &str, definition_id: &str) -> String {
    format!("{prefix}wf:runs:by_def:{definition_id}")
}
fn k_node(prefix: &str, run_id: &str, node_name: &str) -> String {
    format!("{prefix}wf:node:{run_id}:{node_name}")
}
fn k_run_nodes(prefix: &str, run_id: &str) -> String {
    format!("{prefix}wf:run_nodes:{run_id}")
}
fn k_children(prefix: &str, parent_run_id: &str) -> String {
    format!("{prefix}wf:children:{parent_run_id}")
}

// ─── Lua scripts ───────────────────────────────────────────────────────────

/// Atomic compare-and-swap to finalize a fan-out parent node exactly once.
///
/// `KEYS[1]` = `wf:node:{run_id}:{node_name}`
/// `ARGV[1]` = `"1"` for success, `"0"` for failure
/// `ARGV[2]` = `completed_at` (decimal string of i64)
/// `ARGV[3]` = error message (only used when `ARGV[1] == "0"`)
///
/// Returns `1` if this caller transitioned the node, `0` if it was already
/// in a terminal state.
const FINALIZE_FAN_OUT: &str = r#"
local status = redis.call('HGET', KEYS[1], 'status')
if status == 'completed' or status == 'failed' or status == 'skipped' or status == 'cache_hit' then
    return 0
end
if ARGV[1] == '1' then
    redis.call('HSET', KEYS[1], 'status', 'completed', 'completed_at', ARGV[2])
    redis.call('HDEL', KEYS[1], 'error')
else
    redis.call('HSET', KEYS[1], 'status', 'failed', 'error', ARGV[3])
end
return 1
"#;

/// Set hash fields only if the node already exists.
///
/// A bare `HSET` **creates** the key, so a setter aimed at a node that was
/// never created would mint a partial hash carrying only the written field —
/// which then fails `map_to_node` on the next read. `KEYS[1]` is the node hash;
/// `ARGV` is a flat `field, value, …` list. Returns 1 if written, 0 if absent.
const SET_NODE_FIELDS_IF_EXISTS: &str = r#"
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
for i = 1, #ARGV, 2 do
    redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
end
return 1
"#;

// ─── Redis storage handle ─────────────────────────────────────────────────

/// Workflow-aware wrapper around `RedisStorage`.
///
/// Clones share the same `redis::Client`; opening a connection per operation
/// is how the core `RedisStorage` works today, so we follow the same pattern.
#[derive(Clone)]
pub struct WorkflowRedisStorage {
    pub(crate) inner: RedisStorage,
    /// Full prefix used by core, ending with `':'`. We add `"wf:"` on top.
    prefix: String,
    /// Tenant every run this store creates is stamped with, and the only one
    /// it can read or mutate. `None` addresses every namespace.
    namespace: Option<String>,
}

impl WorkflowRedisStorage {
    /// Wrap an existing `RedisStorage`.
    ///
    /// Redis has no "schema" or "migrations" — the key layout is implicit.
    /// A round-trip `PING` happens inside `RedisStorage::new`, so by the time
    /// we receive it the connection is known good.
    pub fn new(inner: RedisStorage, namespace: Option<String>) -> Result<Self> {
        let prefix = inner.prefix().to_string();
        Ok(Self {
            inner,
            prefix,
            namespace,
        })
    }

    /// Access the underlying `RedisStorage`.
    pub fn inner(&self) -> &RedisStorage {
        &self.inner
    }

    fn conn(&self) -> Result<redis::Connection> {
        self.inner
            .conn()
            .map_err(|e| QueueError::Other(format!("redis conn: {e}")))
    }

    /// Apply `fields` to a node hash, reporting a node that does not exist.
    ///
    /// Mirrors the Diesel setters' affected-row check: nothing to set means
    /// the node was never created, or its run is outside this store's
    /// namespace. Both answer `NodeNotFound`, so a scoped caller cannot tell
    /// them apart.
    fn set_node_fields(
        &self,
        run_id: &str,
        node_name: &str,
        fields: &[(&str, String)],
    ) -> Result<()> {
        if self.run_out_of_scope(run_id)? {
            return Err(node_not_found(run_id, node_name));
        }
        let mut conn = self.conn()?;
        let script = redis::Script::new(SET_NODE_FIELDS_IF_EXISTS);
        let mut invocation = script.key(k_node(&self.prefix, run_id, node_name));
        for (field, value) in fields {
            invocation.arg(*field).arg(value.as_str());
        }
        let written: i32 = invocation.invoke(&mut conn).map_err(into_other)?;
        if written == 1 {
            Ok(())
        } else {
            Err(node_not_found(run_id, node_name))
        }
    }

    /// Whether `run_id` is outside this store's namespace, in which case the
    /// caller must be answered as if the run did not exist.
    ///
    /// The node hashes carry no namespace of their own — a node is reachable
    /// only through its run — so every node operation gates on this. An
    /// unscoped store addresses every namespace and skips the round trip.
    fn run_out_of_scope(&self, run_id: &str) -> Result<bool> {
        let Some(ns) = self.namespace.as_deref() else {
            return Ok(false);
        };
        let mut conn = self.conn()?;
        let stored: Option<String> = conn
            .hget(k_run(&self.prefix, run_id), "namespace")
            .map_err(into_other)?;
        Ok(stored.as_deref() != Some(ns))
    }
}

// ─── Serialization helpers ─────────────────────────────────────────────────

fn into_other(e: impl std::fmt::Display) -> QueueError {
    QueueError::Other(e.to_string())
}

fn node_not_found(run_id: &str, node_name: &str) -> QueueError {
    crate::WorkflowError::NodeNotFound {
        run_id: run_id.to_string(),
        node_name: node_name.to_string(),
    }
    .into()
}

fn opt_i64(v: Option<i64>) -> Option<String> {
    v.map(|n| n.to_string())
}

fn parse_i64(s: &str) -> Result<i64> {
    s.parse::<i64>()
        .map_err(|e| QueueError::Serialization(format!("invalid i64 '{s}': {e}")))
}

fn parse_i32(s: &str) -> Result<i32> {
    s.parse::<i32>()
        .map_err(|e| QueueError::Serialization(format!("invalid i32 '{s}': {e}")))
}

fn hash_get<'a>(map: &'a HashMap<String, Value>, key: &str) -> Result<&'a Value> {
    map.get(key)
        .ok_or_else(|| QueueError::Serialization(format!("missing field '{key}'")))
}

fn val_to_string(v: &Value) -> Result<String> {
    String::from_redis_value(v.clone()).map_err(into_other)
}

fn val_to_bytes(v: &Value) -> Result<Vec<u8>> {
    Vec::<u8>::from_redis_value(v.clone()).map_err(into_other)
}

fn val_to_opt_string(v: &Value) -> Result<Option<String>> {
    match v {
        Value::Nil => Ok(None),
        other => Ok(Some(
            String::from_redis_value(other.clone()).map_err(into_other)?,
        )),
    }
}

/// Decode a Redis HASH (returned as a flat `[key, value, key, value, …]` bulk
/// reply or already as a map) into a `HashMap<String, Value>`.
fn hash_to_map(v: Value) -> Result<HashMap<String, Value>> {
    let mut out = HashMap::new();
    match v {
        Value::Array(items) => {
            let mut it = items.into_iter();
            while let (Some(k), Some(v)) = (it.next(), it.next()) {
                let key = String::from_redis_value(k).map_err(into_other)?;
                out.insert(key, v);
            }
        }
        Value::Map(entries) => {
            for (k, v) in entries {
                let key = String::from_redis_value(k).map_err(into_other)?;
                out.insert(key, v);
            }
        }
        Value::Nil => {}
        other => {
            return Err(QueueError::Serialization(format!(
                "unexpected HASH reply: {other:?}"
            )))
        }
    }
    Ok(out)
}

fn map_to_definition(map: HashMap<String, Value>) -> Result<WorkflowDefinition> {
    let id = val_to_string(hash_get(&map, "id")?)?;
    let name = val_to_string(hash_get(&map, "name")?)?;
    let version = parse_i32(&val_to_string(hash_get(&map, "version")?)?)?;
    let dag_data = val_to_bytes(hash_get(&map, "dag_data")?)?;
    let step_meta_json = val_to_string(hash_get(&map, "step_metadata")?)?;
    let created_at = parse_i64(&val_to_string(hash_get(&map, "created_at")?)?)?;
    let step_metadata: HashMap<String, StepMetadata> = serde_json::from_str(&step_meta_json)
        .map_err(|e| {
            QueueError::Serialization(format!("invalid workflow_definitions.step_metadata: {e}"))
        })?;
    Ok(WorkflowDefinition {
        id,
        name,
        version,
        dag_data,
        step_metadata,
        created_at,
    })
}

fn map_to_run(map: HashMap<String, Value>) -> Result<WorkflowRun> {
    let id = val_to_string(hash_get(&map, "id")?)?;
    let definition_id = val_to_string(hash_get(&map, "definition_id")?)?;
    let params = val_to_opt_string(map.get("params").unwrap_or(&Value::Nil))?;
    let state_str = val_to_string(hash_get(&map, "state")?)?;
    let state = WorkflowState::from_str_val(&state_str).unwrap_or(WorkflowState::Pending);
    let started_at = match map.get("started_at") {
        Some(v) if !matches!(v, Value::Nil) => Some(parse_i64(&val_to_string(v)?)?),
        _ => None,
    };
    let completed_at = match map.get("completed_at") {
        Some(v) if !matches!(v, Value::Nil) => Some(parse_i64(&val_to_string(v)?)?),
        _ => None,
    };
    let error = val_to_opt_string(map.get("error").unwrap_or(&Value::Nil))?;
    let parent_run_id = val_to_opt_string(map.get("parent_run_id").unwrap_or(&Value::Nil))?;
    let parent_node_name = val_to_opt_string(map.get("parent_node_name").unwrap_or(&Value::Nil))?;
    let created_at = parse_i64(&val_to_string(hash_get(&map, "created_at")?)?)?;
    Ok(WorkflowRun {
        id,
        definition_id,
        params,
        state,
        started_at,
        completed_at,
        error,
        parent_run_id,
        parent_node_name,
        created_at,
    })
}

fn map_to_node(map: HashMap<String, Value>) -> Result<WorkflowNode> {
    let id = val_to_string(hash_get(&map, "id")?)?;
    let run_id = val_to_string(hash_get(&map, "run_id")?)?;
    let node_name = val_to_string(hash_get(&map, "node_name")?)?;
    let job_id = val_to_opt_string(map.get("job_id").unwrap_or(&Value::Nil))?;
    let status_str = val_to_string(hash_get(&map, "status")?)?;
    let status =
        WorkflowNodeStatus::from_str_val(&status_str).unwrap_or(WorkflowNodeStatus::Pending);
    let result_hash = val_to_opt_string(map.get("result_hash").unwrap_or(&Value::Nil))?;
    let fan_out_count = match map.get("fan_out_count") {
        Some(v) if !matches!(v, Value::Nil) => Some(parse_i32(&val_to_string(v)?)?),
        _ => None,
    };
    let fan_in_data = val_to_opt_string(map.get("fan_in_data").unwrap_or(&Value::Nil))?;
    let started_at = match map.get("started_at") {
        Some(v) if !matches!(v, Value::Nil) => Some(parse_i64(&val_to_string(v)?)?),
        _ => None,
    };
    let completed_at = match map.get("completed_at") {
        Some(v) if !matches!(v, Value::Nil) => Some(parse_i64(&val_to_string(v)?)?),
        _ => None,
    };
    let error = val_to_opt_string(map.get("error").unwrap_or(&Value::Nil))?;
    let compensation_job_id =
        val_to_opt_string(map.get("compensation_job_id").unwrap_or(&Value::Nil))?;
    let compensation_started_at = match map.get("compensation_started_at") {
        Some(v) if !matches!(v, Value::Nil) => Some(parse_i64(&val_to_string(v)?)?),
        _ => None,
    };
    let compensation_completed_at = match map.get("compensation_completed_at") {
        Some(v) if !matches!(v, Value::Nil) => Some(parse_i64(&val_to_string(v)?)?),
        _ => None,
    };
    let compensation_error =
        val_to_opt_string(map.get("compensation_error").unwrap_or(&Value::Nil))?;
    Ok(WorkflowNode {
        id,
        run_id,
        node_name,
        job_id,
        status,
        result_hash,
        fan_out_count,
        fan_in_data,
        started_at,
        completed_at,
        error,
        compensation_job_id,
        compensation_started_at,
        compensation_completed_at,
        compensation_error,
    })
}

// ─── WorkflowStorage impl ──────────────────────────────────────────────────

impl WorkflowStorage for WorkflowRedisStorage {
    fn create_workflow_definition(&self, def: &WorkflowDefinition) -> Result<()> {
        let mut conn = self.conn()?;
        let meta_json = serde_json::to_string(&def.step_metadata).map_err(|e| {
            QueueError::Serialization(format!("failed to serialize step_metadata: {e}"))
        })?;
        let key = k_def(&self.prefix, &def.id);
        let by_name = k_def_by_name(&self.prefix, &def.name);

        redis::pipe()
            .atomic()
            .hset(&key, "id", &def.id)
            .hset(&key, "name", &def.name)
            .hset(&key, "version", def.version)
            .hset(&key, "dag_data", &def.dag_data)
            .hset(&key, "step_metadata", meta_json)
            .hset(&key, "created_at", def.created_at)
            .zadd(&by_name, &def.id, def.version)
            .query::<()>(&mut conn)
            .map_err(into_other)?;
        Ok(())
    }

    fn get_workflow_definition(
        &self,
        name: &str,
        version: Option<i32>,
    ) -> Result<Option<WorkflowDefinition>> {
        let mut conn = self.conn()?;
        let by_name = k_def_by_name(&self.prefix, name);

        let id: Option<String> = match version {
            Some(v) => {
                // Find the def id whose score equals `v`.
                let ids: Vec<String> = conn
                    .zrangebyscore(&by_name, v as f64, v as f64)
                    .map_err(into_other)?;
                ids.into_iter().next()
            }
            None => {
                // Highest version = highest score.
                let ids: Vec<String> = conn.zrevrange(&by_name, 0, 0).map_err(into_other)?;
                ids.into_iter().next()
            }
        };

        match id {
            Some(id) => self.get_workflow_definition_by_id(&id),
            None => Ok(None),
        }
    }

    fn get_workflow_definition_by_id(&self, id: &str) -> Result<Option<WorkflowDefinition>> {
        let mut conn = self.conn()?;
        let key = k_def(&self.prefix, id);
        let raw: Value = conn.hgetall(&key).map_err(into_other)?;
        let map = hash_to_map(raw)?;
        if map.is_empty() {
            return Ok(None);
        }
        Ok(Some(map_to_definition(map)?))
    }

    fn create_workflow_run(&self, run: &WorkflowRun) -> Result<()> {
        // A sub-workflow may only hang off a parent this store can see, and an
        // id already taken by a foreign run must not be overwritten — Redis
        // HSET has no primary key to refuse it the way the Diesel INSERT does.
        if let Some(parent) = run.parent_run_id.as_deref() {
            if self.run_out_of_scope(parent)? {
                return Err(QueueError::Other(format!(
                    "parent workflow run not found: {parent}"
                )));
            }
        }
        if self.namespace.is_some() && self.run_out_of_scope(&run.id)? {
            let mut probe = self.conn()?;
            let exists: bool = probe
                .exists(k_run(&self.prefix, &run.id))
                .map_err(into_other)?;
            if exists {
                return Err(QueueError::Other(format!(
                    "workflow run id already taken: {}",
                    run.id
                )));
            }
        }
        let mut conn = self.conn()?;
        let key = k_run(&self.prefix, &run.id);
        let all = k_runs_all(&self.prefix);
        let by_state = k_runs_by_state(&self.prefix, run.state.as_str());
        let by_def = k_runs_by_def(&self.prefix, &run.definition_id);

        let mut pipe = redis::pipe();
        let pipe = pipe.atomic();
        pipe.hset(&key, "id", &run.id);
        pipe.hset(&key, "definition_id", &run.definition_id);
        if let Some(p) = &run.params {
            pipe.hset(&key, "params", p);
        }
        pipe.hset(&key, "state", run.state.as_str());
        if let Some(s) = opt_i64(run.started_at) {
            pipe.hset(&key, "started_at", s);
        }
        if let Some(s) = opt_i64(run.completed_at) {
            pipe.hset(&key, "completed_at", s);
        }
        if let Some(e) = &run.error {
            pipe.hset(&key, "error", e);
        }
        if let Some(p) = &run.parent_run_id {
            pipe.hset(&key, "parent_run_id", p);
            // Track this run as a child for parent lookups.
            pipe.sadd(k_children(&self.prefix, p), &run.id);
        }
        if let Some(n) = &run.parent_node_name {
            pipe.hset(&key, "parent_node_name", n);
        }
        // Stamped from the store, not the caller: a run must land in the
        // tenant whose queue created it.
        if let Some(ns) = &self.namespace {
            pipe.hset(&key, "namespace", ns);
        }
        pipe.hset(&key, "created_at", run.created_at);
        pipe.zadd(&all, &run.id, run.created_at);
        pipe.zadd(&by_state, &run.id, run.created_at);
        pipe.zadd(&by_def, &run.id, run.created_at);

        pipe.query::<()>(&mut conn).map_err(into_other)?;
        Ok(())
    }

    fn get_workflow_run(&self, run_id: &str) -> Result<Option<WorkflowRun>> {
        let mut conn = self.conn()?;
        let key = k_run(&self.prefix, run_id);
        let raw: Value = conn.hgetall(&key).map_err(into_other)?;
        let map = hash_to_map(raw)?;
        if map.is_empty() {
            return Ok(None);
        }
        // Filtered off the hash already in hand rather than via
        // `run_out_of_scope`, which would repeat the read.
        if let Some(ns) = self.namespace.as_deref() {
            let stored = val_to_opt_string(map.get("namespace").unwrap_or(&Value::Nil))?;
            if stored.as_deref() != Some(ns) {
                return Ok(None);
            }
        }
        Ok(Some(map_to_run(map)?))
    }

    fn update_workflow_run_state(
        &self,
        run_id: &str,
        state: WorkflowState,
        error: Option<&str>,
    ) -> Result<()> {
        if self.run_out_of_scope(run_id)? {
            return Ok(());
        }
        let mut conn = self.conn()?;
        let key = k_run(&self.prefix, run_id);

        // Read the current state + created_at to maintain the state index.
        let current_state: Option<String> = conn.hget(&key, "state").map_err(into_other)?;
        let created_at: Option<i64> = conn.hget(&key, "created_at").map_err(into_other)?;

        let mut pipe = redis::pipe();
        let pipe = pipe.atomic();
        pipe.hset(&key, "state", state.as_str());
        match error {
            Some(e) => {
                pipe.hset(&key, "error", e);
            }
            None => {
                pipe.hdel(&key, "error");
            }
        }
        if let (Some(old), Some(ts)) = (current_state, created_at) {
            if old != state.as_str() {
                pipe.zrem(k_runs_by_state(&self.prefix, &old), run_id);
                pipe.zadd(k_runs_by_state(&self.prefix, state.as_str()), run_id, ts);
            }
        }
        pipe.query::<()>(&mut conn).map_err(into_other)?;
        Ok(())
    }

    fn set_workflow_run_started(&self, run_id: &str, started_at: i64) -> Result<()> {
        if self.run_out_of_scope(run_id)? {
            return Ok(());
        }
        let mut conn = self.conn()?;
        let key = k_run(&self.prefix, run_id);
        let created_at: Option<i64> = conn.hget(&key, "created_at").map_err(into_other)?;
        let prev_state: Option<String> = conn.hget(&key, "state").map_err(into_other)?;

        let mut pipe = redis::pipe();
        let pipe = pipe.atomic();
        pipe.hset(&key, "state", "running");
        pipe.hset(&key, "started_at", started_at);
        if let (Some(old), Some(ts)) = (prev_state, created_at) {
            if old != "running" {
                pipe.zrem(k_runs_by_state(&self.prefix, &old), run_id);
                pipe.zadd(k_runs_by_state(&self.prefix, "running"), run_id, ts);
            }
        }
        pipe.query::<()>(&mut conn).map_err(into_other)?;
        Ok(())
    }

    fn set_workflow_run_completed(&self, run_id: &str, completed_at: i64) -> Result<()> {
        if self.run_out_of_scope(run_id)? {
            return Ok(());
        }
        let mut conn = self.conn()?;
        let key = k_run(&self.prefix, run_id);
        conn.hset::<_, _, _, ()>(&key, "completed_at", completed_at)
            .map_err(into_other)?;
        Ok(())
    }

    fn list_workflow_runs(
        &self,
        definition_name: Option<&str>,
        state: Option<WorkflowState>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<WorkflowRun>> {
        let mut conn = self.conn()?;

        // Resolve the definition_id set if filtering by definition name.
        let def_id_for_filter: Option<String> = match definition_name {
            Some(name) => {
                let by_name = k_def_by_name(&self.prefix, name);
                // Use the highest-version definition for the name.
                let ids: Vec<String> = conn.zrevrange(&by_name, 0, 0).map_err(into_other)?;
                match ids.into_iter().next() {
                    Some(id) => Some(id),
                    None => return Ok(Vec::new()),
                }
            }
            None => None,
        };

        // Pick the most selective index.
        let index_key = match (def_id_for_filter.as_deref(), state) {
            (Some(def_id), _) => k_runs_by_def(&self.prefix, def_id),
            (None, Some(st)) => k_runs_by_state(&self.prefix, st.as_str()),
            (None, None) => k_runs_all(&self.prefix),
        };

        // ZREVRANGE for newest-first ordering (matches Diesel impl). The index
        // is not namespace-partitioned, so a scoped store cannot page in Redis
        // — it ranges the whole index and applies the window after filtering,
        // or the page comes back short. Unscoped keeps the server-side window.
        let scoped = self.namespace.is_some();
        let (start, stop) = if scoped {
            (0, -1)
        } else {
            (offset, if limit <= 0 { -1 } else { offset + limit - 1 })
        };
        let candidate_ids: Vec<String> = conn
            .zrevrange(&index_key, start as isize, stop as isize)
            .map_err(into_other)?;

        let mut runs = Vec::with_capacity(candidate_ids.len());
        for id in candidate_ids {
            // `get_workflow_run` is namespace-filtered, so a foreign run drops
            // out here as if the id were unknown.
            if let Some(run) = self.get_workflow_run(&id)? {
                // If both filters set, the definition index already constrained
                // the candidates; apply state filter in-memory.
                if let Some(st) = state {
                    if run.state != st {
                        continue;
                    }
                }
                runs.push(run);
            }
        }

        if scoped {
            let skip = offset.max(0) as usize;
            let take = if limit <= 0 {
                usize::MAX
            } else {
                limit as usize
            };
            return Ok(runs.into_iter().skip(skip).take(take).collect());
        }
        Ok(runs)
    }

    fn list_workflow_runs_after(
        &self,
        definition_name: Option<&str>,
        state: Option<WorkflowState>,
        limit: i64,
        after: Option<(i64, &str)>,
    ) -> Result<Vec<WorkflowRun>> {
        let mut conn = self.conn()?;

        let def_id_for_filter: Option<String> = match definition_name {
            Some(name) => {
                let by_name = k_def_by_name(&self.prefix, name);
                let ids: Vec<String> = conn.zrevrange(&by_name, 0, 0).map_err(into_other)?;
                match ids.into_iter().next() {
                    Some(id) => Some(id),
                    None => return Ok(Vec::new()),
                }
            }
            None => None,
        };

        let index_key = match (def_id_for_filter.as_deref(), state) {
            (Some(def_id), _) => k_runs_by_def(&self.prefix, def_id),
            (None, Some(st)) => k_runs_by_state(&self.prefix, st.as_str()),
            (None, None) => k_runs_all(&self.prefix),
        };

        // The `by_state` index is re-scored to the transition timestamp on every
        // state change, so its score is not `created_at` — the keyset must run
        // on the loaded run's real `created_at`, in memory. (Workflow-run counts
        // are modest; the Big-O win the Diesel backends get is not attempted
        // here, matching the Redis job listings.)
        let candidate_ids: Vec<String> = conn.zrevrange(&index_key, 0, -1).map_err(into_other)?;

        let mut runs = Vec::new();
        for id in candidate_ids {
            if let Some(run) = self.get_workflow_run(&id)? {
                if let Some(st) = state {
                    if run.state != st {
                        continue;
                    }
                }
                runs.push(run);
            }
        }

        runs.sort_by(|a, b| (b.created_at, &b.id).cmp(&(a.created_at, &a.id)));
        Ok(runs
            .into_iter()
            .filter(|r| match after {
                Some((cursor_created_at, cursor_id)) => {
                    (r.created_at, r.id.as_str()) < (cursor_created_at, cursor_id)
                }
                None => true,
            })
            .take(limit.max(0) as usize)
            .collect())
    }

    fn create_workflow_node(&self, node: &WorkflowNode) -> Result<()> {
        // A create against a run this store cannot see would silently drop the
        // node, so it reports the run missing instead.
        if self.run_out_of_scope(&node.run_id)? {
            return Err(crate::WorkflowError::RunNotFound(node.run_id.clone()).into());
        }
        let mut conn = self.conn()?;
        write_node_pipeline(&self.prefix, node, &mut conn)
    }

    fn create_workflow_nodes_batch(&self, nodes: &[WorkflowNode]) -> Result<()> {
        // Checked per distinct run, not just the first node: a mixed batch
        // would otherwise smuggle a foreign node in behind an in-scope one, in
        // the same atomic pipeline.
        let mut checked = std::collections::HashSet::new();
        for node in nodes {
            if checked.insert(node.run_id.as_str()) && self.run_out_of_scope(&node.run_id)? {
                return Err(crate::WorkflowError::RunNotFound(node.run_id.clone()).into());
            }
        }
        let mut conn = self.conn()?;
        let mut pipe = redis::pipe();
        let pipe = pipe.atomic();
        for node in nodes {
            let key = k_node(&self.prefix, &node.run_id, &node.node_name);
            pipe.hset(&key, "id", &node.id);
            pipe.hset(&key, "run_id", &node.run_id);
            pipe.hset(&key, "node_name", &node.node_name);
            if let Some(j) = &node.job_id {
                pipe.hset(&key, "job_id", j);
            }
            pipe.hset(&key, "status", node.status.as_str());
            if let Some(r) = &node.result_hash {
                pipe.hset(&key, "result_hash", r);
            }
            if let Some(c) = node.fan_out_count {
                pipe.hset(&key, "fan_out_count", c);
            }
            if let Some(f) = &node.fan_in_data {
                pipe.hset(&key, "fan_in_data", f);
            }
            if let Some(s) = opt_i64(node.started_at) {
                pipe.hset(&key, "started_at", s);
            }
            if let Some(c) = opt_i64(node.completed_at) {
                pipe.hset(&key, "completed_at", c);
            }
            if let Some(e) = &node.error {
                pipe.hset(&key, "error", e);
            }
            if let Some(j) = &node.compensation_job_id {
                pipe.hset(&key, "compensation_job_id", j);
            }
            if let Some(s) = opt_i64(node.compensation_started_at) {
                pipe.hset(&key, "compensation_started_at", s);
            }
            if let Some(c) = opt_i64(node.compensation_completed_at) {
                pipe.hset(&key, "compensation_completed_at", c);
            }
            if let Some(e) = &node.compensation_error {
                pipe.hset(&key, "compensation_error", e);
            }
            pipe.sadd(k_run_nodes(&self.prefix, &node.run_id), &node.node_name);
        }
        pipe.query::<()>(&mut conn).map_err(into_other)?;
        Ok(())
    }

    fn get_workflow_node(&self, run_id: &str, node_name: &str) -> Result<Option<WorkflowNode>> {
        if self.run_out_of_scope(run_id)? {
            return Ok(None);
        }
        let mut conn = self.conn()?;
        let key = k_node(&self.prefix, run_id, node_name);
        let raw: Value = conn.hgetall(&key).map_err(into_other)?;
        let map = hash_to_map(raw)?;
        if map.is_empty() {
            return Ok(None);
        }
        Ok(Some(map_to_node(map)?))
    }

    fn get_workflow_nodes(&self, run_id: &str) -> Result<Vec<WorkflowNode>> {
        if self.run_out_of_scope(run_id)? {
            return Ok(Vec::new());
        }
        let mut conn = self.conn()?;
        let names: Vec<String> = conn
            .smembers(k_run_nodes(&self.prefix, run_id))
            .map_err(into_other)?;
        if names.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipe = redis::pipe();
        for name in &names {
            pipe.hgetall(k_node(&self.prefix, run_id, name));
        }
        let raws: Vec<Value> = pipe.query(&mut conn).map_err(into_other)?;
        let mut nodes = Vec::with_capacity(raws.len());
        for raw in raws {
            let map = hash_to_map(raw)?;
            if !map.is_empty() {
                nodes.push(map_to_node(map)?);
            }
        }
        Ok(nodes)
    }

    fn update_workflow_node_status(
        &self,
        run_id: &str,
        node_name: &str,
        status: WorkflowNodeStatus,
    ) -> Result<()> {
        self.set_node_fields(
            run_id,
            node_name,
            &[("status", status.as_str().to_string())],
        )
    }

    fn set_workflow_node_job(&self, run_id: &str, node_name: &str, job_id: &str) -> Result<()> {
        self.set_node_fields(run_id, node_name, &[("job_id", job_id.to_string())])
    }

    fn set_workflow_node_started(
        &self,
        run_id: &str,
        node_name: &str,
        started_at: i64,
    ) -> Result<()> {
        self.set_node_fields(
            run_id,
            node_name,
            &[
                ("status", "running".to_string()),
                ("started_at", started_at.to_string()),
            ],
        )
    }

    fn set_workflow_node_completed(
        &self,
        run_id: &str,
        node_name: &str,
        completed_at: i64,
        result_hash: Option<&str>,
    ) -> Result<()> {
        let mut fields = vec![
            ("status", "completed".to_string()),
            ("completed_at", completed_at.to_string()),
        ];
        if let Some(rh) = result_hash {
            fields.push(("result_hash", rh.to_string()));
        }
        self.set_node_fields(run_id, node_name, &fields)
    }

    fn set_workflow_node_error(&self, run_id: &str, node_name: &str, error: &str) -> Result<()> {
        self.set_node_fields(
            run_id,
            node_name,
            &[
                ("status", "failed".to_string()),
                ("error", error.to_string()),
            ],
        )
    }

    fn get_ready_workflow_nodes(&self, run_id: &str, dag_json: &str) -> Result<Vec<WorkflowNode>> {
        let nodes = self.get_workflow_nodes(run_id)?;
        compute_ready_nodes(nodes, dag_json)
    }

    fn set_workflow_node_fan_out_count(
        &self,
        run_id: &str,
        node_name: &str,
        count: i32,
    ) -> Result<()> {
        self.set_node_fields(
            run_id,
            node_name,
            &[
                ("fan_out_count", count.to_string()),
                ("status", "running".to_string()),
            ],
        )
    }

    fn set_workflow_node_running(
        &self,
        run_id: &str,
        node_name: &str,
        started_at: i64,
    ) -> Result<()> {
        // Same wire effect as set_workflow_node_started for the Redis backend.
        self.set_workflow_node_started(run_id, node_name, started_at)
    }

    fn finalize_fan_out_parent(
        &self,
        run_id: &str,
        node_name: &str,
        succeeded: bool,
        error: Option<&str>,
        completed_at: i64,
    ) -> Result<bool> {
        if self.run_out_of_scope(run_id)? {
            return Ok(false);
        }
        let mut conn = self.conn()?;
        let key = k_node(&self.prefix, run_id, node_name);
        let succeeded_arg = if succeeded { "1" } else { "0" };
        let error_arg = error.unwrap_or("fan-out child failed");

        let res: i32 = redis::Script::new(FINALIZE_FAN_OUT)
            .key(key)
            .arg(succeeded_arg)
            .arg(completed_at)
            .arg(error_arg)
            .invoke(&mut conn)
            .map_err(into_other)?;
        Ok(res == 1)
    }

    fn get_workflow_nodes_by_prefix(
        &self,
        run_id: &str,
        prefix: &str,
    ) -> Result<Vec<WorkflowNode>> {
        let all = self.get_workflow_nodes(run_id)?;
        Ok(all
            .into_iter()
            .filter(|n| n.node_name.starts_with(prefix))
            .collect())
    }

    fn get_child_workflow_runs(&self, parent_run_id: &str) -> Result<Vec<WorkflowRun>> {
        let mut conn = self.conn()?;
        let child_ids: Vec<String> = conn
            .smembers(k_children(&self.prefix, parent_run_id))
            .map_err(into_other)?;
        let mut runs = Vec::with_capacity(child_ids.len());
        for id in child_ids {
            if let Some(run) = self.get_workflow_run(&id)? {
                runs.push(run);
            }
        }
        Ok(runs)
    }

    fn set_workflow_node_compensation_job(
        &self,
        run_id: &str,
        node_name: &str,
        compensation_job_id: &str,
        started_at: i64,
    ) -> Result<()> {
        self.set_node_fields(
            run_id,
            node_name,
            &[
                ("status", "compensating".to_string()),
                ("compensation_job_id", compensation_job_id.to_string()),
                ("compensation_started_at", started_at.to_string()),
            ],
        )
    }

    fn set_workflow_node_compensated(
        &self,
        run_id: &str,
        node_name: &str,
        completed_at: i64,
    ) -> Result<()> {
        self.set_node_fields(
            run_id,
            node_name,
            &[
                ("status", "compensated".to_string()),
                ("compensation_completed_at", completed_at.to_string()),
            ],
        )
    }

    fn set_workflow_node_compensation_failed(
        &self,
        run_id: &str,
        node_name: &str,
        error: &str,
        completed_at: i64,
    ) -> Result<()> {
        self.set_node_fields(
            run_id,
            node_name,
            &[
                ("status", "compensation_failed".to_string()),
                ("compensation_completed_at", completed_at.to_string()),
                ("compensation_error", error.to_string()),
            ],
        )
    }
}

fn write_node_pipeline(
    prefix: &str,
    node: &WorkflowNode,
    conn: &mut redis::Connection,
) -> Result<()> {
    let key = k_node(prefix, &node.run_id, &node.node_name);
    let mut pipe = redis::pipe();
    let pipe = pipe.atomic();
    pipe.hset(&key, "id", &node.id);
    pipe.hset(&key, "run_id", &node.run_id);
    pipe.hset(&key, "node_name", &node.node_name);
    if let Some(j) = &node.job_id {
        pipe.hset(&key, "job_id", j);
    }
    pipe.hset(&key, "status", node.status.as_str());
    if let Some(r) = &node.result_hash {
        pipe.hset(&key, "result_hash", r);
    }
    if let Some(c) = node.fan_out_count {
        pipe.hset(&key, "fan_out_count", c);
    }
    if let Some(f) = &node.fan_in_data {
        pipe.hset(&key, "fan_in_data", f);
    }
    if let Some(s) = opt_i64(node.started_at) {
        pipe.hset(&key, "started_at", s);
    }
    if let Some(c) = opt_i64(node.completed_at) {
        pipe.hset(&key, "completed_at", c);
    }
    if let Some(e) = &node.error {
        pipe.hset(&key, "error", e);
    }
    if let Some(j) = &node.compensation_job_id {
        pipe.hset(&key, "compensation_job_id", j);
    }
    if let Some(s) = opt_i64(node.compensation_started_at) {
        pipe.hset(&key, "compensation_started_at", s);
    }
    if let Some(c) = opt_i64(node.compensation_completed_at) {
        pipe.hset(&key, "compensation_completed_at", c);
    }
    if let Some(e) = &node.compensation_error {
        pipe.hset(&key, "compensation_error", e);
    }
    pipe.sadd(k_run_nodes(prefix, &node.run_id), &node.node_name);
    pipe.query::<()>(conn).map_err(into_other)?;
    Ok(())
}
