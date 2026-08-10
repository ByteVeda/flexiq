//! Management (mutating) methods on `JsQueue`. List/purge methods scan whole
//! tables, so they are async and offload to the blocking pool; single-row
//! operations stay sync.

use std::collections::HashMap;

use napi::bindgen_prelude::{spawn_blocking, Result};
use napi_derive::napi;
use taskito_core::job::{now_millis, NewJob};
use taskito_core::Storage;

use super::JsQueue;
use crate::config::RetentionInput;
use crate::convert::{dead_job_to_js, JsDeadJob, JsDeadJobPage};
use crate::error::{invalid_arg, join_to_napi_err, non_negative, to_napi_err};

const DEFAULT_LIMIT: i64 = 50;

#[napi]
impl JsQueue {
    /// List dead-letter entries (paginated).
    #[napi]
    pub async fn dead_letters(
        &self,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<JsDeadJob>> {
        let limit = non_negative(limit.unwrap_or(DEFAULT_LIMIT), "limit")?;
        let offset = non_negative(offset.unwrap_or(0), "offset")?;
        let storage = self.storage.clone();
        let namespace = self.namespace.clone();
        spawn_blocking(move || {
            let dead = storage
                .list_dead(limit, offset, namespace.as_deref())
                .map_err(to_napi_err)?;
            Ok(dead.into_iter().map(dead_job_to_js).collect())
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Keyset-paginated [`JsQueue::dead_letters`], ordered by failed time. Pass
    /// a page's `nextCursor` back as `after`; `null` means the last page.
    #[napi]
    pub async fn dead_letters_after(
        &self,
        limit: Option<i64>,
        after: Option<String>,
    ) -> Result<JsDeadJobPage> {
        let limit = non_negative(limit.unwrap_or(DEFAULT_LIMIT), "limit")?;
        let storage = self.storage.clone();
        let namespace = self.namespace.clone();
        spawn_blocking(move || {
            // A malformed cursor is a bad request, not a reason to silently
            // restart from the first page.
            let cursor = after
                .as_deref()
                .map(taskito_core::storage::cursor::decode_cursor)
                .transpose()
                .map_err(to_napi_err)?;
            let dead = storage
                .list_dead_after(limit, cursor, namespace.as_deref())
                .map_err(to_napi_err)?;
            let next_cursor =
                taskito_core::storage::cursor::next_cursor(&dead, limit, |d| (d.failed_at, &d.id));
            Ok(JsDeadJobPage {
                items: dead.into_iter().map(dead_job_to_js).collect(),
                next_cursor,
            })
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// List dead-letter entries for a single task (paginated, newest first).
    #[napi]
    pub async fn dead_letters_by_task(
        &self,
        task_name: String,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<JsDeadJob>> {
        let limit = non_negative(limit.unwrap_or(DEFAULT_LIMIT), "limit")?;
        let offset = non_negative(offset.unwrap_or(0), "offset")?;
        let storage = self.storage.clone();
        let namespace = self.namespace.clone();
        spawn_blocking(move || {
            let dead = storage
                .list_dead_by_task(&task_name, limit, offset, namespace.as_deref())
                .map_err(to_napi_err)?;
            Ok(dead.into_iter().map(dead_job_to_js).collect())
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Delete every dead-letter entry for a task. Returns the count removed.
    #[napi]
    pub async fn purge_dead_by_task(&self, task_name: String) -> Result<i64> {
        let storage = self.storage.clone();
        spawn_blocking(move || {
            storage
                .purge_dead_by_task(&task_name)
                .map(|n| n as i64)
                .map_err(to_napi_err)
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Re-enqueue a copy of an existing job and record it in the replay
    /// history. Returns the new job id.
    #[napi]
    pub async fn replay_job(&self, job_id: String) -> Result<String> {
        let storage = self.storage.clone();
        let namespace = self.namespace.clone();
        spawn_blocking(move || {
            let original = storage
                .get_job(&job_id, namespace.as_deref())
                .map_err(to_napi_err)?
                .ok_or_else(|| invalid_arg(format!("Job not found: {job_id}")))?;
            let new_job = NewJob {
                queue: original.queue.clone(),
                task_name: original.task_name.clone(),
                payload: original.payload.clone(),
                priority: original.priority,
                scheduled_at: now_millis(),
                max_retries: original.max_retries,
                timeout_ms: original.timeout_ms,
                unique_key: None,
                metadata: Some(format!("{{\"replayed_from\":\"{job_id}\"}}")),
                notes: original.notes.clone(),
                depends_on: Vec::new(),
                expires_at: None,
                result_ttl_ms: original.result_ttl_ms,
                namespace: original.namespace.clone(),
            };
            let job = storage.enqueue(new_job).map_err(to_napi_err)?;
            // Best-effort audit row — a history write must not fail the replay.
            let _ = storage.record_replay(
                &job_id,
                &job.id,
                original.result.as_deref(),
                None,
                original.error.as_deref(),
                None,
            );
            Ok(job.id)
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Re-enqueue a dead-letter entry. Returns the new job id.
    #[napi]
    pub fn retry_dead(&self, dead_id: String) -> Result<String> {
        self.storage
            .retry_dead(&dead_id, self.namespace.as_deref())
            .map_err(to_napi_err)
    }

    /// Force a stuck Running job back to Pending, releasing its execution claim.
    /// Returns false when the job is missing or is not Running.
    #[napi]
    pub fn requeue_job(&self, job_id: String) -> Result<bool> {
        self.storage
            .requeue_stuck(&job_id, now_millis())
            .map_err(to_napi_err)
    }

    /// Delete a dead-letter entry. Returns false if it didn't exist.
    #[napi]
    pub fn delete_dead(&self, dead_id: String) -> Result<bool> {
        self.storage
            .delete_dead(&dead_id, self.namespace.as_deref())
            .map_err(to_napi_err)
    }

    /// Purge dead-letter entries older than `older_than_ms`. Returns the count removed.
    #[napi]
    pub async fn purge_dead(&self, older_than_ms: i64) -> Result<i64> {
        let older_than_ms = non_negative(older_than_ms, "olderThanMs")?;
        let storage = self.storage.clone();
        spawn_blocking(move || {
            storage
                .purge_dead(older_than_ms)
                .map(|n| n as i64)
                .map_err(to_napi_err)
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Purge completed jobs older than `older_than_ms`. Returns the count removed.
    #[napi]
    pub async fn purge_completed(&self, older_than_ms: i64) -> Result<i64> {
        let older_than_ms = non_negative(older_than_ms, "olderThanMs")?;
        let storage = self.storage.clone();
        spawn_blocking(move || {
            storage
                .purge_completed(older_than_ms)
                .map(|n| n as i64)
                .map_err(to_napi_err)
        })
        .await
        .map_err(join_to_napi_err)?
    }

    /// Pause a queue — workers stop dispatching its jobs until resumed.
    #[napi]
    pub fn pause_queue(&self, queue: String) -> Result<()> {
        self.storage.pause_queue(&queue).map_err(to_napi_err)
    }

    /// Resume a paused queue.
    #[napi]
    pub fn resume_queue(&self, queue: String) -> Result<()> {
        self.storage.resume_queue(&queue).map_err(to_napi_err)
    }

    /// List the names of currently-paused queues.
    #[napi]
    pub fn list_paused_queues(&self) -> Result<Vec<String>> {
        self.storage.list_paused_queues().map_err(to_napi_err)
    }

    /// Read a key/value setting (the shared KV store), or `null`.
    #[napi]
    pub fn get_setting(&self, key: String) -> Result<Option<String>> {
        self.storage.get_setting(&key).map_err(to_napi_err)
    }

    /// Write a key/value setting.
    #[napi]
    pub fn set_setting(&self, key: String, value: String) -> Result<()> {
        self.storage.set_setting(&key, &value).map_err(to_napi_err)
    }

    /// Write a key/value setting only if it still holds `expected`, where
    /// `null`/`undefined` means the key must be unset. Returns false when
    /// another writer got there first, so a read-modify-write caller can
    /// re-read and retry instead of overwriting the edit it never saw.
    #[napi]
    pub fn set_setting_if(
        &self,
        key: String,
        expected: Option<String>,
        value: String,
    ) -> Result<bool> {
        self.storage
            .set_setting_if(&key, expected.as_deref(), &value)
            .map_err(to_napi_err)
    }

    /// Delete a setting. Returns false if it didn't exist.
    #[napi]
    pub fn delete_setting(&self, key: String) -> Result<bool> {
        self.storage.delete_setting(&key).map_err(to_napi_err)
    }

    /// All settings as a key/value map.
    #[napi]
    pub fn list_settings(&self) -> Result<HashMap<String, String>> {
        self.storage.list_settings().map_err(to_napi_err)
    }

    /// The lowest contract level a process may speak and still open this
    /// storage. See `BINDING_CONTRACT.md`.
    #[napi]
    pub fn min_contract(&self) -> Result<u32> {
        taskito_core::min_contract(&self.storage).map_err(to_napi_err)
    }

    /// Raise or lower that floor. Rejects a level this build does not itself
    /// speak, which would lock the caller out of its own storage.
    #[napi]
    pub fn set_min_contract(&self, level: u32) -> Result<()> {
        taskito_core::set_min_contract(&self.storage, level).map_err(to_napi_err)
    }

    /// The retention windows the elected cleaner last published for this
    /// queue's namespace, as a JSON document, or `null` if no worker has swept
    /// yet. See `BINDING_CONTRACT.md`.
    #[napi]
    pub fn effective_retention(&self) -> Result<Option<String>> {
        taskito_core::scheduler::retention::read_effective_retention_json(
            &self.storage,
            self.namespace.as_deref(),
        )
        .map_err(to_napi_err)
    }

    /// Count what a retention purge would delete right now, without deleting
    /// anything. Returns the preview as a JSON document. See
    /// `BINDING_CONTRACT.md`.
    ///
    /// With no `retention` argument the preview follows the policy the elected
    /// cleaner reported for this namespace (recommended defaults only when no
    /// cleaner has swept yet) — retention config lives in the worker here, so
    /// the report is the policy that actually governs the deletes. Pass
    /// candidate windows to size a window before setting it — without
    /// reconfiguring a worker. An all-unset input previews a disabled policy.
    /// Async: the counts scan every history table.
    #[napi]
    pub async fn dry_run_retention(&self, retention: Option<RetentionInput>) -> Result<String> {
        let config = retention.map(|r| r.to_config());
        let storage = self.storage.clone();
        let namespace = self.namespace.clone();
        spawn_blocking(move || {
            match config {
                Some(config) => taskito_core::scheduler::retention::dry_run_json(
                    &storage,
                    Some(&config),
                    None,
                    namespace.as_deref(),
                    now_millis(),
                ),
                None => taskito_core::scheduler::retention::dry_run_reported_json(
                    &storage,
                    namespace.as_deref(),
                    now_millis(),
                ),
            }
            .map_err(to_napi_err)
        })
        .await
        .map_err(join_to_napi_err)?
    }
}
