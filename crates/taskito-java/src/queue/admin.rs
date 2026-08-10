//! Mutating administration entry points for `NativeQueue`.

use jni::objects::{JClass, JString};
use jni::sys::{jboolean, jint, jlong, jobjectArray, jstring, JNI_FALSE};
use jni::JNIEnv;
use taskito_core::job::{now_millis, NewJob};
use taskito_core::Storage;

use super::borrow_queue;
use crate::convert::{parse_json, to_json, DeadJobView, ReplayEntryView, RetentionSpec};
use crate::error::BindingError;
use crate::ffi::{guard, new_string, new_string_array, read_optional_string, read_string};

/// `String listDead(long handle, long limit, long offset)` — dead-letter entries.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_listDead<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    limit: jlong,
    offset: jlong,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let dead =
            queue
                .storage
                .list_dead(limit.max(0), offset.max(0), queue.namespace.as_deref())?;
        let views: Vec<DeadJobView> = dead.iter().map(DeadJobView::from).collect();
        new_string(env, to_json(&views)?)
    })
}

/// `String retryDead(long handle, String deadId)` — re-enqueue; returns new id.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_retryDead<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    dead_id: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let id = read_string(env, &dead_id)?;
        new_string(
            env,
            queue.storage.retry_dead(&id, queue.namespace.as_deref())?,
        )
    })
}

/// `boolean deleteDead(long handle, String deadId)`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_deleteDead<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    dead_id: JString<'local>,
) -> jboolean {
    guard(&mut env, JNI_FALSE, |env| {
        let queue = unsafe { borrow_queue(handle) };
        let id = read_string(env, &dead_id)?;
        Ok(super::to_jboolean(
            queue.storage.delete_dead(&id, queue.namespace.as_deref())?,
        ))
    })
}

/// `boolean requeueJob(long handle, String jobId)` — force a stuck Running job
/// back to Pending, releasing its execution claim. False when the job is missing
/// or is not Running.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_requeueJob<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    job_id: JString<'local>,
) -> jboolean {
    guard(&mut env, JNI_FALSE, |env| {
        let queue = unsafe { borrow_queue(handle) };
        let id = read_string(env, &job_id)?;
        Ok(super::to_jboolean(
            queue.storage.requeue_stuck(&id, now_millis())?,
        ))
    })
}

/// `long purgeDead(long handle, long olderThanMs)` — returns rows removed.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_purgeDead<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    older_than_ms: jlong,
) -> jlong {
    guard(&mut env, 0, |_env| {
        let queue = unsafe { borrow_queue(handle) };
        Ok(queue.storage.purge_dead(older_than_ms)? as jlong)
    })
}

/// `String listDeadByTask(long handle, String taskName, long limit, long offset)`
/// — dead-letter entries for one task, as a JSON array.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_listDeadByTask<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    task_name: JString<'local>,
    limit: jlong,
    offset: jlong,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let task = read_string(env, &task_name)?;
        let dead = queue.storage.list_dead_by_task(
            &task,
            limit.max(0),
            offset.max(0),
            queue.namespace.as_deref(),
        )?;
        let views: Vec<DeadJobView> = dead.iter().map(DeadJobView::from).collect();
        new_string(env, to_json(&views)?)
    })
}

/// `long purgeDeadByTask(long handle, String taskName)` — returns rows removed.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_purgeDeadByTask<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    task_name: JString<'local>,
) -> jlong {
    guard(&mut env, 0, |env| {
        let queue = unsafe { borrow_queue(handle) };
        let task = read_string(env, &task_name)?;
        Ok(queue.storage.purge_dead_by_task(&task)? as jlong)
    })
}

/// `long purgeCompleted(long handle, long olderThanMs)` — returns rows removed.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_purgeCompleted<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    older_than_ms: jlong,
) -> jlong {
    guard(&mut env, 0, |_env| {
        let queue = unsafe { borrow_queue(handle) };
        Ok(queue.storage.purge_completed(older_than_ms)? as jlong)
    })
}

/// `void pauseQueue(long handle, String queue)`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_pauseQueue<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    queue_name: JString<'local>,
) {
    guard(&mut env, (), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let name = read_string(env, &queue_name)?;
        queue.storage.pause_queue(&name)?;
        Ok(())
    })
}

/// `void resumeQueue(long handle, String queue)`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_resumeQueue<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    queue_name: JString<'local>,
) {
    guard(&mut env, (), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let name = read_string(env, &queue_name)?;
        queue.storage.resume_queue(&name)?;
        Ok(())
    })
}

/// `String listPausedQueues(long handle)` — a JSON array of queue names.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_listPausedQueues<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        new_string(env, to_json(&queue.storage.list_paused_queues()?)?)
    })
}

/// `String[] reservedSettingPrefixes()` — settings-key prefixes a dashboard's
/// generic KV surface must hide. Sourced from the core so every shell hides the
/// same keys.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_reservedSettingPrefixes<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jobjectArray {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let prefixes: Vec<String> = taskito_core::RESERVED_SETTING_PREFIXES
            .iter()
            .map(|prefix| (*prefix).to_string())
            .collect();
        new_string_array(env, &prefixes)
    })
}

/// `String getSetting(long handle, String key)` — value, or `null` if unset.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_getSetting<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    key: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let key = read_string(env, &key)?;
        match queue.storage.get_setting(&key)? {
            Some(value) => new_string(env, value),
            None => Ok(std::ptr::null_mut()),
        }
    })
}

/// `void setSetting(long handle, String key, String value)`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_setSetting<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    key: JString<'local>,
    value: JString<'local>,
) {
    guard(&mut env, (), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let key = read_string(env, &key)?;
        let value = read_string(env, &value)?;
        queue.storage.set_setting(&key, &value)?;
        Ok(())
    })
}

/// `boolean setSettingIf(long handle, String key, String expectedOrNull, String value)` —
/// writes only if the key still holds `expectedOrNull`, where `null` means it
/// must be unset. Returns false when another writer got there first.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_setSettingIf<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    key: JString<'local>,
    expected: JString<'local>,
    value: JString<'local>,
) -> jboolean {
    guard(&mut env, JNI_FALSE, |env| {
        let queue = unsafe { borrow_queue(handle) };
        let key = read_string(env, &key)?;
        let expected = read_optional_string(env, &expected)?;
        let value = read_string(env, &value)?;
        Ok(super::to_jboolean(queue.storage.set_setting_if(
            &key,
            expected.as_deref(),
            &value,
        )?))
    })
}

/// `boolean deleteSetting(long handle, String key)`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_deleteSetting<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    key: JString<'local>,
) -> jboolean {
    guard(&mut env, JNI_FALSE, |env| {
        let queue = unsafe { borrow_queue(handle) };
        let key = read_string(env, &key)?;
        Ok(super::to_jboolean(queue.storage.delete_setting(&key)?))
    })
}

/// `String listSettings(long handle)` — a JSON map of key to value.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_listSettings<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        new_string(env, to_json(&queue.storage.list_settings()?)?)
    })
}

/// `String effectiveRetention(long handle)` — the retention windows the elected
/// cleaner last published for this queue's namespace as a JSON document, or
/// `null` if no worker has swept yet. See `BINDING_CONTRACT.md`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_effectiveRetention<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        match taskito_core::scheduler::retention::read_effective_retention_json(
            &queue.storage,
            queue.namespace.as_deref(),
        )? {
            Some(json) => new_string(env, json),
            None => Ok(std::ptr::null_mut()),
        }
    })
}

/// `String dryRunRetention(long handle, String retentionJson)` — count what a
/// retention purge would delete right now, without deleting anything.
/// `retentionJson` is a `RetentionSpec` (camelCase seconds) to preview those
/// candidate windows; `null` previews the policy the elected cleaner reported
/// for this namespace (recommended defaults only when unreported). Returns the
/// preview as a JSON document. See `BINDING_CONTRACT.md`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_dryRunRetention<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    retention_json: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let preview = match read_optional_string(env, &retention_json)? {
            // A present spec previews exactly those candidate windows (an
            // all-unset spec = a disabled policy), overriding any live policy.
            Some(json) => {
                let config = parse_json::<RetentionSpec>(&json, "retention spec")?.to_config();
                taskito_core::scheduler::retention::dry_run_json(
                    &queue.storage,
                    Some(&config),
                    None,
                    queue.namespace.as_deref(),
                    now_millis(),
                )?
            }
            // No candidate: retention config lives in the worker here, so the
            // reported policy is the one that actually governs the deletes.
            None => taskito_core::scheduler::retention::dry_run_reported_json(
                &queue.storage,
                queue.namespace.as_deref(),
                now_millis(),
            )?,
        };
        new_string(env, preview)
    })
}

/// `String replayJob(long handle, String jobId)` — re-enqueue a copy of an
/// existing job and record it in the replay history. Returns the new job id.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_replayJob<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    job_id: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let id = read_string(env, &job_id)?;
        let original = queue
            .storage
            .get_job(&id, queue.namespace.as_deref())?
            .ok_or_else(|| BindingError::new(format!("Job not found: {id}")))?;
        let new_job = NewJob {
            queue: original.queue.clone(),
            task_name: original.task_name.clone(),
            payload: original.payload.clone(),
            priority: original.priority,
            scheduled_at: now_millis(),
            max_retries: original.max_retries,
            timeout_ms: original.timeout_ms,
            unique_key: None,
            metadata: Some(format!("{{\"replayed_from\":\"{id}\"}}")),
            notes: original.notes.clone(),
            depends_on: Vec::new(),
            expires_at: None,
            result_ttl_ms: original.result_ttl_ms,
            namespace: original.namespace.clone(),
        };
        let job = queue.storage.enqueue(new_job)?;
        // Best-effort audit row — a history write must not fail the replay.
        let _ = queue.storage.record_replay(
            &id,
            &job.id,
            original.result.as_deref(),
            None,
            original.error.as_deref(),
            None,
        );
        new_string(env, job.id)
    })
}

/// `String getReplayHistory(long handle, String jobId)` — replays recorded for
/// a job, as a JSON array.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_getReplayHistory<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    job_id: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let id = read_string(env, &job_id)?;
        let rows = queue.storage.get_replay_history(&id)?;
        let views: Vec<ReplayEntryView> = rows.iter().map(ReplayEntryView::from).collect();
        new_string(env, to_json(&views)?)
    })
}

/// `int minContract(long handle)` — the lowest contract level a process may
/// speak and still open this storage.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_minContract<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jint {
    guard(&mut env, 0, |_env| {
        let queue = unsafe { borrow_queue(handle) };
        Ok(taskito_core::min_contract(&queue.storage)? as jint)
    })
}

/// `void setMinContract(long handle, int level)` — raise or lower that floor.
/// A level this build does not itself speak is rejected.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_setMinContract<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    level: jint,
) {
    guard(&mut env, (), |_env| {
        let queue = unsafe { borrow_queue(handle) };
        // A negative level cannot be a contract, and the cast would wrap it into
        // an enormous floor that locks the deployment out.
        let level = u32::try_from(level).map_err(|_| {
            crate::error::BindingError::new(format!("contract level must not be negative: {level}"))
        })?;
        taskito_core::set_min_contract(&queue.storage, level)?;
        Ok(())
    })
}

/// `String migrate(long handle)` — apply pending schema changes, returning the
/// report as JSON (`applied`, `workflowApplied`, `archivedJobs`, `schemaless`).
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_migrate<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let report = queue.storage.migrate()?;
        let workflow_applied = migrate_workflow_storage(queue)?;

        // The schema exists now, so the floor can finally be read — a gated
        // deployment is checked here instead of at open.
        taskito_core::ensure_contract_supported(&queue.storage)?;

        new_string(
            env,
            to_json(&serde_json::json!({
                "applied": report.applied,
                "workflowApplied": workflow_applied,
                "archivedJobs": report.archived_jobs,
                "schemaless": report.schemaless,
            }))?,
        )
    })
}

/// Apply pending workflow schema changes, reporting the versions applied. Built
/// unmigrated first so those versions are visible rather than swallowed by the
/// constructor.
#[cfg(feature = "workflows")]
fn migrate_workflow_storage(
    queue: &crate::backend::QueueHandle,
) -> Result<Vec<String>, crate::error::BindingError> {
    let wf =
        crate::backend::build_workflow_storage(&queue.storage, queue.namespace.clone(), false)?;
    Ok(wf.migrate()?)
}

#[cfg(not(feature = "workflows"))]
fn migrate_workflow_storage(
    _queue: &crate::backend::QueueHandle,
) -> Result<Vec<String>, crate::error::BindingError> {
    Ok(Vec::new())
}
