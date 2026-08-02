//! Task-log entry points for `NativeQueue`.

use jni::objects::{JClass, JString};
use jni::sys::{jlong, jstring};
use jni::JNIEnv;
use taskito_core::Storage;

use super::borrow_queue;
use crate::convert::{to_json, LogView};
use crate::ffi::{guard, new_string, read_optional_string, read_string};

/// `void writeTaskLog(long handle, String jobId, String taskName, String level, String message, String extraOrNull)`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_writeTaskLog<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    job_id: JString<'local>,
    task_name: JString<'local>,
    level: JString<'local>,
    message: JString<'local>,
    extra: JString<'local>,
) {
    guard(&mut env, (), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let job_id = read_string(env, &job_id)?;
        let task_name = read_string(env, &task_name)?;
        let level = read_string(env, &level)?;
        let message = read_string(env, &message)?;
        let extra = read_optional_string(env, &extra)?;
        queue.storage.write_task_log(
            &job_id,
            &task_name,
            &level,
            &message,
            extra.as_deref(),
            queue.namespace.as_deref(),
        )?;
        Ok(())
    })
}

/// `String getTaskLogs(long handle, String jobId)` — a JSON array of log lines.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_getTaskLogs<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    job_id: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let id = read_string(env, &job_id)?;
        let logs = queue
            .storage
            .get_task_logs(&id, queue.namespace.as_deref())?;
        let views: Vec<LogView> = logs.iter().map(LogView::from).collect();
        new_string(env, to_json(&views)?)
    })
}

/// `String getTaskLogsAfter(long handle, String jobId, String afterIdOrNull)` —
/// a JSON array of log lines with id strictly after `afterId` (`null` = all).
/// UUIDv7 ids are time-ordered, so the last id doubles as a stream cursor.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_getTaskLogsAfter<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    job_id: JString<'local>,
    after_id: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let id = read_string(env, &job_id)?;
        let after = read_optional_string(env, &after_id)?;
        let logs =
            queue
                .storage
                .get_task_logs_after(&id, after.as_deref(), queue.namespace.as_deref())?;
        let views: Vec<LogView> = logs.iter().map(LogView::from).collect();
        new_string(env, to_json(&views)?)
    })
}

/// `String queryTaskLogs(long handle, String taskOrNull, String levelOrNull, long sinceMs, long limit)`
/// — logs across jobs filtered by task/level, at or after `sinceMs`, newest capped at `limit`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_queryTaskLogs<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    task_name: JString<'local>,
    level: JString<'local>,
    since_ms: jlong,
    limit: jlong,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let task = read_optional_string(env, &task_name)?;
        let level = read_optional_string(env, &level)?;
        let logs = queue.storage.query_task_logs(
            task.as_deref(),
            level.as_deref(),
            since_ms,
            limit.max(0),
            queue.namespace.as_deref(),
        )?;
        let views: Vec<LogView> = logs.iter().map(LogView::from).collect();
        new_string(env, to_json(&views)?)
    })
}
