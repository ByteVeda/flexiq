//! Periodic (cron) task registration + management. The worker maintenance loop
//! enqueues due tasks; this binds registration plus list / delete / pause /
//! resume. Re-registering a name replaces it.

use flexiq_core::job::now_millis;
use flexiq_core::periodic::{next_cron_time, next_cron_time_tz};
use flexiq_core::storage::records::NewPeriodicTask;
use flexiq_core::Storage;
use jni::objects::{JByteArray, JClass, JString};
use jni::sys::{jboolean, jlong, jstring, JNI_FALSE};
use jni::JNIEnv;

use super::{borrow_queue, to_jboolean};
use crate::convert::{to_json, PeriodicTaskView};
use crate::ffi::{guard, new_string, read_bytes, read_optional_string, read_string};

const DEFAULT_QUEUE: &str = "default";

/// `long registerPeriodic(long handle, String name, String taskName, String cron,
/// byte[] args, String queue, String timezone, boolean enabled)` — register (or
/// replace) a cron task. Returns the next fire time (Unix ms). Rejects bad cron.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_registerPeriodic<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    name: JString<'local>,
    task_name: JString<'local>,
    cron_expr: JString<'local>,
    args: JByteArray<'local>,
    queue_name: JString<'local>,
    timezone: JString<'local>,
    enabled: jboolean,
) -> jlong {
    guard(&mut env, 0, |env| {
        let queue = unsafe { borrow_queue(handle) };
        let name = read_string(env, &name)?;
        let task = read_string(env, &task_name)?;
        let cron = read_string(env, &cron_expr)?;
        let args_bytes = if args.is_null() {
            None
        } else {
            Some(read_bytes(env, &args)?)
        };
        let queue_name = read_optional_string(env, &queue_name)?;
        let timezone = read_optional_string(env, &timezone)?;

        let now = now_millis();
        let next_run = match timezone.as_deref() {
            Some(tz) => next_cron_time_tz(&cron, now, tz)?,
            None => next_cron_time(&cron, now)?,
        };

        let row = NewPeriodicTask {
            name: name.clone(),
            task_name: task,
            cron_expr: cron,
            args: args_bytes,
            kwargs: None,
            queue: queue_name.unwrap_or_else(|| DEFAULT_QUEUE.to_string()),
            enabled: enabled != 0,
            next_run,
            timezone,
        };
        queue.storage.register_periodic(&row)?;
        Ok(next_run)
    })
}

/// `String listPeriodic(long handle)` — a JSON array of registered periodic
/// tasks (enabled and paused).
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_listPeriodic<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let tasks = queue.storage.list_periodic()?;
        let views: Vec<PeriodicTaskView> = tasks.iter().map(PeriodicTaskView::from).collect();
        new_string(env, to_json(&views)?)
    })
}

/// `boolean deletePeriodic(long handle, String name)` — false if none had that name.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_deletePeriodic<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    name: JString<'local>,
) -> jboolean {
    guard(&mut env, JNI_FALSE, |env| {
        let queue = unsafe { borrow_queue(handle) };
        let name = read_string(env, &name)?;
        Ok(to_jboolean(queue.storage.delete_periodic(&name)?))
    })
}

/// `boolean setPeriodicEnabled(long handle, String name, boolean enabled)` —
/// pause (false) or resume (true); false if none had that name.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_setPeriodicEnabled<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    name: JString<'local>,
    enabled: jboolean,
) -> jboolean {
    guard(&mut env, JNI_FALSE, |env| {
        let queue = unsafe { borrow_queue(handle) };
        let name = read_string(env, &name)?;
        Ok(to_jboolean(
            queue.storage.set_periodic_enabled(&name, enabled != 0)?,
        ))
    })
}
