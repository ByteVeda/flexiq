//! Recording what the dashboard changes in the audit trail (#1020).
//!
//! The dashboard calls these from its request handler; the record shape and
//! the off-path write are `flexiq_core::audit`'s, so a row reads the same as
//! one `flexiq-server`'s dashboard writes.

use std::time::Duration;

use jni::objects::{JClass, JString};
use jni::sys::{jint, jlong};
use jni::JNIEnv;

use super::borrow_queue;
use crate::error::BindingError;
use crate::ffi::{guard, read_optional_string, read_string};

/// `void startDashboardAudit(long handle, int retentionDays)` — start
/// recording into this queue's namespace, pruning past `retentionDays`. A
/// second call while recording is a no-op.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_startDashboardAudit<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    retention_days: jint,
) {
    guard(&mut env, (), |_env| {
        let days = u64::try_from(retention_days)
            .ok()
            .filter(|days| *days > 0)
            .ok_or_else(|| BindingError::new("audit retention must be at least 1 day"))?;
        let queue = unsafe { borrow_queue(handle) };
        queue
            .dashboard_audit
            .start(
                queue.storage.clone(),
                queue.namespace.as_deref(),
                Some(Duration::from_secs(days * 86_400)),
            )
            .map_err(|e| BindingError::new(e.to_string()))
    })
}

/// `void recordDashboardAction(long handle, String method, String path, int
/// status, String username)` — record one answered request; `username` is
/// `null` with auth off. Never blocks on storage.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_recordDashboardAction<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    method: JString<'local>,
    path: JString<'local>,
    status: jint,
    username: JString<'local>,
) {
    guard(&mut env, (), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let method = read_string(env, &method)?;
        let path = read_string(env, &path)?;
        let username = read_optional_string(env, &username)?;
        let status = u16::try_from(status).unwrap_or(0);
        queue
            .dashboard_audit
            .record(&method, &path, status, username.as_deref());
        Ok(())
    })
}

/// `void closeDashboardAudit(long handle)` — stop recording and flush what is
/// buffered, waiting at most ten seconds.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_closeDashboardAudit<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    guard(&mut env, (), |_env| {
        let queue = unsafe { borrow_queue(handle) };
        queue.dashboard_audit.close();
        Ok(())
    })
}
