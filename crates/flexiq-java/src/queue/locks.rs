//! Distributed-lock entry points for `NativeQueue`. Locks are advisory,
//! TTL-bounded, and owner-scoped; the worker maintenance loop reaps expired ones.

use flexiq_core::Storage;
use jni::objects::{JClass, JString};
use jni::sys::{jboolean, jlong, jstring, JNI_FALSE};
use jni::JNIEnv;

use super::{borrow_queue, to_jboolean};
use crate::convert::{to_json, LockInfoView};
use crate::error::BindingError;
use crate::ffi::{guard, new_string, read_string};

fn positive_ttl(ttl_ms: jlong) -> Result<i64, BindingError> {
    if ttl_ms <= 0 {
        Err(BindingError::new(format!(
            "ttlMs must be > 0 (got {ttl_ms})"
        )))
    } else {
        Ok(ttl_ms)
    }
}

/// `boolean acquireLock(long handle, String name, String ownerId, long ttlMs)`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_acquireLock<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    name: JString<'local>,
    owner_id: JString<'local>,
    ttl_ms: jlong,
) -> jboolean {
    guard(&mut env, JNI_FALSE, |env| {
        let queue = unsafe { borrow_queue(handle) };
        let name = read_string(env, &name)?;
        let owner = read_string(env, &owner_id)?;
        let ttl = positive_ttl(ttl_ms)?;
        Ok(to_jboolean(queue.storage.acquire_lock(&name, &owner, ttl)?))
    })
}

/// `boolean releaseLock(long handle, String name, String ownerId)`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_releaseLock<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    name: JString<'local>,
    owner_id: JString<'local>,
) -> jboolean {
    guard(&mut env, JNI_FALSE, |env| {
        let queue = unsafe { borrow_queue(handle) };
        let name = read_string(env, &name)?;
        let owner = read_string(env, &owner_id)?;
        Ok(to_jboolean(queue.storage.release_lock(&name, &owner)?))
    })
}

/// `boolean extendLock(long handle, String name, String ownerId, long ttlMs)`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_extendLock<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    name: JString<'local>,
    owner_id: JString<'local>,
    ttl_ms: jlong,
) -> jboolean {
    guard(&mut env, JNI_FALSE, |env| {
        let queue = unsafe { borrow_queue(handle) };
        let name = read_string(env, &name)?;
        let owner = read_string(env, &owner_id)?;
        let ttl = positive_ttl(ttl_ms)?;
        Ok(to_jboolean(queue.storage.extend_lock(&name, &owner, ttl)?))
    })
}

/// `String getLockInfo(long handle, String name)` — JSON holder info, or `null`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeQueue_getLockInfo<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    name: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let name = read_string(env, &name)?;
        match queue.storage.get_lock_info(&name)? {
            Some(info) => new_string(env, to_json(&LockInfoView::from(&info))?),
            None => Ok(std::ptr::null_mut()),
        }
    })
}
