//! JNI entry points backing `org.byteveda.taskito.internal.NativeQueue`.
//!
//! Each function name encodes the Java package, class, and method the JVM links
//! it to. Every body runs inside [`guard`] so failures surface as Java
//! exceptions rather than unwinding across the FFI boundary. This module holds
//! the producer surface; inspection, admin, and log methods live in the sibling
//! submodules.

mod admin;
mod inspect;
mod locks;
mod logs;
mod periodic;
mod pubsub;

use jni::objects::{JByteArray, JClass, JObjectArray, JString};
use jni::sys::{jboolean, jbyteArray, jint, jlong, jobjectArray, jstring, JNI_FALSE, JNI_TRUE};
use jni::JNIEnv;
use taskito_core::Storage;

use crate::backend::{self, QueueHandle};
use crate::convert::{build_new_job, parse_json, EnqueueOptions, OpenOptions};
use crate::ffi::{
    guard, new_bytes, new_string, new_string_array, read_bytes, read_bytes_array, read_string,
};
use crate::handle::{self, drop_handle, into_handle};

/// Borrow the queue behind a handle.
///
/// # Safety
/// `handle` must be a live `QueueHandle` pointer (see [`crate::handle`]).
unsafe fn borrow_queue<'a>(handle: jlong) -> &'a QueueHandle {
    handle::borrow::<QueueHandle>(handle)
}

/// `long open(String optionsJson)` — open a backend, returning its handle.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_open<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    options_json: JString<'local>,
) -> jlong {
    guard(&mut env, 0, |env| {
        let raw = read_string(env, &options_json)?;
        let options: OpenOptions = parse_json(&raw, "open options")?;
        Ok(into_handle(backend::open(options)?))
    })
}

/// `void close(long handle)` — reclaim a handle. A zero handle is a no-op.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_close<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    // Routed through `guard` like every other entry point: a panic in a
    // destructor must not unwind across the FFI boundary.
    guard(&mut env, (), |_env| {
        if handle != 0 {
            unsafe { drop_handle::<QueueHandle>(handle) };
        }
        Ok(())
    })
}

/// `String enqueue(long handle, String taskName, byte[] payload, String optionsJson)`
/// — enqueue a job and return its id. A set `uniqueKey` makes a duplicate
/// enqueue a no-op while the first job is pending/running (idempotency).
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_enqueue<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    task_name: JString<'local>,
    payload: JByteArray<'local>,
    options_json: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let task = read_string(env, &task_name)?;
        let bytes = read_bytes(env, &payload)?;
        let raw_opts = read_string(env, &options_json)?;
        let options: EnqueueOptions = parse_json(&raw_opts, "enqueue options")?;
        let unique = options.unique_key.is_some();
        let new_job = build_new_job(task, bytes, options, queue.namespace.as_deref());
        let job = if unique {
            queue.storage.enqueue_unique(new_job)
        } else {
            queue.storage.enqueue(new_job)
        }?;
        new_string(env, job.id)
    })
}

/// `String[] enqueueMany(long handle, String taskName, byte[][] payloads, String optionsJson)`
/// — enqueue a batch. `optionsJson` is a JSON array of per-job options, the same
/// length as `payloads`. Keyless jobs insert in one storage transaction; a job
/// with a `uniqueKey` takes the dedup path instead (an active duplicate resolves
/// to the existing job's id, matching single `enqueue`). Returns the job ids in
/// input order.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_enqueueMany<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    task_name: JString<'local>,
    payloads: JObjectArray<'local>,
    options_json: JString<'local>,
) -> jobjectArray {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let task = read_string(env, &task_name)?;
        let payload_list = read_bytes_array(env, &payloads)?;
        let raw_opts = read_string(env, &options_json)?;
        let option_list: Vec<EnqueueOptions> = parse_json(&raw_opts, "enqueue batch options")?;
        if option_list.len() != payload_list.len() {
            return Err(crate::error::BindingError::new(
                "payloads and options must have the same length",
            ));
        }
        let namespace = queue.namespace.as_deref();
        let new_jobs = payload_list
            .into_iter()
            .zip(option_list)
            .map(|(payload, options)| build_new_job(task.clone(), payload, options, namespace))
            .collect();
        let ids = backend::enqueue_batch_dedup(&queue.storage, new_jobs)?;
        new_string_array(env, &ids)
    })
}

/// `String getJob(long handle, String jobId)` — a JSON job view, or `null`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_getJob<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    job_id: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let id = read_string(env, &job_id)?;
        match queue.storage.get_job(&id, queue.namespace.as_deref())? {
            Some(job) => new_string(
                env,
                crate::convert::to_json(&crate::convert::JobView::from(&job))?,
            ),
            None => Ok(std::ptr::null_mut()),
        }
    })
}

/// `byte[] getResult(long handle, String jobId)` — the job's serialized result,
/// or `null` if absent or not yet complete.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_getResult<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    job_id: JString<'local>,
) -> jbyteArray {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let id = read_string(env, &job_id)?;
        match queue
            .storage
            .get_job(&id, queue.namespace.as_deref())?
            .and_then(|job| job.result)
        {
            Some(bytes) => new_bytes(env, &bytes),
            None => Ok(std::ptr::null_mut()),
        }
    })
}

/// `boolean cancel(long handle, String jobId)` — cancel a pending job.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_cancel<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    job_id: JString<'local>,
) -> jboolean {
    guard(&mut env, JNI_FALSE, |env| {
        let queue = unsafe { borrow_queue(handle) };
        let id = read_string(env, &job_id)?;
        Ok(to_jboolean(
            queue.storage.cancel_job(&id, queue.namespace.as_deref())?,
        ))
    })
}

/// `boolean requestCancel(long handle, String jobId)` — cooperatively cancel a
/// running job. Returns false if no such running job exists.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_requestCancel<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    job_id: JString<'local>,
) -> jboolean {
    guard(&mut env, JNI_FALSE, |env| {
        let queue = unsafe { borrow_queue(handle) };
        let id = read_string(env, &job_id)?;
        Ok(to_jboolean(
            queue
                .storage
                .request_cancel(&id, queue.namespace.as_deref())?,
        ))
    })
}

/// `boolean isCancelRequested(long handle, String jobId)`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_isCancelRequested<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    job_id: JString<'local>,
) -> jboolean {
    guard(&mut env, JNI_FALSE, |env| {
        let queue = unsafe { borrow_queue(handle) };
        let id = read_string(env, &job_id)?;
        Ok(to_jboolean(
            queue
                .storage
                .is_cancel_requested(&id, queue.namespace.as_deref())?,
        ))
    })
}

/// `void setProgress(long handle, String jobId, int progress)` — clamp to 0..100.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_setProgress<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    job_id: JString<'local>,
    progress: jint,
) {
    guard(&mut env, (), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let id = read_string(env, &job_id)?;
        queue
            .storage
            .update_progress(&id, progress.clamp(0, 100), queue.namespace.as_deref())?;
        Ok(())
    })
}

/// Map a Rust `bool` onto a JNI `jboolean`.
fn to_jboolean(value: bool) -> jboolean {
    if value {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}
