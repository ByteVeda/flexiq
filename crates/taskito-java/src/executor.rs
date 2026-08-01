//! `taskito executor` — attach to a detached scheduler and run its jobs here.
//!
//! The mirror of [`crate::worker`] with storage swapped for a socket. Jobs run
//! on the same [`JavaDispatcher`] and the same `WorkerBridge` callback, so a
//! handler behaves identically whichever way its job arrived; only the
//! transport differs.
//!
//! No `QueueHandle` is involved, and deliberately so: an executor opens no
//! storage. That is the point of the split — the scheduler image holds the
//! database credentials, the app image holds the task bodies.

use std::sync::Arc;
use std::time::Duration;

use jni::objects::{GlobalRef, JByteArray, JClass, JObject, JString};
use jni::sys::{jboolean, jlong, JNI_FALSE};
use jni::JNIEnv;
use serde::Deserialize;

use taskito_core::worker::{
    AttachAddress, ExecutorClient, ExecutorConfig, ExecutorError, ExecutorHandle, ExecutorSession,
    WorkerDispatcher,
};

use crate::convert::parse_json;
use crate::dispatcher::{JavaDispatcher, Registry, TaskOutcome};
use crate::error::BindingError;
use crate::ffi::{guard, new_string, read_bytes, read_string};
use crate::handle::{self, into_handle};

/// How an executor attaches. Durations are milliseconds, matching the Java API.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExecutorOptions {
    /// Scheduler address: `host:port`, `:port`, or `unix:/run/taskito.sock`.
    address: String,
    /// Task names this executor can run. The scheduler sends it nothing else.
    tasks: Vec<String>,
    #[serde(default)]
    slots: Option<u32>,
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    executor_id: Option<String>,
    #[serde(default)]
    connect_timeout_ms: Option<u64>,
    #[serde(default)]
    heartbeat_interval_ms: Option<u64>,
    #[serde(default)]
    shutdown_drain_ms: Option<u64>,
}

/// How long to wait for the connection when the caller gave no budget.
const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 10_000;

/// A running attachment. Holds the completion registry for the executor's life,
/// so a token handed to Java stays resolvable until [`close`] runs.
pub struct AttachedHandle {
    handle: Option<ExecutorHandle>,
    session: ExecutorSession,
    /// Resolves the tokens handed to Java, so a handler can complete its job.
    registry: Arc<Registry>,
    scheduler_id: String,
    executor_id: String,
    peer: String,
}

impl AttachedHandle {
    /// Drain in-flight work, disconnect, and join. Idempotent.
    fn shutdown(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.shutdown();
        }
    }
}

/// Dial, handshake, and start running jobs on the Java bridge.
fn attach(options: ExecutorOptions, callbacks: GlobalRef) -> Result<AttachedHandle, BindingError> {
    if options.tasks.is_empty() {
        // This would attach successfully and then sit idle forever, because the
        // scheduler only dispatches task names an executor advertises.
        return Err(BindingError::new(
            "no handlers were found, so the executor would never be sent any work",
        ));
    }
    let slots = options.slots.unwrap_or(1).max(1);

    let mut config = ExecutorConfig {
        tasks: options.tasks,
        slots,
        token: options.token.map(taskito_core::Secret::new),
        ..ExecutorConfig::new("java", env!("CARGO_PKG_VERSION"))
    };
    if let Some(id) = options.executor_id {
        config.executor_id = id;
    }
    if let Some(interval) = options.heartbeat_interval_ms {
        config.heartbeat_interval = Duration::from_millis(interval);
    }
    if let Some(drain) = options.shutdown_drain_ms {
        config.shutdown_drain = Duration::from_millis(drain);
    }

    let target = AttachAddress::parse(&options.address)
        .map_err(|error| BindingError::new(format!("invalid attach address: {error}")))?;
    let connect_timeout = Duration::from_millis(
        options
            .connect_timeout_ms
            .unwrap_or(DEFAULT_CONNECT_TIMEOUT_MS),
    );
    let transport = target.connect(connect_timeout).map_err(|error| {
        BindingError::new(format!(
            "could not reach the scheduler at {target}: {error}"
        ))
    })?;
    let client = ExecutorClient::connect(transport, config).map_err(|error| match error {
        // Named so a wrong token reads as a refusal rather than a network fault.
        ExecutorError::Refused => BindingError::new(error.to_string()),
        other => BindingError::new(format!("could not attach to {target}: {other}")),
    })?;

    let scheduler_id = client.scheduler_id().to_string();
    let peer = client.peer().to_string();

    let registry = Arc::new(Registry::default());
    let pool: Arc<dyn WorkerDispatcher> =
        Arc::new(JavaDispatcher::detached(callbacks, registry.clone()));
    let handle = client.spawn(pool);

    Ok(AttachedHandle {
        executor_id: handle.executor_id().to_string(),
        session: handle.session(),
        handle: Some(handle),
        registry,
        scheduler_id,
        peer,
    })
}

/// Borrow the value behind an executor handle.
///
/// # Safety
/// `handle` must be a live `AttachedHandle` pointer from [`attach`].
unsafe fn borrow<'a>(handle: jlong) -> &'a AttachedHandle {
    handle::borrow::<AttachedHandle>(handle)
}

/// `long attach(Object bridge, String optionsJson)` — dial and start; returns a
/// handle. `bridge` is a Java `WorkerBridge`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeExecutor_attach<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    bridge: JObject<'local>,
    options_json: JString<'local>,
) -> jlong {
    guard(&mut env, 0, |env| {
        let raw = read_string(env, &options_json)?;
        let options: ExecutorOptions = parse_json(&raw, "executor options")?;
        let callbacks = env
            .new_global_ref(&bridge)
            .map_err(|e| BindingError::new(format!("global ref failed: {e}")))?;
        Ok(into_handle(attach(options, callbacks)?))
    })
}

/// `void completeJob(long handle, long token, byte[] result)`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeExecutor_completeJob<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    token: jlong,
    result: JByteArray<'local>,
) {
    guard(&mut env, (), |env| {
        let executor = unsafe { borrow(handle) };
        let bytes = read_bytes(env, &result)?;
        executor
            .registry
            .complete(token as u64, TaskOutcome::Success(bytes));
        Ok(())
    })
}

/// `void failJob(long handle, long token, String error, boolean retryable)`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeExecutor_failJob<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    token: jlong,
    error: JString<'local>,
    retryable: jboolean,
) {
    guard(&mut env, (), |env| {
        let executor = unsafe { borrow(handle) };
        let message = read_string(env, &error)?;
        executor.registry.complete(
            token as u64,
            TaskOutcome::Failure(message, retryable != JNI_FALSE),
        );
        Ok(())
    })
}

/// `void cancelJob(long handle, long token)`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeExecutor_cancelJob(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    token: jlong,
) {
    guard(&mut env, (), |_env| {
        let executor = unsafe { borrow(handle) };
        executor
            .registry
            .complete(token as u64, TaskOutcome::Cancelled);
        Ok(())
    })
}

/// `String schedulerId(long handle)`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeExecutor_schedulerId<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jni::sys::jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let executor = unsafe { borrow(handle) };
        new_string(env, executor.scheduler_id.clone())
    })
}

/// `String executorId(long handle)`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeExecutor_executorId<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jni::sys::jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let executor = unsafe { borrow(handle) };
        new_string(env, executor.executor_id.clone())
    })
}

/// `String peer(long handle)`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeExecutor_peer<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jni::sys::jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let executor = unsafe { borrow(handle) };
        new_string(env, executor.peer.clone())
    })
}

/// `boolean isRunning(long handle)` — whether the scheduler session is open.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeExecutor_isRunning<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jboolean {
    guard(&mut env, 0, |_env| {
        let executor = unsafe { borrow(handle) };
        Ok(jboolean::from(executor.session.is_running()))
    })
}

/// `void awaitSession(long handle)` — block until the scheduler ends the
/// session. Java calls it from a thread it is willing to park.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeExecutor_awaitSession<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    guard(&mut env, (), |_env| {
        let executor = unsafe { borrow(handle) };
        // Cloned so the wait holds no borrow of the handle: `close` may run as
        // soon as this returns.
        let session = executor.session.clone();
        session.wait();
        Ok(())
    })
}

/// `void stop(long handle)` — stop accepting work; returns at once.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeExecutor_stop<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    guard(&mut env, (), |_env| {
        if handle != 0 {
            let executor = unsafe { borrow(handle) };
            if let Some(running) = executor.handle.as_ref() {
                running.stop();
            }
        }
        Ok(())
    })
}

/// `void close(long handle)` — drain, disconnect, and reclaim the handle.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeExecutor_close<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    // Route through `guard` so a panic in teardown cannot unwind across FFI.
    guard(&mut env, (), |_env| {
        if handle != 0 {
            // Reclaimed by hand rather than via `drop_handle`: the drain has to
            // run before the box drops, and it needs `&mut`.
            let mut executor = unsafe { Box::from_raw(handle as *mut AttachedHandle) };
            executor.shutdown();
        }
        Ok(())
    })
}
