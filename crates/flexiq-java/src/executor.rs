//! `flexiq executor` — attach to a detached scheduler and run its jobs here.
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
use jni::sys::{jboolean, jint, jlong, JNI_FALSE};
use jni::JNIEnv;
use serde::Deserialize;

use flexiq_core::worker::{
    AttachAddress, ExecutorClient, ExecutorConfig, ExecutorError, ExecutorHandle, ExecutorSession,
    ExecutorSideChannel, ExecutorSteps, WorkerDispatcher, CAP_STEPS,
};

use crate::attached_steps::RunningJobs;
use crate::convert::parse_json;
use crate::dispatcher::{JavaDispatcher, Registry, TaskOutcome};
use crate::error::BindingError;
use crate::ffi::{guard, new_string, read_bytes, read_optional_string, read_string};
use crate::handle::{self, into_handle};

/// How an executor attaches. Durations are milliseconds, matching the Java API.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExecutorOptions {
    /// Scheduler address: `host:port`, `:port`, or `unix:/run/flexiq.sock`.
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
    /// Progress and task logs — the storage-shaped writes the scheduler
    /// performs on this executor's behalf.
    side_channel: ExecutorSideChannel,
    /// Durable steps, which unlike the side channel block and can be refused: a
    /// commit is what the task is waiting on. Read by [`crate::attached_steps`].
    pub(crate) steps: ExecutorSteps,
    /// The jobs this executor is running, which is where a step session finds
    /// the dispatch it must be opened against.
    pub(crate) running: Arc<RunningJobs>,
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
        token: options.token.map(flexiq_core::Secret::new),
        // Claimed because a job context here can actually open a session: task
        // bodies run in this process, so the snapshot that rides a dispatch is
        // read by the code that replays from it. Announced only where that is
        // true, so a scheduler sends a snapshot to nobody who would discard it.
        capabilities: vec![CAP_STEPS.to_string()],
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
    // Recorded by the dispatcher, read when a task asks for a step session:
    // `ExecutorSteps::open_session` needs the dispatch itself, and Java asks for
    // one by job id.
    let running = Arc::new(RunningJobs::default());
    let pool = Arc::new(JavaDispatcher::detached(
        callbacks,
        registry.clone(),
        Arc::clone(&running),
    ));
    let handle = client.spawn(pool.clone() as Arc<dyn WorkerDispatcher>);
    // Installed after the handshake, which is the earliest it exists. The
    // dispatcher reads each job's toggle list through it.
    pool.set_side_channel(handle.side_channel());

    Ok(AttachedHandle {
        executor_id: handle.executor_id().to_string(),
        session: handle.session(),
        side_channel: handle.side_channel(),
        steps: handle.steps(),
        running,
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
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeExecutor_attach<'local>(
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
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeExecutor_completeJob<'local>(
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
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeExecutor_failJob<'local>(
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

/// `void sleepJob(long handle, long token, long wakeAt)` — the attempt ended in
/// a `step.sleep`.
///
/// Not a completion and not a failure: the sleep was committed through the
/// scheduler, which released the claim and left the job `Pending` at `wakeAt`,
/// so this only tells it what happened. The deadline is the one storage settled
/// on, echoed by the commit's ack, which on a replay is not the one proposed.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeExecutor_sleepJob(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    token: jlong,
    wake_at: jlong,
) {
    guard(&mut env, (), |_env| {
        let executor = unsafe { borrow(handle) };
        executor
            .registry
            .complete(token as u64, TaskOutcome::Slept(wake_at));
        Ok(())
    })
}

/// `void cancelJob(long handle, long token)`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeExecutor_cancelJob(
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
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeExecutor_schedulerId<'local>(
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
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeExecutor_executorId<'local>(
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
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeExecutor_peer<'local>(
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
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeExecutor_isRunning<'local>(
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
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeExecutor_awaitSession<'local>(
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

/// `void reportProgress(long handle, String jobId, int progress)`.
///
/// An executor has no storage of its own, so this travels to the scheduler,
/// which applies it. Fire-and-forget: it never blocks the calling task.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeExecutor_reportProgress<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    job_id: JString<'local>,
    progress: jint,
) {
    guard(&mut env, (), |env| {
        let executor = unsafe { borrow(handle) };
        let job_id = read_string(env, &job_id)?;
        executor.side_channel.report_progress(&job_id, progress);
        Ok(())
    })
}

/// `void writeTaskLog(long handle, String jobId, String taskName, String level,
/// String message, String extra)` — `extra` may be null.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeExecutor_writeTaskLog<'local>(
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
        let executor = unsafe { borrow(handle) };
        let job_id = read_string(env, &job_id)?;
        let task_name = read_string(env, &task_name)?;
        let level = read_string(env, &level)?;
        let message = read_string(env, &message)?;
        // Absent and empty are different: a published partial with no value is
        // not the same as one whose value is the empty string.
        let extra = read_optional_string(env, &extra)?;
        executor.side_channel.write_task_log(
            &job_id,
            &task_name,
            &level,
            &message,
            extra.as_deref(),
        );
        Ok(())
    })
}

/// `void stop(long handle)` — stop accepting work; returns at once.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeExecutor_stop<'local>(
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
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeExecutor_close<'local>(
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
