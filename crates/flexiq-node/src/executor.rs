//! `flexiq executor` — attach to a detached scheduler and run its jobs here.
//!
//! A free function rather than a `JsQueue` method, and deliberately so: an
//! executor opens no storage. That is the point of the split — the scheduler
//! image holds the database credentials, the app image holds the task bodies,
//! and everything a job needs to run arrives on the wire.
//!
//! Task execution is the same [`NodeDispatcher`] the in-process worker uses, so
//! concurrency, timeouts and the cancel signal behave identically; only the
//! transport differs.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use flexiq_core::worker::{
    AttachAddress, CancelSignals, ExecutorClient, ExecutorConfig, ExecutorError, ExecutorHandle,
    ExecutorSession, ExecutorSideChannel, ExecutorSteps, WorkerDispatcher, CAP_STEPS,
};
use napi::bindgen_prelude::{spawn_blocking, Promise, Result, Status};
use napi::threadsafe_function::ThreadsafeFunction;
use napi_derive::napi;

use crate::attached_steps::RunningJobs;
use crate::convert::{JsTaskInvocation, JsTaskOutcome};
use crate::dispatcher::NodeDispatcher;
use crate::error::invalid_arg;

/// Defaults chosen to match the other SDKs rather than to be tuned here.
const DEFAULT_SLOTS: u32 = 1;
const DEFAULT_CONNECT_TIMEOUT_MS: u32 = 10_000;

/// How an executor attaches. Durations are milliseconds, per Node convention.
#[napi(object)]
pub struct ExecutorOptions {
    /// Scheduler address: `host:port`, `:port`, or `unix:/run/flexiq.sock`.
    pub address: String,
    /// Task names this executor can run. The scheduler sends it nothing else,
    /// so a name missing here is a job that never arrives.
    pub tasks: Vec<String>,
    /// Jobs to run at once (default 1).
    pub slots: Option<u32>,
    /// Shared secret, when the scheduler requires one.
    pub token: Option<String>,
    /// Identity announced to the scheduler (default: generated per process).
    pub executor_id: Option<String>,
    /// How long to wait for the connection (default 10000).
    pub connect_timeout_ms: Option<u32>,
    /// How often to send a liveness heartbeat (default 5000).
    pub heartbeat_interval_ms: Option<u32>,
    /// How long a drain waits for in-flight jobs before disconnecting anyway
    /// (default 30000).
    pub shutdown_drain_ms: Option<u32>,
}

/// A running attachment to a scheduler.
#[napi]
pub struct JsExecutor {
    /// Taken by `shutdown`, which consumes the handle to join its threads.
    handle: Arc<Mutex<Option<ExecutorHandle>>>,
    session: ExecutorSession,
    /// Cancels delivered as protocol frames. The JS side polls this instead of
    /// a storage flag, which a detached executor does not have.
    cancels: Arc<CancelSignals>,
    /// Progress, task logs and toggles — the storage-shaped operations the
    /// scheduler performs on this executor's behalf.
    side_channel: ExecutorSideChannel,
    /// Durable steps, which unlike the side channel block and can be refused:
    /// a commit is what the task is waiting on. Read by `attached_steps.rs`.
    pub(crate) steps: ExecutorSteps,
    /// The jobs this executor is running, which is where a step session finds
    /// the dispatch it must be opened against.
    pub(crate) running: Arc<RunningJobs>,
    scheduler_id: String,
    executor_id: String,
    peer: String,
}

#[napi]
impl JsExecutor {
    /// Identity the scheduler announced when it accepted this attach.
    #[napi(getter)]
    pub fn scheduler_id(&self) -> String {
        self.scheduler_id.clone()
    }

    /// Identity this executor attached under.
    #[napi(getter)]
    pub fn executor_id(&self) -> String {
        self.executor_id.clone()
    }

    /// Peer label of the scheduler connection.
    #[napi(getter)]
    pub fn peer(&self) -> String {
        self.peer.clone()
    }

    /// Whether the scheduler session is still open.
    #[napi]
    pub fn is_running(&self) -> bool {
        self.session.is_running()
    }

    /// Whether the scheduler has asked for `job_id` to be cancelled.
    ///
    /// The cancel arrives as a frame, not a storage flag, so this is the only
    /// way a running handler can observe one.
    #[napi]
    pub fn is_cancel_requested(&self, job_id: String) -> bool {
        self.cancels.is_cancelled(&job_id)
    }

    /// Whether the scheduler applies progress and task logs on our behalf.
    ///
    /// False against a scheduler with no storage configured for it, or one
    /// built before the side-channel existed. The methods below are no-ops
    /// either way; this exists so the shell can say so once rather than
    /// silently dropping a task's progress bar.
    #[napi]
    pub fn supports_side_channel(&self) -> bool {
        self.side_channel.is_supported()
    }

    /// Report a running job's progress (0-100).
    ///
    /// An executor has no storage of its own, so this travels to the scheduler
    /// instead. Fire-and-forget: it never blocks the calling task and never
    /// fails its job.
    #[napi]
    pub fn report_progress(&self, job_id: String, progress: i32) {
        self.side_channel.report_progress(&job_id, progress);
    }

    /// Write one structured log line for a running job. A published partial is
    /// this at level `result`, with the value as `extra`.
    #[napi]
    pub fn write_task_log(
        &self,
        job_id: String,
        task_name: String,
        level: String,
        message: String,
        extra: Option<String>,
    ) {
        self.side_channel
            .write_task_log(&job_id, &task_name, &level, &message, extra.as_deref());
    }

    /// Middleware the operator has disabled for a running job's task.
    ///
    /// Resolved by the scheduler at dispatch and carried on the job frame, so
    /// a dashboard toggle is honoured without the settings read this process
    /// has no storage to perform.
    #[napi]
    pub fn disabled_middleware(&self, job_id: String) -> Vec<String> {
        self.side_channel.disabled_middleware(&job_id)
    }

    /// Resolve once the scheduler ends the session — a `shutdown` frame, or the
    /// connection dropping. Does not drain; call `shutdown()` for that.
    #[napi]
    pub async fn wait(&self) -> Result<()> {
        let session = self.session.clone();
        spawn_blocking(move || session.wait())
            .await
            .map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    /// Ask the scheduler to stop sending work and finish what is in flight.
    /// Returns at once, so it is safe from a signal handler.
    #[napi]
    pub fn stop(&self) {
        if let Some(handle) = self.locked().as_ref() {
            handle.stop();
        }
    }

    /// Drain in-flight work, disconnect, and join. Idempotent.
    #[napi]
    pub async fn shutdown(&self) -> Result<()> {
        let taken = self.locked().take();
        let Some(handle) = taken else {
            return Ok(());
        };
        // Joining blocks on the drain budget; never park the JS event loop on it.
        spawn_blocking(move || handle.shutdown())
            .await
            .map_err(|error| napi::Error::from_reason(error.to_string()))
    }
}

impl JsExecutor {
    fn locked(&self) -> std::sync::MutexGuard<'_, Option<ExecutorHandle>> {
        self.handle.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Attach to a scheduler and run `callback` for each job it dispatches.
///
/// The handshake happens here, so a bad token or an unreachable scheduler
/// rejects before any pool is built.
///
/// Async because dialling and the handshake block: run on the JS thread they
/// would freeze the event loop until the scheduler answers, or for the whole
/// handshake timeout when it never does.
#[napi]
pub async fn start_executor(
    // Spelled out rather than via the `TaskCallback` alias: napi-derive resolves
    // these generics syntactically, and an alias reaches the generated
    // `index.d.ts` as an undefined type name.
    callback: ThreadsafeFunction<
        JsTaskInvocation,
        Promise<JsTaskOutcome>,
        JsTaskInvocation,
        Status,
        false,
    >,
    options: ExecutorOptions,
) -> Result<JsExecutor> {
    if options.tasks.is_empty() {
        // This would attach successfully and then sit idle forever, because the
        // scheduler only dispatches task names an executor advertises.
        return Err(invalid_arg(
            "no tasks are registered on this app, so the executor would never be sent any work",
        ));
    }
    let slots = options.slots.unwrap_or(DEFAULT_SLOTS).max(1);

    let mut config = ExecutorConfig {
        tasks: options.tasks,
        slots,
        token: options.token.map(flexiq_core::Secret::new),
        // Claimed because a job context here can actually open a session: task
        // bodies run in this process, so the snapshot that rides a dispatch is
        // read by the code that replays from it. Announced only where that is
        // true, so a scheduler sends a snapshot to nobody who would discard it.
        capabilities: vec![CAP_STEPS.to_string()],
        ..ExecutorConfig::new("node", env!("CARGO_PKG_VERSION"))
    };
    if let Some(id) = options.executor_id {
        config.executor_id = id;
    }
    if let Some(interval) = options.heartbeat_interval_ms {
        config.heartbeat_interval = Duration::from_millis(interval.into());
    }
    if let Some(drain) = options.shutdown_drain_ms {
        config.shutdown_drain = Duration::from_millis(drain.into());
    }

    let connect_timeout = Duration::from_millis(
        options
            .connect_timeout_ms
            .unwrap_or(DEFAULT_CONNECT_TIMEOUT_MS)
            .into(),
    );
    let target = AttachAddress::parse(&options.address)
        .map_err(|error| invalid_arg(format!("invalid attach address: {error}")))?;

    spawn_blocking(move || {
        let transport = target.connect(connect_timeout).map_err(|error| {
            napi::Error::from_reason(format!(
                "could not reach the scheduler at {target}: {error}"
            ))
        })?;
        let client = ExecutorClient::connect(transport, config).map_err(|error| match error {
            // Named so a wrong token reads as a refusal rather than a network fault.
            ExecutorError::Refused => napi::Error::from_reason(error.to_string()),
            other => napi::Error::from_reason(format!("could not attach to {target}: {other}")),
        })?;

        let scheduler_id = client.scheduler_id().to_string();
        let peer = client.peer().to_string();

        // Recorded by the dispatcher, read when a task asks for a step session:
        // `ExecutorSteps::open_session` needs the dispatch itself, and JS asks
        // for one by job id.
        let running = Arc::new(RunningJobs::default());
        let dispatcher =
            NodeDispatcher::detached(callback, Some(slots as usize), Arc::clone(&running));
        let cancels = dispatcher.cancels();
        let handle = client.spawn(Arc::new(dispatcher) as Arc<dyn WorkerDispatcher>);

        Ok(JsExecutor {
            executor_id: handle.executor_id().to_string(),
            session: handle.session(),
            side_channel: handle.side_channel(),
            steps: handle.steps(),
            running,
            cancels,
            handle: Arc::new(Mutex::new(Some(handle))),
            scheduler_id,
            peer,
        })
    })
    .await
    .map_err(|error| napi::Error::from_reason(error.to_string()))?
}
