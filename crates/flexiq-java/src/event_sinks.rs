//! Event sinks for a worker: the hub a worker starts from its `events` option,
//! the stop-time drain, and the `NativeWorker` entry points that read them.

use std::sync::{Arc, Condvar, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use flexiq_core::{EventHub, SinkStats};
use jni::objects::JClass;
use jni::sys::{jlong, jstring};
use jni::JNIEnv;
use serde::Serialize;

use crate::error::BindingError;
use crate::ffi::guard;
use crate::handle;
use crate::worker::WorkerHandle;

/// How long a stopping worker waits for buffered events, when unset.
const DEFAULT_DRAIN_MS: u64 = 5_000;

/// The longest drain honoured (~49 days), so `now + drain` cannot overflow `Instant`.
const MAX_DRAIN_MS: u64 = u32::MAX as u64;

/// A bad document is the caller's argument, not a queue failure.
const ILLEGAL_ARGUMENT: &str = "java/lang/IllegalArgumentException";

/// A worker's running hub and the deadline it drains by once stopped.
///
/// The budget starts when `close` has finished waiting for in-flight handlers
/// (`awaitEventDrain`), or when the result loop ends, whichever comes first.
/// Starting it at `stop` would only drop the events of the handlers `close` is
/// already waiting for, without making `close` any shorter.
pub(crate) struct WorkerEvents {
    hub: Arc<EventHub>,
    drain: Duration,
    /// Fixed once, by whichever of `drain` / `wait_drained` runs first.
    deadline: OnceLock<Instant>,
    /// Flips to `true` once the result-drain thread has drained the hub.
    drained: Mutex<bool>,
    drained_signal: Condvar,
}

impl WorkerEvents {
    /// Start the hub `document` describes, if any. A bad document, or a sink
    /// kind this build lacks, throws `IllegalArgumentException` with the core's
    /// message, before the worker writes anything.
    pub(crate) fn start(
        document: Option<&str>,
        drain_ms: Option<u64>,
    ) -> Result<Option<Arc<Self>>, BindingError> {
        let Some(document) = document else {
            return Ok(None);
        };
        let hub = EventHub::from_json(document)
            .map_err(|err| BindingError::with_class(ILLEGAL_ARGUMENT, err.to_string()))?;
        Ok(Some(Arc::new(Self {
            hub: Arc::new(hub),
            drain: Duration::from_millis(drain_ms.unwrap_or(DEFAULT_DRAIN_MS).min(MAX_DRAIN_MS)),
            deadline: OnceLock::new(),
            drained: Mutex::new(false),
            drained_signal: Condvar::new(),
        })))
    }

    /// The hub, for `Scheduler::set_events`.
    pub(crate) fn hub(&self) -> Arc<EventHub> {
        Arc::clone(&self.hub)
    }

    /// Start the drain budget. Idempotent: a later call keeps the first deadline.
    fn mark_stopping(&self) -> Instant {
        *self.deadline.get_or_init(|| Instant::now() + self.drain)
    }

    /// Deliver what is buffered by the deadline, then wake `wait_drained`.
    /// Runs on the result-drain thread once the last result is handled, so it
    /// comes after the worker's final emit.
    pub(crate) fn drain(&self) {
        let budget = self
            .mark_stopping()
            .saturating_duration_since(Instant::now());
        self.hub.shutdown(budget);
        *self.drained.lock().unwrap_or_else(PoisonError::into_inner) = true;
        self.drained_signal.notify_all();
    }

    /// Block until the hub has drained, or the deadline passes. Past it, a job
    /// still in flight keeps the result loop open, so close the hub anyway:
    /// whatever is left is counted as dropped for shutdown, never sent.
    fn wait_drained(&self) {
        let remaining = self
            .mark_stopping()
            .saturating_duration_since(Instant::now());
        let drained = self.drained.lock().unwrap_or_else(PoisonError::into_inner);
        let (drained, _) = self
            .drained_signal
            .wait_timeout_while(drained, remaining, |done| !*done)
            .unwrap_or_else(PoisonError::into_inner);
        if !*drained {
            drop(drained);
            self.hub.shutdown(Duration::ZERO);
        }
    }
}

impl Drop for WorkerEvents {
    /// A start that fails after the hub began, or a handle freed without a
    /// drain, must not leave sink threads retrying with no deadline.
    fn drop(&mut self) {
        self.hub.shutdown(Duration::ZERO);
    }
}

/// One sink's counters, in the shape `EventSinkStats` decodes.
#[derive(Serialize)]
struct SinkStatsWire {
    name: String,
    kind: &'static str,
    delivered: i64,
    dropped_buffer_full: i64,
    dropped_rejected: i64,
    dropped_failed: i64,
    dropped_shutdown: i64,
    queued: i64,
}

impl From<SinkStats> for SinkStatsWire {
    fn from(stats: SinkStats) -> Self {
        Self {
            name: stats.name,
            kind: stats.kind,
            delivered: saturating(stats.delivered),
            dropped_buffer_full: saturating(stats.dropped_buffer_full),
            dropped_rejected: saturating(stats.dropped_rejected),
            dropped_failed: saturating(stats.dropped_failed),
            dropped_shutdown: saturating(stats.dropped_shutdown),
            queued: saturating(stats.queued),
        }
    }
}

/// Java has no `u64`; a counter past `i64::MAX` is unreachable in practice.
fn saturating(count: u64) -> i64 {
    i64::try_from(count).unwrap_or(i64::MAX)
}

/// `String eventSinkStats(long workerHandle)` — a JSON array of each sink's
/// counters, in configuration order; `[]` when the worker has no sinks.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeWorker_eventSinkStats<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let worker = unsafe { handle::borrow::<WorkerHandle>(handle) };
        let stats: Vec<SinkStatsWire> = worker
            .events
            .as_ref()
            .map(|events| events.hub.stats())
            .unwrap_or_default()
            .into_iter()
            .map(SinkStatsWire::from)
            .collect();
        let json = serde_json::to_string(&stats)
            .map_err(|e| BindingError::new(format!("failed to encode event sink stats: {e}")))?;
        crate::ffi::new_string(env, json)
    })
}

/// `void awaitEventDrain(long workerHandle)` — block until a stopped worker's
/// buffered events are delivered or counted as dropped, for at most the drain
/// budget from this call. Returns at once for a worker without sinks.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_flexiq_internal_NativeWorker_awaitEventDrain<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    guard(&mut env, (), |_env| {
        let worker = unsafe { handle::borrow::<WorkerHandle>(handle) };
        if let Some(events) = &worker.events {
            events.wait_drained();
        }
        Ok(())
    })
}
