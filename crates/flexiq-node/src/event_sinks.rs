//! Event sinks for a worker: the hub a worker starts from its `events`
//! option, and that hub's counters on the worker handle.

use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use flexiq_core::{EventHub, SinkStats};
use napi::bindgen_prelude::{spawn_blocking, Result};
use napi_derive::napi;
use tokio::sync::watch;

use crate::error::{invalid_arg, join_to_napi_err};
use crate::worker::JsWorker;

/// How long a stopping worker waits for buffered events, when unset.
const DEFAULT_DRAIN_MS: u32 = 5_000;

/// A worker's running hub and the deadline it drains by once stopped.
///
/// The budget runs from `stop`, not from the last result: in-flight jobs spend
/// it too, so a job that never settles cannot hold `stop` open past it.
pub(crate) struct WorkerEvents {
    hub: Arc<EventHub>,
    drain: Duration,
    /// Fixed by the first `stop`: when buffered events must be out.
    deadline: OnceLock<Instant>,
    /// Flips to `true` once the result-drain thread has drained the hub.
    drained: watch::Sender<bool>,
}

impl WorkerEvents {
    /// Start the hub `document` describes, if any. A bad document, or a sink
    /// kind this build lacks, throws here, before the worker starts anything.
    pub(crate) fn start(
        document: Option<&str>,
        drain_ms: Option<u32>,
    ) -> Result<Option<Arc<Self>>> {
        let Some(document) = document else {
            return Ok(None);
        };
        let hub = EventHub::from_json(document).map_err(|err| invalid_arg(err.to_string()))?;
        Ok(Some(Arc::new(Self {
            hub: Arc::new(hub),
            drain: Duration::from_millis(u64::from(drain_ms.unwrap_or(DEFAULT_DRAIN_MS))),
            deadline: OnceLock::new(),
            drained: watch::Sender::new(false),
        })))
    }

    /// The hub, for `Scheduler::set_events`.
    pub(crate) fn hub(&self) -> Arc<EventHub> {
        Arc::clone(&self.hub)
    }

    /// Start the drain budget. Idempotent: a later `stop` keeps the first deadline.
    pub(crate) fn mark_stopping(&self) -> Instant {
        *self.deadline.get_or_init(|| Instant::now() + self.drain)
    }

    /// Deliver what is buffered by the deadline, then signal `wait_drained`.
    /// Blocks, so callers run it on a blocking thread, never the JS thread.
    pub(crate) fn drain(&self) {
        let budget = self
            .mark_stopping()
            .saturating_duration_since(Instant::now());
        self.hub.shutdown(budget);
        self.drained.send_replace(true);
    }

    /// Resolve once the hub has drained, or at the deadline. Past it, whatever
    /// is still buffered or not yet emitted is counted as dropped for shutdown.
    async fn wait_drained(&self) -> Result<()> {
        let remaining = self
            .mark_stopping()
            .saturating_duration_since(Instant::now());
        let mut drained = self.drained.subscribe();
        // The sender lives in `self`, so `wait_for` cannot fail on a closed
        // channel. `is_ok` drops the borrowed guard before the next await.
        let settled = tokio::time::timeout(remaining, drained.wait_for(|done| *done))
            .await
            .is_ok();
        if !settled {
            // An in-flight job is still running: close the hub at the deadline
            // anyway. Its later events are counted as dropped, never sent.
            let hub = Arc::clone(&self.hub);
            spawn_blocking(move || hub.shutdown(Duration::ZERO))
                .await
                .map_err(join_to_napi_err)?;
        }
        Ok(())
    }
}

/// One event sink's counters at one moment.
#[napi(object)]
pub struct JsSinkStats {
    /// The sink's configured name.
    pub name: String,
    /// The sink's `kind`, e.g. `http`.
    pub kind: String,
    /// Events the destination accepted.
    pub delivered: i64,
    /// Events dropped because the sink's buffer was full.
    pub dropped_buffer_full: i64,
    /// Events the destination refused outright.
    pub dropped_rejected: i64,
    /// Events dropped after every delivery attempt failed.
    pub dropped_failed: i64,
    /// Events dropped because the worker was stopping.
    pub dropped_shutdown: i64,
    /// Events accepted but not yet delivered or dropped (approximate).
    pub queued: i64,
}

impl From<SinkStats> for JsSinkStats {
    fn from(stats: SinkStats) -> Self {
        Self {
            name: stats.name,
            kind: stats.kind.to_string(),
            delivered: saturating(stats.delivered),
            dropped_buffer_full: saturating(stats.dropped_buffer_full),
            dropped_rejected: saturating(stats.dropped_rejected),
            dropped_failed: saturating(stats.dropped_failed),
            dropped_shutdown: saturating(stats.dropped_shutdown),
            queued: saturating(stats.queued),
        }
    }
}

/// JS numbers have no `u64`; a counter past `i64::MAX` is unreachable in practice.
fn saturating(count: u64) -> i64 {
    i64::try_from(count).unwrap_or(i64::MAX)
}

#[napi]
impl JsWorker {
    /// Each event sink's counters, in configuration order. Empty when the
    /// worker was started without sinks. Still readable after `stop()`, where
    /// it reports the final counts once `wait_event_drain` resolves.
    #[napi]
    pub fn event_sink_stats(&self) -> Vec<JsSinkStats> {
        let Some(events) = &self.events else {
            return Vec::new();
        };
        events
            .hub
            .stats()
            .into_iter()
            .map(JsSinkStats::from)
            .collect()
    }

    /// Resolve once a stopped worker's buffered events are delivered or
    /// counted as dropped, within the drain budget that `stop` started.
    /// Resolves at once for a worker without sinks.
    #[napi]
    pub async fn wait_event_drain(&self) -> Result<()> {
        match &self.events {
            Some(events) => events.wait_drained().await,
            None => Ok(()),
        }
    }
}
