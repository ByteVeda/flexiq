//! Event sinks for a worker: the hub a worker starts from its `events`
//! option, and that hub's counters on the worker handle.

use std::sync::Arc;
use std::time::Duration;

use flexiq_core::{EventHub, SinkStats};
use napi::bindgen_prelude::Result;
use napi_derive::napi;

use crate::error::invalid_arg;
use crate::worker::JsWorker;

/// How long a stopping worker waits for buffered events, when unset.
const DEFAULT_DRAIN_MS: u32 = 5_000;

/// A worker's running hub and the budget it drains within on stop.
pub(crate) struct WorkerEvents {
    pub(crate) hub: Arc<EventHub>,
    pub(crate) drain: Duration,
}

impl WorkerEvents {
    /// Start the hub `document` describes, if any. A bad document, or a sink
    /// kind this build lacks, throws here, before the worker starts anything.
    pub(crate) fn start(document: Option<&str>, drain_ms: Option<u32>) -> Result<Option<Self>> {
        let Some(document) = document else {
            return Ok(None);
        };
        let hub = EventHub::from_json(document).map_err(|err| invalid_arg(err.to_string()))?;
        Ok(Some(Self {
            hub: Arc::new(hub),
            drain: Duration::from_millis(u64::from(drain_ms.unwrap_or(DEFAULT_DRAIN_MS))),
        }))
    }

    /// Deliver what is buffered within the drain budget. Blocks, so callers
    /// run it on a blocking thread, never the JS thread.
    pub(crate) fn drain(&self) {
        self.hub.shutdown(self.drain);
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
    /// it reports the final counts once the drain finishes.
    #[napi]
    pub fn event_sink_stats(&self) -> Vec<JsSinkStats> {
        self.events
            .as_ref()
            .map(|hub| hub.stats().into_iter().map(JsSinkStats::from).collect())
            .unwrap_or_default()
    }
}
