//! Event sinks for a queue's workers: the configuration a `Queue` is built
//! with, the hub each `run_worker` starts from it, and that hub's counters.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use flexiq_core::{EventHub, EventsConfig, EventsConfigError, SinkStats};

use super::PyQueue;

/// A queue's event-sink settings, validated when the `Queue` is built.
pub(crate) struct EventSinks {
    /// The configuration document, already known to parse.
    document: String,
    /// How long a stopping worker waits for buffered events to go out.
    drain: Duration,
}

impl EventSinks {
    /// Check the document and drain budget once, at construction, so a bad
    /// config fails where it is written rather than when a worker starts.
    pub(crate) fn parse(document: Option<String>, drain_secs: f64) -> PyResult<Option<Self>> {
        let drain = Duration::try_from_secs_f64(drain_secs).map_err(|_| {
            PyValueError::new_err(format!(
                "event_sinks_drain must be a finite, non-negative number of seconds, \
                 got {drain_secs}"
            ))
        })?;
        let Some(document) = document else {
            return Ok(None);
        };
        EventsConfig::parse(&document).map_err(config_error)?;
        Ok(Some(Self { document, drain }))
    }
}

/// One `run_worker` call's hub. Published for `event_sink_stats` while the run
/// lives; dropping it drains the hub and withdraws it, on every exit path.
pub(crate) struct EventsRun<'q> {
    slot: &'q Mutex<Option<Arc<EventHub>>>,
    hub: Arc<EventHub>,
    drain: Duration,
}

impl EventsRun<'_> {
    /// The hub, for `Scheduler::set_events`.
    pub(crate) fn hub(&self) -> Arc<EventHub> {
        Arc::clone(&self.hub)
    }
}

impl Drop for EventsRun<'_> {
    fn drop(&mut self) {
        let (hub, drain) = (&self.hub, self.drain);
        // The drain blocks for up to `drain`; other Python threads keep running.
        Python::attach(|py| py.detach(|| hub.shutdown(drain)));
        let mut slot = lock(self.slot);
        // A sibling run on the same queue may have published its own hub since.
        if slot.as_ref().is_some_and(|live| Arc::ptr_eq(live, hub)) {
            *slot = None;
        }
    }
}

impl PyQueue {
    /// Start this run's hub, if the queue has sinks. A sink kind this build
    /// lacks is refused here, since only a started hub builds its backends.
    pub(crate) fn start_event_hub(&self) -> PyResult<Option<EventsRun<'_>>> {
        let Some(sinks) = &self.event_sinks else {
            return Ok(None);
        };
        let hub = Arc::new(EventHub::from_json(&sinks.document).map_err(config_error)?);
        *lock(&self.event_hub) = Some(Arc::clone(&hub));
        Ok(Some(EventsRun {
            slot: &self.event_hub,
            hub,
            drain: sinks.drain,
        }))
    }
}

#[pymethods]
impl PyQueue {
    /// Each event sink's counters for the worker running on this queue, in
    /// configuration order. Empty when no worker with sinks is running.
    pub fn event_sink_stats<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyDict>>> {
        let hub = lock(&self.event_hub).clone();
        let Some(hub) = hub else {
            return Ok(Vec::new());
        };
        hub.stats()
            .iter()
            .map(|stats| stats_dict(py, stats))
            .collect()
    }
}

fn stats_dict<'py>(py: Python<'py>, stats: &SinkStats) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("name", &stats.name)?;
    dict.set_item("kind", stats.kind)?;
    dict.set_item("delivered", stats.delivered)?;
    dict.set_item("dropped_buffer_full", stats.dropped_buffer_full)?;
    dict.set_item("dropped_rejected", stats.dropped_rejected)?;
    dict.set_item("dropped_failed", stats.dropped_failed)?;
    dict.set_item("dropped_shutdown", stats.dropped_shutdown)?;
    dict.set_item("queued", stats.queued)?;
    Ok(dict)
}

fn config_error(error: EventsConfigError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

/// The slot holds only an `Option<Arc>`, so a poisoned lock has nothing to repair.
fn lock(slot: &Mutex<Option<Arc<EventHub>>>) -> MutexGuard<'_, Option<Arc<EventHub>>> {
    slot.lock().unwrap_or_else(PoisonError::into_inner)
}
