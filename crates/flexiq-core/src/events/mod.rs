//! Job lifecycle events sent out of the process as CloudEvents.
//!
//! The scheduler and producer doors [`emit`](EventHub::emit) a [`JobEvent`]
//! per transition; the [`EventHub`] fans it out to the configured sinks, each
//! with its own bounded buffer and delivery thread.
//!
//! # Delivery semantics
//!
//! At-most-once overall: an event is lost when its sink's buffer is full, when
//! its delivery attempts run out, or when the process dies. Each accepted
//! event may also be delivered more than once, so consumers dedupe on the
//! CloudEvents `id`, which is `<job_id>:<attempt>:<epoch>:<type>` with `-` for
//! an unknown part (see [`JobEvent::id`]). Never exactly-once.
//!
//! # No back-pressure
//!
//! [`EventHub::emit`] never blocks and never does I/O. A full buffer drops the
//! event and counts it in `flexiq_events_dropped_total{reason="buffer_full"}`;
//! a slow sink never slows the queue.
//!
//! # Payloads
//!
//! Off by default. A sink sends job payloads only with `include_payload`,
//! and the hub logs a warning naming each such sink at start.

pub mod config;
pub mod event;
mod hub;
pub mod reason;
pub(crate) mod sink;

pub use config::{
    Delivery, EventsConfig, EventsConfigError, Filter, HttpSinkConfig, RedisSinkConfig, SinkConfig,
};
pub use event::{EventType, JobEvent, CLOUDEVENTS_TYPE_PREFIX, DEFAULT_SOURCE};
pub use hub::{EventHub, SinkStats};

#[cfg(test)]
pub(crate) use hub::test_support;
