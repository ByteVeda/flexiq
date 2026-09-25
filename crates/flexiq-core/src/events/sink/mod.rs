//! The seam between the hub and a destination.
//!
//! The hub owns buffering, batching, retries and counting; a backend only
//! turns one batch into one blocking send and says how it went.

use std::sync::Arc;

use super::config::{EventsConfigError, SinkConfig};
use super::event::JobEvent;

#[cfg(feature = "events-http")]
mod http;
#[cfg(feature = "events-kafka")]
mod kafka;
#[cfg(feature = "events-nats")]
mod nats;
#[cfg(feature = "redis")]
mod redis_streams;
#[cfg(any(feature = "events-kafka", feature = "events-nats"))]
mod tls;

/// How one delivery attempt of one batch went.
// Only feature-gated backends construct the first two; the hub matches on
// them in every build, including one compiled with no sink kind at all.
#[cfg_attr(
    not(any(
        feature = "events-http",
        feature = "events-kafka",
        feature = "events-nats",
        feature = "redis"
    )),
    allow(dead_code)
)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeliveryResult {
    /// The destination accepted the whole batch.
    Delivered,
    /// A transient failure; the hub backs off and tries again. The message is
    /// logged, so it must carry no payload and no credentials.
    Retry(String),
    /// The destination refused the batch for good; retrying cannot help.
    Reject(String),
}

/// A destination the hub's per-sink thread delivers to.
///
/// A panic is caught and read as a rejection, but the default panic hook
/// still prints its message: never format event data or a secret into one.
pub(crate) trait SinkBackend: Send + 'static {
    /// Blocking. Sends the whole batch or reports why not.
    fn deliver(&mut self, batch: &[Arc<JobEvent>]) -> DeliveryResult;
}

/// Build the backend a sink's `kind` names.
///
/// A kind this build was compiled without is refused here, at hub start, so a
/// config that cannot deliver never looks healthy.
#[cfg_attr(
    not(any(
        feature = "events-http",
        feature = "events-kafka",
        feature = "events-nats",
        feature = "redis"
    )),
    allow(unused_variables)
)]
pub(crate) fn build_backend(
    sink: &SinkConfig,
    source: &str,
) -> Result<Box<dyn SinkBackend>, EventsConfigError> {
    match sink {
        #[cfg(feature = "events-http")]
        SinkConfig::Http(config) => Ok(Box::new(http::HttpSink::new(config, source)?)),
        #[cfg(not(feature = "events-http"))]
        SinkConfig::Http(_) => Err(not_compiled(sink, "events-http")),
        #[cfg(feature = "redis")]
        SinkConfig::RedisStreams(config) => Ok(Box::new(redis_streams::RedisStreamsSink::new(
            config, source,
        )?)),
        #[cfg(not(feature = "redis"))]
        SinkConfig::RedisStreams(_) => Err(not_compiled(sink, "redis")),
        #[cfg(feature = "events-kafka")]
        SinkConfig::Kafka(config) => Ok(Box::new(kafka::KafkaSink::new(config, source)?)),
        #[cfg(not(feature = "events-kafka"))]
        SinkConfig::Kafka(_) => Err(not_compiled(sink, "events-kafka")),
        #[cfg(feature = "events-nats")]
        SinkConfig::Nats(config) => Ok(Box::new(nats::NatsSink::new(config, source)?)),
        #[cfg(not(feature = "events-nats"))]
        SinkConfig::Nats(_) => Err(not_compiled(sink, "events-nats")),
    }
}

// Unused only when every sink kind is compiled in, and so never refused.
#[cfg_attr(
    all(
        feature = "events-http",
        feature = "events-kafka",
        feature = "events-nats",
        feature = "redis"
    ),
    allow(dead_code)
)]
fn not_compiled(sink: &SinkConfig, feature: &'static str) -> EventsConfigError {
    EventsConfigError::NotCompiled {
        sink: sink.name().to_string(),
        kind: sink.kind(),
        feature,
    }
}
