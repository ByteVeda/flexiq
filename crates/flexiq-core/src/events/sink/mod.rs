//! The seam between the hub and a destination.
//!
//! The hub owns buffering, batching, retries and counting; a backend only
//! turns one batch into one blocking send and says how it went.

use std::sync::Arc;

use super::config::{EventsConfigError, SinkConfig};
use super::event::JobEvent;

#[cfg(feature = "events-http")]
mod http;

/// How one delivery attempt of one batch went.
// Only feature-gated backends construct the first two; the hub matches on
// them in every build, including one compiled with no sink kind at all.
#[cfg_attr(not(feature = "events-http"), allow(dead_code))]
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
#[cfg_attr(not(feature = "events-http"), allow(unused_variables))]
pub(crate) fn build_backend(
    sink: &SinkConfig,
    source: &str,
) -> Result<Box<dyn SinkBackend>, EventsConfigError> {
    match sink {
        #[cfg(feature = "events-http")]
        SinkConfig::Http(config) => Ok(Box::new(http::HttpSink::new(config, source)?)),
        #[cfg(not(feature = "events-http"))]
        SinkConfig::Http(_) => Err(not_compiled(sink, "events-http")),
        SinkConfig::RedisStreams(_) => Err(not_compiled(sink, "redis")),
    }
}

fn not_compiled(sink: &SinkConfig, feature: &'static str) -> EventsConfigError {
    EventsConfigError::NotCompiled {
        sink: sink.name().to_string(),
        kind: sink.kind(),
        feature,
    }
}
