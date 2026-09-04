//! The counters the gRPC door keeps, and the text they render as.
//!
//! A `Mutex<BTreeMap<..>>` rather than a lock-free registry: the map is touched
//! once per call for a few hundred nanoseconds, the process already serialises
//! every call behind a bounded storage pool, and a `BTreeMap` renders in a
//! stable order without a sort. Nothing in this crate had a metrics
//! abstraction to reuse — the dashboard builds its exposition ad hoc — and one
//! crate-sized dependency for two counters would be the larger change.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::metrics::escape_label;

/// One completed call, as the layer observed it.
pub struct Observation {
    /// The call's identity in the exposition — never a raw request path.
    pub method: Cow<'static, str>,
    /// Which door carried it: `grpc` or `http`.
    pub door: &'static str,
    /// The canonical `google.rpc.Code` name, or `OK`.
    pub code: &'static str,
    /// Time to the response head.
    pub elapsed: Duration,
}

/// Sum and count for one method's latency.
#[derive(Debug, Default, Clone, Copy)]
struct Latency {
    seconds: f64,
    calls: u64,
}

#[derive(Debug, Default)]
struct Tallies {
    /// `(method, door, code)` → calls.
    calls: BTreeMap<(Cow<'static, str>, &'static str, &'static str), u64>,
    /// `(method, door)` → latency.
    latency: BTreeMap<(Cow<'static, str>, &'static str), Latency>,
}

/// Per-method call counters for the gRPC listener.
#[derive(Debug, Default)]
pub struct RpcMetrics {
    tallies: Mutex<Tallies>,
}

impl RpcMetrics {
    /// A registry with nothing recorded yet.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Record one completed call.
    pub fn record(&self, observation: Observation) {
        // A poisoned lock would mean a panic inside `record` itself; the
        // counters are not worth failing a request over, so the poison is
        // stepped over rather than propagated.
        let mut tallies = match self.tallies.lock() {
            Ok(tallies) => tallies,
            Err(poisoned) => poisoned.into_inner(),
        };
        *tallies
            .calls
            .entry((
                observation.method.clone(),
                observation.door,
                observation.code,
            ))
            .or_default() += 1;
        let latency = tallies
            .latency
            .entry((observation.method, observation.door))
            .or_default();
        latency.seconds += observation.elapsed.as_secs_f64();
        latency.calls += 1;
    }

    /// The Prometheus exposition for what has been recorded.
    ///
    /// The duration is time to the **response head**, which is the whole call
    /// for every unary RPC — the entire producer service and every JSON facade
    /// route. For `Attach`, `Health/Watch` and reflection it is the time to
    /// open the stream, not the time the stream then lived, and their terminal
    /// status is reported as `OK` because it rides trailers this layer never
    /// sees. Neither is a number to alert a stream on; `flexiq_executors` is.
    pub fn render(&self) -> String {
        let tallies = match self.tallies.lock() {
            Ok(tallies) => tallies,
            Err(poisoned) => poisoned.into_inner(),
        };

        let mut body = String::new();
        body.push_str("# HELP flexiq_grpc_requests_total Calls answered by the gRPC listener.\n");
        body.push_str("# TYPE flexiq_grpc_requests_total counter\n");
        for ((method, door, code), count) in &tallies.calls {
            body.push_str(&format!(
                "flexiq_grpc_requests_total{{method=\"{}\",door=\"{door}\",code=\"{code}\"}} \
                 {count}\n",
                escape_label(method)
            ));
        }

        body.push_str("# HELP flexiq_grpc_request_duration_seconds Time to the response head.\n");
        body.push_str("# TYPE flexiq_grpc_request_duration_seconds summary\n");
        for ((method, door), latency) in &tallies.latency {
            let method = escape_label(method);
            body.push_str(&format!(
                "flexiq_grpc_request_duration_seconds_sum{{method=\"{method}\",door=\"{door}\"}} \
                 {}\n",
                latency.seconds
            ));
            body.push_str(&format!(
                "flexiq_grpc_request_duration_seconds_count{{method=\"{method}\",door=\"{door}\"}} \
                 {}\n",
                latency.calls
            ));
        }

        body
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observed(method: &'static str, code: &'static str) -> Observation {
        Observation {
            method: Cow::Borrowed(method),
            door: "grpc",
            code,
            elapsed: Duration::from_millis(10),
        }
    }

    #[test]
    fn calls_accumulate_per_method_and_code() {
        let metrics = RpcMetrics::new();
        metrics.record(observed("flexiq.v1.ProducerService/Enqueue", "OK"));
        metrics.record(observed("flexiq.v1.ProducerService/Enqueue", "OK"));
        metrics.record(observed(
            "flexiq.v1.ProducerService/Enqueue",
            "UNAUTHENTICATED",
        ));

        let body = metrics.render();
        assert!(body.contains(
            "flexiq_grpc_requests_total{method=\"flexiq.v1.ProducerService/Enqueue\",\
             door=\"grpc\",code=\"OK\"} 2"
        ));
        assert!(body.contains(
            "flexiq_grpc_requests_total{method=\"flexiq.v1.ProducerService/Enqueue\",\
             door=\"grpc\",code=\"UNAUTHENTICATED\"} 1"
        ));
        assert!(body.contains(
            "flexiq_grpc_request_duration_seconds_count{method=\"flexiq.v1.ProducerService/\
             Enqueue\",door=\"grpc\"} 3"
        ));
    }

    /// An empty registry still has to be a parseable exposition, or a scrape
    /// before the first call is an error rather than a zero.
    #[test]
    fn nothing_recorded_still_renders_the_headers() {
        let body = RpcMetrics::new().render();
        assert!(body.contains("# TYPE flexiq_grpc_requests_total counter"));
        assert!(!body.contains("flexiq_grpc_requests_total{"));
    }
}
