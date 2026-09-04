//! Telemetry for the gRPC listener.
//!
//! The dashboard publishes `/metrics`, but a deployment may enable the gRPC
//! role and nothing else — the Helm chart offers exactly that combination —
//! and such a process had no scrape target at all. So this door serves its own
//! `/metrics`, over the plain HTTP the JSON facade already made available on
//! the same port, carrying the same storage gauges plus what only this door
//! knows: how many calls it answered, with what status, and how long they took.
//!
//! **It is credentialled like everything else here.** `/metrics` is not a
//! public path: [`crate::grpc::auth::gate`] requires a valid token on it, of
//! either scope, because an operational read is not a data door but it is
//! still a read of one tenant's state.
//!
//! ## Label cardinality is closed by construction
//!
//! A `method` label is never a raw request path. A gRPC path is kept only when
//! it names an RPC this build actually serves, and a facade path is resolved
//! back to the binding it reached — so `/v1/jobs/018f…` is `GetJob`, not one
//! series per job. Everything else, including anything unrouted, collapses to
//! `other`. A caller cannot mint a series by asking for a path that does not
//! exist.

pub mod layer;
pub mod registry;
pub mod routes;

use std::borrow::Cow;

pub use layer::MetricsLayer;
pub use registry::{Observation, RpcMetrics};
pub use routes::router;

use crate::grpc::facade;

/// The path `/metrics` is served on, named once.
pub const METRICS_PATH: &str = "/metrics";

/// The label a path that is not a served method collapses to.
const OTHER: &str = "other";

/// The `flexiq.v1` service, as the wire spells it.
const PRODUCER_SERVICE: &str = "flexiq.v1.ProducerService";

/// Full method paths this build serves outside `flexiq.v1`.
///
/// Written out rather than derived: the executor package's RPCs are not in the
/// producer descriptor, and health and reflection come from crates that publish
/// no such list. Five names is cheaper than a lookup that could go stale
/// silently, and a name missing from here degrades to `other` rather than
/// misreporting.
const OTHER_SERVED_METHODS: [&str; 5] = [
    "flexiq.executor.v1.ExecutorService/Attach",
    "flexiq.executor.v1.ExecutorService/Heartbeat",
    "grpc.health.v1.Health/Check",
    "grpc.health.v1.Health/Watch",
    "grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
];

/// The `method` and `door` labels for one request.
pub fn labels(
    method: &http::Method,
    path: &str,
    headers: &http::HeaderMap,
) -> (Cow<'static, str>, &'static str) {
    let door = if facade::is_grpc(headers) {
        "grpc"
    } else {
        "http"
    };

    if path == METRICS_PATH {
        return (Cow::Borrowed(METRICS_PATH.trim_start_matches('/')), door);
    }

    let bare = path.trim_start_matches('/');

    if let Some(rpc) = bare.strip_prefix(PRODUCER_SERVICE).and_then(|rest| {
        let name = rest.strip_prefix('/')?;
        facade::routes::Rpc::ALL
            .into_iter()
            .find(|rpc| rpc.as_str() == name)
    }) {
        return (
            Cow::Owned(format!("{PRODUCER_SERVICE}/{}", rpc.as_str())),
            door,
        );
    }

    if let Some(served) = OTHER_SERVED_METHODS
        .into_iter()
        // `v1alpha` reflection is the same method under an older package name;
        // folding it in keeps one series for one question.
        .find(|served| *served == bare || bare == served.replace(".v1.", ".v1alpha."))
    {
        return (Cow::Borrowed(served), door);
    }

    if let Some(binding) = facade::routes::resolve(method, path) {
        return (
            Cow::Owned(format!("{PRODUCER_SERVICE}/{}", binding.rpc().as_str())),
            door,
        );
    }

    (Cow::Borrowed(OTHER), door)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(content_type: &str) -> http::HeaderMap {
        let mut headers = http::HeaderMap::new();
        if !content_type.is_empty() {
            headers.insert(
                http::header::CONTENT_TYPE,
                content_type.parse().expect("a valid header value"),
            );
        }
        headers
    }

    fn grpc() -> http::HeaderMap {
        headers("application/grpc")
    }

    #[test]
    fn a_served_rpc_keeps_its_full_method_name() {
        let (method, door) = labels(
            &http::Method::POST,
            "/flexiq.v1.ProducerService/Enqueue",
            &grpc(),
        );
        assert_eq!(method, "flexiq.v1.ProducerService/Enqueue");
        assert_eq!(door, "grpc");
    }

    /// The whole point of the closed set: an unrouted path must not be able to
    /// mint a series, and a job id must not become one either.
    #[test]
    fn nothing_a_caller_invents_becomes_a_series() {
        for path in [
            "/flexiq.v1.ProducerService/Bogus",
            "/flexiq.v1.SomethingElse/Enqueue",
            "/nope",
            "/v1/jobs/018f/extra/segments",
        ] {
            let (method, _) = labels(&http::Method::POST, path, &grpc());
            assert_eq!(method, OTHER, "unexpected label for {path}");
        }
    }

    #[test]
    fn a_facade_path_resolves_to_the_rpc_it_reached() {
        let (method, door) = labels(&http::Method::GET, "/v1/jobs/018fabc", &headers(""));
        assert_eq!(method, "flexiq.v1.ProducerService/GetJob");
        assert_eq!(door, "http");
    }

    /// `GET /v1/jobs` and `POST /v1/jobs` are two bindings on one path.
    #[test]
    fn the_verb_picks_between_two_bindings_on_one_path() {
        let (listed, _) = labels(&http::Method::GET, "/v1/jobs", &headers(""));
        let (enqueued, _) = labels(&http::Method::POST, "/v1/jobs", &headers(""));
        assert_eq!(listed, "flexiq.v1.ProducerService/ListJobs");
        assert_eq!(enqueued, "flexiq.v1.ProducerService/Enqueue");
    }

    /// The one route whose registered pattern is not the path a client types.
    #[test]
    fn the_cancel_suffix_resolves_to_cancel_and_not_to_get() {
        let (method, _) = labels(&http::Method::POST, "/v1/jobs/018fabc:cancel", &headers(""));
        assert_eq!(method, "flexiq.v1.ProducerService/CancelJob");
    }

    #[test]
    fn both_reflection_package_names_fold_into_one_series() {
        for path in [
            "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
            "/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo",
        ] {
            let (method, _) = labels(&http::Method::POST, path, &grpc());
            assert_eq!(
                method,
                "grpc.reflection.v1.ServerReflection/ServerReflectionInfo"
            );
        }
    }
}
