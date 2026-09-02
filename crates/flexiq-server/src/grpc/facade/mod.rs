//! The JSON facade: `flexiq.v1` for a client that speaks HTTP and not gRPC.
//!
//! `structured` arguments exist so that a client with no CBOR library can
//! enqueue (#715). That is only true if such a client has a door to knock on,
//! and gRPC from a shell script is not one. So the producer RPCs are served a
//! second way, over ordinary HTTP with JSON bodies, on **the same listener**:
//!
//! ```text
//! curl -X POST http://localhost:50051/v1/jobs \
//!   -H "authorization: Bearer $FLEXIQ_TOKEN" -H "content-type: application/json" \
//!   -d '{"taskName": "send_email", "structured": {"args": ["a@b.c"]}}'
//! ```
//!
//! ## What it is, and what it is not
//!
//! It is hand-written, deliberately. Rust has no mature `grpc-gateway`: every
//! crate that transcodes from `google.api.http` is at 0.1 or 0.2 with a single
//! maintainer, and the sidecar alternative puts a proxy in the data path of a
//! project whose whole pitch is that it needs no broker.
//!
//! It is **not a second implementation of anything**. [`routes`]'s handlers
//! call the same `ProducerService` trait methods the tonic codec calls, on the
//! same `Producer` value, behind the same
//! [`AuthLayer`](crate::grpc::auth::AuthLayer) — one process, one handler, no
//! loopback hop. What differs is the reading and the writing, and both live
//! in [`json`].
//!
//! It serves the `flexiq.v1` package and nothing else. The executor service is
//! not transcoded: a worker surface has different credentials, different
//! failure modes, and no reason to be reachable from a browser. Temporal's
//! protos carry the comment "We do not expose worker API to HTTP" once per
//! worker RPC; here it is a property of which package the facade covers, and a
//! test.
//!
//! Streaming is not transcoded either, and there is nothing to transcode: v1
//! has no server stream. A completion watch is a real feature deserving its own
//! design rather than a field.
//!
//! ## One listener, two doors
//!
//! The listener accepts HTTP/1.1 as well as HTTP/2, and what tells the two
//! doors apart is the content type: `application/grpc*` is a gRPC call, and
//! everything else is this one. That matters in exactly one place beyond
//! routing — a refusal. An unauthenticated gRPC call must come back with
//! `grpc-status` trailers, and an unauthenticated `curl` must come back with a
//! JSON body and an HTTP status, so [`refusal`] renders the one `Status` for
//! whichever door asked.

#[cfg(test)]
pub mod descriptor;
pub mod error;
pub mod json;
pub mod routes;

use axum::response::Response;
use axum::Router;
use http::request::Parts;
use http::{header, HeaderMap};
use tonic::Status;

use crate::grpc::producer::Producer;
use crate::grpc::status::WireError;

/// The facade's routes, with the two fallbacks that keep every answer JSON.
pub fn router(producer: Producer) -> Router {
    routes::router(producer)
        // A path this door serves, with a method it does not: there is no such
        // RPC, which is the same answer an unrouted path gets.
        .method_not_allowed_fallback(unrouted)
        .fallback(unrouted)
}

/// Whether a request came in through the gRPC door.
pub fn is_grpc(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/grpc"))
}

/// One refusal, rendered for the door the request arrived at.
///
/// The gRPC arm is exactly what `Routes` answers on its own — a `Status` in the
/// response headers — so nothing about a gRPC caller's experience changes by
/// this module existing.
pub fn refusal(headers: &HeaderMap, status: &Status) -> http::Response<tonic::body::Body> {
    if is_grpc(headers) {
        status.clone().into_http::<tonic::body::Body>()
    } else {
        error::response(status).map(tonic::body::Body::new)
    }
}

/// Nothing is served here.
///
/// Both fallbacks answer this, and both answer `UNIMPLEMENTED` — the code the
/// gRPC router answers for a method it does not have, so the two doors give one
/// answer to one mistake. A gRPC request that reaches here gets that answer in
/// the shape gRPC expects it.
async fn unrouted(parts: Parts) -> Response {
    if is_grpc(&parts.headers) {
        return Status::unimplemented("").into_http::<axum::body::Body>();
    }
    error::refuse(WireError::no_such_method(
        parts.method.as_str(),
        parts.uri.path(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderValue, StatusCode};

    fn headers(content_type: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_str(content_type).expect("an ASCII header value"),
        );
        headers
    }

    #[test]
    fn a_grpc_content_type_is_recognised_with_or_without_its_codec() {
        for value in [
            "application/grpc",
            "application/grpc+proto",
            "application/grpc-web+proto",
        ] {
            assert!(is_grpc(&headers(value)), "value: {value}");
        }
        for value in ["application/json", "text/plain", ""] {
            assert!(!is_grpc(&headers(value)), "value: {value}");
        }
        // A `curl` with no body sends no content type at all.
        assert!(!is_grpc(&HeaderMap::new()));
    }

    /// The property the whole module turns on: one `Status`, two renderings,
    /// and neither door is handed the other's.
    #[test]
    fn a_refusal_is_rendered_for_the_door_that_asked() {
        let status = Status::from(WireError::unauthenticated());

        let over_grpc = refusal(&headers("application/grpc"), &status);
        assert_eq!(over_grpc.status(), StatusCode::OK);
        assert!(over_grpc.headers().contains_key("grpc-status"));

        let over_json = refusal(&headers("application/json"), &status);
        assert_eq!(over_json.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            over_json.headers()[header::CONTENT_TYPE],
            "application/json"
        );
        assert!(!over_json.headers().contains_key("grpc-status"));
    }
}
