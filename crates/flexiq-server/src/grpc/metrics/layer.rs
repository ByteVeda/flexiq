//! The tower layer that counts calls.
//!
//! It wraps the router **outside** [`crate::grpc::auth::AuthLayer`], so a call
//! refused for want of a credential is counted like any other. A refusal that
//! never appears in the metrics is the one an operator most needs to see: it is
//! how a misconfigured client looks.
//!
//! The shape — `Layer` + a `Service` that clones itself into its own future —
//! is the one `auth/layer.rs` already establishes, and for the same reason:
//! `poll_ready`'s reservation belongs to the service that was polled, so that
//! one moves into the future and the clone stays behind.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use tonic::Code;
use tower_layer::Layer;
use tower_service::Service;

use super::registry::{Observation, RpcMetrics};
use crate::grpc::facade::error::{code_name, AnsweredCode};

/// Counts every call the listener answers.
#[derive(Debug, Clone)]
pub struct MetricsLayer {
    metrics: Arc<RpcMetrics>,
}

impl MetricsLayer {
    /// Record into `metrics`.
    pub fn new(metrics: Arc<RpcMetrics>) -> Self {
        Self { metrics }
    }
}

impl<S> Layer<S> for MetricsLayer {
    type Service = Measured<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Measured {
            inner,
            metrics: Arc::clone(&self.metrics),
        }
    }
}

/// A service whose calls are counted.
#[derive(Debug, Clone)]
pub struct Measured<S> {
    inner: S,
    metrics: Arc<RpcMetrics>,
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for Measured<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = http::Response<ResBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<ReqBody>) -> Self::Future {
        let (method, door) =
            super::labels(request.method(), request.uri().path(), request.headers());
        let metrics = Arc::clone(&self.metrics);
        let started = Instant::now();

        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            let answered = inner.call(request).await;
            if let Ok(response) = &answered {
                metrics.record(Observation {
                    method,
                    door,
                    code: code_name(answered_code(response)),
                    elapsed: started.elapsed(),
                });
            }
            answered
        })
    }
}

/// The code a response answered with, as far as its head can say.
///
/// Three sources, in order of exactness. The facade records the code it chose
/// in an extension, because its HTTP status cannot express which of several
/// codes it stood for. tonic writes `grpc-status` into the header block for a
/// trailers-only response, which is every refusal and every unary handler that
/// returned `Err`. Anything else answered without a status in its head, which
/// for a unary RPC means it succeeded.
///
/// The gap this leaves is a *stream* that fails after its head — its status
/// rides trailers this layer never sees, and it is counted `OK`. That is stated
/// where the metric is rendered rather than papered over by wrapping every
/// response body to watch for trailers.
fn answered_code<B>(response: &http::Response<B>) -> Code {
    if let Some(AnsweredCode(code)) = response.extensions().get::<AnsweredCode>() {
        return *code;
    }
    response
        .headers()
        .get("grpc-status")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i32>().ok())
        .map_or(Code::Ok, Code::from_i32)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(build: impl FnOnce(&mut http::Response<()>)) -> http::Response<()> {
        let mut response = http::Response::new(());
        build(&mut response);
        response
    }

    #[test]
    fn a_head_with_no_status_is_a_success() {
        assert_eq!(answered_code(&response(|_| {})), Code::Ok);
    }

    #[test]
    fn a_trailers_only_refusal_is_read_off_the_header() {
        let response = response(|response| {
            response.headers_mut().insert(
                "grpc-status",
                http::HeaderValue::from_static("16"), // UNAUTHENTICATED
            );
        });
        assert_eq!(answered_code(&response), Code::Unauthenticated);
    }

    /// 400 stands for three codes; the extension is what tells them apart.
    #[test]
    fn the_facade_extension_wins_over_the_http_status() {
        let response = response(|response| {
            *response.status_mut() = http::StatusCode::BAD_REQUEST;
            response
                .extensions_mut()
                .insert(AnsweredCode(Code::FailedPrecondition));
        });
        assert_eq!(answered_code(&response), Code::FailedPrecondition);
    }
}
