//! The one place every gRPC call is checked.
//!
//! This is a `tower::Layer` over the whole router rather than a
//! `tonic::service::InterceptedService` per service, and rather than a check
//! inside each handler. `Server::layer` takes `L: Layer<Routes>`, so one
//! registration covers every service the router carries and every RPC those
//! services will ever grow — which is the acceptance criterion for #716: *a
//! newly added RPC is checked without anyone touching auth code.* The dashboard
//! router is wrapped once for the same reason
//! (`dashboard/auth/middleware.rs::gate_request`).
//!
//! A `tonic` interceptor is not enough, and the reason is worth writing down so
//! nobody simplifies this back into one: an `Interceptor` is handed a
//! `Request<()>` built from the request's metadata and extensions, and a
//! `tonic::Request` has no URI. The gate needs the path, because
//! `grpc.health.v1` must stay reachable without a credential — a kubelet
//! `grpc:` probe has no way to send one.
//!
//! The [`Principal`] is inserted into the request's extensions, which is how
//! the namespace reaches the handlers. A handler that finds none fails closed,
//! so a service registered *without* this layer serves nothing rather than
//! serving everything unauthenticated.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tonic::metadata::MetadataMap;
use tonic::Status;
use tower_layer::Layer;
use tower_service::Service;

use super::authenticator::Authenticator;
use super::gate::{self, Requirement};
use super::principal::Principal;
use crate::grpc::status::WireError;

/// Wraps a service so that every request is authenticated before it is routed.
#[derive(Clone)]
pub struct AuthLayer {
    authenticator: Arc<dyn Authenticator>,
}

impl AuthLayer {
    /// Gate every call with `authenticator`.
    pub fn new(authenticator: Arc<dyn Authenticator>) -> Self {
        Self { authenticator }
    }
}

impl std::fmt::Debug for AuthLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The authenticator holds a secret; nothing about it is printable.
        f.debug_struct("AuthLayer").finish_non_exhaustive()
    }
}

impl<S> Layer<S> for AuthLayer {
    type Service = Authenticated<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Authenticated {
            inner,
            authenticator: Arc::clone(&self.authenticator),
        }
    }
}

/// A service whose requests are authenticated first.
#[derive(Clone)]
pub struct Authenticated<S> {
    inner: S,
    authenticator: Arc<dyn Authenticator>,
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for Authenticated<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>>,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ResBody: Default + Send + 'static,
{
    type Response = http::Response<ResBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<ReqBody>) -> Self::Future {
        let (mut parts, body) = request.into_parts();

        // The headers are moved into the `MetadataMap` and moved back, rather
        // than cloned: an `Authenticator` is handed metadata by contract, and
        // the request still needs its headers afterwards. tonic's own
        // interceptor takes a request apart the same way.
        let metadata = MetadataMap::from_headers(std::mem::take(&mut parts.headers));
        let outcome = authorize(&*self.authenticator, parts.uri.path(), &metadata);
        parts.headers = metadata.into_headers();

        match outcome {
            Ok(Some(principal)) => {
                parts.extensions.insert(principal);
            }
            // A public path: routed with no principal, because nothing behind
            // one needs a namespace.
            Ok(None) => {}
            Err(status) => {
                let response = status.into_http();
                return Box::pin(std::future::ready(Ok(response)));
            }
        }

        let future = self.inner.call(http::Request::from_parts(parts, body));
        Box::pin(future)
    }
}

/// Apply the gate: identify the caller if the path needs one, then check that
/// what it carries covers the package it asked for.
///
/// `Ok(None)` is a public path. Split out of [`Authenticated::call`] so the
/// policy is testable without a service behind it.
fn authorize(
    authenticator: &dyn Authenticator,
    path: &str,
    metadata: &MetadataMap,
) -> Result<Option<Principal>, Status> {
    let requirement = gate::requirement(path);
    if requirement == Requirement::Public {
        return Ok(None);
    }

    let principal = authenticator.authenticate(metadata)?;
    if let Requirement::Scoped(scope) = requirement {
        if !principal.grants(scope) {
            return Err(WireError::scope_denied(scope.as_str()).into());
        }
    }
    Ok(Some(principal))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::auth::authenticator::Anonymous;
    use crate::grpc::auth::principal::{Scope, ScopeSet};
    use crate::grpc::status::reason;
    use tonic::Code;
    use tonic_types::StatusExt;

    /// An authenticator that answers with a fixed principal, for exercising the
    /// scope half of the gate without a credential scheme in the way.
    struct Fixed(Principal);

    impl Authenticator for Fixed {
        fn authenticate(&self, _metadata: &MetadataMap) -> Result<Principal, Status> {
            Ok(self.0.clone())
        }
    }

    /// An authenticator that refuses everything, for the other half.
    struct Refuses;

    impl Authenticator for Refuses {
        fn authenticate(&self, _metadata: &MetadataMap) -> Result<Principal, Status> {
            Err(WireError::unauthenticated().into())
        }
    }

    #[test]
    fn health_is_routed_without_a_credential() {
        let outcome = authorize(
            &Refuses,
            "/grpc.health.v1.Health/Check",
            &MetadataMap::new(),
        )
        .expect("health must not need a credential");
        assert!(outcome.is_none(), "health needs no principal either");
    }

    #[test]
    fn every_other_path_needs_one() {
        for path in [
            "/flexiq.v1.ProducerService/Enqueue",
            "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
            "/whatever",
        ] {
            let Err(status) = authorize(&Refuses, path, &MetadataMap::new()) else {
                panic!("{path} must be gated");
            };
            assert_eq!(status.code(), Code::Unauthenticated, "path: {path}");
        }
    }

    #[test]
    fn an_authenticated_call_carries_its_principal_onward() {
        let principal = authorize(
            &Anonymous::new("prod"),
            "/flexiq.v1.ProducerService/Enqueue",
            &MetadataMap::new(),
        )
        .expect("accepted")
        .expect("a gated path yields a principal");
        assert_eq!(&**principal.namespace(), "prod");
    }

    #[test]
    fn a_credential_without_the_package_scope_is_refused() {
        let produce_only = Fixed(Principal::new("prod", ScopeSet::of(&[Scope::Produce])));
        assert!(authorize(
            &produce_only,
            "/flexiq.v1.ProducerService/Enqueue",
            &MetadataMap::new()
        )
        .is_ok());

        let status = authorize(
            &produce_only,
            "/flexiq.executor.v1.ExecutorService/Dispatch",
            &MetadataMap::new(),
        )
        .expect_err("a produce credential must not open an executor stream");
        assert_eq!(status.code(), Code::PermissionDenied);
        let all = status.get_error_details();
        let details = all.error_info().expect("every error carries an ErrorInfo");
        assert_eq!(details.reason, reason::SCOPE_DENIED);
        assert_eq!(
            details.metadata.get(reason::KEY_SCOPE).map(String::as_str),
            Some("execute")
        );
    }
}
