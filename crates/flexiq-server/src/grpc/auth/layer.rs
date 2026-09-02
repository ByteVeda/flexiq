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
//!
//! The check is `async` because the credential is a stored row (#717), so the
//! inner service has to be owned by the future rather than borrowed from
//! `&mut self`. That is what the clone-and-replace in [`Authenticated::call`]
//! is for, and the direction matters: the *ready* service is moved into the
//! future and the fresh clone is left behind, because `poll_ready`'s
//! reservation belongs to the one that was polled.

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
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Default + Send + 'static,
{
    type Response = http::Response<ResBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<ReqBody>) -> Self::Future {
        let authenticator = Arc::clone(&self.authenticator);
        // The service this future calls must be the one `poll_ready` reserved
        // capacity on, so the ready service moves into the future and the clone
        // stays behind to be polled again.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            let (mut parts, body) = request.into_parts();

            // The headers are moved into the `MetadataMap` and moved back,
            // rather than cloned: an `Authenticator` is handed metadata by
            // contract, and the request still needs its headers afterwards.
            // tonic's own interceptor takes a request apart the same way.
            let metadata = MetadataMap::from_headers(std::mem::take(&mut parts.headers));
            let outcome = authorize(&*authenticator, parts.uri.path(), &metadata).await;
            parts.headers = metadata.into_headers();

            match outcome {
                Ok(Some(principal)) => {
                    parts.extensions.insert(principal);
                }
                // A public path: routed with no principal, because nothing
                // behind one needs a namespace.
                Ok(None) => {}
                Err(status) => return Ok(status.into_http()),
            }

            inner.call(http::Request::from_parts(parts, body)).await
        })
    }
}

/// Apply the gate: identify the caller if the path needs one, then check that
/// what it carries covers the package it asked for.
///
/// `Ok(None)` is a public path. Split out of [`Authenticated::call`] so the
/// policy is testable without a service behind it.
async fn authorize(
    authenticator: &dyn Authenticator,
    path: &str,
    metadata: &MetadataMap,
) -> Result<Option<Principal>, Status> {
    let requirement = gate::requirement(path);
    if requirement == Requirement::Public {
        // Not merely allowed through: not even *asked*. A public path must not
        // reach storage, or an unauthenticated caller could keep the pool busy.
        return Ok(None);
    }

    let principal = authenticator.authenticate(metadata).await?;
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
    use crate::grpc::auth::principal::{Scope, ScopeSet};
    use crate::grpc::status::reason;
    use tonic::Code;
    use tonic_types::StatusExt;

    /// An authenticator that answers with a fixed principal, for exercising the
    /// scope half of the gate without a credential scheme in the way.
    struct Fixed(Principal);

    #[async_trait::async_trait]
    impl Authenticator for Fixed {
        async fn authenticate(&self, _metadata: &MetadataMap) -> Result<Principal, Status> {
            Ok(self.0.clone())
        }
    }

    /// An authenticator that refuses everything, for the other half.
    struct Refuses;

    #[async_trait::async_trait]
    impl Authenticator for Refuses {
        async fn authenticate(&self, _metadata: &MetadataMap) -> Result<Principal, Status> {
            Err(WireError::unauthenticated().into())
        }
    }

    fn grants_everything() -> Fixed {
        Fixed(Principal::new("prod", ScopeSet::ALL))
    }

    #[tokio::test]
    async fn health_is_routed_without_a_credential() {
        let outcome = authorize(
            &Refuses,
            "/grpc.health.v1.Health/Check",
            &MetadataMap::new(),
        )
        .await
        .expect("health must not need a credential");
        assert!(outcome.is_none(), "health needs no principal either");
    }

    #[tokio::test]
    async fn every_other_path_needs_one() {
        for path in [
            "/flexiq.v1.ProducerService/Enqueue",
            "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
            "/whatever",
        ] {
            let Err(status) = authorize(&Refuses, path, &MetadataMap::new()).await else {
                panic!("{path} must be gated");
            };
            assert_eq!(status.code(), Code::Unauthenticated, "path: {path}");
        }
    }

    #[tokio::test]
    async fn an_authenticated_call_carries_its_principal_onward() {
        let principal = authorize(
            &grants_everything(),
            "/flexiq.v1.ProducerService/Enqueue",
            &MetadataMap::new(),
        )
        .await
        .expect("accepted")
        .expect("a gated path yields a principal");
        assert_eq!(&**principal.namespace(), "prod");
    }

    #[tokio::test]
    async fn a_credential_without_the_package_scope_is_refused() {
        let produce_only = Fixed(Principal::new("prod", ScopeSet::of(&[Scope::Produce])));
        assert!(authorize(
            &produce_only,
            "/flexiq.v1.ProducerService/Enqueue",
            &MetadataMap::new()
        )
        .await
        .is_ok());

        let status = authorize(
            &produce_only,
            "/flexiq.executor.v1.ExecutorService/Dispatch",
            &MetadataMap::new(),
        )
        .await
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
