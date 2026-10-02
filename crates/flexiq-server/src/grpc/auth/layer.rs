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
//! The response body is tonic's own rather than a generic one, because a
//! refusal has to be *rendered*: a gRPC caller needs `grpc-status` trailers and
//! a JSON caller needs a body with an HTTP status, and only a body type that
//! can hold bytes can carry the second. Which of the two a request gets is
//! [`facade::refusal`]'s decision, made from the content type, so this layer
//! keeps one refusal path for both doors.
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
use crate::grpc::audit::AuditContext;
use crate::grpc::facade;
use crate::grpc::status::WireError;
use crate::tokens::grant::NARROWABLE;

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

impl<S, ReqBody> Service<http::Request<ReqBody>> for Authenticated<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<tonic::body::Body>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
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
            let audit = AuditContext::of(&parts.extensions);
            let outcome = authorize(
                &*authenticator,
                &parts.method,
                parts.uri.path(),
                &metadata,
                audit.as_ref(),
            )
            .await;
            parts.headers = metadata.into_headers();

            match outcome {
                Ok(Some(principal)) => {
                    parts.extensions.insert(principal);
                }
                // A public path: routed with no principal, because nothing
                // behind one needs a namespace.
                Ok(None) => {}
                // One refusal, rendered for whichever door asked: trailers for
                // a gRPC caller, a JSON body and an HTTP status for the facade.
                Err(status) => return Ok(facade::refusal(&parts.headers, &status)),
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
///
/// A believed credential is named in `audit` before its scope is checked, so
/// a call refused for want of one is still attributed in the audit trail.
async fn authorize(
    authenticator: &dyn Authenticator,
    method: &http::Method,
    path: &str,
    metadata: &MetadataMap,
    audit: Option<&AuditContext>,
) -> Result<Option<Principal>, Status> {
    let requirement = gate::requirement(method, path);
    if requirement == Requirement::Public {
        // Not merely allowed through: not even *asked*. A public path must not
        // reach storage, or an unauthenticated caller could keep the pool busy.
        return Ok(None);
    }

    let principal = authenticator.authenticate(metadata).await?;
    if let Some(audit) = audit {
        audit.identify(&principal);
    }
    let Requirement::Scoped(scope) = requirement else {
        return Ok(Some(principal));
    };
    if !principal.grants(scope) {
        return Err(WireError::scope_denied(scope.as_str()).into());
    }
    // What the caller reaches behind this door is fixed here, once, so a
    // handler reads it rather than recomputing it (#839).
    let principal = principal.behind(scope);
    // A door none of whose methods checks a queue or a task admits whole
    // grants only (a narrowable door's handlers check, or refuse narrowed). The grammar already refuses to narrow these scopes; this is
    // the line that holds if a row ever says otherwise.
    if !NARROWABLE.contains(&scope) && !principal.reaches_everything() {
        return Err(WireError::scope_denied(scope.as_str()).into());
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

    /// What every gRPC call is sent as.
    const POST: http::Method = http::Method::POST;

    fn grants_everything() -> Fixed {
        Fixed(Principal::new("tok", "prod", ScopeSet::ALL))
    }

    #[tokio::test]
    async fn health_is_routed_without_a_credential() {
        let outcome = authorize(
            &Refuses,
            &POST,
            "/grpc.health.v1.Health/Check",
            &MetadataMap::new(),
            None,
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
            let Err(status) = authorize(&Refuses, &POST, path, &MetadataMap::new(), None).await
            else {
                panic!("{path} must be gated");
            };
            assert_eq!(status.code(), Code::Unauthenticated, "path: {path}");
        }
    }

    #[tokio::test]
    async fn an_authenticated_call_carries_its_principal_onward() {
        let principal = authorize(
            &grants_everything(),
            &POST,
            "/flexiq.v1.ProducerService/Enqueue",
            &MetadataMap::new(),
            None,
        )
        .await
        .expect("accepted")
        .expect("a gated path yields a principal");
        assert_eq!(&**principal.namespace(), "prod");
    }

    #[tokio::test]
    async fn a_credential_without_the_package_scope_is_refused() {
        let produce_only = Fixed(Principal::new(
            "tok",
            "prod",
            ScopeSet::of(&[Scope::Produce]),
        ));
        assert!(authorize(
            &produce_only,
            &POST,
            "/flexiq.v1.ProducerService/Enqueue",
            &MetadataMap::new(),
            None,
        )
        .await
        .is_ok());

        let status = authorize(
            &produce_only,
            &POST,
            "/flexiq.executor.v1.ExecutorService/Dispatch",
            &MetadataMap::new(),
            None,
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

    #[tokio::test]
    async fn a_scope_refusal_is_still_attributed_for_the_audit_trail() {
        let read_only = Fixed(Principal::new("tok", "prod", ScopeSet::of(&[Scope::Read])));
        let audit = AuditContext::default();
        let status = authorize(
            &read_only,
            &POST,
            "/flexiq.v1.ProducerService/Enqueue",
            &MetadataMap::new(),
            Some(&audit),
        )
        .await
        .expect_err("a read credential must not enqueue");
        assert_eq!(status.code(), Code::PermissionDenied);
        let (principal, _) = audit.take();
        assert_eq!(
            principal.map(|p| p.credential().to_string()).as_deref(),
            Some("tok")
        );
    }

    #[tokio::test]
    async fn an_unbelieved_credential_names_no_one() {
        let audit = AuditContext::default();
        authorize(
            &Refuses,
            &POST,
            "/flexiq.v1.ProducerService/Enqueue",
            &MetadataMap::new(),
            Some(&audit),
        )
        .await
        .expect_err("refused");
        assert!(audit.take().0.is_none());
    }
}
