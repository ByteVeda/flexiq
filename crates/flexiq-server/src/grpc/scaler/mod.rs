//! `externalscaler.ExternalScaler`: KEDA's external scaler, served on the gRPC
//! listener (#850).
//!
//! KEDA polls it for whether a deployment should run at all ([`service::is_active`])
//! and how many replicas the queue depth asks for ([`service::metric_spec`],
//! [`service::metrics`]). The contract is KEDA's, vendored under
//! `contracts/proto/externalscaler`. A scaled object names its queue and its
//! token in trigger metadata ([`metadata`]); the token is checked in-band
//! ([`auth`]) because KEDA cannot send a header, and the namespace measured is
//! the token's.

pub mod auth;
pub mod metadata;
pub mod service;

use std::pin::Pin;
use std::sync::Arc;

use flexiq_core::StorageBackend;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::grpc::audit::{self, AuditContext};
use crate::grpc::auth::{Authenticator, Principal, Scope};
use crate::grpc::limits::SCALER_MAX_MESSAGE_BYTES;
use crate::grpc::pb::externalscaler as pb;
use crate::grpc::pb::externalscaler::external_scaler_server::{
    ExternalScaler, ExternalScalerServer,
};
use crate::grpc::status::WireError;
use metadata::Query;

/// The scaler's state: storage, and the authenticator the auth layer holds.
#[derive(Clone)]
pub struct Scaler {
    storage: StorageBackend,
    authenticator: Arc<dyn Authenticator>,
}

// Hand-written: neither field is `Debug`, and the authenticator guards secrets.
impl std::fmt::Debug for Scaler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scaler").finish_non_exhaustive()
    }
}

impl Scaler {
    /// Serve out of `storage`, checking tokens with `authenticator` — the one
    /// the auth layer was built with, so both share its caches.
    pub fn new(storage: StorageBackend, authenticator: Arc<dyn Authenticator>) -> Self {
        Self {
            storage,
            authenticator,
        }
    }

    /// The registered service, capped small: it is reachable without a header
    /// credential, so an oversized request must fail before it is decoded.
    pub fn into_service(self) -> ExternalScalerServer<Self> {
        ExternalScalerServer::new(self)
            .max_decoding_message_size(SCALER_MAX_MESSAGE_BYTES)
            .max_encoding_message_size(SCALER_MAX_MESSAGE_BYTES)
    }

    /// Authenticate the caller, read its query and check it reaches the queue.
    async fn scope<T>(
        &self,
        request: &Request<T>,
        object: Option<&pb::ScaledObjectRef>,
    ) -> Result<Scoped, Status> {
        let empty = pb::ScaledObjectRef::default();
        let object = object.unwrap_or(&empty);
        let principal =
            auth::caller(&*self.authenticator, request, &object.scaler_metadata).await?;
        let query = Query::parse(&object.scaler_metadata)?;
        audit::filter(
            AuditContext::of(request.extensions()).as_ref(),
            query.queue.as_deref(),
            None,
        );
        // Counts span every task, so a task-narrowed grant reaches none of
        // them, and a whole-namespace query needs an unnarrowed one.
        if !principal.reaches(query.queue.as_deref(), None) {
            return Err(WireError::beyond_grant(
                Scope::Inspect.as_str(),
                query.queue.as_deref(),
                None,
            )
            .into());
        }
        Ok(Scoped {
            storage: self.storage.clone(),
            principal,
            query,
        })
    }
}

/// One checked call: who asked, in which namespace, for what.
pub struct Scoped {
    storage: StorageBackend,
    principal: Principal,
    query: Query,
}

/// A server stream of the scaler's answers.
type Answers<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

#[tonic::async_trait]
impl ExternalScaler for Scaler {
    async fn is_active(
        &self,
        request: Request<pb::ScaledObjectRef>,
    ) -> Result<Response<pb::IsActiveResponse>, Status> {
        let scoped = self.scope(&request, Some(request.get_ref())).await?;
        service::is_active(&scoped).await.map(Response::new)
    }

    type StreamIsActiveStream = Answers<pb::IsActiveResponse>;

    async fn stream_is_active(
        &self,
        _request: Request<pb::ScaledObjectRef>,
    ) -> Result<Response<Self::StreamIsActiveStream>, Status> {
        Err(Status::unimplemented("StreamIsActive is not served yet"))
    }

    async fn get_metric_spec(
        &self,
        request: Request<pb::ScaledObjectRef>,
    ) -> Result<Response<pb::GetMetricSpecResponse>, Status> {
        let scoped = self.scope(&request, Some(request.get_ref())).await?;
        Ok(Response::new(service::metric_spec(&scoped)))
    }

    async fn get_metrics(
        &self,
        request: Request<pb::GetMetricsRequest>,
    ) -> Result<Response<pb::GetMetricsResponse>, Status> {
        let scoped = self
            .scope(&request, request.get_ref().scaled_object_ref.as_ref())
            .await?;
        service::metrics(&scoped).await.map(Response::new)
    }

    type StreamMetricSpecStream = Answers<pb::GetMetricSpecResponse>;

    /// Optional upstream: KEDA falls back to polling `GetMetricSpec` on
    /// `UNIMPLEMENTED`, and a fixed target never changes anyway.
    async fn stream_metric_spec(
        &self,
        _request: Request<pb::ScaledObjectRef>,
    ) -> Result<Response<Self::StreamMetricSpecStream>, Status> {
        Err(Status::unimplemented("StreamMetricSpec is not served"))
    }
}
