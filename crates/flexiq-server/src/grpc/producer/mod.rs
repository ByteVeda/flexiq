//! `flexiq.v1.ProducerService`: submit work, read it back, cancel it, count it.
//!
//! The handlers are grouped by what they do — [`enqueue`], [`reads`],
//! [`cancel`] — and the trait implementation below is the only place they are
//! joined, because a trait may only be implemented once.
//!
//! ## The namespace
//!
//! Every storage call this service makes passes `Some(namespace)`, and the
//! namespace comes off the request's [`Principal`] — never off a request
//! message, in any RPC. That is structural rather than a convention: `None`
//! means three different things inside `Storage` — only the NULL rows to a
//! dequeue, *every* namespace to an id-addressed read, and no filter at all to
//! a listing — so a service that forwarded a caller's "no namespace" into
//! `get_job` would read every tenant's jobs.
//!
//! [`Producer`] therefore holds **no namespace of its own**. Under #716's
//! shared secret the principal's namespace is the process's, from
//! `FLEXIQ_GRPC_LISTEN`'s configuration; under a credential that carries one it
//! is the credential's, and nothing in this module moves. The extraction
//! happens in `Producer::scope`, called once per RPC in the trait
//! implementation below — the one place all six are joined, so a seventh is one
//! line beside six identical ones.
//!
//! A request that arrives with no principal fails `INTERNAL`. That is the
//! layer's absence, not a caller's mistake, and failing closed is what makes
//! registering this service without [`AuthLayer`](crate::grpc::auth::AuthLayer)
//! serve nothing rather than serve everything unauthenticated.

pub mod cancel;
pub mod convert;
pub mod cursor;
pub mod enqueue;
pub mod reads;
pub mod structured;
pub mod workflows;

use std::sync::Arc;

use flexiq_core::StorageBackend;
use flexiq_workflows::WorkflowStorageBackend;
use tonic::{Request, Response, Status};

use crate::grpc::auth::Principal;
use crate::grpc::limits::PRODUCER_MAX_MESSAGE_BYTES;
use crate::grpc::pb;
use crate::grpc::pb::producer_service_server::{ProducerService, ProducerServiceServer};
use crate::grpc::status::WireError;

/// The producer door's state: the two storage handles this process holds, and
/// nothing else.
#[derive(Clone)]
pub struct Producer {
    storage: StorageBackend,
    workflows: WorkflowStorageBackend,
}

// Hand-written rather than derived: `StorageBackend` is not `Debug`, and it
// should not become so through this — a backend's debug output is where a DSN
// would end up in a log.
impl std::fmt::Debug for Producer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer").finish_non_exhaustive()
    }
}

impl Producer {
    /// Serve out of `storage` and `workflows`. The namespace arrives per
    /// request.
    pub fn new(storage: StorageBackend, workflows: WorkflowStorageBackend) -> Self {
        Self { storage, workflows }
    }

    /// The registered service, capped at the producer door's message size.
    pub fn into_service(self) -> ProducerServiceServer<Self> {
        ProducerServiceServer::new(self)
            .max_decoding_message_size(PRODUCER_MAX_MESSAGE_BYTES)
            .max_encoding_message_size(PRODUCER_MAX_MESSAGE_BYTES)
    }

    /// Split a request into the caller's scope and its message.
    ///
    /// Both at once, because the principal lives in the request's extensions
    /// and the message is behind `into_inner`: taking them in two steps would
    /// mean either cloning the principal or borrowing a request that has been
    /// consumed.
    fn scope<T>(&self, request: Request<T>) -> Result<(Scoped<'_>, T), Status> {
        let principal = request
            .extensions()
            .get::<Principal>()
            // Only reachable if this service is registered without the auth
            // layer. There is no caller to blame and nothing useful to say, and
            // the alternative — falling back to some namespace — is the
            // cross-tenant read the whole design refuses.
            .ok_or_else(|| {
                log::error!(
                    "grpc: a producer request carried no principal; the service \
                     is registered without the auth layer"
                );
                Status::from(WireError::internal())
            })?;
        let scoped = Scoped {
            storage: &self.storage,
            workflows: &self.workflows,
            namespace: Arc::clone(principal.namespace()),
        };
        Ok((scoped, request.into_inner()))
    }
}

/// One request's view of the door: the storage handles, and the namespace
/// this caller's credential grants.
///
/// The handlers take this rather than [`Producer`] so that "which namespace"
/// has exactly one answer inside a request and it is never the process's by
/// default.
pub(crate) struct Scoped<'a> {
    storage: &'a StorageBackend,
    workflows: &'a WorkflowStorageBackend,
    namespace: Arc<str>,
}

impl Scoped<'_> {
    /// The namespace every storage call is scoped to. Never `None`, never
    /// empty: the role refuses to start without one.
    pub(crate) fn namespace(&self) -> &str {
        &self.namespace
    }

    pub(crate) fn storage(&self) -> &StorageBackend {
        self.storage
    }

    /// The workflow storage handle. Already scoped to this process's one
    /// namespace at construction — unlike [`Self::storage`], no per-call
    /// namespace argument exists on `WorkflowStorage`.
    pub(crate) fn workflows(&self) -> &WorkflowStorageBackend {
        self.workflows
    }
}

#[tonic::async_trait]
impl ProducerService for Producer {
    async fn enqueue(
        &self,
        request: Request<pb::EnqueueRequest>,
    ) -> Result<Response<pb::EnqueueResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        enqueue::one(&scoped, message).await
    }

    async fn enqueue_batch(
        &self,
        request: Request<pb::EnqueueBatchRequest>,
    ) -> Result<Response<pb::EnqueueBatchResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        enqueue::batch(&scoped, message).await
    }

    async fn get_job(
        &self,
        request: Request<pb::GetJobRequest>,
    ) -> Result<Response<pb::GetJobResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        reads::get_job(&scoped, message).await
    }

    async fn list_jobs(
        &self,
        request: Request<pb::ListJobsRequest>,
    ) -> Result<Response<pb::ListJobsResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        reads::list_jobs(&scoped, message).await
    }

    async fn cancel_job(
        &self,
        request: Request<pb::CancelJobRequest>,
    ) -> Result<Response<pb::CancelJobResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        cancel::cancel_job(&scoped, message).await
    }

    async fn queue_stats(
        &self,
        request: Request<pb::QueueStatsRequest>,
    ) -> Result<Response<pb::QueueStatsResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        reads::queue_stats(&scoped, message).await
    }

    async fn submit_workflow(
        &self,
        request: Request<pb::SubmitWorkflowRequest>,
    ) -> Result<Response<pb::SubmitWorkflowResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        workflows::submit_workflow(&scoped, message).await
    }

    async fn get_workflow_run(
        &self,
        request: Request<pb::GetWorkflowRunRequest>,
    ) -> Result<Response<pb::GetWorkflowRunResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        workflows::get_workflow_run(&scoped, message).await
    }
}
