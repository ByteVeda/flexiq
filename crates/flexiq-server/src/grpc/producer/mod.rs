//! `flexiq.v1.ProducerService`: submit work, read it back, cancel it, count it.
//!
//! The handlers are grouped by what they do — [`enqueue`], [`reads`],
//! [`cancel`] — and the trait implementation below is the only place they are
//! joined, because a trait may only be implemented once.
//!
//! ## The namespace
//!
//! Every storage call this service makes passes `Some(namespace)`, and the
//! namespace is [`Producer::namespace`] — the process's own, from
//! `FLEXIQ_GRPC_LISTEN`'s configuration. No request message carries one, in any
//! RPC, and that is structural rather than a convention: `None` means three
//! different things inside `Storage` — only the NULL rows to a dequeue, *every*
//! namespace to an id-addressed read, and no filter at all to a listing — so a
//! service that forwarded a caller's "no namespace" into `get_job` would read
//! every tenant's jobs.
//!
//! [`Producer::namespace`] is therefore the seam an authenticator replaces: when
//! a credential carries the namespace, this accessor reads it off the request's
//! principal instead of off the process, and nothing else in this module moves.

pub mod cancel;
pub mod convert;
pub mod cursor;
pub mod enqueue;
pub mod reads;

use std::sync::Arc;

use flexiq_core::StorageBackend;
use tonic::{Request, Response, Status};

use crate::grpc::limits::PRODUCER_MAX_MESSAGE_BYTES;
use crate::grpc::pb;
use crate::grpc::pb::producer_service_server::{ProducerService, ProducerServiceServer};

/// The producer door's state: one storage handle and one namespace.
#[derive(Clone)]
pub struct Producer {
    storage: StorageBackend,
    namespace: Arc<str>,
}

// Hand-written rather than derived: `StorageBackend` is not `Debug`, and it
// should not become so through this — a backend's debug output is where a DSN
// would end up in a log.
impl std::fmt::Debug for Producer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer")
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

impl Producer {
    /// Serve `namespace` out of `storage`.
    pub fn new(storage: StorageBackend, namespace: impl Into<Arc<str>>) -> Self {
        Self {
            storage,
            namespace: namespace.into(),
        }
    }

    /// The registered service, capped at the producer door's message size.
    pub fn into_service(self) -> ProducerServiceServer<Self> {
        ProducerServiceServer::new(self)
            .max_decoding_message_size(PRODUCER_MAX_MESSAGE_BYTES)
            .max_encoding_message_size(PRODUCER_MAX_MESSAGE_BYTES)
    }

    /// The namespace every storage call is scoped to. Never `None`.
    pub(crate) fn namespace(&self) -> &str {
        &self.namespace
    }

    pub(crate) fn storage(&self) -> &StorageBackend {
        &self.storage
    }
}

#[tonic::async_trait]
impl ProducerService for Producer {
    async fn enqueue(
        &self,
        request: Request<pb::EnqueueRequest>,
    ) -> Result<Response<pb::EnqueueResponse>, Status> {
        enqueue::one(self, request.into_inner()).await
    }

    async fn enqueue_batch(
        &self,
        request: Request<pb::EnqueueBatchRequest>,
    ) -> Result<Response<pb::EnqueueBatchResponse>, Status> {
        enqueue::batch(self, request.into_inner()).await
    }

    async fn get_job(
        &self,
        request: Request<pb::GetJobRequest>,
    ) -> Result<Response<pb::GetJobResponse>, Status> {
        reads::get_job(self, request.into_inner()).await
    }

    async fn list_jobs(
        &self,
        request: Request<pb::ListJobsRequest>,
    ) -> Result<Response<pb::ListJobsResponse>, Status> {
        reads::list_jobs(self, request.into_inner()).await
    }

    async fn cancel_job(
        &self,
        request: Request<pb::CancelJobRequest>,
    ) -> Result<Response<pb::CancelJobResponse>, Status> {
        cancel::cancel_job(self, request.into_inner()).await
    }

    async fn queue_stats(
        &self,
        request: Request<pb::QueueStatsRequest>,
    ) -> Result<Response<pb::QueueStatsResponse>, Status> {
        reads::queue_stats(self, request.into_inner()).await
    }
}
