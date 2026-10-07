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
pub mod watch;
pub mod workflows;

use std::sync::Arc;

use flexiq_core::StorageBackend;
use flexiq_workflows::WorkflowStorageBackend;
use tonic::{Request, Response, Status};

use crate::events::Events;
use crate::grpc::audit::{self, AuditContext, TargetKind};
use crate::grpc::auth::{Principal, Scope};
use crate::grpc::limits::PRODUCER_MAX_MESSAGE_BYTES;
use crate::grpc::pb;
use crate::grpc::pb::producer_service_server::{ProducerService, ProducerServiceServer};
use crate::grpc::status::WireError;

use watch::Watches;

/// The producer door's state: the two storage handles this process holds, the
/// event hub its writes are announced on, and the watch streams it serves.
#[derive(Clone)]
pub struct Producer {
    storage: StorageBackend,
    workflows: WorkflowStorageBackend,
    events: Events,
    watches: Arc<Watches>,
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
    /// Serve out of `storage` and `workflows`, announcing enqueues and cancels
    /// on `events` when set, and `WatchJobs` out of `watches`. The namespace
    /// arrives per request.
    pub fn new(
        storage: StorageBackend,
        workflows: WorkflowStorageBackend,
        events: Events,
        watches: Arc<Watches>,
    ) -> Self {
        Self {
            storage,
            workflows,
            events,
            watches,
        }
    }

    /// The registered service, capped at the producer door's message size.
    pub fn into_service(self) -> ProducerServiceServer<Self> {
        ProducerServiceServer::new(self)
            .max_decoding_message_size(PRODUCER_MAX_MESSAGE_BYTES)
            .max_encoding_message_size(PRODUCER_MAX_MESSAGE_BYTES)
    }

    /// Split a request into the caller's scope and its message, for a method
    /// that checks no queue and no task: the caller must reach every one.
    ///
    /// This is the default on purpose (#839). A credential narrowed to some
    /// queues is refused by every method that has not been taught to check
    /// them, so a new RPC is closed to it until someone opens it deliberately
    /// through [`Self::scope_narrowed`].
    #[expect(
        dead_code,
        reason = "every RPC checks its own queues today (#990); the next one starts here"
    )]
    fn scope<T>(&self, request: Request<T>) -> Result<(Scoped<'_>, T), Status> {
        let (scoped, message) = self.scope_narrowed(request)?;
        if !scoped.principal.reaches_everything() {
            return Err(scoped.beyond(None, None).into());
        }
        Ok((scoped, message))
    }

    /// Split a request into the caller's scope and its message, for a method
    /// that checks each queue and task it touches through [`Scoped::require`].
    ///
    /// Both at once, because the principal lives in the request's extensions
    /// and the message is behind `into_inner`: taking them in two steps would
    /// mean borrowing a request that has been consumed.
    fn scope_narrowed<T>(&self, request: Request<T>) -> Result<(Scoped<'_>, T), Status> {
        let principal = principal(&request)?.clone();
        let scoped = Scoped {
            storage: &self.storage,
            workflows: &self.workflows,
            events: self.events.clone(),
            namespace: Arc::clone(principal.namespace()),
            principal,
            audit: AuditContext::of(request.extensions()),
        };
        Ok((scoped, request.into_inner()))
    }
}

/// The caller the auth layer established.
fn principal<T>(request: &Request<T>) -> Result<&Principal, Status> {
    request
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
        })
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
    /// Owned rather than borrowed so a handler can move it onto the blocking
    /// pool and emit right after the write it announces.
    events: Events,
    namespace: Arc<str>,
    /// The caller, for what its grants reach behind this door.
    principal: Principal,
    /// The audit slot, on a call the audit layer records.
    audit: Option<AuditContext>,
}

impl Scoped<'_> {
    /// Name one thing this call acted on, for the audit trail.
    pub(crate) fn audit(&self, kind: TargetKind, id: impl Into<String>) {
        audit::target(self.audit.as_ref(), kind, id);
    }

    /// Name a listing's queue and task filters, for the audit trail.
    pub(crate) fn audit_filter(&self, queue: Option<&str>, task: Option<&str>) {
        audit::filter(self.audit.as_ref(), queue, task);
    }

    /// Whether the caller may touch `queue` and `task`; `None` asks about
    /// every queue (or task) at once.
    pub(crate) fn reaches(&self, queue: Option<&str>, task: Option<&str>) -> bool {
        self.principal.reaches(queue, task)
    }

    /// Refuse a call on a queue or task the caller's grants do not reach.
    pub(crate) fn require(&self, queue: Option<&str>, task: Option<&str>) -> Result<(), WireError> {
        if self.reaches(queue, task) {
            Ok(())
        } else {
            Err(self.beyond(queue, task))
        }
    }

    /// The refusal for a call beyond the caller's grants.
    fn beyond(&self, queue: Option<&str>, task: Option<&str>) -> WireError {
        WireError::beyond_grant(self.door().as_str(), queue, task)
    }

    /// The refusal for a deduplicated answer outside the caller's grants,
    /// which names nothing about the job it hides.
    pub(crate) fn deduplicated_beyond(&self) -> WireError {
        WireError::deduplicated_beyond_grant(self.door().as_str())
    }

    /// The scope the layer let this caller through on.
    fn door(&self) -> Scope {
        // The layer fixes the door before any handler runs; without one the
        // caller reaches nothing, and `produce` is the scope this package
        // would have asked for.
        self.principal.door().unwrap_or(Scope::Produce)
    }

    /// The namespace every storage call is scoped to. Never `None`, never
    /// empty: the role refuses to start without one.
    pub(crate) fn namespace(&self) -> &str {
        &self.namespace
    }

    /// The public id of the caller's token, stamped on every job it submits
    /// as `enqueued_by` — the same id its audit records carry.
    pub(crate) fn token_id(&self) -> &str {
        self.principal.credential()
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

    /// The event hub, when events are configured.
    pub(crate) fn events(&self) -> Events {
        self.events.clone()
    }
}

#[tonic::async_trait]
impl ProducerService for Producer {
    async fn enqueue(
        &self,
        request: Request<pb::EnqueueRequest>,
    ) -> Result<Response<pb::EnqueueResponse>, Status> {
        let (scoped, message) = self.scope_narrowed(request)?;
        enqueue::one(&scoped, message).await
    }

    async fn enqueue_batch(
        &self,
        request: Request<pb::EnqueueBatchRequest>,
    ) -> Result<Response<pb::EnqueueBatchResponse>, Status> {
        let (scoped, message) = self.scope_narrowed(request)?;
        enqueue::batch(&scoped, message).await
    }

    async fn get_job(
        &self,
        request: Request<pb::GetJobRequest>,
    ) -> Result<Response<pb::GetJobResponse>, Status> {
        let (scoped, message) = self.scope_narrowed(request)?;
        reads::get_job(&scoped, message).await
    }

    async fn list_jobs(
        &self,
        request: Request<pb::ListJobsRequest>,
    ) -> Result<Response<pb::ListJobsResponse>, Status> {
        let (scoped, message) = self.scope_narrowed(request)?;
        reads::list_jobs(&scoped, message).await
    }

    async fn cancel_job(
        &self,
        request: Request<pb::CancelJobRequest>,
    ) -> Result<Response<pb::CancelJobResponse>, Status> {
        let (scoped, message) = self.scope_narrowed(request)?;
        cancel::cancel_job(&scoped, message).await
    }

    async fn queue_stats(
        &self,
        request: Request<pb::QueueStatsRequest>,
    ) -> Result<Response<pb::QueueStatsResponse>, Status> {
        let (scoped, message) = self.scope_narrowed(request)?;
        reads::queue_stats(&scoped, message).await
    }

    async fn submit_workflow(
        &self,
        request: Request<pb::SubmitWorkflowRequest>,
    ) -> Result<Response<pb::SubmitWorkflowResponse>, Status> {
        let (scoped, message) = self.scope_narrowed(request)?;
        workflows::submit_workflow(&scoped, message).await
    }

    async fn get_workflow_run(
        &self,
        request: Request<pb::GetWorkflowRunRequest>,
    ) -> Result<Response<pb::GetWorkflowRunResponse>, Status> {
        let (scoped, message) = self.scope_narrowed(request)?;
        workflows::get_workflow_run(&scoped, message).await
    }

    type WatchJobsStream = watch::stream::Outlet;

    async fn watch_jobs(
        &self,
        request: Request<pb::WatchJobsRequest>,
    ) -> Result<Response<Self::WatchJobsStream>, Status> {
        // The credential as well as the namespace: the stream cap counts per
        // credential, and a narrowed one's grants are checked per target.
        let principal = principal(&request)?.clone();
        let audit = AuditContext::of(request.extensions());
        self.watches
            .watch(&principal, audit.as_ref(), request.into_inner())
    }
}
