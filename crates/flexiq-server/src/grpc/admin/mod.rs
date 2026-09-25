//! `flexiq.admin.v1.AdminService`: the operator door (#836).
//!
//! The dashboard's operations — pause a queue, work the dead-letter queue, look
//! at workers, manage periodic tasks, change an override — as RPCs, grouped by
//! what they act on: [`queues`], [`dead_letters`], [`workers`], [`periodic`],
//! [`overrides`]. The trait implementation below is the only place they are
//! joined.
//!
//! Every handler acts on the credential's namespace, read off the request's
//! [`Principal`] exactly as the producer door reads it, and on nothing else. A
//! resource in another namespace answers the way an absent one does. Which
//! scope a method needs is the gate's business (`auth::gate`), not this
//! module's: by the time a handler runs, the caller may call it.

pub mod convert;
pub mod dead_letters;
pub mod overrides;
pub mod periodic;
pub mod queues;
pub mod workers;

use std::sync::Arc;

use flexiq_core::StorageBackend;
use tonic::{Request, Response, Status};

use crate::events::Events;
use crate::grpc::auth::Principal;
use crate::grpc::limits::PRODUCER_MAX_MESSAGE_BYTES;
use crate::grpc::pb::admin as pb;
use crate::grpc::pb::admin::admin_service_server::{AdminService, AdminServiceServer};
use crate::grpc::status::WireError;

/// The operator door's state: the storage handle this process holds, and the
/// event hub the jobs it enqueues are announced on.
#[derive(Clone)]
pub struct Admin {
    storage: StorageBackend,
    events: Events,
}

// Hand-written: `StorageBackend` is not `Debug`, and a backend's debug output
// is where a DSN would end up in a log.
impl std::fmt::Debug for Admin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Admin").finish_non_exhaustive()
    }
}

impl Admin {
    /// Serve out of `storage`, announcing the jobs it enqueues on `events`
    /// when set. The namespace arrives per request.
    pub fn new(storage: StorageBackend, events: Events) -> Self {
        Self { storage, events }
    }

    /// The registered service. Its messages are the producer door's size: the
    /// largest is a periodic task's arguments, which are an enqueue's.
    pub fn into_service(self) -> AdminServiceServer<Self> {
        AdminServiceServer::new(self)
            .max_decoding_message_size(PRODUCER_MAX_MESSAGE_BYTES)
            .max_encoding_message_size(PRODUCER_MAX_MESSAGE_BYTES)
    }

    /// Split a request into the caller's scope and its message.
    fn scope<T>(&self, request: Request<T>) -> Result<(Scoped, T), Status> {
        let principal = request
            .extensions()
            .get::<Principal>()
            // Only reachable if this service is registered without the auth
            // layer; falling back to some namespace is the cross-tenant access
            // the design refuses, so fail closed.
            .ok_or_else(|| {
                log::error!(
                    "grpc: an admin request carried no principal; the service \
                     is registered without the auth layer"
                );
                Status::from(WireError::internal())
            })?;
        let scoped = Scoped {
            storage: self.storage.clone(),
            events: self.events.clone(),
            namespace: Arc::clone(principal.namespace()),
        };
        Ok((scoped, request.into_inner()))
    }
}

/// One request's view of the door: the storage handle and the namespace the
/// caller's credential grants.
pub(crate) struct Scoped {
    storage: StorageBackend,
    events: Events,
    namespace: Arc<str>,
}

impl Scoped {
    /// The namespace every storage call is scoped to, owned for a closure that
    /// runs on the blocking pool. Never empty: the role refuses to start
    /// without one.
    pub(crate) fn namespace_owned(&self) -> String {
        self.namespace.to_string()
    }

    pub(crate) fn storage(&self) -> &StorageBackend {
        &self.storage
    }

    /// The event hub, when events are configured.
    pub(crate) fn events(&self) -> Events {
        self.events.clone()
    }
}

/// A name the caller actually sent. Every handle in this service is one.
pub(crate) fn require(field: &str, value: String) -> Result<String, WireError> {
    if value.is_empty() {
        return Err(WireError::invalid_request(format!(
            "{field} must not be empty"
        )));
    }
    Ok(value)
}

#[tonic::async_trait]
impl AdminService for Admin {
    async fn list_queues(
        &self,
        request: Request<pb::ListQueuesRequest>,
    ) -> Result<Response<pb::ListQueuesResponse>, Status> {
        let (scoped, _) = self.scope(request)?;
        queues::list(&scoped).await
    }

    async fn pause_queue(
        &self,
        request: Request<pb::PauseQueueRequest>,
    ) -> Result<Response<pb::PauseQueueResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        queues::pause(&scoped, message).await
    }

    async fn resume_queue(
        &self,
        request: Request<pb::ResumeQueueRequest>,
    ) -> Result<Response<pb::ResumeQueueResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        queues::resume(&scoped, message).await
    }

    async fn get_throughput(
        &self,
        request: Request<pb::GetThroughputRequest>,
    ) -> Result<Response<pb::GetThroughputResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        queues::throughput(&scoped, message).await
    }

    async fn list_dead_letters(
        &self,
        request: Request<pb::ListDeadLettersRequest>,
    ) -> Result<Response<pb::ListDeadLettersResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        dead_letters::list(&scoped, message).await
    }

    async fn get_dead_letter(
        &self,
        request: Request<pb::GetDeadLetterRequest>,
    ) -> Result<Response<pb::GetDeadLetterResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        dead_letters::get(&scoped, message).await
    }

    async fn replay_dead_letter(
        &self,
        request: Request<pb::ReplayDeadLetterRequest>,
    ) -> Result<Response<pb::ReplayDeadLetterResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        dead_letters::replay(&scoped, message).await
    }

    async fn delete_dead_letter(
        &self,
        request: Request<pb::DeleteDeadLetterRequest>,
    ) -> Result<Response<pb::DeleteDeadLetterResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        dead_letters::delete(&scoped, message).await
    }

    async fn purge_dead_letters(
        &self,
        request: Request<pb::PurgeDeadLettersRequest>,
    ) -> Result<Response<pb::PurgeDeadLettersResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        dead_letters::purge(&scoped, message).await
    }

    async fn list_workers(
        &self,
        request: Request<pb::ListWorkersRequest>,
    ) -> Result<Response<pb::ListWorkersResponse>, Status> {
        let (scoped, _) = self.scope(request)?;
        workers::list(&scoped).await
    }

    async fn drain_worker(
        &self,
        request: Request<pb::DrainWorkerRequest>,
    ) -> Result<Response<pb::DrainWorkerResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        workers::drain(&scoped, message).await
    }

    async fn list_periodic_tasks(
        &self,
        request: Request<pb::ListPeriodicTasksRequest>,
    ) -> Result<Response<pb::ListPeriodicTasksResponse>, Status> {
        let (scoped, _) = self.scope(request)?;
        periodic::list(&scoped).await
    }

    async fn get_periodic_task(
        &self,
        request: Request<pb::GetPeriodicTaskRequest>,
    ) -> Result<Response<pb::GetPeriodicTaskResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        periodic::get(&scoped, message).await
    }

    async fn put_periodic_task(
        &self,
        request: Request<pb::PutPeriodicTaskRequest>,
    ) -> Result<Response<pb::PutPeriodicTaskResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        periodic::put(&scoped, message).await
    }

    async fn delete_periodic_task(
        &self,
        request: Request<pb::DeletePeriodicTaskRequest>,
    ) -> Result<Response<pb::DeletePeriodicTaskResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        periodic::delete(&scoped, message).await
    }

    async fn pause_periodic_task(
        &self,
        request: Request<pb::PausePeriodicTaskRequest>,
    ) -> Result<Response<pb::PausePeriodicTaskResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        let task = periodic::set_enabled(&scoped, message.name, false).await?;
        Ok(Response::new(pb::PausePeriodicTaskResponse {
            periodic_task: Some(task),
        }))
    }

    async fn resume_periodic_task(
        &self,
        request: Request<pb::ResumePeriodicTaskRequest>,
    ) -> Result<Response<pb::ResumePeriodicTaskResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        let task = periodic::set_enabled(&scoped, message.name, true).await?;
        Ok(Response::new(pb::ResumePeriodicTaskResponse {
            periodic_task: Some(task),
        }))
    }

    async fn trigger_periodic_task(
        &self,
        request: Request<pb::TriggerPeriodicTaskRequest>,
    ) -> Result<Response<pb::TriggerPeriodicTaskResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        periodic::trigger(&scoped, message).await
    }

    async fn list_overrides(
        &self,
        request: Request<pb::ListOverridesRequest>,
    ) -> Result<Response<pb::ListOverridesResponse>, Status> {
        let (scoped, _) = self.scope(request)?;
        overrides::list(&scoped).await
    }

    async fn set_task_override(
        &self,
        request: Request<pb::SetTaskOverrideRequest>,
    ) -> Result<Response<pb::SetTaskOverrideResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        overrides::set_task(&scoped, message).await
    }

    async fn clear_task_override(
        &self,
        request: Request<pb::ClearTaskOverrideRequest>,
    ) -> Result<Response<pb::ClearTaskOverrideResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        overrides::clear_task(&scoped, message).await
    }

    async fn set_queue_override(
        &self,
        request: Request<pb::SetQueueOverrideRequest>,
    ) -> Result<Response<pb::SetQueueOverrideResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        overrides::set_queue(&scoped, message).await
    }

    async fn clear_queue_override(
        &self,
        request: Request<pb::ClearQueueOverrideRequest>,
    ) -> Result<Response<pb::ClearQueueOverrideResponse>, Status> {
        let (scoped, message) = self.scope(request)?;
        overrides::clear_queue(&scoped, message).await
    }
}
