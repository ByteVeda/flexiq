//! The operator door's handlers: `flexiq.admin.v1` over HTTP.
//!
//! Each calls the `AdminService` trait method a gRPC request reaches, on the
//! same `Admin`, with the caller's principal attached exactly as the producer
//! handlers attach it — so a JSON caller and a gRPC caller cannot reach
//! different behaviour, only different spellings of it. Which scope a path
//! needs is decided before any of this runs: `auth::gate` gives every `GET`
//! under `/v1/admin` to `inspect` and everything else to `admin`, the same
//! split the facade's `GET`-iff-`NO_SIDE_EFFECTS` rule makes.
//!
//! The custom methods (`:pause`, `:replay`, `:trigger`, …) are not axum
//! handlers: [`super::routes`] splits the verb off the path and calls the
//! function here with the id it found.

use std::future::Future;

use axum::extract::rejection::PathRejection;
use axum::extract::{Path, Request, State};
use axum::response::Response;
use http::request::Parts;
use serde_json::Value;
use tonic::Status;

use super::error;
use super::json::{admin_request as read, admin_response as write};
use super::routes::{decode, finish, path_param, query, scoped};
use crate::grpc::admin::Admin;
use crate::grpc::pb::admin as pb;
use crate::grpc::pb::admin::admin_service_server::AdminService;
use crate::grpc::status::WireError;

/// Scope `message` to the caller, call the RPC, and render its answer — or
/// refuse, if the message could not be read.
async fn answer<Req, Res, Fut>(
    parts: &Parts,
    message: Result<Req, WireError>,
    rpc: impl FnOnce(tonic::Request<Req>) -> Fut,
    render: fn(&Res) -> Value,
) -> Response
where
    Fut: Future<Output = Result<tonic::Response<Res>, Status>>,
{
    match message.and_then(|message| scoped(parts, message)) {
        Ok(request) => finish(rpc(request).await, render),
        Err(error) => error::refuse(error),
    }
}

/// A JSON body, read and converted, with a conversion failure named as an
/// invalid request rather than a malformed one: it parsed, and it is not a
/// request this service accepts.
async fn body<T, M>(
    request: Request,
    convert: impl FnOnce(T) -> Result<M, String>,
) -> (Parts, Result<M, WireError>)
where
    T: serde::de::DeserializeOwned,
{
    let (parts, body) = request.into_parts();
    let message = match decode::<T>(body).await {
        Ok(value) => convert(value).map_err(WireError::invalid_request),
        Err(error) => Err(error),
    };
    (parts, message)
}

// ── Queues ───────────────────────────────────────────────────────────

pub(super) async fn list_queues(State(admin): State<Admin>, parts: Parts) -> Response {
    answer(
        &parts,
        Ok(pb::ListQueuesRequest {}),
        |request| admin.list_queues(request),
        write::list_queues,
    )
    .await
}

pub(super) async fn pause_queue(admin: &Admin, parts: &Parts, queue: String) -> Response {
    answer(
        parts,
        Ok(pb::PauseQueueRequest { queue }),
        |request| admin.pause_queue(request),
        write::pause_queue,
    )
    .await
}

pub(super) async fn resume_queue(admin: &Admin, parts: &Parts, queue: String) -> Response {
    answer(
        parts,
        Ok(pb::ResumeQueueRequest { queue }),
        |request| admin.resume_queue(request),
        write::resume_queue,
    )
    .await
}

pub(super) async fn get_throughput(State(admin): State<Admin>, parts: Parts) -> Response {
    let message = query::<read::GetThroughput>(&parts).map(read::GetThroughput::into_message);
    answer(
        &parts,
        message,
        |request| admin.get_throughput(request),
        write::get_throughput,
    )
    .await
}

// ── Dead letters ─────────────────────────────────────────────────────

pub(super) async fn list_dead_letters(State(admin): State<Admin>, parts: Parts) -> Response {
    let message = query::<read::ListDeadLetters>(&parts).map(read::ListDeadLetters::into_message);
    answer(
        &parts,
        message,
        |request| admin.list_dead_letters(request),
        write::list_dead_letters,
    )
    .await
}

pub(super) async fn get_dead_letter(
    State(admin): State<Admin>,
    id: Result<Path<String>, PathRejection>,
    parts: Parts,
) -> Response {
    let message = path_param(id).and_then(|dead_letter_id| {
        let blobs: read::IncludePayload = query(&parts)?;
        Ok(pb::GetDeadLetterRequest {
            dead_letter_id,
            include_payload: blobs.include_payload,
        })
    });
    answer(
        &parts,
        message,
        |request| admin.get_dead_letter(request),
        write::get_dead_letter,
    )
    .await
}

pub(super) async fn replay_dead_letter(
    admin: &Admin,
    parts: &Parts,
    dead_letter_id: String,
) -> Response {
    answer(
        parts,
        Ok(pb::ReplayDeadLetterRequest { dead_letter_id }),
        |request| admin.replay_dead_letter(request),
        write::replay_dead_letter,
    )
    .await
}

pub(super) async fn delete_dead_letter(
    admin: &Admin,
    parts: &Parts,
    dead_letter_id: String,
) -> Response {
    answer(
        parts,
        Ok(pb::DeleteDeadLetterRequest { dead_letter_id }),
        |request| admin.delete_dead_letter(request),
        write::empty,
    )
    .await
}

pub(super) async fn purge_dead_letters(State(admin): State<Admin>, request: Request) -> Response {
    let (parts, message) = body(request, read::PurgeDeadLetters::into_message).await;
    answer(
        &parts,
        message,
        |request| admin.purge_dead_letters(request),
        write::purge_dead_letters,
    )
    .await
}

// ── Workers ──────────────────────────────────────────────────────────

pub(super) async fn list_workers(State(admin): State<Admin>, parts: Parts) -> Response {
    answer(
        &parts,
        Ok(pb::ListWorkersRequest {}),
        |request| admin.list_workers(request),
        write::list_workers,
    )
    .await
}

// ── Periodic tasks ───────────────────────────────────────────────────

pub(super) async fn list_periodic_tasks(State(admin): State<Admin>, parts: Parts) -> Response {
    answer(
        &parts,
        Ok(pb::ListPeriodicTasksRequest {}),
        |request| admin.list_periodic_tasks(request),
        write::list_periodic_tasks,
    )
    .await
}

pub(super) async fn get_periodic_task(
    State(admin): State<Admin>,
    name: Result<Path<String>, PathRejection>,
    parts: Parts,
) -> Response {
    let message = path_param(name).and_then(|name| {
        let blobs: read::IncludePayload = query(&parts)?;
        Ok(pb::GetPeriodicTaskRequest {
            name,
            include_payload: blobs.include_payload,
        })
    });
    answer(
        &parts,
        message,
        |request| admin.get_periodic_task(request),
        write::get_periodic_task,
    )
    .await
}

pub(super) async fn put_periodic_task(State(admin): State<Admin>, request: Request) -> Response {
    let (parts, message) = body(request, read::PutPeriodicTask::into_message).await;
    answer(
        &parts,
        message,
        |request| admin.put_periodic_task(request),
        write::put_periodic_task,
    )
    .await
}

pub(super) async fn delete_periodic_task(admin: &Admin, parts: &Parts, name: String) -> Response {
    answer(
        parts,
        Ok(pb::DeletePeriodicTaskRequest { name }),
        |request| admin.delete_periodic_task(request),
        write::empty,
    )
    .await
}

pub(super) async fn pause_periodic_task(admin: &Admin, parts: &Parts, name: String) -> Response {
    answer(
        parts,
        Ok(pb::PausePeriodicTaskRequest { name }),
        |request| admin.pause_periodic_task(request),
        write::pause_periodic_task,
    )
    .await
}

pub(super) async fn resume_periodic_task(admin: &Admin, parts: &Parts, name: String) -> Response {
    answer(
        parts,
        Ok(pb::ResumePeriodicTaskRequest { name }),
        |request| admin.resume_periodic_task(request),
        write::resume_periodic_task,
    )
    .await
}

pub(super) async fn trigger_periodic_task(admin: &Admin, parts: &Parts, name: String) -> Response {
    answer(
        parts,
        Ok(pb::TriggerPeriodicTaskRequest { name }),
        |request| admin.trigger_periodic_task(request),
        write::trigger_periodic_task,
    )
    .await
}

// ── Overrides ────────────────────────────────────────────────────────

pub(super) async fn list_overrides(State(admin): State<Admin>, parts: Parts) -> Response {
    answer(
        &parts,
        Ok(pb::ListOverridesRequest {}),
        |request| admin.list_overrides(request),
        write::list_overrides,
    )
    .await
}

/// The body is the `TaskOverride` itself — the binding names `task_override`
/// as the body — and the task comes from the path.
pub(super) async fn set_task_override(
    State(admin): State<Admin>,
    task_name: Result<Path<String>, PathRejection>,
    request: Request,
) -> Response {
    let (parts, task_override) = body(
        request,
        |value: read::TaskOverride| Ok(value.into_message()),
    )
    .await;
    let message = path_param(task_name).and_then(|task_name| {
        Ok(pb::SetTaskOverrideRequest {
            task_name,
            task_override: Some(task_override?),
        })
    });
    answer(
        &parts,
        message,
        |request| admin.set_task_override(request),
        write::set_task_override,
    )
    .await
}

pub(super) async fn clear_task_override(
    State(admin): State<Admin>,
    task_name: Result<Path<String>, PathRejection>,
    parts: Parts,
) -> Response {
    let message = path_param(task_name).map(|task_name| pb::ClearTaskOverrideRequest { task_name });
    answer(
        &parts,
        message,
        |request| admin.clear_task_override(request),
        write::empty,
    )
    .await
}

/// The body is the `QueueOverride` itself; the queue comes from the path.
pub(super) async fn set_queue_override(
    State(admin): State<Admin>,
    queue: Result<Path<String>, PathRejection>,
    request: Request,
) -> Response {
    let (parts, queue_override) = body(request, |value: read::QueueOverride| {
        Ok(value.into_message())
    })
    .await;
    let message = path_param(queue).and_then(|queue| {
        Ok(pb::SetQueueOverrideRequest {
            queue,
            queue_override: Some(queue_override?),
        })
    });
    answer(
        &parts,
        message,
        |request| admin.set_queue_override(request),
        write::set_queue_override,
    )
    .await
}

pub(super) async fn clear_queue_override(
    State(admin): State<Admin>,
    queue: Result<Path<String>, PathRejection>,
    parts: Parts,
) -> Response {
    let message = path_param(queue).map(|queue| pb::ClearQueueOverrideRequest { queue });
    answer(
        &parts,
        message,
        |request| admin.clear_queue_override(request),
        write::empty,
    )
    .await
}
