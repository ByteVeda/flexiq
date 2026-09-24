//! The periodic-task RPCs.
//!
//! A task is identified by `(namespace, name)` (#918); every call here passes
//! the caller's namespace, so a name only ever reaches that tenant's row.

use flexiq_core::periodic::{next_run, periodic_job};
use flexiq_core::{now_millis, NewPeriodicTask, PeriodicTask, Storage, StorageBackend};
use tonic::{Response, Status};

use super::{convert, require, Scoped};
use crate::grpc::blocking::on_storage;
use crate::grpc::pb;
use crate::grpc::pb::admin::put_periodic_task_request::Body;
use crate::grpc::producer::convert::{job_to_wire, Blobs};
use crate::grpc::producer::structured;
use crate::grpc::status::{reason, WireError};

/// The queue a task fires into when the request names none.
const DEFAULT_QUEUE: &str = "default";

/// Every periodic task in the namespace, by name, with no payloads.
pub(crate) async fn list(
    scoped: &Scoped,
) -> Result<Response<pb::admin::ListPeriodicTasksResponse>, Status> {
    let namespace = scoped.namespace_owned();
    let mut tasks = on_storage(scoped.storage(), move |storage| {
        storage.list_periodic(Some(&namespace))
    })
    .await?;
    tasks.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(Response::new(pb::admin::ListPeriodicTasksResponse {
        periodic_tasks: tasks
            .into_iter()
            .map(|task| convert::periodic_task(task, false))
            .collect(),
    }))
}

/// One task, with its payload when asked for.
pub(crate) async fn get(
    scoped: &Scoped,
    request: pb::admin::GetPeriodicTaskRequest,
) -> Result<Response<pb::admin::GetPeriodicTaskResponse>, Status> {
    let name = require("name", request.name)?;
    let task = read(scoped, name).await?;
    Ok(Response::new(pb::admin::GetPeriodicTaskResponse {
        periodic_task: Some(convert::periodic_task(task, request.include_payload)),
    }))
}

/// Create a task or replace its definition, through the conditional write a
/// code declaration makes (#919): an existing task keeps its pause and its
/// last run, and its next run moves only if the schedule changed.
pub(crate) async fn put(
    scoped: &Scoped,
    request: pb::admin::PutPeriodicTaskRequest,
) -> Result<Response<pb::admin::PutPeriodicTaskResponse>, Status> {
    let name = require("name", request.name)?;
    let task_name = require("task_name", request.task_name)?;
    let cron = require("cron", request.cron)?;
    let queue = if request.queue.is_empty() {
        DEFAULT_QUEUE.to_string()
    } else {
        request.queue
    };
    let timezone = request.timezone;
    // Computing the first run is what validates both the expression and the
    // timezone, before anything is written.
    let first_run = next_run(&cron, timezone.as_deref(), now_millis())
        .map_err(|error| WireError::invalid_request(error.to_string()))?;
    // No arm is a call with no arguments — the empty envelope, never an empty
    // payload, which no worker could decode.
    let payload = match request.body {
        Some(Body::Raw(bytes)) => bytes,
        Some(Body::Structured(args)) => structured::encode(args)?,
        None => structured::encode(pb::StructuredArgs::default())?,
    };

    let declaration = NewPeriodicTask {
        name: name.clone(),
        task_name,
        cron_expr: cron,
        args: Some(payload),
        // The shells fold keyword arguments into `args`; the scheduler never
        // reads this column.
        kwargs: None,
        queue,
        enabled: !request.start_paused,
        next_run: first_run,
        timezone,
        namespace: Some(scoped.namespace_owned()),
    };
    on_storage(scoped.storage(), move |storage| {
        storage.declare_periodic(&declaration)
    })
    .await?;

    let task = read(scoped, name).await?;
    Ok(Response::new(pb::admin::PutPeriodicTaskResponse {
        periodic_task: Some(convert::periodic_task(task, false)),
    }))
}

/// Delete a task. An absent one is `NOT_FOUND`.
pub(crate) async fn delete(
    scoped: &Scoped,
    request: pb::admin::DeletePeriodicTaskRequest,
) -> Result<Response<pb::admin::DeletePeriodicTaskResponse>, Status> {
    let name = require("name", request.name)?;
    let namespace = scoped.namespace_owned();
    let lookup = name.clone();
    let deleted = on_storage(scoped.storage(), move |storage| {
        storage.delete_periodic(&lookup, Some(&namespace))
    })
    .await?;
    if !deleted {
        return Err(not_found(&name));
    }
    Ok(Response::new(pb::admin::DeletePeriodicTaskResponse {}))
}

/// Pause or resume a task, answering with the state that leaves it in.
pub(crate) async fn set_enabled(
    scoped: &Scoped,
    name: String,
    enabled: bool,
) -> Result<pb::admin::PeriodicTask, Status> {
    let name = require("name", name)?;
    let namespace = scoped.namespace_owned();
    let lookup = name.clone();
    let found = on_storage(scoped.storage(), move |storage| {
        storage.set_periodic_enabled(&lookup, enabled, Some(&namespace))
    })
    .await?;
    if !found {
        return Err(not_found(&name));
    }
    let task = read(scoped, name).await?;
    Ok(convert::periodic_task(task, false))
}

/// Enqueue the job the schedule would fire, now, leaving the schedule alone.
///
/// No unique key: the scheduler's is `periodic:<name>:<now>`, and reusing it
/// would silently fold a trigger into a firing that landed in the same
/// millisecond. A trigger is an operator asking for one more run.
pub(crate) async fn trigger(
    scoped: &Scoped,
    request: pb::admin::TriggerPeriodicTaskRequest,
) -> Result<Response<pb::admin::TriggerPeriodicTaskResponse>, Status> {
    let name = require("name", request.name)?;
    let task = read(scoped, name).await?;
    let events = scoped.events();
    let job = on_storage(scoped.storage(), move |storage| {
        let job = storage.enqueue(periodic_job(&task, now_millis(), None))?;
        crate::events::enqueued(events.as_deref(), &job);
        Ok(job)
    })
    .await?;
    Ok(Response::new(pb::admin::TriggerPeriodicTaskResponse {
        job: Some(job_to_wire(job, Blobs::NONE)),
    }))
}

/// The caller's task named `name`, or `NOT_FOUND`.
///
/// `Storage` has no point read for a periodic task; a namespace's schedules are
/// few enough that reading them to find one costs nothing a point read would
/// save.
async fn read(scoped: &Scoped, name: String) -> Result<PeriodicTask, Status> {
    let namespace = scoped.namespace_owned();
    let lookup = name.clone();
    on_storage(scoped.storage(), move |storage: &StorageBackend| {
        Ok(storage
            .list_periodic(Some(&namespace))?
            .into_iter()
            .find(|task| task.name == lookup))
    })
    .await?
    .ok_or_else(|| not_found(&name))
}

fn not_found(name: &str) -> Status {
    WireError::not_found(reason::PERIODIC_TASK_NOT_FOUND, "periodic task", name).into()
}
