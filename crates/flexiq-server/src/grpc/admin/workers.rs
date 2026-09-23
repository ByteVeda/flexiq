//! `ListWorkers` and `DrainWorker`: the namespace's registry rows.

use flexiq_core::Storage;
use tonic::{Response, Status};

use super::{convert, require, Scoped};
use crate::grpc::blocking::on_storage;
use crate::grpc::pb::admin as pb;
use crate::grpc::status::{reason, WireError};

/// Every worker registered in the caller's namespace, by id.
///
/// A worker registered before `0021_worker_namespace` reads as the default
/// namespace's until it restarts, so a namespaced door cannot see it — the
/// answer a namespaced listing has to give.
pub(crate) async fn list(scoped: &Scoped) -> Result<Response<pb::ListWorkersResponse>, Status> {
    let namespace = scoped.namespace_owned();
    let mut workers = on_storage(scoped.storage(), move |storage| {
        storage.list_workers(Some(&namespace))
    })
    .await?;
    workers.sort_by(|a, b| a.worker_id.cmp(&b.worker_id));

    Ok(Response::new(pb::ListWorkersResponse {
        workers: workers.into_iter().map(convert::worker).collect(),
    }))
}

/// Record a drain request for one of the namespace's workers, and answer with
/// its row as the request left it.
///
/// The request is a status on the row; the worker reads it on its next
/// heartbeat and stops as it would on SIGTERM. A worker in another namespace,
/// or one already gone, answers `NOT_FOUND`.
pub(crate) async fn drain(
    scoped: &Scoped,
    request: pb::DrainWorkerRequest,
) -> Result<Response<pb::DrainWorkerResponse>, Status> {
    let worker_id = require("worker_id", request.worker_id)?;
    let namespace = scoped.namespace_owned();
    let lookup = worker_id.clone();
    let worker = on_storage(scoped.storage(), move |storage| {
        if !storage.request_worker_drain(&lookup, Some(&namespace))? {
            return Ok(None);
        }
        Ok(storage
            .list_workers(Some(&namespace))?
            .into_iter()
            .find(|worker| worker.worker_id == lookup))
    })
    .await?
    // Also reached when the worker unregistered between the request and the
    // read-back — gone either way.
    .ok_or_else(|| {
        Status::from(WireError::not_found(
            reason::WORKER_NOT_FOUND,
            "worker",
            &worker_id,
        ))
    })?;

    Ok(Response::new(pb::DrainWorkerResponse {
        worker: Some(convert::worker(worker)),
    }))
}
