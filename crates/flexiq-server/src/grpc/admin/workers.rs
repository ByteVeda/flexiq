//! `ListWorkers`: the namespace's registry rows.

use flexiq_core::Storage;
use tonic::{Response, Status};

use super::{convert, Scoped};
use crate::grpc::blocking::on_storage;
use crate::grpc::pb::admin as pb;

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
