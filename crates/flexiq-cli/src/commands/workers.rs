//! `fq workers` and `fq drain`.
//!
//! `fq workers` lists the namespace's registered workers and their heartbeats.
//! `fq drain` asks one of them to stop claiming, finish what it holds and
//! unregister; the worker reads the request on its next heartbeat, so the
//! answer is the row marked `DRAINING`, not the worker gone.

use anyhow::Result;

use super::{emit, refused};
use crate::cli::WorkerIdArgs;
use crate::connect::AdminClient;
use crate::output::admin::{list_workers_json, worker_envelope_json, worker_row, WORKER_COLUMNS};
use crate::{output, pb};

/// Fetch the workers and print them, by id as the server orders them.
pub async fn run(client: &mut AdminClient, json: bool) -> Result<()> {
    let response = client
        .list_workers(pb::admin::ListWorkersRequest {})
        .await
        .map_err(refused)?
        .into_inner();
    emit(
        json,
        || list_workers_json(&response),
        || {
            let rows: Vec<_> = response.workers.iter().map(worker_row).collect();
            output::table(&WORKER_COLUMNS, &rows)
        },
    )
}

/// The drain request.
pub fn drain_request(args: &WorkerIdArgs) -> pb::admin::DrainWorkerRequest {
    pb::admin::DrainWorkerRequest {
        worker_id: args.worker_id.clone(),
    }
}

/// `fq drain`. Idempotent: draining a draining worker is the same answer.
pub async fn drain(client: &mut AdminClient, args: &WorkerIdArgs, json: bool) -> Result<()> {
    let response = client
        .drain_worker(drain_request(args))
        .await
        .map_err(refused)?
        .into_inner();
    let worker = response.worker.as_ref();
    emit(
        json,
        || worker_envelope_json(worker),
        || {
            let rows = worker
                .map(|worker| vec![worker_row(worker)])
                .unwrap_or_default();
            output::table(&WORKER_COLUMNS, &rows)
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_drain_names_the_worker() {
        let args = WorkerIdArgs {
            worker_id: "w-1".into(),
        };
        assert_eq!(drain_request(&args).worker_id, "w-1");
    }
}
