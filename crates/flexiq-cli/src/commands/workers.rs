//! `fq workers` — the namespace's registered workers and their heartbeats.
//!
//! Read-only: the wire cannot tell a worker to drain yet, and a verb that
//! reported success for a signal nobody reads would lie.

use anyhow::Result;

use super::{emit, refused};
use crate::connect::AdminClient;
use crate::output::admin::{list_workers_json, worker_row, WORKER_COLUMNS};
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
