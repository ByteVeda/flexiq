//! One module per subcommand group.
//!
//! Each owns the construction of its own request, as a plain function of the
//! parsed flags, so the mapping from what an operator typed to what goes on the
//! wire is testable without a server. Only the thin `run` around it needs one.

pub mod dlq;
pub mod enqueue;
pub mod jobs;
pub mod overrides;
pub mod periodic;
pub mod queues;
pub mod throughput;
pub mod workers;

use anyhow::{anyhow, Result};
use serde_json::Value;

use crate::{error, output, pb};

/// One job — got, cancelled, replayed or triggered — as JSON or a one-row
/// table. Every response carrying a lone `Job` is the same `{job}` shape.
fn print_job(job: Option<&pb::Job>, json: bool) -> Result<()> {
    emit(
        json,
        || output::job_envelope_json(job),
        || {
            let rows = job
                .map(|job| vec![output::job_row(job)])
                .unwrap_or_default();
            output::table(&output::JOB_COLUMNS, &rows)
        },
    )
}

/// A non-OK status as an error an operator can act on.
fn refused(status: tonic::Status) -> anyhow::Error {
    anyhow!("{}", error::describe(&status))
}

/// Print one response: proto3 JSON under `--json`, the table otherwise.
///
/// Both renderings are closures so only the one asked for is built.
fn emit(
    json: bool,
    as_json: impl FnOnce() -> Value,
    as_table: impl FnOnce() -> String,
) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(&as_json())?);
    } else {
        print!("{}", as_table());
    }
    Ok(())
}
