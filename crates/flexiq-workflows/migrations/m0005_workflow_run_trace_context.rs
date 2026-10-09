//! Trace context on workflow runs (`0005_workflow_run_trace_context`).
//!
//! A node job's metadata is built from the run, not from a caller's enqueue,
//! so the submitter's W3C trace context has to live on the run row for every
//! node — including successors released long after submit — to carry it.
//! Nullable: existing runs, and runs submitted without a trace, have none.
//!
//! Idempotent: `add_column` swallows the duplicate on SQLite and emits
//! `IF NOT EXISTS` on Postgres.

use sea_query::{Alias, ColumnDef};

use flexiq_core::storage::migrate::{add_column, Backend, Migration, Stmt};

pub struct M0005WorkflowRunTraceContext;

fn col(name: &str) -> ColumnDef {
    ColumnDef::new(Alias::new(name))
}

impl Migration for M0005WorkflowRunTraceContext {
    fn version(&self) -> &'static str {
        "0005_workflow_run_trace_context"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        vec![
            add_column(b, "workflow_runs", col("traceparent").text()),
            add_column(b, "workflow_runs", col("tracestate").text()),
        ]
    }
}
