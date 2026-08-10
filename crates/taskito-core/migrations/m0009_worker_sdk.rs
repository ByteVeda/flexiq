//! SDK identity on workers (`0009_worker_sdk`).
//!
//! A polyglot deployment runs workers built from different SDKs, and those SDKs
//! upgrade independently. The registry recorded `pool_type` — which shell ran
//! the worker — but not which release of it, so an operator looking at a
//! misbehaving fleet could not tell a stale worker from a current one without
//! going host by host.
//!
//! Both columns are nullable: a worker registered before this migration keeps
//! its row, and a shell that does not report its version yet is a missing
//! value rather than a wrong one.
//!
//! Idempotent: `add_column` swallows the duplicate on SQLite and emits
//! `IF NOT EXISTS` on Postgres.

use sea_query::{Alias, ColumnDef};

use crate::storage::migrate::{add_column, Backend, Migration, Stmt};

pub struct M0009WorkerSdk;

fn col(name: &str) -> ColumnDef {
    ColumnDef::new(Alias::new(name))
}

impl Migration for M0009WorkerSdk {
    fn version(&self) -> &'static str {
        "0009_worker_sdk"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        vec![
            add_column(b, "workers", col("sdk").text()),
            add_column(b, "workers", col("sdk_version").text()),
        ]
    }
}
