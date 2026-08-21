//! Task-registry fingerprint on workers (`0012_worker_registry_fingerprint`).
//!
//! Autodiscovery made a worker's task registry implicit — it is whatever the
//! import walk found — and an unregistered task name is a fatal, non-retryable
//! failure. A worker that imported eleven of twelve modules dead-letters every
//! job for the twelfth and says nothing anywhere. The registry already recorded
//! which SDK and release a worker runs (`0009_worker_sdk`); this records *what
//! it can run*, as one short comparable value, so the odd one out in a fleet is
//! visible in the dashboard instead of by going host by host.
//!
//! The column holds the fingerprint, not the names: it exists to be compared at
//! a glance across a fleet, and a name list would grow the row without making
//! that comparison any easier. Attached executors need no column at all — they
//! send `tasks[]` on their `hello` frame, and the scheduler holds both name
//! sets in memory and can name the difference outright.
//!
//! Nothing here compares one row against another. A heterogeneous fleet is
//! normal in this table — one worker serves `email`, another serves `video` —
//! so a blanket warning would be noise. The comparison belongs where it has a
//! premise: attached executors all feed one scheduler over one queue set.
//!
//! Nullable: a worker registered before this migration keeps its row, and a
//! shell that does not report a registry yet is a missing value rather than a
//! wrong one — the same rule the check itself follows, where "reports nothing"
//! never counts as "differs from everyone".
//!
//! Idempotent: `add_column` swallows the duplicate on SQLite and emits
//! `IF NOT EXISTS` on Postgres.

use sea_query::{Alias, ColumnDef};

use crate::storage::migrate::{add_column, Backend, Migration, Stmt};

pub struct M0012WorkerRegistryFingerprint;

impl Migration for M0012WorkerRegistryFingerprint {
    fn version(&self) -> &'static str {
        "0012_worker_registry_fingerprint"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        vec![add_column(
            b,
            "workers",
            ColumnDef::new(Alias::new("registry_fingerprint")).text(),
        )]
    }
}
