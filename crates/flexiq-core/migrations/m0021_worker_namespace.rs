//! Record which namespace a worker serves (`0021_worker_namespace`).
//!
//! The worker registry was one list for the whole database, so a namespaced
//! dashboard — and the admin door (#836) — showed every tenant's workers. A
//! worker already knows its namespace (its scheduler dequeues by it); this
//! stores it beside the row so a listing can be scoped.
//!
//! A plain nullable column, not a rebuild: `worker_id` is globally unique and
//! stays the key, so identity does not change — the namespace is an attribute
//! a listing filters on. NULL is the default namespace, which is also what a
//! row registered before this migration reads as until that worker restarts
//! and registers again.
//!
//! Idempotent: `add_column` swallows the duplicate on SQLite and emits
//! `IF NOT EXISTS` on Postgres.

use sea_query::{Alias, ColumnDef};

use crate::storage::migrate::{add_column, Backend, Migration, Stmt};

pub struct M0021WorkerNamespace;

impl Migration for M0021WorkerNamespace {
    fn version(&self) -> &'static str {
        "0021_worker_namespace"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        vec![add_column(
            b,
            "workers",
            ColumnDef::new(Alias::new("namespace")).text(),
        )]
    }
}
