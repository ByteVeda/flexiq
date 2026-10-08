//! Whether an audit record is a read or a write (`0027_audit_access`, #1018).
//!
//! Reads are recorded only when an operator switches them on (#993), and they
//! are far more numerous than writes, so they get their own, shorter retention
//! window. The prune can only tell them apart if the row says so: `access` is
//! `write` or `read`.
//!
//! `NOT NULL DEFAULT 'write'` backfills older rows conservatively: a row from
//! before the column keeps the longer, write window, so the migration can only
//! keep a record longer than it would have been, never drop one early.
//!
//! The index leads with `namespace` and `access` because the read prune asks
//! exactly "this namespace's reads older than the cutoff".
//!
//! Redis has no schema to migrate; `AuditRecord` gains a `#[serde(default)]`
//! field reading back `write`.
//!
//! Idempotent: `add_column` swallows the duplicate on SQLite and emits
//! `IF NOT EXISTS` on Postgres; the index is `.if_not_exists()`.

use sea_query::{Alias, ColumnDef, Index};

use crate::storage::migrate::{add_column, ddl, Backend, Migration, Stmt};

pub struct M0027AuditAccess;

impl Migration for M0027AuditAccess {
    fn version(&self) -> &'static str {
        "0027_audit_access"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        let by_access = Index::create()
            .if_not_exists()
            .name("idx_audit_log_ns_access_at")
            .table(Alias::new("audit_log"))
            .col(Alias::new("namespace"))
            .col(Alias::new("access"))
            .col(Alias::new("at_ms"))
            .to_owned();

        vec![
            add_column(
                b,
                "audit_log",
                ColumnDef::new(Alias::new("access"))
                    .text()
                    .not_null()
                    .default("write"),
            ),
            ddl(b, &by_access),
        ]
    }
}
