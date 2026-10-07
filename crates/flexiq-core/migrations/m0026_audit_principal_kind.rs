//! What kind of caller an audit record names (`0026_audit_principal_kind`, #994).
//!
//! Until now every record was a token's. The dashboard records its users'
//! actions and the token command line its mints and revokes, so a record now
//! says which credential `token_id` holds: `token`, `user`, `cli` or
//! `anonymous`. A column rather than a prefix inside `token_id`, because a
//! listing filters by exact match — "every dashboard action" is a filter on
//! this column, never a pattern on that one.
//!
//! `NOT NULL DEFAULT 'token'` backfills the existing rows truthfully: before
//! this migration only the token door wrote the table. No index — the kind is
//! a coarse cut asked alongside the time or target indexes, never alone.
//!
//! Redis has no schema to migrate; `AuditRecord` gains a `#[serde(default)]`
//! field reading back `token` from records written before it.
//!
//! Idempotent: `add_column` swallows the duplicate on SQLite and emits
//! `IF NOT EXISTS` on Postgres.

use sea_query::{Alias, ColumnDef};

use crate::storage::migrate::{add_column, Backend, Migration, Stmt};

pub struct M0026AuditPrincipalKind;

impl Migration for M0026AuditPrincipalKind {
    fn version(&self) -> &'static str {
        "0026_audit_principal_kind"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        vec![add_column(
            b,
            "audit_log",
            ColumnDef::new(Alias::new("principal_kind"))
                .text()
                .not_null()
                .default("token"),
        )]
    }
}
