//! Audit trail of token-authorised calls (`0022_audit_log`, #840).
//!
//! One row per authorised mutating call a network door served: which token
//! (its public id, never the secret or its digest), under which name, what it
//! did, to what, when, and how it ended. Append-only — nothing updates a row.
//!
//! `namespace` is `NOT NULL`: a door is always bound to exactly one namespace,
//! so there is no unnamespaced audit row to represent.
//!
//! The three indexes are the three questions the table exists to answer: what
//! happened recently (`at_ms`), what did this token do (`token_id`), and who
//! touched this job/queue/worker (`target_kind`, `target`). Each leads with
//! `namespace` because every read and the retention delete are scoped by it.
//!
//! Idempotent: `.if_not_exists()` on the table and on every index.

use sea_query::{Alias, ColumnDef, Index, Table};

use crate::storage::migrate::{ddl, Backend, Migration, Stmt};

pub struct M0022AuditLog;

fn col(name: &str) -> ColumnDef {
    ColumnDef::new(Alias::new(name))
}

fn t(name: &str) -> Alias {
    Alias::new(name)
}

impl Migration for M0022AuditLog {
    fn version(&self) -> &'static str {
        "0022_audit_log"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        let audit = Table::create()
            .table(t("audit_log"))
            .if_not_exists()
            .col(col("id").text().not_null().primary_key())
            .col(col("namespace").text().not_null())
            .col(col("at_ms").big_integer().not_null())
            .col(col("token_id").text().not_null())
            .col(col("principal").text().not_null())
            .col(col("operation").text().not_null())
            .col(col("target_kind").text())
            .col(col("target").text())
            .col(col("outcome").text().not_null())
            .to_owned();

        let by_time = Index::create()
            .if_not_exists()
            .name("idx_audit_log_ns_at")
            .table(t("audit_log"))
            .col(t("namespace"))
            .col(t("at_ms"))
            .to_owned();

        let by_token = Index::create()
            .if_not_exists()
            .name("idx_audit_log_ns_token_at")
            .table(t("audit_log"))
            .col(t("namespace"))
            .col(t("token_id"))
            .col(t("at_ms"))
            .to_owned();

        let by_target = Index::create()
            .if_not_exists()
            .name("idx_audit_log_ns_target")
            .table(t("audit_log"))
            .col(t("namespace"))
            .col(t("target_kind"))
            .col(t("target"))
            .to_owned();

        vec![
            ddl(b, &audit),
            ddl(b, &by_time),
            ddl(b, &by_token),
            ddl(b, &by_target),
        ]
    }
}
