//! Retention purge indexes (`0003_retention_indexes`).
//!
//! Indexes for the predicates the retention sweeps run. Without these, batching
//! a purge turns one full scan into a full scan *per batch* — strictly worse
//! than the unbatched delete it replaced.
//!
//! The per-entry TTL predicates (`completed_at + result_ttl_ms < now`) are
//! expressions over two columns, which no ordinary index serves. They get
//! partial indexes instead: a per-entry TTL is opt-in and rare, so
//! `WHERE result_ttl_ms IS NOT NULL` is highly selective and keeps the scan
//! proportional to the rows that actually carry one.

use sea_query::{Alias, ConditionalStatement, Expr, ExprTrait, Index};

use crate::storage::migrate::{ddl, Backend, Migration, Stmt};

pub struct M0003RetentionIndexes;

fn t(name: &str) -> Alias {
    Alias::new(name)
}

impl Migration for M0003RetentionIndexes {
    fn version(&self) -> &'static str {
        "0003_retention_indexes"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        vec![
            // `job_errors` is the only purge target with no index on its
            // timestamp — the others already have one from the baseline.
            ddl(
                b,
                &Index::create()
                    .if_not_exists()
                    .name("idx_job_errors_failed_at")
                    .table(t("job_errors"))
                    .col(t("failed_at"))
                    .to_owned(),
            ),
            ddl(
                b,
                &Index::create()
                    .if_not_exists()
                    .name("idx_dead_letter_ttl")
                    .table(t("dead_letter"))
                    .col(t("failed_at"))
                    .and_where(Expr::col(t("result_ttl_ms")).is_not_null())
                    .to_owned(),
            ),
            ddl(
                b,
                &Index::create()
                    .if_not_exists()
                    .name("idx_archived_jobs_ttl")
                    .table(t("archived_jobs"))
                    .col(t("completed_at"))
                    .and_where(Expr::col(t("result_ttl_ms")).is_not_null())
                    .to_owned(),
            ),
            // The dead-worker reap scans `workers` on every heartbeat. The reaper
            // election is the real fix; this keeps the scan cheap for fleets large
            // enough that even the elected reaper's sweep matters.
            ddl(
                b,
                &Index::create()
                    .if_not_exists()
                    .name("idx_workers_last_heartbeat")
                    .table(t("workers"))
                    .col(t("last_heartbeat"))
                    .to_owned(),
            ),
        ]
    }
}
