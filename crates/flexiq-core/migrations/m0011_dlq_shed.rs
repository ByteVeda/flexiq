//! Shed flag on dead-letter entries (`0011_dlq_shed`).
//!
//! A shed job is dead-lettered like a failed one, but it must never be
//! auto-retried — resurrecting a job the scheduler deliberately dropped would
//! undo the shed. The sweep used to enforce that in Rust, *after* asking storage
//! for a page of candidates ordered by `failed_at` and truncated in the query.
//! Because a shed entry is never retried, its `dlq_retry_count` stays 0 and it
//! never leaves the candidate window: enough shed rows older than a genuine
//! failure fill every page, and that failure is never retried until retention
//! purges them. The flag lets the query drop them instead.
//!
//! Two decisions worth recording:
//!
//! **A column, not a sentinel `dlq_retry_count`.** Writing shed rows at the
//! retry ceiling so the existing `dlq_retry_count < max_retries` filter excludes
//! them is the column-overloading trap #648 calls out, and `max_retries` is
//! configuration — no fixed sentinel is correct for every deployment.
//!
//! **Storage learns "shed", not `codel:`.** The reason prefixes stay the
//! scheduler's vocabulary; `move_to_dlq` is told the disposition explicitly
//! rather than parsing the error string in SQL and Lua across three backends.
//!
//! The index is partial on `shed = false` because the sweep only ever reads
//! retryable rows, and the case this migration exists for is a `dead_letter`
//! table that is mostly shed. It leads with `namespace` — always an equality
//! bound in the sweep — so `failed_at` still serves the range and the ordering.
//!
//! Idempotent: `add_column` swallows the duplicate on SQLite and emits
//! `IF NOT EXISTS` on Postgres, and the index uses `.if_not_exists()`.

use sea_query::{Alias, ColumnDef, ConditionalStatement, Expr, ExprTrait, Index};

use crate::storage::migrate::{add_column, ddl, Backend, Migration, Stmt};

pub struct M0011DlqShed;

fn col(name: &str) -> ColumnDef {
    ColumnDef::new(Alias::new(name))
}

fn t(name: &str) -> Alias {
    Alias::new(name)
}

impl Migration for M0011DlqShed {
    fn version(&self) -> &'static str {
        "0011_dlq_shed"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        let retry_candidates = Index::create()
            .if_not_exists()
            .name("idx_dead_letter_retry")
            .table(t("dead_letter"))
            .col(t("namespace"))
            .col(t("failed_at"))
            .and_where(Expr::col(t("shed")).eq(false))
            .to_owned();

        vec![
            add_column(
                b,
                "dead_letter",
                col("shed").boolean().not_null().default(false),
            ),
            ddl(b, &retry_candidates),
        ]
    }
}
