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
//! **The backfill is where the prefixes are allowed.** Rows written before this
//! column existed default to `false`, so an existing deployment — the only kind
//! that has the bug — would keep starving until retention aged its shed rows
//! out. A one-shot `UPDATE` matching the reserved prefixes fixes them on the
//! spot. That is a migration reading history, not the sweep's hot path learning
//! a vocabulary: `list_dead_for_retry` still only ever sees `shed`. Redis has
//! no equivalent because it has no schema to migrate (`RedisStorage::migrate`
//! is a documented no-op); its legacy entries deserialize as unshed and stay
//! covered by the sweep's reason-prefix guard until retention removes them.
//!
//! Idempotent: `add_column` swallows the duplicate on SQLite and emits
//! `IF NOT EXISTS` on Postgres, the index uses `.if_not_exists()`, and the
//! backfill is a no-op once every matching row is already flagged.

use sea_query::{Alias, ColumnDef, Cond, ConditionalStatement, Expr, ExprTrait, Index, LikeExpr, Query};

use crate::storage::migrate::{add_column, ddl, dml, Backend, Migration, Stmt};

pub struct M0011DlqShed;

/// `LIKE` patterns for the dead-letter reason prefixes the scheduler reserves
/// for a shed. Pinned as literals: a migration records the values that were
/// correct when it was written, never the live constants. The `_` is escaped so
/// it matches a literal underscore rather than any character.
const SHED_REASON_PATTERNS: [&str; 2] = [r"codel:%", r"rate\_limit:%"];

/// Escape character for [`SHED_REASON_PATTERNS`]; `\` is not special in a SQL
/// pattern unless a query declares it, which `ESCAPE` does.
const LIKE_ESCAPE: char = '\\';

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

        let reason_is_shed = SHED_REASON_PATTERNS.iter().fold(Cond::any(), |cond, p| {
            cond.add(
                Expr::col(t("error")).like(LikeExpr::new(*p).escape(LIKE_ESCAPE)),
            )
        });
        // Guarded on `shed = false` so a re-run touches nothing.
        let backfill = Query::update()
            .table(t("dead_letter"))
            .value(t("shed"), true)
            .cond_where(
                Cond::all()
                    .add(Expr::col(t("shed")).eq(false))
                    .add(reason_is_shed),
            )
            .to_owned();

        vec![
            add_column(
                b,
                "dead_letter",
                col("shed").boolean().not_null().default(false),
            ),
            ddl(b, &retry_candidates),
            dml(b, &backfill),
        ]
    }
}
