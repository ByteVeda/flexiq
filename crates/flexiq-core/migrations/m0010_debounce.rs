//! Debounce key on jobs (`0010_debounce`).
//!
//! Debounce collapses a burst of enqueues into one run by sliding a pending
//! job's `scheduled_at` forward. It needs a key to collide on, and that key
//! gets its own column rather than a third meaning on `unique_key` — which
//! already carries idempotency auto-keys and the pub/sub `key::sub_name` salt.
//!
//! Two decisions worth recording:
//!
//! **The index is deliberately not unique.** `jobs.namespace` is nullable and
//! both SQLite and Postgres treat NULLs as *distinct* inside a unique index, so
//! `UNIQUE (namespace, debounce_key)` would constrain nothing in the default
//! namespace — the common case. This is a lookup index; the "one pending job
//! per key" invariant belongs to the write transaction, which is also why
//! `INSERT … ON CONFLICT` is not an option for the debounced enqueue.
//!
//! **The column is not mirrored on `dead_letter` / `archived_jobs`.** A debounce
//! window only ever collides with a pending row in `jobs`; once a job goes
//! terminal it has left the window and the next burst opens a fresh one, so no
//! read path on those tables would consult the key. DLQ requeue already drops
//! `unique_key` for the same reason — an operator retry must not be swallowed
//! by a coalescing rule.
//!
//! Idempotent: `add_column` swallows the duplicate on SQLite and emits
//! `IF NOT EXISTS` on Postgres, and the index uses `.if_not_exists()`.

use sea_query::{Alias, ColumnDef, ConditionalStatement, Expr, ExprTrait, Index};

use crate::storage::migrate::{add_column, ddl, Backend, Migration, Stmt};

pub struct M0010Debounce;

/// `JobStatus::Pending`, pinned as a literal: a migration records the value
/// that was correct when it was written, never the live enum.
const STATUS_PENDING: i32 = 0;

fn col(name: &str) -> ColumnDef {
    ColumnDef::new(Alias::new(name))
}

fn t(name: &str) -> Alias {
    Alias::new(name)
}

impl Migration for M0010Debounce {
    fn version(&self) -> &'static str {
        "0010_debounce"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        // A debounce write looks up the one live row for a key, so the index
        // leads with the tenant scope and is restricted to the only rows that
        // can match: pending, still-unclaimed jobs that carry a key at all.
        let by_debounce_key = Index::create()
            .if_not_exists()
            .name("idx_jobs_debounce_key")
            .table(t("jobs"))
            .col(t("namespace"))
            .col(t("debounce_key"))
            .and_where(
                Expr::col(t("debounce_key"))
                    .is_not_null()
                    .and(Expr::col(t("status")).eq(STATUS_PENDING)),
            )
            .to_owned();

        vec![
            add_column(b, "jobs", col("debounce_key").text()),
            ddl(b, &by_debounce_key),
        ]
    }
}
