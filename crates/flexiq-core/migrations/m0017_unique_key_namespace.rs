//! Scope `unique_key` dedup by namespace (`0017_unique_key_namespace`).
//!
//! `idx_jobs_unique_key` was a partial unique index on `(unique_key)` alone —
//! the same key collided across every namespace sharing the database, so one
//! tenant's `enqueue_unique` could return another tenant's job instead of
//! creating its own (#773). Every other id-addressed `Storage` member scopes
//! by namespace; this was the one gap.
//!
//! Rebuilt as `(COALESCE(namespace, ''), unique_key)` rather than plain
//! `(namespace, unique_key)` — `namespace` is nullable, and both SQLite and
//! Postgres treat two NULLs as *distinct* inside a unique index, so the naive
//! version would silently stop deduping the default namespace, which is the
//! common case. `m0010_debounce` hit the identical NULL trap and sidestepped
//! it by leaving that index non-unique; that option isn't available here
//! because Postgres has no whole-database write lock serializing the
//! read-then-write the way SQLite's `BEGIN IMMEDIATE` does — the DB-enforced
//! constraint is what the existing `UniqueViolation`-then-retry logic in
//! `enqueue_unique_reporting` is already built around.
//!
//! `sea_query`'s `Index` builder has no expression-column support, so the
//! create side goes through the `raw_ddl` escape hatch — one literal, since
//! `CREATE UNIQUE INDEX IF NOT EXISTS … WHERE …` over a `COALESCE` column is
//! valid, identical SQL on both SQLite and Postgres. The drop is a plain
//! `sea_query` statement.
//!
//! Idempotent: `.if_exists()` on the drop, `IF NOT EXISTS` on the raw create.
//!
//! A `COALESCE(namespace, …)` index term isn't a bare column reference, so
//! (unlike `idx_jobs_unique_key`'s original, all-bare-column form) SQLite
//! resolves every term in the index — including the bare `unique_key` one —
//! eagerly at `CREATE INDEX` time rather than deferring. `unique_key` has
//! been part of the schema since `m0001`, so no real database can lack it,
//! but the `ADD COLUMN` below costs nothing on one that does and one line to
//! avoid depending on which SQLite index-validation path a caller's schema
//! happens to hit.

use sea_query::{Alias, ColumnDef, Index};

use crate::storage::migrate::{add_column, ddl, raw_ddl, Backend, Migration, Stmt};

pub struct M0017UniqueKeyNamespace;

fn t(name: &str) -> Alias {
    Alias::new(name)
}

fn col(name: &str) -> ColumnDef {
    ColumnDef::new(Alias::new(name))
}

/// Same partial predicate as `m0001`'s original index
/// (`JobStatus::Pending as i32 == 0`, `Running as i32 == 1`, pinned as
/// literals — a migration records the value that was correct when it was
/// written, never the live enum).
const CREATE_INDEX_SQL: &str = "CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_unique_key \
     ON jobs (COALESCE(namespace, ''), unique_key) \
     WHERE unique_key IS NOT NULL AND status IN (0, 1)";

impl Migration for M0017UniqueKeyNamespace {
    fn version(&self) -> &'static str {
        "0017_unique_key_namespace"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        vec![
            add_column(b, "jobs", col("unique_key").text()),
            ddl(
                b,
                &Index::drop()
                    .if_exists()
                    .name("idx_jobs_unique_key")
                    .table(t("jobs"))
                    .to_owned(),
            ),
            raw_ddl(CREATE_INDEX_SQL),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drops_and_recreates_the_index_over_the_coalesced_column() {
        let m = M0017UniqueKeyNamespace;
        let rendered = m
            .up(Backend::Sqlite)
            .iter()
            .map(|s| s.sql())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(rendered.contains("ADD COLUMN \"unique_key\""), "{rendered}");
        assert!(rendered.contains("DROP INDEX"), "{rendered}");
        assert!(rendered.contains("idx_jobs_unique_key"), "{rendered}");
        assert!(
            rendered.contains("COALESCE(namespace, '')"),
            "{rendered}"
        );
        assert!(rendered.contains("unique_key"), "{rendered}");
        assert!(
            rendered.contains("WHERE unique_key IS NOT NULL AND status IN (0, 1)"),
            "{rendered}"
        );
    }
}
