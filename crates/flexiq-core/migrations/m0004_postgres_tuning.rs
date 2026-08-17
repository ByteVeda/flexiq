//! Postgres storage tuning (`0004_postgres_tuning`).
//!
//! Two per-table storage-parameter overrides that `sea-query` cannot express
//! (`ALTER TABLE … SET (…)`), so they go through the `raw_ddl` escape hatch:
//!
//! - **fillfactor** leaves in-page room for HOT updates. `jobs` churns through
//!   status/progress/retry updates and `workers` rewrites its heartbeat every
//!   few seconds — both benefit from not having to migrate a row to a new page
//!   on every update. It only affects *future* writes; existing pages turn over
//!   as they are rewritten (we do not `VACUUM FULL` from a migration).
//! - **autovacuum** is made aggressive on the tables the retention purges churn.
//!   The 20% default means a 10M-row table waits for 2M dead tuples before it is
//!   vacuumed; the bulk deletes create dead tuples in exactly these tables. The
//!   `threshold = 1000` is not optional — `scale_factor` alone still adds the
//!   default `threshold = 50`, which fires constantly on a small table.
//!
//! Postgres-only: `up()` returns no statements on SQLite (which has neither
//! knob), so the migration still records as applied there but changes nothing.
//! `ALTER TABLE … SET` is catalog-only — instant, no table rewrite, only a brief
//! `ACCESS EXCLUSIVE` lock. These are per-table overrides that take precedence
//! over any an operator has tuned by hand.

use crate::storage::migrate::{raw_ddl, Backend, Migration, Stmt};

pub struct M0004PostgresTuning;

/// `(table, fillfactor)`. `jobs` carries a payload BLOB so rows are large and
/// few fit per page — 85 wastes less than a lower value would. `workers` is a
/// few hundred rows updated every heartbeat, so reserving space is pure upside.
const FILLFACTOR: &[(&str, u32)] = &[("jobs", 85), ("workers", 70)];

/// Tables the retention purges delete from in bulk, so their dead tuples must be
/// reclaimed promptly rather than at the 20% default.
const AUTOVACUUM_TABLES: &[&str] = &[
    "jobs",
    "archived_jobs",
    "dead_letter",
    "task_logs",
    "task_metrics",
    "job_errors",
];

const AUTOVACUUM_PARAMS: &str = "autovacuum_vacuum_scale_factor = 0.02, \
     autovacuum_vacuum_threshold = 1000, \
     autovacuum_analyze_scale_factor = 0.02";

impl Migration for M0004PostgresTuning {
    fn version(&self) -> &'static str {
        "0004_postgres_tuning"
    }

    fn up(&self, backend: Backend) -> Vec<Stmt> {
        // SQLite has no per-table storage parameters; record as applied, do
        // nothing. Bare identifiers are correct — `conn()` sets `search_path`.
        if backend != Backend::Postgres {
            return Vec::new();
        }

        let mut stmts = Vec::with_capacity(FILLFACTOR.len() + AUTOVACUUM_TABLES.len());
        for (table, factor) in FILLFACTOR {
            stmts.push(raw_ddl(format!("ALTER TABLE {table} SET (fillfactor = {factor})")));
        }
        for table in AUTOVACUUM_TABLES {
            stmts.push(raw_ddl(format!("ALTER TABLE {table} SET ({AUTOVACUUM_PARAMS})")));
        }
        stmts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The Postgres arm renders through `raw_ddl` (a code literal, not a dialect
    /// builder), so — unlike every other migration — it is testable without the
    /// `postgres` feature and needs no database.
    #[test]
    fn tunes_postgres_and_is_a_noop_on_sqlite() {
        let m = M0004PostgresTuning;

        // SQLite has no storage parameters: nothing to run, yet still recorded.
        assert!(m.up(Backend::Sqlite).is_empty());

        let pg = m
            .up(Backend::Postgres)
            .iter()
            .map(|s| s.sql())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(pg.contains("ALTER TABLE jobs SET (fillfactor = 85)"), "{pg}");
        assert!(
            pg.contains("ALTER TABLE workers SET (fillfactor = 70)"),
            "{pg}"
        );
        // `threshold = 1000` must ride alongside `scale_factor` — scale alone
        // keeps the default threshold of 50 and fires constantly on a small table.
        assert!(pg.contains("autovacuum_vacuum_scale_factor = 0.02"), "{pg}");
        assert!(pg.contains("autovacuum_vacuum_threshold = 1000"), "{pg}");
        assert!(pg.contains("ALTER TABLE task_logs SET ("), "{pg}");
    }
}
