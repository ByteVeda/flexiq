//! Scope periodic tasks by namespace (`0018_periodic_task_namespace`).
//!
//! `periodic_tasks` was the last table in the job model keyed by `name` alone
//! (#918). On a database shared by more than one namespace that meant two
//! tenants registering the same task name overwrote one another's row, a
//! listing returned every tenant's schedules, and a delete or a pause reached a
//! name the caller did not own. Jobs have been scoped since `m0017`.
//!
//! Identity becomes `(namespace, name)`, which means the `name` PRIMARY KEY has
//! to go — and SQLite cannot drop one. So this rebuilds the table: create the
//! new shape beside it, copy the rows in as default-namespace rows, drop the
//! old, rename. A migration's statements and its ledger row run inside one
//! transaction and both backends have transactional DDL, so the rebuild is
//! atomic — there is no half-renamed state a later boot has to recover from.
//!
//! Uniqueness is a **unique index over expressions**, not `(namespace, name)`:
//! `namespace` is nullable and `None` is the default namespace, and both SQLite
//! and Postgres treat two NULLs as *distinct* inside a unique index, so the
//! obvious form would silently stop constraining the common case. This is the
//! same trap `m0017` documents, and the leading `namespace IS NULL`
//! discriminator is there for the same reason: `COALESCE` alone folds `None`
//! and `Some("")` together, and the rest of the codebase treats those as two
//! different namespaces.
//!
//! `sea_query`'s `Index` builder has no expression-column support, so the
//! create goes through `raw_ddl` — one literal, valid and identical on both
//! backends.
//!
//! Because the index is the only uniqueness constraint left, `register_periodic`
//! can no longer upsert through a Diesel conflict target (Diesel cannot name an
//! expression index), which is why both Diesel backends moved to one shared
//! UPDATE-then-INSERT in `diesel_common::periodic`.

use sea_query::{Alias, ColumnDef, Query, Table};

use crate::storage::migrate::{ddl, insert_select, raw_ddl, Backend, Migration, Stmt};

pub struct M0018PeriodicTaskNamespace;

/// The table the rows are carried through. Dropped by the rename at the end of
/// the same transaction, so it is never visible to anything but this migration.
const SCRATCH: &str = "periodic_tasks_ns";
const LIVE: &str = "periodic_tasks";

/// Every column `m0001` left on `periodic_tasks`, in the order the rebuild
/// carries them. `namespace` is deliberately absent: the copied rows take its
/// default, NULL, which is the default namespace.
const CARRIED: [&str; 10] = [
    "name",
    "task_name",
    "cron_expr",
    "args",
    "kwargs",
    "queue",
    "enabled",
    "last_run",
    "next_run",
    "timezone",
];

/// One row per `(namespace, name)`. See the module doc for why this is an
/// expression index rather than `UNIQUE (namespace, name)`.
const CREATE_INDEX_SQL: &str = "CREATE UNIQUE INDEX IF NOT EXISTS idx_periodic_tasks_identity \
     ON periodic_tasks ((namespace IS NULL), COALESCE(namespace, ''), name)";

fn t(name: &str) -> Alias {
    Alias::new(name)
}

fn col(name: &str) -> ColumnDef {
    ColumnDef::new(Alias::new(name))
}

impl Migration for M0018PeriodicTaskNamespace {
    fn version(&self) -> &'static str {
        "0018_periodic_task_namespace"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        let rebuilt = Table::create()
            .table(t(SCRATCH))
            .if_not_exists()
            .col(col("name").text().not_null())
            .col(col("task_name").text().not_null())
            .col(col("cron_expr").text().not_null())
            .col(col("args").blob())
            .col(col("kwargs").blob())
            .col(col("queue").text().not_null().default("default"))
            .col(col("enabled").boolean().not_null().default(true))
            .col(col("last_run").big_integer())
            .col(col("next_run").big_integer().not_null())
            .col(col("timezone").text())
            .col(col("namespace").text())
            .to_owned();

        let carry_rows = Query::select()
            .columns(CARRIED.map(Alias::new))
            .from(t(LIVE))
            .to_owned();
        let mut carry = Query::insert();
        carry
            .into_table(t(SCRATCH))
            .columns(CARRIED.map(Alias::new));
        // Only fails on a column-count mismatch between the two lists above,
        // which is one constant used twice.
        carry
            .select_from(carry_rows)
            .expect("the insert and the select name the same columns");

        vec![
            ddl(b, &rebuilt),
            insert_select(b, &carry.to_owned()),
            ddl(b, &Table::drop().table(t(LIVE)).if_exists().to_owned()),
            ddl(b, &Table::rename().table(t(SCRATCH), t(LIVE)).to_owned()),
            raw_ddl(CREATE_INDEX_SQL),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rebuilds_the_table_and_indexes_the_coalesced_namespace() {
        let rendered = M0018PeriodicTaskNamespace
            .up(Backend::Sqlite)
            .iter()
            .map(|s| s.sql())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(rendered.contains("CREATE TABLE IF NOT EXISTS"), "{rendered}");
        assert!(rendered.contains("\"namespace\""), "{rendered}");
        assert!(rendered.contains("INSERT INTO"), "{rendered}");
        assert!(rendered.contains("DROP TABLE"), "{rendered}");
        assert!(rendered.contains("RENAME TO"), "{rendered}");
        assert!(rendered.contains("(namespace IS NULL)"), "{rendered}");
        assert!(rendered.contains("COALESCE(namespace, '')"), "{rendered}");
    }

    /// The copy must not name `namespace` — the rows it carries predate the
    /// column, and naming it would need a literal the `SELECT` cannot supply.
    #[test]
    fn the_copy_leaves_the_namespace_at_its_default() {
        let copy = M0018PeriodicTaskNamespace.up(Backend::Sqlite)[1]
            .sql()
            .to_string();

        assert!(copy.starts_with("INSERT INTO"), "{copy}");
        assert!(!copy.contains("namespace"), "{copy}");
        assert!(copy.contains("\"timezone\""), "{copy}");
    }
}
