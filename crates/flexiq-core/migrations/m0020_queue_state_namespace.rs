//! Scope queue pauses by namespace (`0020_queue_state_namespace`).
//!
//! `queue_state` was keyed by `queue_name` alone, and the scheduler read it
//! unscoped: on a database shared by more than one namespace, pausing
//! `emails` in one tenant stopped `emails` in every tenant (#836).
//!
//! Identity becomes `(namespace, queue_name)`, which means the `queue_name`
//! PRIMARY KEY has to go — and SQLite cannot drop one. So this rebuilds the
//! table exactly as `m0018` rebuilt `periodic_tasks`: new shape beside it, rows
//! copied in as default-namespace rows, old dropped, new renamed, all inside
//! the migration's transaction.
//!
//! Uniqueness is the same **index over expressions** `m0018` documents —
//! `(namespace IS NULL), COALESCE(namespace, ''), queue_name` — because both
//! backends treat two NULLs as distinct in a unique index, and `COALESCE` alone
//! would fold `None` into `Some("")`. Diesel cannot target it on conflict, so
//! `pause_queue` is an UPDATE-else-INSERT in `diesel_common::queue_state`.

use sea_query::{Alias, ColumnDef, Query, Table};

use crate::storage::migrate::{ddl, insert_select, raw_ddl, Backend, Migration, Stmt};

pub struct M0020QueueStateNamespace;

/// The table the rows are carried through, renamed over the live one before
/// the transaction commits.
const SCRATCH: &str = "queue_state_ns";
const LIVE: &str = "queue_state";

/// Every column `m0001` created, in carry order. `namespace` is absent so the
/// copied rows take NULL, the default namespace.
const CARRIED: [&str; 3] = ["queue_name", "paused", "paused_at"];

/// One row per `(namespace, queue_name)`. See the module doc.
const CREATE_INDEX_SQL: &str = "CREATE UNIQUE INDEX IF NOT EXISTS idx_queue_state_identity \
     ON queue_state ((namespace IS NULL), COALESCE(namespace, ''), queue_name)";

fn t(name: &str) -> Alias {
    Alias::new(name)
}

fn col(name: &str) -> ColumnDef {
    ColumnDef::new(Alias::new(name))
}

impl Migration for M0020QueueStateNamespace {
    fn version(&self) -> &'static str {
        "0020_queue_state_namespace"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        let rebuilt = Table::create()
            .table(t(SCRATCH))
            .if_not_exists()
            .col(col("queue_name").text().not_null())
            .col(col("paused").boolean().not_null().default(false))
            .col(col("paused_at").big_integer())
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
        let rendered = M0020QueueStateNamespace
            .up(Backend::Sqlite)
            .iter()
            .map(|s| s.sql())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(rendered.contains("CREATE TABLE IF NOT EXISTS"), "{rendered}");
        assert!(rendered.contains("\"namespace\""), "{rendered}");
        assert!(rendered.contains("DROP TABLE"), "{rendered}");
        assert!(rendered.contains("RENAME TO"), "{rendered}");
        assert!(rendered.contains("(namespace IS NULL)"), "{rendered}");
        assert!(rendered.contains("COALESCE(namespace, '')"), "{rendered}");
    }

    /// The copy must not name `namespace`: the rows it carries predate it.
    #[test]
    fn the_copy_leaves_the_namespace_at_its_default() {
        let copy = M0020QueueStateNamespace.up(Backend::Sqlite)[1]
            .sql()
            .to_string();

        assert!(copy.starts_with("INSERT INTO"), "{copy}");
        assert!(!copy.contains("namespace"), "{copy}");
        assert!(copy.contains("\"paused_at\""), "{copy}");
    }
}
