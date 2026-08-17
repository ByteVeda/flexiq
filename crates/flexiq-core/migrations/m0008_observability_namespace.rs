//! Namespace on logs and metrics (`0008_observability_namespace`).
//!
//! `jobs` and `dead_letter` have carried a namespace since the start, so every
//! listing over them can be scoped to one tenant. `task_logs` and
//! `task_metrics` could not: the column did not exist, so a namespaced
//! deployment saw its own jobs but every tenant's logs and metrics. This adds
//! the column to both, nullable so existing rows stay readable and keep the
//! pre-namespace meaning of "unscoped".
//!
//! Idempotent: `add_column` swallows the duplicate on SQLite and emits
//! `IF NOT EXISTS` on Postgres, and the indexes use `.if_not_exists()`.

use sea_query::{Alias, ColumnDef, Index};

use crate::storage::migrate::{add_column, ddl, Backend, Migration, Stmt};

pub struct M0008ObservabilityNamespace;

fn col(name: &str) -> ColumnDef {
    ColumnDef::new(Alias::new(name))
}

fn t(name: &str) -> Alias {
    Alias::new(name)
}

impl Migration for M0008ObservabilityNamespace {
    fn version(&self) -> &'static str {
        "0008_observability_namespace"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        // The scoped reads filter on namespace and then range-scan the
        // timestamp, so the composite leads with the namespace.
        let logs_by_namespace = Index::create()
            .if_not_exists()
            .name("idx_task_logs_namespace_logged_at")
            .table(t("task_logs"))
            .col(t("namespace"))
            .col(t("logged_at"))
            .to_owned();

        let metrics_by_namespace = Index::create()
            .if_not_exists()
            .name("idx_task_metrics_namespace_recorded_at")
            .table(t("task_metrics"))
            .col(t("namespace"))
            .col(t("recorded_at"))
            .to_owned();

        vec![
            add_column(b, "task_logs", col("namespace").text()),
            add_column(b, "task_metrics", col("namespace").text()),
            ddl(b, &logs_by_namespace),
            ddl(b, &metrics_by_namespace),
        ]
    }
}
