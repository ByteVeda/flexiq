//! Namespace on workflow runs (`0003_workflow_run_namespace`).
//!
//! `jobs` has carried a namespace since the start, so a scoped deployment sees
//! only its own jobs. `workflow_runs` could not: the column did not exist, so a
//! run id reached its run — and through it every node — from any scope. This
//! adds the column, nullable so existing rows stay readable and keep the
//! pre-namespace meaning of "unscoped". `workflow_nodes` needs none of its own;
//! a node is reachable only through its run.
//!
//! Idempotent: `add_column` swallows the duplicate on SQLite and emits
//! `IF NOT EXISTS` on Postgres, and the index uses `.if_not_exists()`.

use sea_query::{Alias, ColumnDef, Index};

use flexiq_core::storage::migrate::{add_column, ddl, Backend, Migration, Stmt};

pub struct M0003WorkflowRunNamespace;

fn col(name: &str) -> ColumnDef {
    ColumnDef::new(Alias::new(name))
}

fn t(name: &str) -> Alias {
    Alias::new(name)
}

impl Migration for M0003WorkflowRunNamespace {
    fn version(&self) -> &'static str {
        "0003_workflow_run_namespace"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        // The scoped listings filter on namespace and then order by
        // `created_at`, so the composite leads with the namespace.
        let runs_by_namespace = Index::create()
            .if_not_exists()
            .name("idx_workflow_runs_namespace_created_at")
            .table(t("workflow_runs"))
            .col(t("namespace"))
            .col(t("created_at"))
            .to_owned();

        vec![
            add_column(b, "workflow_runs", col("namespace").text()),
            ddl(b, &runs_by_namespace),
        ]
    }
}
