//! Baseline workflow schema (`0001_workflow_initial`).
//!
//! Idempotent reproduction of the historical `run_workflow_migrations`: the
//! three workflow tables, their indexes, and the saga `ADD COLUMN` backfills —
//! built with sea-query, no hand-written SQL. `UNIQUE(name, version)` and
//! `UNIQUE(run_id, node_name)` are expressed as standalone unique indexes (the
//! portable form across SQLite and Postgres); they enforce the same constraint
//! and satisfy the upsert `ON CONFLICT` targets.

use sea_query::{Alias, ColumnDef, Index, Table};

use flexiq_core::storage::migrate::{add_column, ddl, Backend, Migration, Stmt};

pub struct M0001WorkflowInitial;

fn col(name: &str) -> ColumnDef {
    ColumnDef::new(Alias::new(name))
}

fn t(name: &str) -> Alias {
    Alias::new(name)
}

impl Migration for M0001WorkflowInitial {
    fn version(&self) -> &'static str {
        "0001_workflow_initial"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        let mut out = Vec::new();

        let definitions = Table::create()
            .table(t("workflow_definitions"))
            .if_not_exists()
            .col(col("id").text().not_null().primary_key())
            .col(col("name").text().not_null())
            .col(col("version").integer().not_null().default(1))
            .col(col("dag_data").blob().not_null())
            .col(col("step_metadata").text().not_null())
            .col(col("created_at").big_integer().not_null())
            .to_owned();
        out.push(ddl(b, &definitions));

        let runs = Table::create()
            .table(t("workflow_runs"))
            .if_not_exists()
            .col(col("id").text().not_null().primary_key())
            .col(col("definition_id").text().not_null())
            .col(col("params").text())
            .col(col("state").text().not_null().default("pending"))
            .col(col("started_at").big_integer())
            .col(col("completed_at").big_integer())
            .col(col("error").text())
            .col(col("parent_run_id").text())
            .col(col("parent_node_name").text())
            .col(col("created_at").big_integer().not_null())
            .to_owned();
        out.push(ddl(b, &runs));

        let nodes = Table::create()
            .table(t("workflow_nodes"))
            .if_not_exists()
            .col(col("id").text().not_null().primary_key())
            .col(col("run_id").text().not_null())
            .col(col("node_name").text().not_null())
            .col(col("job_id").text())
            .col(col("status").text().not_null().default("pending"))
            .col(col("result_hash").text())
            .col(col("fan_out_count").integer())
            .col(col("fan_in_data").text())
            .col(col("started_at").big_integer())
            .col(col("completed_at").big_integer())
            .col(col("error").text())
            .to_owned();
        out.push(ddl(b, &nodes));

        // Unique constraints as standalone unique indexes (portable form).
        out.push(ddl(
            b,
            &Index::create()
                .if_not_exists()
                .unique()
                .name("idx_wf_def_name_version")
                .table(t("workflow_definitions"))
                .col(t("name"))
                .col(t("version"))
                .to_owned(),
        ));
        out.push(ddl(
            b,
            &Index::create()
                .if_not_exists()
                .unique()
                .name("idx_wf_node_run_name")
                .table(t("workflow_nodes"))
                .col(t("run_id"))
                .col(t("node_name"))
                .to_owned(),
        ));

        // Lookup indexes.
        out.push(ddl(
            b,
            &Index::create()
                .if_not_exists()
                .name("idx_wf_def_name")
                .table(t("workflow_definitions"))
                .col(t("name"))
                .to_owned(),
        ));
        out.push(ddl(
            b,
            &Index::create()
                .if_not_exists()
                .name("idx_wf_run_def")
                .table(t("workflow_runs"))
                .col(t("definition_id"))
                .to_owned(),
        ));
        out.push(ddl(
            b,
            &Index::create()
                .if_not_exists()
                .name("idx_wf_run_state")
                .table(t("workflow_runs"))
                .col(t("state"))
                .to_owned(),
        ));
        out.push(ddl(
            b,
            &Index::create()
                .if_not_exists()
                .name("idx_wf_node_run")
                .table(t("workflow_nodes"))
                .col(t("run_id"))
                .to_owned(),
        ));
        out.push(ddl(
            b,
            &Index::create()
                .if_not_exists()
                .name("idx_wf_node_status")
                .table(t("workflow_nodes"))
                .col(t("run_id"))
                .col(t("status"))
                .to_owned(),
        ));

        // Saga columns — added in 0.13 alongside the saga feature.
        out.push(add_column(
            b,
            "workflow_nodes",
            col("compensation_job_id").text(),
        ));
        out.push(add_column(
            b,
            "workflow_nodes",
            col("compensation_started_at").big_integer(),
        ));
        out.push(add_column(
            b,
            "workflow_nodes",
            col("compensation_completed_at").big_integer(),
        ));
        out.push(add_column(
            b,
            "workflow_nodes",
            col("compensation_error").text(),
        ));

        out
    }
}
