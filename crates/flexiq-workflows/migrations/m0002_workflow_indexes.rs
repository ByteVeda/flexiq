//! Scaling index (`0002_workflow_indexes`).
//!
//! The `created_at` index on `workflow_runs` deferred from the Tier-0 scaling
//! work (formerly "S05"), for time-ordered / keyset run listing. Clean one-shot
//! DDL — runs exactly once per database after the recorded baseline.

use sea_query::{Alias, Index};

use flexiq_core::storage::migrate::{ddl, Backend, Migration, Stmt};

pub struct M0002WorkflowIndexes;

impl Migration for M0002WorkflowIndexes {
    fn version(&self) -> &'static str {
        "0002_workflow_indexes"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        vec![ddl(
            b,
            &Index::create()
                .if_not_exists()
                .name("idx_workflow_runs_created_at")
                .table(Alias::new("workflow_runs"))
                .col(Alias::new("created_at"))
                .to_owned(),
        )]
    }
}
