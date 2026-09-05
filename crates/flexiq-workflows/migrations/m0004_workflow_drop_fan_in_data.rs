//! Drop the never-written `fan_in_data` column (`0004_workflow_drop_fan_in_data`).
//!
//! `workflow_nodes.fan_in_data` has been in the schema since `m0001` and no
//! code path ever wrote it: collected fan-in results reach the fan-in job as
//! its *arguments*, so the payload travels with the job and never lands in a
//! node row. Every backend still named the column in its `SELECT` lists and
//! bound a literal `None` on insert, which is the cost this removes.
//!
//! This is the repository's first drop-column migration, and it is one-way in a
//! way an `ADD COLUMN` is not: a build whose `SELECT` still names the column
//! fails on *every* read of the table, not just on the rows it wrote. That is
//! why the same change bumps `CONTRACT_VERSION`, giving an operator a floor to
//! raise once the rollout is done.
//!
//! `m0001` is deliberately left alone — a baseline records the schema as it
//! was, so a fresh database creates the column and this migration drops it.
//! Idempotent all the same: `drop_column` emits `IF EXISTS` on Postgres and
//! swallows the missing-column error on SQLite.

use flexiq_core::storage::migrate::{drop_column, Backend, Migration, Stmt};

pub struct M0004WorkflowDropFanInData;

impl Migration for M0004WorkflowDropFanInData {
    fn version(&self) -> &'static str {
        "0004_workflow_drop_fan_in_data"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        vec![drop_column(b, "workflow_nodes", "fan_in_data")]
    }
}
