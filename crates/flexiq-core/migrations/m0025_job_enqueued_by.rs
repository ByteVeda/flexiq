//! Who submitted the job, on the job itself (`0025_job_enqueued_by`, #992).
//!
//! `enqueued_by` is the public id of the token that authorised the enqueue —
//! the same bare hex id `audit_log.token_id` holds, so the two join directly.
//! The server stamps it from the authenticated principal; no request field,
//! SDK option or `metadata` key can set it. NULL means no token door was
//! involved (an in-process SDK enqueue, a trigger, the scheduler).
//!
//! Mirrored onto `archived_jobs` and `dead_letter` because the column's point
//! is to outlive the audit trail's retention window: a job that finished or
//! died is exactly the one that gets asked about later, and `retry_dead` keeps
//! the original submitter on the replacement job.
//!
//! **No backfill and no index.** Pre-migration rows have no token to recover,
//! and "which jobs did token X submit" is answered by `audit_log`'s index.
//!
//! Redis has no schema to migrate; `Job` and its dead-letter entry gain a
//! `#[serde(default)]` field, so documents written before it read back absent.
//!
//! Idempotent: `add_column` swallows the duplicate on SQLite and emits
//! `IF NOT EXISTS` on Postgres.

use sea_query::{Alias, ColumnDef};

use crate::storage::migrate::{add_column, Backend, Migration, Stmt};

pub struct M0025JobEnqueuedBy;

fn enqueued_by() -> ColumnDef {
    ColumnDef::new(Alias::new("enqueued_by"))
}

impl Migration for M0025JobEnqueuedBy {
    fn version(&self) -> &'static str {
        "0025_job_enqueued_by"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        vec![
            add_column(b, "jobs", enqueued_by().text()),
            add_column(b, "archived_jobs", enqueued_by().text()),
            add_column(b, "dead_letter", enqueued_by().text()),
        ]
    }
}
