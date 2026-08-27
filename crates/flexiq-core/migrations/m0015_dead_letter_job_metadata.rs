//! The job's own metadata, kept out of the caller's way (`0015_dead_letter_job_metadata`).
//!
//! `dead_letter.metadata` is two things at once: the job's metadata, carried so
//! `retry_dead` can rebuild the job faithfully, and the scheduler's annotation
//! for *why* this job died — `{"codel":true}`, `{"shed":"rate_limit"}`,
//! `retry_budget_exhausted`. The annotation is passed as `move_to_dlq`'s
//! `metadata` argument, which **replaces** the blob, so every marker path
//! silently threw the job's own metadata away: an operator retrying that entry
//! got a job stripped of the `tenant`/`user_id`/correlation keys it was
//! enqueued with.
//!
//! The annotation cannot move instead. Three SDK suites assert
//! `entry.metadata == "retry_budget_exhausted"` by equality, so that column's
//! observable value is fixed. So the job's metadata gets the new home, exactly
//! as the run's origin did in `0014_dead_letter_origin` — the same overloaded
//! column, split a second time.
//!
//! **Written only when a replacement is in play.** This is the one place the
//! project's "write it unconditionally so NULL means one thing" rule is worth
//! bending: unlike an origin id, a metadata blob is unbounded, and copying it
//! into a second column on the *common* path (no replacement, where `metadata`
//! already holds it verbatim) would double the DLQ's storage for nothing. One
//! reader rule covers both cases and the pre-migration rows at the same time:
//! `job_metadata` if present, else `metadata`.
//!
//! **No backfill and no index.** A pre-migration row has nothing to recover —
//! its replacement already overwrote the blob at write time — and the only
//! reader is `retry_dead`, which already has the row by primary key.
//!
//! Redis has no schema to migrate; its entries gain a `#[serde(default)]` field
//! and ones written before it read back absent, taking the same fallback.
//!
//! Idempotent: `add_column` swallows the duplicate on SQLite and emits
//! `IF NOT EXISTS` on Postgres.

use sea_query::{Alias, ColumnDef};

use crate::storage::migrate::{add_column, Backend, Migration, Stmt};

pub struct M0015DeadLetterJobMetadata;

fn col(name: &str) -> ColumnDef {
    ColumnDef::new(Alias::new(name))
}

impl Migration for M0015DeadLetterJobMetadata {
    fn version(&self) -> &'static str {
        "0015_dead_letter_job_metadata"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        // Nullable: NULL is the signal that `metadata` is itself the job's own.
        vec![add_column(b, "dead_letter", col("job_metadata").text())]
    }
}
