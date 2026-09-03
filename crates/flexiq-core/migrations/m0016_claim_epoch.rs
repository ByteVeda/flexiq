//! The epoch an execution claim was won under (`0016_claim_epoch`).
//!
//! A result is fenced on the claim's owner and the job's `retry_count`, and
//! between them those separate a reclaim from a reap. They do not separate two
//! dispatches produced by `requeue_stuck`, which returns a `Running` job to
//! `Pending` and deletes its claim without touching `retry_count` — so the
//! next dispatch carries the same owner at the same attempt, and the stalled
//! executor's late result authorizes. The epoch is what tells them apart: a
//! claim is never won twice under the same one.
//!
//! Nullable, and deliberately: a claim written before this migration has a
//! *missing* epoch rather than a wrong one, and the fence skips the comparison
//! for it — the same answer that row got before the column existed. Live claims
//! expire within a day on every backend, so the untyped window is short.
//!
//! Idempotent: `add_column` swallows the duplicate on SQLite and emits
//! `IF NOT EXISTS` on Postgres.

use sea_query::{Alias, ColumnDef};

use crate::storage::migrate::{add_column, Backend, Migration, Stmt};

pub struct M0016ClaimEpoch;

impl Migration for M0016ClaimEpoch {
    fn version(&self) -> &'static str {
        "0016_claim_epoch"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        vec![add_column(
            b,
            "execution_claims",
            ColumnDef::new(Alias::new("epoch")).big_integer(),
        )]
    }
}
