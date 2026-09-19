//! When a dispatch accepted out of band stops being waited for
//! (`0019_claim_settle_deadline`).
//!
//! A push target that cannot finish inside the platform's request deadline
//! answers `202 Accepted` and reports the outcome afterwards. Between those two
//! moments the scheduler holds nothing that says the job is *expected* to take
//! long — it looks exactly like a job running towards its timeout, and the
//! reaper would collect it on schedule.
//!
//! This column is that missing fact. **Non-null means the dispatch was accepted
//! and is awaiting a settle**, and the value is when patience runs out. One
//! column carries both because they are one fact: a dispatch is awaiting a
//! settle for exactly as long as something is still willing to wait for it.
//!
//! It is also the arbiter. Three things can settle an accepted dispatch and
//! none of them can see the others — a `Settle` here, a `Settle` on another
//! replica, and the deadline passing — so each must consume this row's marker
//! in the same statement that tests it. Exactly one wins.
//!
//! Nullable, and deliberately, like `m0016_claim_epoch`: a claim written before
//! this migration has a *missing* marker rather than a wrong one, which reads
//! as "not awaiting a settle" — the answer it would have given before the
//! column existed.
//!
//! Idempotent: `add_column` swallows the duplicate on SQLite and emits
//! `IF NOT EXISTS` on Postgres.

use sea_query::{Alias, ColumnDef};

use crate::storage::migrate::{add_column, Backend, Migration, Stmt};

pub struct M0019ClaimSettleDeadline;

impl Migration for M0019ClaimSettleDeadline {
    fn version(&self) -> &'static str {
        "0019_claim_settle_deadline"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        vec![add_column(
            b,
            "execution_claims",
            ColumnDef::new(Alias::new("settle_deadline_ms")).big_integer(),
        )]
    }
}
