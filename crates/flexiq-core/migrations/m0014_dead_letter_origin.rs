//! The run a dead-lettered job belongs to (`0014_dead_letter_origin`).
//!
//! A durable step hands the downstream service `{run_key}:{step_key}`, and
//! `run_key` is the id the run *began* under — so an operator retrying a
//! dead-lettered charge three days later re-sends the key the first attempt
//! sent instead of charging the customer again. `retry_dead` mints a new job
//! id, so it has to be told what the old one was.
//!
//! It used to be told in the metadata blob (`__origin_job_id`), and that blob
//! is not a safe carrier: `move_to_dlq`/`shed_to_dlq` take a `metadata`
//! argument that **replaces** the job's own wholesale. A merge closed the
//! replacements that are JSON objects, but not `RETRY_BUDGET_EXHAUSTED` — a
//! bare string three SDK suites match byte-for-byte, so it has no object to
//! merge into and cannot be given one. A run resurrected once, killed by budget
//! exhaustion and retried again lost its origin there and double-charged.
//!
//! A column no `metadata` argument can reach closes it. Written on **every**
//! dead-letter, not only when a replacement is in play: a conditionally-written
//! column is one a reader has to reason about, whereas this way NULL means
//! exactly one thing — a row older than this migration.
//!
//! **No backfill**, unlike `0011_dlq_shed`. `retry_dead` resolves
//! column → blob → `original_job_id`, so a pre-migration row resolves exactly
//! as well as it does today; extracting the old value in SQL would need
//! dialect-branched JSON (`json_extract` versus `->>`) to buy nothing.
//!
//! **No index.** The only reader is `retry_dead`, which already has the row by
//! primary key.
//!
//! Redis has no schema to migrate (`RedisStorage::migrate` is a documented
//! no-op); its entries gain a `#[serde(default)]` field, and ones written
//! before it read back as absent and fall through to the same blob fallback.
//!
//! Idempotent: `add_column` swallows the duplicate on SQLite and emits
//! `IF NOT EXISTS` on Postgres.

use sea_query::{Alias, ColumnDef};

use crate::storage::migrate::{add_column, Backend, Migration, Stmt};

pub struct M0014DeadLetterOrigin;

fn col(name: &str) -> ColumnDef {
    ColumnDef::new(Alias::new(name))
}

impl Migration for M0014DeadLetterOrigin {
    fn version(&self) -> &'static str {
        "0014_dead_letter_origin"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        // Nullable: rows written before this migration have no origin to record,
        // and NULL is the signal that sends the reader to the blob fallback.
        vec![add_column(
            b,
            "dead_letter",
            col("origin_job_id").text(),
        )]
    }
}
