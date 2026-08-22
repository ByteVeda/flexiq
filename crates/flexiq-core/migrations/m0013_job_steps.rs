//! Durable inline steps (`0013_job_steps`).
//!
//! `ctx.step.run` checkpoints inside one job: a committed step is replayed from
//! storage on the next attempt instead of re-executing. The rows follow the
//! `job_errors` shape — text `job_id`, an index, and deliberately **no foreign
//! key**, because a job leaves `jobs` for `archived_jobs` the moment it
//! completes and an FK would break exactly when the feature is working.
//!
//! **No `status` column and no `error` column.** A step whose closure raised is
//! never committed — that is what makes the retry re-run it — so a stored `run`
//! row is complete by construction, and a `sleep` row's completeness is
//! `now >= wake_at`. Every state is derivable from what is already here. A
//! `status` column would be a second source of truth some path has to remember
//! to advance, and a schema able to express a failed step would invite a reader
//! to treat one as a memo hit.
//!
//! Both unique indexes are load-bearing: `(job_id, seq)` makes a double commit
//! of the same position a database error rather than a race, and
//! `(job_id, step_key)` catches an explicit key reused at two positions.
//!
//! `namespace` is denormalised from the job so the scoped read and delete stay
//! single-table. `result_len` is stored beside the blob so the per-job total cap
//! is a `SUM` over integers rather than over blobs.
//!
//! Rows are deleted inside the job's terminal write, not swept by age — a step
//! memo has no value after the job ends, and under an encrypting codec those
//! blobs are ciphertext an operator has no reason to keep at rest. So there is
//! no retention entry and no cutoff for this table.
//!
//! Idempotent: `.if_not_exists()` on the table and on every index.

use sea_query::{Alias, ColumnDef, Index, Table};

use crate::storage::migrate::{ddl, Backend, Migration, Stmt};

pub struct M0013JobSteps;

fn col(name: &str) -> ColumnDef {
    ColumnDef::new(Alias::new(name))
}

fn t(name: &str) -> Alias {
    Alias::new(name)
}

impl Migration for M0013JobSteps {
    fn version(&self) -> &'static str {
        "0013_job_steps"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        let steps = Table::create()
            .table(t("job_steps"))
            .if_not_exists()
            .col(col("id").text().not_null().primary_key())
            .col(col("job_id").text().not_null())
            .col(col("namespace").text())
            .col(col("step_key").text().not_null())
            .col(col("seq").integer().not_null())
            .col(col("kind").text().not_null().default("run"))
            .col(col("result").binary())
            .col(col("result_len").integer().not_null().default(0))
            .col(col("wake_at").big_integer())
            .col(col("created_at").big_integer().not_null())
            .to_owned();

        // A double commit at the same position is a constraint violation, not a
        // race the reader has to notice.
        let by_job_seq = Index::create()
            .if_not_exists()
            .unique()
            .name("idx_job_steps_job_seq")
            .table(t("job_steps"))
            .col(t("job_id"))
            .col(t("seq"))
            .to_owned();

        // The same explicit key at two positions is the collision an unordered
        // loop's `key=` exists to reject.
        let by_job_key = Index::create()
            .if_not_exists()
            .unique()
            .name("idx_job_steps_job_key")
            .table(t("job_steps"))
            .col(t("job_id"))
            .col(t("step_key"))
            .to_owned();

        // The snapshot read and the terminal delete both scan by job alone.
        let by_job = Index::create()
            .if_not_exists()
            .name("idx_job_steps_job_id")
            .table(t("job_steps"))
            .col(t("job_id"))
            .to_owned();

        vec![
            ddl(b, &steps),
            ddl(b, &by_job_seq),
            ddl(b, &by_job_key),
            ddl(b, &by_job),
        ]
    }
}
