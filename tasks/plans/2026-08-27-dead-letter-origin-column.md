# Carry a run's origin on a `dead_letter` column, not in metadata (#728)

Follow-up from #668 (PR #727). Part of #663.

## The gap

`run_key(job)` is `job.metadata["__origin_job_id"] ?? job.id`, and `retry_dead`
stamps that key so a resurrected run keeps minting the downstream idempotency
keys it has always minted. The stamp rides in the metadata blob, and
`move_to_dlq` / `shed_to_dlq` let a caller **replace** that blob wholesale.

#727 closed the object-shaped replacements by merging the origin back in
(`carry_origin_job_id`). It cannot close a replacement that is not a JSON
object, and there is exactly one: `RETRY_BUDGET_EXHAUSTED`, the bare string
`"retry_budget_exhausted"`, which three SDK suites match byte-for-byte and so
cannot be given a shape.

So: `job-1` charges and dies → retried as `job-2` stamped `job-1` → `job-2`
exhausts its retry budget, dead-lettering with the bare marker → the origin is
gone → the next `retry_dead` stamps `job-2`, and the operator's retry charges
the customer a second time.

## The fix

Take the origin off the blob and give it a column no `metadata` argument can
reach.

## Decisions

| # | Decision | Rationale |
|---|---|---|
| D1 | A nullable `dead_letter.origin_job_id` column, written on **every** dead-letter — not only when a replacement is in play. | A column written conditionally is a column a reader has to reason about. Written always, "NULL" means exactly one thing: a row older than the migration. |
| D2 | No backfill. `retry_dead` resolves **column → blob → `original_job_id`**. | The blob fallback resolves a pre-migration row exactly as well as today's code does, which is the acceptance criterion. A backfill would need dialect-branched JSON extraction (`json_extract` vs `->>`) to buy nothing. |
| D3 | `carry_origin_job_id` is **deleted**, and the DLQ blob goes back to `metadata.or(job.metadata)`. | The column now covers every replacement shape, object or not. Keeping the merge would leave a second, weaker source of truth that can disagree with the column. |
| D4 | The origin is surfaced as `DeadJob.origin_job_id`, not left as a blob key. | A typed field beats a reserved `__` key for anyone who wants to see which run a dead entry belongs to. The shells' DLQ types are left alone — the issue says they need not carry it. |
| D5 | `run_key(&Job)` is unchanged: a **live** job still reads its origin off metadata. | `retry_dead` writes the resolved origin into the new job's metadata, so the live-job read is still correct. The column is the dead-letter carrier, not a second key derivation. |

## Steps

1. `migrations/m0014_dead_letter_origin.rs` — `add_column` a nullable
   `origin_job_id TEXT`. No index: it is read only by `retry_dead`, already
   addressing the row by primary key.
2. `storage/schema.rs` — append the column to the `dead_letter` `table!`.
3. `storage/models.rs` — `DeadLetterRow`, `NarrowDeadLetterRow`,
   `NewDeadLetterRow`.
4. `storage/mod.rs` — `DeadJob.origin_job_id` plus both conversions.
5. `step/idempotency.rs` — `stamp_origin_job_id` takes the column and owns the
   whole precedence chain; `carry_origin_job_id` and its merge are removed.
6. `diesel_common/dead_letter.rs` + `redis_backend/dead_letter.rs` — write
   `run_key(job)` into the column/field, read it back in `retry_dead`. Redis has
   no schema to migrate, so `DeadJobEntry` gains a `#[serde(default)]` field and
   entries written before it read back as `None`.

## Tests

- `step/idempotency.rs` — unit coverage of the precedence chain, including a
  column that is present but blank.
- `storage/migrate.rs` — the render test for `0014`, both dialects.
- `storage/sqlite/tests.rs` — a row written before the column (NULL) whose blob
  carries the origin still resolves through `retry_dead`.
- `tests/rust/storage_tests.rs` — the four-step sequence from the issue, with
  the bare-string `RETRY_BUDGET_EXHAUSTED` marker, so it is checked on SQLite,
  Postgres and Redis.

## Verification

`cargo test --workspace`, `cargo clippy --workspace --all-targets` on default,
`--features postgres` and `--features redis`.
