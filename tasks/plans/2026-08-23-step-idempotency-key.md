# #668 — the deterministic downstream idempotency key

Reviewed against `tasks/specs/2026-08-22-durable-steps-design.md`: D8, §6, §12.

`#666` made a step's result memoizable and `#667` made a sleep durable, but neither closes
the window this issue exists for. Between "the payment API returned 200" and "the step
row committed" there is an instant in which the process can die, and the replay has no
record that the call happened — so it makes it again. Nothing on this side of the
network can fix that; the only fix is a key the *downstream* service dedupes on, minted
the same way on every attempt.

## Scope calls

- **`step.idempotency_key` on the shells stays out** (#669–#671). What lands here is the
  minting rule and the one thing a shell cannot derive for itself: the run key, which
  outlives the job row it started on.
- **Nothing enforces the `__` metadata prefix.** `__dlq_retry_count` set the precedent
  and is unguarded too; adding validation would mean a check at enqueue in three shells
  for a foot-gun that is self-inflicted. Documented on the constant instead.
- **No site docs.** §12 gives them to #672. The recipe people arrive with — a
  Stripe-style API — lands as rustdoc on `idempotency_key`, where it renders for the
  published crate; the module itself stays private like every other `step` submodule.

## Commits

1. **`feat(core): mint the downstream idempotency key for a step`** (§6.1)
   - New `step/idempotency.rs`: `idempotency_key(run_key, step_key)` → `{run_key}:{step_key}`,
     and `run_key(&Job)` → `metadata["__origin_job_id"] ?? job.id`.
   - Derived from the run's identity and the step's position and from nothing else — no
     clock, no payload, no serializer, no codec. That is the contrast the rustdoc draws
     with the `idempotent=True` auto-key, which hashes the *serialized* payload and so
     does move when a codec is nondeterministic.
   - An unusable stamped origin — absent, unparseable, not a string, blank — falls back
     to `job.id`, never to a blank run key: a blank one would put every job in the
     deployment into one key space, deduping each other's charges away.

2. **`fix(storage): carry the origin job id through a DLQ retry`** (D8, §6.2)
   - `retry_dead` mints a **new** job id, so it is the one boundary where the job id and
     the run diverge — and it is the case that matters most, an operator retrying a
     dead-lettered charge three days later through the admin UI.
   - `stamp_origin_job_id` writes `__origin_job_id` into the retry metadata beside the
     `__dlq_retry_count` already there, *preserving* a usable value so a twice-retried
     job keeps the id its first attempt ran under. An unusable one is replaced rather
     than left, since `run_key` would otherwise fall back to the new job id.
   - One shared helper, two edit sites: the Diesel macro (SQLite + Postgres) and Redis.

3. **`feat(core): hand each step its idempotency key`**
   - `StepSession` resolves the run key once at `load` and exposes `run_key()` and
     `idempotency_key(step_key)`. `run`'s closure now receives the key, so the Rust path
     demonstrates the feature; the split `begin_run` / `commit_run` path a shell uses
     mints it from the pending step's key.
   - Tests for the three properties the feature rests on: stability across an ordinary
     retry, across a sleep/wake, and across a `retry_dead` — the last one in the contract
     suite, so it is checked on all three backends.

## Verification

- `cargo test --workspace` green; `cargo clippy --workspace --all-targets` clean on
  default, `--features postgres` and `--features redis`.
- Postgres and Redis contract runs are CI's: no hosted URL was available in this session,
  so `FLEXIQ_POSTGRES_TEST_URL` / `FLEXIQ_REDIS_TEST_URL` went unset and both suites
  skipped locally. The Postgres path is the same Diesel macro SQLite exercises; the Redis
  one is separate code and is only compile-checked here.

## Review

**CodeRabbit, 1 real finding.** `move_to_dlq` / `shed_to_dlq` let a caller replace the job's
metadata wholesale — `{"shed":"rate_limit"}`, `{"codel":true}`, `RETRY_BUDGET_EXHAUSTED`. That
took `__origin_job_id` with it, so a run already resurrected once, dead-lettered down one of
those paths and retried again, had the *intermediate* job id stamped and started sending
different downstream keys. `carry_origin_job_id` now merges the origin into a replacement that
is a JSON object, at both `dead_letter` sites.

**Round two, same helper.** The job's origin now *overwrites* one a replacement object carries
rather than deferring to it. `move_to_dlq` is a public `Storage` method, so that blob is
caller-supplied and its claim about a run it does not own must not win.

**Left open, deliberately:** a replacement that is *not* a JSON object still drops the origin.
`RETRY_BUDGET_EXHAUSTED` is the bare string `"retry_budget_exhausted"` and three SDK suites
assert on it exactly, so it cannot be given a shape here — and that path already discards the
whole of the job's metadata on the way back out. Closing it properly means taking the origin
off the metadata blob altogether, onto a `dead_letter` column no replacement can reach: a
migration on three backends plus the row structs and the shells' DLQ types. That reverses §6.2
of the design, so it belongs to the epic, not to a review round. Needs a follow-up issue.
