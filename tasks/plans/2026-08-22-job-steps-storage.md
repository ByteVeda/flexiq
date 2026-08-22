# #665 — `job_steps` and its lifecycle (storage + the fence)

Reviewed against `tasks/specs/2026-08-22-durable-steps-design.md` §12: D1, D3, D6,
D10, D14, §1.4, §8.4, §10.

## Deviations from the §1.3 code block (both forced by §1.4 / §4.1)

- `record_step_result` / `sleep_job` take **`attempt: i32`** as well as `owner`.
  §1.3 was written before §1.4 settled on `(owner, attempt)`; §1.4 is the rule.
- `record_step_result` takes **`limits: &StepLimits`**. §4.1 makes the caps
  queue-configurable, so a fixed `StepLimits::default()` at the boundary would
  contradict it; storage clamps what it is handed to the hard ceilings so the
  check still holds against a shell that forgets (§4.3).

## Commits

1. **`feat(core): add step limits and the step records`**
   - `src/step.rs` — `StepLimits { max_step_bytes, max_total_bytes, max_steps }`,
     defaults 256 KiB / 4 MiB / 1000, `clamped()` to the 1 MiB ceiling.
     #666 fills in `StepKey` / `StepSequence` / `digest` / `idempotency_key`.
   - `storage/records.rs` — `StepKind{Run,Sleep}`, `JobStep`, `NewJobStep`,
     `StepCommit{Committed,AlreadyCommitted}`, `SleepOutcome{Slept,AlreadySleeping}`.
   - `error.rs` — `ClaimLost`, `StepDiverged`, `StepLimitExceeded`.

2. **`feat(storage): add the job_steps migration`**
   - `migrations/m0013_job_steps.rs` — table per §10.1, both unique indexes
     (`job_id,seq` and `job_id,step_key`), the lookup index, **no** `status`
     and **no** `error` column, no FK.
   - `storage/schema.rs` + `storage/models.rs` rows.

3. **`feat(storage): put the step store on the Storage trait`**
   - Five methods on `Storage`, every one defaulted to an **error** (D3):
     `supports_steps`, `get_job_steps`, `record_step_result`, `sleep_job`,
     `delete_job_steps`. `impl_storage!` forwarding + `delegate!` on
     `StorageBackend`.

4. **`feat(sqlite,postgres): implement the step store on Diesel`**
   - `diesel_common/steps.rs` — `impl_diesel_step_ops!`.
   - The §1.4 four-case fence resolved **inside** the write transaction:
     claim names owner + `Running` + attempt → proceed · claim absent + same →
     re-assert via insert-only claim, then proceed · anything else → `ClaimLost`.
   - Caps on encoded bytes, `seq == len(existing)`, byte-identical re-commit →
     `AlreadyCommitted`, `kind` part of the match.
   - `sleep_job` = one transaction: fence, upsert the sleep row, delete the
     claim, reschedule to the **stored** deadline (first commit pins it).

5. **`fix(storage): revoke the claim and drop steps in the terminal write`**
   - Diesel: `archive_job_row` (the one funnel for `complete`, `complete_batch`,
     `fail`, `cancel_job`, `mark_cancelled`, `cascade_cancel`, `move_to_dlq`,
     the expiry archive and the chunked mass-mutation paths) deletes the step
     rows **and** the execution claim in the caller's transaction.
   - `retry` moves into a write transaction and revokes the claim with the bump.
   - No terminal path calls `delete_job_steps`; `requeue_stuck` and the
     dead-owner reclaim delete nothing.

6. **`feat(redis): implement the step store`**
   - `redis_backend/steps.rs` — hash `{prefix}job_steps:{job_id}` with
     `<seq>` → JSON, `k:<step_key>` → seq, `__total`.
   - One Lua script per write (commit, sleep), never `HSETNX` + `MULTI`.
   - TTL `max(now, latest wake_at) + 7d` on every commit.
   - `push_archive_ops` adds the `DEL` + claim revoke to the same atomic pipe;
     `retry` revokes the claim in its script.

7. **`fix(scheduler): fence results on the dispatch record`** (§8.4)
   - `InFlight` records `(owner, attempt)` per job, populated **unconditionally**
     (not only under `max_in_flight`).
   - `handle_result` applies the same four-case resolution before any mutation
     and drops a superseded result with a warning.
   - `recover_orphaned_jobs` writes its dispatch record from the winning
     `reclaim_execution`.

8. **`test: cover the step store on every backend`**
   - Contract suite (`tests/rust/storage_tests.rs`) so the Postgres and Redis
     legs exercise it: commit/replay, `AlreadyCommitted`, cap refusal, purged
     claim re-asserts, previous-attempt write refused after `retry`, commit
     racing a terminal write, orphan left by no terminal path.
   - `sqlite/tests.rs` for the Diesel-only details.

## Out of scope (other issues in the epic)

`flexiq_core::step` key derivation and the divergence pre-check (#666) ·
`reschedule` gaining a namespace and the `Slept` outcome (#667) ·
`__origin_job_id` (#668) · `CONTRACT_VERSION` → 2 and the 2.0.0 (§11, release).

---

## Review

Landed as nine commits on `feat/job-steps-storage`, unpushed.

**Two things the plan did not anticipate, both found while implementing:**

- **`handle_results` needed the fence too.** §8.4 names `handle_result`, but the batched
  success path is the default drain path and bypassed it entirely. Both now go through
  one `authorize_finished` helper.
- **The claim window §1.4 warns about was live in `result_handler`.** It cleared the
  execution claim a dozen statements before the transition. Removed: `retry` and the
  archive each revoke inside their own transaction, which is what closes it.

**One addition beyond the plan:** `flexiq_core::classify_step_failure` — §12's "error
classification at the acknowledgement boundary, mapping tests included". Pure, so the
shells cannot disagree about which failures are retryable.

**One performance correction:** the first Diesel commit path loaded every committed row to
decide one commit, which is quadratic over a 1 000-step job. Replaced with `COUNT` + `SUM`
plus a keyed lookup, which the gapless-`seq` invariant makes exact.

### Verification

| | Result |
|---|---|
| `cargo test --workspace` (SQLite) | 450 pass |
| Redis contract suite (Redis Cloud) | pass, 789 s |
| Postgres step contract (Neon, narrow test) | 12/12 pass |
| Postgres full contract suite | blocked by a pre-existing environment red — `test_dequeue_batch_archives_expired_jobs` fails the same way on clean `master`, verified in a `master` worktree; it is Neon round-trip latency against the test's 1 s window |
| `cargo clippy --all-targets`, default / `postgres` / `redis` | clean |
| Python suite | 1510 pass, 14 skipped |

### Not in this branch

`CONTRACT_VERSION` → 2 and the repo-wide 2.0.0 (§11). `ResultOutcome` gained a
`Superseded` variant and `QueueError` three more — both are breaking, and the epic bundles
them into one major release rather than discovering the break at publish time.
