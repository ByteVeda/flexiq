# #667 — `step.sleep` by ending the attempt

Reviewed against `tasks/specs/2026-08-22-durable-steps-design.md` §12: D9, D11, D14, §7.

#665 built the storage half — `sleep_job` already commits the row, revokes the claim and
reschedules the job in one transaction. #666 built the decision half for `step.run`.
Nothing yet *decides* a sleep: the storage layer sleeps to whatever `seq` and `wake_at` a
caller invents, and a slept attempt has no way to report itself. This closes both, in the
core, so the three shells (#669–#671) drive one implementation.

## Scope calls

- **`on_sleep`, contrib middleware and the swallow latch stay out.** §12 lists them under
  #667 *and* under the shells' own line (§7.6, §7.7). They are language-native by
  definition — a `BaseException` subclass, an `Error` subclass, a `try/catch` latch — and
  there is no `ctx.step` in any shell to hang them off until #669–#671. What lands here is
  the contract they implement against: `ResultOutcome::Slept`, and `job.sleeping` in the
  taxonomy so a webhook subscription stays portable between SDKs.
- **The dashboard surface stays out; the decision does not.** §7.3 already answers #667's
  open question — the latest step row of a `Pending` job reads `sleeping until …`, derived
  from `now >= wake_at`, with no new column on `jobs`. Building the read is four dashboard
  surfaces plus `api-types.ts`, and §13 puts a step timeline out of scope. What this adds
  is the *rule* in one place, `JobStep::has_elapsed`, with the sleep memo check as its
  caller — so every later reader answers the question the same way instead of four times.
- **`CONTRACT_VERSION` → 2 stays out.** §11.1 belongs to the release, not to a branch:
  `MIN_CONTRACT_VERSION` at 2 makes the *default* floor 2, locking out every process that
  has not upgraded, and it is bundled with the repo-wide 2.0.0 (§11.2) that no sub-issue
  owns. The two breaking pieces this issue does own — the `Slept` variants with
  `#[non_exhaustive]`, and `reschedule`'s namespace — land here and wait for that bump.

## Commits

1. **`feat(storage): scope reschedule to a namespace`** (§7.2)
   - `Storage::reschedule` gains `namespace: Option<&str>` — the one id-addressed method
     #614 missed. It was safe while only the poller called it with a job it had just
     claimed; a sleep is issued by task code, the least trusted caller in the system.
   - Diesel filters like `retry` does; Redis resolves through `get_job_required_in`. A job
     in another namespace reports `JobNotFound`, the same answer an unknown id gets.
   - Both poller call sites pass `self.namespace`; contract test for the cross-namespace
     refusal.

2. **`feat(core): decide a sleep against the recorded sequence`** (D9, §7.3)
   - `StepSequence::begin_sleep` → `SleepDecision::{Elapsed, Sleep, Resume}`. A sleep row
     is a memo hit **only once `now >= wake_at`** — derived by the reader, so there is no
     status to move and nothing a crash can strand.
   - `Resume` is the recovery arm of §7.1: a committed sleep whose deadline has not
     arrived re-issues `sleep_job` at the **recorded** `seq`, so storage answers
     `AlreadySleeping` and the stored deadline stands. `Sleep` is new ground at the next
     free `seq`, and only it can be refused by the step-count cap — matching what
     `sleep_job` itself checks.
   - `resolve` now returns where the step landed (`StepMatch`) instead of a run-shaped
     decision, so `begin_run` and `begin_sleep` share one match and one divergence path.
     A `sleep` replaying onto a recorded `run` row diverges by `kind`, as §7.1's table
     requires — that rule already existed, it just had no second caller.
   - An unnamed sleep is `sleep#N` from the occurrence counter; a name is accepted and
     recommended, because the divergence messages are unreadable otherwise.

3. **`feat(core): end the attempt in a step sleep`** (§7.1)
   - `StepSession::{sleep_for, sleep_until}` → `StepSleep::{Elapsed, Sleeping}`. One call:
     decide, commit through `sleep_job`, and report the deadline the job was **actually**
     rescheduled to, which on a replay is the stored one and not the candidate.
   - No split `begin`/`commit` form. `run` has one because the closure lives in the shell;
     a sleep has no closure to cross the boundary.
   - `sleep_for` reads the clock once, at the call: a binding that recomputed `now + 1h`
     on each replay would push the deadline an hour further out every time the job crashed
     into it — a sleep that outlives the job, produced by the recovery path itself.

4. **`feat(core): Slept joins the result taxonomy`** (§7.5)
   - `JobResult::Slept { job_id, task_name, wake_at, wall_time_ns }` and
     `ResultOutcome::Slept { .., queue, .. }`, plus `#[non_exhaustive]` on both enums —
     §11.2 puts every break in one release, and adding the attribute later would be a
     second one.
   - `handle_result` routes a sleep **before** the fence. `sleep_job` already left the job
     `Pending` with no claim, so `authorize_attempt` would read a correctly-slept attempt
     as superseded and drop it. The write was fenced where it happened; re-fencing the
     acknowledgement of it is the wrong question.
   - Frees the in-flight slot and signals the wake, and touches nothing else: no
     `retry_count`, no retry-budget token, no breaker, no `job_errors`, no `task_metrics`.
     A `succeeded` flag would inflate one counter or the other, and nothing failed.
   - `ExecutorMessage::Slept` keeps the frame protocol total, so an attached executor can
     report a sleep the same way it reports a cancel.
   - Each binding crate gets an explicit no-op arm naming its own issue, not a bare
     wildcard: `Slept` is unreachable until that shell has a `ctx.step`, and a silent
     wildcard is how it would stay unreachable after.

5. **`feat(events): job.sleeping in the event taxonomy`** (§7.5)
   - The subscribable-event list is a cross-SDK contract, not any one surface's
     vocabulary, so the constant lands on all four at once. No emitter yet — the shells
     that emit it are #669–#671.

6. **`test(core): a sleep ends the attempt without costing it`**
   - The acceptance tests: a slept job releases its worker slot, comes back at `wake_at`,
     and replays its memoized steps instead of re-running them.
   - Replaying `sleep_for(1h)` three times still wakes at the original instant.
   - A sleeping job is not stale-reaped — `sleep_job` clears `started_at`, and this pins
     the property §7.1 depends on.
   - A killed attempt mid-sleep heals on the next one.
   - `ClaimLost` at a sleep emits no `JobResult` and changes no job state.
   - Retry count, retry budget, breaker and metrics are all untouched by a sleep.
   - Session-level sleep cases join the contract suite so Postgres and Redis run them too.

---

## Review

Landed in 7 commits. Two things the plan did not anticipate:

- **The fence would have eaten every sleep.** `handle_result` fences each result on
  `(owner, attempt)` before touching it — and `sleep_job` has already left the job
  `Pending` with no claim, which is precisely the shape `authorize_attempt` calls
  `Superseded`. Routed through it, a correctly slept job would report `Superseded` and the
  one outcome explaining where it went would be dropped with a warning. The sleep skips the
  fence and frees its own in-flight slot; the write was fenced where it happened, and
  re-fencing the acknowledgement of it asks a different question than the one that matters.
- **Sharing the naming path with `run` moved the occurrence spend.** Splitting `resolve`
  into "name it" and "match it" put the spend before the match, so a *diverged* step would
  have spent an occurrence — and the next call of that name would derive `charge#1` and
  report a second divergence against a step the code never wrote, caused entirely by the
  first. Collapsed back into one `resolve` that spends only after the match succeeds, with
  a test that pins it: the duplicate-key message naming `charge#0` is the proof the counter
  stood still.

**Verified.** 401 lib + 69 integration tests on SQLite; the full contract suite green
against Redis Cloud (70/70, 830s); Postgres and the post-refactor Redis proved with narrow
temporary `#[test]`s against the Neon direct endpoint and Redis Cloud — the full PG suite
still carries its two known environment-only reds. `cargo check` clean on default,
`postgres`, `redis` and `native-async`; clippy clean workspace-wide. Python (ruff, mypy,
`test_events.py`), Node (biome, tsc, `webhooks.test.ts`) and Java (`EventNameTest`) green.

The Node typecheck needed `pnpm build:native` first — `native/index.d.ts` is generated and
gitignored, and the local copy predated `registryFingerprint`. Not a regression from this
work, but it blocks the pre-commit hook until rebuilt.

**Left for the issues that own it:** `on_sleep` and the swallow latch in each shell
(#669–#671, which is also where `job.sleeping` gains an emitter), the dashboard's
"sleeping until …" read, the taxonomy tables in `docs/` (#672), and `CONTRACT_VERSION` → 2
with the repo-wide 2.0.0 (§11, the release).
