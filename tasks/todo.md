# A settle callback for push work that outlives the request deadline (#845)

Design: `tasks/specs/2026-09-19-push-settle-callback-design.md`
Plan: `tasks/plans/2026-09-19-push-settle-callback.md`

Push dispatch settles a job on the connection that started it, so a job is
capped at the platform's request deadline — 60 minutes on Cloud Run, 15 on
Lambda. A `202` becomes a hand-off: the request ends, the target keeps working,
and it reports later through a `Settle` RPC on the executor door.

**The thing that must be right** is the fence. A `Settle` arriving after its
lease expired and the job was retried elsewhere must be refused, not applied.
Two claimants can consume an accepted dispatch's marker — a `Settle` reaching
the replica that dispatched, and that dispatch being given up (its deadline
passing, a cancel, or a shutdown) — and each must consume the marker in the
statement that tests it. Exactly one wins; the loser emits nothing.

A `Settle` that reaches a *different* replica is **not** a third claimant: it
is refused as `NotHere` and consumes nothing, because the waiting attempt with
the permit and the result channel lives in the process that dispatched.

Two decisions carry the rest:

- **The 202 wait lives inside `run_one`.** The attempt task keeps its semaphore
  permit and swaps what it awaits. So "exactly one `JobResult` per job, never
  zero, never two" survives verbatim, and `FLEXIQ_PUSH_TARGET_CAPACITY` keeps
  one meaning in both modes.
- **One nullable column** — `execution_claims.settle_deadline_ms` — is both the
  extension mechanism and the "accepted, never settled" marker, because those
  are one fact.

## Tasks

- [x] 1. Proto: four RPCs carrying the existing frames + the contract's `202` row
- [x] 2. Storage: migration `m0019`, the records, `await_settle`/`claim_settle`/
      `supports_settle`, `lease_authorizes` beside `epochs_agree`
- [x] 3. Storage: SQLite, Postgres and Redis impls + the `delegate!` block
      (the claim purge preserving a live marker landed here, not in 4 — it is
      part of the marker's semantics on each backend, and its test is here)
- [x] 4. Reaper honours the marker and flags it; the scheduler's message
- [x] 5. Dispatcher accepts a 202 and waits
- [x] 6+7+8. The settle-only door, `FLEXIQ_PUSH_TARGET_SETTLE`, the chart and
      the progress/task-log RPCs — one commit, because none of the three
      compiles without the others: the door needs the config to be reachable,
      and the handlers need the target methods to exist
- [x] 9. Tests, including both mutation checks
- [x] 10. Docs

## Found on the way in

`purge_execution_claims` drops every claim older than a hard-coded hour
(`scheduler/maintenance.rs:104`). With the row gone the epoch is absent,
`epochs_agree` agrees with everything, and a job running past an hour has no
epoch fence at all. Pre-existing; #845 makes it the normal case. Closed here
for marked rows only — the general case (a long *attached* job) needs a join
the Diesel backends do not have, and is filed rather than half-done.

## Review

Ten tasks, nine commits (6-8 merged: see above). All three feature combos
compile; SQLite and Redis storage suites green; Postgres green apart from
`test_count_expired_rows_matches_seeded_rows`, **confirmed pre-existing** by
re-running the same assertion on a stashed tree.

Three things worth carrying forward:

- **The wait lives inside `run_one`.** Keeping the attempt task alive and
  swapping what it awaits meant the "exactly one `JobResult` per job" invariant
  needed no exception, and capacity kept one meaning across both modes.
- **`claim_settle` must clear the marker, not delete the claim.** The first
  draft deleted the row — which takes the epoch with it, and the fence that
  runs moments later when the result is applied then compares against an
  absence, which agrees with everything. Caught by a storage test.
- **A parked accepted dispatch is a shutdown hazard.** An attempt waiting on an
  hour-long deadline made the first test run hang: the abandon signal is the
  only thing that reaches it. `a_shutdown_releases_an_accepted_dispatch` pins
  that on a short drain budget so a regression is a failure, not a hang.

Known limits, all stated in the contract and docs rather than left to be found:
a settle must reach the replica that dispatched; cancel semantics are still
#846's; the claim-purge fence hole is closed only for marked rows.
