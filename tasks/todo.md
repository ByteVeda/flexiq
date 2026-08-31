# #759 — an executor's in-flight map keys overlapping dispatches together

## Problem
`Executor::in_flight` (`crates/flexiq-core/src/worker/remote.rs`) is a
`HashMap<String, InFlight>` keyed by job id alone. `dispatch_to` inserted
unconditionally and the result path removes by id, both correct only while one
job id is in flight on one connection at a time — which nothing enforced. The
reaper cannot tell a slow attempt from a dead one, so it reclaims a job an
executor is still running, and the poller may place the next attempt on that
same connection.

Two failures follow, neither needing a thread race:

1. A stale attempt's step commit resolves against the newer entry, so it is
   fenced on the *live* attempt and accepted — the one write the fence exists to
   refuse.
2. Whichever attempt reports first spends the shared exactly-once token; the
   other result is logged as "unknown job" and dropped, leaving the live attempt
   unreported until the reaper takes it.

## Design
The issue offers two shapes. The token-and-newest-wins one closes (2) but not
(1): "newest wins" keeps the very entry a stale commit is then fenced under.
Nothing on the wire carries an attempt — a `step_commit`, `success`, `cancelled`
or `slept` frame names a job and nothing else — so while two dispatches of one id
coexist on a connection, a frame is unattributable and the aliasing cannot be
resolved after the fact, only prevented.

So: prevent it, at both ends of the same invariant.

- `register_dispatch` makes registration a vacant-entry insert. An occupied entry
  gives the slot back and fails the job retryably rather than aliasing. This is
  where the invariant every reader depends on is established.
- `try_acquire` takes the `&Job` and skips an executor already running that id,
  so a peer takes the attempt, or it waits for the entry to be released. New
  `Placement::AlreadyRunning` keeps the fail-back reason honest.

No dispatch token is needed once the map holds one dispatch per id: "is this id
here" and "is this the dispatch I am reasoning about" become the same question.

## Tasks
- [x] `register_dispatch` — vacant-entry insert, `Err` carries the running attempt
- [x] `dispatch_to` — refuse an aliasing dispatch, release the slot, fail retryably
- [x] `Executor::is_running` + `Placement::AlreadyRunning` + `try_acquire(&job)`
- [x] `place` — a reason string that names the aliasing
- [x] Docs: the rule and its symptom, in the executor guide
- [x] Unit tests on `register_dispatch` (refusal, release, per-id scope)
- [x] Integration test: a stale attempt's commit is never fenced under the live one
- [x] Integration test: a superseded dispatch leaves the running attempt reportable
- [x] Integration test: the attempt is placed on the peer, not the executor running it

## Review
All three integration tests red-checked against the unfixed code — two by
`expect_result` timing out on a failure the aliasing never produced, one by the
attempt landing on the busy executor. The unit tests red-check against a mutation
that makes `register_dispatch` insert unconditionally.

The two halves are pinned separately, both checked: removing the placement guard
reds only the third integration test — the first two then fall through to
`dispatch_to`'s refusal and still pass — and the write-side refusal is what the
unit tests cover, red against an unconditional insert.

Verified: `cargo fmt --all --check`; `cargo clippy --all-targets --all-features
-D warnings`; `cargo check --workspace` on postgres, redis and native-async;
`cargo test --workspace` (32 suites, 0 failures); `pnpm --dir docs lint`.

Not in scope: cancelling the superseded attempt still running on that executor.
It holds a slot until it reports and its writes are refused by the fence — a
separate change.
