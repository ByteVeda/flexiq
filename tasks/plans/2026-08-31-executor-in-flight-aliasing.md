# #759 — an executor's in-flight map keys overlapping dispatches together

The scheduler-side twin of #757 (node) and the java round-2 finding in #744, one level
down: `Executor::in_flight` in `crates/flexiq-core/src/worker/remote.rs` is a
`HashMap<String, InFlight>` keyed by job id alone, `dispatch_to` inserts unconditionally,
and the result path removes by id. Worse here than in the shells, because this is the side
that resolves the step fence.

## Why the issue's primary shape is not the one built

The issue offers two: tag each dispatch with a token and keep the higher `retry_count`, or
refuse to place a job on an executor already running that id.

The first does not close **mode 1** (a stale attempt's step landing in the live attempt's
sequence). "Newest wins" *keeps* attempt 1, which is exactly the entry attempt 0's commit
then resolves against. It cannot be made to: a `step_commit` frame carries `job_id`, `seq`,
`step_key`, `kind`, `wake_at` — no attempt, no dispatch token — so while two dispatches of
one id coexist on one connection, a commit is **unattributable**. Same for `success`,
`cancelled` and `slept`; only `failure` echoes a `retry_count`. Tolerating the aliasing
therefore cannot be made correct without growing the wire; it can only be made less wrong.

So: remove the aliasing. Confirmed with the user before implementing.

## The fix

The map's documented assumption — "one job id is in flight on one connection at a time" —
becomes an enforced invariant, at both the placement and the write.

1. **`try_acquire` takes the `&Job`, not the task name**, and skips an executor already
   running that id. The poller then picks another executor, or the job waits on
   `capacity_changed` — and the notify that matters fires exactly when the stale dispatch's
   result removes its entry. A new `Placement::AlreadyRunning` keeps the fail-back reason
   honest: "every executor advertising it is busy" would be a lie.
2. **`dispatch_to` refuses to overwrite a live entry.** Registration becomes a vacant-entry
   insert under the same lock; an occupied entry gives the slot back, notifies, and fails
   the job back retryably rather than aliasing. This is the invariant's enforcement point —
   the placement guard is advisory, this is the write that would break it.

With that, no token is needed anywhere: `running()`, the result path's `remove`, and the
sleep ack's `get_mut` are correct by construction, because there is only ever one dispatch
of an id on a connection to be wrong about. `InFlight::attempt`'s doc says so.

Not in scope: cancelling the superseded attempt still running on that executor. It holds a
slot until it reports, and its writes are refused by the fence — a separate change.

## Tests — `crates/flexiq-core/tests/rust/remote_tests.rs`

3. `a_superseded_dispatch_never_relabels_the_running_attempts_fence` — the issue's mode 1,
   literally. Claim a job, dispatch attempt 0, then `retry()` + re-dequeue + re-claim so
   storage is at attempt 1, and send attempt 1 to the same executor. The still-running
   attempt 0 commits a step: the ack must be a refusal (`Superseded`) and `job_steps` must
   stay empty. Red today — the entry is relabelled attempt 1, the commit is fenced on 1 and
   **accepted**.
4. `a_superseded_dispatch_leaves_the_running_attempt_reportable` — the issue's mode 2. Two
   dispatches of one id at one executor produce two outcomes: a retryable failure for the
   refused one and the running attempt's own success. Red today: the second dispatch takes
   the entry, the first result spends it, and the second is dropped as "unknown job".
5. `a_job_already_running_on_one_executor_is_placed_on_another` — the placement half. Two
   executors advertise the task; the second dispatch must land on the peer, not on the
   executor already running the id. Red today (the executor with the most free slots wins,
   which is the one already running it).

## Verify

`cargo fmt --check` · `cargo clippy --all-targets --all-features -D warnings` ·
`cargo check --workspace` on default/postgres/redis/native-async · `cargo test --workspace`
(`-j2`). No shell surface changes, so no SDK rebuild.
