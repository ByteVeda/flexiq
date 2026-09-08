# #890 — a swallowed `step.sleep` can dead-letter a correctly sleeping job

## Root cause (settled by reading the code, not by re-running the flake)

1. `step.sleep` commits first and raises second — `StepSession::sleep_at`
   (`session.rs:275`) runs `sleep_job` (step row + claim delete + reschedule,
   one transaction) and only then does the shell raise `StepSleepSignal`. So the
   issue's candidate fix #1 is already the behaviour and changes nothing.
2. A sleep is **not** a retry: `retry_count` stays at the same attempt number.
3. The body swallows the signal, returns, and the latch raises
   `StepSwallowedError` → the shell reports a `Failure` for attempt *n*.
4. Meanwhile the woken job is re-claimed. `Scheduler::track_in_flight` keys the
   dispatch record **by job id**, so the new claim's `(owner, attempt, epoch)`
   **overwrites** the slept attempt's record.
5. The late `Failure` calls `release_in_flight` and gets the *new* record back.
   `authorize_attempt` then compares the new claim against itself — job
   `Running`, `retry_count == attempt`, same owner, epoch matches — and returns
   `Authorized`. With `max_retries=0` the job dead-letters on the spot.
6. The running attempt's own success then finds the job `dead` and is dropped as
   superseded, logged as "from attempt 0" because `retry_count` never moved.
   That is exactly the log in the issue: a success and a supersede *and* a dead
   letter, all true at once.

The fence cannot fix this: after a sleep-wake the two attempts are the same
`(owner, attempt)` by design. The fix belongs where the knowledge is — the shell
knows its attempt already slept.

## The fix

**An attempt that committed a sleep reports the sleep, whatever the body did
next.** The latch stores the *signal* instead of a bool (first one wins, so a
sleep is never overwritten by a later signal):

- body swallows the sleep and returns → re-raise the sleep, log the swallow
- body swallows the sleep and then throws → report the sleep, log the swallow
- body swallows a divergence / cap / refusal → `StepSwallowedError`, unchanged:
  that attempt still holds its claim and its failure is the only defence

## Tasks

### Python
- [ ] `_active_context.py`: `step_control_raised: bool` → `step_control_signal`
- [ ] `steps/latch.py`: `latch(ctx, signal)` first-wins, add `latched_sleep()`
- [ ] `steps/context.py`: `_ControlScope.__exit__` passes the signal instance
- [ ] `steps/__init__.py`: export `latched_sleep`
- [ ] `context.py::_check_step_control`: a latched sleep re-raises, loudly
- [ ] `task_lifecycle.py`: the failure arm reports a latched sleep instead
- [ ] `steps/errors.py`: `StepSwallowedError` docstring — no longer the sleep
- [ ] tests: fix the wrong invariant in
      `test_swallowing_a_sleep_loses_the_attempt_but_not_the_job`, add the
      swallow-then-raise case, assert no dead letter and no recorded error

### Node
- [ ] `steps/latch.ts`: `latch(signal)`, `get sleep`, `check()` rethrows a sleep
- [ ] `steps/context.ts`: the three `latch.latch()` sites pass their signal
- [ ] `task-callback.ts`: the catch prefers the latched sleep
- [ ] tests: `test/worker/steps.test.ts`

### Java
- [ ] `steps/StepLatch.java`: hold the signal, `sleep()`, `check()` rethrows
- [ ] `steps/StepContext.java`: pass the signal at each latch site
- [ ] `worker/WorkerDispatchBridge.java`: the `catch (Throwable)` arm
- [ ] tests: `StepRefusalTest`, `StepsTest`

### Shared
- [ ] `docs/content/docs/shared/modules/steps.mdx`: the latch section and the
      `StepSwallowedError` row now describe the sleep case correctly
- [ ] `result_handler.rs`: the supersede log names the `(owner, attempt, epoch)`
      it compared, which is what the issue asks for to settle the next one
- [ ] verify: `cargo test -j2`, python pytest + ruff + mypy, node test/lint,
      `./gradlew build`
- [ ] commit per SDK, authored as kartikeya-27, no push

## Review

The mechanism above is settled by reading the code rather than by reproducing
the flake. Two facts pin it: `Scheduler::track_in_flight` keys its dispatch
record by job id, and a sleep does not move `retry_count`. Together they mean a
late failure from a slept attempt is authorized against the *woken* attempt's
own claim — same owner, same attempt number, and the epoch it compares to is the
new claim's, because the record was overwritten. `authorize_finished` then says
`Authorized` and `max_retries=0` dead-letters on the spot; the woken attempt's
success arrives afterwards, finds the job `dead`, and is dropped as superseded
"from attempt 0". That is every line of the reported log, in order.

Both new Python tests and both new Node tests were run against the unfixed
source and fail there; the divergence-swallow tests pass throughout, which is
the boundary that matters — a swallowed divergence still fails its attempt
permanently.

Verified:

- Rust — `cargo check --workspace`; `cargo test -j2 --workspace`, 40 test
  binaries ok, 0 failed.
- Python — `pytest tests/`, 1585 passed, 16 skipped; `ruff check`,
  `ruff format --check`, `mypy` all clean.
- Node — `pnpm typecheck`, `pnpm lint` (one pre-existing warning in
  `executorAttach.test.ts`, untouched), `pnpm test` 780 passed.
- Java — `./gradlew build` BUILD SUCCESSFUL, which covers spotless, checkstyle
  and the `-Xwerror` javadoc; `StepsTest` 12/12, `StepRefusalTest` 10/10.

Not done, and deliberately: nothing in the scheduler changed. The fence cannot
tell these two attempts apart — after a sleep-wake they *are* the same
`(owner, attempt)` by design — so the only place the knowledge exists is the
shell that took the sleep. The supersede log now names the `(owner, attempt,
epoch)` it compared, which is what the issue asked for to settle the next one.
