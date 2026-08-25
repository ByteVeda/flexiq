# #669 — `ctx.step.run` and `ctx.step.sleep` on the Python shell

Part of #663. Implements the shell line of
`tasks/specs/2026-08-22-durable-steps-design.md` §12:
§2.1 (names mandatory, positional) · §2.2 (mixed keyed/unkeyed) · §4.2 (the error
text) · §7.6 (`on_sleep` in contrib) · §7.7 (both swallow layers) · §9.2
(retryability, and that a step frame never carries an owner) · §9.4 (refuse
rather than degrade).

Core is done and untouched by this branch: `flexiq_core::step`
(#666/#667/#668) over `job_steps` (#665). This is the first shell, so the
decisions here are the ones #670 (node) and #671 (java) mirror.

## Surface

```python
from flexiq.context import current_job

@queue.task()
def checkout(order):
    charge = current_job.step.run(
        "charge",
        lambda: stripe.charge(order, idempotency_key=current_job.step.idempotency_key),
    )
    current_job.step.sleep("1h")
    current_job.step.run("receipt", lambda: send_receipt(charge))
```

- `current_job` **is** the Python task context — the same object `.log`,
  `.check_cancelled` and every middleware hook already take. No parameter
  injection: it would collide with `Inject[...]` resource kwargs and it is the
  one thing the three shells need not agree on (node passes `ctx`, java has
  `ctx.step()`, Python has `current_job`).
- The step body takes **no arguments**. The downstream key is read off the
  context (`current_job.step.idempotency_key`), matching node's
  `ctx.step.idempotencyKey` and java's `ctx.step().idempotencyKey()`. It is
  valid only inside a running step body; outside it raises.
- `arun` / `asleep` are the `a*` twins, per the SDK convention.
- The name is positional and required — an unnamed step is a `TypeError`, never
  inferred from the callable (§2.1).
- `key=` is the escape hatch for unordered loops (§2.2); the core decides
  keyed-by-key / unkeyed-by-position.

## Where a step may run (§9.4 — refuse, never degrade)

| Path | Steps | Why |
|---|---|---|
| in-process (native async / classic pool) | yes | the scheduler's own process holds storage and the claim owner |
| prefork child | yes | a child of an in-process worker holds real storage (`prefork/child.rs`), and gets the owner at spawn |
| attached executor (`flexiq executor`) | **refuses** | §9.1/§9.2's `job_steps` / `step_commit` / `step_ack` frames do not exist in core; there is no channel to commit on |
| backend without a step store | **refuses** | core already refuses in `StepSession::load` |
| `queue.test_mode()` | inline, documented | no job row to memoize against; the closure runs and a sleep is a no-op |

A refusal is a `StepUnavailableError` raised at the first `step.run`, ending the
attempt as a **retryable** failure: a heterogeneous fleet mid-rollout may place
the next attempt on a capable worker.

## The claim owner rides the spawn, not a frame

The fence is `(owner, attempt)` and `owner` must never be something the running
code asserts about itself (§1.4, §9.2).

- In-process: `py_queue/worker.rs` already resolves `worker_id` and calls
  `scheduler.set_claim_owner`. The same string is handed to the pools and
  reaches `flexiq.context._set_context`.
- Prefork: `PreforkPool::new` gains the owner, and `spawn_child` sets
  `FLEXIQ_CLAIM_OWNER` in the child's environment. **Not** a field on
  `SchedulerMessage::Job` — the same struct crosses the socket to an attached
  executor, and an owner an executor can fill in is an owner it can get wrong.
  `PyExecutor` constructs its pool without one, so its children see no variable
  and refuse. `env_remove` on that path, so an inherited value cannot leak in.
- `attempt` is the dispatched job's `retry_count`, carried on the frame that
  dispatched the work. `open_step_session` re-reads the job row and refuses when
  the two disagree — a stale child replaying is superseded, and the storage
  fence would refuse the write anyway.

## Both swallow layers (§7.7)

1. **Language-native.** `StepControlSignal(BaseException)` is the base of
   `StepSleepSignal` and of the fatal step errors, so a bare `except Exception`
   in the task body misses it, like `KeyboardInterrupt`.
2. **The latch.** `ctx.step` sets a flag on the active context when it raises a
   control signal. If the task body returns normally with the flag set,
   `run_lifecycle` fails the attempt with *"step control flow was swallowed by
   the task body"*. One place, because every path runs `run_lifecycle`.

## Retryability (§9.2)

The core classifies (`classify_step_failure`); the shell carries the answer on
the exception as `flexiq_should_retry`, and every path's retry decision reads
that attribute before consulting `retry_on` / `dont_retry_on`:

- divergence, cap violation, invalid step name, a superseded attempt →
  `should_retry = False` → DLQ. Deterministic: the code will not change between
  attempts.
- backend unavailable, pool exhaustion, timeout, no step channel →
  retryable.

## `on_sleep`, not `after` (§7.6)

`after(ctx, None, None)` reads as "the task returned None" to OTel, Prometheus
and Sentry. So `TaskMiddleware.on_sleep(ctx, wake_at)`, defaulting to a no-op,
and the invariant **every `before` is matched by exactly one of `after` /
`on_sleep`**. `run_lifecycle` owns the pairing, releases resources and proxies
(the attempt is over), emits `EventType.JOB_SLEEPING`, and logs a one-time
warning naming middleware that override `before` but not `on_sleep`. Contrib
(otel, sentry, prometheus) implements it.

---

## Work

### 1 — Expose the core session to Python
- `crates/flexiq-python/src/py_step.rs`: `#[pyclass] StepSession` over
  `flexiq_core::step::StepSession<StorageBackend>` — `begin_run`, `commit_run`,
  `sleep_for`, `sleep_until`, `run_key`, `idempotency_key`, `finish`; and
  `#[pyclass] StepDecision` holding the `PendingStep` so a caller cannot invent
  a position. `QueueError` → `flexiq.steps` exceptions carrying
  `flexiq_should_retry` from `classify_step_failure`.
- `PyQueue.open_step_session(job_id, namespace, owner, attempt, limits…)`:
  reads the job row, refuses a mismatched attempt, returns the session.
- `PyQueue.claim_owner` recorded when `run_worker` resolves the worker id.
- `_flexiq.pyi`.

### 2 — `current_job.step.run`
- `flexiq/steps/` — `errors.py`, `signals.py`, `context.py`, `session.py`,
  `__init__.py` barrel (one concern per file).
- Step blobs use the **queue** serializer + codec chain (`queue._serializer`),
  never the per-task one (§5.2) — that is how `Queue(codec=…)` encryption is
  inherited, and the test asserts on the raw stored row.
- `_ActiveContext` gains `namespace`, `claim_owner`, the lazily-opened session
  and the latch; `_set_context` gains the fields; `JobContext.step`.
- Owner plumbed into `AsyncWorkerPool`, `NativeAsyncPool`, `AsyncTaskExecutor`.

### 3 — `current_job.step.sleep`
- `StepSleepSignal` unwinds the body; `run_lifecycle` turns it into the sleep
  path; each dispatch path reports it as `JobResult::Slept`:
  blocking (`py_worker.rs` / `async_worker.rs` / `native_async`), native async
  (`PyResultSender.try_report_slept`), prefork (a `{"type":"slept"}` frame —
  the parent's reader already routes `ExecutorMessage::Slept` through
  `into_job_result`, so no Rust change there).
- Durations reuse `parse_duration_ms` (`"1h"`, `timedelta`, seconds).

### 4 — Prefork
- Owner via `FLEXIQ_CLAIM_OWNER` at spawn; the child opens its session against
  its own storage; `slept` result frame; refusal when detached or unowned.

### 5 — `on_sleep`
- `TaskMiddleware.on_sleep`; pairing + event + one-time warning in
  `run_lifecycle`; the `ResultOutcome::Slept` comment in `py_queue/worker.rs`
  updated to name where the hook actually fires.

### 6 — Contrib
- `otel.py`, `sentry.py`, `prometheus.py` implement `on_sleep` — a sleep is
  neither a success nor a failure on any of the three.

### 7 — Tests (`sdks/python/tests/core/`)
- memo hit after a forced retry (the closure runs once across two attempts)
- sleep/wake replay (the job goes `Pending` at the deadline, holds no worker
  slot, and the earlier step is a memo hit on wake)
- the first commit fixes the deadline — a replayed `sleep("1h")` does not push
  an hour further out
- idempotency-key stability across a retry, and its `{run_key}:{step_key}` shape
- divergence dead-letters, non-retryably
- a sleep costs no `retry_count`
- both swallow layers, and the `before`/`on_sleep` pairing
- the codec test asserts on the **raw** stored row
- refusal paths: no owner, detached
- `ruff check` + `mypy` over `flexiq/` and `tests/`

---

## Review

Built as planned, with four things the plan did not anticipate.

**The swallow latch is invisible for a sleep.** §7.7 says the runner fails an
attempt whose body caught a control signal and returned. It does — and for a
*sleep* the scheduler then drops that failure, because `sleep_job` has already
left the job `Pending` and unclaimed, which `handle_result`'s `(owner, attempt)`
fence calls `Superseded`. The job wakes, the sleep is a memo hit, and the body
finishes: one attempt wasted, nothing broken. The latch only bites on a
swallowed **divergence**, where the attempt still holds its claim and nothing
downstream would question the value it goes on to return. The test that claimed
to prove the latch was rewritten around a divergence, and a second test
documents the sleep case rather than pretending it fails.

**A test cannot read the database in-process while a worker runs.** Python's
`sqlite3` and the SQLite the extension links are two separate SQLite libraries
in one process; they do not share the WAL index. The first version of the codec
and pinned-deadline tests read `jobs` already rescheduled by the sleep
transaction and `job_steps` **empty** — no error, just nothing, and only under
pytest. Both now query through a subprocess, which is also 3× faster: the poll
loop's in-process connections had been fighting the writer for the file lock.

**Test mode had no queue.** `_set_queue_ref` was only called by `run_worker`, so
progress, logs and steps were all unreachable from a task under `test_mode()`.
Set and restored by `TestMode` now.

**Step limits are not exposed.** Three optional arguments on
`open_step_session` had no caller and tripped `clippy::too_many_arguments`;
§4.2's answer to an oversized result is to store it elsewhere and memoize the
handle, so a shell knob has nothing to do. `StepLimits::default()`.

Two pre-existing things cleaned up on the way: `WorkerPool` in `py_worker.rs`
had no constructor since the async pools replaced it, and all three pools
carried their own drifting copy of "cancelled or failed, and does it retry".

**Verified.** 1531 Python tests (20 new) on the default wheel and the whole step
suite again on a `native-async` one, prefork included · `cargo test --workspace`
· clippy clean on `--all-targets --all-features` · `cargo check` on default,
`postgres`, `redis` and `native-async`.

**Found, not fixed:** `docs/content/docs/shared/guides/reliability/retries.mdx:435`
calls `queue.add_middleware(...)`, which does not exist — middleware is
registered through `Queue(middleware=[…])` or `@queue.task(middleware=[…])`.

**Left open**, and belonging to the core rather than a shell: §9.1/§9.2's
`job_steps` snapshot frame and the `step_commit`/`step_ack` pair were never
built, so an attached executor still refuses. Docs are #672.
