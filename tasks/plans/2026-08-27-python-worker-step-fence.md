# #733 — fence a Python step session on its own worker's claim

Follow-up to #669, found while building #670 (node, PR #734). Part of #663.
Implements the correction §9.2 already required and the node shell landed: the
owner half of the `(owner, attempt)` fence belongs to the **worker**, not to the
`Queue` handle it was started from.

Folded in on the same branch (also from the #670 review, which found the gap in
Python while fixing it in node): a step name or sleep duration the API rejects
must fail the attempt **permanently**, not burn the retry budget.

## The bug

`PyQueue` held one `claim_owner: Arc<Mutex<Option<String>>>`. `run_worker` wrote
its worker id into that slot and cleared it on exit; `open_step_session` read it
as the fence's owner. One `Queue` can drive several workers — `app.py:132`
documents `queue.run_worker()` "in another process/thread" — and the second
`run_worker` overwrote the first's id. From then on:

- a job claimed by worker **A** opened its session naming worker **B**;
- `record_step_result` refused the commit — the row is locked by A;
- the attempt reported `StepSupersededError`, which is non-retryable;
- `handle_result` fenced on `(A, attempt)`, and A *does* own the job, so the
  failure was applied rather than dropped, and the job dead-lettered.

Nothing was ever written under a wrong claim. But every step-using job on the
older worker failed instead of running, naming a superseded attempt rather than
the real cause. A cheaper second case: A stopping while B ran cleared the slot
outright, and B's next step refused with "this worker holds no execution claim".

## The shape of the fix

`PyWorkerSteps` (`crates/flexiq-python/src/py_worker_steps.rs`, exposed as
`_flexiq.WorkerSteps`) holds one worker's `storage`, `namespace` and `owner`, and
is the only place a `StepSession` is built. `run_worker` mints one per run and
hands it to whichever pool it built; each pool passes it to `_set_context`
alongside the job, so it reaches the task through the active context rather than
through the queue. The shell then supplies only a job id and an attempt —
`StepContext._open()` never names an owner, and could not.

The namespace comes off the worker too, for the same reason: a scheduler only
dispatches from the namespace it polls.

Prefork was already correct and stays so — `FLEXIQ_CLAIM_OWNER` is set per pool
on the spawn (`prefork/child.rs:96`). The child now resolves it once through
`PyQueue.inherited_worker_steps()` and passes the handle down its own
`_execute_job`. Under an attached executor there is no claim and therefore no
handle, which is what makes steps refuse there — the capability probe
(`hasattr(queue._inner, "open_step_session")`) is gone, and with it the chance of
an `AttributeError` escaping the detached stand-in.

Files: `py_worker_steps.rs` (new) · `py_queue/{mod,steps,worker}.rs` ·
`py_worker.rs` · `async_worker.rs` · `native_async/{pool,task_executor}.rs` ·
`lib.rs` · `_active_context.py` · `context.py` ·
`async_support/{context,executor}.py` · `steps/context.py` · `prefork/child.py` ·
`_flexiq.pyi`.

## Deterministic misuse is not retryable

`_begin` raised a bare `TypeError` for an empty name, and the two sleep parsers
raise `TypeError`/`ValueError` for a duration the grammar rejects. None of those
is a `StepControlSignal`, so `_ControlScope` never latched them and nothing
stamped `flexiq_should_retry` — the task's retry filters decided, and a fault
that is written in the code was re-run to the same end until the budget ran out.

`StepContext._refuse()` (and `_millis`, which wraps the parsers) raises a
non-retryable `StepError` instead — node's `refuse()` and java's `refuse()`
expressed through the latch Python already has. `parse_duration_ms` itself is
untouched: it is shared with debounce, and only the step path converts.

## Verification

- **Red first.** `test_each_worker_fences_on_its_own_claim` was run against the
  single-slot code: worker A (started first) died with
  `StepSupersededError('execution claim lost for job …')` while B succeeded.
- `tests/core/test_steps.py` — 26 passed (2 new, 3 rewritten).
- `tests/worker/test_prefork.py`, `tests/worker/test_native_async.py` — green on
  a wheel built with `native-async`, which is what CI ships; the coroutine path
  carries the handle through `AsyncTaskExecutor`.
- Full Python suite; `ruff check flexiq/ tests/`; `mypy flexiq/ --no-incremental`.
- `cargo check --workspace` on default / postgres / redis / native-async;
  `cargo clippy -p flexiq-python --all-targets --all-features`; `cargo fmt`;
  `cargo test --workspace`.

`Closes #733` has never fired on a squash merge for this epic (#666–#671, five
times). Close it by hand.
