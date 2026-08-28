# #742 — python: durable steps on an attached executor

Follows #736 (core: `job_steps` + `step_commit`/`step_ack`). Sibling of #743 (node),
#744 (java).

## The thing the issue's file list misses

Python's attached executor is **prefork**. `PyExecutor` builds a `PreforkPool`, so
the task body — and therefore the `StepSession` — runs in a *child process* that
holds neither storage nor the socket. `ExecutorSteps` lives in the parent.

So this is not "hand the job context the handle". It is one more hop:

```
scheduler  ──job_steps──▶ parent (ExecutorSteps)  ──job_steps──▶ child (StepSession)
           ◀─step_commit─                         ◀─step_commit─
           ──step_ack───▶                         ──step_ack───▶
```

The hop is free of new protocol: **the prefork child already is an executor and the
pool already is its scheduler** — same `FrameReader`/`FrameWriter`, same three
frames. The parent becomes a *proxy* for them, not a second session. One session,
in the child, where the closure is.

## Core additions (2, both on `ExecutorSteps`)

A proxy cannot be built from today's surface: `ExecutorStepStore` has no public
constructor, and re-deriving an ack from a `QueueError` is lossy (`ClaimLost`
drops the scheduler's message). So:

- `ExecutorSteps::snapshot(&self, job_id) -> Result<Vec<JobStep>>` — exposes
  `Shared::snapshot_for`, which the parent encodes with the already-public
  `encode_step_snapshot` for the child's `job_steps` frame.
- `ExecutorSteps::relay_commit(&self, StepRelay<'_>) -> SchedulerMessage` — frames
  one commit upstream and hands back the `step_ack` to write downstream,
  **unaltered**. Built on a new `StepAnswer::into_ack`, the mirror of the existing
  `into_error`.

`StepRelay { job_id, timeout_ms, seq, step_key, kind, wake_at, result }` — the
frame's own fields plus the job's timeout, which is what bounds the ack wait.

Nothing else in core changes. No new frames.

## `flexiq-python` — parent side

- `executor.rs`: connect **before** building the pool (so the pool knows at
  construction whether the scheduler advertised `steps`), announce `CAP_STEPS` on
  `ExecutorConfig.capabilities`, and `pool.set_steps(handle.steps())` beside
  `set_side_channel`.
- `prefork/child.rs`: `spawn_child` takes `steps: bool` and advertises `CAP_STEPS`
  in the `hello_ack` it sends the child; returns the capabilities the child
  claimed in its `hello`, so the pool pays a snapshot read only for a child that
  uses it.
- `prefork/mod.rs`:
  - `steps_supported: bool` (ctor) + `steps: Arc<Mutex<Option<ExecutorSteps>>>`
    (`set_steps`, same late-install shape as the side channel).
  - `dispatch_job`: read the snapshot and write `job_steps` **before** `job`,
    under the one writer lock, when both sides claimed the capability. A snapshot
    read that **fails does not dispatch** — the job is reported as a retryable
    failure instead, because an empty snapshot re-runs every committed step.
    Same rule for "advertised but the handle is not installed yet".
  - reader thread: `ExecutorMessage::StepCommit` → check the job is the one this
    child is running (the slot), → `relay_commit` → write the `step_ack` back to
    that child. **Inline on the reader thread, deliberately**: `least_loaded`
    dispatch gives a child at most one in-flight job, so nothing queues behind it,
    and the child is blocked on this answer anyway.

## `flexiq-python` — child side

- `py_step.rs`: `PyStepSession` holds `StepSession<BoxedStepStore>` where
  `BoxedStepStore(Box<dyn StepStore + Send>)` is a local newtype — no core change,
  and the two shapes stop being two pyclasses.
- new `py_attached_steps.rs`: `PipeStepStore`, a `StepStore` whose `load_steps`
  decodes the snapshot the dispatch carried and whose `commit_step`/`commit_sleep`
  re-acquire the GIL to call a Python relay object, which writes the frame and
  parks on an `Event` (releasing the GIL, so the stdin reader can deliver).
- `py_worker_steps.rs`: `AttachedSteps` pyclass — the twin of `WorkerSteps`. Built
  per job (it already knows the job), same `open_step_session(job_id, attempt)`
  signature, and refuses with the core's own wording when the scheduler
  advertised no step store.

## Python SDK

- `worker_protocol.py`: `declared_payload_len` learns `job_steps` and
  `step_commit` (both `payload_len`). Without this the child desyncs on the first
  snapshot.
- new `flexiq/prefork/steps.py`: `StepRelay` — `commit(...)` writes the frame and
  waits for its `(job_id, seq)` ack; `deliver(ack)` from the reader thread;
  `abandon()` on reader exit so a dead parent releases a blocked commit as a
  disconnect rather than a full backstop wait.
- `prefork/child.py`: advertise `steps` in `hello`, read the parent's ack
  capabilities, demux `job_steps` (stashed per job id, popped by the `job` frame
  that follows) and `step_ack`, and hand `_execute_job` an `AttachedSteps` for the
  job when detached.
- `steps/context.py`: `_open`'s refusal loses the "an attached executor commits
  nothing" clause — it is no longer true. Still retryable, still a control signal.
- `_active_context.py` + `_flexiq.pyi`: `WorkerSteps | AttachedSteps | None`.

## Tests (`sdks/python/tests/worker/`)

`executor_apps/steps_app.py` + `test_executor_attach.py`, with `FakeScheduler`
gaining `send_job_steps` / `expect_step_commit` / `ack_step`:

1. **memo hit from a dispatch snapshot** — the scheduler sends a `job_steps` frame
   carrying `charge#0`; the body's closure must not run and the job's result must
   be the memoized value.
2. **a fresh step commits through the scheduler** — `step_commit` arrives with the
   encoded result, the ack lets the body finish, `success` carries the value.
3. **a commit the scheduler does not answer fails the attempt retryably** — a job
   with a short timeout, no ack: whichever deadline fires (the ack budget or the
   prefork watchdog, which are the same instant by construction) the frame is a
   `failure` with `should_retry: true`.
4. **a scheduler that never advertised `steps` refuses (§9.4)** — accept with no
   `steps` capability, dispatch a step-using job, assert a retryable failure
   naming "offers no step store".
5. `tests/core/test_steps.py::test_steps_refuse_on_an_attached_executor` is
   rewritten: it was asserting the message this change removes.

Plus `cargo test --workspace`, clippy `--all-targets --all-features`,
`cargo check` on default/postgres/redis/native-async, `ruff` + `mypy` on
`flexiq/ tests/`.

## Commits

1. core: `ExecutorSteps::snapshot` + `relay_commit`
2. python core: the prefork pool relays step frames
3. python core: a step session over the prefork pipe
4. python: the child opens durable steps on an attached executor
5. tests
