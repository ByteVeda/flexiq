# #670 — `ctx.step.run` and `ctx.step.sleep` on the Node shell

Part of #663. Mirrors #669 (Python, PR #732) over the same core:
`flexiq_core::step` (#666/#667/#668) on `job_steps` (#665). Implements the
shell line of `tasks/specs/2026-08-22-durable-steps-design.md` §12:
§2.1 (names mandatory, positional) · §2.2 (mixed keyed/unkeyed) · §4.2 (the
error text) · §7.6 (`on_sleep` in contrib) · §7.7 (the swallow layers) ·
§9.2 (retryability, and that a step frame never carries an owner) · §9.4
(refuse rather than degrade).

Core is done and untouched by this branch.

## Surface

```ts
import { currentJob } from "@byteveda/flexiq";

queue.task("checkout", async (order) => {
  const { step } = currentJob()!;
  const charge = await step.run("charge", () =>
    stripe.charge(order, { idempotencyKey: step.idempotencyKey }),
  );
  await step.sleep("1h");
  await step.run("receipt", () => sendReceipt(charge));
});
```

- **`currentJob().step`, not a handler parameter.** The issue sketches
  `(order, ctx)`, but node's task context *is* `currentJob()` — the same object
  `setProgress`, `log` and `publish` already hang off, and the same call Python
  resolved this to for #669. A trailing `ctx` parameter would collide with the
  `inject` deps `createTaskCallback` already appends, and would have to be
  passed to every handler whether or not it wants one.
- **Everything is async**, memo hit included (the issue's own requirement), so
  the call shape does not change between a fresh run and a replay. That also
  keeps the napi session methods off the event loop: each is `spawn_blocking`
  behind a `Promise`, unlike the Python binding, which only had to drop the GIL.
- `run(name, fn, { key })` · `sleep(duration, { name, key })` ·
  `sleepUntil(when, { name, key })` · `idempotencyKey` (sync getter, valid only
  inside a step body) · `runKey()` (async, because the session is).
- Duration grammar is the one debounce already established (`"500ms"`, `"5m"`,
  `"2h"`, a bare number is ms). `parseDuration` moves out of `debounce.ts` into
  `utils/duration.ts` so the SDK has one answer to "how do I write a duration".

## Where a step may run (§9.4 — refuse, never degrade)

| Path | Steps | Why |
|---|---|---|
| in-process worker (`runWorker`) | yes | it holds storage and the claim owner |
| attached executor (`runExecutor`) | **refuses** | §9.1/§9.2's `job_steps` / `step_commit` / `step_ack` frames do not exist in core; there is no channel to commit on |
| backend without a step store | **refuses** | core already refuses in `StepSession::load` |
| a queue with no worker | **refuses** | no execution claim to fence on |

The executor refuses by construction: `createTaskCallback` takes an optional
`steps` dep and `Executor.start` does not pass one — the same override pattern
it already uses for `isCancelled`, `setProgress` and `writeTaskLog`, and for
the same reason (`queue` is only the detached stand-in when the process is a
`flexiq executor`; an executor started from a process that *does* have storage
must not reach it either).

A refusal is a `StepUnavailableError`, **retryable**: a heterogeneous fleet
mid-rollout may put the next attempt on a worker that can commit.

## The claim owner lives on the queue handle, not on the frame

The fence is `(owner, attempt)` and `owner` must never be something the running
code asserts about itself (§1.4, §9.2).

- `JsQueue` gains `claim_owner: Mutex<Option<String>>`. `run_worker` mints the
  worker id, records it, *then* calls `start_worker` — so the owner is in place
  before the scheduler loop can dispatch anything. (`start_worker` stops
  generating its own id and takes one.)
- **Not** a field on `JsTaskInvocation`: the same struct is what the attached
  executor's dispatcher fills in from a socket frame, and an owner an executor
  supplies is one it can get wrong.
- `attempt` *is* new on `JsTaskInvocation` — it is the dispatched job's
  `retry_count`, which is not a claim and not forgeable into one:
  `openStepSession` re-reads the job row and refuses a mismatch as a lost claim,
  and the storage fence would refuse the write regardless.
- Known limitation, documented on `openStepSession`: two workers on **one**
  `Queue` share one owner slot, so the older worker's step commits are refused
  as superseded. Nothing is ever written under the wrong claim — the fence sees
  to that — but such a job fails rather than running. One worker per queue
  handle when using steps. (Same shape as the Python shell.)

## Both swallow layers (§7.7), in a language where `catch` catches everything

Layer 1 does not exist in JavaScript: there is no `BaseException` a bare
`catch` misses. That is exactly the case §7.7's second layer was written for,
and it is the whole of the answer here.

`StepLatch` is one flag per invocation. `ctx.step` sets it before it rejects
with a `StepControlSignal`; `createTaskCallback` reads it the moment the
handler resolves, *before* the `after` hooks, and fails the attempt with
`StepSwallowedError` if it is set. Documented precisely on `run`, per the
issue's second bullet: the rejection is ordinary and catchable, and catching it
does not let the attempt succeed.

A swallowed **sleep** is invisible, and that is not a bug (the lesson #669
learned): `sleepFor` already left the job `Pending` and unclaimed, so
`handle_result`'s fence calls the swallow failure `Superseded` and drops it —
the job wakes, the sleep is a memo hit, the body finishes. One attempt wasted.
The latch only *bites* on a swallowed divergence, where the attempt still holds
its claim. The tests say so, rather than pretending otherwise.

## Retryability (§9.2)

The core classifies (`classify_step_failure`); the binding carries the answer
across the FFI as a JSON reason — the pattern #413 already set for
`encodeTaskError` — and `steps/errors.ts` rebuilds the right class from it.
`isRetryable` in `task-callback.ts` reads `flexiqShouldRetry` **before** it
consults the task's `retryOn` predicate: that predicate has an opinion about
the task's exceptions and nothing useful to say about a divergence.

- divergence, cap violation, invalid step name, superseded attempt → DLQ.
- backend unavailable, no step store, no claim → retryable.

## `onSleep`, not `after` (§7.6)

`after(ctx, undefined)` reads as "the task returned undefined" to OTel and
Prometheus. So `Middleware.onSleep(ctx, wakeAt)`, and the invariant **every
`before` is matched by exactly one of `after` / `onSleep`**.
`createTaskCallback` owns the pairing, emits `job.sleeping` (its `SleepEvent`
gains `stepKey`, matching the Python payload), and warns once per middleware
that defines `before` but not `onSleep`. `contrib/otel` and
`contrib/prometheus` implement it; `contrib/sentry` has no `before` on this
shell, so it has nothing to pair and gains nothing.

---

## Work

### 1 — Expose the core session to Node
- `crates/flexiq-node/src/steps.rs`: `JsStepSession` over
  `StepSession<StorageBackend>` (`beginRun`, `commitRun`, `sleepFor`,
  `sleepUntil`, `runKey`, `idempotencyKey`, `finish`), `JsStepDecision` holding
  the `PendingStep` so a caller cannot invent a position, `JsStepSleepOutcome`.
  Every storage-touching method is `async` (`spawn_blocking`).
- `step_error(QueueError) -> napi::Error`: a JSON reason
  `{"flexiqStep":"<kind>","message":…,"retryable":…}` built from
  `classify_step_failure`.
- `crates/flexiq-node/src/queue/steps.rs`: `JsQueue::openStepSession(jobId,
  attempt)` (resolves the owner, re-reads the job, refuses a mismatched
  attempt, `StepLimits::default()`) and `supportsSteps()`.
- `queue/mod.rs`: the `claim_owner` field, set in `run_worker`;
  `worker.rs::start_worker` takes the id.

### 2 — Report a slept attempt
- `convert/job.rs`: `JsTaskInvocation.attempt`, `JsTaskOutcome.sleptUntil`.
- `dispatcher.rs`: fill `attempt`; `sleptUntil` → `JobResult::Slept` before any
  error/result branch.
- `convert/outcome.rs`: `ResultOutcome::Slept` still emits nothing — the
  comment now names where the hook and event actually fire.

### 3 — `steps/` on the TypeScript side
`sdks/node/src/steps/` — `errors.ts`, `latch.ts`, `durations.ts`,
`context.ts`, `index.ts` (one concern per file, one barrel).
Step results are encoded with the **queue** serializer (`deps.serializer`),
which already carries the queue codec chain — that is how `new Queue({ codec })`
encryption reaches `job_steps` with no extra plumbing.

### 4 — Wire it into the task path
- `context.ts`: `JobContext.step`.
- `task-callback.ts`: build the latch + `StepContext` (or a refusing one when
  no `steps` dep); sleep branch (no `onError`, no `job.failed`); swallow check;
  `isRetryable` reads the step verdict first; `finish()` in the `finally`.
- `middleware.ts`: `onSleep`. `events.ts`: `stepKey` on `SleepEvent`.
- `index.ts`: export the step surface.
- `utils/duration.ts` + barrel; `debounce.ts` imports it.

### 5 — Contrib
`otel.ts` ends the span with no status and a `sleep` event; `prometheus.ts`
decrements `activeWorkers` and counts a separate `sleeps_total` — neither
`completed` nor `failed` describes an attempt that has not finished.

### 6 — Tests (`sdks/node/test/worker/steps.test.ts`)
- memo hit across a forced retry (the closure runs once over two attempts)
- sleep/wake replay: the job goes `Pending`, holds no slot, earlier steps are
  memo hits on wake, and the attempt costs no `retryCount`
- exactly one `job.sleeping` for a sleep replayed past its deadline (the
  deadline is not pushed forward). The pre-deadline `Resume` arm is core-owned
  (#667) and not reachable from the shell's public surface.
- idempotency-key stability across a retry, and its `{runKey}:{stepKey}` shape
- divergence dead-letters, non-retryably, with retries left
- the swallow latch bites a swallowed divergence; a swallowed sleep is
  documented as harmless (the job still wakes and completes)
- `onSleep` pairs with `before` and `after` does not run
- refusal with no `steps` dep (the executor's shape), through
  `createTaskCallback` directly
- **Never read the queue database from the test process** — assert through
  `queue.getJob`, events and closure counters ([[python-durable-steps]]).

`pnpm build:native` (the addon is regenerated: `native/index.d.ts` is
gitignored and `tsc` reads it), `pnpm typecheck`, `pnpm lint`, `pnpm test`,
plus `cargo check`/`clippy` on default, postgres, redis.

---

## Review

Built as planned. Five things worth not re-deriving.

**The retry verdict had to cross the FFI as JSON.** The Python binding builds a
real exception object with a `flexiq_should_retry` attribute; napi carries only
a status and a string, and no status can say "this is a divergence and it must
not be retried". So `steps.rs::step_error` puts
`{"flexiqStep":…,"message":…,"retryable":…}` in the reason and
`steps/errors.ts::stepErrorFrom` rebuilds the class from it — the same shape
#413 established for task errors. A reason that does not parse becomes a
**retryable** `StepError` carrying the raw message: an addon older than the
shell is a reason to fail the attempt, not to guess that it is permanent.

**The pending step lives in the session, not in the decision.** `PyStepDecision`
holds the `PendingStep` and `commit_run` takes it back, but an `async fn` on a
napi class cannot hold a `&JsStepDecision` across its await. Keeping it inside
`SessionState` is not a workaround: it is strictly tighter — there is no token
for a caller to invent a position with, and there is never more than one,
because `check_issuable` refuses a second `beginRun` while one is uncommitted.

**Everything is `spawn_blocking` behind a `Promise`.** The Python binding only
had to drop the GIL; node has one thread for every task on the worker, so a
synchronous commit stalls every other job's timers and cancel polls. The one
casualty is `runKey`, a property in Python and a `Promise` here. `sleepFor` is
a storage write on the same path, which is why it is async too.

**The claim owner is minted in `run_worker`, not in `start_worker`.** Recording
it after `start_worker` returned would leave a window where the scheduler loop
is already dispatching and the queue's owner slot is still empty — a task that
reached for `ctx.step` in that window would refuse. `start_worker` now takes the
id.

**A `supportsSteps()` probe is exposed but is not the gate.** `open()` refuses
only on a missing store (the executor) and otherwise lets the core's
`StepSession::load` answer: mirroring the backend check in the shell is exactly
the drift #669's review caught four times. `supportsSteps()` is public surface
on `Queue` instead, for an app that wants the answer without paying a job read.

**Two smaller deviations from the Python shell**, both deliberate:

- A step result the queue serializer cannot encode is raised as a *permanent*
  step failure naming the step, and latches. Python lets the serializer's own
  `TypeError` out, which the body can swallow while the step has already run
  its side effect and committed nothing.
- `SleepEvent.queue` stays undefined: the dispatch frame carries no queue name,
  and inventing one from the task's registration would be a guess. Python reads
  it off `current_job.queue_name`.

**Known limitation, documented on `openStepSession`:** two workers started from
one `Queue` share one owner slot, so the older worker's commits are refused as
superseded — nothing is written under the wrong claim, but such a job fails
rather than running. The Python shell has the same shape.

**Left where they belong:** the dashboard's "sleeping until …" read (four
surfaces plus `api-types.ts`), the docs taxonomy tables (#672), and
`CONTRACT_VERSION` → 2 (§11, which belongs to the release).

### Verified

`pnpm test` 761 passed / 6 skipped over 102 files, including 14 new step tests
and one on the executor refusal path; `pnpm typecheck`; `pnpm lint`;
`cargo clippy -p flexiq-node --all-targets --features postgres,redis,mesh,workflows`;
`cargo check --workspace` on default, `postgres` and `redis`; `cargo fmt --check`.

Three of the new tests were checked **red** against a broken implementation
rather than trusted: dropping `latch.check()` and the step retry verdict fails
the divergence and swallow tests, removing the `onSleep` dispatch fails the
pairing test, and dropping `{ key }` fails the keyed-step test with
`[ 'alice', 'bob' ]` where `[ 'bob', 'alice' ]` was expected — the assertion
that separates key matching from position matching. The first version of that
test only counted callback runs, which a positional match satisfies too.
