# #671 — `ctx.step().run` and `ctx.step().sleep` on the Java shell

Part of #663, and the last of the three shells. Mirrors #669 (Python, PR #732)
and #670 (Node, PR #734) over the same core: `flexiq_core::step`
(#666/#667/#668) on `job_steps` (#665). Implements the shell line of
`tasks/specs/2026-08-22-durable-steps-design.md` §12: §2.1 (names mandatory,
positional) · §2.2 (mixed keyed/unkeyed) · §4.2 (the error text) · §7.6
(`on_sleep` in contrib) · §7.7 (both swallow layers) · §9.2 (the retryability
table, and that a step frame never carries an owner) · §9.4 (refuse without a
step store).

Core is done and untouched by this branch.

## Surface

```java
@TaskHandler("checkout")
public Receipt checkout(Order order) throws Exception {
    JobContext ctx = JobContext.current();
    Charge charge = ctx.step().run("charge", Charge.class,
            () -> stripe.charge(order, ctx.step().idempotencyKey()));
    ctx.step().sleep(Duration.ofHours(1));
    return ctx.step().run("receipt", Receipt.class, () -> sendReceipt(charge));
}
```

- **`JobContext.current().step()`**, not a handler parameter. Java's task
  context is `JobContext` — the object `setProgress`, `log` and `publish`
  already hang off, and the same resolution Python (`current_job.step`) and Node
  (`currentJob().step`) reached. A trailing `ctx` parameter would change
  `TaskFunction`'s arity for every handler whether or not it wants one, and the
  annotation processor generates call sites against that arity.
- **Typed result, per the issue**: `run(name, Class<T>, body)` and
  `run(name, TypeReference<T>, body)`, the pair `Task.of` and
  `Worker.Builder.handle` already establish. A side-effect-only step is
  `run(name, StepAction)`.
- `java.time.Duration` / `java.time.Instant` for the sleeps — Java has the types
  the other two shells had to parse out of strings, so there is no duration
  grammar here at all.
- Explicit identity is `StepOptions.key("...")`; a sleep also takes
  `StepOptions.named("...")`.

## Checked exceptions, and why the control signals are `Error`s (§7.7)

The issue's second bullet: a step body must be able to throw without forcing
callers into a `try/catch` that swallows the divergence failure. Two decisions
answer it, and they are the same decision.

- `StepBody<T> { T get() throws Exception; }`, and `run` declares
  `throws Exception`. `TaskFunction.apply` already throws `Exception`, so a
  handler propagates a step body's checked exception for free.
- **Every step control signal extends `java.lang.Error`.** Java has the tier
  JavaScript lacks and Python spells `BaseException`: a `catch (Exception e)`
  around a step — the exact catch the issue is worried about — does not see a
  divergence or a sleep. That is §7.7 layer 1, and unlike Node the shell really
  has it.
- Layer 2 is still needed for `catch (Throwable)`: `StepLatch` is one flag per
  invocation, set before any control signal is thrown, checked the moment the
  handler returns and *before* the `after` hooks. A swallowed **sleep** stays
  invisible on purpose (the job is already `Pending` and unclaimed, so
  `handle_result`'s fence calls the swallow failure `Superseded` and drops it);
  the latch only bites a swallowed divergence.

Naming follows Node exactly — `StepDivergedError`, `StepLimitExceededError`,
`StepSupersededError`, `StepUnavailableError`, `StepSwallowedError`,
`StepSleepSignal` — which is both cross-SDK consistency and, here, literally
true: they are `Error`s.

## The retry verdict crosses JNI as a class, not as JSON

Node had to encode `{flexiqStep, message, retryable}` into a napi error string
because napi carries only a status and a string. JNI can throw *any* class, so
`steps.rs::step_error` maps `classify_step_failure` straight onto the exception
class and throws that — the shape Python's binding uses. There is no reason to
re-derive a verdict the class already names, so `StepControlSignal.shouldRetry()`
is the whole contract and `RetryDecision` reads it **before** the task's
`retryOn` predicate: that predicate has an opinion about the task's exceptions
and nothing useful to say about a divergence.

- divergence, cap violation, invalid step name, superseded attempt → DLQ.
- backend unavailable, no step store, no claim → retryable.

## The claim owner belongs to the worker (§1.4, §9.2)

`openStepSession` is a method on **`WorkerControl`**, whose JNI implementation
holds the native worker handle — so the owner is the id *that* worker claims
execution under and Java supplies only a job id and an attempt. #670's one
design correction was that a queue-level `claim_owner` slot is overwritten by a
second worker on the same handle; there is no such slot here and none is added.

The readiness problem Node solved with a holder is already solved in this shell:
`WorkerDispatchBridge` awaits `control` (a `CompletableFuture<WorkerControl>`
completed by `bind`) before it touches a job at all.

`attempt` is the `retryCount` the job was dispatched with, checked against the
row rather than trusted — a superseded attempt must not write into the live
attempt's sequence.

## Where a step may run (§9.4 — refuse, never degrade)

| Path | Steps | Why |
|---|---|---|
| in-process worker (`queue.worker()`) | yes | it holds storage and the claim owner |
| attached executor (`Executor`) | **refuses** | §9.1/§9.2's `job_steps` / `step_commit` / `step_ack` frames do not exist in core; there is no channel to commit on |
| `InMemoryFlexiQ` (test-support) | **refuses** | a pure-Java backend with no `job_steps` |
| backend without a step store | **refuses** | core already refuses in `StepSession::load` |

All four refuse *by construction*, through one throwing default on
`WorkerControl.openStepSession`: only `JniWorkerControl` overrides it. The
refusal is a `StepUnavailableError`, **retryable** — a heterogeneous fleet
mid-rollout may put the next attempt on a worker that can commit.

Deliberately **not** copying Python's `test_mode` inline path. §9.4 says refuse,
and #669's review spent four rounds on inline steps drifting from the core's
rules. `InMemoryFlexiQ` is a full queue simulation, not a `test_mode`, so a
step-using task is tested against a real worker over a temp SQLite file — which
is how every other worker test in this SDK already runs.

## `onSleep`, not `after` (§7.6)

`after(ctx, null)` reads as "the task returned null" to a Micrometer
observation. So `Middleware.onSleep(TaskContext, long wakeAt)`, and the
invariant **every `before` is matched by exactly one of `after` / `onSleep`**.
`WorkerDispatchBridge` owns the pairing, emits `job.sleeping`, and warns once
per middleware that defines `before` but not `onSleep`. `FlexiQObservation`
implements it (stop the observation with a `sleep` event and no error);
`SentryMiddleware` has no `before`, so it has nothing to pair.

---

## Work

### 1 — Expose the core session over JNI
- `crates/flexiq-java/src/steps.rs` (new): `StepSessionHandle` over
  `StepSession<StorageBackend>` with the `PendingStep` held *inside* the handle
  (as Node does, and for the stronger reason: a caller must not be able to
  invent a position). JNI entries
  `Java_..._NativeStepSession_{beginRun,commitRun,sleepFor,sleepUntil,runKey,finish,close}`
  and `Java_..._NativeWorker_openStepSession`. `step_error(QueueError)` maps
  `classify_step_failure` onto the exception class. `StepDecision` and
  `StepSleepOutcome` records are constructed natively (`NewObject`) so one call
  carries the whole answer and no bytes are re-encoded.
- `error.rs`: `BindingError::with_class`, so a step failure throws its own class
  instead of `FlexiQException`.
- `worker.rs`: `WorkerHandle` keeps `storage`, `namespace`, `worker_id`;
  `Java_..._NativeWorker_sleepJob`.
- `dispatcher.rs`: `TaskOutcome::Slept(wake_at)` → `JobResult::Slept` before any
  error/cancel branch; `submit_to_java` carries the dispatched `attempt`.
- `queue/inspect.rs`: `NativeQueue.supportsSteps`.

### 2 — SPI + internal plumbing
- `spi/StepSession.java` (`AutoCloseable`), `spi/StepDecision.java`,
  `spi/StepSleepOutcome.java` — records the native constructs.
- `spi/WorkerControl.java`: throwing-default `openStepSession` + `sleepJob`.
- `spi/QueueBackend.java`: `supportsSteps()` default `false`.
- `internal/NativeStepSession.java`, `internal/JniStepSession.java` (the
  read/write-lock contract `JniWorkerControl` already uses),
  `internal/JniWorkerControl.java`, `internal/JniQueueBackend.java`,
  `internal/NativeWorker.java`, `internal/NativeQueue.java`.

### 3 — `org.byteveda.flexiq.steps`
One concern per file: the eight signal classes, `StepBody`, `StepAction`,
`StepOptions`, `StepLatch`, `StepStore`, `StepContext`, `package-info`.
Step results are encoded with the **queue's** serializer, which already carries
the codec chain — that is how an encrypting codec reaches `job_steps` with no
extra plumbing.

### 4 — Wire it into the task path
- `JobContext`: `step()`, plus the constructor that takes one; the existing
  public constructor keeps working and yields a refusing context.
- `WorkerDispatchBridge`: build the latch + `StepContext`; sleep branch (no
  `onError`, no `job.failed`); `latch.check()` before `after`; the step verdict
  ahead of `retryOn`; close the session in the `finally`.
- `middleware/Middleware.java`: `onSleep`.
- `events/SleepEvent.java` (+ `FlexiQEvent` permits, `EventName.JOB_SLEEPING`
  doc).
- `worker/RetryDecision.java`: the step verdict outranks everything.
- `FlexiQ.supportsSteps()`.

### 5 — Contrib
`FlexiQObservation.onSleep` — the attempt has not finished, so neither
`after`'s success nor `onError`'s error describes it.

### 6 — Tests (`src/test/java/org/byteveda/flexiq/worker/StepsTest.java`)
- memo hit across a forced retry (the body runs once over two attempts)
- **keyed steps matched by key, not position** — asserted on the *returned
  values*, because counting body invocations is satisfied by a positional match
  too ([[node-durable-steps]]'s vacuous-test lesson)
- sleep/wake replay: the job goes `Pending`, earlier steps are memo hits on
  wake, and the attempt costs no `retryCount`
- the deadline is fixed by the first commit (a replayed `Duration.ofHours(1)`
  does not walk forward)
- idempotency-key stability across a retry, and its `{runKey}:{stepKey}` shape
- divergence dead-letters, non-retryably, with retries left
- the swallow latch bites a swallowed divergence (`catch (Throwable)`), and
  `catch (Exception)` does not see one at all
- `onSleep` pairs with `before`; `after` does not run; one `job.sleeping`
- refusal on a control with no step store (the executor's shape)
- an invalid step name is permanent, not retried

Never read the queue database from the test process while a worker runs
([[python-durable-steps]]) — assert through the queue API, events and counters.

`./gradlew build` (spotless, checkstyle, NullAway, tests) plus
`plainJavadocJar` for strict doclint; `cargo check`/`clippy -p flexiq-java` on
the feature sets the Gradle build uses.

---

## Review

Built as planned. Six things worth not re-deriving.

**Java has §7.7 layer 1, and it changes the whole shape.** Node's plan says the
first layer "does not exist in JavaScript"; here it does. `StepControlSignal
extends java.lang.Error`, so a `catch (Exception e)` around a step body — the
catch the issue's second bullet is worried about — genuinely cannot swallow a
divergence or a sleep. The latch is still needed for `catch (Throwable t)`, but
it is now the second line rather than the only one. A test asserts both halves:
the `Exception` arm never fires, the `Throwable` arm does.

**The retry verdict crosses as a class, not as JSON.** `steps.rs::step_error`
maps `classify_step_failure` onto an exception class name and throws it;
`BindingError` gained a `with_class` for that. Node had to serialize
`{flexiqStep, message, retryable}` into a napi message because napi carries only
a status and a string — JNI can throw any `Throwable`, so the class *is* the
verdict and `StepControlSignal.shouldRetry()` is the whole contract. Nothing
parses a message, and there is no "an older addon sent something unparseable"
branch to get wrong.

**The readiness problem Node solved with a holder was already solved here.**
`WorkerDispatchBridge` awaits a `CompletableFuture<WorkerControl>` that `bind`
completes, so the control is in hand before any job is touched. Putting
`openStepSession` on `WorkerControl` therefore needed no new plumbing — and it
is the right home for the same reason `JsWorker` was: the control holds the
native worker handle, so the owner every step write is fenced on is the id
*that* worker claims execution under. No `PyQueue.claim_owner` shape was copied
(#733).

**One throwing default covers all four refusal paths.** An attached executor, the
in-memory test backend, a backend with no step store and any custom control all
inherit `WorkerControl.openStepSession`'s `StepUnavailableError`. So the bridge
passes `bound::openStepSession` unconditionally rather than deciding for itself
whether steps are possible — which is exactly the shell-mirrors-a-core-rule
drift #669's review caught four times. `StepContext.available()` was written and
then deleted for the same reason: it could only have answered a question the
control already answers.

**Deliberately no `test_mode`.** Python degrades to inline steps under
`test_mode`; four of that branch's seven review findings were that inline path
drifting from the core. Java's `InMemoryFlexiQ` is a full queue simulation
rather than a harness, so it refuses like any other backend without a step
store, and a step-using task is tested against a real worker over a temp SQLite
file — which is how every other worker test in this SDK already runs.

**Local validation runs before the session is opened.** `checkName` and both
sleep converters refuse ahead of `session()`, so an empty step name is a
permanent `StepError` rather than the session's *retryable*
`StepUnavailableError` — and needs no storage read to say so. This is #670's
round-1 finding, applied at construction rather than found in review.

### Verified

`./gradlew build` — 629 tests, 0 failures, plus spotless, checkstyle and
NullAway; `plainJavadocJar` (strict doclint) clean on every new file;
`cargo clippy --all-targets --all-features -- -D warnings`; `cargo check
--workspace` on default, `postgres` and `redis`; `cargo fmt --check`.

Five of the new tests were checked **red** against a broken implementation
rather than trusted:

| break | failure |
|---|---|
| drop `StepOptions.key` | `[hello-alice, hello-bob]` where `[hello-bob, hello-alice]` was expected — the assertion that separates key matching from position matching |
| drop `latch.check()` | the swallowed divergence completes instead of dead-lettering |
| drop the step verdict in `RetryDecision` | the divergence spends the whole budget: 6 attempts, not 2 |
| `after` on the sleep path | two `after`s for two `before`s, breaking the pairing invariant |
| `sleep` never ends the attempt | the post-sleep step runs twice, and the replayed sleep emits no event |

The first of those is the vacuous-test trap [[node-durable-steps]] recorded:
counting how often a step body ran cannot tell key matching from position
matching, because both are memo hits. Only the returned values can.

**Left where they belong:** the dashboard's "sleeping until …" read, the docs
taxonomy tables (#672), and the attached executor's `job_steps` /
`step_commit` / `step_ack` frames, which do not exist in core and belong there
rather than in a shell.
