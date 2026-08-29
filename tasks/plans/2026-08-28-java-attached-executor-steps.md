# #744 — java: durable steps on an attached executor

Part of #663, follows #736 (core), #742 (python), #743 (node).

## Shape

Java's attached executor runs task bodies **in this process** — `JavaDispatcher::detached`
hands each job to a `WorkerBridge` on the SDK's own handler pool. So this is the deployment
the issue describes literally: the session opens here over `ExecutorSteps::open_session`,
and none of the proxy machinery a prefork shell needs applies.

Everything the Java side needs is already wired: `WorkerDispatchBridge` passes
`bound::openStepSession` unconditionally and calls `bound.sleepJob` on the sleep path.
Both defaults on `WorkerControl` throw. The gap is entirely in the executor's own control.

## Rust — `crates/flexiq-java`

1. **`steps.rs`** — erase the store. `StepSessionHandle` holds
   `StepSession<Box<dyn StepStore + Send>>` (a `BoxedStepSession` alias), because a
   `#[no_mangle]` handle is one concrete type and the two deployments write through
   different stores. `ExecutorStepStore` has no public constructor, so core's
   `StepSession::boxed()` is what makes this possible. Worker path calls `.boxed()`.
2. **`attached_steps.rs`** (new) — `RunningJobs` / `RunningJob`, the job-id → dispatch
   registry, plus `NativeExecutor.openStepSession` and `NativeExecutor.supportsSteps`.
   `open_session` wants a `&Job`; Java asks by job id, and only the dispatcher ever holds
   the dispatch.
3. **`executor.rs`** — announce `CAP_STEPS` on the `ExecutorConfig`; hold `ExecutorSteps`
   and the `RunningJobs` on `AttachedHandle`; add `NativeExecutor.sleepJob` beside the
   other completion entry points.
4. **`dispatcher.rs`** — `running: Option<Arc<RunningJobs>>`, set only by `detached`.
   `run_one` takes the payload out of the job, registers the guard **before**
   `submit_to_java` (`onJob` hands the job to a handler thread that may ask for a session
   the moment it returns), and passes the payload separately.
5. **`lib.rs`** — `mod attached_steps;`.

## Java — `sdks/java`

6. **`internal/NativeExecutor.java`** — `openStepSession`, `sleepJob`, `supportsSteps`.
7. **`internal/JniExecutorControl.java`** — override `openStepSession` and `sleepJob`;
   expose `supportsSteps()`.
8. **`spi/WorkerControl.java`** — reword the two defaults. They stay the honest answer for
   a backend with no step store and for any custom control; they are no longer the answer
   for an attached executor.
9. **`worker/Executor.java`** — `supportsSteps()` accessor, and one **info** log at attach
   when the scheduler offers no step store. Info, not warn: progress belongs to every job,
   steps are opt-in, and a fleet using none would be warned for nothing.
10. **`steps/StepContext.java`** — the `@Nullable StepStore` comment names the attached
    executor as the case that cannot commit. Stale now.

## Tests

11. Extract `FakeScheduler` from `ExecutorAttachTest` into its own public test class so the
    step tests can drive one. Its framing rule must read `payload_len` **by field** with
    `result_len` / `extra_len` aliases — the per-frame-type version returns 0 for
    `step_commit` and desyncs the wire on the first commit carrying bytes. Add
    `sendJobSteps`, `ackStep`, `refuseStep`, `nextFrame(type)`, `disconnect()`.
12. `steps/ExecutorStepsTest.java` — six tests:
    - `steps` in the `hello` capability list
    - a memo hit replayed from a `job_steps` snapshot; the body never runs
    - a fresh commit + ack: `step_key`, `kind`, the serializer's bytes, **no owner**
    - a sleep: two frames, and the `slept` frame carries the *ack's* deadline
    - an unacknowledged commit (dropped connection) → retryable, asserted **in the
      handler**: the connection carrying the answer is the one being dropped, so there is
      no failure frame to read
    - §9.4: a scheduler with no `steps` capability refuses, retryably
13. Rust unit tests for `RunningJobs` (entry lifetime, concurrent attempts kept apart),
    building the `Job` through `SchedulerMessage::into_dispatch`.

Every test checked **red** against a broken implementation before being trusted.

## Docs

14. `guides/core/steps.mdx` — the `<SdkSwap>` on the attached-executor bullet collapses:
    all three shells commit through the scheduler now.

## Verify

`cargo fmt --all --check` · `cargo clippy --all-targets --all-features -- -D warnings` ·
`cargo check --workspace` on default/postgres/redis/native-async · `cargo test --workspace`
· `cargo test --doc` · `./gradlew build` (spotless, checkstyle, NullAway, every module) ·
`:plainJavadocJar` · `pnpm --dir docs build`.
