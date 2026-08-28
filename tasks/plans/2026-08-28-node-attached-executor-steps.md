# #743 — node: durable steps on an attached executor

Follows #736 (core, `09640ac4`) and #742 (python, `13bd619f`). Siblings: #744 java.

## Shape of the change

Node's executor is **not** prefork — `NodeDispatcher::detached` runs the task
callback in this process, so the session opens here and there is no relay hop.
That makes this the case the issue's file list describes literally: hold the
`ExecutorSteps` handle beside the `ExecutorSideChannel` and open sessions over
it.

The one thing the issue's list does not mention: `ExecutorSteps::open_session`
takes a `&Job`, and JS asks for a session by **job id**. The dispatcher is the
only place the dispatched `Job` exists, so the executor needs a registry of the
jobs it is running. Python did not hit this — it rebuilds the `Job` from the
dispatch frame's own JSON, because its pool relays rather than opens.

## Steps

### Rust — `crates/flexiq-node`

1. `steps.rs`: add `BoxedStepStore` (mirror of the python shell's) and make
   `JsStepSession` hold `StepSession<BoxedStepStore>`. A `#[napi]` class cannot
   be generic, and the executor's store is a different type from the worker's.
   `JsWorker::open_step_session` builds `StorageSteps::new(storage, owner,
   attempt)` explicitly instead of going through `StorageStepSession::load`.
   No napi surface change.
2. New `attached_steps.rs` (peer of python's `py_attached_steps.rs`):
   - `RunningJobs` — job id → the dispatched `Job`, minus its payload.
   - `impl JsExecutor { supports_steps, open_step_session }`. Refusal for a
     scheduler without `CAP_STEPS` is left to `ExecutorSteps::open_session`,
     which already raises the retryable §9.4 error; mirroring that rule in the
     shell is the drift #669's review caught four times.
   - A job id the executor is not running, or an attempt that is not the
     dispatched one, is `ClaimLost` — same guard as both worker twins.
3. `dispatcher.rs`: `NodeDispatcher::detached` takes the registry. `run_one`
   registers the job **after** `mem::take(&mut job.payload)` (so the clone is
   cheap) and deregisters through a guard, on every exit path.
4. `executor.rs`: announce `capabilities: vec![CAP_STEPS]`, build the registry,
   hold `handle.steps()` on `JsExecutor`.

### TypeScript — `sdks/node/src`

5. `executor.ts`: pass a `steps` dep to `createTaskCallback` that opens over the
   native executor, and warn once at attach when the scheduler advertises no
   step store — the shape the side-channel warning already uses. Expose
   `Executor.supportsSteps`.
6. `steps/context.ts`: the `!this.store` branch stays — `TaskCallbackDeps.steps`
   is still optional — but its comment no longer says "attached, so refuse".

### Docs

7. `docs/content/docs/shared/guides/core/steps.mdx`: the node arm of the
   attached-executor `<SdkSwap>` joins python's wording. Java keeps the refusal
   until #744.

### Tests — `sdks/node/test`

8. Extract `FakeScheduler` from `executorAttach.test.ts` into
   `worker/fakeScheduler.ts` so the step tests can drive a scheduler frame by
   frame. Teach its `declaredPayloadLength` about `step_commit`, or the reader
   desyncs on the first commit that carries bytes.
9. `worker/steps.test.ts` gains the acceptance set:
   - a memo hit replayed from a dispatch `job_steps` snapshot — the callback
     never runs and the result is the memoized value;
   - a fresh commit crossing as `step_commit` and the job succeeding on the ack;
   - a commit the scheduler never acknowledges → retryable failure;
   - the §9.4 refusal for a scheduler that advertised no `steps` capability
     (moved here from `executorAttach.test.ts`, whose copy asserted the old
     "attached executor" message).

## Verify

`pnpm build:native` (napi signatures change), `pnpm typecheck`, `pnpm lint`,
`pnpm test`; `cargo clippy -p flexiq-node --all-targets`; `cargo check
--workspace`; `cargo fmt --check`; `pnpm --dir docs build` for the MDX.

## Review

### The core addition the plan did not foresee

`ExecutorSteps::open_session` hands back a `StepSession<ExecutorStepStore>`, and
`ExecutorStepStore` has **no public constructor** — so a shell cannot rebuild
that session over a store of its own. A `#[napi]` class cannot be generic, which
left three options: duplicate the split-form session per transport, match an
enum in all six methods, or erase the store. Erasing it is the same answer the
prefork shell reached, but it needed the erasure to happen *after* core built
the session rather than before. Hence `StepSession::boxed()` and
`impl StepStore for Box<dyn StepStore + Send>` in the core — reopening instead
would re-read the snapshot, which §5.1 allows exactly once per attempt.

### Where the dispatch comes from

Node's executor is not prefork, so the session opens in this process — but JS
asks for one by **job id**, and the core opens a session against the `Job`. Only
the dispatcher ever holds one. `RunningJobs` records it for the length of the
attempt, after `run_one` has moved the payload out (so an entry costs a handful
of strings), and an RAII guard removes it on every exit path including the
timeout. A step opened past that point is refused as a lost claim rather than
committed into an attempt the scheduler has already reaped.

### Verified

`cargo fmt --all --check` · `cargo clippy --all-targets --all-features -D
warnings` · `cargo check --workspace` on default/postgres/redis/native-async ·
`cargo test --workspace` (37 suites) and `--doc` · `cargo test -p flexiq-node`
(the `RunningJobs` guard) · node `biome ci`, `tsc`, `pnpm test` 772 passed /
6 skipped · `pnpm --dir docs build`, the only thing that compiles the MDX.

**Every new test was checked red.** Deleting the `steps` dependency from
`Executor.start` fails all five executor step tests (two by assertion, three by
frame timeout); stubbing out `Drop for RunningJob` fails both registry tests.
The capability assertion reads `hello.capabilities`, which serialises as `[]`
without the change.
