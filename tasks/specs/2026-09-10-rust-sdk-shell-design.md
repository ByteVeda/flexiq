# A Rust SDK shell over flexiq-core — design

Issue [#830](https://github.com/ByteVeda/flexiq/issues/830). Milestone: Next. Epic
[#835](https://github.com/ByteVeda/flexiq/issues/835).

This is a decision record for `crates/flexiq` and `crates/flexiq-macros`, not a contract. The
contract is [`crates/flexiq-core/BINDING_CONTRACT.md`](../../crates/flexiq-core/BINDING_CONTRACT.md);
where the two disagree, that one is right and this one is out of date.

## The question the issue asks first

#830 refuses to be implemented before someone decides whether a Rust SDK is embedded, remote, or
both behind one API, and says the answer belongs in the issue before the code does. It is **both,
embedded first, and not behind one API.**

"One API behind a feature flag" cannot be built as stated. Cargo features are additive: if
anything in a dependency graph enables `remote` while something else enables `embedded`, both are
on. A feature can decide whether tonic compiles in. It can never decide which API the caller gets.
So the design has to work with both enabled, which means two types rather than one type with two
bodies.

The two doors also disagree in the signature, not the implementation:

| | Embedded | Remote |
|---|---|---|
| Namespace | `NewJob.namespace`, set per job | **No field on the wire**, and never will be — `REMOTE_SDK_CONTRACT.md` §The namespace: "anything a client could name is something a client could forge" |
| Batch | `Result<Vec<Job>>` | A result per item, *or* the whole RPC fails carrying the failing item's `index`; a client MUST handle both shapes |
| Reads | `get_job` always carries the payload | Omitted unless `include_payload` is set |
| Errors | `QueueError`, wrapping `diesel::result::Error` | gRPC status; branching on `ErrorInfo.reason` is mandatory, branching on the message is forbidden |

Unify those and one of two things happens. Either the embedded surface degrades to the remote
vocabulary — which defeats the premise, since a Rust user who already has the crate would go on
using `flexiq-core` raw — or half the methods answer `Err(Unsupported)` at runtime, which is a
compile-time flag producing runtime refusals.

What is worth unifying is not the handle but **the call**. `charge::call("ord_1", 4200)` is a task
name plus CBOR bytes; it is transport-free, and both doors accept it. So `TaskCall` and the option
types are shared, `remote` is a feature that *adds* a handle rather than reshaping one, and
anything the remote door cannot do is simply not a method on it.

## Why embedded is the half that ships first

- `crates/flexiq-core/src/wire/mod.rs:6-8` already names its audience. The envelope writer exists
  for two callers and one of them is "a Rust producer using the `flexiq` crate directly". Half of
  this work was done in advance.
- `REMOTE_SDK_CONTRACT.md` §The delta from an embedded SDK lists **task registration** and durable
  steps as recorded decisions, not gaps: "The server holds no task registry. A task name is a
  string." An attribute macro and a worker — most of what #830 asks for — cannot be built over the
  producer door at all.
- The remote half is largely a re-run of `sdks/go`, and it would land in a crate that is already
  published. Adding tonic, prost and a stub-generation story to `flexiq` brings a publish
  obligation and a `publish-readiness` job that verifies the crates as a set.

The remote handle becomes its own issue under #835. The docs tier becomes a second, stacked PR —
see [Sequencing](#sequencing).

## Shape

Two crates. `crates/flexiq` is today a pure re-export facade (`pub use flexiq_core::*`) with no API
of its own; it gains the shell **additively**, so nothing published at 2.0.0 changes meaning.

| File | Holds |
| --- | --- |
| `flexiq-macros/src/lib.rs` | the `#[task]` entry point |
| `flexiq-macros/src/attrs.rs` | attribute parsing → `TaskAttrs` |
| `flexiq-macros/src/expand.rs` | codegen |
| `flexiq-macros/src/duration.rs` | `"30s"` and bare-millisecond literals |
| `flexiq/src/task.rs` | the `Task` trait the macro implements |
| `flexiq/src/call.rs` | `TaskCall<T>` and its per-call overrides |
| `flexiq/src/options.rs` | `EnqueueOptions` → `NewJob` + `DebounceOptions` |
| `flexiq/src/encode.rs` | `serde::Serialize` → `WireValue` |
| `flexiq/src/decode.rs` | payload bytes → typed arguments |
| `flexiq/src/queue.rs` | `FlexiQ`: open, enqueue, batch, cancel, reads, stats |
| `flexiq/src/worker.rs` | `WorkerBuilder` and the shell's `WorkerDispatcher` |
| `flexiq/src/step.rs` | the step handle a running task holds |
| `flexiq/src/periodic.rs` | cron registration |
| `flexiq/src/error.rs` | `Error`, `Abort`, and the contract's task-error JSON |

The glob re-export stays as the escape hatch, so `flexiq::Storage` and friends keep working. New
names were checked against `flexiq-core/src/lib.rs:44-87` and none collide. `Worker` is avoided
deliberately: core exports it, and a shell type by that name would silently retype `flexiq::Worker`
for existing callers — a breaking change with no signal. The shell's builder is `WorkerBuilder` and
`spawn()` returns core's `WorkerHandle`.

## Decisions

### A task is named after its function, not its module path

```rust
#[flexiq::task(max_retries = 5, timeout = "30s", queue = "billing", on_excess = "drop")]
fn charge(order_id: String, cents: i64) -> Outcome<Receipt> { ... }
```

expands to a zero-sized `charge` implementing `Task`, an inherent `charge::call(..) -> TaskCall<charge>`,
and `charge::run(..)` holding the original body so it stays directly callable in a unit test.

The default name is `"charge"`. `module_path!()` was rejected: it embeds the crate name, so renaming
a binary would change a task name that a Python producer has to type. Python is the only shell that
derives a name from source location; Node and Java both require an explicit one, and a cross-SDK
name a build can change is worse than a short one. `name = "billing.charge"` overrides it, and
duplicate names are a registration error at `spawn()` rather than a silent last-write-wins.

Arguments are `Serialize + DeserializeOwned`. Rust has no keyword arguments, so `kwargs` is always
the empty map — the same positional-only asymmetry `BINDING_CONTRACT.md` records for Node.

### The attributes split along a line core already drew

| Attribute | Lands on |
|---|---|
| `max_retries`, `retry_backoff`, `retry_delays` | `RetryPolicy` |
| `rate_limit`, `on_excess`, `retry_budget`, `circuit_breaker`, `max_concurrent`, `max_in_flight_per_task` | `TaskConfig` |
| `queue`, `priority`, `timeout`, `expires`, `result_ttl`, `unique_key`, `idempotent`, `debounce*` | shell defaults, seeded into `NewJob` by `call()` |
| `cron`, `timezone` | `NewPeriodicTask` |

`TaskConfig` (`scheduler/mod.rs:283`) carries dispatch policy and nothing else — no `timeout`, no
`priority`, no `queue`, because those are per-job `NewJob` fields. So core owns dispatch config and
the shell owns enqueue defaults, and the macro is just a typed front for both.

### Encoding goes through core; decoding cannot

`flexiq_core::wire::encode_call` is the pinned writer, asserted byte-for-byte by the `encode` cases
in `contracts/wire-vectors.json`. The shell converts `Serialize` values to `WireValue` and hands
them over. It does **not** encode CBOR itself: `wire/mod.rs:13` is explicit that this is "the Rust
implementation of that one format, not a second definition of it", and a second encoder that merely
happens to agree today is how `auto:` idempotency keys quietly stop deduping.

There is no reader to reuse — `wire/cbor.rs:11`: "There is no reader. Nothing in this crate decodes
a payload." A Rust worker is the first thing in the tree that has to, so decoding uses `ciborium`,
the one new third-party dependency, added to `crates/flexiq` rather than to core so the engine's
dependency list is untouched. The asymmetry is intentional: writing has one legal answer and
reading has to accept everything any writer may legally emit.

### The shell brings its own dispatcher, and it carries the fence

This is the load-bearing change. `NativeDispatcher` cannot support durable steps, because a handler
never sees what steps are fenced on. `TaskHandler` (`worker/registry.rs:61-66`) hands a closure
`&Job` and nothing else, and while `Worker::spawn` calls `set_claim_owner` and `set_lease_book` on
whatever dispatcher it was given (`worker/runner.rs:217,220`), `NativeDispatcher` overrides neither
— both are default no-ops at `worker/mod.rs:76,87`. The scheduler mints the fence and the built-in
pool drops it on the floor.

So the shell ships a `WorkerDispatcher` of its own, the same shape as `NativeDispatcher`, which
stores the owner and the `Arc<LeaseBook>` and per job builds:

```rust
StorageSteps::new(storage, &owner, job.retry_count)
    .with_epoch(lease_book.current(&job.id).and_then(|l| l.epoch()))
```

`.with_epoch` is called by **no existing shell**. Python, Node and Java all fence on
`(owner, attempt)` and leave the epoch unset, because each of them reaches steps through an FFI
class that has to be one concrete non-generic type. A pure-Rust shell has none of those reasons, so
it wires the third term rather than making a fourth copy of the gap.

For the same reason the shell keeps `StorageStepSession<StorageBackend>` concrete and skips
`StepSession::boxed`/`BoxedStepStore` entirely, and calls `StepSession::run` directly instead of the
split `begin_run`/`commit_run` the FFI shells need.

### Sleeping is a third outcome, not an error

`sleep_for`/`sleep_until` already do the storage write — release the claim, reschedule, commit the
sleep row — inside one transaction via `commit_sleep`. The dispatcher only reports what happened,
and it must report it as `JobResult::Slept`, which deliberately skips the `(owner, attempt)` fence
in `handle_result` (`scheduler/result_handler.rs:86-103`) because the job is already `Pending` and
unclaimed; re-checking would misread a correctly slept job as superseded.

`TaskResult` is `Ok`/`Err` and cannot say "sleeping". The shell adds `Outcome<T> = Result<T, Abort>`
with `Abort::{Fail(TaskError), Sleep(StepSleep)}`. `step.sleep(..)?` unwinds through the user's
body; the dispatcher maps `Sleep` to `JobResult::Slept` and never to `Success` or `Failure`. Rust
needs no signal exception for this, unlike Python's `StepSleepSignal`.

A caller's own error reaches `Abort` through `TaskError`, which is the type that already carries the
retryable/fatal distinction the scheduler acts on. `Abort` gets `From<TaskError>` and nothing wider:
a blanket `From<E: std::error::Error>` would collide with it, and — more to the point — it would
have to guess retryability, which is the one bit of a failure only the task author knows. So a task
body ends `.map_err(TaskError::retryable)?` or `.map_err(TaskError::fatal)?`, and the choice stays
explicit at the call site.

`Abort` and the shell's matching on `JobResult`/`ResultOutcome` both have to respect that those two
core enums are `#[non_exhaustive]` (`scheduler/mod.rs:113,195`).

### A failed Rust task produces the error shape the contract specifies

`NativeDispatcher::job_result` (`worker/dispatcher.rs:52-63`) stores `TaskError.message` raw.
`BINDING_CONTRACT.md:349-377` specifies `{"errtype","message","traceback"}` JSON in
`JobResult::Failure.error`, which is what every other shell writes and reads. The shell's dispatcher
emits it, so a Python or Node reader inspecting a Rust task's failure gets a structured error rather
than a bare string.

`FlexiQ::open` also calls `ensure_contract_supported`, which `BINDING_CONTRACT.md:556-563` requires
of every shell at storage open and which neither `Worker::spawn` nor the core examples do today.

### Periodics need no new plumbing, and one written-down caveat

Registration is three storage calls the shell makes directly — `register_periodic` at `spawn()`,
with `next_run` from `periodic::next_cron_time{,_tz}` — plus `list`/`delete`/`set_enabled` for
management. `Scheduler::check_periodic` already runs inside the loop `Worker::spawn` starts. Six-field
cron (seconds first), IANA timezone, `None` meaning UTC.

The caveat is not new but is nowhere documented: firing is per-`Scheduler` and **not** leader-elected,
and the dedup key is `"periodic:{name}:{now}"` computed from each process's own clock, so two workers
that both registered the same periodic can both fire it in the same window. Every shell inherits this
silently. The Rust one says so in its rustdoc, and the guidance is to register periodics from one
process.

### What this does not do

No remote handle, no `flexiq.executor.v1` client, no admin surface, no middleware, no workflows, no
prefork pool. The macro covers task registration and periodics; everything else stays where it is.

## Testing

- **Wire vectors.** Every `encode` case in `contracts/wire-vectors.json` driven through
  `call(...)`, byte-exact, and the `decode_only` / `round_trip_only` cases through the new decoder.
  **The #900 trap applies verbatim**: the vectors live above `crates/flexiq`, so this goes in its
  own `tests/` target with a matching `exclude` in the manifest, or the packaged crate ships a test
  it cannot build. `cargo package`'s verify pass builds lib and bin targets only and would not catch
  it.
- **Macro rejections** via `trybuild`: a bad `on_excess` spelling, an unparseable duration, a task
  whose argument is not `Serialize`.
- **End to end** on `SqliteStorage::in_memory`: enqueue → run → result; a step memoized across a
  forced retry; a sleep that ends the attempt and leaves `retry_count` alone; a periodic that fires.

## Publishing and CI

`flexiq-macros` is a fifth published crate, so: workspace `members`, a `[workspace.dependencies]`
entry with `path` + `version`, a `MIRRORS` entry in `scripts/version.mjs` (a path dep with no version
cannot be packaged, and `--check` gates the literal), `CRATES` and `PACKAGES` in
`publish-crates.yml`, and the `publish-readiness` set in `ci-rust.yml` — the crates are verified as a
set, because verifying one alone tries to resolve its siblings from the registry.

`syn`, `quote` and `proc-macro2` are already in `Cargo.lock` transitively. MSRV stays 1.88.
`#![deny(missing_docs)]` is already on `crates/flexiq`, and the rustdoc coverage gate runs per PR.

## Sequencing

Two stacked branches, both under #830.

1. **This one.** The two crates, tests, publish and CI wiring, crate README, full rustdoc.
2. **Docs.** `"rust"` joins `SDK_IDS` and the site's parity gates come with it. This cannot land
   partially: `docs/scripts/parity/checks/code-tabs.mjs` requires every `<CodeTabs>` block in the
   shared tree to carry a `<Tab>` per SDK with no grandfathering — **160 blocks across 34 files** —
   and `section-shape.mjs` then demands a full tree against `SECTION_SKELETON`. With the content
   tree (~78 pages, comparable to Java's 78 / Node's 80) and a new `scripts/api/extract/rust.mjs`
   plus its `SOURCES` entry, it is roughly twice the size of the crate work and has no green
   intermediate state.

## Follow-ups to file

- A remote producer handle behind the `remote` feature, under #835.
- `flexiq::worker::TaskHandler` is missing from core's root re-export list (`lib.rs:80-87`) while
  every sibling type is there.
- `NewPeriodicTask.kwargs` is written by no shell and read by no scheduler
  (`scheduler/maintenance.rs:310` builds the payload from `args` alone).
- `register_periodic` diverges across backends: Postgres's upsert preserves `last_run`, SQLite's
  `REPLACE INTO` and Redis's explicit `None` both reset it on every re-registration. Not covered by
  the shared contract suite.
