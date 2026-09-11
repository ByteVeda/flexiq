# #830 — a Rust client shell over flexiq-core

Branch `feat/rust-sdk-shell` off `master` at `9f3bdef4`. Branch 1 of two.

**Spec:** [`tasks/specs/2026-09-10-rust-sdk-shell-design.md`](specs/2026-09-10-rust-sdk-shell-design.md)
**Plan:** [`tasks/plans/2026-09-11-rust-sdk-shell.md`](plans/2026-09-11-rust-sdk-shell.md)

## The decision the issue asked for first

Embedded, then remote — and **not** behind one API. Cargo features are additive, so
`embedded`/`remote` cannot be a switch; and the two doors disagree in the signature, not the
implementation (namespace, batch shape, payload-on-read, error vocabulary). What is shared is the
*call*, not the handle. Embedded goes first because `REMOTE_SDK_CONTRACT.md`'s delta table records
task registration and durable steps as absent from the remote tier **by decision**, so the macro
and the worker this issue asks for cannot be built over the producer door at all.

## Tasks

- [x] 1. Shell error types — `Abort`, `Outcome`, the contract's task-error JSON
- [x] 2. Encode arguments through core's wire writer (+ the #900 packaging trap)
- [x] 3. Decode call envelopes into typed arguments (`ciborium`)
- [x] 4. `Task`, `TaskCall`, `EnqueueOptions`
- [x] 5. The `FlexiQ` handle (+ `ensure_contract_supported` at open)
- [x] 6. `flexiq-macros` and `#[flexiq::task]` (+ publish wiring)
- [x] 7. `WorkerBuilder` and the shell's fence-carrying dispatcher
- [x] 8. Durable steps, fenced on `(owner, attempt, epoch)`
- [x] 9. Periodic tasks
- [x] 10. README, rustdoc, example, full gate

## Not in this branch

Docs site tier — branch 2. Adding `"rust"` to `SDK_IDS` turns `check:parity` red until ~78 pages
and 160 `<Tab sdk="rust">` panels all exist, so it has no green intermediate state and cannot ride
along.

Polyglot example worker — dropped from scope.

## Review

**What shipped.** Two crates. `crates/flexiq` stops being a pure re-export and becomes the SDK —
additively, so nothing published at 2.0.0 changes meaning and the glob re-export of the engine
stays as the escape hatch. `crates/flexiq-macros` is a new published crate holding `#[task]` and
nothing else, because a proc-macro crate can export nothing else.

**Decisions worth remembering.**

- **Embedded first, remote as a separate handle.** "One API behind a feature flag" cannot be built
  as the issue describes it: Cargo features are additive, so both can be on at once, and a flag can
  only decide whether tonic compiles in — never which API a caller gets. What the two doors share
  is `TaskCall`, not the handle.
- **A shell module must not be named after a core one.** `mod error;` shadows the glob re-export of
  `flexiq_core::error`, silently breaking `flexiq::error::QueueError` for anyone already using it.
  Rustc reports it as `hidden_glob_reexports`, a *warning*. Hence `outcome`, `steps`, `cron`,
  `pool` — and `WorkerBuilder`, never `Worker`.
- **Encoding goes through core; decoding could not.** `wire::encode_call` is the pinned writer and
  the `encode` vectors assert it byte for byte. There is no reader in the tree — `wire/cbor.rs`
  says so outright — so decoding uses `ciborium`, the one new third-party dependency, in this crate
  rather than in the engine.
- **The shell needs its own pool, and that is the whole reason it has one.** `NativeDispatcher`
  overrides neither `set_claim_owner` nor `set_lease_book`; both are default no-ops, so the
  `(owner, attempt, epoch)` the scheduler mints is dropped before a handler could see it. A step is
  written under that fence. `ShellDispatcher` keeps all three, and calls `StorageSteps::with_epoch`
  — which **no other shell does**, each fencing on `(owner, attempt)` alone because each reaches
  steps through an FFI class that has to be one concrete non-generic type.
- **A failed job records the contract's JSON.** `NativeDispatcher` stores `TaskError.message` raw;
  `BINDING_CONTRACT.md` specifies `{errtype,message,traceback}`, which is what every other shell
  reads.
- **`FlexiQ::open` calls `ensure_contract_supported`.** Required of every shell at storage open, and
  done by neither `Worker::spawn` nor the core examples.
- **Two things the type system does that the other shells do at runtime.** A debounce window is one
  value, so a partial window does not compile — where Python refuses it with a message, because an
  absent `max_wait_ms` is an unbounded debounce that starves the job. And a `cron` task with
  parameters is a compile error, because `check_periodic` builds the payload from the stored `args`
  alone and a parameter would decode as missing on every fire, forever.
- **A task is named after its function.** Not `module_path!()`: that embeds the crate name, so
  renaming a binary would change a name a producer in another language has to type.

**Two places the plan was wrong, corrected against the code.**

- The plan asked for a test asserting the committed step row carries an epoch. `JobStep` has no
  epoch column — `NewJobStep`'s own doc says owner and attempt "are deliberately *not* here: they
  fence the write". Replaced with a unit test of `ShellDispatcher::fence()` over a hand-built
  `LeaseBook`, which asserts all three terms survive.
- A completed job correctly has **zero** step rows: archival deletes them in the same transaction
  that archives the job, because a memo is execution state with no value past the job's end. The
  first version of that test read them afterwards and failed for a good reason; it now reads
  mid-run, after a `sleep_ms` has ended the attempt, and covers both step kinds.

**Verified.** 63 tests in the two crates — 18 wire-vector (every `encode` case byte-exact, every
`decode_only` case round-tripped), 18 queue, 7 worker, 7 dispatcher/options unit, 4 steps, 4
periodic, 5 compile-fail cases, plus doctests. `cargo test --workspace`; clippy `--all-targets`
with `-D warnings` on both crates; `cargo check --workspace` under `postgres` and under `redis`;
the rustdoc gate; `node scripts/version.mjs --check`; `cargo package` for both crates, with the
packaged `flexiq` tarball confirmed to exclude `tests/wire_vectors.rs` and still build `--tests`
(the #900 trap). `cargo run --example quickstart` runs and exits 0.

**Follow-ups to file.**

1. A remote producer handle behind a `remote` feature, under epic #835.
2. The docs site's `rust` tier — branch 2, sized like #780 was for the other three.
3. `flexiq::worker::TaskHandler` and `DebounceOptions` are both missing from core's root re-export
   list while every sibling type is there.
4. `NewPeriodicTask.kwargs` is written by no shell and read by no scheduler.
5. `register_periodic` diverges across backends on `last_run`: Postgres's upsert preserves it,
   SQLite's `REPLACE INTO` and Redis's explicit `None` both reset it. Not covered by the shared
   contract suite.
6. Async task bodies. The macro refuses them by name; the pool runs every handler on
   `spawn_blocking`, and core's `register_async` path is the seam if they are wanted.
