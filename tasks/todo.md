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

- [ ] 1. Shell error types — `Abort`, `Outcome`, the contract's task-error JSON
- [ ] 2. Encode arguments through core's wire writer (+ the #900 packaging trap)
- [ ] 3. Decode call envelopes into typed arguments (`ciborium`)
- [ ] 4. `Task`, `TaskCall`, `EnqueueOptions`
- [ ] 5. The `FlexiQ` handle (+ `ensure_contract_supported` at open)
- [ ] 6. `flexiq-macros` and `#[flexiq::task]` (+ publish wiring)
- [ ] 7. `WorkerBuilder` and the shell's fence-carrying dispatcher
- [ ] 8. Durable steps, fenced on `(owner, attempt, epoch)`
- [ ] 9. Periodic tasks
- [ ] 10. README, rustdoc, example, full gate

## Not in this branch

Docs site tier — branch 2. Adding `"rust"` to `SDK_IDS` turns `check:parity` red until ~78 pages
and 160 `<Tab sdk="rust">` panels all exist, so it has no green intermediate state and cannot ride
along.

Polyglot example worker — dropped from scope.

## Review

_Filled in at Task 10._
