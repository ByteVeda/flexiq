# Six more records missing from the core root re-export (#948)

Follow-up to #921/#949, which put `DebounceOptions` and `TaskHandler` on
`crates/flexiq-core/src/lib.rs`'s root list and said every other record was
already there. Six were not: `SubscriptionMode`, `Topic`, `TopicMessage`,
`TopicLogStats`, `WorkerRegistration`, `WorkerStatus` — each named through
`flexiq_core::storage::records::` by a binding crate.

- [x] 1. Add the six to the `pub use storage::records::{…}` block
- [x] 2. Convert the call sites in `flexiq-java`, `flexiq-node`, `flexiq-python`
      and `flexiq-server` to the root path, including the grouped import in
      `flexiq-java/src/convert.rs` that #921 could not move
- [x] 3. Guard the list with a test so it cannot drift a third time
- [x] 4. `cargo fmt --check`, `clippy --all-targets --all-features -D warnings`,
      `cargo test --workspace`

## Review

All 25 public types in `storage::records` are now on the root, so they are
`flexiq_core::X` and — through `flexiq`'s `pub use flexiq_core::*` — `flexiq::X`
too. Purely additive: nothing was removed from `storage::records`, so every
existing module path still resolves.

The list had been declared complete twice and was wrong both times, because
nothing compares it against `records.rs`. `tests/rust/root_reexport_tests.rs`
now reads both files as text and diffs the two sets. It was proven to fail by
dropping `TopicMessage` from the block:

    records missing from the `pub use storage::records::{…}` block in
    crates/flexiq-core/src/lib.rs: ["TopicMessage"]

Column-0 matching only, which keeps `records.rs`'s own `#[cfg(test)]` module out
without parsing Rust. It catches an omission, not a rename — a stale name in the
group fails to compile on its own.

Core's internal `crate::storage::records::` uses and its own test files were left
alone: the root re-export is the path for *consumers*, and inside the crate the
module path is the direct one.
