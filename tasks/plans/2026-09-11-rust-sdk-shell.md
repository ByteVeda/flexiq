# Rust SDK shell — implementation plan (branch 1)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give Rust an ergonomic embedded SDK — an attribute macro for task registration, a queue handle, and a worker that runs registered tasks — on top of `flexiq-core`.

**Architecture:** `crates/flexiq` is today a pure re-export facade; it gains the shell additively, so nothing published at 2.0.0 changes meaning. A new `crates/flexiq-macros` provides `#[flexiq::task]`. The shell brings its own `WorkerDispatcher` because the built-in `NativeDispatcher` cannot carry the `(owner, attempt, epoch)` fence that durable steps need.

**Tech Stack:** Rust 2021, MSRV 1.88. `syn`/`quote`/`proc-macro2` (already transitively in `Cargo.lock`), `ciborium` (the one new third-party dependency), `serde`, `trybuild` (dev-only).

**Spec:** [`tasks/specs/2026-09-10-rust-sdk-shell-design.md`](../specs/2026-09-10-rust-sdk-shell-design.md)

## Global Constraints

- **Build with `-j2`.** This machine has 13 GB RAM. `cargo test -j2`, `cargo build -j2`, and prefix `CARGO_BUILD_JOBS=2` on any `cargo clippy --all-targets --all-features`.
- **MSRV is 1.88**, set in `[workspace.package]` of the root `Cargo.toml`. Nothing may require newer.
- **`#![deny(missing_docs)]` is already on `crates/flexiq`** and must go on `crates/flexiq-macros`. Every public item needs a doc comment or the build fails. The rustdoc coverage gate runs per PR.
- **No AI attribution** in commit messages. No `Co-Authored-By`, no mention of Claude or Anthropic. Conventional prefixes (`feat:`, `fix:`, `test:`, `chore:`, `docs:`).
- **Commit subjects ≤ 60 chars, imperative.** Body only when the *why* is not obvious from the diff. No `@` anywhere in a subject — a bare `@name` becomes a GitHub mention in release notes.
- **Never hand-edit a version literal.** Run `node scripts/version.mjs --set X.Y.Z`; `--check` gates CI.
- **Error strings are an interface.** Before rewording any user-visible message, `grep -rn` for `match=` / `toThrow` / `assertThrows` across every SDK.
- **Task names in tests use the shell's own default** (bare function name), never a module path.
- **Feature combinations that must stay green:** default, `--features postgres`, `--features redis`.
- **Pre-commit clippy is workspace-wide with `-D warnings`, so an unused helper fails the commit.** Building bottom-up means a `pub(crate)` item often lands one task before its only consumer, and `dead_code` is an error there, not a warning. Either land the helper with its consumer, or carry `#[allow(dead_code)]` with a comment naming the task that removes it — and make the removal an explicit step in that task.
- **A shell module must not be named after a core one.** `crates/flexiq/src/lib.rs` re-exports `flexiq_core::*` with a glob, and a private `mod x;` of the same name **shadows** it — `flexiq::error::QueueError` would silently stop resolving, which is a breaking change to a published crate. Rustc catches it as `hidden_glob_reexports`, a warning, not an error. Core owns `contract, error, job, lease, periodic, pubsub, resilience, scheduler, settings, step, storage, wire, worker`; the shell uses `outcome, steps, cron, pool, task, call, options, queue, encode, decode`. The same rule already governs type names — hence `WorkerBuilder`, never `Worker`.

---

### Task 1: Shell error types — `Abort`, `Outcome`, and the contract's task-error JSON

**Files:**
- Create: `crates/flexiq/src/outcome.rs`
- Modify: `crates/flexiq/src/lib.rs`
- Test: inline `#[cfg(test)] mod tests` in `crates/flexiq/src/outcome.rs`

**Interfaces:**
- Consumes: `flexiq_core::{TaskError, StepSleep}`.
- Produces: `Abort` (enum, arms `Fail(TaskError)` and `Sleep(StepSleep)`), `Outcome<T> = Result<T, Abort>`, `impl From<TaskError> for Abort`, and `pub(crate) fn task_error_json(err: &TaskError) -> String`.

`BINDING_CONTRACT.md:349-377` specifies the canonical failure shape as JSON `{"errtype","message","traceback"}`. `NativeDispatcher` does not produce it (`worker/dispatcher.rs:52-63` stores `task_error.message` raw); the shell does, so a Python or Node reader gets a structured error off a Rust task.

- [ ] **Step 1: Write the failing test**

Add to `crates/flexiq/src/outcome.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_error_json_uses_the_contract_shape() {
        let err = TaskError::fatal("card declined");
        let encoded = task_error_json(&err);
        let parsed: serde_json::Value = serde_json::from_str(&encoded).expect("valid JSON");

        assert_eq!(parsed["errtype"], "TaskError");
        assert_eq!(parsed["message"], "card declined");
        assert_eq!(parsed["traceback"], serde_json::Value::Null);
    }

    #[test]
    fn a_task_error_converts_into_a_failing_abort() {
        let abort: Abort = TaskError::retryable("upstream 503").into();
        match abort {
            Abort::Fail(err) => {
                assert_eq!(err.message, "upstream 503");
                assert!(err.retryable);
            }
            Abort::Sleep(_) => panic!("expected Fail"),
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p flexiq -j2 error::tests`
Expected: FAIL — `crates/flexiq/src/outcome.rs` does not exist, so the module cannot be found.

- [ ] **Step 3: Write minimal implementation**

Create `crates/flexiq/src/outcome.rs`:

```rust
//! What a task body can end with, and the failure shape the wire expects.

use flexiq_core::{StepSleep, TaskError};

/// How a task body ended when it did not return a value.
///
/// Two arms rather than one, because a `step.sleep` is not a failure: the job
/// is already rescheduled and unclaimed by the time the body unwinds, and
/// reporting it as an error would burn a retry for work that has not gone
/// wrong. The dispatcher maps each arm to a different [`flexiq_core::JobResult`].
#[derive(Debug)]
pub enum Abort {
    /// The task failed. The scheduler applies the retry policy when
    /// [`TaskError::retryable`] built it.
    Fail(TaskError),
    /// The task called `step.sleep` and this attempt is over.
    Sleep(StepSleep),
}

/// What a `#[flexiq::task]` function returns.
///
/// A caller's own error reaches [`Abort`] through [`TaskError`], which carries
/// the retryable/fatal distinction the scheduler acts on. There is deliberately
/// no blanket `From<E: std::error::Error>`: it would collide with
/// `From<TaskError>`, and it would have to guess retryability — the one bit of
/// a failure only the task author knows. End a fallible call with
/// `.map_err(TaskError::retryable)?` or `.map_err(TaskError::fatal)?`.
pub type Outcome<T> = std::result::Result<T, Abort>;

impl From<TaskError> for Abort {
    fn from(err: TaskError) -> Self {
        Abort::Fail(err)
    }
}

/// The canonical JSON a failed job records, per `BINDING_CONTRACT.md`.
///
/// Rust has no exception type to name and no stack trace to attach, so
/// `errtype` is the constant `"TaskError"` and `traceback` is null. Both fields
/// are still written: a reader in another language matches on their presence,
/// and omitting them would make a Rust failure the one shape that needs a
/// special case.
pub(crate) fn task_error_json(err: &TaskError) -> String {
    serde_json::json!({
        "errtype": "TaskError",
        "message": err.message,
        "traceback": serde_json::Value::Null,
    })
    .to_string()
}
```

- [ ] **Step 4: Wire the module and its dependencies**

In `crates/flexiq/src/lib.rs`, after the existing `pub use flexiq_core;` line:

```rust
mod error;

pub use error::{Abort, Outcome};
```

In `crates/flexiq/Cargo.toml`, add to `[dependencies]`:

```toml
serde_json = { workspace = true }
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test -p flexiq -j2 error::tests`
Expected: PASS, 2 tests.

- [ ] **Step 6: Commit**

```bash
git add crates/flexiq/src/outcome.rs crates/flexiq/src/lib.rs crates/flexiq/Cargo.toml
git commit -m "feat: shell error types and the contract's failure JSON"
```

---

### Task 2: Encoding arguments — `serde::Serialize` into `WireValue`

**Files:**
- Create: `crates/flexiq/src/encode.rs`
- Create: `crates/flexiq/tests/wire_vectors.rs`
- Modify: `crates/flexiq/src/lib.rs`, `crates/flexiq/Cargo.toml`

**Interfaces:**
- Consumes: `flexiq_core::wire::{encode_call, WireValue}`.
- Produces: `pub(crate) fn to_wire<T: serde::Serialize>(value: &T) -> Result<WireValue, Error>` and `pub(crate) fn encode_args(args: &[WireValue]) -> Vec<u8>`.

The shell converts values to `WireValue` and hands them to core's writer. It must **not** encode CBOR itself — `wire/mod.rs:13` states this is "the Rust implementation of that one format, not a second definition of it", and a second encoder that merely agrees today is how `auto:` idempotency keys quietly stop deduping.

**The #900 trap applies here.** `contracts/wire-vectors.json` lives above `crates/flexiq`, so an `include_str!` reaching for it makes the packaged crate ship a test it cannot build. `cargo package`'s verify pass builds lib and bin targets only and will not catch it. The test therefore gets its own target plus an `exclude` in the manifest, exactly as `flexiq-core` does.

- [ ] **Step 1: Confirm the precedent before copying it**

Run: `grep -n "exclude" crates/flexiq-core/Cargo.toml`
Expected: an `exclude` entry naming `tests/wire_vectors.rs`. Read it and mirror the shape.

- [ ] **Step 2: Write the failing test**

Create `crates/flexiq/tests/wire_vectors.rs`:

```rust
//! The shell's encoder against the pinned cross-SDK vectors.
//!
//! Excluded from the published crate: the vectors live above this crate's root,
//! so a packaged copy could not read them. See `Cargo.toml`'s `exclude`.

use serde::Serialize;

#[derive(Serialize)]
struct Order {
    order_id: String,
    amount_cents: i64,
}

/// `contracts/wire-vectors.json`, case `positional-args`: `f(1, "a")`.
#[test]
fn positional_args_match_the_pinned_bytes() {
    let payload = flexiq::testing::encode_call_for_test(&(1_i64, "a"));
    assert_eq!(hex::encode(payload), "028282016161a0");
}

/// Case `no-args`: `f()`.
#[test]
fn no_args_match_the_pinned_bytes() {
    let payload = flexiq::testing::encode_call_for_test(&());
    assert_eq!(hex::encode(payload), "028280a0");
}

/// Case `single-object-arg`: field order is declaration order, not sorted.
/// `order_id` precedes `amount_cents` because the struct declares it first.
#[test]
fn a_struct_argument_keeps_declaration_order() {
    let order = Order {
        order_id: "ord_1".into(),
        amount_cents: 4200,
    };
    let payload = flexiq::testing::encode_call_for_test(&(order,));
    let hex = hex::encode(payload);
    let order_id_at = hex.find(&hex::encode("order_id")).expect("order_id present");
    let amount_at = hex.find(&hex::encode("amount_cents")).expect("amount present");
    assert!(
        order_id_at < amount_at,
        "declaration order must survive encoding"
    );
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p flexiq -j2 --test wire_vectors`
Expected: FAIL to compile — `flexiq::testing` does not exist.

- [ ] **Step 4: Write the serializer**

Create `crates/flexiq/src/encode.rs`. It implements `serde::Serializer` producing a `WireValue`. Key rules, each of which the vectors enforce:

- A map or struct becomes `WireValue::Map` with entries **in the order serde yields them**, never sorted — `WireValue`'s own doc says the caller decides order and the encoder never reorders, because `auto:` keys hash the bytes.
- Integers become `WireValue::Integer(i64)`. A `u64` above `i64::MAX` is an error, not a silent wrap.
- `f32` widens to `f64`; core always writes 64-bit floats.
- `None` and unit become `WireValue::Null`.
- A newtype struct is transparent; a tuple or seq becomes `WireValue::Array`.
- A non-string map key is an error — the envelope's maps are text-keyed.

```rust
//! Turning a `Serialize` value into the [`WireValue`] tree core's writer walks.

use flexiq_core::wire::{encode_call, WireValue};
use serde::{ser, Serialize};

/// A value that cannot be represented in the call envelope.
#[derive(Debug, thiserror::Error)]
#[error("cannot encode task argument: {0}")]
pub struct EncodeError(String);

impl ser::Error for EncodeError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        EncodeError(msg.to_string())
    }
}

/// Convert one value into a [`WireValue`].
pub(crate) fn to_wire<T: Serialize + ?Sized>(value: &T) -> Result<WireValue, EncodeError> {
    value.serialize(WireSerializer)
}

/// Encode a positional argument list as a call envelope.
///
/// `kwargs` is always the empty map: Rust has no keyword arguments, the same
/// positional-only asymmetry `BINDING_CONTRACT.md` records for Node.
pub(crate) fn encode_args(args: &[WireValue]) -> Vec<u8> {
    encode_call(args, &[])
}

struct WireSerializer;
```

Then write `impl ser::Serializer for WireSerializer` with `type Ok = WireValue` and `type Error = EncodeError`, covering every required method:

| Method(s) | Returns |
|---|---|
| `serialize_bool` | `WireValue::Bool` |
| `serialize_i8/i16/i32/i64`, `serialize_u8/u16/u32` | `WireValue::Integer` |
| `serialize_u64` | `WireValue::Integer` when `<= i64::MAX`, else `Err` — never a silent wrap |
| `serialize_f32/f64` | `WireValue::Float`, widening `f32` (core always writes 64-bit) |
| `serialize_char`, `serialize_str` | `WireValue::Text` |
| `serialize_bytes` | `WireValue::Bytes` |
| `serialize_none`, `serialize_unit`, `serialize_unit_struct` | `WireValue::Null` |
| `serialize_some`, `serialize_newtype_struct` | transparent — recurse into the inner value |
| `serialize_unit_variant` | `WireValue::Text` of the variant name |
| `serialize_newtype_variant` | single-entry `Map` keyed by the variant name |
| `serialize_seq`, `serialize_tuple`, `serialize_tuple_struct` | `SerializeSeq`-style collector finishing into `WireValue::Array` |
| `serialize_map`, `serialize_struct` | collector finishing into `WireValue::Map`, **preserving insertion order** |
| `serialize_tuple_variant`, `serialize_struct_variant` | single-entry `Map` keyed by the variant name, value built as above |

Five companion structs implement `SerializeSeq`/`SerializeTuple`/`SerializeTupleStruct`, `SerializeMap`/`SerializeStruct`, and the two variant forms; each pushes into a `Vec` and `end()`s into the matching arm.

Two rules the vectors enforce and a generic value-serializer would get wrong:

- **Never sort map keys.** `WireValue`'s own doc says the caller decides order and the encoder never reorders, because the `auto:` key hashes the bytes. The `single-object-arg` vector pins `order_id` before `amount_cents`, which is not sorted order.
- **A non-string map key is an error**, not a coerced one — the envelope's maps are text-keyed. Return `EncodeError` naming the offending type.

- [ ] **Step 5: Add the test seam and dependencies**

In `crates/flexiq/src/lib.rs`:

```rust
mod encode;

/// Helpers the crate's own integration tests reach for. Not a stable API.
#[doc(hidden)]
pub mod testing {
    /// Encode a tuple of arguments exactly as `TaskCall` will.
    pub fn encode_call_for_test<T: serde::Serialize>(args: &T) -> Vec<u8> {
        let wire = crate::encode::to_wire(args).expect("encodable");
        match wire {
            flexiq_core::wire::WireValue::Array(items) => crate::encode::encode_args(&items),
            other => crate::encode::encode_args(&[other]),
        }
    }
}
```

In `crates/flexiq/Cargo.toml`:

```toml
[dependencies]
serde = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
hex = "0.4"

exclude = ["tests/wire_vectors.rs"]
```

`exclude` belongs under `[package]`, not `[dependencies]` — place it beside the existing package metadata.

- [ ] **Step 6: Run test to verify it passes**

Run: `cargo test -p flexiq -j2 --test wire_vectors`
Expected: PASS, 3 tests.

- [ ] **Step 7: Prove the packaged crate still builds its tests**

```bash
cargo package -p flexiq --no-verify --allow-dirty
```
Then extract the `.crate` from `target/package/`, append `[workspace]` to its `Cargo.toml`, and run `cargo build --tests --offline` inside it. Expected: succeeds, and the extracted tree contains no `tests/wire_vectors.rs`. This is the reproduction recipe #900 left behind.

- [ ] **Step 8: Commit**

```bash
git add crates/flexiq/src/encode.rs crates/flexiq/src/lib.rs crates/flexiq/Cargo.toml crates/flexiq/tests/wire_vectors.rs
git commit -m "feat: encode task arguments through the core wire writer"
```

---

### Task 3: Decoding arguments — payload bytes into typed parameters

**Files:**
- Create: `crates/flexiq/src/decode.rs`
- Modify: `crates/flexiq/src/lib.rs`, `crates/flexiq/Cargo.toml`, `crates/flexiq/tests/wire_vectors.rs`

**Interfaces:**
- Consumes: `flexiq_core::wire::TAG_CBOR`.
- Produces: `pub(crate) fn decode_args<T: serde::de::DeserializeOwned>(payload: &[u8]) -> Result<T, DecodeError>`, which strips the tag byte, reads the 2-element `[args, kwargs]` array, and deserializes `args` into `T`.

There is no reader in core to reuse — `wire/cbor.rs:11`: "There is no reader. Nothing in this crate decodes a payload." Reading has to accept everything any writer may legally emit (indefinite lengths, either float width, integers beyond 2^53), which is why this uses `ciborium` rather than a hand-rolled mirror of the writer.

- [ ] **Step 1: Write the failing test**

Append to `crates/flexiq/tests/wire_vectors.rs`:

```rust
#[test]
fn a_positional_call_round_trips() {
    let payload = flexiq::testing::encode_call_for_test(&(1_i64, "a".to_string()));
    let (n, s): (i64, String) =
        flexiq::testing::decode_args_for_test(&payload).expect("decodes");
    assert_eq!(n, 1);
    assert_eq!(s, "a");
}

#[test]
fn a_payload_with_an_unknown_tag_is_refused() {
    let err = flexiq::testing::decode_args_for_test::<(i64,)>(&[0x7f, 0x00])
        .expect_err("unknown tag must not decode");
    assert!(
        err.to_string().contains("codec"),
        "message should name the codec: {err}"
    );
}

#[test]
fn a_float_decodes_at_either_width() {
    // `wire-vectors.json` leaves float width free for a writer, so a reader
    // must take a half, single or double. 0xf93e00 is half-precision 1.5.
    let payload = [0x02, 0x82, 0x81, 0xf9, 0x3e, 0x00, 0xa0];
    let (f,): (f64,) = flexiq::testing::decode_args_for_test(&payload).expect("decodes");
    assert_eq!(f, 1.5);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p flexiq -j2 --test wire_vectors`
Expected: FAIL to compile — `decode_args_for_test` does not exist.

- [ ] **Step 3: Write minimal implementation**

Create `crates/flexiq/src/decode.rs`:

```rust
//! Reading a call envelope back into typed arguments.

use flexiq_core::wire::TAG_CBOR;

/// A payload that is not a call this shell can run.
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    /// The leading tag byte named a codec this shell does not read.
    #[error("unsupported payload codec: tag 0x{0:02x}")]
    Codec(u8),
    /// The payload was empty, so it carries no tag at all.
    #[error("empty payload: no codec tag")]
    Empty,
    /// The body was not a two-element `[args, kwargs]` array.
    #[error("malformed call envelope: {0}")]
    Envelope(String),
    /// The arguments did not match the task's parameter types.
    #[error("task arguments do not match the handler: {0}")]
    Arguments(String),
}

/// Strip the tag, read `[args, kwargs]`, deserialize `args` into `T`.
///
/// `kwargs` is read and discarded rather than refused when non-empty: a
/// producer in a language that has keyword arguments may legally send them, and
/// a Rust handler that has no parameter for one should fail on the argument
/// tuple's shape rather than on the map's presence.
pub(crate) fn decode_args<T: serde::de::DeserializeOwned>(
    payload: &[u8],
) -> Result<T, DecodeError> {
    let (tag, body) = payload.split_first().ok_or(DecodeError::Empty)?;
    if *tag != TAG_CBOR {
        return Err(DecodeError::Codec(*tag));
    }

    let call: ciborium::Value = ciborium::from_reader(body)
        .map_err(|e| DecodeError::Envelope(e.to_string()))?;
    let mut items = match call {
        ciborium::Value::Array(items) if items.len() == 2 => items,
        other => {
            return Err(DecodeError::Envelope(format!(
                "expected a 2-element array, found {other:?}"
            )))
        }
    };
    let args = items.remove(0);
    args.deserialized()
        .map_err(|e| DecodeError::Arguments(e.to_string()))
}
```

- [ ] **Step 4: Wire the module, the test seam and the dependency**

In `crates/flexiq/src/lib.rs` add `mod decode;`, and inside `pub mod testing`:

```rust
    /// Decode a call envelope exactly as the dispatcher will.
    pub fn decode_args_for_test<T: serde::de::DeserializeOwned>(
        payload: &[u8],
    ) -> Result<T, crate::decode::DecodeError> {
        crate::decode::decode_args(payload)
    }
```

`DecodeError` must be `pub` for that signature to compile — export it from `lib.rs` as `pub use decode::DecodeError;`.

In `crates/flexiq/Cargo.toml` `[dependencies]`:

```toml
ciborium = "0.2"
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test -p flexiq -j2 --test wire_vectors`
Expected: PASS, 6 tests.

- [ ] **Step 6: Commit**

```bash
git add crates/flexiq/src/decode.rs crates/flexiq/src/lib.rs crates/flexiq/Cargo.toml crates/flexiq/tests/wire_vectors.rs
git commit -m "feat: decode call envelopes into typed arguments"
```

---

### Task 4: `Task`, `TaskCall` and `EnqueueOptions`

**Files:**
- Create: `crates/flexiq/src/task.rs`, `crates/flexiq/src/call.rs`, `crates/flexiq/src/options.rs`, `crates/flexiq/src/steps.rs` (stub only — Task 8 fills it)
- Modify: `crates/flexiq/src/lib.rs`

**Interfaces:**
- Consumes: Task 2's `encode_args`, Task 3's `decode_args`, `flexiq_core::{NewJob, TaskConfig, DebounceOptions, now_millis}`.
- Produces:
  - `pub trait Task: Send + Sync + 'static { const NAME: &'static str; fn config() -> TaskConfig; fn defaults() -> EnqueueOptions; fn run_encoded(job: &Job, step: &mut StepHandle) -> Outcome<Option<Vec<u8>>>; }`
  - `pub struct TaskCall<T: Task> { payload: Vec<u8>, options: EnqueueOptions, _task: PhantomData<T> }` with `priority`, `delay`, `queue`, `max_retries`, `timeout`, `unique_key`, `idempotent`, `metadata`, `notes`, `depends_on`, `expires`, `result_ttl`, `namespace`, `debounce_ms(key, window_ms)` builder methods, each `mut self -> Self`.
  - `pub struct EnqueueOptions { .. }` with `pub(crate) fn into_new_job(self, task_name: &str, payload: Vec<u8>) -> NewJob`.

`StepHandle` is defined in Task 8; until then `run_encoded` takes it as an opaque parameter the macro passes through. Define `StepHandle` as an empty struct in `task.rs` now and grow it in Task 8, so the trait signature never changes.

`EnqueueOptions` covers every knob the cross-SDK table marks present in all three shells. `into_new_job` fills all 15 `NewJob` fields — core provides no `Default` and no builder, which is the ergonomic gap this task closes.

- [ ] **Step 1: Write the failing test**

Add to `crates/flexiq/src/options.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_fill_every_new_job_field() {
        let job = EnqueueOptions::default().into_new_job("charge", vec![0x02]);

        assert_eq!(job.queue, "default");
        assert_eq!(job.task_name, "charge");
        assert_eq!(job.payload, vec![0x02]);
        assert_eq!(job.priority, 0);
        assert_eq!(job.max_retries, 3);
        assert_eq!(job.timeout_ms, 300_000);
        assert!(job.unique_key.is_none());
        assert!(job.depends_on.is_empty());
        assert!(job.namespace.is_none());
    }

    #[test]
    fn a_delay_moves_scheduled_at_forward() {
        let before = flexiq_core::now_millis();
        let job = EnqueueOptions::default()
            .with_delay_ms(5_000)
            .into_new_job("charge", Vec::new());
        assert!(job.scheduled_at >= before + 5_000);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p flexiq -j2 options::tests`
Expected: FAIL — module does not exist.

- [ ] **Step 3: Write `options.rs`**

```rust
//! Per-enqueue options, and the one place they become a [`NewJob`].

use flexiq_core::{now_millis, DebounceOptions, NewJob};

/// Everything a caller may set on a single enqueue.
///
/// Core's [`NewJob`] has fifteen public fields, no `Default` and no builder, so
/// every caller today hand-writes all fifteen. This is that builder, with the
/// defaults the other three shells already agree on.
#[derive(Debug, Clone)]
pub struct EnqueueOptions {
    pub(crate) queue: String,
    pub(crate) priority: i32,
    pub(crate) delay_ms: Option<i64>,
    pub(crate) max_retries: i32,
    pub(crate) timeout_ms: i64,
    pub(crate) unique_key: Option<String>,
    pub(crate) idempotent: bool,
    pub(crate) metadata: Option<String>,
    pub(crate) notes: Option<String>,
    pub(crate) depends_on: Vec<String>,
    pub(crate) expires_in_ms: Option<i64>,
    pub(crate) result_ttl_ms: Option<i64>,
    pub(crate) namespace: Option<String>,
    pub(crate) debounce: Option<(String, DebounceOptions)>,
}

impl Default for EnqueueOptions {
    /// Matches the cross-SDK defaults: queue `default`, priority 0, three
    /// retries, a five-minute timeout.
    fn default() -> Self {
        Self {
            queue: "default".into(),
            priority: 0,
            delay_ms: None,
            max_retries: 3,
            timeout_ms: 300_000,
            unique_key: None,
            idempotent: false,
            metadata: None,
            notes: None,
            depends_on: Vec::new(),
            expires_in_ms: None,
            result_ttl_ms: None,
            namespace: None,
            debounce: None,
        }
    }
}

impl EnqueueOptions {
    /// Schedule the job `ms` milliseconds from now.
    pub fn with_delay_ms(mut self, ms: i64) -> Self {
        self.delay_ms = Some(ms);
        self
    }

    /// Fill every [`NewJob`] field. Relative times resolve against one `now`,
    /// read once, so `scheduled_at` and `expires_at` cannot disagree.
    pub(crate) fn into_new_job(self, task_name: &str, payload: Vec<u8>) -> NewJob {
        let now = now_millis();
        NewJob {
            queue: self.queue,
            task_name: task_name.to_string(),
            payload,
            priority: self.priority,
            scheduled_at: now + self.delay_ms.unwrap_or(0),
            max_retries: self.max_retries,
            timeout_ms: self.timeout_ms,
            unique_key: self.unique_key,
            metadata: self.metadata,
            notes: self.notes,
            depends_on: self.depends_on,
            expires_at: self.expires_in_ms.map(|ms| now + ms),
            result_ttl_ms: self.result_ttl_ms,
            namespace: self.namespace,
            debounce_key: self.debounce.as_ref().map(|(key, _)| key.clone()),
        }
    }
}
```

Add the remaining `with_*` setters — one per field, each `mut self -> Self`, each with a doc comment.

- [ ] **Step 4: Write `task.rs` and `call.rs`**

`task.rs` defines the `Task` trait and a placeholder `StepHandle`:

```rust
//! What the attribute macro implements.

use flexiq_core::{Job, TaskConfig};

use crate::{EnqueueOptions, Outcome};

/// A registered task: its name, its policy, and how to run one encoded job.
///
/// Implemented by `#[flexiq::task]`. Implementing it by hand is supported and
/// is what the crate's own tests do — the macro is ergonomics, not plumbing.
pub trait Task: Send + Sync + 'static {
    /// The name a job carries and a producer in any language enqueues.
    const NAME: &'static str;

    /// Dispatch policy: retries, rate limits, breaker, concurrency caps.
    fn config() -> TaskConfig;

    /// Enqueue defaults this task was declared with.
    fn defaults() -> EnqueueOptions;

    /// Decode the job's payload, run the body, encode the result.
    fn run_encoded(job: &Job, step: &mut StepHandle) -> Outcome<Option<Vec<u8>>>;
}

/// The durable-step handle a running task holds. Grown in Task 8; it exists
/// now so [`Task::run_encoded`]'s signature never changes.
pub struct StepHandle {
    pub(crate) inner: Option<crate::steps::Session>,
}
```

`StepHandle` names `crate::steps::Session`, so `steps.rs` must exist for this to compile. Create it now as a stub — Task 8 replaces the body, not the name:

```rust
//! Durable steps for a running task. Filled in when steps land.

/// An open step session. A stub until the dispatcher can hand one over.
pub(crate) struct Session;
```

Add `mod steps;` to `lib.rs` alongside the others.

`call.rs` defines `TaskCall<T>`:

```rust
//! One encoded call to one task, before it is enqueued.

use std::marker::PhantomData;

use crate::{EnqueueOptions, Task};

/// A task name plus its encoded arguments, with the options to enqueue it.
///
/// Transport-free on purpose: this is the value both the embedded handle and a
/// future remote handle accept, which is the only thing worth unifying between
/// them.
pub struct TaskCall<T: Task> {
    pub(crate) payload: Vec<u8>,
    pub(crate) options: EnqueueOptions,
    pub(crate) _task: PhantomData<T>,
}

impl<T: Task> TaskCall<T> {
    /// Build a call from an already-encoded payload, seeded with the task's
    /// declared defaults.
    ///
    /// `pub`, not `pub(crate)`: `#[flexiq::task]` expands in the *caller's*
    /// crate, so everything the expansion touches has to be reachable from
    /// outside this one. Hidden from the docs because a hand-written `Task`
    /// impl is the only other caller.
    #[doc(hidden)]
    pub fn from_args(payload: Vec<u8>) -> Self {
        Self {
            payload,
            options: T::defaults(),
            _task: PhantomData,
        }
    }

    /// Run this job ahead of lower-priority ones.
    pub fn priority(mut self, priority: i32) -> Self {
        self.options.priority = priority;
        self
    }
}
```

Add one builder method per `EnqueueOptions` field, each delegating the same way.

- [ ] **Step 5: Export the new surface**

Everything a caller or a macro expansion names has to leave the crate. In `crates/flexiq/src/lib.rs`:

```rust
mod call;
mod options;
mod steps;
mod task;

pub use call::TaskCall;
pub use options::EnqueueOptions;
pub use task::{StepHandle, Task};
```

`Job`, `TaskConfig` and the rest already arrive through the existing `pub use flexiq_core::*`, so the expansion can spell `::flexiq::Job` without a second re-export.

- [ ] **Step 6: Run test to verify it passes**

Run: `cargo test -p flexiq -j2 options::tests`
Expected: PASS, 2 tests.

- [ ] **Step 7: Commit**

```bash
git add crates/flexiq/src/task.rs crates/flexiq/src/call.rs crates/flexiq/src/options.rs crates/flexiq/src/steps.rs crates/flexiq/src/lib.rs
git commit -m "feat: the Task trait, TaskCall and enqueue options"
```

---

### Task 5: The `FlexiQ` handle

**Files:**
- Create: `crates/flexiq/src/queue.rs`
- Create: `crates/flexiq/tests/queue.rs`
- Modify: `crates/flexiq/src/lib.rs`

**Interfaces:**
- Consumes: Task 4's `TaskCall`, `EnqueueOptions`.
- Produces: `pub struct FlexiQ` with `open(path) -> Result<Self>`, `in_memory() -> Result<Self>`, `from_storage(StorageBackend) -> Result<Self>`, `enqueue<T: Task>(&self, TaskCall<T>) -> Result<Job>`, `enqueue_batch<T: Task>(&self, Vec<TaskCall<T>>) -> Result<Vec<Job>>`, `cancel(&self, &str) -> Result<bool>`, `request_cancel(&self, &str) -> Result<bool>`, `get_job(&self, &str) -> Result<Option<Job>>`, `list_jobs(&self, JobFilter) -> Result<Vec<Job>>`, `stats(&self) -> Result<QueueStats>`, `storage(&self) -> &StorageBackend`, `namespace(&self) -> Option<&str>`.

`open` calls `ensure_contract_supported`, which `BINDING_CONTRACT.md:556-563` requires of every shell at storage open and which neither `Worker::spawn` nor the core examples do today.

Routing rule for `enqueue`: a call with `debounce` set goes to `enqueue_debounced`; one with `idempotent` or a `unique_key` goes to `enqueue_unique`; otherwise `enqueue`. `enqueue_batch` refuses a batch containing a debounced call — storage has no batched debounce, and looping it would cost the Diesel backends the atomicity that is the reason to send a batch. That refusal is the same one `flexiq-server` makes.

- [ ] **Step 1: Write the failing test**

Create `crates/flexiq/tests/queue.rs`:

```rust
use flexiq::{EnqueueOptions, FlexiQ, Outcome, StepHandle, Task, TaskCall};
use flexiq_core::{Job, TaskConfig};

/// A hand-written `Task`, standing in for the macro until Task 6 lands.
struct Greet;

impl Task for Greet {
    const NAME: &'static str = "greet";
    fn config() -> TaskConfig {
        TaskConfig::default()
    }
    fn defaults() -> EnqueueOptions {
        EnqueueOptions::default()
    }
    fn run_encoded(_job: &Job, _step: &mut StepHandle) -> Outcome<Option<Vec<u8>>> {
        Ok(None)
    }
}

fn greet(name: &str) -> TaskCall<Greet> {
    flexiq::testing::call_for_test::<Greet, _>(&(name,))
}

#[test]
fn an_enqueued_job_carries_the_task_name_and_a_tagged_payload() {
    let q = FlexiQ::in_memory().expect("opens");
    let job = q.enqueue(greet("world")).expect("enqueues");

    assert_eq!(job.task_name, "greet");
    assert_eq!(job.payload[0], 0x02, "payload must carry the CBOR tag");
    assert_eq!(job.queue, "default");
}

#[test]
fn a_unique_key_dedupes_a_second_enqueue() {
    let q = FlexiQ::in_memory().expect("opens");
    let first = q
        .enqueue(greet("world").unique_key("only-once"))
        .expect("enqueues");
    let second = q
        .enqueue(greet("world").unique_key("only-once"))
        .expect("enqueues");

    assert_eq!(first.id, second.id, "the second call must find the first job");
}

#[test]
fn a_batch_containing_a_debounced_call_is_refused() {
    let q = FlexiQ::in_memory().expect("opens");
    let err = q
        .enqueue_batch(vec![greet("a"), greet("b").debounce_ms("k", 500)])
        .expect_err("a debounced item must not ride a batch");
    assert!(err.to_string().contains("debounce"), "message: {err}");
}

#[test]
fn cancelling_a_pending_job_reports_true() {
    let q = FlexiQ::in_memory().expect("opens");
    let job = q.enqueue(greet("world")).expect("enqueues");
    assert!(q.cancel(&job.id).expect("cancels"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p flexiq -j2 --test queue`
Expected: FAIL to compile — `FlexiQ` does not exist.

- [ ] **Step 3: Write minimal implementation**

Create `crates/flexiq/src/queue.rs` with the handle. Sketch of the routing:

```rust
    /// Enqueue one call.
    pub fn enqueue<T: Task>(&self, call: TaskCall<T>) -> Result<Job> {
        let TaskCall { payload, mut options, .. } = call;
        if options.namespace.is_none() {
            options.namespace = self.namespace.clone();
        }
        let debounce = options.debounce.clone();
        let unique = options.idempotent || options.unique_key.is_some();
        if options.idempotent && options.unique_key.is_none() {
            options.unique_key = Some(auto_unique_key(T::NAME, &payload));
        }
        let new_job = options.into_new_job(T::NAME, payload);

        match (debounce, unique) {
            (Some((_, opts)), _) => self.storage.enqueue_debounced(new_job, opts),
            (None, true) => self.storage.enqueue_unique(new_job),
            (None, false) => self.storage.enqueue(new_job),
        }
    }
```

`auto_unique_key` reproduces the cross-SDK rule exactly, from `sdks/python/flexiq/app.py:508-509`:

```rust
/// `auto:` + the first 32 hex characters of
/// `sha256(task_name_utf8 || 0x00 || payload)`.
///
/// The separator is a **NUL byte**, not a printable one, and the digest is
/// truncated to 32 hex characters — both are wire-visible. A divergence here
/// does not fail a test, it silently stops `idempotent = true` from deduping
/// against the same call sent from another language.
fn auto_unique_key(task_name: &str, payload: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(task_name.as_bytes());
    hasher.update([0x00]);
    hasher.update(payload);
    format!("auto:{:.32}", hex::encode(hasher.finalize()))
}
```

`sha2` and `hex` become runtime dependencies of `crates/flexiq` for this. Add a test asserting one known `(name, payload)` pair against the digest Python produces for the same input — compute it once with `python3 -c` and paste the literal, so the assertion pins the rule rather than restating the implementation.

- [ ] **Step 4: Add `call_for_test` to the testing module**

```rust
    /// Build a `TaskCall` without the macro.
    pub fn call_for_test<T: crate::Task, A: serde::Serialize>(args: &A) -> crate::TaskCall<T> {
        crate::TaskCall::from_args(encode_call_for_test(args))
    }
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test -p flexiq -j2 --test queue`
Expected: PASS, 4 tests.

- [ ] **Step 6: Check the other two backends compile**

```bash
cargo check -p flexiq -j2 --features postgres
cargo check -p flexiq -j2 --features redis
```
Expected: both clean.

- [ ] **Step 7: Commit**

```bash
git add crates/flexiq/src/queue.rs crates/flexiq/src/lib.rs crates/flexiq/tests/queue.rs
git commit -m "feat: the FlexiQ handle over an embedded backend"
```

---

### Task 6: `flexiq-macros` and `#[flexiq::task]`

**Files:**
- Create: `crates/flexiq-macros/Cargo.toml`, `src/lib.rs`, `src/attrs.rs`, `src/expand.rs`, `src/duration.rs`
- Create: `crates/flexiq/tests/macro_ui.rs`, `crates/flexiq/tests/ui/*.rs` + `*.stderr`
- Modify: root `Cargo.toml`, `scripts/version.mjs`, `.github/workflows/publish-crates.yml`, `.github/workflows/ci-rust.yml`, `crates/flexiq/Cargo.toml`, `crates/flexiq/src/lib.rs`

**Interfaces:**
- Consumes: Task 4's `Task` trait, `TaskCall`, `EnqueueOptions`.
- Produces: `#[flexiq::task]`, re-exported as `pub use flexiq_macros::task;`.

Expansion for `fn charge(order_id: String, cents: i64) -> Outcome<Receipt>`:

```rust
#[allow(non_camel_case_types)]
pub struct charge;

impl charge {
    pub fn call(order_id: String, cents: i64) -> ::flexiq::TaskCall<charge> { /* encode */ }
    pub fn run(order_id: String, cents: i64) -> ::flexiq::Outcome<Receipt> { /* original body */ }
}

impl ::flexiq::Task for charge {
    const NAME: &'static str = "charge";
    fn config() -> ::flexiq::TaskConfig { /* from attrs */ }
    fn defaults() -> ::flexiq::EnqueueOptions { /* from attrs */ }
    fn run_encoded(job: &::flexiq::Job, step: &mut ::flexiq::StepHandle)
        -> ::flexiq::Outcome<Option<Vec<u8>>> { /* decode, call run, encode */ }
}
```

The default name is the bare function name. `module_path!()` was rejected: it embeds the crate name, so renaming a binary would change a name a Python producer has to type.

- [ ] **Step 1: Create the crate and register it**

`crates/flexiq-macros/Cargo.toml`:

```toml
[package]
name = "flexiq-macros"
version.workspace = true
edition.workspace = true
license.workspace = true
repository.workspace = true
rust-version.workspace = true
description = "Attribute macro for the FlexiQ Rust SDK"
keywords = ["queue", "jobs", "macro"]
categories = ["asynchronous"]
readme = "README.md"

[lib]
proc-macro = true

[dependencies]
syn = { version = "2", features = ["full"] }
quote = "1"
proc-macro2 = "1"
```

Root `Cargo.toml`: add `"crates/flexiq-macros"` to `members`, and to `[workspace.dependencies]`:

```toml
flexiq-macros = { path = "crates/flexiq-macros", version = "2.0.0" }
```

- [ ] **Step 2: Mirror the version literal**

In `scripts/version.mjs`, add a `MIRRORS` entry beside the existing three:

```js
  {
    file: "Cargo.toml",
    pattern:
      /^(flexiq-macros = \{ path = "crates\/flexiq-macros", version = ")(.+?)(" \})$/m,
    label: "flexiq-macros registry coordinate",
  },
```

Run `node scripts/version.mjs --check`. Expected: passes, and the new file is listed.

- [ ] **Step 3: Write the failing test**

Create `crates/flexiq/tests/macro_ui.rs`:

```rust
//! Compile-fail cases for `#[flexiq::task]`.

#[test]
fn rejections_have_useful_messages() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
```

Create `crates/flexiq/tests/ui/bad_on_excess.rs`:

```rust
#[flexiq::task(on_excess = "discard")]
fn charge(cents: i64) -> flexiq::Outcome<()> {
    let _ = cents;
    Ok(())
}

fn main() {}
```

Create `crates/flexiq/tests/ui/bad_duration.rs`:

```rust
#[flexiq::task(timeout = "30 fortnights")]
fn charge(cents: i64) -> flexiq::Outcome<()> {
    let _ = cents;
    Ok(())
}

fn main() {}
```

- [ ] **Step 4: Run test to verify it fails**

Run: `cargo test -p flexiq -j2 --test macro_ui`
Expected: FAIL — the `task` attribute does not exist.

- [ ] **Step 5: Implement the macro**

`duration.rs` parses `"30s"`, `"500ms"`, `"5m"`, `"1h"` and a bare integer (milliseconds) into `i64` milliseconds, returning a `syn::Error` spanned at the literal for anything else.

`attrs.rs` parses the comma-separated list into a `TaskAttrs` struct with one `Option<T>` per supported attribute, erroring on an unknown key with a spanned message that lists the accepted ones. `on_excess` accepts only `"defer"` and `"drop"` — the two spellings `OnExcess::parse` accepts.

`expand.rs` emits the four items above. Points that need care:

- Re-emit the original body inside `run`, preserving its parameter list verbatim.
- `call` encodes its parameters as a tuple and hands the bytes to `TaskCall::from_args`, which Task 4 already made public for exactly this reason. The macro must never reach into `flexiq::testing`.
- Every path the expansion emits is absolute (`::flexiq::…`, `::std::…`). A caller who has `use flexiq as q;` or a local type named `Task` must still compile.
- `run_encoded` decodes into a tuple matching the parameter types, destructures, and calls `run`.
- Emit `#[allow(non_camel_case_types)]` on the struct.
- Every emitted public item needs a doc comment, because `#![deny(missing_docs)]` is on `crates/flexiq` and applies to expanded code in the caller's crate too. Emit `#[doc = "..."]` derived from the function's own doc comment where there is one, and a generated line where there is not.

- [ ] **Step 6: Accept the trybuild output**

Run: `TRYBUILD=overwrite cargo test -p flexiq -j2 --test macro_ui`
Then read each generated `.stderr` and confirm the message actually names the problem. Rewrite the macro's `syn::Error` text if it does not; do not accept a message that only says "unexpected token".

- [ ] **Step 7: Add a passing end-to-end macro test**

Append to `crates/flexiq/tests/queue.rs`:

```rust
#[flexiq::task(max_retries = 5, timeout = "30s", queue = "billing")]
fn charge(order_id: String, cents: i64) -> flexiq::Outcome<i64> {
    let _ = order_id;
    Ok(cents)
}

#[test]
fn the_macro_carries_its_attributes_into_the_job() {
    let q = FlexiQ::in_memory().expect("opens");
    let job = q
        .enqueue(charge::call("ord_1".into(), 4200))
        .expect("enqueues");

    assert_eq!(job.task_name, "charge");
    assert_eq!(job.queue, "billing");
    assert_eq!(job.max_retries, 5);
    assert_eq!(job.timeout_ms, 30_000);
}
```

- [ ] **Step 8: Run the whole crate's tests**

Run: `cargo test -p flexiq -j2`
Expected: PASS.

- [ ] **Step 9: Wire the publish set**

`.github/workflows/publish-crates.yml`: add `flexiq-macros` to `CRATES` (line 40) and `-p flexiq-macros` to `PACKAGES` (line 116). Order matters only in that `flexiq-macros` has no intra-workspace dependencies, so it can go first.

`.github/workflows/ci-rust.yml`: add it to the `publish-readiness` package set — the crates are verified together, because verifying one alone tries to resolve its siblings from the registry.

Add `crates/flexiq-macros/README.md` and a `LICENSE` copy, matching what the other published crates carry.

- [ ] **Step 10: Verify the crate is packageable**

```bash
cargo package -p flexiq-macros --no-verify --allow-dirty
```
Expected: succeeds.

- [ ] **Step 11: Commit**

Three commits, because these are three separable changes:

```bash
git add crates/flexiq-macros Cargo.toml
git commit -m "feat: a flexiq-macros crate with the task attribute"

git add crates/flexiq/src/call.rs crates/flexiq/src/lib.rs crates/flexiq/Cargo.toml crates/flexiq/tests/
git commit -m "feat: expand task into a call builder and a handler"

git add scripts/version.mjs .github/workflows/publish-crates.yml .github/workflows/ci-rust.yml
git commit -m "chore: add flexiq-macros to the publish set"
```

---

### Task 7: `WorkerBuilder` and the shell's dispatcher

**Files:**
- Create: `crates/flexiq/src/pool.rs`
- Create: `crates/flexiq/tests/worker.rs`
- Modify: `crates/flexiq/src/lib.rs`, `crates/flexiq/src/queue.rs`

**Interfaces:**
- Consumes: Task 4's `Task`, Task 5's `FlexiQ`.
- Produces: `FlexiQ::worker(&self) -> WorkerBuilder`, and on `WorkerBuilder`: `register<T: Task>(self) -> Self`, `queues(self, impl IntoIterator<Item = impl Into<String>>) -> Self`, `num_workers(self, usize) -> Self`, `worker_id(self, impl Into<String>) -> Self`, `scheduler_config(self, SchedulerConfig) -> Self`, `spawn(self) -> Result<WorkerHandle>`.

The name `Worker` is avoided deliberately: core exports it through `crates/flexiq`'s glob, and a shell type by that name would silently retype `flexiq::Worker` for existing callers.

The dispatcher is the load-bearing part. `NativeDispatcher` hands a handler `&Job` and nothing else (`worker/registry.rs:61-66`), and overrides neither `set_claim_owner` nor `set_lease_book` — both are default no-ops at `worker/mod.rs:76,87`, though `Worker::spawn` calls them (`worker/runner.rs:217,220`). So the scheduler mints the fence and the built-in pool drops it. The shell's dispatcher stores both.

This task builds the dispatcher without steps; Task 8 adds them. Model it on `worker/dispatcher.rs`, which you should read in full first.

- [ ] **Step 1: Read the model**

Run: `sed -n '1,125p' crates/flexiq-core/src/worker/dispatcher.rs`
Note the semaphore, the `spawn_blocking` split, and the unregistered-task branch — all three are reproduced.

- [ ] **Step 2: Write the failing test**

Create `crates/flexiq/tests/worker.rs`:

```rust
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use flexiq::FlexiQ;
use flexiq_core::JobStatus;

static RUNS: AtomicUsize = AtomicUsize::new(0);

#[flexiq::task]
fn double(n: i64) -> flexiq::Outcome<i64> {
    RUNS.fetch_add(1, Ordering::SeqCst);
    Ok(n * 2)
}

#[flexiq::task]
fn always_fails(_n: i64) -> flexiq::Outcome<()> {
    Err(flexiq_core::TaskError::fatal("nope").into())
}

/// Poll until `job` reaches a terminal state, or fail the test.
fn wait_terminal(q: &FlexiQ, job_id: &str) -> flexiq_core::Job {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let job = q.get_job(job_id).expect("reads").expect("job exists");
        if matches!(job.status, JobStatus::Complete | JobStatus::Failed | JobStatus::Dead) {
            return job;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("job {job_id} never reached a terminal state");
}

#[test]
fn a_registered_task_runs_and_records_its_result() {
    let q = FlexiQ::in_memory().expect("opens");
    let job = q.enqueue(double::call(21)).expect("enqueues");

    let worker = q.worker().register::<double>().num_workers(2).spawn().expect("spawns");
    let done = wait_terminal(&q, &job.id);
    worker.shutdown().expect("clean shutdown");

    assert_eq!(done.status, JobStatus::Complete);
    assert_eq!(RUNS.load(Ordering::SeqCst), 1);
}

#[test]
fn a_failure_records_the_contract_json() {
    let q = FlexiQ::in_memory().expect("opens");
    let job = q.enqueue(always_fails::call(1)).expect("enqueues");

    let worker = q.worker().register::<always_fails>().spawn().expect("spawns");
    let done = wait_terminal(&q, &job.id);
    worker.shutdown().expect("clean shutdown");

    let recorded = done.error.expect("an error was recorded");
    let parsed: serde_json::Value = serde_json::from_str(&recorded).expect("valid JSON");
    assert_eq!(parsed["errtype"], "TaskError");
    assert_eq!(parsed["message"], "nope");
}

#[test]
fn an_unregistered_task_dead_letters_without_retrying() {
    let q = FlexiQ::in_memory().expect("opens");
    let job = q.enqueue(double::call(1)).expect("enqueues");

    // No `register` call at all.
    let worker = q.worker().spawn().expect("spawns");
    let done = wait_terminal(&q, &job.id);
    worker.shutdown().expect("clean shutdown");

    assert_eq!(done.retry_count, 0, "an unregistered task must not retry");
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p flexiq -j2 --test worker`
Expected: FAIL to compile — `FlexiQ::worker` does not exist.

- [ ] **Step 4: Write the dispatcher and builder**

`pool.rs` holds two things. First, `ShellDispatcher`:

```rust
/// The shell's pool: [`NativeDispatcher`]'s shape, plus the fence.
///
/// `NativeDispatcher` ignores `set_claim_owner` and `set_lease_book`, so a
/// handler under it can never open a step session — the fence those steps are
/// written under is minted by the scheduler and dropped before any handler
/// sees it. This one keeps both.
struct ShellDispatcher {
    handlers: Arc<HashMap<String, ShellHandler>>,
    storage: StorageBackend,
    namespace: Option<String>,
    num_workers: usize,
    shutdown: AtomicBool,
    owner: Mutex<String>,
    leases: Mutex<Option<Arc<LeaseBook>>>,
}

impl WorkerDispatcher for ShellDispatcher {
    fn set_claim_owner(&self, owner: &str) {
        *self.owner.lock().expect("owner lock") = owner.to_string();
    }

    fn set_lease_book(&self, leases: Arc<LeaseBook>) {
        *self.leases.lock().expect("lease lock") = Some(leases);
    }
    // run / shutdown as in NativeDispatcher
}
```

A `ShellHandler` is `Arc<dyn Fn(&Job, &mut StepHandle) -> Outcome<Option<Vec<u8>>> + Send + Sync>`, which is what `T::run_encoded` becomes when `register::<T>()` stores it.

The result mapping — the one place three outcomes become three `JobResult` arms:

```rust
fn job_result(job: &Job, outcome: Outcome<Option<Vec<u8>>>, started: Instant) -> JobResult {
    let wall_time_ns: i64 = started.elapsed().as_nanos().try_into().unwrap_or(i64::MAX);
    match outcome {
        Ok(result) => JobResult::Success { /* .. */ },
        Err(Abort::Fail(err)) => JobResult::Failure {
            error: crate::outcome::task_error_json(&err),
            should_retry: err.retryable,
            /* .. */
        },
        Err(Abort::Sleep(sleep)) => JobResult::Slept {
            job_id: job.id.clone(),
            task_name: job.task_name.clone(),
            wake_at: sleep_wake_at(&sleep),
            wall_time_ns,
        },
    }
}
```

Second, `WorkerBuilder`, which collects handlers and `TaskConfig`s and hands them to core:

```rust
    /// Start the worker. Registers this process, starts the scheduler and the
    /// pool, and returns a handle whose `shutdown` drains and unregisters.
    pub fn spawn(self) -> Result<WorkerHandle> {
        let dispatcher = Arc::new(ShellDispatcher::new(/* .. */));
        let mut worker = Worker::new(self.storage)
            .queues(self.queues)
            .num_workers(self.num_workers)
            .dispatcher("rust", dispatcher);
        for (name, config) in self.configs {
            worker = worker.task_config(name, config);
        }
        worker.spawn()
    }
```

Note that `Worker::spawn` skips the registry fingerprint when a custom dispatcher is supplied, so nothing else needs doing about it.

- [ ] **Step 5: Drop the temporary `allow`**

`outcome.rs` carries `#[allow(dead_code)]` on `task_error_json` because this dispatcher is its only caller and did not exist yet. Delete the attribute and its comment now, and let clippy prove the call is real.

- [ ] **Step 6: Run test to verify it passes**

Run: `cargo test -p flexiq -j2 --test worker`
Expected: PASS, 3 tests.

Then: `CARGO_BUILD_JOBS=2 cargo clippy -p flexiq --all-targets -- -D warnings`
Expected: clean, and no `dead_code` on `task_error_json`.

- [ ] **Step 6: Commit**

```bash
git add crates/flexiq/src/pool.rs crates/flexiq/src/lib.rs crates/flexiq/src/queue.rs crates/flexiq/tests/worker.rs
git commit -m "feat: a worker that runs registered Rust tasks"
```

---

### Task 8: Durable steps

**Files:**
- Create: `crates/flexiq/src/steps.rs`
- Create: `crates/flexiq/tests/steps.rs`
- Modify: `crates/flexiq/src/task.rs`, `crates/flexiq/src/pool.rs`

**Interfaces:**
- Consumes: Task 7's `ShellDispatcher`, `flexiq_core::{StorageStepSession, StorageSteps, StepSession, StepLimits, StepSleep, LeaseBook}`.
- Produces: `pub fn current_step() -> StepHandle` at the crate root, and on `StepHandle`: `run<T, F>(&mut self, name: &str, body: F) -> Outcome<T>` where `F: FnOnce() -> Outcome<T>` and `T: Serialize + DeserializeOwned`; `run_keyed<T, F>(&mut self, name: &str, key: &str, body: F) -> Outcome<T>`; `sleep_ms(&mut self, name: &str, ms: i64) -> Outcome<()>`; `sleep_until(&mut self, name: &str, wake_at: i64) -> Outcome<()>`.

Per job the dispatcher builds:

```rust
let epoch = leases.as_ref().and_then(|b| b.current(&job.id)).and_then(|l| l.epoch());
let store = StorageSteps::new(storage.clone(), &owner, job.retry_count).with_epoch(epoch);
let session = StepSession::open(store, &job, StepLimits::default())?;
```

`.with_epoch` is called by **no existing shell** — Python, Node and Java all fence on `(owner, attempt)` only, because each reaches steps through an FFI class that must be one concrete non-generic type. Rust has none of those reasons. Verify the claim still holds before relying on it: `grep -rn "with_epoch" crates/flexiq-python crates/flexiq-node crates/flexiq-java` must return nothing.

Keep the session concrete (`StorageStepSession<StorageBackend>`) and call `StepSession::run` directly. Do **not** use `StepSession::boxed` / `BoxedStepStore`, and do not use the split `begin_run`/`commit_run` — both exist for FFI shells whose closures cross a language boundary.

Call `session.finish()` after the body returns, win or lose; it logs orphaned steps.

- [ ] **Step 1: Write the failing test**

Create `crates/flexiq/tests/steps.rs`:

```rust
use std::sync::atomic::{AtomicUsize, Ordering};

use flexiq::FlexiQ;
use flexiq_core::{JobStatus, TaskError};

static CHARGES: AtomicUsize = AtomicUsize::new(0);
static ATTEMPTS: AtomicUsize = AtomicUsize::new(0);

/// Fails once after its step commits, so the retry must find the memo.
#[flexiq::task(max_retries = 2)]
fn charge_once(order: String) -> flexiq::Outcome<()> {
    let mut step = flexiq::current_step();
    let receipt: String = step.run("charge", || {
        CHARGES.fetch_add(1, Ordering::SeqCst);
        Ok(format!("receipt-for-{order}"))
    })?;
    assert!(receipt.starts_with("receipt-for-"));

    if ATTEMPTS.fetch_add(1, Ordering::SeqCst) == 0 {
        return Err(TaskError::retryable("crash after the charge").into());
    }
    Ok(())
}

#[test]
fn a_committed_step_is_not_re_run_on_retry() {
    let q = FlexiQ::in_memory().expect("opens");
    let job = q.enqueue(charge_once::call("ord_1".into())).expect("enqueues");
    let worker = q.worker().register::<charge_once>().spawn().expect("spawns");

    let done = wait_terminal(&q, &job.id);
    worker.shutdown().expect("clean shutdown");

    assert_eq!(done.status, JobStatus::Complete);
    assert_eq!(ATTEMPTS.load(Ordering::SeqCst), 2, "the job must have retried");
    assert_eq!(
        CHARGES.load(Ordering::SeqCst),
        1,
        "the committed step must be memoized, not re-run"
    );
}
```

Add a second test for sleep:

```rust
#[flexiq::task]
fn nap(_n: i64) -> flexiq::Outcome<()> {
    let mut step = flexiq::current_step();
    step.sleep_ms("wait", 400)?;
    Ok(())
}

#[test]
fn a_sleep_ends_the_attempt_without_touching_retry_count() {
    let q = FlexiQ::in_memory().expect("opens");
    let job = q.enqueue(nap::call(1)).expect("enqueues");
    let worker = q.worker().register::<nap>().spawn().expect("spawns");

    let done = wait_terminal(&q, &job.id);
    worker.shutdown().expect("clean shutdown");

    assert_eq!(done.status, JobStatus::Complete);
    assert_eq!(done.retry_count, 0, "a sleep is not a retry");
}
```

Copy `wait_terminal` from `tests/worker.rs` into this file — an integration test target cannot import from another one, and duplicating ten lines is cheaper than a shared crate.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p flexiq -j2 --test steps`
Expected: FAIL to compile — `flexiq::current_step` does not exist.

- [ ] **Step 3: Decide and implement how a body reaches its handle**

`Task::run_encoded` already takes `&mut StepHandle`, but a macro-expanded body is the user's own code and cannot take an extra parameter without changing the function's signature. Use a task-local: the dispatcher sets the handle before calling the body and clears it after, and `flexiq::current_step()` reads it.

Use `tokio::task_local!` for async handlers and a `thread_local!` for the `spawn_blocking` path, or a single `thread_local!` if the shell runs every handler through `spawn_blocking`. Prefer the latter for this branch — one mechanism, and the async story can be its own change. Document the restriction: a body that moves work to another thread cannot call `current_step()` from it.

`current_step()` outside a task must not panic in a way that kills the pool. Return a `StepHandle` whose `inner` is `None`, and have every method on it return `Abort::Fail(TaskError::fatal(..))` naming the misuse.

- [ ] **Step 4: Write `steps.rs`**

`StepHandle::run` encodes the body's return value with Task 2's encoder, hands the bytes to `StepSession::run`, and decodes the returned bytes with Task 3's decoder — so a memo written on one attempt and read on the next goes through exactly the same codec as a job payload.

`sleep_ms` maps `StepSleep::Sleeping` to `Err(Abort::Sleep(..))` and `StepSleep::Elapsed` to `Ok(())`. The `Elapsed` arm is what a *replay* sees once the sleep is over; treating it as an error would make a woken job sleep forever.

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test -p flexiq -j2 --test steps`
Expected: PASS, 2 tests.

- [ ] **Step 6: Confirm the epoch is actually being set**

Add a test asserting the committed `job_steps` row carries a non-null epoch, reading it through `Storage::get_job_steps`. Without this the `.with_epoch` call is untested and could regress to `None` silently.

- [ ] **Step 7: Commit**

```bash
git add crates/flexiq/src/steps.rs crates/flexiq/src/task.rs crates/flexiq/src/pool.rs crates/flexiq/tests/steps.rs
git commit -m "feat: durable steps for Rust tasks, fenced on the epoch"
```

---

### Task 9: Periodic tasks

**Files:**
- Create: `crates/flexiq/src/cron.rs`
- Create: `crates/flexiq/tests/periodic.rs`
- Modify: `crates/flexiq/src/pool.rs`, `crates/flexiq-macros/src/attrs.rs`, `crates/flexiq-macros/src/expand.rs`

**Interfaces:**
- Consumes: `flexiq_core::{NewPeriodicTask, PeriodicTask}`, `flexiq_core::periodic::{next_cron_time, next_cron_time_tz}`.
- Produces: `Task::periodic() -> Option<PeriodicSpec>` (a new trait method defaulting to `None`), `FlexiQ::list_periodic()`, `delete_periodic(&str)`, `pause_periodic(&str)`, `resume_periodic(&str)`, and registration inside `WorkerBuilder::spawn`.

Registration is a plain storage call; `Scheduler::check_periodic` already runs inside the loop `Worker::spawn` starts, so there is no new plumbing. Six-field cron (seconds first). `PeriodicTask.timezone` is an IANA name; `None` means UTC.

- [ ] **Step 1: Write the failing test**

Create `crates/flexiq/tests/periodic.rs`:

```rust
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use flexiq::FlexiQ;

static TICKS: AtomicUsize = AtomicUsize::new(0);

#[flexiq::task(cron = "* * * * * *")]
fn every_second() -> flexiq::Outcome<()> {
    TICKS.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[test]
fn a_periodic_task_registers_and_fires() {
    let q = FlexiQ::in_memory().expect("opens");
    let worker = q.worker().register::<every_second>().spawn().expect("spawns");

    let registered = q.list_periodic().expect("lists");
    assert_eq!(registered.len(), 1);
    assert_eq!(registered[0].task_name, "every_second");
    assert_eq!(registered[0].cron_expr, "* * * * * *");

    let deadline = Instant::now() + Duration::from_secs(15);
    while TICKS.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    worker.shutdown().expect("clean shutdown");

    assert!(TICKS.load(Ordering::SeqCst) >= 1, "the periodic never fired");
}

#[test]
fn a_paused_periodic_stops_being_due() {
    let q = FlexiQ::in_memory().expect("opens");
    let worker = q.worker().register::<every_second>().spawn().expect("spawns");
    assert!(q.pause_periodic("every_second").expect("pauses"));
    worker.shutdown().expect("clean shutdown");

    let listed = q.list_periodic().expect("lists");
    assert!(!listed[0].enabled);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p flexiq -j2 --test periodic`
Expected: FAIL — `cron` is not an accepted attribute.

- [ ] **Step 3: Implement**

Add `cron` and `timezone` to `TaskAttrs`. A task carrying `cron` must take **no parameters** — a periodic fire has no arguments, and `Scheduler::check_periodic` builds the payload from the stored `args` alone. Reject a parameterised cron task in the macro with a spanned error saying so.

In `WorkerBuilder::spawn`, before starting core's worker, loop the registered tasks and for each with a `PeriodicSpec` compute `next_run` and call `storage.register_periodic`. `register_periodic` upserts on `name`, so calling it on every worker start is correct.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p flexiq -j2 --test periodic`
Expected: PASS, 2 tests.

- [ ] **Step 5: Document the fleet caveat**

In `cron.rs`'s module doc, state plainly: firing is per-`Scheduler` and not leader-elected, and the dedup key is `"periodic:{name}:{now}"` computed from each process's own clock, so two workers that both registered the same periodic can both fire it in the same window. Register periodics from one process.

- [ ] **Step 6: Commit**

```bash
git add crates/flexiq/src/cron.rs crates/flexiq/src/pool.rs crates/flexiq-macros/src crates/flexiq/tests/periodic.rs
git commit -m "feat: cron tasks for the Rust shell"
```

---

### Task 10: README, rustdoc, and the full gate

**Files:**
- Modify: `crates/flexiq/README.md`, `crates/flexiq/src/lib.rs`
- Create: `crates/flexiq/examples/quickstart.rs`
- Modify: `tasks/todo.md`

`crates/flexiq/README.md` is included as the crate doc via `#![doc = include_str!("../README.md")]` and is doctested, so every snippet in it must compile. The current quick-start hand-writes all fifteen `NewJob` fields; replace it with the shell.

- [ ] **Step 1: Rewrite the README quick-start**

Show the macro, the handle and the worker, with the code fence as a real doctest (no `ignore`, no `no_run` unless the snippet genuinely needs a database it cannot open — prefer `FlexiQ::in_memory()` so it runs).

- [ ] **Step 2: Run the doctests**

Run: `cargo test -p flexiq -j2 --doc`
Expected: PASS.

- [ ] **Step 3: Add a runnable example**

`crates/flexiq/examples/quickstart.rs`, mirroring `flexiq-core`'s `examples/hello.rs` but through the shell — the before/after is the point of the issue.

Run: `cargo run -p flexiq --example quickstart -j2`
Expected: prints the task's result and exits 0.

- [ ] **Step 4: Run every gate**

```bash
cargo test --workspace -j2
CARGO_BUILD_JOBS=2 cargo clippy --all-targets --all-features -- -D warnings
cargo doc -p flexiq -p flexiq-macros --no-deps
cargo check --workspace -j2 --features postgres
cargo check --workspace -j2 --features redis
node scripts/version.mjs --check
```
Expected: all clean. `cargo doc` must emit no warnings — `missing_docs` is denied and the coverage gate runs per PR.

- [ ] **Step 5: Record the plan's outcome**

Add a review section to `tasks/todo.md` describing what landed. `tasks/todo.md` is a per-branch scratchpad rewritten whole each task, so every master merge conflicts on it — resolve `--ours`.

- [ ] **Step 6: Commit**

```bash
git add crates/flexiq/README.md crates/flexiq/src/lib.rs crates/flexiq/examples/quickstart.rs tasks/todo.md
git commit -m "docs: rewrite the flexiq quick-start around the shell"
```

---

## After the plan

Do not push and do not open a PR without asking — that is a standing rule on this repo, and the account to raise it under changes between sessions.

Four follow-up issues to file, recorded in the spec:

1. A remote producer handle behind a `remote` feature, under epic #835.
2. `flexiq::worker::TaskHandler` is missing from core's root re-export list (`crates/flexiq-core/src/lib.rs:80-87`) while every sibling type is there.
3. `NewPeriodicTask.kwargs` is written by no shell and read by no scheduler (`scheduler/maintenance.rs:310` builds the payload from `args` alone).
4. `register_periodic` diverges across backends on `last_run`: Postgres's upsert preserves it, SQLite's `REPLACE INTO` and Redis's explicit `None` both reset it on every re-registration. Not covered by the shared contract suite.

Branch 2 — the docs tier — is a separate plan against the same spec.
