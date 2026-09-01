# #715 — raw envelope or structured args on the wire

## Problem
`EnqueueRequest.body` ships one arm today: `raw`, the tagged envelope from
`BINDING_CONTRACT.md` — `0x02` followed by CBOR `[args, kwargs]`. A client that
wants to submit a job therefore needs a CBOR library before it can send its
first one, which makes "any language" mean "any language with a CBOR library"
and rules out `curl` entirely.

Issue #714 left field 3 of the oneof unspent for exactly this. A new arm inside an
existing oneof is additive, so nothing already shipped moves.

Reviewed against §11 of `tasks/specs/2026-09-01-flexiq-v1-proto-design.md`,
which fails #715 if a second envelope encoder appears, if `structured` rounds a
`round_trip_only` vector instead of refusing it, or if `raw` is re-encoded on
the way through.

## Scope calls

- **`structured` uses protobuf's dynamic value types** —
  `repeated google.protobuf.Value args`, `map<string, Value> kwargs`. That is
  what makes the arm worth having: proto3 JSON renders a `Value` as a plain
  JSON value, so `curl` (and #718's JSON facade) sends
  `{"args":[{"order_id":"ord-1"}],"kwargs":{}}` rather than tagged noise.

- **CBOR map keys are emitted sorted, and that is documented, not hidden.**
  A proto3 `map` is unordered by definition and prost decodes
  `google.protobuf.Struct` into a `BTreeMap`, so a client's key order does not
  survive the wire and no encoder can recover it. Consequence, stated plainly
  in the proto and in the docs: eight of the nine `encode` vectors in
  `contracts/wire-vectors.json` reproduce byte-for-byte through `structured`;
  `single-object-arg` is the ninth, and differs only in the order of a two-key
  map. Nothing decodes differently. The one thing that notices is the SDKs'
  `auto:` idempotency key, a sha256 over the payload bytes — and a `structured`
  client cannot compute one anyway, it sets `unique_key` by hand.

- **The three `decode_only` vectors are the documented refusal.** An integer
  past `2^53 - 1` is rejected rather than truncated — 2^53 itself included,
  since it is what 2^53 + 1 rounds to; byte strings and CBOR tags have
  no `google.protobuf.Value` arm at all, so they are structurally unreachable.
  `raw` remains the lossless door and stays the one an SDK uses.

- **The envelope encoder lands in `flexiq-core`, not in `flexiq-server`.**
  Rust has no envelope encoder today — the three shells each use their
  language's CBOR library, and the published `flexiq` crate hands a Rust
  producer nothing. `flexiq_core::wire` becomes *the* Rust implementation, so
  D5's "one implementation of the envelope" holds as the surface grows, and
  the vectors that pin it are asserted in the crate that owns it.

- **Hand-written CBOR writer, no new dependency.** Encoding only, ~150 lines.
  The contract demands definite-length containers and shortest-form integers;
  a serde-backed codec makes both a property of how it is driven rather than of
  the code, on a crate published to crates.io. There is no decoder because
  nothing here decodes.

## Plan

### 1. Core: the envelope encoder (its own commit)
- [x] `crates/flexiq-core/src/wire/value.rs` — `WireValue`: `Null`, `Bool`,
      `Integer(i64)`, `Float(f64)`, `Text`, `Bytes`, `Array`, `Map(Vec<(String,
      WireValue)>)`. The map is a `Vec` so a caller keeps its own key order;
      the gRPC door feeds it already sorted.
- [x] `crates/flexiq-core/src/wire/cbor.rs` — the writer. Definite-length heads,
      shortest-form integer arguments, major type 1 for negatives, `f64` for
      floats.
- [x] `crates/flexiq-core/src/wire/envelope.rs` — `TAG_CBOR`, and
      `encode_call(args, kwargs) -> Vec<u8>` emitting `0x02 || [args, kwargs]`.
- [x] `crates/flexiq-core/src/wire/mod.rs` — barrel + the module doc that
      points at `BINDING_CONTRACT.md`; `pub mod wire;` in `lib.rs`.
- [x] `crates/flexiq-core/tests/rust/wire_vector_tests.rs` — every `encode` case
      in `contracts/wire-vectors.json` asserted against its pinned hex. The
      JSON is read through a serde `Visitor` so object key order survives
      (`serde_json::Value` sorts, and one of the vectors is order-sensitive).
- [x] One line in `BINDING_CONTRACT.md` naming the Rust encoder.

### 2. Protos
- [x] `job.proto`: `StructuredArgs`, importing `google/protobuf/struct.proto`.
      Comments carry the ceiling — what is refused, and that key order is
      normalised — because a client reads the `.proto` and not the design doc.
- [x] `producer_service.proto`: `StructuredArgs structured = 3;` in the `body`
      oneof, replacing the placeholder comment.
- [x] `scripts/proto-check.sh --fix` (buf 1.72.0 == the pin) to regenerate
      `contracts/descriptor.binpb`. Additive, so no `proto-breaking` label.

### 3. Server: the structured door
- [x] `crates/flexiq-server/src/grpc/producer/structured.rs` —
      `pb::StructuredArgs` → `WireValue` → `flexiq_core::wire::encode_call`.
      Refusals, all `INVALID_REQUEST`: a `Value` with no `kind`; a non-finite
      `number_value`; an integral `number_value` past ±2^53; an unknown
      `null_value` enumerator.
- [x] `enqueue.rs::prepare` gains the `Structured` arm. `EnqueueBatch` inherits
      it, since it prepares each item through the same function.
- [x] Unit tests beside the conversion: each refusal, and structured/raw
      byte-identity.
- [x] `crates/flexiq-server/tests/grpc_producer.rs` — the `encode` vectors
      through the real RPC (read back with `include_payload`), the 2^53
      rejection end to end, and one batch item sent structured.

### 4. Docs
- [x] `deployment.mdx`: the "structured-argument door, which is not here yet"
      paragraph becomes the section that states which door loses precision,
      with a `grpcurl` example of each.

## Verification
- [x] `cargo test -j2 -p flexiq-core --test rust`
- [x] `cargo test -j2 -p flexiq-server --features grpc`
- [x] `cargo check -j2 --workspace` + `--features postgres` + `--features redis`
- [x] `cargo fmt`, `cargo clippy -j2 --workspace --all-targets`
- [x] `scripts/proto-check.sh` and `scripts/proto-guard.sh`
- [x] `buf breaking` against `origin/master` — additive, no label needed.
- [ ] `pnpm --dir docs build` is not run here; docs change is prose in one mdx.

## Review

Three commits.

1. **`feat: a CBOR envelope encoder in the core`** — `flexiq_core::wire`
   (`value.rs` / `cbor.rs` / `envelope.rs`). Rust genuinely had no envelope
   encoder: §7.1's "the same encoder the shells use" was written expecting one,
   and each shell in fact reaches for its language's CBOR library. All nine
   `encode` vectors reproduce byte-for-byte, read through a serde `Visitor`
   because `serde_json::Value` sorts object keys and one vector is
   order-sensitive. Both `round_trip_only` vectors are reachable from Rust,
   which is the test that says *why* `raw` stays.

2. **`feat: structured arguments on the enqueue wire`** — `StructuredArgs` in
   `job.proto`, field 3 of the `body` oneof, and
   `grpc/producer/structured.rs`. `raw` still reaches storage untouched.

3. **`docs: state which enqueue door loses precision`** — a section, not a
   footnote, with a `grpcurl` example of the structured arm.

### What the issue did not anticipate

**Object key order cannot survive a protobuf map.** `google.protobuf.Value`'s
object arm is `google.protobuf.Struct`, a proto3 `map`; prost decodes it into a
`BTreeMap` and the client's order is not on the wire to recover. So
`single-object-arg` — the one pinned vector with a two-key object — cannot be
byte-reproduced through `structured`, and is pinned in its sorted form beside
the conversion instead. Eight of nine are byte-identical. The alternative was a
FlexiQ-local ordered value type, which would have made proto3 JSON tagged and
verbose and taken `curl` — the whole motive — away. Chosen deliberately, with
the user; recorded as an amendment in the design doc §7.1 and stated in the
proto, the docs and the test.

**2^53 is not the safe boundary; 2^53 - 1 is.** 9007199254740993 rounds to
9007199254740992 = 2^53 exactly, so a server that accepted 2^53 could not tell
the two apart and would answer a request nobody made. The refusal is
`|value| > 2^53 - 1`.

**The float head byte is not a shortest-form argument.** Routing `0xfb` through
the integer head writer emits `f8 1b` — major type 7's low bits are an
additional-information code, not a number to shorten. Caught by the first test
run; the major-type-7 heads are literal constants now.
