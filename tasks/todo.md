# Pin the CBOR float width so `auto:` keys cannot diverge (#905)

`contracts/wire-vectors.json` left float width free by design, so the `float`
case pinned a value and not bytes. An `auto:` idempotency key is a sha256 over
the serialized payload, so two conforming encoders can derive different keys for
the same float argument and each enqueues its own active job — the same class of
silent divergence as the indefinite-length maps of #638.

**Decision: option 2 from the issue — a float is always a 64-bit double.** Not
"shortest lossless width" (option 1), for two reasons: every runtime already
emits `fb` for a finite double, so no already-enqueued payload's bytes move; and
option 1 would have to *add* narrowing logic to five encoders that have none,
changing the bytes Rust emits today.

The rule, as it reads: **a finite float MUST carry the 64-bit head `fb`, never
`f9` or `fa`, even when a narrower width would round-trip the value exactly.
Readers MUST accept all three widths.** A non-finite float is a stated exemption
— see the last section.

## Where each runtime stood

| Runtime | Finite float | 32-bit float input |
|---|---|---|
| Rust core (`wire/cbor.rs`) | `fb`, unconditional | n/a (`WireValue::Float` is `f64`) |
| Rust SDK (`crates/flexiq`) | `fb` | widened in `serialize_f32` |
| Python (`cbor2`, non-canonical) | `fb` | no `float32` type |
| Node (`cbor-x`, `useFloat32` unset) | `fb` | no `float32` type |
| Java (`DefiniteLengthCbor`) | `fb` for a `double` | **`fa` for a `float` — the bug** |
| Go (`sdks/go/wire.go`) | `fb` (`ShortestFloatNone`) | `fa` — out of scope, see below |

## Tasks

- [x] 1. `contracts/wire-vectors.json` — move `float` from `decode_only` into
      `encode` (bytes now pinned); add `float-narrow-half` (`f9 3e00`) and
      `float-narrow-single` (`fa 3fc00000`), the reader's half of the rule at both
      widths a writer may not use; rewrite the header comment so `decode_only`
      states its two reasons — JSON cannot hold the value, or a conforming writer
      must not produce those bytes — and add the float rule beside the
      definite-length and shortest-integer ones.
- [x] 2. `crates/flexiq-core/BINDING_CONTRACT.md` — the float rule on the
      cross-SDK bullet that already carries definite length and shortest-form
      integers, with `1.5` spelled both ways, plus the non-finite exemption as a
      bullet of its own.
- [x] 3. `contracts/REMOTE_SDK_CONTRACT.md` — encode count 9 → 10; replace the
      `float` exemption bullet with the `float-narrow-*` obligation; narrow the
      float exception in conformance claim 2 to a non-finite one; add the rule to
      the restated encoder rules.
- [x] 4. Rust core — `wire/value.rs` doc comment, `wire/cbor.rs` module doc (a
      third structural rule) and a unit test that no float is narrowed.
- [x] 5. `crates/flexiq-core/tests/wire_vectors.rs` — the moved vector now flows
      through the encode loop, which is also what makes `visit_f64` live; a
      non-finite float asserted beside the two `round_trip_only` cases.
- [x] 6. `crates/flexiq/tests/wire_vectors.rs` — a `float` encode case with both
      an `f64` and an `f32` argument producing the same bytes; the narrow-float
      reader test renamed onto its new vector.
- [x] 7. Java — `DefiniteLengthCbor.writeNumber` widens a `FloatNode` to a
      double; javadoc explains the asymmetry (integers narrow, floats widen);
      regression tests at the top level and nested, plus `serializeCall(1.5f)`
      against the pinned vector.
- [x] 8. Go — the float rule stated in `wire.go`'s comment block, and a test
      pinning both halves of it.
- [x] 9. Python and Node — comment the encoder option each rule depends on
      (`cbor2`'s `canonical`, `cbor-x`'s `useFloat32`).
- [x] 10. Docs — `docs/content/docs/server/clients.mdx` restates the encoder
      rules; `docs/content/docs/shared/guides/extend/serializers.mdx` said an
      `f32` is widened "because core always writes one", which is now a contract
      rule rather than an implementation detail.
- [x] 11. `CHANGELOG.md` — an Unreleased `### Fixed` entry.
- [x] 12. Verify: all five conformance suites, `cargo fmt`, clippy, ruff, mypy,
      biome, spotless, strict javadoc.

## Review

**Done, and every suite asserts the new vector.** `float` is an `encode` case, so
its bytes are now pinned in Rust core, the Rust SDK, Python, Node, Java and Go,
and the two `float-narrow-*` cases pin the reader's half. One encoder was actually
non-conforming: a Java `float` reached the wire as `fa`, at any depth in a
payload, so `f(1.5f)` and `f(1.5d)` produced different `auto:` keys for the same
call. `DefiniteLengthCbor` widens in the tree walk, which covers every depth.

Verified: `cargo test -p flexiq-core --lib wire::` (9) and `--test wire_vectors`
(2); `cargo test -p flexiq --test wire_vectors` (23); Python
`tests/core/test_wire_vectors.py` (24); Node `test/core/wireVectors.test.ts`
(22); Java `WireVectorsTest` + `DefiniteLengthCborTest` (28); Go `./tests/` whole
package. Then `cargo fmt --all -- --check`, clippy over both Rust crates with
`--all-targets -D warnings`, `ruff check` + `ruff format --check` + `mypy`,
`biome check` on both touched TypeScript files, `gofmt -l` + `go vet`,
`spotlessCheck` and the strict `:javadoc`.

### Three things the plan had wrong

**A non-finite float's width cannot be pinned, and the vectors now say so.** The
plan had `float-infinity` and `float-nan` as `round_trip_only` vectors and a Go
one-liner to stop narrowing them. Python failed both: `cbor2` hard-codes RFC
8949's preferred two-byte spelling for an infinity and a NaN in its C encoder,
with no option to write one wide. Java's `CBORGenerator` and `cbor-x` have the
opposite gap — no way to write one narrow. So no width is pinnable for either
value, the two vectors are gone, the rule says "finite", and the exemption is
stated with its consequence: a payload carrying one still wants `unique_key`.
The Go change was reverted with it — `NaNConvert7e00` / `InfConvertFloat16` are
conforming, and they match what Python emits.

**A `round_trip_only` vector must carry exactly one argument.** The first
non-finite vector held `[inf, nan]` and Java failed it: its call API takes a
single payload, so its round-trip assertion re-encodes `args[0]` alone. The two
existing round-trip cases are single-argument and nothing said why.

**Half precision decodes to a Java `float`, and Jackson node equality is by
type.** `FloatNode(1.5)` does not equal `DoubleNode(1.5)`, so `float-narrow-half`
failed the shared decode comparison until it widened the decoded tree through a
JSON round trip. The vector pins the value a narrower width decodes to, not the
Java type it lands in.

### Review feedback on PR #952

**Taken: a single-precision narrow-float vector.** One narrow case pinned only
`f9`, so a reader could accept half and double precision, pass every vector, and
still refuse a legacy `fa` payload it is obliged to read. `float-narrow-single`
(`028281fa3fc00000a0`) now sits beside `float-narrow-half`, and the pair named the
existing case: `float-narrow` became `float-narrow-half`.

**Declined: widening a Go `float32` at the encoder boundary.** See the section
below — the reason is a library capability, not an oversight, and it is why the
same gap in Java *was* fixed here. Tracked separately rather than folded in.

### Out of scope, deliberately

**A Go `float32` still encodes as `fa`.** `fxamacker/cbor` marshals Go values
directly, so widening one nested inside a struct would mean rebuilding the value
through reflection, and refusing it would mean a read-only reflect walk on every
enqueue. Neither belongs in a change whose subject is the contract. The Go client
derives no `auto:` key of its own — it is a remote producer and a caller sets
`unique_key` — so the gap cannot bite the way Java's did. `wire.go` names it
where the encoder options are chosen, and it wants its own issue.
