//! The shell's encoder against `contracts/wire-vectors.json`.
//!
//! Every case in the file's `encode` array appears here as the Rust a caller
//! would actually write, rather than as JSON re-read at runtime: the vectors
//! pin *bytes*, and the thing that has to produce them is a Rust value going
//! through this crate's serializer. Reading the JSON instead would test
//! `serde_json`'s map ordering, which is not what ships.
//!
//! Excluded from the published crate — the vectors live above this crate's
//! root, so a packaged copy could not read them even if it wanted to. See
//! `Cargo.toml`'s `exclude`, and #900 for what that trap cost last time.

use flexiq::__private::{decode_args, encode_args, to_wire, WireValue};
use serde::Serialize;

/// Encode a positional argument list and render it the way the vectors do.
fn encode(args: &[WireValue]) -> String {
    hex::encode(encode_args(args))
}

/// One argument, converted the way the macro converts each parameter.
fn arg<T: Serialize>(value: &T) -> WireValue {
    to_wire(value).expect("encodable")
}

/// Case `no-args`: `f()`.
#[test]
fn no_args() {
    assert_eq!(encode(&[]), "028280a0");
}

/// Case `contract-vector`: `f(1, "a")`. The one `BINDING_CONTRACT.md` quotes.
#[test]
fn contract_vector() {
    assert_eq!(encode(&[arg(&1_i64), arg(&"a")]), "028282016161a0");
}

/// Case `single-object-arg`: a struct keeps **declaration** order.
///
/// `order_id` precedes `amount_cents` in the pinned bytes, which is not sorted
/// order. A serializer that sorted its keys would still interoperate and would
/// silently stop `auto:` idempotency keys deduping across languages, so this is
/// the case worth staring at.
#[test]
fn single_object_arg() {
    #[derive(Serialize)]
    struct Order {
        order_id: &'static str,
        amount_cents: i64,
    }

    let order = Order {
        order_id: "ord-0001",
        amount_cents: 1000,
    };
    assert_eq!(
        encode(&[arg(&order)]),
        "028281a2686f726465725f6964686f72642d303030316c616d6f756e745f63656e74731903e8a0"
    );
}

/// Case `null-and-bools`: `f(None, true, false)`.
#[test]
fn null_and_bools() {
    assert_eq!(
        encode(&[arg(&Option::<i64>::None), arg(&true), arg(&false)]),
        "028283f6f5f4a0"
    );
}

/// Case `nested-structures`: `f({"a": [1, 2, {"b": "c"}]})`.
#[test]
fn nested_structures() {
    #[derive(Serialize)]
    struct Inner {
        b: &'static str,
    }
    #[derive(Serialize)]
    struct Outer {
        a: (i64, i64, Inner),
    }

    let value = Outer {
        a: (1, 2, Inner { b: "c" }),
    };
    assert_eq!(encode(&[arg(&value)]), "028281a16161830102a161626163a0");
}

/// Case `unicode-and-empty-string`: `f("", "héllo")`.
#[test]
fn unicode_and_empty_string() {
    assert_eq!(
        encode(&[arg(&""), arg(&"héllo")]),
        "028282606668c3a96c6c6fa0"
    );
}

/// Case `negative-int`: `f(-1, -1000)`, major type 1 in shortest form.
#[test]
fn negative_int() {
    assert_eq!(encode(&[arg(&-1_i64), arg(&-1000_i64)]), "028282203903e7a0");
}

/// Cases `kwargs-only` and `positional-and-kwargs`.
///
/// A Rust producer never writes these — the language has no keyword arguments,
/// so [`encode_args`] always sends an empty map. They are asserted through
/// core's writer directly to prove the shell's [`to_wire`] produces values a
/// kwargs-carrying producer could use, and that the empty-map tail in every
/// other case is a real choice rather than the only thing that works.
#[test]
fn kwargs_are_encodable_even_though_rust_sends_none() {
    use flexiq_core::wire::encode_call;

    let kwargs = vec![("k".to_string(), arg(&true))];
    assert_eq!(hex::encode(encode_call(&[], &kwargs)), "028280a1616bf5");
    assert_eq!(
        hex::encode(encode_call(&[arg(&1_i64), arg(&"a")], &kwargs)),
        "028282016161a1616bf5"
    );
}

/// A `u64` above `i64::MAX` has no representation in the envelope's integer
/// arm, so it is refused rather than wrapped into a negative.
#[test]
fn an_oversized_unsigned_is_refused() {
    let err = to_wire(&(i64::MAX as u64 + 1)).expect_err("out of range");
    assert!(err.to_string().contains("out of range"), "{err}");
}

/// The envelope's maps are text-keyed, so a map with integer keys is refused
/// rather than coerced into one with stringified keys.
#[test]
fn a_non_string_map_key_is_refused() {
    let mut map = std::collections::BTreeMap::new();
    map.insert(1_i64, "one");
    let err = to_wire(&map).expect_err("non-string key");
    assert!(err.to_string().contains("key"), "{err}");
}

// ── Decoding ─────────────────────────────────────────────────────────
//
// Core ships no reader — `wire/cbor.rs`: "There is no reader." A Rust worker is
// the first thing in the tree that has to decode one of these, and where the
// writer has exactly one legal output, a reader has to take everything any
// writer may legally emit.

/// What a handler does: decode the payload back into its parameter types.
#[test]
fn a_positional_call_round_trips() {
    let payload = encode_args(&[arg(&1_i64), arg(&"a")]);
    let (n, s): (i64, String) = decode_args(&payload).expect("decodes");
    assert_eq!(n, 1);
    assert_eq!(s, "a");
}

/// Case `float`: the pinned bytes are a double.
#[test]
fn the_pinned_float_decodes() {
    let payload = hex::decode("028281fb3ff8000000000000a0").expect("valid hex");
    let (f,): (f64,) = decode_args(&payload).expect("decodes");
    assert_eq!(f, 1.5);
}

/// The same value at half precision.
///
/// Not a vector — the file pins one width and says a writer may choose a
/// narrower one, so this is the half of that rule a reader has to satisfy and
/// no vector can express. `f9 3e 00` is 1.5 in binary16.
#[test]
fn a_narrower_float_decodes_to_the_same_value() {
    let payload = hex::decode("028281f93e00a0").expect("valid hex");
    let (f,): (f64,) = decode_args(&payload).expect("decodes");
    assert_eq!(f, 1.5);
}

/// Case `int-beyond-double-precision`: 9007199254740993, one past 2^53.
///
/// The value is asserted here even though the vectors file cannot assert it:
/// JSON would lose the last digit, Rust's `i64` does not.
#[test]
fn an_integer_past_double_precision_keeps_its_last_digit() {
    let payload = hex::decode("0282811b0020000000000001a0").expect("valid hex");
    let (n,): (i64,) = decode_args(&payload).expect("decodes");
    assert_eq!(n, 9_007_199_254_740_993);
}

/// Every `decode_only` vector, decoded and re-encoded to the same bytes.
///
/// `round_trip_only` is the file's way of saying a value has no cross-language
/// spelling — a byte string surfaces differently in every runtime — so the
/// assertion is that this crate's two halves agree, not that a literal matches.
#[test]
fn every_decode_only_vector_round_trips() {
    for hex_text in [
        "028281fb3ff8000000000000a0", // float
        "0282811b0020000000000001a0", // int-beyond-double-precision
        "028281420102a0",             // byte-string
    ] {
        let payload = hex::decode(hex_text).expect("valid hex");
        let args: Vec<ciborium::Value> = decode_args(&payload).expect("decodes");
        let rewritten: Vec<WireValue> = args.iter().map(arg).collect();
        assert_eq!(
            hex::encode(encode_args(&rewritten)),
            hex_text,
            "{hex_text} did not survive a decode/encode round trip"
        );
    }
}

/// A payload whose leading byte names a codec this shell does not read.
#[test]
fn an_unknown_codec_tag_is_refused_by_number() {
    let err = decode_args::<(i64,)>(&[0x7f, 0x00]).expect_err("unknown tag");
    assert!(err.to_string().contains("0x7f"), "{err}");
}

/// An empty payload carries no tag at all, which is a different fault from a
/// tag this build does not know.
#[test]
fn an_empty_payload_is_refused() {
    let err = decode_args::<(i64,)>(&[]).expect_err("no tag");
    assert!(err.to_string().contains("empty"), "{err}");
}

/// Arguments that do not fit the handler's parameters.
#[test]
fn a_shape_mismatch_names_the_arguments() {
    let payload = encode_args(&[arg(&"not a number")]);
    let err = decode_args::<(i64,)>(&payload).expect_err("wrong type");
    assert!(err.to_string().contains("arguments"), "{err}");
}
