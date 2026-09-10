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

use flexiq::__private::{encode_args, to_wire, WireValue};
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
