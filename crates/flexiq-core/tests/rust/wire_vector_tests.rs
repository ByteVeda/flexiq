//! The Rust envelope encoder against `contracts/wire-vectors.json`.
//!
//! Every SDK asserts this file in its own suite, so a runtime whose encoding
//! drifts fails its own build instead of quietly producing payloads its peers
//! cannot read. This is that assertion for the Rust core.
//!
//! The vectors are read with an order-preserving visitor rather than through
//! `serde_json::Value`, which sorts object keys: one case pins a two-key map in
//! the order it was written, and comparing against sorted bytes would assert
//! the wrong thing.

use std::fmt;

use flexiq_core::wire::{encode_call, WireValue};
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

const VECTORS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../contracts/wire-vectors.json"
));

#[test]
fn every_encode_vector_reproduces_its_pinned_bytes() {
    let vectors = load();
    assert!(!vectors.encode.is_empty(), "no encode cases were parsed");

    for case in &vectors.encode {
        let args: Vec<WireValue> = case.args.iter().map(|arg| arg.0.clone()).collect();
        let encoded = hex(&encode_call(&args, &kwargs_of(case)));
        assert_eq!(
            encoded, case.hex,
            "vector `{}` encoded differently",
            case.name
        );
    }
}

/// The two `round_trip_only` cases are reachable from Rust, and that is the
/// point of `raw`.
///
/// A byte string and an integer past 2^53 are pinned as decode-only because
/// JSON cannot hold either — which is what a JSON-shaped producer, including
/// the gRPC `structured` arm, runs into. The encoder itself has no such
/// ceiling, so an SDK sending `raw` loses nothing.
#[test]
fn the_round_trip_only_vectors_are_reachable_from_rust() {
    let vectors = load();

    let big = pinned(&vectors, "int-beyond-double-precision");
    assert_eq!(
        hex(&encode_call(
            &[WireValue::Integer(9_007_199_254_740_993)],
            &[]
        )),
        big
    );

    let bytes = pinned(&vectors, "byte-string");
    assert_eq!(
        hex(&encode_call(&[WireValue::Bytes(vec![1, 2])], &[])),
        bytes
    );
}

fn load() -> Vectors {
    serde_json::from_str(VECTORS).expect("contracts/wire-vectors.json is not the shape expected")
}

/// The hex of a `decode_only` case, by name.
fn pinned(vectors: &Vectors, name: &str) -> String {
    vectors
        .decode_only
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("no decode_only vector named `{name}`"))
        .hex
        .clone()
}

fn kwargs_of(case: &EncodeCase) -> Vec<(String, WireValue)> {
    match &case.kwargs.0 {
        WireValue::Map(entries) => entries.clone(),
        other => panic!("vector `{}` has non-object kwargs: {other:?}", case.name),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Deserialize)]
struct Vectors {
    encode: Vec<EncodeCase>,
    decode_only: Vec<DecodeCase>,
}

#[derive(Deserialize)]
struct EncodeCase {
    name: String,
    args: Vec<Json>,
    kwargs: Json,
    hex: String,
}

#[derive(Deserialize)]
struct DecodeCase {
    name: String,
    hex: String,
}

/// A JSON value read straight into a [`WireValue`], keys in document order.
struct Json(WireValue);

impl<'de> Deserialize<'de> for Json {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(JsonVisitor).map(Json)
    }
}

struct JsonVisitor;

impl<'de> Visitor<'de> for JsonVisitor {
    type Value = WireValue;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(WireValue::Null)
    }

    fn visit_bool<E: de::Error>(self, value: bool) -> Result<Self::Value, E> {
        Ok(WireValue::Bool(value))
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
        Ok(WireValue::Integer(value))
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
        i64::try_from(value)
            .map(WireValue::Integer)
            .map_err(|_| E::custom("a vector holds an integer wider than i64"))
    }

    fn visit_f64<E: de::Error>(self, value: f64) -> Result<Self::Value, E> {
        Ok(WireValue::Float(value))
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        Ok(WireValue::Text(value.to_owned()))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut items = Vec::new();
        while let Some(Json(item)) = seq.next_element()? {
            items.push(item);
        }
        Ok(WireValue::Array(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut entries = Vec::new();
        while let Some((key, Json(value))) = map.next_entry::<String, Json>()? {
            entries.push((key, value));
        }
        Ok(WireValue::Map(entries))
    }
}
