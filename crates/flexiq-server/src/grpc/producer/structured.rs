//! The `structured` body arm: protobuf values in, the payload envelope out.
//!
//! The envelope itself is built by `flexiq_core::wire`, the same encoder a Rust
//! producer calls — there is one implementation of the format in this tree, and
//! this module only decides which values may reach it.
//!
//! What it refuses, it refuses on purpose. `google.protobuf.Value` is JSON's
//! type system, and `contracts/wire-vectors.json` already records the three
//! things JSON cannot hold: an integer past the exact range of a double, a byte
//! string, and a value whose precision a double does not keep. The first is
//! rejected here rather than truncated; the other two have no `Value` arm to
//! arrive through. A client that needs them sends `raw`.

use flexiq_core::wire::{encode_call, WireValue};
use prost_types::value::Kind;
use prost_types::{NullValue, Value};

use crate::grpc::pb;
use crate::grpc::status::WireError;

/// The largest integer a `double` represents unambiguously, 2^53 - 1.
///
/// 2^53 itself is excluded deliberately. It is exactly representable, but it is
/// also what 2^53 + 1 rounds to, so a server that accepted it could not tell
/// the two apart — and answering a request that said 9007199254740993 with a
/// job carrying 9007199254740992 is the silent corruption this door exists to
/// avoid.
const MAX_EXACT_INTEGER: f64 = 9_007_199_254_740_991.0;

/// Encode structured arguments into the payload envelope.
///
/// The result is byte-identical to what an SDK would have sent through `raw`
/// for the same call, with one documented exception: object keys arrive in a
/// protobuf map and leave in sorted order, because a protobuf map does not
/// carry the order they were written in.
pub fn encode(args: pb::StructuredArgs) -> Result<Vec<u8>, WireError> {
    let positional = args
        .args
        .iter()
        .map(convert)
        .collect::<Result<Vec<_>, _>>()?;
    let keyword = args
        .kwargs
        .iter()
        .map(|(key, value)| Ok((key.clone(), convert(value)?)))
        .collect::<Result<Vec<_>, WireError>>()?;

    Ok(encode_call(&positional, &keyword))
}

/// One `google.protobuf.Value`, or the reason it cannot become a CBOR one.
fn convert(value: &Value) -> Result<WireValue, WireError> {
    match value.kind.as_ref() {
        // Proto3 gives a message-typed field presence, and an unset oneof is
        // not an implicit null: a client that meant null says so.
        None => Err(WireError::invalid_request(
            "a structured argument has no kind set; every google.protobuf.Value needs one of its arms",
        )),
        Some(Kind::NullValue(code)) => {
            if *code == NullValue::NullValue as i32 {
                Ok(WireValue::Null)
            } else {
                Err(WireError::invalid_request(format!(
                    "a structured argument carries null_value {code}, which is not a value this contract knows"
                )))
            }
        }
        Some(Kind::BoolValue(flag)) => Ok(WireValue::Bool(*flag)),
        Some(Kind::NumberValue(number)) => convert_number(*number),
        Some(Kind::StringValue(text)) => Ok(WireValue::Text(text.clone())),
        Some(Kind::ListValue(list)) => list
            .values
            .iter()
            .map(convert)
            .collect::<Result<Vec<_>, _>>()
            .map(WireValue::Array),
        // Sorted, because prost decodes a protobuf map into a BTreeMap — the
        // order the client wrote is not on the wire to recover.
        Some(Kind::StructValue(object)) => object
            .fields
            .iter()
            .map(|(key, field)| Ok((key.clone(), convert(field)?)))
            .collect::<Result<Vec<_>, WireError>>()
            .map(WireValue::Map),
    }
}

/// A JSON number: a CBOR integer when it is one, a double when it is not.
///
/// A client cannot say "1.0, and I mean a float" — JSON cannot either, and the
/// pinned vectors encode `1` as a CBOR integer. So an integral value becomes an
/// integer, which is also what every SDK produces for the same call.
fn convert_number(number: f64) -> Result<WireValue, WireError> {
    if !number.is_finite() {
        return Err(WireError::invalid_request(
            "a structured argument is NaN or infinite; JSON cannot express either, so send the call through `raw`",
        ));
    }

    if number.fract() != 0.0 {
        return Ok(WireValue::Float(number));
    }

    if number.abs() > MAX_EXACT_INTEGER {
        return Err(WireError::invalid_request(format!(
            "a structured argument is the integer {number:.0}, which is beyond the {MAX_EXACT_INTEGER:.0} a double holds exactly; \
             send the call through `raw` rather than have it silently rounded"
        )));
    }

    Ok(WireValue::Integer(number as i64))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use prost_types::{ListValue, Struct};

    use super::*;

    /// The cross-SDK vectors, read from the same file every SDK asserts.
    const VECTORS: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../contracts/wire-vectors.json"
    ));

    /// `single-object-arg` with its two keys sorted, which is the only way a
    /// protobuf map can deliver them.
    ///
    /// The pinned vector writes `order_id` first because every SDK's map keeps
    /// insertion order; a protobuf map does not carry one. The call decodes to
    /// the same thing either way — this pins the fact that the *only*
    /// difference is the order, so a change to anything else still fails.
    const SINGLE_OBJECT_ARG_SORTED: &str =
        "028281a26c616d6f756e745f63656e74731903e8686f726465725f6964686f72642d30303031a0";

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn value(kind: Kind) -> Value {
        Value { kind: Some(kind) }
    }

    /// A `serde_json` value as the protobuf one a client would have sent.
    fn from_json(json: &serde_json::Value) -> Value {
        match json {
            serde_json::Value::Null => value(Kind::NullValue(NullValue::NullValue as i32)),
            serde_json::Value::Bool(flag) => value(Kind::BoolValue(*flag)),
            serde_json::Value::Number(number) => value(Kind::NumberValue(
                number.as_f64().expect("a vector holds a JSON number"),
            )),
            serde_json::Value::String(text) => value(Kind::StringValue(text.clone())),
            serde_json::Value::Array(items) => value(Kind::ListValue(ListValue {
                values: items.iter().map(from_json).collect(),
            })),
            serde_json::Value::Object(fields) => value(Kind::StructValue(Struct {
                fields: fields
                    .iter()
                    .map(|(key, field)| (key.clone(), from_json(field)))
                    .collect(),
            })),
        }
    }

    fn args_of(case: &serde_json::Value) -> pb::StructuredArgs {
        pb::StructuredArgs {
            args: case["args"]
                .as_array()
                .expect("every encode case has args")
                .iter()
                .map(from_json)
                .collect(),
            kwargs: case["kwargs"]
                .as_object()
                .expect("every encode case has kwargs")
                .iter()
                .map(|(key, field)| (key.clone(), from_json(field)))
                .collect(),
        }
    }

    #[test]
    fn every_encode_vector_reproduces_its_pinned_bytes() {
        let vectors: serde_json::Value =
            serde_json::from_str(VECTORS).expect("the vectors file is JSON");
        let cases = vectors["encode"].as_array().expect("encode is an array");
        assert!(!cases.is_empty(), "no encode cases were parsed");

        for case in cases {
            let name = case["name"].as_str().expect("every case is named");
            let encoded = hex(&encode(args_of(case)).expect("a vector is encodable"));

            // The one case whose bytes depend on a map key order the wire does
            // not carry. Everything else is byte-identical to what an SDK sends.
            let expected = match name {
                "single-object-arg" => SINGLE_OBJECT_ARG_SORTED,
                _ => case["hex"].as_str().expect("every case pins bytes"),
            };
            assert_eq!(encoded, expected, "vector `{name}` encoded differently");
        }
    }

    #[test]
    fn structured_and_raw_agree_on_the_same_call() {
        // f(1, "a") — the vector in BINDING_CONTRACT.md, as a client would send
        // each arm.
        let structured = encode(pb::StructuredArgs {
            args: vec![
                value(Kind::NumberValue(1.0)),
                value(Kind::StringValue("a".into())),
            ],
            kwargs: BTreeMap::new(),
        })
        .expect("encodable");

        assert_eq!(structured, [0x02, 0x82, 0x82, 0x01, 0x61, 0x61, 0xa0]);
    }

    #[test]
    fn an_integer_past_the_exact_range_is_refused_not_rounded() {
        // 9007199254740993 is not representable; it arrives as 2^53. Accepting
        // it would answer with a job carrying a different number.
        let error = encode(pb::StructuredArgs {
            args: vec![value(Kind::NumberValue(9_007_199_254_740_993.0))],
            kwargs: BTreeMap::new(),
        })
        .expect_err("must be refused");

        assert!(
            error.message().contains("beyond"),
            "unexpected message: {}",
            error.message()
        );
    }

    #[test]
    fn the_largest_exact_integer_is_still_accepted() {
        let encoded = encode(pb::StructuredArgs {
            args: vec![value(Kind::NumberValue(9_007_199_254_740_991.0))],
            kwargs: BTreeMap::new(),
        })
        .expect("2^53 - 1 is exact");

        assert_eq!(hex(&encoded), "0282811b001fffffffffffffa0");
    }

    #[test]
    fn a_fractional_number_stays_a_float() {
        let encoded = encode(pb::StructuredArgs {
            args: vec![value(Kind::NumberValue(1.5))],
            kwargs: BTreeMap::new(),
        })
        .expect("encodable");

        assert_eq!(hex(&encoded), "028281fb3ff8000000000000a0");
    }

    #[test]
    fn non_finite_numbers_are_refused() {
        for number in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let error = encode(pb::StructuredArgs {
                args: vec![value(Kind::NumberValue(number))],
                kwargs: BTreeMap::new(),
            })
            .expect_err("must be refused");
            assert!(error.message().contains("NaN or infinite"));
        }
    }

    #[test]
    fn a_value_with_no_kind_is_refused() {
        let error = encode(pb::StructuredArgs {
            args: vec![Value { kind: None }],
            kwargs: BTreeMap::new(),
        })
        .expect_err("must be refused");

        assert!(error.message().contains("no kind set"));
    }

    #[test]
    fn an_unknown_null_value_enumerator_is_refused() {
        let error = encode(pb::StructuredArgs {
            args: vec![value(Kind::NullValue(7))],
            kwargs: BTreeMap::new(),
        })
        .expect_err("must be refused");

        assert!(error.message().contains("null_value 7"));
    }
}
