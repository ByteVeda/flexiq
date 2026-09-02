//! The well-known types, in the spellings proto3 JSON gives them.
//!
//! A `google.protobuf.Timestamp` is an RFC 3339 string, a `Duration` is a
//! decimal number of seconds with an `s`, `bytes` is base64 and a `Value` is
//! whatever JSON value it holds. None of that is guessable from the protobuf
//! encoding, and a client with no `.proto` has only these spellings to go on —
//! so they live in one module with the rules that produced them written down,
//! rather than inline at twenty field conversions.
//!
//! Every reader here is **permissive in the ways the specification requires and
//! in no others**: base64 in either alphabet with or without padding, an
//! `int64` as a string or a number, an RFC 3339 instant at any offset. Anything
//! else is refused, because a facade that guesses is a facade that enqueues a
//! job the caller did not describe.

use base64::Engine as _;
use chrono::{DateTime, SecondsFormat, Utc};
use prost_types::{Duration as ProtoDuration, ListValue, Struct, Timestamp, Value};
use serde::de::{self, Deserializer, Visitor};
use serde::Deserialize;

/// Nanoseconds in a second, the unit both well-known time types count in.
const NANOS_PER_SECOND: u32 = 1_000_000_000;

/// What bytes are written as: standard alphabet, padded.
const BASE64_WRITE: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::STANDARD;

/// What bytes are read as. proto3 JSON accepts both alphabets and treats
/// padding as optional, so a client that reached for its language's URL-safe
/// encoder is not refused over a character it never chose.
const BASE64_READ: [base64::engine::general_purpose::GeneralPurpose; 2] = [
    base64::engine::general_purpose::STANDARD_PAD_INDIFFERENT,
    base64::engine::general_purpose::URL_SAFE_PAD_INDIFFERENT,
];

// ── Timestamp ────────────────────────────────────────────────────────

/// An instant as RFC 3339, or `None` if the message does not hold one.
///
/// `SecondsFormat::AutoSi` is exactly proto3 JSON's rule — zero, three, six or
/// nine fractional digits, whichever shows every non-zero one — so a storage
/// value in milliseconds renders as `…:00.123Z` and a whole second as `…:00Z`.
///
/// `None` means the message is not a valid `Timestamp` (nanos outside
/// `0..1_000_000_000`, or a year past what a date can express). That is a
/// server-side bug rather than anything a caller did, and omitting the field is
/// the only answer that is not actively wrong: a fallback instant would be a
/// time the job does not have.
pub fn timestamp_to_json(value: &Timestamp) -> Option<String> {
    let nanos = u32::try_from(value.nanos).ok()?;
    if nanos >= NANOS_PER_SECOND {
        return None;
    }
    DateTime::<Utc>::from_timestamp(value.seconds, nanos)
        .map(|moment| moment.to_rfc3339_opts(SecondsFormat::AutoSi, true))
}

/// An RFC 3339 instant, at any offset, as a `Timestamp`.
///
/// A leap second is refused rather than folded into the second before it: this
/// module is permissive only where the specification says to be, and quietly
/// answering `:60` with `:59.999999999` schedules a job at an instant the
/// caller did not write.
pub fn timestamp_from_json(text: &str) -> Result<Timestamp, String> {
    let moment = DateTime::parse_from_rfc3339(text).map_err(|error| {
        format!("`{text}` is not an RFC 3339 instant, which is how a timestamp is written: {error}")
    })?;
    // A leap second lands in the 1_000_000_000..2_000_000_000 range chrono
    // reserves for one, which is not a value a Timestamp may carry.
    let nanos = moment.timestamp_subsec_nanos();
    if nanos >= NANOS_PER_SECOND {
        return Err(format!(
            "`{text}` is a leap second, which a timestamp cannot carry"
        ));
    }
    Ok(Timestamp {
        seconds: moment.timestamp(),
        // The guard above puts this below `NANOS_PER_SECOND`, so it fits.
        nanos: i32::try_from(nanos).unwrap_or_default(),
    })
}

// ── Duration ─────────────────────────────────────────────────────────

/// A duration as seconds with an `s`, to zero, three, six or nine decimals.
///
/// The two halves are added up before being split again, rather than being
/// signed and formatted where they sit. A canonical `Duration` signs both the
/// same way — but the conversion from milliseconds uses Euclidean division, so
/// a negative one arrives as a negative `seconds` beside a **positive**
/// `nanos`, and formatting that pair as it stands would move the value by a
/// second. Summing first is correct for both spellings and costs one `i128`.
pub fn duration_to_json(value: &ProtoDuration) -> String {
    let total = i128::from(value.seconds) * i128::from(NANOS_PER_SECOND) + i128::from(value.nanos);
    let sign = if total < 0 { "-" } else { "" };
    let magnitude = total.unsigned_abs();
    let seconds = magnitude / u128::from(NANOS_PER_SECOND);
    let nanos = (magnitude % u128::from(NANOS_PER_SECOND)) as u32;
    if nanos == 0 {
        format!("{sign}{seconds}s")
    } else if nanos.is_multiple_of(1_000_000) {
        format!("{sign}{seconds}.{:03}s", nanos / 1_000_000)
    } else if nanos.is_multiple_of(1_000) {
        format!("{sign}{seconds}.{:06}s", nanos / 1_000)
    } else {
        format!("{sign}{seconds}.{nanos:09}s")
    }
}

/// A duration written as seconds with an `s`.
///
/// The suffix is mandatory: proto3 JSON has no bare-number form, and accepting
/// one would leave `30` meaning seconds here and milliseconds in the dashboard
/// API, which is the kind of ambiguity a unit suffix exists to end.
pub fn duration_from_json(text: &str) -> Result<ProtoDuration, String> {
    let complaint = || {
        format!("`{text}` is not a duration; write seconds with an `s`, as in `30s` or `1.500s`")
    };

    let body = text.strip_suffix('s').ok_or_else(complaint)?;
    let (negative, body) = match body.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, body),
    };
    let (whole, fraction) = match body.split_once('.') {
        Some((whole, fraction)) => (whole, fraction),
        None => (body, ""),
    };
    if whole.is_empty() || !whole.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(complaint());
    }
    if fraction.len() > 9 || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(complaint());
    }

    let seconds: i64 = whole.parse().map_err(|_| complaint())?;
    // Right-pad to nanoseconds: `.5` is 500 000 000ns, not 5.
    let nanos: u32 = if fraction.is_empty() {
        0
    } else {
        format!("{fraction:0<9}").parse().map_err(|_| complaint())?
    };

    let nanos = i32::try_from(nanos).map_err(|_| complaint())?;
    Ok(ProtoDuration {
        seconds: if negative { -seconds } else { seconds },
        nanos: if negative { -nanos } else { nanos },
    })
}

// ── bytes ────────────────────────────────────────────────────────────

/// Opaque bytes as base64.
pub fn bytes_to_json(bytes: &[u8]) -> String {
    BASE64_WRITE.encode(bytes)
}

/// base64 in either alphabet, padded or not.
pub fn bytes_from_json(text: &str) -> Result<Vec<u8>, String> {
    BASE64_READ
        .iter()
        .find_map(|engine| engine.decode(text).ok())
        .ok_or_else(|| "expected base64".to_string())
}

// ── Value ────────────────────────────────────────────────────────────

/// A JSON value as a `google.protobuf.Value`.
///
/// Every JSON value has an arm, so this cannot fail on shape. What it can lose
/// is precision — a JSON number is a double here, exactly as it is in the
/// protobuf type — and that loss is caught downstream, where an integer past
/// the range a double holds exactly is refused rather than rounded.
pub fn value_from_json(value: serde_json::Value) -> Result<Value, String> {
    use prost_types::value::Kind;

    let kind = match value {
        serde_json::Value::Null => Kind::NullValue(prost_types::NullValue::NullValue as i32),
        serde_json::Value::Bool(flag) => Kind::BoolValue(flag),
        serde_json::Value::Number(number) => Kind::NumberValue(
            number
                .as_f64()
                .ok_or_else(|| format!("`{number}` is not a number a double can hold"))?,
        ),
        serde_json::Value::String(text) => Kind::StringValue(text),
        serde_json::Value::Array(items) => Kind::ListValue(ListValue {
            values: items
                .into_iter()
                .map(value_from_json)
                .collect::<Result<Vec<_>, _>>()?,
        }),
        serde_json::Value::Object(fields) => Kind::StructValue(Struct {
            fields: fields
                .into_iter()
                .map(|(key, field)| Ok((key, value_from_json(field)?)))
                .collect::<Result<_, String>>()?,
        }),
    };
    Ok(Value { kind: Some(kind) })
}

// ── The reading side, as serde newtypes ──────────────────────────────
//
// A request message is a serde struct, so each of the readers above needs a
// type serde can name. They are newtypes rather than `deserialize_with`
// functions because `Option<T>` then works without a second wrapper per field.

/// An RFC 3339 instant in a request body.
#[derive(Debug, Clone)]
pub struct JsonTimestamp(pub Timestamp);

impl<'de> Deserialize<'de> for JsonTimestamp {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        timestamp_from_json(&text)
            .map(Self)
            .map_err(de::Error::custom)
    }
}

/// A duration in a request body.
#[derive(Debug, Clone)]
pub struct JsonDuration(pub ProtoDuration);

impl<'de> Deserialize<'de> for JsonDuration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        duration_from_json(&text)
            .map(Self)
            .map_err(de::Error::custom)
    }
}

/// base64 bytes in a request body.
#[derive(Debug, Clone)]
pub struct JsonBytes(pub Vec<u8>);

impl<'de> Deserialize<'de> for JsonBytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        bytes_from_json(&text).map(Self).map_err(de::Error::custom)
    }
}

/// A 64-bit integer in a request body, written as a string or as a number.
///
/// proto3 JSON writes 64-bit integers as strings, because a JSON number is a
/// double and cannot hold all of them — so a client that reads one of our
/// responses and sends the value back must be able to send the string form. The
/// number form is accepted too, since it is what a hand-written body carries.
#[derive(Debug, Clone, Copy)]
pub struct JsonInt64(pub i64);

impl<'de> Deserialize<'de> for JsonInt64 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Either;

        impl Visitor<'_> for Either {
            type Value = JsonInt64;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a 64-bit integer, as a number or as a string")
            }

            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
                Ok(JsonInt64(value))
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
                i64::try_from(value).map(JsonInt64).map_err(E::custom)
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                value.parse().map(JsonInt64).map_err(E::custom)
            }
        }

        deserializer.deserialize_any(Either)
    }
}

/// One `google.protobuf.Value` in a request body.
#[derive(Debug, Clone)]
pub struct JsonValue(pub Value);

impl<'de> Deserialize<'de> for JsonValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        value_from_json(value).map(Self).map_err(de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_instant_carries_only_the_fractional_digits_it_needs() {
        let cases = [
            (0, 0, "1970-01-01T00:00:00Z"),
            (1_756_900_000, 0, "2025-09-03T11:46:40Z"),
            (1_756_900_000, 123_000_000, "2025-09-03T11:46:40.123Z"),
            (1_756_900_000, 123_456_000, "2025-09-03T11:46:40.123456Z"),
            (1_756_900_000, 123_456_789, "2025-09-03T11:46:40.123456789Z"),
        ];
        for (seconds, nanos, expected) in cases {
            let value = Timestamp { seconds, nanos };
            assert_eq!(timestamp_to_json(&value).as_deref(), Some(expected));
        }
    }

    #[test]
    fn an_instant_round_trips_through_its_string() {
        for millis in [0_i64, 1_756_900_000_123, -86_400_000] {
            let value = crate::grpc::producer::convert::timestamp(millis);
            let text = timestamp_to_json(&value).expect("a valid instant renders");
            assert_eq!(timestamp_from_json(&text).expect("it reads back"), value);
        }
    }

    #[test]
    fn an_offset_is_normalised_to_utc() {
        let value = timestamp_from_json("2025-09-03T18:00:00+05:30").expect("a valid offset");
        assert_eq!(
            timestamp_to_json(&value).as_deref(),
            Some("2025-09-03T12:30:00Z")
        );
    }

    /// chrono reads `:60` and reports it as a nanosecond past the second it
    /// follows. Clamping that into range would hand back a different instant
    /// than the one asked for, so the string is refused instead.
    #[test]
    fn a_leap_second_is_refused_rather_than_moved() {
        let complaint =
            timestamp_from_json("2016-12-31T23:59:60Z").expect_err("a leap second is not a moment");
        assert!(complaint.contains("leap second"), "{complaint}");
    }

    /// A malformed `Timestamp` is a server bug, and the answer is to say
    /// nothing rather than to say a time the job does not have.
    #[test]
    fn an_impossible_instant_renders_as_nothing() {
        assert_eq!(
            timestamp_to_json(&Timestamp {
                seconds: 0,
                nanos: -1
            }),
            None
        );
        assert_eq!(
            timestamp_to_json(&Timestamp {
                seconds: 0,
                nanos: 1_000_000_000
            }),
            None
        );
    }

    #[test]
    fn a_duration_carries_only_the_fractional_digits_it_needs() {
        let cases = [
            (30, 0, "30s"),
            (0, 500_000_000, "0.500s"),
            (1, 123_456_000, "1.123456s"),
            (1, 123_456_789, "1.123456789s"),
            (-1, -500_000_000, "-1.500s"),
            (0, 0, "0s"),
        ];
        for (seconds, nanos, expected) in cases {
            assert_eq!(
                duration_to_json(&ProtoDuration { seconds, nanos }),
                expected
            );
        }
    }

    /// The round trip that matters is in milliseconds, because milliseconds are
    /// what storage holds — and the negative case is exactly where the two
    /// spellings of one duration meet.
    #[test]
    fn a_duration_round_trips_through_its_string() {
        use crate::grpc::producer::convert::{duration, millis_from_duration};

        for millis in [0_i64, 30_000, 1_500, -1_500, 3_600_000] {
            let text = duration_to_json(&duration(millis));
            let read_back = duration_from_json(&text).expect("it reads back");
            assert_eq!(millis_from_duration(&read_back), millis, "text: {text}");
        }
    }

    /// A `Duration` whose halves disagree in sign is what Euclidean division
    /// produces, and it is a second away from what naive formatting would say.
    #[test]
    fn a_negative_duration_is_summed_before_it_is_split() {
        assert_eq!(
            duration_to_json(&ProtoDuration {
                seconds: -2,
                nanos: 500_000_000
            }),
            "-1.500s"
        );
    }

    /// `.5s` is half a second. Reading the fraction as a plain integer would
    /// make it five nanoseconds, which is the bug this pads against.
    #[test]
    fn a_fraction_is_padded_to_nanoseconds_and_not_parsed_as_an_integer() {
        assert_eq!(
            duration_from_json("0.5s").expect("valid"),
            ProtoDuration {
                seconds: 0,
                nanos: 500_000_000
            }
        );
    }

    #[test]
    fn a_duration_without_its_unit_is_refused() {
        for text in [
            "30",
            "",
            "s",
            "-s",
            "1.2345678901s",
            "1.2.3s",
            "abcs",
            "1e3s",
        ] {
            assert!(duration_from_json(text).is_err(), "text: {text:?}");
        }
    }

    /// The sign is stripped before the magnitude is parsed, so the negation at
    /// the end never sees a value `i64` cannot hold: `i64::MIN` seconds is
    /// refused by the parse itself, and the largest magnitude that does parse
    /// negates exactly.
    #[test]
    fn the_signed_boundary_is_refused_by_the_parse_and_not_by_the_negation() {
        assert!(duration_from_json("-9223372036854775808s").is_err());
        assert_eq!(
            duration_from_json("-9223372036854775807s").expect("the largest magnitude"),
            ProtoDuration {
                seconds: -9_223_372_036_854_775_807,
                nanos: 0
            }
        );
    }

    #[test]
    fn bytes_read_back_from_either_alphabet_and_either_padding() {
        let payload = vec![0xfb_u8, 0xff, 0x00, 0x3e];
        let standard = bytes_to_json(&payload);
        assert_eq!(standard, "+/8APg==");
        for encoded in ["+/8APg==", "+/8APg", "-_8APg==", "-_8APg"] {
            assert_eq!(bytes_from_json(encoded).expect("valid base64"), payload);
        }
        assert!(bytes_from_json("not base64!").is_err());
    }

    #[test]
    fn a_json_value_keeps_its_shape() {
        use prost_types::value::Kind;

        let value = value_from_json(serde_json::json!({"a": [1, "x", null, true]}))
            .expect("every JSON value has an arm");
        let Some(Kind::StructValue(object)) = value.kind else {
            panic!("an object becomes a Struct");
        };
        let Some(Kind::ListValue(list)) = object.fields["a"].kind.clone() else {
            panic!("an array becomes a ListValue");
        };
        assert_eq!(list.values.len(), 4);
        assert!(matches!(list.values[0].kind, Some(Kind::NumberValue(n)) if n == 1.0));
    }

    #[test]
    fn a_64_bit_integer_reads_from_a_string_or_a_number() {
        let from_number: JsonInt64 = serde_json::from_str("42").expect("a number");
        let from_string: JsonInt64 = serde_json::from_str("\"42\"").expect("a string");
        assert_eq!(from_number.0, 42);
        assert_eq!(from_string.0, 42);
        assert!(serde_json::from_str::<JsonInt64>("\"nope\"").is_err());
    }
}
