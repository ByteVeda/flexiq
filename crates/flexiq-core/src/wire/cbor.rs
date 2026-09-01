//! A CBOR (RFC 8949) writer for [`WireValue`], and nothing else.
//!
//! Two rules from `BINDING_CONTRACT.md` are structural here rather than
//! configurable, because a writer that gets either wrong still interoperates
//! and silently stops `auto:` idempotency keys deduping across SDKs:
//!
//! * every array and map carries a **definite-length** head;
//! * every integer argument uses the **shortest form** that holds it.
//!
//! There is no reader. Nothing in this crate decodes a payload — the shells do,
//! each with its language's CBOR library.

use super::value::WireValue;

/// Major type 0: a non-negative integer.
const MAJOR_UNSIGNED: u8 = 0;
/// Major type 1: a negative integer, encoded as `-1 - n`.
const MAJOR_NEGATIVE: u8 = 1;
/// Major type 2: a byte string.
const MAJOR_BYTES: u8 = 2;
/// Major type 3: a UTF-8 string.
const MAJOR_TEXT: u8 = 3;
/// Major type 4: an array.
const MAJOR_ARRAY: u8 = 4;
/// Major type 5: a map.
const MAJOR_MAP: u8 = 5;
// Major type 7 heads. These are written as literal bytes rather than through
// `write_head`: their low five bits are an additional-information code, not a
// number to be shortened, and 27 routed through the shortest-form path would
// come back as a two-byte head carrying the value 27.
/// `false`.
const HEAD_FALSE: u8 = 0xf4;
/// `true`.
const HEAD_TRUE: u8 = 0xf5;
/// `null`.
const HEAD_NULL: u8 = 0xf6;
/// A 64-bit float follows.
const HEAD_F64: u8 = 0xfb;

/// Append the CBOR encoding of `value` to `out`.
pub(super) fn write_value(out: &mut Vec<u8>, value: &WireValue) {
    match value {
        WireValue::Null => out.push(HEAD_NULL),
        WireValue::Bool(true) => out.push(HEAD_TRUE),
        WireValue::Bool(false) => out.push(HEAD_FALSE),
        WireValue::Integer(n) => write_integer(out, *n),
        WireValue::Float(f) => {
            out.push(HEAD_F64);
            out.extend_from_slice(&f.to_be_bytes());
        }
        WireValue::Text(text) => {
            write_head(out, MAJOR_TEXT, text.len() as u64);
            out.extend_from_slice(text.as_bytes());
        }
        WireValue::Bytes(bytes) => {
            write_head(out, MAJOR_BYTES, bytes.len() as u64);
            out.extend_from_slice(bytes);
        }
        WireValue::Array(items) => {
            write_head(out, MAJOR_ARRAY, items.len() as u64);
            for item in items {
                write_value(out, item);
            }
        }
        WireValue::Map(entries) => {
            write_head(out, MAJOR_MAP, entries.len() as u64);
            for (key, item) in entries {
                write_head(out, MAJOR_TEXT, key.len() as u64);
                out.extend_from_slice(key.as_bytes());
                write_value(out, item);
            }
        }
    }
}

/// Major type 0 for a non-negative integer, major type 1 for a negative one.
///
/// The negative form encodes `-1 - n`, which is exactly the bitwise complement
/// — and taking it that way is also what keeps `i64::MIN` from overflowing the
/// subtraction.
fn write_integer(out: &mut Vec<u8>, n: i64) {
    if n >= 0 {
        write_head(out, MAJOR_UNSIGNED, n as u64);
    } else {
        write_head(out, MAJOR_NEGATIVE, (!n) as u64);
    }
}

/// Write a major type and its argument in the shortest form that holds it.
fn write_head(out: &mut Vec<u8>, major: u8, argument: u64) {
    let major = major << 5;
    match argument {
        // Arguments below 24 ride in the head byte itself.
        0..=23 => out.push(major | argument as u8),
        24..=0xff => {
            out.push(major | 24);
            out.push(argument as u8);
        }
        0x100..=0xffff => {
            out.push(major | 25);
            out.extend_from_slice(&(argument as u16).to_be_bytes());
        }
        0x1_0000..=0xffff_ffff => {
            out.push(major | 26);
            out.extend_from_slice(&(argument as u32).to_be_bytes());
        }
        _ => {
            out.push(major | 27);
            out.extend_from_slice(&argument.to_be_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(value: &WireValue) -> String {
        let mut out = Vec::new();
        write_value(&mut out, value);
        out.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn integers_use_the_shortest_form() {
        assert_eq!(hex(&WireValue::Integer(0)), "00");
        assert_eq!(hex(&WireValue::Integer(23)), "17");
        assert_eq!(hex(&WireValue::Integer(24)), "1818");
        assert_eq!(hex(&WireValue::Integer(1000)), "1903e8");
        assert_eq!(hex(&WireValue::Integer(1_000_000)), "1a000f4240");
        assert_eq!(
            hex(&WireValue::Integer(1_000_000_000_000)),
            "1b000000e8d4a51000"
        );
    }

    #[test]
    fn negative_integers_use_major_type_one() {
        assert_eq!(hex(&WireValue::Integer(-1)), "20");
        assert_eq!(hex(&WireValue::Integer(-24)), "37");
        assert_eq!(hex(&WireValue::Integer(-1000)), "3903e7");
        // -1 - i64::MIN does not fit an i64; the complement does.
        assert_eq!(hex(&WireValue::Integer(i64::MIN)), "3b7fffffffffffffff");
    }

    #[test]
    fn containers_carry_a_definite_length_head() {
        assert_eq!(hex(&WireValue::Array(Vec::new())), "80");
        assert_eq!(hex(&WireValue::empty_map()), "a0");
        assert_eq!(
            hex(&WireValue::Array(vec![
                WireValue::Integer(1),
                WireValue::Text("a".into())
            ])),
            "82016161"
        );
    }

    #[test]
    fn simple_values_and_floats() {
        assert_eq!(hex(&WireValue::Null), "f6");
        assert_eq!(hex(&WireValue::Bool(true)), "f5");
        assert_eq!(hex(&WireValue::Bool(false)), "f4");
        assert_eq!(hex(&WireValue::Float(1.5)), "fb3ff8000000000000");
    }

    #[test]
    fn strings_and_byte_strings() {
        assert_eq!(hex(&WireValue::Text(String::new())), "60");
        // Length is in bytes, not characters.
        assert_eq!(hex(&WireValue::Text("héllo".into())), "6668c3a96c6c6f");
        assert_eq!(hex(&WireValue::Bytes(vec![1, 2])), "420102");
    }

    #[test]
    fn map_entries_keep_the_order_they_were_given() {
        let value = WireValue::Map(vec![
            ("order_id".into(), WireValue::Text("ord-0001".into())),
            ("amount_cents".into(), WireValue::Integer(1000)),
        ]);
        assert_eq!(
            hex(&value),
            "a2686f726465725f6964686f72642d303030316c616d6f756e745f63656e74731903e8"
        );
    }
}
