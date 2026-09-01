//! The tagged call envelope: one tag byte, then the codec body.

use super::cbor::write_value;
use super::value::WireValue;

/// The tag byte for a CBOR body — the cross-SDK default.
///
/// The other tags (`0x00` native, `0x01` msgpack, `0x03`+ reserved) are named
/// in `BINDING_CONTRACT.md`. This crate writes only CBOR, so it names only the
/// one it writes.
pub const TAG_CBOR: u8 = 0x02;

/// Encode a call into the wire payload a `Job` carries.
///
/// The body is the two-element array `[args, kwargs]` — positional arguments
/// first, keyword arguments second — behind [`TAG_CBOR`]. A language with no
/// keyword arguments sends an empty map, never a missing element: the array is
/// always two long, or the shape is not the one the shells decode.
///
/// ```
/// use flexiq_core::wire::{encode_call, WireValue};
///
/// // f(1, "a") with no keyword arguments — the vector in BINDING_CONTRACT.md.
/// let payload = encode_call(&[WireValue::Integer(1), WireValue::Text("a".into())], &[]);
/// assert_eq!(payload, [0x02, 0x82, 0x82, 0x01, 0x61, 0x61, 0xa0]);
/// ```
pub fn encode_call(args: &[WireValue], kwargs: &[(String, WireValue)]) -> Vec<u8> {
    let body = WireValue::Array(vec![
        WireValue::Array(args.to_vec()),
        WireValue::Map(kwargs.to_vec()),
    ]);
    let mut out = vec![TAG_CBOR];
    write_value(&mut out, &body);
    out
}

/// Encode a task's return value.
///
/// A result is a bare CBOR value behind the same tag byte — no array wrapper,
/// because there is nothing to pair it with.
pub fn encode_result(value: &WireValue) -> Vec<u8> {
    let mut out = vec![TAG_CBOR];
    write_value(&mut out, value);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_call_is_still_two_elements() {
        assert_eq!(encode_call(&[], &[]), [0x02, 0x82, 0x80, 0xa0]);
    }

    #[test]
    fn a_result_carries_no_array_wrapper() {
        assert_eq!(encode_result(&WireValue::Bool(true)), [0x02, 0xf5]);
    }
}
