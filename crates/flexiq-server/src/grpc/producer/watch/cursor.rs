//! The opaque `cursor` a queue watch hands out and takes back.
//!
//! It names a feed position *and* the process that numbered it: positions from
//! two processes, or from two runs of one, are unrelated numbers, and resuming
//! from the wrong one would silently skip or repeat transitions.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;

use super::feed::Seq;

/// Sixteen bytes: the instance, then the position, both big-endian.
const LEN: usize = 16;

/// The cursor for position `seq` of the feed numbered by `instance`.
pub fn encode(instance: u64, seq: Seq) -> String {
    let mut bytes = [0u8; LEN];
    bytes[..8].copy_from_slice(&instance.to_be_bytes());
    bytes[8..].copy_from_slice(&seq.to_be_bytes());
    URL_SAFE_NO_PAD.encode(bytes)
}

/// The `(instance, seq)` a cursor names, or `None` for anything this server
/// did not produce.
pub fn decode(raw: &str) -> Option<(u64, Seq)> {
    let bytes: [u8; LEN] = URL_SAFE_NO_PAD.decode(raw).ok()?.try_into().ok()?;
    let instance = u64::from_be_bytes(bytes[..8].try_into().ok()?);
    let seq = u64::from_be_bytes(bytes[8..].try_into().ok()?);
    Some((instance, seq))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cursor_round_trips() {
        let raw = encode(0xdead_beef, 42);
        assert_eq!(decode(&raw), Some((0xdead_beef, 42)));
    }

    #[test]
    fn anything_else_is_not_a_cursor() {
        for raw in ["", "not base64!", &URL_SAFE_NO_PAD.encode([1u8; 8])] {
            assert_eq!(decode(raw), None, "{raw:?}");
        }
    }
}
