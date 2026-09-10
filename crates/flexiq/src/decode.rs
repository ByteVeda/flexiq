//! Reading a call envelope back into typed arguments.
//!
//! Core has no reader — `wire/cbor.rs`: "There is no reader. Nothing in this
//! crate decodes a payload." Every other shell decodes with its language's CBOR
//! library, and this is Rust's.
//!
//! The asymmetry with [`crate::encode`] is deliberate. Writing has exactly one
//! legal output, which is why it goes through core's pinned writer. Reading has
//! to accept everything any writer may legally emit — either float width, an
//! integer past 2^53, a byte string, an indefinite-length head from a producer
//! that predates the definite-length rule — so it uses a real CBOR
//! implementation rather than a mirror of the writer.

use flexiq_core::wire::TAG_CBOR;

/// A payload that is not a call this shell can run.
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    /// The leading tag byte named a codec this shell does not read.
    #[error("unsupported payload codec: tag 0x{0:02x}")]
    Codec(u8),
    /// The payload was empty, so it carries no tag at all.
    #[error("empty payload: no codec tag")]
    Empty,
    /// The body was not a two-element `[args, kwargs]` array.
    #[error("malformed call envelope: {0}")]
    Envelope(String),
    /// The arguments did not match the task's parameter types.
    #[error("task arguments do not match the handler: {0}")]
    Arguments(String),
}

/// Strip the tag, read `[args, kwargs]`, deserialize `args` into `T`.
///
/// `kwargs` is read and discarded rather than refused when non-empty. A
/// producer in a language that has keyword arguments may legally send them, and
/// a Rust handler with no parameter for one should fail on the argument tuple's
/// shape — a message naming the mismatch — rather than on the map's presence.
pub fn decode_args<T: serde::de::DeserializeOwned>(payload: &[u8]) -> Result<T, DecodeError> {
    let (tag, body) = payload.split_first().ok_or(DecodeError::Empty)?;
    if *tag != TAG_CBOR {
        return Err(DecodeError::Codec(*tag));
    }

    let call: ciborium::Value =
        ciborium::from_reader(body).map_err(|e| DecodeError::Envelope(e.to_string()))?;

    let mut items = match call {
        ciborium::Value::Array(items) if items.len() == 2 => items,
        ciborium::Value::Array(items) => {
            return Err(DecodeError::Envelope(format!(
                "expected a 2-element [args, kwargs] array, found {} elements",
                items.len()
            )))
        }
        other => {
            return Err(DecodeError::Envelope(format!(
                "expected a 2-element [args, kwargs] array, found {}",
                describe(&other)
            )))
        }
    };

    items
        .remove(0)
        .deserialized()
        .map_err(|e| DecodeError::Arguments(e.to_string()))
}

/// Name a CBOR value's shape for an error message, without printing its
/// contents — a payload can be large and can carry a caller's data.
fn describe(value: &ciborium::Value) -> &'static str {
    match value {
        ciborium::Value::Integer(_) => "an integer",
        ciborium::Value::Bytes(_) => "a byte string",
        ciborium::Value::Float(_) => "a float",
        ciborium::Value::Text(_) => "a text string",
        ciborium::Value::Bool(_) => "a boolean",
        ciborium::Value::Null => "null",
        ciborium::Value::Tag(..) => "a tagged value",
        ciborium::Value::Array(_) => "an array",
        ciborium::Value::Map(_) => "a map",
        _ => "a value of an unrecognised kind",
    }
}
