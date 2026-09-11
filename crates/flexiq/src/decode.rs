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
    /// The call carried keyword arguments, which a Rust task has no parameter
    /// for.
    #[error(
        "call carries {0} keyword argument(s); a Rust task takes positional arguments only, \
         so passing them would drop them silently"
    )]
    Kwargs(usize),
}

/// Split a payload into its `args` and `kwargs` halves.
fn envelope(payload: &[u8]) -> Result<(ciborium::Value, ciborium::Value), DecodeError> {
    let (tag, body) = payload.split_first().ok_or(DecodeError::Empty)?;
    if *tag != TAG_CBOR {
        return Err(DecodeError::Codec(*tag));
    }

    let call: ciborium::Value =
        ciborium::from_reader(body).map_err(|e| DecodeError::Envelope(e.to_string()))?;

    match call {
        ciborium::Value::Array(mut items) if items.len() == 2 => {
            let kwargs = items.remove(1);
            let args = items.remove(0);
            Ok((args, kwargs))
        }
        ciborium::Value::Array(items) => Err(DecodeError::Envelope(format!(
            "expected a 2-element [args, kwargs] array, found {} elements",
            items.len()
        ))),
        other => Err(DecodeError::Envelope(format!(
            "expected a 2-element [args, kwargs] array, found {}",
            describe(&other)
        ))),
    }
}

/// Refuse a call that carries keyword arguments.
///
/// Rust has no keyword arguments, so there is nothing to bind them to. Running
/// the task anyway would drop what the caller sent without saying so, and a
/// caller who sent them meant something by it.
fn reject_kwargs(kwargs: &ciborium::Value) -> Result<(), DecodeError> {
    match kwargs {
        ciborium::Value::Map(entries) if entries.is_empty() => Ok(()),
        ciborium::Value::Map(entries) => Err(DecodeError::Kwargs(entries.len())),
        other => Err(DecodeError::Envelope(format!(
            "expected a kwargs map, found {}",
            describe(other)
        ))),
    }
}

/// Strip the tag, read `[args, kwargs]`, deserialize `args` into `T`.
///
/// `T` is the task's parameter tuple, so a payload with too many or too few
/// positional arguments fails here on the tuple's length.
pub fn decode_args<T: serde::de::DeserializeOwned>(payload: &[u8]) -> Result<T, DecodeError> {
    let (args, kwargs) = envelope(payload)?;
    reject_kwargs(&kwargs)?;
    args.deserialized()
        .map_err(|e| DecodeError::Arguments(e.to_string()))
}

/// Validate the envelope of a call to a task that takes no parameters.
///
/// A task with no parameters has no tuple to deserialize into, and skipping the
/// read entirely would let it run on *any* payload — a malformed envelope, or a
/// call carrying arguments it was never going to receive. Both mean the caller
/// and the task disagree, which is worth hearing about.
pub fn decode_no_args(payload: &[u8]) -> Result<(), DecodeError> {
    let (args, kwargs) = envelope(payload)?;
    reject_kwargs(&kwargs)?;
    match args {
        ciborium::Value::Array(items) if items.is_empty() => Ok(()),
        ciborium::Value::Array(items) => Err(DecodeError::Arguments(format!(
            "call carries {} positional argument(s), and this task takes none",
            items.len()
        ))),
        other => Err(DecodeError::Envelope(format!(
            "expected an args array, found {}",
            describe(&other)
        ))),
    }
}

/// Strip the tag and read a bare value, the shape `wire::encode_result` writes.
///
/// A result is not a call: no `[args, kwargs]` array around it. Used for a
/// step's memoized value, which is written with the same writer a job result
/// is, so a memo read on a later attempt goes through exactly the codec the
/// value was written with.
pub(crate) fn decode_result<T: serde::de::DeserializeOwned>(
    payload: &[u8],
) -> Result<T, DecodeError> {
    let (tag, body) = payload.split_first().ok_or(DecodeError::Empty)?;
    if *tag != TAG_CBOR {
        return Err(DecodeError::Codec(*tag));
    }
    ciborium::from_reader(body).map_err(|e| DecodeError::Arguments(e.to_string()))
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
