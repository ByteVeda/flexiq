//! The value tree the envelope encoder walks.

/// One CBOR value, in the subset the call envelope carries.
///
/// This is a writer's input, not a decoder's output: there is no `Tag` arm and
/// no indefinite-length arm, because the contract forbids writing either.
///
/// [`Map`](Self::Map) keeps its entries as an ordered `Vec` rather than a
/// `BTreeMap`. Key order is not semantically meaningful to a CBOR reader, but
/// it *is* part of the bytes, and the `auto:` idempotency key is a hash over
/// those bytes — so the caller decides the order and the encoder never
/// reorders behind its back.
#[derive(Debug, Clone, PartialEq)]
pub enum WireValue {
    /// CBOR `null`.
    Null,
    /// CBOR `true` / `false`.
    Bool(bool),
    /// A signed integer. Encoded as major type 0 when non-negative and major
    /// type 1 otherwise, always in the shortest form that holds it.
    Integer(i64),
    /// A double. Always written as a 64-bit float: narrowing is legal for a
    /// writer but changes bytes for no benefit here.
    Float(f64),
    /// A UTF-8 string.
    Text(String),
    /// A byte string. Unreachable through a JSON-shaped producer, which is why
    /// the wire vectors pin it as round-trip only.
    Bytes(Vec<u8>),
    /// An array, definite length.
    Array(Vec<WireValue>),
    /// A map with text keys, definite length, in the order given.
    Map(Vec<(String, WireValue)>),
}

impl WireValue {
    /// An empty map — the `kwargs` a language with no keyword arguments sends.
    pub fn empty_map() -> Self {
        WireValue::Map(Vec::new())
    }
}
