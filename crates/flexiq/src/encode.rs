//! Turning a `Serialize` value into the [`WireValue`] tree core's writer walks.
//!
//! This crate does not encode CBOR. `flexiq_core::wire` is the one writer in the
//! tree and `contracts/wire-vectors.json` pins its output byte for byte; a
//! second encoder here that merely agreed today is how `auto:` idempotency keys
//! quietly stop deduping across languages. All that happens here is the shape
//! change from Rust's type system into the eight arms core knows.
//!
//! Two rules are structural rather than configurable, because getting either
//! wrong still interoperates:
//!
//! * **Map keys keep insertion order.** `WireValue::Map` is an ordered `Vec` for
//!   exactly this reason — the `auto:` key hashes the encoded bytes, so sorting
//!   behind the caller's back changes an identity.
//! * **Nothing widens silently.** A `u64` past `i64::MAX` is refused, not
//!   wrapped.

use flexiq_core::wire::{encode_call, WireValue};
use serde::{ser, Serialize};

/// A value with no representation in the call envelope.
#[derive(Debug, thiserror::Error)]
#[error("cannot encode task argument: {0}")]
pub struct EncodeError(String);

impl ser::Error for EncodeError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        EncodeError(msg.to_string())
    }
}

/// Convert one value into a [`WireValue`].
pub fn to_wire<T: Serialize + ?Sized>(value: &T) -> Result<WireValue, EncodeError> {
    value.serialize(WireSerializer)
}

/// Encode a positional argument list as a call envelope.
///
/// `kwargs` is always the empty map: Rust has no keyword arguments, the same
/// positional-only asymmetry `BINDING_CONTRACT.md` records for the other
/// positional shells. A producer that *does* have them is why core's
/// `encode_call` still takes the parameter.
pub fn encode_args(args: &[WireValue]) -> Vec<u8> {
    encode_call(args, &[])
}

/// The `Serialize` sink that builds a [`WireValue`].
struct WireSerializer;

/// Refuse a value the envelope has no arm for.
fn refuse(what: &str) -> EncodeError {
    EncodeError(what.to_string())
}

impl ser::Serializer for WireSerializer {
    type Ok = WireValue;
    type Error = EncodeError;

    type SerializeSeq = SeqBuilder;
    type SerializeTuple = SeqBuilder;
    type SerializeTupleStruct = SeqBuilder;
    type SerializeTupleVariant = VariantSeqBuilder;
    type SerializeMap = MapBuilder;
    type SerializeStruct = MapBuilder;
    type SerializeStructVariant = VariantMapBuilder;

    /// Binary, because the reader is.
    ///
    /// Serde defaults this to `true` and `ciborium` — which decodes everything
    /// this writes — reports `false`. A type that branches on it, `uuid::Uuid`
    /// among them, would otherwise encode as text here and be asked for bytes
    /// at dispatch: the enqueue succeeds and the job fails when it runs.
    ///
    /// The cost is that such a type travels in its binary form, which a worker
    /// in another language sees as a byte string rather than as text. The two
    /// runtimes disagree about that representation whichever answer is given
    /// here; only one answer round-trips. Send a `String` when the far side is
    /// another language.
    fn is_human_readable(&self) -> bool {
        false
    }

    fn serialize_bool(self, value: bool) -> Result<Self::Ok, Self::Error> {
        Ok(WireValue::Bool(value))
    }

    fn serialize_i8(self, value: i8) -> Result<Self::Ok, Self::Error> {
        Ok(WireValue::Integer(value.into()))
    }

    fn serialize_i16(self, value: i16) -> Result<Self::Ok, Self::Error> {
        Ok(WireValue::Integer(value.into()))
    }

    fn serialize_i32(self, value: i32) -> Result<Self::Ok, Self::Error> {
        Ok(WireValue::Integer(value.into()))
    }

    fn serialize_i64(self, value: i64) -> Result<Self::Ok, Self::Error> {
        Ok(WireValue::Integer(value))
    }

    fn serialize_i128(self, value: i128) -> Result<Self::Ok, Self::Error> {
        i64::try_from(value)
            .map(WireValue::Integer)
            .map_err(|_| refuse("an i128 out of range for the envelope's 64-bit integer"))
    }

    fn serialize_u8(self, value: u8) -> Result<Self::Ok, Self::Error> {
        Ok(WireValue::Integer(value.into()))
    }

    fn serialize_u16(self, value: u16) -> Result<Self::Ok, Self::Error> {
        Ok(WireValue::Integer(value.into()))
    }

    fn serialize_u32(self, value: u32) -> Result<Self::Ok, Self::Error> {
        Ok(WireValue::Integer(value.into()))
    }

    /// Refused rather than wrapped: the envelope's integer arm is signed, and a
    /// wrap would put a different number on the wire than the caller passed.
    fn serialize_u64(self, value: u64) -> Result<Self::Ok, Self::Error> {
        i64::try_from(value)
            .map(WireValue::Integer)
            .map_err(|_| refuse("a u64 out of range for the envelope's signed integer"))
    }

    fn serialize_u128(self, value: u128) -> Result<Self::Ok, Self::Error> {
        i64::try_from(value)
            .map(WireValue::Integer)
            .map_err(|_| refuse("a u128 out of range for the envelope's signed integer"))
    }

    /// Widened, not narrowed: core always writes a 64-bit float, so an `f32`
    /// argument and an `f64` one of the same value produce the same bytes.
    fn serialize_f32(self, value: f32) -> Result<Self::Ok, Self::Error> {
        Ok(WireValue::Float(value.into()))
    }

    fn serialize_f64(self, value: f64) -> Result<Self::Ok, Self::Error> {
        Ok(WireValue::Float(value))
    }

    fn serialize_char(self, value: char) -> Result<Self::Ok, Self::Error> {
        Ok(WireValue::Text(value.to_string()))
    }

    fn serialize_str(self, value: &str) -> Result<Self::Ok, Self::Error> {
        Ok(WireValue::Text(value.to_string()))
    }

    fn serialize_bytes(self, value: &[u8]) -> Result<Self::Ok, Self::Error> {
        Ok(WireValue::Bytes(value.to_vec()))
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(WireValue::Null)
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(WireValue::Null)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Ok(WireValue::Null)
    }

    /// A fieldless variant travels as its name, which is what a reader in a
    /// language without enums receives and can branch on.
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Ok(WireValue::Text(variant.to_string()))
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        value.serialize(self)
    }

    /// A data-carrying variant travels as a one-entry map keyed by the variant
    /// name — serde's externally tagged shape, and the one a JSON-shaped
    /// producer in another language would send.
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        Ok(WireValue::Map(vec![(
            variant.to_string(),
            value.serialize(WireSerializer)?,
        )]))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(SeqBuilder::with_capacity(len))
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(SeqBuilder::with_capacity(Some(len)))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Ok(SeqBuilder::with_capacity(Some(len)))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(VariantSeqBuilder {
            variant,
            items: SeqBuilder::with_capacity(Some(len)),
        })
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(MapBuilder::with_capacity(len))
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(MapBuilder::with_capacity(Some(len)))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Ok(VariantMapBuilder {
            variant,
            entries: MapBuilder::with_capacity(Some(len)),
        })
    }
}

/// Collects array elements in order.
struct SeqBuilder {
    items: Vec<WireValue>,
}

impl SeqBuilder {
    fn with_capacity(len: Option<usize>) -> Self {
        Self {
            items: Vec::with_capacity(len.unwrap_or_default()),
        }
    }

    fn push<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), EncodeError> {
        self.items.push(value.serialize(WireSerializer)?);
        Ok(())
    }
}

impl ser::SerializeSeq for SeqBuilder {
    type Ok = WireValue;
    type Error = EncodeError;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.push(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(WireValue::Array(self.items))
    }
}

impl ser::SerializeTuple for SeqBuilder {
    type Ok = WireValue;
    type Error = EncodeError;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.push(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(WireValue::Array(self.items))
    }
}

impl ser::SerializeTupleStruct for SeqBuilder {
    type Ok = WireValue;
    type Error = EncodeError;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.push(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(WireValue::Array(self.items))
    }
}

/// A tuple variant: an array under the variant's name.
struct VariantSeqBuilder {
    variant: &'static str,
    items: SeqBuilder,
}

impl ser::SerializeTupleVariant for VariantSeqBuilder {
    type Ok = WireValue;
    type Error = EncodeError;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.items.push(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(WireValue::Map(vec![(
            self.variant.to_string(),
            WireValue::Array(self.items.items),
        )]))
    }
}

/// Collects map entries in insertion order.
struct MapBuilder {
    entries: Vec<(String, WireValue)>,
    pending_key: Option<String>,
}

impl MapBuilder {
    fn with_capacity(len: Option<usize>) -> Self {
        Self {
            entries: Vec::with_capacity(len.unwrap_or_default()),
            pending_key: None,
        }
    }
}

impl ser::SerializeMap for MapBuilder {
    type Ok = WireValue;
    type Error = EncodeError;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Self::Error> {
        self.pending_key = Some(key.serialize(KeySerializer)?);
        Ok(())
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        let key = self
            .pending_key
            .take()
            .ok_or_else(|| refuse("a map value arrived before its key"))?;
        self.entries.push((key, value.serialize(WireSerializer)?));
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(WireValue::Map(self.entries))
    }
}

impl ser::SerializeStruct for MapBuilder {
    type Ok = WireValue;
    type Error = EncodeError;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.entries
            .push((key.to_string(), value.serialize(WireSerializer)?));
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(WireValue::Map(self.entries))
    }
}

/// A struct variant: a map under the variant's name.
struct VariantMapBuilder {
    variant: &'static str,
    entries: MapBuilder,
}

impl ser::SerializeStructVariant for VariantMapBuilder {
    type Ok = WireValue;
    type Error = EncodeError;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.entries
            .entries
            .push((key.to_string(), value.serialize(WireSerializer)?));
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(WireValue::Map(vec![(
            self.variant.to_string(),
            WireValue::Map(self.entries.entries),
        )]))
    }
}

/// Serializes a map key, which the envelope requires to be text.
///
/// A separate serializer rather than a check on the result, so the refusal
/// names the offending shape rather than reporting "not a string" about a
/// value that was already built.
struct KeySerializer;

/// Every non-text key lands here.
macro_rules! refuse_key {
    ($method:ident, $ty:ty) => {
        fn $method(self, _value: $ty) -> Result<String, EncodeError> {
            Err(refuse(concat!(
                "a map key of type ",
                stringify!($ty),
                ": the call envelope's maps are text-keyed"
            )))
        }
    };
}

impl ser::Serializer for KeySerializer {
    type Ok = String;
    type Error = EncodeError;

    type SerializeSeq = ser::Impossible<String, EncodeError>;
    type SerializeTuple = ser::Impossible<String, EncodeError>;
    type SerializeTupleStruct = ser::Impossible<String, EncodeError>;
    type SerializeTupleVariant = ser::Impossible<String, EncodeError>;
    type SerializeMap = ser::Impossible<String, EncodeError>;
    type SerializeStruct = ser::Impossible<String, EncodeError>;
    type SerializeStructVariant = ser::Impossible<String, EncodeError>;

    fn serialize_str(self, value: &str) -> Result<String, EncodeError> {
        Ok(value.to_string())
    }

    fn serialize_char(self, value: char) -> Result<String, EncodeError> {
        Ok(value.to_string())
    }

    /// A fieldless variant is a legitimate key: it is already a name.
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
    ) -> Result<String, EncodeError> {
        Ok(variant.to_string())
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<String, EncodeError> {
        value.serialize(self)
    }

    refuse_key!(serialize_bool, bool);
    refuse_key!(serialize_i8, i8);
    refuse_key!(serialize_i16, i16);
    refuse_key!(serialize_i32, i32);
    refuse_key!(serialize_i64, i64);
    refuse_key!(serialize_i128, i128);
    refuse_key!(serialize_u8, u8);
    refuse_key!(serialize_u16, u16);
    refuse_key!(serialize_u32, u32);
    refuse_key!(serialize_u64, u64);
    refuse_key!(serialize_u128, u128);
    refuse_key!(serialize_f32, f32);
    refuse_key!(serialize_f64, f64);
    refuse_key!(serialize_bytes, &[u8]);

    fn serialize_none(self) -> Result<String, EncodeError> {
        Err(refuse("a null map key"))
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<String, EncodeError> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<String, EncodeError> {
        Err(refuse("a unit map key"))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<String, EncodeError> {
        Err(refuse("a unit-struct map key"))
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<String, EncodeError> {
        Err(refuse("a map key built from a data-carrying variant"))
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(refuse("a sequence map key"))
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(refuse("a tuple map key"))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(refuse("a tuple-struct map key"))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(refuse("a tuple-variant map key"))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(refuse("a map used as a map key"))
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Err(refuse("a struct map key"))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(refuse("a struct-variant map key"))
    }
}
