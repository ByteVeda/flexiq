//! Proto3 JSON, expressed as JSON Schema 2020-12.
//!
//! The mapping here is the same one `crates/flexiq-server`'s facade implements
//! by hand on both sides of the wire — 64-bit integers as strings, enums as
//! their declared names, `bytes` as base64 — and the descriptor is the only
//! input, so a field renamed in a `.proto` is renamed here without anyone
//! touching this file.
//!
//! Two schemas are **not** derived. The facade renders `google.rpc.Status`
//! into a shape of its own ([`STATUS`]) and wraps a failed response in an
//! `error` key ([`ERROR`]); a client parses those, not the protobuf ones, so
//! they are written out rather than generated.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{json, Map, Value};

use crate::descriptor::{Contract, Field, Kind, Message};
use crate::Error;

/// The component name of the facade's rendering of `google.rpc.Status`.
pub const STATUS: &str = "Status";

/// The component name of the body of a failed response.
pub const ERROR: &str = "Error";

/// Component schemas, built on demand from the messages that are reached.
pub struct Registry<'a> {
    contract: &'a Contract,
    schemas: BTreeMap<String, Value>,
}

impl<'a> Registry<'a> {
    /// A registry holding only the two hand-written schemas.
    pub fn new(contract: &'a Contract) -> Self {
        let mut schemas = BTreeMap::new();
        schemas.insert(STATUS.to_string(), status_schema());
        schemas.insert(ERROR.to_string(), error_schema());
        Self { contract, schemas }
    }

    /// How a field of `full_name` is written: a `$ref` for a message that earns
    /// a component, an inline schema for a well-known type.
    pub fn reference(&mut self, full_name: &str) -> Result<Value, Error> {
        if let Some(inline) = well_known(full_name) {
            return Ok(inline);
        }
        if !self.schemas.contains_key(full_name) {
            // Seeded before recursing: `SubWorkflowSpec` reaches a graph that
            // reaches a node config, and a cycle would otherwise not terminate.
            self.schemas.insert(full_name.to_string(), Value::Null);
            let message = self.contract.message(full_name)?;
            let built = self.message_schema(message)?;
            self.schemas.insert(full_name.to_string(), built);
        }
        Ok(json!({ "$ref": format!("#/components/schemas/{full_name}") }))
    }

    /// The schema of one field, including its comment as a description.
    pub fn field(&mut self, field: &Field) -> Result<Value, Error> {
        let mut schema = self.field_shape(field)?;
        if let (Some(comment), Some(object)) = (&field.comment, schema.as_object_mut()) {
            object.insert("description".to_string(), comment.as_str().into());
        }
        Ok(schema)
    }

    /// The same schema without the description.
    ///
    /// A parameter object carries its own `description`, and repeating it on
    /// the schema inside says the same thing twice in a generated artifact.
    pub fn field_shape(&mut self, field: &Field) -> Result<Value, Error> {
        self.shape(field)
    }

    /// Refuse unknown properties on the named schemas.
    ///
    /// The facade's request readers are `deny_unknown_fields`, so a document
    /// that left request bodies open would describe a laxer door than the one
    /// running. Responses stay open: a server is free to add a field, and a
    /// client that refuses one is a client that breaks on the next release.
    pub fn deny_unknown(&mut self, names: &BTreeSet<String>) {
        for name in names {
            if let Some(Value::Object(schema)) = self.schemas.get_mut(name) {
                schema.insert("additionalProperties".to_string(), false.into());
            }
        }
    }

    /// Every schema built, by component name.
    pub fn into_schemas(self) -> BTreeMap<String, Value> {
        self.schemas
    }

    /// A field's schema before its description is attached.
    fn shape(&mut self, field: &Field) -> Result<Value, Error> {
        if let Some(entry) = self.map_entry(field)? {
            let value = entry
                .fields
                .iter()
                .find(|field| field.name == "value")
                .ok_or_else(|| Error::Undeclared(format!("{}.value", entry.full_name)))?;
            let values = self.shape(value)?;
            // A protobuf map key is always a string once it is JSON, whatever
            // the declared key type is.
            return Ok(json!({ "type": "object", "additionalProperties": values }));
        }

        let single = match &field.kind {
            Kind::Scalar(scalar) => scalar_schema(*scalar, &field.name)?,
            Kind::Message(name) => self.reference(name)?,
            Kind::Enum(name) => self.enum_schema(name)?,
        };

        match field.repeated {
            true => Ok(json!({ "type": "array", "items": single })),
            false => Ok(single),
        }
    }

    /// The synthetic entry message behind a `map<K, V>`, if this field is one.
    fn map_entry(&self, field: &Field) -> Result<Option<&'a Message>, Error> {
        let Kind::Message(name) = &field.kind else {
            return Ok(None);
        };
        if !field.repeated {
            return Ok(None);
        }
        let message = self.contract.message(name)?;
        Ok(message.map_entry.then_some(message))
    }

    /// An enum, written as the value names proto3 JSON uses.
    fn enum_schema(&mut self, full_name: &str) -> Result<Value, Error> {
        let declared = self.contract.enumeration(full_name)?;
        let names: Vec<Value> = declared
            .values
            .iter()
            .map(|value| value.name.as_str().into())
            .collect();
        let mut schema = Map::new();
        schema.insert("type".to_string(), "string".into());
        schema.insert("enum".to_string(), Value::Array(names));
        if let Some(comment) = &declared.comment {
            schema.insert("description".to_string(), comment.as_str().into());
        }
        Ok(Value::Object(schema))
    }

    fn message_schema(&mut self, message: &'a Message) -> Result<Value, Error> {
        let mut properties = Map::new();
        for field in &message.fields {
            properties.insert(field.json_name.clone(), self.field(field)?);
        }

        let mut schema = Map::new();
        schema.insert("type".to_string(), "object".into());
        if let Some(comment) = &message.comment {
            schema.insert("description".to_string(), comment.as_str().into());
        }
        schema.insert("properties".to_string(), Value::Object(properties));

        let exclusions: Vec<Value> = message
            .oneofs()
            .into_iter()
            .map(|oneof| exclusive(message, &oneof))
            .collect();
        match exclusions.len() {
            0 => {}
            // The common case, and worth keeping flat: one `oneOf` on the
            // schema itself rather than an `allOf` wrapping a single arm.
            1 => {
                let Value::Object(only) = &exclusions[0] else {
                    unreachable!("exclusive() builds an object")
                };
                schema.extend(only.clone());
            }
            _ => {
                schema.insert("allOf".to_string(), Value::Array(exclusions));
            }
        }

        Ok(Value::Object(schema))
    }
}

/// "At most one of these keys", as a schema.
///
/// Not "exactly one": a oneof that must be set is a rule the service enforces
/// and states in its own prose, the same way it enforces a non-empty
/// `taskName`. What is structural — and what a client generator needs — is that
/// the arms exclude each other.
fn exclusive(message: &Message, oneof: &str) -> Value {
    let arms: Vec<Value> = message
        .fields
        .iter()
        .filter(|field| field.oneof.as_deref() == Some(oneof))
        .map(|field| json!({ "required": [field.json_name] }))
        .collect();
    let mut alternatives = arms.clone();
    // The escape hatch, so an unset oneof still validates. Without it `oneOf`
    // would read as "exactly one".
    alternatives.push(json!({ "not": { "anyOf": arms } }));
    json!({ "oneOf": alternatives })
}

/// Every message that earns a component, reachable from `roots`.
///
/// Well-known types are inlined rather than referenced and map entries are
/// synthetic, so neither appears — but a map's *value* type does.
pub fn closure(contract: &Contract, roots: &[String]) -> Result<BTreeSet<String>, Error> {
    let mut found = BTreeSet::new();
    let mut pending: Vec<String> = roots.to_vec();
    while let Some(name) = pending.pop() {
        if well_known(&name).is_some() {
            continue;
        }
        let message = contract.message(&name)?;
        let component = !message.map_entry;
        if component && !found.insert(name.clone()) {
            continue;
        }
        for field in &message.fields {
            if let Kind::Message(referenced) = &field.kind {
                pending.push(referenced.clone());
            }
        }
    }
    Ok(found)
}

/// The inline schema of a well-known type, if `full_name` is one.
fn well_known(full_name: &str) -> Option<Value> {
    let schema = match full_name {
        "google.protobuf.Timestamp" => json!({
            "type": "string",
            "format": "date-time",
            "description": "An instant, RFC 3339 with a `Z` offset."
        }),
        "google.protobuf.Duration" => json!({
            "type": "string",
            "pattern": r"^-?[0-9]+(\.[0-9]{1,9})?s$",
            "description": "A span of time, seconds with up to nine fractional digits and a trailing `s`."
        }),
        "google.protobuf.Struct" => json!({
            "type": "object",
            "description": "Any JSON object."
        }),
        "google.protobuf.ListValue" => json!({
            "type": "array",
            "description": "Any JSON array."
        }),
        "google.protobuf.Value" => json!({
            "description": "Any JSON value."
        }),
        "google.protobuf.Empty" => json!({
            "type": "object",
            "description": "No fields."
        }),
        // Rendered by the facade rather than by the protobuf mapping, so it is
        // a reference to the hand-written schema and not an inline one.
        "google.rpc.Status" => json!({ "$ref": format!("#/components/schemas/{STATUS}") }),
        _ => return None,
    };
    Some(schema)
}

/// One protobuf scalar.
fn scalar_schema(
    scalar: prost_types::field_descriptor_proto::Type,
    field: &str,
) -> Result<Value, Error> {
    use prost_types::field_descriptor_proto::Type;

    // 64-bit integers are strings in proto3 JSON, because a JSON number cannot
    // hold every value one takes. The facade's reader also accepts a number;
    // the canonical form is what a generated client should send.
    let schema = match scalar {
        Type::Double => json!({ "type": "number", "format": "double" }),
        Type::Float => json!({ "type": "number", "format": "float" }),
        Type::Int64 | Type::Sint64 | Type::Sfixed64 => {
            json!({ "type": "string", "format": "int64", "pattern": "^-?[0-9]+$" })
        }
        Type::Uint64 | Type::Fixed64 => {
            json!({ "type": "string", "format": "uint64", "pattern": "^[0-9]+$" })
        }
        Type::Int32 | Type::Sint32 | Type::Sfixed32 => {
            json!({ "type": "integer", "format": "int32" })
        }
        Type::Uint32 | Type::Fixed32 => {
            json!({ "type": "integer", "format": "int64", "minimum": 0 })
        }
        Type::Bool => json!({ "type": "boolean" }),
        Type::String => json!({ "type": "string" }),
        Type::Bytes => json!({ "type": "string", "contentEncoding": "base64" }),
        Type::Message | Type::Group | Type::Enum => {
            return Err(Error::Unsupported {
                element: field.to_string(),
                reason: "a message, group or enum reached the scalar mapping".to_string(),
            })
        }
    };
    Ok(schema)
}

/// The facade's rendering of `google.rpc.Status`.
fn status_schema() -> Value {
    json!({
        "type": "object",
        "description": "One failure. `status` names the `google.rpc.Code` and is what a client \
    branches on; `code` is the HTTP status the same failure carries, and `message` is prose that may \
    be reworded in any release.",
        "properties": {
            "code": { "type": "integer", "format": "int32", "description": "The HTTP status." },
            "status": { "type": "string", "description": "The `google.rpc.Code` by name, such as `NOT_FOUND`." },
            "message": { "type": "string", "description": "A human-readable description. Not stable." },
            "details": {
                "type": "array",
                "description": "Typed detail messages, each tagged with `@type`. `google.rpc.ErrorInfo` \
    carries the stable `reason` a client branches on.",
                "items": { "type": "object" }
            }
        }
    })
}

/// The body of a failed response.
fn error_schema() -> Value {
    json!({
        "type": "object",
        "description": "The body of any response that is not a 2xx.",
        "properties": { "error": { "$ref": format!("#/components/schemas/{STATUS}") } },
        "required": ["error"]
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost_types::field_descriptor_proto::Type;

    #[test]
    fn a_64_bit_integer_is_a_string() {
        let schema = scalar_schema(Type::Int64, "pending").expect("a scalar");
        assert_eq!(schema["type"], "string");
        assert_eq!(schema["format"], "int64");
    }

    #[test]
    fn a_32_bit_integer_is_a_number() {
        let schema = scalar_schema(Type::Int32, "priority").expect("a scalar");
        assert_eq!(schema["type"], "integer");
    }

    #[test]
    fn bytes_are_base64() {
        let schema = scalar_schema(Type::Bytes, "payload").expect("a scalar");
        assert_eq!(schema["contentEncoding"], "base64");
    }

    #[test]
    fn a_message_never_reaches_the_scalar_mapping() {
        assert!(scalar_schema(Type::Message, "job").is_err());
    }

    #[test]
    fn a_well_known_type_is_inlined_and_anything_else_is_not() {
        assert!(well_known("google.protobuf.Timestamp").is_some());
        assert!(well_known("flexiq.v1.Job").is_none());
    }

    #[test]
    fn a_status_is_referenced_rather_than_inlined() {
        let schema = well_known("google.rpc.Status").expect("a well-known type");
        assert_eq!(schema["$ref"], "#/components/schemas/Status");
    }
}
