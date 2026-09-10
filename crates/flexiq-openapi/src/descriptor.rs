//! The committed contract, flattened into what a document generator needs.
//!
//! One read of `contracts/descriptor.binpb` produces every message, enum and
//! method in it, keyed by fully qualified name and carrying the `.proto`'s own
//! comments. Those comments are the reason this goes through the descriptor
//! rather than the generated Rust: `buf build` keeps source info, so the prose
//! a client would read in the `.proto` is available to put in the document.

use std::collections::BTreeMap;

use prost::Message as _;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{DescriptorProto, EnumDescriptorProto, FileDescriptorProto, FileDescriptorSet};

use crate::Error;

/// What a field holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    /// A protobuf scalar, as `prost-types` names it.
    Scalar(Type),
    /// A message, by fully qualified name without the leading dot.
    Message(String),
    /// An enum, by fully qualified name without the leading dot.
    Enum(String),
}

/// One field of one message.
#[derive(Debug, Clone)]
pub struct Field {
    /// The name the `.proto` gives it.
    pub name: String,
    /// The name JSON gives it, which is what a client sees.
    pub json_name: String,
    /// What it holds.
    pub kind: Kind,
    /// Whether it is `repeated` — including the repeated map entry a `map<>`
    /// compiles to.
    pub repeated: bool,
    /// The oneof it belongs to, if any.
    ///
    /// `None` for a `optional` field: proto3 compiles explicit presence into a
    /// synthetic one-arm oneof, which is a storage detail and not a choice a
    /// client makes.
    pub oneof: Option<String>,
    /// Its leading comment in the `.proto`.
    pub comment: Option<String>,
}

/// One message.
#[derive(Debug, Clone)]
pub struct Message {
    /// Fully qualified, without the leading dot.
    pub full_name: String,
    /// In declaration order.
    pub fields: Vec<Field>,
    /// Whether this is the synthetic entry type behind a `map<K, V>`.
    pub map_entry: bool,
    /// Its leading comment in the `.proto`.
    pub comment: Option<String>,
}

impl Message {
    /// The names of the real oneofs its fields belong to, in the order the
    /// fields first mention them.
    pub fn oneofs(&self) -> Vec<String> {
        let mut names: Vec<String> = Vec::new();
        for field in &self.fields {
            if let Some(oneof) = &field.oneof {
                if !names.contains(oneof) {
                    names.push(oneof.clone());
                }
            }
        }
        names
    }
}

/// One enum value.
#[derive(Debug, Clone)]
pub struct EnumValue {
    /// The name, which is what proto3 JSON writes.
    pub name: String,
    /// Its leading comment in the `.proto`.
    pub comment: Option<String>,
}

/// One enum.
#[derive(Debug, Clone)]
pub struct Enum {
    /// Fully qualified, without the leading dot.
    pub full_name: String,
    /// In declaration order, so the zero value comes first.
    pub values: Vec<EnumValue>,
    /// Its leading comment in the `.proto`.
    pub comment: Option<String>,
}

/// One RPC.
#[derive(Debug, Clone)]
pub struct Method {
    /// The request message, fully qualified without the leading dot.
    pub input: String,
    /// The response message, fully qualified without the leading dot.
    pub output: String,
    /// Its leading comment in the `.proto`.
    pub comment: Option<String>,
}

/// Everything the descriptor declares, by fully qualified name.
#[derive(Debug, Clone)]
pub struct Contract {
    messages: BTreeMap<String, Message>,
    enums: BTreeMap<String, Enum>,
    methods: BTreeMap<String, Method>,
}

impl Contract {
    /// Read a `FileDescriptorSet`.
    pub fn load(descriptor: &[u8]) -> Result<Self, Error> {
        let mut contract = Self {
            messages: BTreeMap::new(),
            enums: BTreeMap::new(),
            methods: BTreeMap::new(),
        };
        for file in FileDescriptorSet::decode(descriptor)?.file {
            contract.absorb(&file);
        }
        Ok(contract)
    }

    /// One message, or the reason it is not there.
    pub fn message(&self, full_name: &str) -> Result<&Message, Error> {
        self.messages
            .get(full_name)
            .ok_or_else(|| Error::Undeclared(full_name.to_string()))
    }

    /// One enum, or the reason it is not there.
    pub fn enumeration(&self, full_name: &str) -> Result<&Enum, Error> {
        self.enums
            .get(full_name)
            .ok_or_else(|| Error::Undeclared(full_name.to_string()))
    }

    /// One RPC, by `Service.Method`.
    pub fn method(&self, service: &str, method: &str) -> Result<&Method, Error> {
        let key = format!("{service}.{method}");
        self.methods.get(&key).ok_or(Error::Undeclared(key))
    }

    fn absorb(&mut self, file: &FileDescriptorProto) {
        let comments = Comments::of(file);
        let package = file.package().to_string();

        for (index, message) in file.message_type.iter().enumerate() {
            self.absorb_message(message, &package, &[MESSAGE_TYPE, index as i32], &comments);
        }
        for (index, declared) in file.enum_type.iter().enumerate() {
            self.absorb_enum(declared, &package, &[ENUM_TYPE, index as i32], &comments);
        }
        for (index, service) in file.service.iter().enumerate() {
            for (nth, method) in service.method.iter().enumerate() {
                let path = [SERVICE, index as i32, SERVICE_METHOD, nth as i32];
                self.methods.insert(
                    format!("{}.{}", service.name(), method.name()),
                    Method {
                        input: unqualify(method.input_type()),
                        output: unqualify(method.output_type()),
                        comment: comments.at(&path),
                    },
                );
            }
        }
    }

    fn absorb_message(
        &mut self,
        message: &DescriptorProto,
        scope: &str,
        path: &[i32],
        comments: &Comments,
    ) {
        let full_name = format!("{scope}.{}", message.name());
        let fields = message
            .field
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let at = [path, &[MESSAGE_FIELD, index as i32]].concat();
                Field {
                    name: field.name().to_string(),
                    json_name: json_name(field.name(), field.json_name()),
                    kind: match field.r#type() {
                        Type::Message | Type::Group => Kind::Message(unqualify(field.type_name())),
                        Type::Enum => Kind::Enum(unqualify(field.type_name())),
                        scalar => Kind::Scalar(scalar),
                    },
                    repeated: field.label() == Label::Repeated,
                    // A `optional` field is compiled into a synthetic oneof of
                    // its own; only a oneof the author wrote is a choice.
                    oneof: match field.proto3_optional() {
                        true => None,
                        false => field
                            .oneof_index
                            .and_then(|index| message.oneof_decl.get(index as usize))
                            .map(|oneof| oneof.name().to_string()),
                    },
                    comment: comments.at(&at),
                }
            })
            .collect();

        self.messages.insert(
            full_name.clone(),
            Message {
                full_name,
                fields,
                map_entry: message.options.as_ref().is_some_and(|o| o.map_entry()),
                comment: comments.at(path),
            },
        );

        let scope = format!("{scope}.{}", message.name());
        for (index, nested) in message.nested_type.iter().enumerate() {
            let at = [path, &[MESSAGE_NESTED, index as i32]].concat();
            self.absorb_message(nested, &scope, &at, comments);
        }
        for (index, declared) in message.enum_type.iter().enumerate() {
            let at = [path, &[MESSAGE_ENUM, index as i32]].concat();
            self.absorb_enum(declared, &scope, &at, comments);
        }
    }

    fn absorb_enum(
        &mut self,
        declared: &EnumDescriptorProto,
        scope: &str,
        path: &[i32],
        comments: &Comments,
    ) {
        let full_name = format!("{scope}.{}", declared.name());
        let values = declared
            .value
            .iter()
            .enumerate()
            .map(|(index, value)| {
                let at = [path, &[ENUM_VALUE, index as i32]].concat();
                EnumValue {
                    name: value.name().to_string(),
                    comment: comments.at(&at),
                }
            })
            .collect();
        self.enums.insert(
            full_name.clone(),
            Enum {
                full_name,
                values,
                comment: comments.at(path),
            },
        );
    }
}

// Field numbers of the descriptor's own fields, which is what a source-info
// path is made of. Spelled out rather than inlined: a bare `4` in a path
// literal is unreadable and a wrong one silently yields no comment.
const MESSAGE_TYPE: i32 = 4;
const ENUM_TYPE: i32 = 5;
const SERVICE: i32 = 6;
const MESSAGE_FIELD: i32 = 2;
const MESSAGE_NESTED: i32 = 3;
const MESSAGE_ENUM: i32 = 4;
const ENUM_VALUE: i32 = 2;
const SERVICE_METHOD: i32 = 2;

/// The leading comments of one file, by the path that identifies an element.
struct Comments(BTreeMap<Vec<i32>, String>);

impl Comments {
    fn of(file: &FileDescriptorProto) -> Self {
        let mut found = BTreeMap::new();
        if let Some(info) = &file.source_code_info {
            for location in &info.location {
                let comment = location.leading_comments();
                if !comment.is_empty() {
                    found.insert(location.path.clone(), tidy(comment));
                }
            }
        }
        Self(found)
    }

    fn at(&self, path: &[i32]) -> Option<String> {
        self.0.get(path).cloned()
    }
}

/// A protobuf comment carries the space that followed each `//` and a trailing
/// newline. Neither belongs in a description.
fn tidy(comment: &str) -> String {
    comment
        .lines()
        .map(|line| line.strip_prefix(' ').unwrap_or(line))
        .collect::<Vec<_>>()
        .join("\n")
        .trim_end()
        .to_string()
}

/// A descriptor names types with a leading dot. Nothing else does.
fn unqualify(name: &str) -> String {
    name.strip_prefix('.').unwrap_or(name).to_string()
}

/// The JSON name of a field, deriving it when the descriptor carries none.
fn json_name(name: &str, declared: &str) -> String {
    if !declared.is_empty() {
        return declared.to_string();
    }
    // The specification's own derivation: drop each underscore and capitalise
    // the letter that followed it.
    let mut out = String::with_capacity(name.len());
    let mut capitalise = false;
    for character in name.chars() {
        if character == '_' {
            capitalise = true;
        } else if capitalise {
            out.extend(character.to_uppercase());
            capitalise = false;
        } else {
            out.push(character);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_declared_json_name_wins_over_the_derivation() {
        assert_eq!(json_name("task_name", "taskName"), "taskName");
        assert_eq!(json_name("task_name", ""), "taskName");
        assert_eq!(json_name("id", ""), "id");
    }

    #[test]
    fn a_leading_dot_is_not_part_of_a_type_name() {
        assert_eq!(unqualify(".flexiq.v1.Job"), "flexiq.v1.Job");
        assert_eq!(unqualify("flexiq.v1.Job"), "flexiq.v1.Job");
    }

    #[test]
    fn a_comment_loses_its_indent_and_its_trailing_newline() {
        assert_eq!(tidy(" One line.\n"), "One line.");
        assert_eq!(tidy(" First.\n Second.\n"), "First.\nSecond.");
        assert_eq!(
            tidy(" Indented:\n     four spaces.\n"),
            "Indented:\n    four spaces."
        );
    }
}
