//! From a request to a task's arguments.
//!
//! A mapping is data: each argument names where its value comes from — a JSON
//! Pointer (RFC 6901) into the body, a header, a query parameter, or a
//! constant. There is no expression, no conditional and no transformation,
//! because anything more is a scripting engine, and the issue that asked for
//! triggers put that out of scope. A payload that needs reshaping is the
//! task's to reshape.
//!
//! The arguments leave through `flexiq_core::wire::encode_call`, the encoder
//! every producer uses, so a job a trigger enqueued is indistinguishable from
//! one an SDK enqueued with the same values.

use std::collections::BTreeMap;

use flexiq_core::wire::{encode_call, WireValue};
use serde::Deserialize;
use serde_json::Value;

use crate::trigger::auth::Inbound;

/// Where one value comes from.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "from", rename_all = "snake_case", deny_unknown_fields)]
pub enum Selector {
    /// A JSON Pointer into the body document. `""` is the whole document.
    Body {
        /// RFC 6901 pointer: empty, or starting with `/`.
        #[serde(default)]
        pointer: String,
        /// Resolve a missing value to absent rather than refusing the request.
        #[serde(default)]
        optional: bool,
    },
    /// A request header, as text.
    Header {
        /// Header name, compared case-insensitively.
        name: String,
        /// Resolve a missing header to absent rather than refusing.
        #[serde(default)]
        optional: bool,
    },
    /// A query parameter, as text.
    Query {
        /// Parameter name.
        name: String,
        /// Resolve a missing parameter to absent rather than refusing.
        #[serde(default)]
        optional: bool,
    },
    /// A fixed value, written into the definition.
    Const {
        /// The value, any JSON.
        value: Value,
    },
}

/// Why a request could not be mapped. Answered `422`; the message names the
/// selector, never the value it found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappingError(pub String);

impl Selector {
    /// The whole body, which is what a trigger maps when it says nothing.
    pub fn whole_body() -> Self {
        Self::Body {
            pointer: String::new(),
            optional: false,
        }
    }

    /// Refuse a selector that can never resolve, at boot.
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Body { pointer, .. } if !pointer.is_empty() && !pointer.starts_with('/') => {
                Err(format!(
                    "pointer {pointer:?} is not a JSON Pointer: it must be empty or start with '/'"
                ))
            }
            Self::Header { name, .. } if axum::http::HeaderName::try_from(name).is_err() => {
                Err(format!("{name:?} is not a valid header name"))
            }
            Self::Query { name, .. } if name.is_empty() => {
                Err("a query selector needs a parameter name".to_string())
            }
            _ => Ok(()),
        }
    }

    /// The selected value, `None` for an optional one that is absent.
    pub fn resolve(
        &self,
        inbound: &Inbound<'_>,
        document: &Value,
    ) -> Result<Option<Value>, MappingError> {
        let (found, optional) = match self {
            Self::Const { value } => return Ok(Some(value.clone())),
            Self::Body { pointer, optional } => (document.pointer(pointer).cloned(), *optional),
            Self::Header { name, optional } => (
                inbound.header(name).map(|text| Value::String(text.into())),
                *optional,
            ),
            Self::Query { name, optional } => {
                (inbound.query_param(name).map(Value::String), *optional)
            }
        };
        match found {
            Some(value) => Ok(Some(value)),
            None if optional => Ok(None),
            None => Err(MappingError(format!("{} is missing", self.describe()))),
        }
    }

    /// The selected value as text, for a deduplication key: a string as it
    /// is, a number in its JSON spelling, anything else refused.
    pub fn resolve_text(
        &self,
        inbound: &Inbound<'_>,
        document: &Value,
    ) -> Result<Option<String>, MappingError> {
        match self.resolve(inbound, document)? {
            None => Ok(None),
            Some(Value::String(text)) if !text.is_empty() => Ok(Some(text)),
            Some(Value::Number(number)) => Ok(Some(number.to_string())),
            Some(_) => Err(MappingError(format!(
                "{} must be a non-empty string or a number to key on",
                self.describe()
            ))),
        }
    }

    fn describe(&self) -> String {
        match self {
            Self::Body { pointer, .. } if pointer.is_empty() => "the body".to_string(),
            Self::Body { pointer, .. } => format!("body pointer {pointer:?}"),
            Self::Header { name, .. } => format!("header {name:?}"),
            Self::Query { name, .. } => format!("query parameter {name:?}"),
            Self::Const { .. } => "a constant".to_string(),
        }
    }
}

/// A task's positional and keyword arguments, each from one selector.
#[derive(Debug, Clone, PartialEq)]
pub struct Mapping {
    /// Positional arguments, in order. An absent optional one becomes `null`,
    /// because dropping it would shift every argument after it.
    pub args: Vec<Selector>,
    /// Keyword arguments, in name order. An absent optional one is omitted,
    /// so the task's own default applies.
    pub kwargs: BTreeMap<String, Selector>,
}

impl Mapping {
    /// The payload envelope for one request.
    pub fn payload(
        &self,
        inbound: &Inbound<'_>,
        document: &Value,
    ) -> Result<Vec<u8>, MappingError> {
        let mut positional = Vec::with_capacity(self.args.len());
        for selector in &self.args {
            let value = selector.resolve(inbound, document)?;
            positional.push(value.as_ref().map_or(Ok(WireValue::Null), to_wire)?);
        }
        let mut keyword = Vec::with_capacity(self.kwargs.len());
        for (name, selector) in &self.kwargs {
            if let Some(value) = selector.resolve(inbound, document)? {
                keyword.push((name.clone(), to_wire(&value)?));
            }
        }
        Ok(encode_call(&positional, &keyword))
    }
}

/// A JSON value as a wire value.
///
/// An integer past `i64` is refused rather than rounded into a float: the
/// wire contract keeps integers exact, and a job carrying a different number
/// from the one the sender wrote is worse than a `422`.
fn to_wire(value: &Value) -> Result<WireValue, MappingError> {
    Ok(match value {
        Value::Null => WireValue::Null,
        Value::Bool(flag) => WireValue::Bool(*flag),
        Value::Number(number) => {
            if let Some(integer) = number.as_i64() {
                WireValue::Integer(integer)
            } else if number.is_u64() {
                return Err(MappingError(format!(
                    "the integer {number} does not fit a signed 64-bit integer"
                )));
            } else {
                // A JSON number that is neither integer form is an f64 by
                // construction, and serde_json never produces a non-finite one.
                WireValue::Float(number.as_f64().unwrap_or_default())
            }
        }
        Value::String(text) => WireValue::Text(text.clone()),
        Value::Array(items) => {
            WireValue::Array(items.iter().map(to_wire).collect::<Result<_, _>>()?)
        }
        Value::Object(fields) => WireValue::Map(
            fields
                .iter()
                .map(|(key, value)| Ok((key.clone(), to_wire(value)?)))
                .collect::<Result<_, MappingError>>()?,
        ),
    })
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue};
    use serde_json::json;

    use super::*;

    fn inbound<'a>(headers: &'a HeaderMap, query: &'a str) -> Inbound<'a> {
        Inbound {
            headers,
            query,
            body: b"",
            now_secs: 0,
        }
    }

    fn selector(raw: Value) -> Selector {
        serde_json::from_value(raw).expect("a valid selector")
    }

    #[test]
    fn selectors_parse_from_their_config_spelling() {
        assert_eq!(
            selector(json!({"from": "body", "pointer": "/a"})),
            Selector::Body {
                pointer: "/a".into(),
                optional: false
            }
        );
        assert_eq!(
            selector(json!({"from": "const", "value": [1, 2]})),
            Selector::Const {
                value: json!([1, 2])
            }
        );
        let unknown: Result<Selector, _> =
            serde_json::from_value(json!({"from": "body", "pointr": "/a"}));
        assert!(unknown.is_err(), "a misspelt field must not be ignored");
        let script: Result<Selector, _> =
            serde_json::from_value(json!({"from": "expression", "value": "a+b"}));
        assert!(script.is_err());
    }

    #[test]
    fn validation_catches_what_can_never_resolve() {
        assert!(selector(json!({"from": "body", "pointer": "a"}))
            .validate()
            .is_err());
        assert!(selector(json!({"from": "header", "name": "bad header"}))
            .validate()
            .is_err());
        assert!(selector(json!({"from": "body"})).validate().is_ok());
    }

    #[test]
    fn every_source_resolves() {
        let mut headers = HeaderMap::new();
        headers.insert("x-delivery", HeaderValue::from_static("d-1"));
        let request = inbound(&headers, "source=ci");
        let document = json!({"event": {"type": "push", "n": 7}});

        let resolve = |raw| selector(raw).resolve(&request, &document);
        assert_eq!(
            resolve(json!({"from": "body", "pointer": "/event/type"})),
            Ok(Some(json!("push")))
        );
        assert_eq!(
            resolve(json!({"from": "header", "name": "X-Delivery"})),
            Ok(Some(json!("d-1")))
        );
        assert_eq!(
            resolve(json!({"from": "query", "name": "source"})),
            Ok(Some(json!("ci")))
        );
        assert_eq!(
            resolve(json!({"from": "body", "pointer": "/nope", "optional": true})),
            Ok(None)
        );
        assert!(resolve(json!({"from": "body", "pointer": "/nope"})).is_err());
    }

    #[test]
    fn a_dedup_key_is_text_or_a_number_only() {
        let headers = HeaderMap::new();
        let request = inbound(&headers, "");
        let document = json!({"id": "evt_1", "n": 42, "o": {}, "e": ""});
        let text = |pointer: &str| {
            Selector::Body {
                pointer: pointer.into(),
                optional: false,
            }
            .resolve_text(&request, &document)
        };
        assert_eq!(text("/id"), Ok(Some("evt_1".into())));
        assert_eq!(text("/n"), Ok(Some("42".into())));
        assert!(text("/o").is_err());
        assert!(
            text("/e").is_err(),
            "an empty key would collide every delivery"
        );
    }

    #[test]
    fn the_payload_matches_what_a_producer_would_encode() {
        let headers = HeaderMap::new();
        let request = inbound(&headers, "");
        let document = json!({"id": "evt_1", "amount": 1250, "ratio": 0.5});
        let mapping = Mapping {
            args: vec![
                selector(json!({"from": "body", "pointer": "/id"})),
                selector(json!({"from": "body", "pointer": "/gone", "optional": true})),
            ],
            kwargs: BTreeMap::from([
                (
                    "amount".to_string(),
                    selector(json!({"from": "body", "pointer": "/amount"})),
                ),
                (
                    "ratio".to_string(),
                    selector(json!({"from": "body", "pointer": "/ratio"})),
                ),
                (
                    "skipped".to_string(),
                    selector(json!({"from": "body", "pointer": "/gone", "optional": true})),
                ),
            ]),
        };

        let expected = encode_call(
            &[WireValue::Text("evt_1".into()), WireValue::Null],
            &[
                ("amount".to_string(), WireValue::Integer(1250)),
                ("ratio".to_string(), WireValue::Float(0.5)),
            ],
        );
        assert_eq!(mapping.payload(&request, &document), Ok(expected));
    }

    #[test]
    fn an_integer_past_i64_is_refused_not_rounded() {
        let error = to_wire(&json!(u64::MAX)).expect_err("must refuse");
        assert!(error.0.contains("64-bit"), "{error:?}");
        assert_eq!(to_wire(&json!(i64::MIN)), Ok(WireValue::Integer(i64::MIN)));
    }
}
