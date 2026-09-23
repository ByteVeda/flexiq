//! The request body as a JSON document, which is what mappings point into.
//!
//! Two media types, because those are the two webhook senders use: JSON, and
//! the HTML form encoding Twilio and older providers post. A form becomes an
//! object of strings so one pointer syntax addresses both. Anything else is
//! refused rather than guessed at — a body this door cannot read is a body a
//! mapping would silently resolve to nothing.

use axum::http::HeaderMap;
use serde_json::{Map, Value};

/// Why a body could not become a document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DocumentError {
    /// The content type is neither JSON nor a form. Answered `415`.
    UnsupportedMediaType(String),
    /// The body does not parse as its declared type. Answered `400`.
    Malformed(String),
}

/// Parse `body` according to its `content-type`.
///
/// An empty body is `null`, whatever its type: a trigger that maps only
/// headers, the query or constants needs no body at all. A body with no
/// content type is read as JSON, which is what every sender this door expects
/// sends when it forgets to say so.
pub fn parse(headers: &HeaderMap, body: &[u8]) -> Result<Value, DocumentError> {
    if body.is_empty() {
        return Ok(Value::Null);
    }
    let mime = headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(|mime| mime.trim().to_ascii_lowercase());

    match mime.as_deref() {
        None | Some("application/json") => json(body),
        Some(mime) if mime.starts_with("application/") && mime.ends_with("+json") => json(body),
        Some("application/x-www-form-urlencoded") => Ok(form(body)),
        Some(other) => Err(DocumentError::UnsupportedMediaType(format!(
            "content-type {other} is not accepted; send application/json or \
             application/x-www-form-urlencoded"
        ))),
    }
}

fn json(body: &[u8]) -> Result<Value, DocumentError> {
    serde_json::from_slice(body)
        .map_err(|error| DocumentError::Malformed(format!("the body is not valid JSON: {error}")))
}

/// A form body as an object of strings. A repeated name keeps its last value,
/// so a pointer resolves to one string rather than sometimes to a list.
fn form(body: &[u8]) -> Value {
    let fields: Map<String, Value> = url::form_urlencoded::parse(body)
        .map(|(name, value)| (name.into_owned(), Value::String(value.into_owned())))
        .collect();
    Value::Object(fields)
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;
    use serde_json::json;

    use super::*;

    fn typed(mime: &'static str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static(mime));
        headers
    }

    #[test]
    fn json_with_parameters_and_vendor_suffixes() {
        let body = br#"{"a":1}"#;
        for mime in [
            "application/json",
            "application/json; charset=utf-8",
            "application/cloudevents+json",
        ] {
            assert_eq!(parse(&typed(mime), body), Ok(json!({"a": 1})), "{mime}");
        }
        assert_eq!(parse(&HeaderMap::new(), body), Ok(json!({"a": 1})));
    }

    #[test]
    fn a_form_is_an_object_of_strings() {
        let parsed = parse(
            &typed("application/x-www-form-urlencoded"),
            b"From=%2B1555&Body=hi+there&Body=last",
        );
        assert_eq!(parsed, Ok(json!({"From": "+1555", "Body": "last"})));
    }

    #[test]
    fn an_empty_body_is_null() {
        assert_eq!(parse(&typed("text/plain"), b""), Ok(Value::Null));
    }

    #[test]
    fn other_types_and_bad_json_are_refused_differently() {
        assert!(matches!(
            parse(&typed("text/plain"), b"hi"),
            Err(DocumentError::UnsupportedMediaType(_))
        ));
        assert!(matches!(
            parse(&typed("application/json"), b"{"),
            Err(DocumentError::Malformed(_))
        ));
    }
}
