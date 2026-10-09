//! W3C trace context carried in a job's metadata (cross-SDK).
//!
//! The carrier is the metadata JSON object itself: `traceparent` and
//! `tracestate` sit at its top level, under the header names the W3C Trace
//! Context spec gives them, so every SDK's OpenTelemetry propagator reads and
//! writes them as a plain text map. Nothing in the core interprets them; a job
//! carries them through retry, `step.sleep`, pub/sub fan-out and dead-letter
//! replay because those paths keep the job's metadata.
//!
//! Injecting into someone else's metadata follows one rule everywhere — the
//! shells' middleware and the server's producer door alike:
//!
//! - absent or empty metadata becomes an object holding just the carrier;
//! - an object gains the carrier keys **unless it already names either one**,
//!   in which case the caller's own context wins whole;
//! - anything else (an array, a string, text that is not JSON) is left as it is,
//!   because there is no way to add a key to it without destroying what the
//!   caller wrote.
//!
//! An object's existing bytes are kept: the keys are spliced in after its
//! opening brace rather than the document being re-serialised, so metadata the
//! caller formatted for its own readers reads back the way it went in.

use serde_json::Value;

/// The metadata key, and header name, of the W3C `traceparent`.
pub const TRACEPARENT: &str = "traceparent";

/// The metadata key, and header name, of the W3C `tracestate`.
pub const TRACESTATE: &str = "tracestate";

/// The longest `tracestate` carried. W3C asks every platform to propagate at
/// least 512 bytes of it and allows dropping it whole past that.
pub const MAX_TRACESTATE_BYTES: usize = 512;

/// One validated W3C trace context: a `traceparent` and its optional
/// `tracestate`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceContext {
    traceparent: String,
    tracestate: Option<String>,
}

impl TraceContext {
    /// The context two header values describe, or `None` without a valid
    /// `traceparent`.
    ///
    /// A `tracestate` with no valid `traceparent` is dropped with it, as W3C
    /// requires; an empty or oversized one is dropped on its own.
    pub fn from_headers(traceparent: Option<&str>, tracestate: Option<&str>) -> Option<Self> {
        let traceparent = traceparent.map(str::trim)?;
        if !is_valid_traceparent(traceparent) {
            return None;
        }
        let tracestate = tracestate
            .map(str::trim)
            .filter(|state| !state.is_empty() && state.len() <= MAX_TRACESTATE_BYTES)
            .map(str::to_string);
        Some(Self {
            traceparent: traceparent.to_string(),
            tracestate,
        })
    }

    /// The `traceparent` value.
    pub fn traceparent(&self) -> &str {
        &self.traceparent
    }

    /// The `tracestate` value, when there is one.
    pub fn tracestate(&self) -> Option<&str> {
        self.tracestate.as_deref()
    }

    /// `metadata` with this context added, by the rule in the module docs.
    pub fn merge_into(&self, metadata: Option<String>) -> Option<String> {
        let Some(text) = metadata.filter(|text| !text.trim().is_empty()) else {
            return Some(format!("{{{}}}", self.entries()));
        };
        let Ok(Value::Object(object)) = serde_json::from_str::<Value>(&text) else {
            return Some(text);
        };
        if object.contains_key(TRACEPARENT) || object.contains_key(TRACESTATE) {
            return Some(text);
        }
        // A parsed object's first non-whitespace byte is its `{`.
        let brace = text.len() - text.trim_start().len();
        let (head, tail) = text.split_at(brace + 1);
        let separator = if object.is_empty() { "" } else { "," };
        Some(format!("{head}{}{separator}{tail}", self.entries()))
    }

    /// The carrier as JSON object members, without the braces.
    fn entries(&self) -> String {
        let mut entries = format!("\"{TRACEPARENT}\":{}", json_string(&self.traceparent));
        if let Some(state) = &self.tracestate {
            entries.push_str(&format!(",\"{TRACESTATE}\":{}", json_string(state)));
        }
        entries
    }
}

/// Whether `value` is a `traceparent` W3C Trace Context accepts.
///
/// Version `00` is exactly `00-<32 hex>-<16 hex>-<2 hex>`, lowercase, with
/// neither id all zeros. A later version may append fields after a `-`, which
/// is read past rather than refused; `ff` is never valid.
pub fn is_valid_traceparent(value: &str) -> bool {
    let bytes = value.as_bytes();
    // ASCII first, so slicing by byte offset below cannot split a character.
    if !value.is_ascii() || bytes.len() < 55 || (bytes.len() > 55 && bytes[55] != b'-') {
        return false;
    }
    let version = &value[..2];
    if version == "00" && bytes.len() != 55 {
        return false;
    }
    let fields = [(0, 2), (3, 35), (36, 52), (53, 55)];
    let dashes = [2, 35, 52];
    if dashes.iter().any(|&at| bytes[at] != b'-')
        || !fields
            .iter()
            .all(|&(from, to)| is_lower_hex(&value[from..to]))
    {
        return false;
    }
    version != "ff" && !is_zero(&value[3..35]) && !is_zero(&value[36..52])
}

fn is_lower_hex(field: &str) -> bool {
    field
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_zero(field: &str) -> bool {
    field.bytes().all(|byte| byte == b'0')
}

fn json_string(value: &str) -> String {
    Value::String(value.to_string()).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    const PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    fn context(state: Option<&str>) -> TraceContext {
        TraceContext::from_headers(Some(PARENT), state).expect("a valid traceparent")
    }

    #[test]
    fn a_w3c_example_traceparent_is_valid() {
        assert!(is_valid_traceparent(PARENT));
        assert!(is_valid_traceparent(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"
        ));
    }

    #[test]
    fn a_malformed_traceparent_is_refused() {
        for value in [
            "",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00_4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01extra",
            "00-4bf92f3577b34da6a3ce929d0e0e473g-00f067aa0ba902b7-01",
            "0é-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        ] {
            assert!(!is_valid_traceparent(value), "{value:?} was accepted");
        }
    }

    #[test]
    fn a_later_version_may_carry_trailing_fields() {
        assert!(is_valid_traceparent(
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-future"
        ));
    }

    #[test]
    fn tracestate_rides_only_with_a_valid_traceparent() {
        assert_eq!(
            TraceContext::from_headers(Some("garbage"), Some("a=1")),
            None
        );
        assert_eq!(TraceContext::from_headers(None, Some("a=1")), None);
        assert_eq!(context(Some(" a=1 ")).tracestate(), Some("a=1"));
        assert_eq!(context(Some("  ")).tracestate(), None);
        let oversized = "a".repeat(MAX_TRACESTATE_BYTES + 1);
        assert_eq!(context(Some(&oversized)).tracestate(), None);
    }

    #[test]
    fn absent_or_empty_metadata_becomes_the_carrier() {
        let expected = format!(r#"{{"traceparent":"{PARENT}","tracestate":"a=1"}}"#);
        assert_eq!(
            context(Some("a=1")).merge_into(None),
            Some(expected.clone())
        );
        assert_eq!(
            context(Some("a=1")).merge_into(Some(" ".into())),
            Some(expected)
        );
    }

    #[test]
    fn an_object_gains_the_keys_with_its_own_bytes_kept() {
        let merged = context(None).merge_into(Some(r#"  { "user" : 1 }"#.into()));
        assert_eq!(
            merged.as_deref(),
            Some(format!(r#"  {{"traceparent":"{PARENT}", "user" : 1 }}"#).as_str())
        );
        let empty = context(None).merge_into(Some("{ }".into()));
        assert_eq!(
            empty.as_deref(),
            Some(format!(r#"{{"traceparent":"{PARENT}" }}"#).as_str())
        );
        for merged in [merged, empty] {
            let value: Value = serde_json::from_str(&merged.expect("merged")).expect("JSON");
            assert_eq!(value[TRACEPARENT], PARENT);
        }
    }

    #[test]
    fn a_callers_own_context_wins_whole() {
        for own in [r#"{"traceparent":"mine"}"#, r#"{"tracestate":"mine=1"}"#] {
            assert_eq!(
                context(Some("a=1")).merge_into(Some(own.into())).as_deref(),
                Some(own)
            );
        }
    }

    #[test]
    fn metadata_that_is_not_an_object_is_left_alone() {
        for other in ["[1,2]", r#""text""#, "42", "not json", "{broken"] {
            assert_eq!(
                context(None).merge_into(Some(other.into())).as_deref(),
                Some(other)
            );
        }
    }

    #[test]
    fn values_are_json_escaped() {
        let traced = context(Some(r#"k="v""#)).merge_into(None).expect("merged");
        let value: Value = serde_json::from_str(&traced).expect("JSON");
        assert_eq!(value[TRACESTATE], r#"k="v""#);
    }
}
