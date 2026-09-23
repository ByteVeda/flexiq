//! Object-store events: the provider's envelope in, one plain event per object
//! out.
//!
//! Each provider wraps "an object changed" differently — an EventBridge event,
//! a Pub/Sub push message with base64 data, an Event Grid array — so a mapping
//! written against the raw body would be a mapping written against one
//! provider. The adapters here unwrap each into the same shape, and that shape
//! is what the trigger's body pointers address:
//!
//! ```json
//! {"id": "…", "provider": "s3", "event_type": "Object Created",
//!  "bucket": "…", "key": "…", "size": 1024, "etag": "…",
//!  "time": "2026-09-23T10:00:00Z", "raw": { …the provider's own event… }}
//! ```
//!
//! `size`, `etag` and `time` are `null` when the provider did not say; `raw`
//! keeps anything the common shape drops. Proof of origin is not here: each
//! platform's documented mechanism is a shared secret on the endpoint, which
//! the trigger's ordinary verifier checks before any of this runs.

pub mod azure;
pub mod gcs;
pub mod s3;

use serde::Deserialize;
use serde_json::{json, Value};

/// Most events one request may carry. Event Grid batches; a batch past this
/// is refused whole, so the sender retries it smaller instead of this door
/// enqueueing an unbounded number of jobs off one request.
pub const MAX_EVENTS: usize = 100;

/// Which platform delivers a trigger's events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    /// Amazon S3 through an EventBridge API destination.
    S3,
    /// Google Cloud Storage through a Pub/Sub push subscription.
    Gcs,
    /// Azure Blob Storage through an Event Grid webhook subscription.
    Azure,
}

impl Provider {
    /// The configuration spelling, which is also the event's `provider`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::S3 => "s3",
            Self::Gcs => "gcs",
            Self::Azure => "azure",
        }
    }
}

/// What a provider's request turned out to be.
#[derive(Debug, Clone, PartialEq)]
pub enum Unwrapped {
    /// A subscription handshake, answered with this body and no enqueue.
    Handshake(Value),
    /// Object events, in delivery order.
    Events(Vec<Value>),
}

/// Unwrap `document` as `provider` delivers it.
pub fn unwrap(provider: Provider, document: &Value) -> Result<Unwrapped, String> {
    let unwrapped = match provider {
        Provider::S3 => s3::unwrap(document).map(|event| Unwrapped::Events(vec![event])),
        Provider::Gcs => gcs::unwrap(document).map(|event| Unwrapped::Events(vec![event])),
        Provider::Azure => azure::unwrap(document),
    }?;
    if let Unwrapped::Events(events) = &unwrapped {
        if events.len() > MAX_EVENTS {
            return Err(format!(
                "the request carries {} events; at most {MAX_EVENTS} are accepted at once",
                events.len()
            ));
        }
    }
    Ok(unwrapped)
}

/// The common event shape.
struct Event<'a> {
    provider: Provider,
    id: &'a str,
    event_type: &'a str,
    bucket: &'a str,
    key: &'a str,
    size: Option<i64>,
    etag: Option<&'a str>,
    time: Option<&'a str>,
    raw: &'a Value,
}

impl Event<'_> {
    fn into_value(self) -> Value {
        json!({
            "id": self.id,
            "provider": self.provider.as_str(),
            "event_type": self.event_type,
            "bucket": self.bucket,
            "key": self.key,
            "size": self.size,
            "etag": self.etag,
            "time": self.time,
            "raw": self.raw,
        })
    }
}

/// A required string at `pointer`, or an error naming it.
fn text<'a>(document: &'a Value, pointer: &str) -> Result<&'a str, String> {
    document
        .pointer(pointer)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .ok_or_else(|| format!("the event has no {pointer}"))
}

/// An optional string at `pointer`.
fn optional_text<'a>(document: &'a Value, pointer: &str) -> Option<&'a str> {
    document.pointer(pointer).and_then(Value::as_str)
}

/// A size given as a JSON number or, as GCS writes it, a decimal string.
fn size(value: Option<&Value>) -> Option<i64> {
    match value? {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_batch_past_the_cap_is_refused_whole() {
        let events: Vec<Value> = (0..=MAX_EVENTS)
            .map(|n| {
                json!({
                    "id": format!("e{n}"),
                    "eventType": "Microsoft.Storage.BlobCreated",
                    "subject": "/blobServices/default/containers/c/blobs/k",
                    "data": {}
                })
            })
            .collect();
        let error = unwrap(Provider::Azure, &Value::Array(events)).expect_err("must refuse");
        assert!(error.contains("at most"), "{error}");
    }

    #[test]
    fn sizes_read_from_numbers_and_strings() {
        assert_eq!(size(Some(&json!(12))), Some(12));
        assert_eq!(size(Some(&json!("12"))), Some(12));
        assert_eq!(size(Some(&json!("big"))), None);
        assert_eq!(size(None), None);
    }
}
