//! Google Cloud Storage, delivered by a Pub/Sub push subscription.
//!
//! One message per request, in Pub/Sub's push wrapper: `{"message":
//! {"attributes": {"eventType", "bucketId", "objectId", "eventTime", …},
//! "data": <base64 JSON object resource>, "messageId"}, "subscription"}`. The
//! identifying fields are attributes, so they are read there; `data` carries
//! the object resource (size, etag) unless the notification was created with
//! `payload_format=NONE`, in which case those two are `null`.
//!
//! The subscription must use the default wrapped format — an unwrapped push
//! has no attributes left to read. The push endpoint URL carries the shared
//! secret as a query parameter, which the trigger's verifier checks.

use base64::Engine;
use serde_json::Value;

use super::{optional_text, size, text, Event, Provider};

/// Unwrap one Pub/Sub push message carrying a GCS notification.
pub fn unwrap(document: &Value) -> Result<Value, String> {
    let message = document
        .get("message")
        .ok_or("the body is not a Pub/Sub push message: it has no message")?;
    let resource = match optional_text(message, "/data") {
        None | Some("") => Value::Null,
        Some(data) => {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(data)
                .map_err(|_| "message.data is not base64".to_string())?;
            serde_json::from_slice(&bytes)
                .map_err(|_| "message.data is not a JSON object resource".to_string())?
        }
    };

    Ok(Event {
        provider: Provider::Gcs,
        id: text(message, "/messageId")?,
        event_type: text(message, "/attributes/eventType")?,
        bucket: text(message, "/attributes/bucketId")?,
        key: text(message, "/attributes/objectId")?,
        size: size(resource.get("size")),
        etag: optional_text(&resource, "/etag"),
        time: optional_text(message, "/attributes/eventTime"),
        raw: document,
    }
    .into_value())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn push(data: Option<&Value>) -> Value {
        let mut message = json!({
            "attributes": {
                "bucketId": "uploads",
                "objectId": "reports/2026/q3.csv",
                "eventType": "OBJECT_FINALIZE",
                "eventTime": "2026-09-23T10:00:00.000Z",
                "payloadFormat": "JSON_API_V1",
                "notificationConfig": "projects/_/buckets/uploads/notificationConfigs/1"
            },
            "messageId": "13046383729431470",
            "publishTime": "2026-09-23T10:00:00.123Z"
        });
        if let Some(resource) = data {
            message["data"] =
                json!(base64::engine::general_purpose::STANDARD.encode(resource.to_string()));
        }
        json!({"message": message, "subscription": "projects/p/subscriptions/s"})
    }

    #[test]
    fn a_finalize_notification_unwraps() {
        let resource = json!({"name": "reports/2026/q3.csv", "size": "2048", "etag": "CJ3v"});
        let event = unwrap(&push(Some(&resource))).expect("unwraps");
        assert_eq!(event["id"], "13046383729431470");
        assert_eq!(event["provider"], "gcs");
        assert_eq!(event["event_type"], "OBJECT_FINALIZE");
        assert_eq!(event["bucket"], "uploads");
        assert_eq!(event["key"], "reports/2026/q3.csv");
        assert_eq!(event["size"], 2048);
        assert_eq!(event["etag"], "CJ3v");
        assert_eq!(event["time"], "2026-09-23T10:00:00.000Z");
    }

    #[test]
    fn a_notification_without_a_payload_still_names_its_object() {
        let event = unwrap(&push(None)).expect("unwraps");
        assert_eq!(event["key"], "reports/2026/q3.csv");
        assert_eq!(event["size"], Value::Null);
        assert_eq!(event["etag"], Value::Null);
    }

    #[test]
    fn an_unwrapped_push_is_refused() {
        assert!(unwrap(&json!({"name": "x", "bucket": "y"})).is_err());
    }
}
