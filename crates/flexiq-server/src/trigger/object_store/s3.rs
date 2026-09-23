//! Amazon S3, delivered by an EventBridge rule to an API destination.
//!
//! One event per request, in EventBridge's envelope:
//! `{"id", "detail-type", "source": "aws.s3", "time", "detail": {"bucket":
//! {"name"}, "object": {"key", "size", "etag"}}}`. The API destination's
//! connection carries the shared secret as a header — an API-key or basic
//! connection — and that is what the trigger's verifier checks.
//!
//! EventBridge writes the object key as stored, not URL-encoded the way S3's
//! older bucket notifications do, so it is passed through untouched.

use serde_json::Value;

use super::{optional_text, size, text, Event, Provider};

/// Unwrap one EventBridge S3 event.
pub fn unwrap(document: &Value) -> Result<Value, String> {
    if optional_text(document, "/source") != Some("aws.s3") {
        return Err("the event's source is not aws.s3".to_string());
    }
    Ok(Event {
        provider: Provider::S3,
        id: text(document, "/id")?,
        event_type: text(document, "/detail-type")?,
        bucket: text(document, "/detail/bucket/name")?,
        key: text(document, "/detail/object/key")?,
        size: size(document.pointer("/detail/object/size")),
        etag: optional_text(document, "/detail/object/etag"),
        time: optional_text(document, "/time"),
        raw: document,
    }
    .into_value())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// Trimmed from the "Object Created" example in the S3 EventBridge docs.
    fn created() -> Value {
        json!({
            "version": "0",
            "id": "2d4eba74-fd51-3966-4bfa-b013c9da8ff1",
            "detail-type": "Object Created",
            "source": "aws.s3",
            "account": "123456789012",
            "time": "2021-11-12T00:00:00Z",
            "region": "ca-central-1",
            "resources": ["arn:aws:s3:::example-bucket"],
            "detail": {
                "version": "0",
                "bucket": {"name": "example-bucket"},
                "object": {
                    "key": "example-key",
                    "size": 5,
                    "etag": "b1946ac92492d2347c6235b4d2611184",
                    "version-id": "IYV3p45BT0ac8hjHg1houSdS1a.Mro8e",
                    "sequencer": "617f08299329d189"
                },
                "request-id": "N4N7GDK58NMKJ12R",
                "requester": "123456789012",
                "source-ip-address": "1.2.3.4",
                "reason": "PutObject"
            }
        })
    }

    #[test]
    fn an_object_created_event_unwraps() {
        let event = unwrap(&created()).expect("unwraps");
        assert_eq!(event["id"], "2d4eba74-fd51-3966-4bfa-b013c9da8ff1");
        assert_eq!(event["provider"], "s3");
        assert_eq!(event["event_type"], "Object Created");
        assert_eq!(event["bucket"], "example-bucket");
        assert_eq!(event["key"], "example-key");
        assert_eq!(event["size"], 5);
        assert_eq!(event["etag"], "b1946ac92492d2347c6235b4d2611184");
        assert_eq!(event["time"], "2021-11-12T00:00:00Z");
        assert_eq!(event["raw"]["detail"]["reason"], "PutObject");
    }

    #[test]
    fn another_source_is_refused() {
        let mut other = created();
        other["source"] = json!("aws.ec2");
        assert!(unwrap(&other).is_err());
    }

    #[test]
    fn a_missing_key_is_named() {
        let mut broken = created();
        broken["detail"]["object"]
            .as_object_mut()
            .expect("an object")
            .remove("key");
        let error = unwrap(&broken).expect_err("must refuse");
        assert!(error.contains("/detail/object/key"), "{error}");
    }
}
