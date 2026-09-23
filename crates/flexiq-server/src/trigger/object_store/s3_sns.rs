//! Amazon S3 event notifications, delivered through an SNS HTTPS subscription.
//!
//! The older of S3's two event paths, and the one a bucket without EventBridge
//! uses. SNS wraps the notification: its `Message` is a string holding S3's
//! own `{"Records": [...]}` document, each record one object. The SNS
//! signature — checked by the `sns` verifier before this runs — covers that
//! string, so every record in it is authentic.
//!
//! Three messages are not events. A `SubscriptionConfirmation` is confirmed by
//! fetching its `SubscribeURL`, which the handler does; an
//! `UnsubscribeConfirmation` is acknowledged; and the `s3:TestEvent` S3 sends
//! when a notification is configured is acknowledged without an enqueue.
//!
//! S3 writes object keys URL-encoded, `+` for a space, so they are decoded
//! into the key the object actually has. Raw message delivery is refused: it
//! drops the envelope the signature is over.

use serde_json::{json, Value};

use super::{optional_text, size, text, Event, Provider, Unwrapped};

/// Unwrap one SNS delivery.
pub fn unwrap(document: &Value) -> Result<Unwrapped, String> {
    match optional_text(document, "/Type") {
        Some("SubscriptionConfirmation") => Ok(Unwrapped::Confirm(
            text(document, "/SubscribeURL")?.to_string(),
        )),
        Some("UnsubscribeConfirmation") => Ok(Unwrapped::Handshake(
            json!({ "acknowledged": "UnsubscribeConfirmation" }),
        )),
        Some("Notification") => notification(document),
        _ => Err(
            "the body is not an SNS message; subscribe without raw message delivery".to_string(),
        ),
    }
}

fn notification(document: &Value) -> Result<Unwrapped, String> {
    let message_id = text(document, "/MessageId")?;
    let message: Value = serde_json::from_str(text(document, "/Message")?)
        .map_err(|_| "the SNS Message is not an S3 notification".to_string())?;

    if optional_text(&message, "/Event") == Some("s3:TestEvent") {
        return Ok(Unwrapped::Handshake(
            json!({ "acknowledged": "s3:TestEvent" }),
        ));
    }
    let records = message
        .get("Records")
        .and_then(Value::as_array)
        .filter(|records| !records.is_empty())
        .ok_or("the S3 notification carries no records")?;

    records
        .iter()
        .enumerate()
        .map(|(index, record)| {
            // One SNS message can carry several records, so the message id
            // alone would collapse them into one job.
            let id = format!("{message_id}:{index}");
            let key = decode_key(text(record, "/s3/object/key")?)?;
            Ok(Event {
                provider: Provider::S3Sns,
                id: &id,
                event_type: text(record, "/eventName")?,
                bucket: text(record, "/s3/bucket/name")?,
                key: &key,
                size: size(record.pointer("/s3/object/size")),
                etag: optional_text(record, "/s3/object/eTag"),
                time: optional_text(record, "/eventTime"),
                raw: record,
            }
            .into_value())
        })
        .collect::<Result<_, String>>()
        .map(Unwrapped::Events)
}

/// An S3 notification key, decoded: `+` is a space, `%XX` a byte.
fn decode_key(raw: &str) -> Result<String, String> {
    // `+` first: a literal plus arrives as `%2B`, so decoding after this
    // restores it rather than turning it into a space.
    percent_decode(&raw.replace('+', " "))
        .ok_or_else(|| format!("the object key {raw:?} is not validly encoded"))
}

fn percent_decode(text: &str) -> Option<String> {
    let mut out = Vec::with_capacity(text.len());
    let mut bytes = text.bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'%' {
            let high = char::from(bytes.next()?).to_digit(16)?;
            let low = char::from(bytes.next()?).to_digit(16)?;
            out.push(u8::try_from(high * 16 + low).ok()?);
        } else {
            out.push(byte);
        }
    }
    String::from_utf8(out).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Trimmed from the S3 notification message structure in the S3 docs.
    fn notification(records: Value) -> Value {
        json!({
            "Type": "Notification",
            "MessageId": "msg-1",
            "TopicArn": "arn:aws:sns:us-east-1:123456789012:uploads",
            "Message": json!({ "Records": records }).to_string(),
            "Timestamp": "2027-01-15T08:00:00.000Z"
        })
    }

    fn record(key: &str) -> Value {
        json!({
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "eventTime": "2027-01-15T08:00:00.000Z",
            "eventName": "ObjectCreated:Put",
            "s3": {
                "bucket": {"name": "uploads"},
                "object": {"key": key, "size": 1024, "eTag": "d41d8cd9", "sequencer": "0A1B"}
            }
        })
    }

    #[test]
    fn each_record_is_an_event_with_a_decoded_key() {
        let unwrapped = unwrap(&notification(json!([
            record("photos/my+cat%21.jpg"),
            record("a%2Bb.txt")
        ])))
        .expect("unwraps");
        let Unwrapped::Events(events) = unwrapped else {
            panic!("expected events");
        };
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["id"], "msg-1:0");
        assert_eq!(events[0]["provider"], "s3_sns");
        assert_eq!(events[0]["event_type"], "ObjectCreated:Put");
        assert_eq!(events[0]["bucket"], "uploads");
        assert_eq!(events[0]["key"], "photos/my cat!.jpg");
        assert_eq!(events[0]["size"], 1024);
        assert_eq!(events[1]["id"], "msg-1:1");
        assert_eq!(events[1]["key"], "a+b.txt");
    }

    #[test]
    fn a_subscription_is_confirmed_through_its_url() {
        let confirm = json!({
            "Type": "SubscriptionConfirmation",
            "SubscribeURL": "https://sns.us-east-1.amazonaws.com/?Action=ConfirmSubscription&Token=t"
        });
        assert_eq!(
            unwrap(&confirm),
            Ok(Unwrapped::Confirm(
                "https://sns.us-east-1.amazonaws.com/?Action=ConfirmSubscription&Token=t".into()
            ))
        );
    }

    #[test]
    fn the_s3_test_event_is_acknowledged_not_enqueued() {
        let mut test = notification(json!([]));
        test["Message"] = json!(json!({
            "Service": "Amazon S3", "Event": "s3:TestEvent", "Bucket": "uploads"
        })
        .to_string());
        assert!(matches!(unwrap(&test), Ok(Unwrapped::Handshake(_))));
    }

    #[test]
    fn raw_delivery_and_bad_keys_are_refused() {
        assert!(unwrap(&json!({"Records": []})).is_err());
        assert!(unwrap(&notification(json!([record("bad%zzkey")]))).is_err());
        assert!(unwrap(&notification(json!([]))).is_err());
    }
}
