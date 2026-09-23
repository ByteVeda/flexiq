//! Azure Blob Storage, delivered by an Event Grid webhook subscription.
//!
//! Event Grid posts an **array** of events in its own schema: `{"id",
//! "eventType", "subject", "eventTime", "data": {"url", "contentLength",
//! "eTag", …}}`. The container and blob name are in `subject`, as
//! `/blobServices/default/containers/<container>/blobs/<blob>`.
//!
//! Creating the subscription sends one `SubscriptionValidationEvent`, which
//! must be answered with its `validationCode` before any real event is sent.
//! That handshake arrives at the same URL, query secret included, so it is
//! verified like everything else and then answered without an enqueue.
//!
//! Only the Event Grid schema is read. The CloudEvents schema validates with
//! an `OPTIONS` request this door does not answer; a subscription must be
//! created with `--event-delivery-schema eventgridschema`.

use serde_json::{json, Value};

use super::{optional_text, size, text, Event, Provider, Unwrapped};

const VALIDATION_EVENT: &str = "Microsoft.EventGrid.SubscriptionValidationEvent";
const SUBJECT_PREFIX: &str = "/blobServices/default/containers/";

/// Unwrap an Event Grid delivery: a handshake, or its blob events.
pub fn unwrap(document: &Value) -> Result<Unwrapped, String> {
    let Some(events) = document.as_array() else {
        return Err(if document.get("specversion").is_some() {
            "a CloudEvents delivery is not accepted; subscribe with the Event Grid schema"
                .to_string()
        } else {
            "the body is not an Event Grid array".to_string()
        });
    };

    if events.is_empty() {
        return Err("the Event Grid delivery carries no events".to_string());
    }
    if let Some(validation) = events
        .iter()
        .find(|event| optional_text(event, "/eventType") == Some(VALIDATION_EVENT))
    {
        let code = text(validation, "/data/validationCode")?;
        return Ok(Unwrapped::Handshake(json!({ "validationResponse": code })));
    }

    events
        .iter()
        .map(blob_event)
        .collect::<Result<_, _>>()
        .map(Unwrapped::Events)
}

fn blob_event(event: &Value) -> Result<Value, String> {
    let subject = text(event, "/subject")?;
    let (container, blob) = subject
        .strip_prefix(SUBJECT_PREFIX)
        .and_then(|rest| rest.split_once("/blobs/"))
        .filter(|(container, blob)| !container.is_empty() && !blob.is_empty())
        .ok_or_else(|| format!("the subject {subject:?} does not name a blob"))?;

    Ok(Event {
        provider: Provider::Azure,
        id: text(event, "/id")?,
        event_type: text(event, "/eventType")?,
        bucket: container,
        key: blob,
        size: size(event.pointer("/data/contentLength")),
        etag: optional_text(event, "/data/eTag"),
        time: optional_text(event, "/eventTime"),
        raw: event,
    }
    .into_value())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Trimmed from the BlobCreated example in the Event Grid docs.
    fn created(id: &str) -> Value {
        json!({
            "topic": "/subscriptions/s/resourceGroups/g/providers/Microsoft.Storage/storageAccounts/acct",
            "subject": "/blobServices/default/containers/uploads/blobs/in/2026/a.json",
            "eventType": "Microsoft.Storage.BlobCreated",
            "eventTime": "2026-09-23T10:00:00.0000000Z",
            "id": id,
            "data": {
                "api": "PutBlob",
                "eTag": "0x8D4BCC2E4835CD0",
                "contentType": "application/json",
                "contentLength": 524288,
                "blobType": "BlockBlob",
                "url": "https://acct.blob.core.windows.net/uploads/in/2026/a.json"
            },
            "dataVersion": "",
            "metadataVersion": "1"
        })
    }

    #[test]
    fn a_batch_unwraps_to_one_event_each() {
        let unwrapped = unwrap(&json!([created("e1"), created("e2")])).expect("unwraps");
        let Unwrapped::Events(events) = unwrapped else {
            panic!("expected events");
        };
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["id"], "e1");
        assert_eq!(events[0]["provider"], "azure");
        assert_eq!(events[0]["bucket"], "uploads");
        assert_eq!(events[0]["key"], "in/2026/a.json");
        assert_eq!(events[0]["size"], 524288);
        assert_eq!(events[0]["etag"], "0x8D4BCC2E4835CD0");
        assert_eq!(events[1]["id"], "e2");
    }

    #[test]
    fn the_validation_handshake_is_answered_with_its_code() {
        let handshake = json!([{
            "id": "2d1781af-3a4c-4d7c-bd0c-e34b19da4e66",
            "topic": "/subscriptions/s",
            "subject": "",
            "data": {
                "validationCode": "512d38b6-c7b8-40c8-89fe-f46f9e9622b6",
                "validationUrl": "https://rp-eastus2.eventgrid.azure.net/…"
            },
            "eventType": VALIDATION_EVENT,
            "eventTime": "2026-09-23T10:00:00Z",
            "metadataVersion": "1",
            "dataVersion": "1"
        }]);
        assert_eq!(
            unwrap(&handshake),
            Ok(Unwrapped::Handshake(json!({
                "validationResponse": "512d38b6-c7b8-40c8-89fe-f46f9e9622b6"
            })))
        );
    }

    #[test]
    fn cloudevents_and_foreign_subjects_are_refused() {
        let error = unwrap(&json!({"specversion": "1.0"})).expect_err("must refuse");
        assert!(error.contains("CloudEvents"), "{error}");

        let mut queue_event = created("e1");
        queue_event["subject"] = json!("/queueServices/default/queues/q");
        assert!(unwrap(&json!([queue_event])).is_err());
    }
}
