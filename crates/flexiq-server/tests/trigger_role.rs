//! End-to-end: the trigger listener over real HTTP, into real storage.
//!
//! Each test binds the router to an ephemeral port and posts to it with an
//! HTTP client, so the body limit, the header handling and the status codes
//! are the ones a webhook sender would see.

mod support;

use std::net::SocketAddr;
use std::sync::Arc;

use flexiq_core::wire::{encode_call, WireValue};
use flexiq_core::Storage;
use flexiq_server::config::trigger::TriggerConfig;
use flexiq_server::config::Env;
use flexiq_server::trigger::definition;
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::Sha256;

use support::{temp_storage, TempStorage};

const NAMESPACE: &str = "trigger-tests";
const GITHUB_SECRET: &str = "github-webhook-secret";
const SHARED_SECRET: &str = "a-shared-secret-token";

/// A listener serving `triggers`, and the storage its jobs land in.
struct Harness {
    base: String,
    storage: TempStorage,
    client: reqwest::Client,
}

async fn start(label: &str, triggers: Value) -> Harness {
    let env = Env::from([
        ("GITHUB_SECRET".to_string(), GITHUB_SECRET.to_string()),
        ("SHARED_SECRET".to_string(), SHARED_SECRET.to_string()),
    ]);
    let parsed = definition::parse(&json!({ "triggers": triggers }).to_string(), &env)
        .expect("valid definitions");
    let config = TriggerConfig {
        bind: "127.0.0.1:0".parse().expect("an address"),
        namespace: NAMESPACE.to_string(),
        file: "inline".into(),
        triggers: Arc::from(parsed),
    };

    let storage = temp_storage(label);
    let router = flexiq_server::trigger::router(&config, (*storage).clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("an address");
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });

    Harness {
        base: format!("http://{addr}"),
        storage,
        client: reqwest::Client::new(),
    }
}

fn github_trigger(rate_limit: &str) -> Value {
    json!({
        "name": "github-push",
        "path": "/t/github",
        "task": "ci.on_push",
        "queue": "webhooks",
        "rate_limit": rate_limit,
        "auth": {"kind": "github", "secret_env": "GITHUB_SECRET"},
        "args": [{"from": "body", "pointer": "/repository/full_name"}],
        "kwargs": {
            "event": {"from": "header", "name": "X-GitHub-Event"},
            "source": {"from": "const", "value": "github"}
        },
        "unique_key": {"from": "header", "name": "X-GitHub-Delivery"}
    })
}

fn sign(body: &[u8]) -> String {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(GITHUB_SECRET.as_bytes()).expect("a key");
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

impl Harness {
    async fn github(&self, delivery: &str, body: &[u8], signature: &str) -> reqwest::Response {
        self.client
            .post(format!("{}/t/github", self.base))
            .header("content-type", "application/json")
            .header("x-github-event", "push")
            .header("x-github-delivery", delivery)
            .header("x-hub-signature-256", signature)
            .body(body.to_vec())
            .send()
            .await
            .expect("the listener answers")
    }
}

const PUSH: &[u8] = br#"{"repository": {"full_name": "acme/widgets"}}"#;

async fn job_ids(response: reqwest::Response) -> Vec<(String, bool)> {
    let body: Value = response.json().await.expect("a JSON body");
    body["jobs"]
        .as_array()
        .expect("a jobs array")
        .iter()
        .map(|job| {
            (
                job["id"].as_str().expect("an id").to_string(),
                job["deduplicated"].as_bool().expect("a flag"),
            )
        })
        .collect()
}

#[tokio::test]
async fn a_signed_delivery_becomes_the_job_its_definition_describes() {
    let harness = start("signed", json!([github_trigger("10/s")])).await;

    let response = harness.github("delivery-1", PUSH, &sign(PUSH)).await;
    assert_eq!(response.status(), 202);
    let jobs = job_ids(response).await;
    assert_eq!(jobs.len(), 1);
    assert!(!jobs[0].1);

    let job = harness
        .storage
        .get_job(&jobs[0].0, Some(NAMESPACE))
        .expect("storage answers")
        .expect("the job exists in the trigger's namespace");
    assert_eq!(job.task_name, "ci.on_push");
    assert_eq!(job.queue, "webhooks");
    assert_eq!(job.namespace.as_deref(), Some(NAMESPACE));
    assert_eq!(
        job.unique_key.as_deref(),
        Some("trigger:github-push:delivery-1")
    );
    assert!(job
        .metadata
        .as_deref()
        .is_some_and(|metadata| metadata.contains("github-push")));

    // Byte-identical to what a producer would have sent for the same call.
    let expected = encode_call(
        &[WireValue::Text("acme/widgets".into())],
        &[
            ("event".to_string(), WireValue::Text("push".into())),
            ("source".to_string(), WireValue::Text("github".into())),
        ],
    );
    assert_eq!(job.payload, expected);
}

#[tokio::test]
async fn a_redelivery_answers_with_the_job_it_already_made() {
    let harness = start("redelivery", json!([github_trigger("10/s")])).await;

    let first = job_ids(harness.github("delivery-1", PUSH, &sign(PUSH)).await).await;
    let again = harness.github("delivery-1", PUSH, &sign(PUSH)).await;
    assert_eq!(again.status(), 200);
    let again = job_ids(again).await;
    assert_eq!(again, vec![(first[0].0.clone(), true)]);
}

#[tokio::test]
async fn forgeries_are_refused_without_spending_the_senders_budget() {
    // One token an hour: if any forgery drew from the bucket, the genuine
    // delivery after them would be the one refused.
    let harness = start("forgery", json!([github_trigger("1/h")])).await;

    for forged in ["sha256=00", "", "sha1=abc"] {
        let response = harness.github("forged", PUSH, forged).await;
        assert_eq!(response.status(), 401, "{forged:?}");
        let body: Value = response.json().await.expect("a JSON body");
        assert_eq!(body, json!({"error": "unauthorized"}));
    }
    let tampered = harness
        .github(
            "forged",
            br#"{"repository": {"full_name": "evil/x"}}"#,
            &sign(PUSH),
        )
        .await;
    assert_eq!(tampered.status(), 401);

    let genuine = harness.github("delivery-1", PUSH, &sign(PUSH)).await;
    assert_eq!(genuine.status(), 202);

    let limited = harness.github("delivery-2", PUSH, &sign(PUSH)).await;
    assert_eq!(limited.status(), 429);
    assert_eq!(
        limited
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok()),
        Some("3600")
    );

    // Other tests in this binary share the trigger name, so only presence is
    // asserted, not a count.
    let metrics = flexiq_server::trigger::metrics::render();
    for outcome in ["unauthorized", "enqueued", "rate_limited"] {
        let series = format!("trigger=\"github-push\",outcome=\"{outcome}\"");
        assert!(metrics.contains(&series), "{series} missing:\n{metrics}");
    }
}

#[tokio::test]
async fn paths_methods_and_health() {
    let harness = start("routing", json!([github_trigger("10/s")])).await;

    let unknown = harness
        .client
        .post(format!("{}/t/nope", harness.base))
        .send()
        .await
        .expect("answers");
    assert_eq!(unknown.status(), 404);

    let wrong_method = harness
        .client
        .get(format!("{}/t/github", harness.base))
        .send()
        .await
        .expect("answers");
    assert_eq!(wrong_method.status(), 405);
    assert_eq!(
        wrong_method
            .headers()
            .get("allow")
            .and_then(|v| v.to_str().ok()),
        Some("POST")
    );

    let health = harness
        .client
        .get(format!("{}/healthz", harness.base))
        .send()
        .await
        .expect("answers");
    assert_eq!(health.status(), 200);
}

#[tokio::test]
async fn an_oversized_body_is_refused_before_it_is_verified() {
    let mut trigger = github_trigger("10/s");
    trigger["max_body_bytes"] = json!(64);
    let harness = start("oversized", json!([trigger])).await;

    let big = vec![b' '; 65];
    let response = harness.github("delivery-1", &big, &sign(&big)).await;
    assert_eq!(response.status(), 413);
}

#[tokio::test]
async fn a_body_the_mapping_cannot_resolve_is_unprocessable() {
    let harness = start("unmappable", json!([github_trigger("10/s")])).await;
    let body = br#"{"no_repository": true}"#;
    let response = harness.github("delivery-1", body, &sign(body)).await;
    assert_eq!(response.status(), 422);
    let message: Value = response.json().await.expect("a JSON body");
    assert!(
        message["error"]
            .as_str()
            .is_some_and(|text| text.contains("/repository/full_name")),
        "{message}"
    );
}

#[tokio::test]
async fn a_form_post_with_a_query_secret() {
    let harness = start(
        "form",
        json!([{
            "name": "sms",
            "path": "/t/sms",
            "task": "sms.receive",
            "rate_limit": "10/s",
            "auth": {"kind": "shared_secret", "secret_env": "SHARED_SECRET", "query": "token"},
            "args": [],
            "kwargs": {
                "sender": {"from": "body", "pointer": "/From"},
                "text": {"from": "body", "pointer": "/Body"}
            }
        }]),
    )
    .await;

    let url = format!("{}/t/sms?token={SHARED_SECRET}", harness.base);
    let response = harness
        .client
        .post(&url)
        .header("content-type", "application/x-www-form-urlencoded")
        .body("From=%2B15550100&Body=hello+there")
        .send()
        .await
        .expect("answers");
    assert_eq!(response.status(), 202);
    let jobs = job_ids(response).await;
    let job = harness
        .storage
        .get_job(&jobs[0].0, Some(NAMESPACE))
        .expect("storage answers")
        .expect("the job exists");
    assert_eq!(
        job.payload,
        encode_call(
            &[],
            &[
                ("sender".to_string(), WireValue::Text("+15550100".into())),
                ("text".to_string(), WireValue::Text("hello there".into())),
            ],
        )
    );

    let unsigned = harness
        .client
        .post(format!("{}/t/sms", harness.base))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("From=x&Body=y")
        .send()
        .await
        .expect("answers");
    assert_eq!(unsigned.status(), 401);
}

fn object_store_trigger(provider: &str, rate_limit: &str) -> Value {
    json!({
        "name": format!("{provider}-uploads"),
        "path": format!("/t/{provider}"),
        "task": "uploads.process",
        "kind": "object_store",
        "provider": provider,
        "rate_limit": rate_limit,
        "auth": {"kind": "shared_secret", "secret_env": "SHARED_SECRET", "query": "key"},
        "args": [],
        "kwargs": {
            "bucket": {"from": "body", "pointer": "/bucket"},
            "key": {"from": "body", "pointer": "/key"}
        }
    })
}

fn blob_created(id: &str, blob: &str) -> Value {
    json!({
        "id": id,
        "eventType": "Microsoft.Storage.BlobCreated",
        "subject": format!("/blobServices/default/containers/uploads/blobs/{blob}"),
        "eventTime": "2026-09-23T10:00:00Z",
        "data": {"contentLength": 10, "eTag": "0x1"}
    })
}

impl Harness {
    async fn post_json(&self, path: &str, body: &Value) -> reqwest::Response {
        self.client
            .post(format!("{}{path}?key={SHARED_SECRET}", self.base))
            .json(body)
            .send()
            .await
            .expect("answers")
    }
}

#[tokio::test]
async fn an_event_grid_subscription_validates_then_delivers_a_batch() {
    let harness = start("azure", json!([object_store_trigger("azure", "10/s")])).await;

    let handshake = json!([{
        "id": "v-1",
        "eventType": "Microsoft.EventGrid.SubscriptionValidationEvent",
        "subject": "",
        "data": {"validationCode": "code-123"}
    }]);
    let answered = harness.post_json("/t/azure", &handshake).await;
    assert_eq!(answered.status(), 200);
    let body: Value = answered.json().await.expect("a JSON body");
    assert_eq!(body, json!({"validationResponse": "code-123"}));

    let batch = json!([blob_created("e-1", "a.csv"), blob_created("e-2", "b.csv")]);
    let delivered = harness.post_json("/t/azure", &batch).await;
    assert_eq!(delivered.status(), 202);
    let jobs = job_ids(delivered).await;
    assert_eq!(jobs.len(), 2);

    let job = harness
        .storage
        .get_job(&jobs[1].0, Some(NAMESPACE))
        .expect("storage answers")
        .expect("the job exists");
    assert_eq!(job.task_name, "uploads.process");
    assert_eq!(job.unique_key.as_deref(), Some("trigger:azure-uploads:e-2"));
    assert_eq!(
        job.payload,
        encode_call(
            &[],
            &[
                ("bucket".to_string(), WireValue::Text("uploads".into())),
                ("key".to_string(), WireValue::Text("b.csv".into())),
            ],
        )
    );

    // Event Grid redelivers a batch it did not see acknowledged; the event
    // ids make the second delivery a no-op rather than two more jobs.
    let redelivered = harness.post_json("/t/azure", &batch).await;
    assert_eq!(redelivered.status(), 200);
    let again = job_ids(redelivered).await;
    assert!(again.iter().all(|(_, deduplicated)| *deduplicated));
}

#[tokio::test]
async fn an_event_batch_costs_one_token_per_event() {
    let harness = start("azure-rate", json!([object_store_trigger("azure", "1/h")])).await;
    let batch = json!([blob_created("e-1", "a"), blob_created("e-2", "b")]);
    let response = harness.post_json("/t/azure", &batch).await;
    assert_eq!(response.status(), 429);
}

#[tokio::test]
async fn a_pubsub_push_becomes_a_job_keyed_on_its_message() {
    use base64::Engine;

    let harness = start("gcs", json!([object_store_trigger("gcs", "10/s")])).await;
    let resource = json!({"size": "7", "etag": "CAE="}).to_string();
    let push = json!({
        "message": {
            "attributes": {
                "bucketId": "media",
                "objectId": "photos/cat.jpg",
                "eventType": "OBJECT_FINALIZE",
                "eventTime": "2026-09-23T10:00:00Z"
            },
            "data": base64::engine::general_purpose::STANDARD.encode(resource),
            "messageId": "m-42"
        },
        "subscription": "projects/p/subscriptions/s"
    });

    let response = harness.post_json("/t/gcs", &push).await;
    assert_eq!(response.status(), 202);
    let jobs = job_ids(response).await;
    let job = harness
        .storage
        .get_job(&jobs[0].0, Some(NAMESPACE))
        .expect("storage answers")
        .expect("the job exists");
    assert_eq!(job.unique_key.as_deref(), Some("trigger:gcs-uploads:m-42"));

    let unauthenticated = harness
        .client
        .post(format!("{}/t/gcs", harness.base))
        .json(&push)
        .send()
        .await
        .expect("answers");
    assert_eq!(unauthenticated.status(), 401);
}
