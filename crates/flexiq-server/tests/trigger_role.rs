//! End-to-end: the trigger listener over real HTTP, into real storage.
//!
//! Each test binds the router to an ephemeral port and posts to it with an
//! HTTP client, so the body limit, the header handling and the status codes
//! are the ones a webhook sender would see.

mod support;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use flexiq_core::wire::{encode_call, WireValue};
use flexiq_core::Storage;
use flexiq_server::config::trigger::TriggerConfig;
use flexiq_server::config::Env;
use flexiq_server::trigger::auth::google::GOOGLE_JWKS_URL;
use flexiq_server::trigger::auth::KeyFetcher;
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
    /// Every URL the listener fetched, in order.
    fetched: Arc<Mutex<Vec<String>>>,
}

const GOOGLE_JWKS: &[u8] = include_bytes!("fixtures/oidc_test_jwks.json");
const GOOGLE_SIGNING_KEY: &[u8] = include_bytes!("fixtures/oidc_test_key.pem");
const SNS_CERT: &[u8] = include_bytes!("fixtures/sns_test_cert.pem");
const SNS_SIGNING_KEY: &[u8] = include_bytes!("fixtures/sns_test_key.der");

/// Published keys served from fixtures, so no test reaches the network. Any
/// other URL answers `ok`, which is what confirming a subscription needs.
fn fixture_keys(fetched: Arc<Mutex<Vec<String>>>) -> KeyFetcher {
    KeyFetcher::new(Arc::new(move |url: String| {
        fetched.lock().expect("unpoisoned").push(url.clone());
        let body = if url == GOOGLE_JWKS_URL {
            GOOGLE_JWKS.to_vec()
        } else if url.ends_with(".pem") {
            SNS_CERT.to_vec()
        } else {
            b"ok".to_vec()
        };
        Box::pin(async move { Ok(body) })
    }))
}

async fn start(label: &str, triggers: Value) -> Harness {
    start_with_events(label, triggers, None).await
}

/// A harness whose enqueues are announced on `events`.
async fn start_with_events(
    label: &str,
    triggers: Value,
    events: Option<Arc<flexiq_core::EventHub>>,
) -> Harness {
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
    let fetched = Arc::new(Mutex::new(Vec::new()));
    let router = flexiq_server::trigger::router_with_keys(
        &config,
        (*storage).clone(),
        fixture_keys(fetched.clone()),
        events,
    );
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
        fetched,
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

/// A trigger announces the job it wrote, and a redelivery that answers with
/// that same job announces nothing.
#[cfg(feature = "events-http")]
#[tokio::test]
async fn a_triggered_job_is_announced_once() {
    let receiver = support::webhook_receiver::WebhookReceiver::start().await;
    let document = json!({"sinks": [{
        "kind": "http", "name": "loopback", "url": receiver.url,
        "allow": ["127.0.0.1"], "allow_loopback": true
    }]});
    let hub =
        Arc::new(flexiq_core::EventHub::from_json(&document.to_string()).expect("the hub starts"));
    let harness = start_with_events(
        "announced",
        json!([github_trigger("10/s")]),
        Some(Arc::clone(&hub)),
    )
    .await;

    let first = job_ids(harness.github("delivery-1", PUSH, &sign(PUSH)).await).await;
    let again = job_ids(harness.github("delivery-1", PUSH, &sign(PUSH)).await).await;
    assert!(again[0].1, "the redelivery deduplicated");
    // The shutdown drain flushes whatever is buffered, so after it the
    // receiver holds everything the hub was ever handed.
    tokio::task::spawn_blocking({
        let hub = Arc::clone(&hub);
        move || hub.shutdown(std::time::Duration::from_secs(5))
    })
    .await
    .expect("the drain");

    let received = receiver.received();
    assert_eq!(received.len(), 1, "{received:?}");
    let event = received[0].body.as_ref().expect("a JSON event");
    assert_eq!(event["type"], "org.byteveda.flexiq.job.enqueued");
    assert_eq!(event["subject"], first[0].0.as_str());
    assert_eq!(event["flexiqtask"], "ci.on_push");
    assert_eq!(event["flexiqnamespace"], NAMESPACE);
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
    let harness = start("azure-rate", json!([object_store_trigger("azure", "3/h")])).await;
    let first = json!([blob_created("e-1", "a"), blob_created("e-2", "b")]);
    assert_eq!(harness.post_json("/t/azure", &first).await.status(), 202);
    // One token left, and this batch needs two.
    let second = json!([blob_created("e-3", "c"), blob_created("e-4", "d")]);
    assert_eq!(harness.post_json("/t/azure", &second).await.status(), 429);
}

#[tokio::test]
async fn a_batch_the_full_bucket_cannot_hold_is_refused_without_draining_it() {
    let harness = start("azure-burst", json!([object_store_trigger("azure", "2/h")])).await;
    let oversized = json!([
        blob_created("e-1", "a"),
        blob_created("e-2", "b"),
        blob_created("e-3", "c")
    ]);
    // Refused on every attempt, and none of them spends a token: otherwise the
    // sender's retries of this batch would starve every other delivery.
    for _ in 0..3 {
        let refused = harness.post_json("/t/azure", &oversized).await;
        assert_eq!(refused.status(), 413);
    }
    let fits = json!([blob_created("e-4", "d"), blob_created("e-5", "e")]);
    assert_eq!(harness.post_json("/t/azure", &fits).await.status(), 202);
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

#[tokio::test]
async fn a_pubsub_push_proves_itself_with_a_google_token() {
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

    const AUDIENCE: &str = "https://hooks.example.com/t/gcs";
    const ACCOUNT: &str = "pusher@project.iam.gserviceaccount.com";

    let mut trigger = object_store_trigger("gcs", "10/s");
    trigger["auth"] =
        json!({"kind": "google_oidc", "audience": AUDIENCE, "service_account": ACCOUNT});
    let harness = start("gcs-oidc", json!([trigger])).await;

    let sign = |email: &str| {
        let now = chrono::Utc::now().timestamp();
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test-key".into());
        encode(
            &header,
            &json!({
                "iss": "https://accounts.google.com", "aud": AUDIENCE,
                "exp": now + 600, "iat": now,
                "email": email, "email_verified": true, "sub": "1"
            }),
            &EncodingKey::from_rsa_pem(GOOGLE_SIGNING_KEY).expect("a test key"),
        )
        .expect("signed")
    };
    let push = json!({
        "message": {
            "attributes": {
                "bucketId": "media", "objectId": "a.jpg",
                "eventType": "OBJECT_FINALIZE", "eventTime": "2026-09-23T10:00:00Z"
            },
            "messageId": "m-oidc"
        },
        "subscription": "projects/p/subscriptions/s"
    });
    let post = |token: String| {
        harness
            .client
            .post(format!("{}/t/gcs", harness.base))
            .bearer_auth(token)
            .json(&push)
            .send()
    };

    let accepted = post(sign(ACCOUNT)).await.expect("answers");
    assert_eq!(accepted.status(), 202);
    assert_eq!(
        harness.fetched.lock().expect("unpoisoned").as_slice(),
        [GOOGLE_JWKS_URL.to_string()]
    );

    let impostor = post(sign("someone@else.iam.gserviceaccount.com"))
        .await
        .expect("answers");
    assert_eq!(impostor.status(), 401);
    // The key set came from the cache the second time.
    assert_eq!(harness.fetched.lock().expect("unpoisoned").len(), 1);
}

const SNS_TOPIC: &str = "arn:aws:sns:us-east-1:123456789012:uploads";

/// `message`, signed the way SNS signs one (signature version 2).
fn sns_signed(mut message: Value) -> String {
    use base64::Engine;
    use rsa::pkcs1v15::SigningKey;
    use rsa::pkcs8::DecodePrivateKey;
    use rsa::signature::{SignatureEncoding, Signer};

    message["TopicArn"] = json!(SNS_TOPIC);
    message["Timestamp"] = json!(chrono::Utc::now().to_rfc3339());
    message["SignatureVersion"] = json!("2");
    message["SigningCertURL"] =
        json!("https://sns.us-east-1.amazonaws.com/SimpleNotificationService-test.pem");
    let fields: &[&str] = if message["Type"] == "Notification" {
        &[
            "Message",
            "MessageId",
            "Subject",
            "Timestamp",
            "TopicArn",
            "Type",
        ]
    } else {
        &[
            "Message",
            "MessageId",
            "SubscribeURL",
            "Timestamp",
            "Token",
            "TopicArn",
            "Type",
        ]
    };
    let canonical: String = fields
        .iter()
        .filter_map(|name| {
            message
                .get(*name)
                .and_then(Value::as_str)
                .map(|value| format!("{name}\n{value}\n"))
        })
        .collect();
    let key = rsa::RsaPrivateKey::from_pkcs8_der(SNS_SIGNING_KEY).expect("a test key");
    let signature = SigningKey::<Sha256>::new(key).sign(canonical.as_bytes());
    message["Signature"] =
        json!(base64::engine::general_purpose::STANDARD.encode(signature.to_vec()));
    message.to_string()
}

#[tokio::test]
async fn an_sns_subscription_confirms_then_delivers_signed_s3_events() {
    let harness = start(
        "s3-sns",
        json!([{
            "name": "s3-uploads",
            "path": "/t/s3",
            "task": "uploads.process",
            "kind": "object_store",
            "provider": "s3_sns",
            "rate_limit": "10/s",
            "auth": {"kind": "sns", "topic_arns": [SNS_TOPIC], "require_signature_v2": true},
            "args": [],
            "kwargs": {"key": {"from": "body", "pointer": "/key"}}
        }]),
    )
    .await;
    // SNS labels its JSON text/plain; the listener must read it anyway.
    let post = |body: String| {
        harness
            .client
            .post(format!("{}/t/s3", harness.base))
            .header("content-type", "text/plain; charset=UTF-8")
            .body(body)
            .send()
    };

    let subscribe_url =
        "https://sns.us-east-1.amazonaws.com/?Action=ConfirmSubscription&TopicArn=t&Token=abc";
    let confirmation = sns_signed(json!({
        "Type": "SubscriptionConfirmation",
        "MessageId": "c-1",
        "Token": "abc",
        "Message": "You have chosen to subscribe to the topic.",
        "SubscribeURL": subscribe_url
    }));
    let confirmed = post(confirmation).await.expect("answers");
    assert_eq!(confirmed.status(), 200);
    assert!(harness
        .fetched
        .lock()
        .expect("unpoisoned")
        .contains(&subscribe_url.to_string()));

    let records = json!({"Records": [
        {"eventName": "ObjectCreated:Put", "eventTime": "2027-01-15T08:00:00Z",
         "s3": {"bucket": {"name": "uploads"}, "object": {"key": "in/a+b.csv", "size": 3}}},
        {"eventName": "ObjectCreated:Put", "eventTime": "2027-01-15T08:00:00Z",
         "s3": {"bucket": {"name": "uploads"}, "object": {"key": "in/c.csv", "size": 4}}}
    ]});
    let notification = json!({
        "Type": "Notification",
        "MessageId": "n-1",
        "Subject": "Amazon S3 Notification",
        "Message": records.to_string()
    });
    let delivered = post(sns_signed(notification.clone()))
        .await
        .expect("answers");
    assert_eq!(delivered.status(), 202);
    let jobs = job_ids(delivered).await;
    assert_eq!(jobs.len(), 2);
    let job = harness
        .storage
        .get_job(&jobs[0].0, Some(NAMESPACE))
        .expect("storage answers")
        .expect("the job exists");
    assert_eq!(job.unique_key.as_deref(), Some("trigger:s3-uploads:n-1:0"));
    assert_eq!(
        job.payload,
        encode_call(
            &[],
            &[("key".to_string(), WireValue::Text("in/a b.csv".into()))]
        )
    );

    // Signed, then altered: the records are no longer the ones SNS signed.
    let mut forged: Value =
        serde_json::from_str(&sns_signed(notification)).expect("a JSON message");
    forged["Message"] = json!(json!({"Records": [
        {"eventName": "ObjectCreated:Put",
         "s3": {"bucket": {"name": "uploads"}, "object": {"key": "evil.csv"}}}
    ]})
    .to_string());
    let refused = post(forged.to_string()).await.expect("answers");
    assert_eq!(refused.status(), 401);
}
