//! What actually goes out on a webhook delivery.
//!
//! Signing, headers, and the recorded outcome are only observable from the
//! receiving end — the store tests cannot see any of it.

mod support;

use axum::http::StatusCode;
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::Sha256;

use flexiq_server::dashboard::state::SharedState;
use support::webhook_receiver::WebhookReceiver;
use support::{call, dashboard_state_allowing_loopback_webhooks, get, json_request, temp_storage};

/// Create a subscription pointing at `url`, returning `(id, secret)`.
async fn create_webhook(state: &SharedState, url: &str) -> (String, String) {
    let (status, _, body) = call(
        state,
        json_request(
            "POST",
            "/api/webhooks",
            json!({ "url": url, "generate_secret": true }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "webhook creation failed: {body}");
    (
        body["id"].as_str().expect("an id").to_string(),
        body["secret"].as_str().expect("a secret").to_string(),
    )
}

fn expected_signature(secret: &str, body: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("any key length");
    mac.update(body.as_bytes());
    let hex: String = mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("sha256={hex}")
}

#[tokio::test]
async fn a_test_send_is_signed_and_reported() {
    let storage = temp_storage("webhook-test-send");
    let state = dashboard_state_allowing_loopback_webhooks(&storage);
    let receiver = WebhookReceiver::start().await;
    let (id, secret) = create_webhook(&state, &receiver.url).await;

    let (status, _, body) = call(
        &state,
        json_request("POST", &format!("/api/webhooks/{id}/test"), json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], json!(200));
    assert_eq!(body["delivered"], json!(true));

    let received = receiver.received();
    assert_eq!(received.len(), 1, "exactly one delivery");
    let delivery = &received[0];

    assert_eq!(
        delivery.header("content-type"),
        Some("application/json"),
        "the body is JSON and must say so"
    );
    assert_eq!(
        delivery.header("x-flexiq-signature"),
        Some(expected_signature(&secret, &delivery.raw_body).as_str()),
        "the signature must cover the exact bytes sent"
    );

    let payload = delivery.body.as_ref().expect("a JSON body");
    assert_eq!(payload["event"], json!("test.ping"));
    assert_eq!(payload["subscription_id"], json!(id));
}

#[tokio::test]
async fn a_subscription_without_a_secret_is_sent_unsigned() {
    let storage = temp_storage("webhook-unsigned");
    let state = dashboard_state_allowing_loopback_webhooks(&storage);
    let receiver = WebhookReceiver::start().await;

    let (_, _, created) = call(
        &state,
        json_request("POST", "/api/webhooks", json!({ "url": receiver.url })),
    )
    .await;
    let id = created["id"].as_str().expect("an id");

    call(
        &state,
        json_request("POST", &format!("/api/webhooks/{id}/test"), json!({})),
    )
    .await;

    let received = receiver.received();
    assert_eq!(received.len(), 1);
    assert!(
        received[0].header("x-flexiq-signature").is_none(),
        "nothing to sign with means no signature header, not an empty one"
    );
}

#[tokio::test]
async fn custom_headers_ride_along() {
    let storage = temp_storage("webhook-headers");
    let state = dashboard_state_allowing_loopback_webhooks(&storage);
    let receiver = WebhookReceiver::start().await;

    let (_, _, created) = call(
        &state,
        json_request(
            "POST",
            "/api/webhooks",
            json!({
                "url": receiver.url,
                "headers": { "X-Tenant": "acme", "Authorization": "Bearer downstream" },
            }),
        ),
    )
    .await;
    let id = created["id"].as_str().expect("an id");

    call(
        &state,
        json_request("POST", &format!("/api/webhooks/{id}/test"), json!({})),
    )
    .await;

    let received = receiver.received();
    assert_eq!(received[0].header("x-tenant"), Some("acme"));
    assert_eq!(
        received[0].header("authorization"),
        Some("Bearer downstream")
    );
}

#[tokio::test]
async fn a_rejecting_endpoint_reports_the_failure() {
    let storage = temp_storage("webhook-rejected");
    let state = dashboard_state_allowing_loopback_webhooks(&storage);
    let receiver = WebhookReceiver::start().await;
    receiver.respond_with(503);
    let (id, _) = create_webhook(&state, &receiver.url).await;

    let (status, _, body) = call(
        &state,
        json_request("POST", &format!("/api/webhooks/{id}/test"), json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "the endpoint failed, not the API");
    assert_eq!(body["status"], json!(503));
    assert_eq!(body["delivered"], json!(false));
}

#[tokio::test]
async fn an_unreachable_endpoint_reports_no_status() {
    let storage = temp_storage("webhook-unreachable");
    let state = dashboard_state_allowing_loopback_webhooks(&storage);

    // Bind and drop, so the port is almost certainly closed.
    let dead_url = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        format!("http://{}/hook", listener.local_addr().expect("addr"))
    };
    let (id, _) = create_webhook(&state, &dead_url).await;

    let (status, _, body) = call(
        &state,
        json_request("POST", &format!("/api/webhooks/{id}/test"), json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], Value::Null);
    assert_eq!(body["delivered"], json!(false));
}

#[tokio::test]
async fn replaying_a_delivery_resends_it_and_records_a_new_attempt() {
    let storage = temp_storage("webhook-replay");
    let state = dashboard_state_allowing_loopback_webhooks(&storage);
    let receiver = WebhookReceiver::start().await;
    let (id, _) = create_webhook(&state, &receiver.url).await;

    // A test send is not logged, so seed the log through a replayable record:
    // send once, then replay whatever the log holds.
    call(
        &state,
        json_request("POST", &format!("/api/webhooks/{id}/test"), json!({})),
    )
    .await;

    // Nothing is logged yet — the test endpoint deliberately does not write to
    // the delivery log, matching the SDK dashboards.
    let (_, _, listed) = call(&state, get(&format!("/api/webhooks/{id}/deliveries"))).await;
    assert_eq!(listed["total"], json!(0));

    // Seed one record directly, as a worker's delivery would.
    let seeded = seed_delivery(&state, &id).await;

    let (status, _, body) = call(
        &state,
        json_request(
            "POST",
            &format!("/api/webhooks/{id}/deliveries/{seeded}/replay"),
            json!({}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["replayed_of"], json!(seeded));
    assert_eq!(body["delivered"], json!(true));

    // The endpoint saw the original payload plus a replay marker.
    let received = receiver.received();
    let replayed = received.last().expect("a replayed delivery");
    let payload = replayed.body.as_ref().expect("a JSON body");
    assert_eq!(payload["event"], json!("job.completed"));
    assert_eq!(payload["replay_of"], json!(seeded));

    // The replay is a new record on top of the original, not a mutation of it.
    let (_, _, listed) = call(&state, get(&format!("/api/webhooks/{id}/deliveries"))).await;
    assert_eq!(listed["total"], json!(2));
    let newest = &listed["items"][0];
    assert_eq!(newest["status"], json!("delivered"));
    assert_eq!(newest["attempts"], json!(1));
    assert_ne!(newest["id"], json!(seeded));
}

#[tokio::test]
async fn a_replay_of_an_unknown_delivery_is_a_404() {
    let storage = temp_storage("webhook-replay-missing");
    let state = dashboard_state_allowing_loopback_webhooks(&storage);
    let receiver = WebhookReceiver::start().await;
    let (id, _) = create_webhook(&state, &receiver.url).await;

    let (status, _, _) = call(
        &state,
        json_request(
            "POST",
            &format!("/api/webhooks/{id}/deliveries/nope/replay"),
            json!({}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(receiver.received().is_empty(), "nothing may be sent");
}

/// Write one delivery record straight into the store, as a worker would, and
/// return its id.
async fn seed_delivery(state: &SharedState, subscription_id: &str) -> String {
    use flexiq_server::dashboard::stores::deliveries::{
        record_attempt, DeliveryRecord, DeliveryStatus,
    };

    let record = DeliveryRecord {
        id: String::new(),
        subscription_id: subscription_id.to_string(),
        event: "job.completed".to_string(),
        payload: serde_json::from_value(json!({
            "event": "job.completed",
            "task_name": "send_email",
            "job_id": "job-1",
        }))
        .expect("an object"),
        task_name: Some("send_email".to_string()),
        job_id: Some("job-1".to_string()),
        status: DeliveryStatus::Delivered,
        attempts: 1,
        response_code: Some(200),
        response_body: None,
        latency_ms: Some(12),
        error: None,
        created_at: 0,
        completed_at: None,
    };
    record_attempt(&state.storage, record)
        .expect("seed a delivery record")
        .id
}
