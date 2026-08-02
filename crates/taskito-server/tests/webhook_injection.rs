//! The admission webhook, driven through the real router.
//!
//! TLS is the transport, not the logic, so these go through the same handler
//! the HTTPS server serves without a certificate in the way. What they cover is
//! the contract the API server sees: the uid echoes, the patch is base64 of
//! RFC 6902 operations, and a malformed annotation denies rather than admitting
//! a pod that would never run a job.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use base64::Engine;
use serde_json::{json, Value};
use tower::ServiceExt;

use taskito_server::config::webhook::WebhookConfig;
use taskito_server::webhook::{router, MUTATE_PATH};

/// A config whose TLS paths are never read: the router does not open them, only
/// `serve` does.
fn config() -> Arc<WebhookConfig> {
    Arc::new(WebhookConfig {
        bind: "127.0.0.1:9443".parse().expect("valid address"),
        cert: "/dev/null".into(),
        key: "/dev/null".into(),
    })
}

/// POST an `AdmissionReview` and read the response envelope back.
async fn review(body: Value) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("POST")
        .uri(MUTATE_PATH)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("valid request");

    let response = router(config())
        .oneshot(request)
        .await
        .expect("the router answers");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .expect("a body");
    let parsed = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, parsed)
}

fn admission_review(pod: Value) -> Value {
    json!({
        "apiVersion": "admission.k8s.io/v1",
        "kind": "AdmissionReview",
        "request": { "uid": "uid-1", "namespace": "prod", "object": pod },
    })
}

fn pod(annotations: Value) -> Value {
    json!({
        "metadata": { "name": "app-1", "annotations": annotations },
        "spec": { "containers": [{ "name": "app", "image": "myapp:1.4.2" }] },
    })
}

/// Decode the patch the response carried.
fn patch(body: &Value) -> Vec<Value> {
    let encoded = body["response"]["patch"]
        .as_str()
        .expect("a patch was returned");
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .expect("valid base64");
    serde_json::from_slice(&decoded).expect("valid JSON patch")
}

#[tokio::test]
async fn an_annotated_pod_is_admitted_with_a_sidecar_patch() {
    let (status, body) = review(admission_review(pod(json!({
        "taskito.dev/inject": "true",
        "taskito.dev/attach": "taskito-scheduler:7777",
        "taskito.dev/command": "taskito executor --app myapp:queue",
        "taskito.dev/slots": "4",
    }))))
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["response"]["uid"], "uid-1");
    assert_eq!(body["response"]["allowed"], true);
    assert_eq!(body["response"]["patchType"], "JSONPatch");

    let ops = patch(&body);
    let container = ops
        .iter()
        .find(|op| op["path"] == "/spec/containers/-")
        .expect("a container is added");
    assert_eq!(container["value"]["name"], "taskito-executor");
    // The whole point: the sidecar rides the app's own image, so nothing new is
    // pulled onto the node.
    assert_eq!(container["value"]["image"], "myapp:1.4.2");
    assert!(container["value"]["env"]
        .as_array()
        .expect("env")
        .contains(&json!({ "name": "TASKITO_SLOTS", "value": "4" })));
}

#[tokio::test]
async fn a_pod_that_did_not_opt_in_is_admitted_untouched() {
    let (status, body) = review(admission_review(pod(json!({ "team": "payments" })))).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["response"]["allowed"], true);
    assert!(body["response"]["patch"].is_null());
}

#[tokio::test]
async fn a_pod_missing_a_required_annotation_is_denied_with_the_reason() {
    let (status, body) = review(admission_review(pod(json!({
        "taskito.dev/inject": "true",
        "taskito.dev/attach": "taskito-scheduler:7777",
    }))))
    .await;

    // The HTTP call succeeded; the verdict inside it is the denial.
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["response"]["allowed"], false);
    let message = body["response"]["status"]["message"]
        .as_str()
        .expect("a message");
    assert!(
        message.contains("taskito.dev/command"),
        "the operator has to learn which annotation was wrong, got: {message}"
    );
}

#[tokio::test]
async fn re_admitting_an_injected_pod_adds_nothing() {
    let already = json!({
        "metadata": { "name": "app-1", "annotations": {
            "taskito.dev/inject": "true",
            "taskito.dev/attach": "taskito-scheduler:7777",
            "taskito.dev/command": "taskito executor --app myapp:queue",
        }},
        "spec": { "containers": [
            { "name": "app", "image": "myapp:1.4.2" },
            { "name": "taskito-executor", "image": "myapp:1.4.2" },
        ]},
    });

    let (status, body) = review(admission_review(already)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["response"]["allowed"], true);
    assert!(
        body["response"]["patch"].is_null(),
        "a second sidecar would double the pod's slots"
    );
}

#[tokio::test]
async fn a_body_that_is_not_json_is_rejected() {
    // Every field on the envelope is optional, so only malformed JSON reaches
    // the parse-error branch — valid JSON of the wrong shape does not.
    let request = Request::builder()
        .method("POST")
        .uri(MUTATE_PATH)
        .header("content-type", "application/json")
        .body(Body::from("{ this is not json"))
        .expect("valid request");
    let response = router(config())
        .oneshot(request)
        .await
        .expect("the router answers");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_review_with_no_request_is_rejected() {
    let (status, _) = review(json!({ "nonsense": true })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn the_webhook_serves_its_own_liveness_probe() {
    let request = Request::builder()
        .uri("/health")
        .body(Body::empty())
        .expect("valid request");
    let response = router(config())
        .oneshot(request)
        .await
        .expect("the router answers");
    assert_eq!(response.status(), StatusCode::OK);
}
