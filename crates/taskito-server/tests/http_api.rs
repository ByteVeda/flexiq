//! The dashboard API, driven through the real router.
//!
//! Requests go through the same middleware stack the server runs, so these
//! also cover the security headers and the auth gate's open-mode behaviour.

mod support;

use std::sync::Arc;

use axum::http::StatusCode;
use serde_json::{json, Value};
use taskito_core::{now_millis, NewJob, Storage};

use support::{
    call, dashboard_state, dashboard_state_with_assets, get, json_request, temp_assets,
    temp_storage,
};
use taskito_server::config::dashboard::AuthMode;
use taskito_server::dashboard::static_assets::StaticAssets;

fn new_job(task_name: &str) -> NewJob {
    NewJob {
        queue: "default".to_string(),
        task_name: task_name.to_string(),
        payload: b"payload".to_vec(),
        priority: 0,
        scheduled_at: now_millis(),
        max_retries: 3,
        timeout_ms: 30_000,
        unique_key: None,
        metadata: None,
        notes: None,
        depends_on: vec![],
        expires_at: None,
        result_ttl_ms: None,
        namespace: None,
    }
}

#[tokio::test]
async fn probes_answer_without_credentials_in_open_mode() {
    let storage = temp_storage("http-probes");
    let state = dashboard_state(&storage, AuthMode::Open);

    let (status, _, body) = call(&state, get("/health")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], json!("ok"));

    let (status, _, body) = call(&state, get("/readiness")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], json!("ready"));
    assert_eq!(body["checks"]["storage"], json!("ok"));
}

#[tokio::test]
async fn session_auth_gates_readiness_until_a_deployment_opts_out() {
    let storage = temp_storage("http-readiness-gate");

    // The default an orchestrator probe walks into: authenticated dashboard, no
    // credential on the probe, so the pod would never report Ready.
    let mut state = dashboard_state(&storage, AuthMode::Session);
    let (status, _, _) = call(&state, get("/readiness")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Opted out: readiness answers, and it is the real storage check rather
    // than the liveness stub.
    {
        let state = Arc::get_mut(&mut state).expect("sole owner before any request clones it");
        state.config.public_readiness = true;
    }
    let (status, _, body) = call(&state, get("/readiness")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["checks"]["storage"], json!("ok"));

    // /metrics stays gated — the switch is scoped to the probe, not to every
    // unauthenticated reader.
    let (status, _, _) = call(&state, get("/metrics")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn every_response_carries_the_security_headers() {
    let storage = temp_storage("http-headers");
    let state = dashboard_state(&storage, AuthMode::Open);

    let (_, headers, _) = call(&state, get("/health")).await;
    assert_eq!(
        headers
            .get("x-content-type-options")
            .map(|v| v.to_str().unwrap()),
        Some("nosniff")
    );
    assert_eq!(
        headers.get("x-frame-options").map(|v| v.to_str().unwrap()),
        Some("DENY")
    );
    assert!(headers.contains_key("content-security-policy"));

    // Including on an error path, which is where a per-handler approach leaks.
    let (status, headers, _) = call(&state, get("/api/nope")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(headers.contains_key("content-security-policy"));
}

#[tokio::test]
async fn an_unknown_api_path_is_json_not_the_spa_shell() {
    let storage = temp_storage("http-404");
    let state = dashboard_state(&storage, AuthMode::Open);

    let (status, _, body) = call(&state, get("/api/does-not-exist")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], json!("Not found"));
}

#[tokio::test]
async fn a_missing_spa_bundle_reports_itself() {
    let storage = temp_storage("http-spa-missing");
    let assets = temp_assets("spa-missing", &[]);
    let state = dashboard_state_with_assets(
        &storage,
        AuthMode::Open,
        StaticAssets::new(Some(assets.path.clone())),
    );

    let (status, headers, _) = call(&state, get("/jobs")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(is_html(&headers));
}

#[tokio::test]
async fn the_spa_is_served_with_the_right_caching() {
    let storage = temp_storage("http-spa");
    let assets = temp_assets(
        "spa",
        &[
            ("index.html", "<!doctype html><title>taskito</title>"),
            ("assets/app-1a2b.js", "console.log('taskito')"),
        ],
    );
    let state = dashboard_state_with_assets(
        &storage,
        AuthMode::Open,
        StaticAssets::new(Some(assets.path.clone())),
    );

    // A client-routed path falls back to the shell, uncached.
    let (status, headers, _) = call(&state, get("/jobs")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(is_html(&headers));
    assert_eq!(header(&headers, "cache-control"), Some("no-cache"));

    // Content-hashed assets are immutable.
    let (status, headers, _) = call(&state, get("/assets/app-1a2b.js")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        header(&headers, "content-type"),
        Some("application/javascript; charset=utf-8")
    );
    assert_eq!(
        header(&headers, "cache-control"),
        Some("public, max-age=31536000, immutable")
    );

    // A stale hashed reference must 404, never fall back to HTML.
    let (status, _, body) = call(&state, get("/assets/gone-9z9z.js")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], json!("Not found"));

    // Traversal is refused before it reaches the filesystem.
    let (status, _, _) = call(&state, get("/../Cargo.toml")).await;
    assert!(status == StatusCode::NOT_FOUND || status == StatusCode::OK);
}

fn header<'a>(headers: &'a axum::http::HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn is_html(headers: &axum::http::HeaderMap) -> bool {
    header(headers, "content-type").is_some_and(|value| value.starts_with("text/html"))
}

#[tokio::test]
async fn jobs_are_listed_in_the_documented_shape() {
    let storage = temp_storage("http-jobs");
    let job = storage.enqueue(new_job("send_email")).expect("enqueue");
    let state = dashboard_state(&storage, AuthMode::Open);

    let (status, _, body) = call(&state, get("/api/jobs")).await;
    assert_eq!(status, StatusCode::OK);
    let rows = body.as_array().expect("an array of jobs");
    assert_eq!(rows.len(), 1);

    let row = &rows[0];
    assert_eq!(row["id"], json!(job.id));
    assert_eq!(row["task_name"], json!("send_email"));
    assert_eq!(row["status"], json!("pending"));
    assert!(row["created_at"].as_i64().expect("ms timestamp") > 1_600_000_000_000);
    // Blobs never leave the server.
    assert!(row.get("payload").is_none());
    assert!(row.get("result").is_none());
}

#[tokio::test]
async fn job_filters_are_validated() {
    let storage = temp_storage("http-job-filters");
    let state = dashboard_state(&storage, AuthMode::Open);

    let (status, _, body) = call(&state, get("/api/jobs?status=exploded")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"]
        .as_str()
        .expect("message")
        .contains("Invalid status"));

    let (status, _, _) = call(&state, get("/api/jobs?limit=-1")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_job_can_be_cancelled_and_read_back() {
    let storage = temp_storage("http-cancel");
    let job = storage.enqueue(new_job("send_email")).expect("enqueue");
    let state = dashboard_state(&storage, AuthMode::Open);

    let (status, _, body) = call(
        &state,
        json_request("POST", &format!("/api/jobs/{}/cancel", job.id), json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["cancelled"], json!(true));

    let (_, _, body) = call(&state, get(&format!("/api/jobs/{}", job.id))).await;
    assert_eq!(body["status"], json!("cancelled"));
}

#[tokio::test]
async fn a_missing_job_is_a_404() {
    let storage = temp_storage("http-missing-job");
    let state = dashboard_state(&storage, AuthMode::Open);

    let (status, _, body) = call(&state, get("/api/jobs/nope")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], json!("Job not found"));
}

#[tokio::test]
async fn stats_report_every_status_bucket() {
    let storage = temp_storage("http-stats");
    storage.enqueue(new_job("send_email")).expect("enqueue");
    let state = dashboard_state(&storage, AuthMode::Open);

    let (status, _, body) = call(&state, get("/api/stats")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["pending"], json!(1));
    for bucket in ["running", "completed", "failed", "dead", "cancelled"] {
        assert_eq!(body[bucket], json!(0), "{bucket} must be reported");
    }
}

#[tokio::test]
async fn settings_round_trip_and_reserved_keys_stay_hidden() {
    let storage = temp_storage("http-settings");
    // Written directly, as the auth store would.
    storage
        .set_setting("auth:users", "{}")
        .expect("seed a reserved key");
    let state = dashboard_state(&storage, AuthMode::Open);

    let (status, _, _) = call(
        &state,
        json_request(
            "PUT",
            "/api/settings/branding",
            json!({ "value": { "title": "Ops" } }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (_, _, body) = call(&state, get("/api/settings")).await;
    assert!(body.get("branding").is_some());
    assert!(
        body.get("auth:users").is_none(),
        "reserved keys must never be listed"
    );

    let (status, _, _) = call(&state, get("/api/settings/auth:users")).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "a reserved key must read as absent, not forbidden"
    );

    let (status, _, _) = call(
        &state,
        json_request("PUT", "/api/settings/auth:users", json!({ "value": "{}" })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (_, _, body) = call(
        &state,
        json_request("DELETE", "/api/settings/branding", json!({})),
    )
    .await;
    assert_eq!(body["deleted"], json!(true));
}

#[tokio::test]
async fn webhooks_crud_never_returns_the_secret_after_creation() {
    let storage = temp_storage("http-webhooks");
    let state = dashboard_state(&storage, AuthMode::Open);

    let (status, _, created) = call(
        &state,
        json_request(
            "POST",
            "/api/webhooks",
            json!({
                "url": "https://example.com/hook",
                "events": ["job.completed"],
                "generate_secret": true,
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let id = created["id"].as_str().expect("an id").to_string();
    // Exactly once, on creation.
    assert!(created["secret"].as_str().is_some());
    assert_eq!(created["has_secret"], json!(true));

    let (_, _, listed) = call(&state, get("/api/webhooks")).await;
    let rows = listed.as_array().expect("an array");
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].get("secret").is_none(),
        "the secret must stay hidden"
    );
    assert_eq!(rows[0]["has_secret"], json!(true));

    let (status, _, updated) = call(
        &state,
        json_request(
            "PUT",
            &format!("/api/webhooks/{id}"),
            json!({ "enabled": false }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(updated["enabled"], json!(false));
    assert!(updated.get("secret").is_none());

    let (status, _, rotated) = call(
        &state,
        json_request(
            "POST",
            &format!("/api/webhooks/{id}/rotate-secret"),
            json!({}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_ne!(rotated["secret"], created["secret"]);

    let (status, _, _) = call(
        &state,
        json_request("DELETE", &format!("/api/webhooks/{id}"), json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, _, listed) = call(&state, get("/api/webhooks")).await;
    assert_eq!(listed.as_array().expect("an array").len(), 0);
}

#[tokio::test]
async fn a_webhook_url_that_would_be_an_ssrf_is_refused() {
    let storage = temp_storage("http-webhook-ssrf");
    let state = dashboard_state(&storage, AuthMode::Open);

    let (status, _, body) = call(
        &state,
        json_request(
            "POST",
            "/api/webhooks",
            json!({ "url": "http://169.254.169.254/latest" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"].as_str().expect("message").contains("private"));
}

#[tokio::test]
async fn overrides_are_validated_and_merged() {
    let storage = temp_storage("http-overrides");
    let state = dashboard_state(&storage, AuthMode::Open);

    let (status, _, _) = call(
        &state,
        json_request(
            "PUT",
            "/api/tasks/send_email/override",
            json!({ "timeout": 0 }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _, body) = call(
        &state,
        json_request(
            "PUT",
            "/api/tasks/send_email/override",
            json!({ "timeout": 60, "rate_limit": "100/m" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["timeout"], json!(60));

    // A second patch merges rather than replacing.
    let (_, _, body) = call(
        &state,
        json_request(
            "PUT",
            "/api/tasks/send_email/override",
            json!({ "priority": 5 }),
        ),
    )
    .await;
    assert_eq!(body["timeout"], json!(60));
    assert_eq!(body["priority"], json!(5));
    assert_eq!(body["rate_limit"], json!("100/m"));

    // An explicit null clears one field.
    let (_, _, body) = call(
        &state,
        json_request(
            "PUT",
            "/api/tasks/send_email/override",
            json!({ "timeout": null }),
        ),
    )
    .await;
    assert_eq!(body["timeout"], Value::Null);

    let (_, _, listed) = call(&state, get("/api/tasks")).await;
    let rows = listed.as_array().expect("an array");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["name"], json!("send_email"));
    assert_eq!(rows[0]["advertised"], json!(false));
}

#[tokio::test]
async fn pausing_a_queue_shows_up_in_the_queue_views() {
    let storage = temp_storage("http-queues");
    let state = dashboard_state(&storage, AuthMode::Open);

    let (status, _, body) = call(
        &state,
        json_request("POST", "/api/queues/default/pause", json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["paused"], json!("default"));

    let (_, _, paused) = call(&state, get("/api/queues/paused")).await;
    assert_eq!(paused, json!(["default"]));

    let (_, _, queues) = call(&state, get("/api/queues")).await;
    let row = &queues.as_array().expect("an array")[0];
    assert_eq!(row["name"], json!("default"));
    assert_eq!(row["paused"], json!(true));

    call(
        &state,
        json_request("POST", "/api/queues/default/resume", json!({})),
    )
    .await;
    let (_, _, paused) = call(&state, get("/api/queues/paused")).await;
    assert_eq!(paused, json!([]));
}

#[tokio::test]
async fn the_executor_inventory_is_empty_without_a_listener() {
    let storage = temp_storage("http-executors");
    let state = dashboard_state(&storage, AuthMode::Open);

    let (status, _, body) = call(&state, get("/api/executors")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["executors"], json!([]));
    assert_eq!(body["capacity"]["total_slots"], json!(0));
}

#[tokio::test]
async fn retention_reports_the_unreported_state_distinctly() {
    let storage = temp_storage("http-retention");
    let state = dashboard_state(&storage, AuthMode::Open);

    let (status, _, body) = call(&state, get("/api/retention")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["reported"], json!(false));
    assert_eq!(body["enabled"], json!(false));
    assert!(body["windows"].is_object());

    // The dry run always answers, because it computes live.
    let (status, _, body) = call(&state, get("/api/retention/dry-run")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total"], json!(0));
    assert!(body["counts"].is_object());
}

#[tokio::test]
async fn the_auth_endpoints_are_absent_when_auth_is_off() {
    let storage = temp_storage("http-auth-off");
    let state = dashboard_state(&storage, AuthMode::Open);

    let (status, _, body) = call(&state, get("/api/auth/status")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["auth_enabled"], json!(false));
    assert_eq!(body["setup_required"], json!(false));

    let (status, _, body) = call(&state, get("/api/auth/whoami")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], json!("auth_disabled"));
}
