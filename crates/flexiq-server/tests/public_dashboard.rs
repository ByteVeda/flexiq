//! `FLEXIQ_DASHBOARD_AUTH=public`: every caller is an anonymous viewer (#867).
//!
//! Driven through the real router, so the gate's classification table is what
//! these pin — a route added later is read-only here without naming it.

mod support;

use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use flexiq_core::{now_millis, AuditFilter, NewJob, Storage};
use serde_json::json;

use flexiq_server::config::dashboard::AuthMode;
use support::{call, dashboard_state, get, json_request, temp_storage};

fn new_job() -> NewJob {
    NewJob {
        queue: "default".to_string(),
        task_name: "demo.ok".to_string(),
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
        debounce_key: None,
        enqueued_by: None,
    }
}

#[tokio::test]
async fn status_reports_read_only_without_a_login() {
    let storage = temp_storage("public-status");
    let state = dashboard_state(&storage, AuthMode::Public);

    let (status, _, body) = call(&state, get("/api/auth/status")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["auth_enabled"], json!(false));
    assert_eq!(body["setup_required"], json!(false));
    assert_eq!(body["read_only"], json!(true));
    assert_eq!(body["demo_enqueue"], json!(null));

    // The other modes answer exactly as the SDK dashboards do.
    let open = dashboard_state(&storage, AuthMode::Open);
    let (_, _, body) = call(&open, get("/api/auth/status")).await;
    assert_eq!(
        body,
        json!({ "auth_enabled": false, "setup_required": false })
    );
}

/// The SPA learns where its enqueue button posts from the status probe.
#[tokio::test]
async fn status_carries_the_demo_enqueue_path() {
    let storage = temp_storage("public-demo-enqueue");
    let mut state = dashboard_state(&storage, AuthMode::Public);
    Arc::get_mut(&mut state)
        .expect("the state is not shared yet")
        .config
        .demo_enqueue = Some("/t/demo?token=flexiq-public-demo".to_string());

    let (_, _, body) = call(&state, get("/api/auth/status")).await;
    assert_eq!(
        body["demo_enqueue"],
        json!("/t/demo?token=flexiq-public-demo")
    );
}

#[tokio::test]
async fn reads_need_no_credential() {
    let storage = temp_storage("public-reads");
    let job = storage.enqueue(new_job()).expect("enqueue");
    let state = dashboard_state(&storage, AuthMode::Public);

    for path in [
        "/api/jobs",
        "/api/stats",
        "/api/dead-letters",
        "/api/queues",
    ] {
        let (status, _, _) = call(&state, get(path)).await;
        assert_eq!(status, StatusCode::OK, "{path}");
    }
    let (status, _, body) = call(&state, get(&format!("/api/jobs/{}", job.id))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["task_name"], json!("demo.ok"));
}

#[tokio::test]
async fn every_write_is_refused_and_nothing_changes() {
    let storage = temp_storage("public-writes");
    let job = storage.enqueue(new_job()).expect("enqueue");
    let state = dashboard_state(&storage, AuthMode::Public);

    let cancel = format!("/api/jobs/{}/cancel", job.id);
    for (method, path) in [
        ("POST", cancel.as_str()),
        ("POST", "/api/dead-letters/purge"),
        ("POST", "/api/queues/default/pause"),
        ("PUT", "/api/settings/ui/theme"),
        ("DELETE", "/api/settings/ui/theme"),
        ("POST", "/api/webhooks"),
    ] {
        let (status, _, body) =
            call(&state, json_request(method, path, json!({ "value": "x" }))).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} {path}");
        assert_eq!(body["error"], json!("read_only"), "{method} {path}");
    }

    let stored = storage
        .get_job(&job.id, None)
        .expect("read")
        .expect("present");
    assert_eq!(stored.status, job.status, "the cancel must not have landed");

    // Refused before the audit opens: an anonymous caller cannot fill the
    // trail by being turned away.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let trail = storage
        .list_audit_after("default", &AuditFilter::default(), 100, None)
        .expect("list");
    assert!(trail.is_empty(), "{trail:?}");
}

#[tokio::test]
async fn admin_reads_stay_closed() {
    let storage = temp_storage("public-admin-reads");
    let state = dashboard_state(&storage, AuthMode::Public);

    for path in ["/api/grpc-tokens", "/api/audit-records"] {
        let (status, _, _) = call(&state, get(path)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{path}");
    }
}

#[tokio::test]
async fn login_routes_do_not_exist() {
    let storage = temp_storage("public-login");
    let state = dashboard_state(&storage, AuthMode::Public);

    for (method, path) in [
        ("POST", "/api/auth/login"),
        ("POST", "/api/auth/setup"),
        ("GET", "/api/auth/whoami"),
    ] {
        let (status, _, _) = call(
            &state,
            json_request(method, path, json!({ "username": "a", "password": "b" })),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{method} {path}");
    }
}

/// Probes and metrics are not opened by read-only mode: with no metrics token
/// they stay closed, as in session mode.
#[tokio::test]
async fn metrics_need_their_token() {
    let storage = temp_storage("public-metrics");
    let state = dashboard_state(&storage, AuthMode::Public);

    let (status, _, _) = call(&state, get("/metrics")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _, _) = call(&state, get("/health")).await;
    assert_eq!(status, StatusCode::OK);
}
