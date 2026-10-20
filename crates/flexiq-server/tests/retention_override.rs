//! The dashboard's retention override, through the real router.

mod support;

use axum::http::StatusCode;
use serde_json::json;

use flexiq_server::config::dashboard::AuthMode;
use support::{call, dashboard_state, get, json_request, temp_storage};

const DAY_MS: i64 = 86_400_000;

#[tokio::test]
async fn an_unsaved_override_reads_as_code() {
    let storage = temp_storage("override-default");
    let state = dashboard_state(&storage, AuthMode::Open);

    let (status, _, body) = call(&state, get("/api/retention/override")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["source"], json!("code"));
    assert_eq!(body["updated_at"], json!(null));
    assert_eq!(body["windows"]["dead_letter_ttl_ms"], json!(null));

    let (_, _, body) = call(&state, get("/api/retention")).await;
    assert_eq!(body["source"], json!("code"));
    assert_eq!(body["code_windows"], json!(null));
}

#[tokio::test]
async fn saving_switches_the_preview_and_the_toggle_keeps_the_windows() {
    let storage = temp_storage("override-roundtrip");
    let state = dashboard_state(&storage, AuthMode::Open);
    let windows = json!({ "task_logs_ttl_ms": DAY_MS, "dead_letter_ttl_ms": 90 * DAY_MS });

    let (status, _, body) = call(
        &state,
        json_request(
            "PUT",
            "/api/retention/override",
            json!({ "source": "dashboard", "windows": windows }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["source"], json!("dashboard"));
    assert!(body["updated_at"].is_i64());

    // The preview follows the override before any sweep has run.
    let (_, _, preview) = call(&state, get("/api/retention/dry-run")).await;
    assert_eq!(preview["windows"]["task_logs_ttl_ms"], json!(DAY_MS));
    assert_eq!(preview["windows"]["archived_jobs_ttl_ms"], json!(null));
    assert_eq!(preview["defaulted"], json!(false));

    // Back to code: the preview drops the override, the editor keeps it.
    let (status, _, _) = call(
        &state,
        json_request(
            "PUT",
            "/api/retention/override",
            json!({ "source": "code", "windows": windows }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, _, preview) = call(&state, get("/api/retention/dry-run")).await;
    assert_eq!(preview["defaulted"], json!(true));
    let (_, _, stored) = call(&state, get("/api/retention/override")).await;
    assert_eq!(stored["source"], json!("code"));
    assert_eq!(stored["windows"]["dead_letter_ttl_ms"], json!(90 * DAY_MS));

    let (status, _, body) = call(
        &state,
        json_request("DELETE", "/api/retention/override", json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["deleted"], json!(true));
    let (_, _, stored) = call(&state, get("/api/retention/override")).await;
    assert_eq!(stored["windows"]["dead_letter_ttl_ms"], json!(null));
}

#[tokio::test]
async fn a_window_that_would_empty_a_table_is_refused() {
    let storage = temp_storage("override-invalid");
    let state = dashboard_state(&storage, AuthMode::Open);

    for body in [
        json!({ "source": "dashboard", "windows": { "task_logs_ttl_ms": 0 } }),
        json!({ "source": "dashboard", "windows": { "task_log_ttl_ms": DAY_MS } }),
        json!({ "source": "always" }),
    ] {
        let (status, _, answer) = call(
            &state,
            json_request("PUT", "/api/retention/override", body.clone()),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body} → {answer}");
    }
    let (_, _, stored) = call(&state, get("/api/retention/override")).await;
    assert_eq!(stored["updated_at"], json!(null), "nothing was written");
}

#[tokio::test]
async fn a_public_dashboard_reads_the_override_but_cannot_change_it() {
    let storage = temp_storage("override-public");
    let state = dashboard_state(&storage, AuthMode::Public);

    let (status, _, _) = call(&state, get("/api/retention/override")).await;
    assert_eq!(status, StatusCode::OK);
    for method in ["PUT", "DELETE"] {
        let (status, _, _) = call(
            &state,
            json_request(
                method,
                "/api/retention/override",
                json!({ "source": "code" }),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method}");
    }
}

/// A process with maintenance off previews keeping everything, override or
/// not — its sweep ignores the override too.
#[tokio::test]
async fn maintenance_off_previews_no_windows_whatever_the_override() {
    let storage = temp_storage("override-maintenance-off");
    let state = dashboard_state(&storage, AuthMode::Open);
    let (status, _, _) = call(
        &state,
        json_request(
            "PUT",
            "/api/retention/override",
            json!({ "source": "dashboard", "windows": { "task_logs_ttl_ms": DAY_MS } }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let mut idle = dashboard_state(&storage, AuthMode::Open);
    std::sync::Arc::get_mut(&mut idle)
        .expect("the state is not shared yet")
        .maintenance = false;
    let (_, _, preview) = call(&idle, get("/api/retention/dry-run")).await;
    assert_eq!(preview["enabled"], json!(false));
}
