//! The same dashboard surface, against Postgres and Redis.
//!
//! Each test is skipped unless its backend URL is in the environment, matching
//! how the rest of the repo tests hosted backends:
//!
//! ```bash
//! TASKITO_POSTGRES_TEST_URL=postgres://… cargo test -p taskito-server --features postgres
//! TASKITO_REDIS_TEST_URL=redis://…       cargo test -p taskito-server --features redis
//! ```
//!
//! Rows are namespaced per run so a shared hosted database can be reused
//! without one run's leftovers failing the next.
//!
//! With neither feature compiled in there is nothing to run, so the whole file
//! is gated rather than left as dead code.
#![cfg(any(feature = "postgres", feature = "redis"))]

mod support;

use axum::http::StatusCode;
use serde_json::json;
use taskito_core::{now_millis, NewJob, Storage};

use support::{call, dashboard_state_for, get, json_request};
use taskito_server::config::backend;
use taskito_server::config::dashboard::AuthMode;
use taskito_server::dashboard::state::SharedState;
use taskito_server::dashboard::static_assets::StaticAssets;

/// Open the backend named by `variable`, or `None` when it is not configured.
fn backend_state(variable: &str) -> Option<(SharedState, String)> {
    let dsn = std::env::var(variable).ok().filter(|dsn| !dsn.is_empty())?;
    let opened = backend::open(&dsn, None, None, true).unwrap_or_else(|error| {
        // The DSN is not interpolated — it carries credentials.
        panic!("{variable} is set but the backend could not be opened: {error}")
    });
    // A per-run task name keeps assertions exact against a shared database.
    let namespace = format!("server-test-{}-{}", std::process::id(), now_millis());
    let state = dashboard_state_for(
        opened.storage,
        opened.workflows,
        AuthMode::Open,
        StaticAssets::new(None),
    );
    Some((state, namespace))
}

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
        debounce_key: None,
    }
}

/// Drive the routes that exercise every layer: a listing, a per-row read, a
/// mutation, the settings store, and the probes.
async fn exercise_dashboard(state: SharedState, task_name: String) {
    let job = state
        .storage
        .enqueue(new_job(&task_name))
        .expect("enqueue against the backend");

    let (status, _, body) = call(&state, get("/health")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], json!("ok"));

    let (status, _, body) = call(&state, get("/readiness")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["checks"]["storage"], json!("ok"));

    let (status, _, body) = call(&state, get(&format!("/api/jobs?task={task_name}"))).await;
    assert_eq!(status, StatusCode::OK);
    let rows = body.as_array().expect("an array of jobs");
    assert_eq!(
        rows.len(),
        1,
        "the listing must find exactly this run's job"
    );
    assert_eq!(rows[0]["id"], json!(job.id));
    assert_eq!(rows[0]["status"], json!("pending"));
    assert!(rows[0].get("payload").is_none(), "listings stay blob-free");

    let (status, _, body) = call(&state, get(&format!("/api/jobs/{}", job.id))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["task_name"], json!(task_name));

    let (status, _, body) = call(&state, get("/api/stats")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["pending"].as_i64().expect("a count") >= 1,
        "the enqueued job must be counted"
    );

    // A mutation, read back through a second query.
    let (status, _, body) = call(
        &state,
        json_request("POST", &format!("/api/jobs/{}/cancel", job.id), json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["cancelled"], json!(true));
    let (_, _, body) = call(&state, get(&format!("/api/jobs/{}", job.id))).await;
    assert_eq!(body["status"], json!("cancelled"));

    // The settings store backs auth, webhooks, and overrides on every backend.
    let key = format!("dashboard:{task_name}");
    let (status, _, _) = call(
        &state,
        json_request(
            "PUT",
            &format!("/api/settings/{key}"),
            json!({ "value": { "checked": true } }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, _, body) = call(&state, get(&format!("/api/settings/{key}"))).await;
    assert_eq!(body["value"], json!(r#"{"checked":true}"#));
    let (_, _, body) = call(
        &state,
        json_request("DELETE", &format!("/api/settings/{key}"), json!({})),
    )
    .await;
    assert_eq!(body["deleted"], json!(true));

    let (status, _, body) = call(&state, get("/api/workers")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.is_array());

    let (status, _, body) = call(&state, get("/api/workflows/runs")).await;
    assert_eq!(status, StatusCode::OK, "the workflow store must answer too");
    assert!(body["runs"].is_array());
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn the_dashboard_serves_postgres() {
    let Some((state, namespace)) = backend_state("TASKITO_POSTGRES_TEST_URL") else {
        eprintln!("skipped: TASKITO_POSTGRES_TEST_URL is not set");
        return;
    };
    exercise_dashboard(state, namespace).await;
}

#[cfg(feature = "redis")]
#[tokio::test]
async fn the_dashboard_serves_redis() {
    let Some((state, namespace)) = backend_state("TASKITO_REDIS_TEST_URL") else {
        eprintln!("skipped: TASKITO_REDIS_TEST_URL is not set");
        return;
    };
    exercise_dashboard(state, namespace).await;
}
