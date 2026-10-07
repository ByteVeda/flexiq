//! The dashboard's audit page reads the trail back (#995): newest first, the
//! same filters as `ListAuditRecords`, keyset pages, one namespace only.

mod support;

use axum::http::StatusCode;
use flexiq_core::{AuditRecord, Storage, StorageBackend};
use serde_json::Value;

use flexiq_server::config::dashboard::AuthMode;
use support::{call, dashboard_state, dashboard_state_in_namespace, get, temp_storage};

/// A record at `at_ms`, by `token_id`, acting on `target_kind:target`.
fn record(namespace: &str, at_ms: i64, token_id: &str, target: (&str, &str)) -> AuditRecord {
    AuditRecord {
        id: format!("{at_ms:020}-{token_id}"),
        namespace: namespace.to_string(),
        at_ms,
        principal_kind: "token".to_string(),
        token_id: token_id.to_string(),
        principal: format!("{token_id}-name"),
        operation: "flexiq.producer.v1.Producer/Enqueue".to_string(),
        target_kind: Some(target.0.to_string()),
        target: Some(target.1.to_string()),
        outcome: "OK".to_string(),
    }
}

/// Seed `default` with five records, `at_ms` 1..=5, alternating two tokens.
fn seed(storage: &StorageBackend) {
    let records: Vec<_> = (1..=5)
        .map(|at| {
            let token = if at % 2 == 0 { "even" } else { "odd" };
            record("default", at, token, ("job", &format!("j{at}")))
        })
        .collect();
    storage.append_audit(&records).expect("append");
}

/// The `at` of every record a listing returned, in order.
fn times(body: &Value) -> Vec<i64> {
    body["records"]
        .as_array()
        .expect("records")
        .iter()
        .map(|record| record["at"].as_i64().expect("at"))
        .collect()
}

#[tokio::test]
async fn the_trail_lists_newest_first_with_every_field() {
    let storage = temp_storage("audit-records-order");
    seed(&storage);
    let state = dashboard_state(&storage, AuthMode::Open);

    let (status, _, body) = call(&state, get("/api/audit-records")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(times(&body), vec![5, 4, 3, 2, 1]);
    assert!(body["next_cursor"].is_null(), "a short page is the last");

    let newest = &body["records"][0];
    assert_eq!(newest["principal_kind"], "token");
    assert_eq!(newest["token_id"], "odd");
    assert_eq!(newest["principal"], "odd-name");
    assert_eq!(newest["operation"], "flexiq.producer.v1.Producer/Enqueue");
    assert_eq!(newest["target_kind"], "job");
    assert_eq!(newest["target"], "j5");
    assert_eq!(newest["outcome"], "OK");
    assert!(
        newest.get("namespace").is_none(),
        "a dashboard lists its own"
    );
}

#[tokio::test]
async fn filters_narrow_the_listing() {
    let storage = temp_storage("audit-records-filters");
    seed(&storage);
    let state = dashboard_state(&storage, AuthMode::Open);

    for (query, expected) in [
        ("token_id=even", vec![4, 2]),
        ("principal_kind=user", vec![]),
        ("target_kind=job&target=j3", vec![3]),
        ("target_kind=queue", vec![]),
        // `since` is inclusive, `until` exclusive.
        ("since=2&until=4", vec![3, 2]),
        ("token_id=odd&since=2", vec![5, 3]),
    ] {
        let (status, _, body) = call(&state, get(&format!("/api/audit-records?{query}"))).await;
        assert_eq!(status, StatusCode::OK, "{query}: {body}");
        assert_eq!(times(&body), expected, "{query}");
    }
}

#[tokio::test]
async fn cursor_pages_walk_the_whole_trail_once() {
    let storage = temp_storage("audit-records-pages");
    seed(&storage);
    let state = dashboard_state(&storage, AuthMode::Open);

    let mut seen = Vec::new();
    let mut path = "/api/audit-records?limit=2".to_string();
    loop {
        let (status, _, body) = call(&state, get(&path)).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        seen.extend(times(&body));
        match body["next_cursor"].as_str() {
            Some(cursor) => path = format!("/api/audit-records?limit=2&after={cursor}"),
            None => break,
        }
    }
    assert_eq!(seen, vec![5, 4, 3, 2, 1]);
}

#[tokio::test]
async fn malformed_parameters_are_the_clients_error() {
    let storage = temp_storage("audit-records-bad");
    let state = dashboard_state(&storage, AuthMode::Open);

    for query in [
        "after=not-a-cursor",
        "since=yesterday",
        "until=-5",
        "limit=x",
    ] {
        let (status, _, body) = call(&state, get(&format!("/api/audit-records?{query}"))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {body}");
    }
}

#[tokio::test]
async fn a_dashboard_lists_only_its_own_namespace() {
    let storage = temp_storage("audit-records-tenant");
    seed(&storage);
    storage
        .append_audit(&[record("acme", 9, "theirs", ("queue", "billing"))])
        .expect("append");

    let default = dashboard_state(&storage, AuthMode::Open);
    let (_, _, body) = call(&default, get("/api/audit-records")).await;
    assert_eq!(times(&body), vec![5, 4, 3, 2, 1]);

    let acme = dashboard_state_in_namespace(&storage, AuthMode::Open, "acme");
    let (_, _, body) = call(&acme, get("/api/audit-records")).await;
    assert_eq!(times(&body), vec![9]);
}
