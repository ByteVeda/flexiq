//! What the dashboard changes, and who changed it, reaches the audit trail
//! (#994) — through the full router, gate included.

mod support;

use std::time::Duration;

use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use flexiq_core::{AuditFilter, AuditRecord, Storage, StorageBackend};
use serde_json::{json, Value};

use flexiq_server::config::dashboard::AuthMode;
use flexiq_server::dashboard::auth::model::Role;
use flexiq_server::dashboard::auth::store;
use flexiq_server::dashboard::state::SharedState;
use support::{
    call, dashboard_state, dashboard_state_in_namespace, get, json_request, temp_storage,
};

/// A logged-in browser: its session cookie and CSRF token.
struct Session {
    cookie: String,
    csrf: String,
}

impl Session {
    /// A mutation carrying the cookie, and the CSRF header when `csrf`.
    fn request(&self, method: &str, path: &str, body: Value, csrf: bool) -> Request<Body> {
        let mut builder = Request::builder()
            .method(method)
            .uri(path)
            .header("content-type", "application/json")
            .header("cookie", &self.cookie);
        if csrf {
            builder = builder.header("x-csrf-token", &self.csrf);
        }
        builder
            .body(Body::from(body.to_string()))
            .expect("valid request")
    }

    fn send(&self, method: &str, path: &str, body: Value) -> Request<Body> {
        self.request(method, path, body, true)
    }

    fn get(&self, path: &str) -> Request<Body> {
        Request::builder()
            .uri(path)
            .header("cookie", &self.cookie)
            .body(Body::empty())
            .expect("valid request")
    }
}

fn session_from(headers: &HeaderMap) -> Session {
    let cookie = |name: &str| {
        headers
            .get_all("set-cookie")
            .iter()
            .filter_map(|value| value.to_str().ok())
            .filter_map(|cookie| cookie.split(';').next()?.split_once('='))
            .find(|(key, _)| *key == name)
            .map(|(_, value)| value.to_string())
            .unwrap_or_else(|| panic!("no {name} cookie"))
    };
    let (session, csrf) = (cookie("flexiq_session"), cookie("flexiq_csrf"));
    Session {
        cookie: format!("flexiq_session={session}; flexiq_csrf={csrf}"),
        csrf,
    }
}

async fn login(state: &SharedState, username: &str) -> Session {
    let (status, headers, _) = call(
        state,
        json_request(
            "POST",
            "/api/auth/login",
            json!({ "username": username, "password": "supersecret" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "login as {username}");
    session_from(&headers)
}

/// Set up the first admin, `ops`, and log in as them.
async fn admin(state: &SharedState) -> Session {
    let (status, _, _) = call(
        state,
        json_request(
            "POST",
            "/api/auth/setup",
            json!({ "username": "ops", "password": "supersecret" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    login(state, "ops").await
}

/// `namespace`'s trail, newest first, once it holds `count` records. Then a
/// short settle, so a test asserting "exactly this many" sees any extra.
async fn trail(storage: &StorageBackend, namespace: &str, count: usize) -> Vec<AuditRecord> {
    let list = || {
        storage
            .list_audit_after(namespace, &AuditFilter::default(), 100, None)
            .expect("list")
    };
    for _ in 0..100 {
        if list().len() >= count {
            tokio::time::sleep(Duration::from_millis(50)).await;
            return list();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the trail never held {count} record(s): {:?}", list());
}

#[tokio::test]
async fn an_admins_change_is_recorded_against_them() {
    let storage = temp_storage("audit-dash-admin");
    let state = dashboard_state(&storage, AuthMode::Session);
    let ops = admin(&state).await;

    let (status, _, _) = call(
        &state,
        ops.send("POST", "/api/queues/emails/pause", json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let records = trail(&storage, "default", 1).await;
    assert_eq!(records.len(), 1, "{records:?}");
    let record = &records[0];
    assert_eq!(record.principal_kind, "user");
    assert_eq!(record.token_id, "ops");
    assert_eq!(record.principal, "ops");
    assert_eq!(record.operation, "dashboard POST /api/queues/{queue}/pause");
    assert_eq!(record.target_kind.as_deref(), Some("queue"));
    assert_eq!(record.target.as_deref(), Some("emails"));
    assert_eq!(record.outcome, "OK");
}

#[tokio::test]
async fn a_refused_change_is_recorded_against_who_tried() {
    let storage = temp_storage("audit-dash-refused");
    let state = dashboard_state(&storage, AuthMode::Session);
    let ops = admin(&state).await;
    let backend: &StorageBackend = &storage;
    store::create_user(backend, "reader", "supersecret", Role::Viewer)
        .expect("storage")
        .expect("valid user");
    let reader = login(&state, "reader").await;

    let (status, _, _) = call(
        &state,
        reader.send("POST", "/api/queues/emails/pause", json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    // A forged cross-site request rides the victim's cookie but not their
    // CSRF token: refused, and the attempt is on the record.
    let (status, _, _) = call(
        &state,
        ops.request("DELETE", "/api/dead-letters/d1", json!({}), false),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let records = trail(&storage, "default", 2).await;
    assert_eq!(records.len(), 2, "{records:?}");
    let by = |who: &str| {
        records
            .iter()
            .find(|record| record.token_id == who)
            .unwrap_or_else(|| panic!("no record by {who}: {records:?}"))
    };
    assert_eq!(by("reader").outcome, "PERMISSION_DENIED");
    assert_eq!(by("reader").target.as_deref(), Some("emails"));
    assert_eq!(by("ops").outcome, "PERMISSION_DENIED");
    assert_eq!(by("ops").target_kind.as_deref(), Some("dead_letter"));
}

#[tokio::test]
async fn reads_and_requests_with_no_session_are_not_recorded() {
    let storage = temp_storage("audit-dash-quiet");
    let state = dashboard_state(&storage, AuthMode::Session);
    let ops = admin(&state).await;

    let (status, _, _) = call(&state, ops.get("/api/jobs")).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, _) = call(
        &state,
        json_request("POST", "/api/queues/emails/pause", json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    // A path no route serves changed nothing.
    let (status, _, _) = call(&state, ops.send("POST", "/api/no-such-thing", json!({}))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // A marker write, so "nothing else" is checked after the writer drained.
    call(
        &state,
        ops.send("POST", "/api/queues/marker/pause", json!({})),
    )
    .await;
    let records = trail(&storage, "default", 1).await;
    assert_eq!(records.len(), 1, "only the marker: {records:?}");
    assert_eq!(records[0].target.as_deref(), Some("marker"));
}

#[tokio::test]
async fn with_auth_off_the_change_is_recorded_anonymously() {
    let storage = temp_storage("audit-dash-open");
    let state = dashboard_state(&storage, AuthMode::Open);

    let (status, _, _) = call(
        &state,
        json_request("PUT", "/api/settings/ui/theme", json!({ "value": "dark" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, _) = call(&state, get("/api/settings")).await;
    assert_eq!(status, StatusCode::OK);

    let records = trail(&storage, "default", 1).await;
    assert_eq!(records.len(), 1, "{records:?}");
    let record = &records[0];
    assert_eq!(record.principal_kind, "anonymous");
    assert_eq!(record.token_id, "");
    assert_eq!(record.operation, "dashboard PUT /api/settings/{*key}");
    assert_eq!(record.target_kind.as_deref(), Some("setting"));
    assert_eq!(record.target.as_deref(), Some("ui/theme"), "the whole key");
}

/// The most security-relevant pair the dashboard serves: both name the token,
/// though only the revoke carries it in its path.
#[tokio::test]
async fn minting_and_revoking_a_token_name_it() {
    let storage = temp_storage("audit-dash-token");
    let state = dashboard_state_in_namespace(&storage, AuthMode::Session, "prod");
    let ops = admin(&state).await;

    let (status, _, minted) = call(
        &state,
        ops.send(
            "POST",
            "/api/grpc-tokens",
            json!({ "name": "ci", "scopes": ["produce"] }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{minted}");
    let id = minted["id"].as_str().expect("an id").to_string();
    let (status, _, _) = call(
        &state,
        ops.send("DELETE", &format!("/api/grpc-tokens/{id}"), json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let records = trail(&storage, "prod", 2).await;
    assert_eq!(records.len(), 2, "{records:?}");
    let (revoke, mint) = (&records[0], &records[1]);
    assert_eq!(mint.operation, "dashboard POST /api/grpc-tokens");
    assert_eq!(revoke.operation, "dashboard DELETE /api/grpc-tokens/{id}");
    for record in [mint, revoke] {
        assert_eq!(record.token_id, "ops");
        assert_eq!(record.target_kind.as_deref(), Some("token"));
        assert_eq!(record.target.as_deref(), Some(id.as_str()));
        assert_eq!(record.outcome, "OK");
    }
}

/// The SDK dashboards record by the core's route list (#1020), so every route
/// on it must be one this router serves under the same template — or an SDK's
/// record of a change would name an operation the server's never does.
#[tokio::test]
async fn every_shared_route_is_recorded_under_the_cores_template() {
    use flexiq_core::audit::dashboard::{operation, ROUTES};

    let storage = temp_storage("audit-dash-parity");
    let state = dashboard_state(&storage, AuthMode::Session);
    let ops = admin(&state).await;

    // The session ends with logout, so that goes last.
    let mut routes = ROUTES.to_vec();
    routes.sort_by_key(|(_, template)| *template == "/api/auth/logout");
    for (method, template) in &routes {
        let path = template
            .split('/')
            .map(|segment| {
                if segment.starts_with('{') {
                    "v"
                } else {
                    segment
                }
            })
            .collect::<Vec<_>>()
            .join("/");
        call(&state, ops.send(method, &path, json!({}))).await;
    }

    let recorded: std::collections::BTreeSet<String> = trail(&storage, "default", ROUTES.len())
        .await
        .into_iter()
        .map(|record| record.operation)
        .collect();
    let expected: std::collections::BTreeSet<String> = ROUTES
        .iter()
        .map(|(method, template)| operation(method, template))
        .collect();
    assert_eq!(recorded, expected);
}

#[tokio::test]
async fn a_namespaced_dashboard_records_into_its_namespace() {
    let storage = temp_storage("audit-dash-ns");
    let state = dashboard_state_in_namespace(&storage, AuthMode::Open, "prod");

    call(
        &state,
        json_request("POST", "/api/queues/emails/resume", json!({})),
    )
    .await;

    let records = trail(&storage, "prod", 1).await;
    assert_eq!(records[0].namespace, "prod");
    let default = storage
        .list_audit_after("default", &AuditFilter::default(), 100, None)
        .expect("list");
    assert!(default.is_empty(), "{default:?}");
}
