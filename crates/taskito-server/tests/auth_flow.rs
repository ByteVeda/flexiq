//! Session auth end to end: setup, login, CSRF, roles, logout.

mod support;

use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use serde_json::{json, Value};
use taskito_core::StorageBackend;

use support::{call, dashboard_state, get, json_request, temp_storage};
use taskito_server::config::dashboard::AuthMode;
use taskito_server::dashboard::auth::model::Role;
use taskito_server::dashboard::auth::store;
use taskito_server::dashboard::state::SharedState;

/// The credentials a live session presents on every request.
struct Credentials {
    session: String,
    csrf: String,
}

impl Credentials {
    /// A GET carrying the session cookie.
    fn get(&self, path: &str) -> Request<Body> {
        Request::builder()
            .uri(path)
            .header("cookie", self.cookie_header())
            .body(Body::empty())
            .expect("valid request")
    }

    /// A mutation carrying the session cookie and the CSRF header.
    fn post(&self, path: &str, body: Value) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json")
            .header("cookie", self.cookie_header())
            .header("x-csrf-token", &self.csrf)
            .body(Body::from(body.to_string()))
            .expect("valid request")
    }

    /// The same mutation with the CSRF header left off.
    fn post_without_csrf(&self, path: &str, body: Value) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json")
            .header("cookie", self.cookie_header())
            .body(Body::from(body.to_string()))
            .expect("valid request")
    }

    fn cookie_header(&self) -> String {
        format!(
            "flexiq_session={}; taskito_csrf={}",
            self.session, self.csrf
        )
    }
}

/// Read the session and CSRF tokens out of the `Set-Cookie` headers.
fn credentials_from(headers: &HeaderMap) -> Credentials {
    let mut session = None;
    let mut csrf = None;
    for value in headers.get_all("set-cookie") {
        let cookie = value.to_str().expect("ascii cookie");
        let (pair, _) = cookie.split_once(';').unwrap_or((cookie, ""));
        match pair.split_once('=') {
            Some(("flexiq_session", token)) => session = Some(token.to_string()),
            Some(("taskito_csrf", token)) => csrf = Some(token.to_string()),
            _ => {}
        }
    }
    Credentials {
        session: session.expect("a session cookie"),
        csrf: csrf.expect("a csrf cookie"),
    }
}

/// Run setup and log in, returning the admin's credentials.
async fn admin_session(state: &SharedState) -> Credentials {
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

    let response = call(
        state,
        json_request(
            "POST",
            "/api/auth/login",
            json!({ "username": "ops", "password": "supersecret" }),
        ),
    )
    .await;
    assert_eq!(response.0, StatusCode::OK);
    credentials_from(&response.1)
}

#[tokio::test]
async fn before_setup_every_api_route_reports_setup_required() {
    let storage = temp_storage("auth-setup-required");
    let state = dashboard_state(&storage, AuthMode::Session);

    let (status, _, body) = call(&state, get("/api/auth/status")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["auth_enabled"], json!(true));
    assert_eq!(body["setup_required"], json!(true));

    let (status, _, body) = call(&state, get("/api/jobs")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["error"], json!("setup_required"));

    // Probes stay reachable so a deployment is not unhealthy while waiting for
    // its first admin.
    let (status, _, _) = call(&state, get("/health")).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn setup_creates_one_admin_and_cannot_be_repeated() {
    let storage = temp_storage("auth-setup-once");
    let state = dashboard_state(&storage, AuthMode::Session);

    let (status, _, body) = call(
        &state,
        json_request(
            "POST",
            "/api/auth/setup",
            json!({ "username": "ops", "password": "supersecret" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["user"]["role"], json!("admin"));
    assert!(body["user"].get("password_hash").is_none());

    let (status, _, body) = call(
        &state,
        json_request(
            "POST",
            "/api/auth/setup",
            json!({ "username": "other", "password": "supersecret" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], json!("setup already complete"));
}

#[tokio::test]
async fn a_weak_or_malformed_signup_is_rejected() {
    let storage = temp_storage("auth-setup-validation");
    let state = dashboard_state(&storage, AuthMode::Session);

    for body in [
        json!({ "username": "ops" }),
        json!({ "username": "ops", "password": "short" }),
        json!({ "username": "bad user", "password": "supersecret" }),
    ] {
        let (status, _, _) = call(&state, json_request("POST", "/api/auth/setup", body)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}

#[tokio::test]
async fn login_sets_cookies_and_keeps_the_token_out_of_the_body() {
    let storage = temp_storage("auth-login");
    let state = dashboard_state(&storage, AuthMode::Session);
    call(
        &state,
        json_request(
            "POST",
            "/api/auth/setup",
            json!({ "username": "ops", "password": "supersecret" }),
        ),
    )
    .await;

    let (status, headers, body) = call(
        &state,
        json_request(
            "POST",
            "/api/auth/login",
            json!({ "username": "ops", "password": "supersecret" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["user"]["username"], json!("ops"));
    assert!(
        body["session"].get("token").is_none(),
        "the session token belongs in the HttpOnly cookie, not the body"
    );
    assert!(body["session"]["csrf_token"].as_str().is_some());

    let credentials = credentials_from(&headers);
    assert!(!credentials.session.is_empty());
    assert_eq!(
        credentials.csrf,
        body["session"]["csrf_token"].as_str().expect("csrf token")
    );
}

#[tokio::test]
async fn bad_credentials_are_indistinguishable_from_an_unknown_user() {
    let storage = temp_storage("auth-bad-credentials");
    let state = dashboard_state(&storage, AuthMode::Session);
    call(
        &state,
        json_request(
            "POST",
            "/api/auth/setup",
            json!({ "username": "ops", "password": "supersecret" }),
        ),
    )
    .await;

    let (wrong_password, _, wrong_password_body) = call(
        &state,
        json_request(
            "POST",
            "/api/auth/login",
            json!({ "username": "ops", "password": "wrong-password" }),
        ),
    )
    .await;
    let (unknown_user, _, unknown_user_body) = call(
        &state,
        json_request(
            "POST",
            "/api/auth/login",
            json!({ "username": "ghost", "password": "supersecret" }),
        ),
    )
    .await;

    assert_eq!(wrong_password, StatusCode::BAD_REQUEST);
    assert_eq!(unknown_user, StatusCode::BAD_REQUEST);
    assert_eq!(wrong_password_body, unknown_user_body);
    assert_eq!(wrong_password_body["error"], json!("invalid_credentials"));
}

#[tokio::test]
async fn an_authenticated_session_reads_and_mutates() {
    let storage = temp_storage("auth-session");
    let state = dashboard_state(&storage, AuthMode::Session);
    let credentials = admin_session(&state).await;

    let (status, _, body) = call(&state, credentials.get("/api/auth/whoami")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["user"]["username"], json!("ops"));
    assert_eq!(body["csrf_token"], json!(credentials.csrf));

    let (status, _, _) = call(&state, credentials.get("/api/jobs")).await;
    assert_eq!(status, StatusCode::OK);

    let (status, _, _) = call(
        &state,
        credentials.post("/api/queues/default/pause", json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn an_unauthenticated_request_is_refused() {
    let storage = temp_storage("auth-unauthenticated");
    let state = dashboard_state(&storage, AuthMode::Session);
    admin_session(&state).await;

    let (status, _, body) = call(&state, get("/api/jobs")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["error"], json!("not_authenticated"));

    let (status, _, _) = call(&state, get("/api/auth/whoami")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_mutation_without_a_csrf_token_is_refused() {
    let storage = temp_storage("auth-csrf");
    let state = dashboard_state(&storage, AuthMode::Session);
    let credentials = admin_session(&state).await;

    let (status, _, body) = call(
        &state,
        credentials.post_without_csrf("/api/queues/default/pause", json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"], json!("csrf_failed"));

    // A token the session never issued must not work either.
    let forged = Credentials {
        session: credentials.session.clone(),
        csrf: "forged-token".to_string(),
    };
    let (status, _, _) = call(&state, forged.post("/api/queues/default/pause", json!({}))).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn a_viewer_can_read_but_not_mutate() {
    let storage = temp_storage("auth-viewer");
    let state = dashboard_state(&storage, AuthMode::Session);
    admin_session(&state).await;

    // A viewer is created out of band; the API only ever mints admins.
    let backend: &StorageBackend = &storage;
    store::create_user(backend, "reader", "supersecret", Role::Viewer)
        .expect("storage")
        .expect("valid user");

    let response = call(
        &state,
        json_request(
            "POST",
            "/api/auth/login",
            json!({ "username": "reader", "password": "supersecret" }),
        ),
    )
    .await;
    assert_eq!(response.0, StatusCode::OK);
    let viewer = credentials_from(&response.1);

    let (status, _, _) = call(&state, viewer.get("/api/jobs")).await;
    assert_eq!(status, StatusCode::OK, "viewers keep read access");

    let (status, _, body) = call(&state, viewer.post("/api/queues/default/pause", json!({}))).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"], json!("forbidden"));

    // Their own account endpoints stay available.
    let (status, _, _) = call(&state, viewer.get("/api/auth/whoami")).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, _) = call(&state, viewer.post("/api/auth/logout", json!({}))).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn logout_invalidates_the_session_everywhere() {
    let storage = temp_storage("auth-logout");
    let state = dashboard_state(&storage, AuthMode::Session);
    let credentials = admin_session(&state).await;

    let (status, headers, body) =
        call(&state, credentials.post("/api/auth/logout", json!({}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], json!(true));
    assert!(headers
        .get_all("set-cookie")
        .iter()
        .all(|value| value.to_str().expect("ascii").contains("Max-Age=0")));

    // The cookie the client still holds no longer names a session.
    let (status, _, _) = call(&state, credentials.get("/api/jobs")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn changing_a_password_requires_the_old_one() {
    let storage = temp_storage("auth-change-password");
    let state = dashboard_state(&storage, AuthMode::Session);
    let credentials = admin_session(&state).await;

    let (status, _, body) = call(
        &state,
        credentials.post(
            "/api/auth/change-password",
            json!({ "old_password": "wrong-password", "new_password": "evenmoresecret" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], json!("invalid_credentials"));

    let (status, _, _) = call(
        &state,
        credentials.post(
            "/api/auth/change-password",
            json!({ "old_password": "supersecret", "new_password": "evenmoresecret" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _, _) = call(
        &state,
        json_request(
            "POST",
            "/api/auth/login",
            json!({ "username": "ops", "password": "evenmoresecret" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn repeated_bad_passwords_are_locked_out() {
    let storage = temp_storage("auth-throttle");
    let state = dashboard_state(&storage, AuthMode::Session);
    admin_session(&state).await;

    let attempt = || {
        json_request(
            "POST",
            "/api/auth/login",
            json!({ "username": "ops", "password": "wrong-password" }),
        )
    };

    // The budget is spent on rejected credentials only.
    for round in 0..10 {
        let (status, _, _) = call(&state, attempt()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "attempt {round}");
    }

    let (status, headers, body) = call(&state, attempt()).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(body["error"], json!("too_many_attempts"));
    assert!(
        headers.contains_key("retry-after"),
        "a lockout must say how long it lasts"
    );

    // The lockout is per identity: another account is unaffected...
    let (status, _, _) = call(
        &state,
        json_request(
            "POST",
            "/api/auth/login",
            json!({ "username": "someone-else", "password": "wrong-password" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // ...and the right password is refused too while the window holds, so a
    // guesser cannot tell a hit from a miss during a lockout. Clearing on a
    // successful login is covered in `throttle.rs`, where it costs no PBKDF2
    // rounds.
    let (status, _, _) = call(
        &state,
        json_request(
            "POST",
            "/api/auth/login",
            json!({ "username": "ops", "password": "supersecret" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn a_session_minted_by_another_sdk_is_accepted() {
    let storage = temp_storage("auth-interop");
    let state = dashboard_state(&storage, AuthMode::Session);
    let backend: &StorageBackend = &storage;

    // Written straight into the settings store, exactly as an SDK dashboard
    // would — this is the cross-SDK contract the `auth:` keys encode.
    let user = store::create_user(backend, "ops", "supersecret", Role::Admin)
        .expect("storage")
        .expect("valid user");
    let session = store::create_session(backend, &user).expect("create session");

    let credentials = Credentials {
        session: session.token.clone(),
        csrf: session.csrf_token.clone(),
    };
    let (status, _, body) = call(&state, credentials.get("/api/auth/whoami")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["user"]["username"], json!("ops"));
}
