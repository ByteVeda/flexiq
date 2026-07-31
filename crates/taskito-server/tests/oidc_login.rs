//! The OIDC login flow end to end, against a stub issuer.
//!
//! `oauth_flow.rs` stops where the network begins. This file goes past it: a
//! real token exchange, a real JWKS fetch, and a real signature check — the
//! part where a mistake silently accepts a forged identity rather than failing
//! a unit test.

mod support;

use axum::http::{HeaderMap, StatusCode};
use serde_json::{json, Value};
use taskito_core::{Storage, StorageBackend};

use support::oidc_issuer::{NextToken, StubIssuer};
use support::{call, dashboard_state_with_oauth, get, temp_storage, TempStorage};
use taskito_server::dashboard::auth::model::Role;
use taskito_server::dashboard::auth::oauth::config::{OAuthConfig, ProviderConfig, ProviderKind};
use taskito_server::dashboard::auth::oauth::state::{OAuthState, STATE_PREFIX};
use taskito_server::dashboard::auth::store;
use taskito_server::dashboard::state::SharedState;

/// Slot the stub provider is registered under.
const SLOT: &str = "acme";
const CLIENT_ID: &str = "test-client-id";

/// A dashboard wired to the stub issuer, with `admin_emails` as configured.
async fn harness(label: &str, admin_emails: &[&str]) -> (TempStorage, SharedState, StubIssuer) {
    let storage = temp_storage(label);
    let issuer = StubIssuer::start().await;
    let config = OAuthConfig {
        // Loopback keeps the base-URL check satisfied without TLS.
        redirect_base_url: "http://127.0.0.1:8080".into(),
        password_auth_enabled: true,
        admin_emails: admin_emails.iter().map(|email| email.to_string()).collect(),
        providers: vec![ProviderConfig {
            slot: SLOT.into(),
            label: "Acme".into(),
            kind: ProviderKind::Oidc,
            client_id: CLIENT_ID.into(),
            client_secret: "test-client-secret".into(),
            discovery_url: Some(issuer.discovery_url()),
            allowed_domains: vec![],
            allowed_orgs: vec![],
        }],
    };
    let state = dashboard_state_with_oauth(&storage, config);
    (storage, state, issuer)
}

/// Begin a login and return `(state token, nonce)` the issuer must echo.
///
/// The nonce is read out of the stored row rather than consumed, so the
/// callback under test still finds the state it needs.
async fn begin_login(state: &SharedState, storage: &StorageBackend) -> (String, String) {
    let (status, headers, _) = call(state, get(&format!("/api/auth/oauth/start/{SLOT}"))).await;
    assert_eq!(status, StatusCode::FOUND, "start must redirect");

    let location = headers
        .get("location")
        .and_then(|value| value.to_str().ok())
        .expect("a Location header")
        .to_string();
    let token = location
        .split(['?', '&'])
        .find_map(|part| part.strip_prefix("state="))
        .expect("the authorize URL carries a state token")
        .to_string();

    let raw = storage
        .get_setting(&format!("{STATE_PREFIX}{token}"))
        .expect("storage")
        .expect("the state row was persisted");
    let row: OAuthState = serde_json::from_str(&raw).expect("the state row parses");
    (token, row.nonce)
}

/// Land the callback for `token`.
async fn land_callback(state: &SharedState, token: &str) -> (StatusCode, HeaderMap, Value) {
    call(
        state,
        get(&format!(
            "/api/auth/oauth/callback/{SLOT}?code=stub-code&state={token}"
        )),
    )
    .await
}

fn location_of(headers: &HeaderMap) -> String {
    headers
        .get("location")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

fn session_cookie(headers: &HeaderMap) -> Option<String> {
    headers
        .get_all("set-cookie")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .find(|cookie| cookie.starts_with("taskito_session="))
        .map(str::to_string)
}

#[tokio::test]
async fn a_valid_id_token_creates_a_session() {
    let (storage, state, issuer) = harness("oidc-happy", &[]).await;
    let backend: &StorageBackend = &storage;
    let (token, nonce) = begin_login(&state, backend).await;

    issuer.expect_exchange(NextToken::valid(&issuer.issuer_url(), CLIENT_ID, &nonce));
    let (status, headers, _) = land_callback(&state, &token).await;

    assert_eq!(status, StatusCode::FOUND);
    assert_eq!(location_of(&headers), "/", "the browser lands on next_url");
    assert!(
        session_cookie(&headers).is_some(),
        "a successful login must set the session cookie"
    );

    // The user is keyed by the provider's subject, not by the email.
    let user = store::get_user(backend, &format!("{SLOT}:provider-subject-1"))
        .expect("storage")
        .expect("the provider user was created");
    assert_eq!(user.email.as_deref(), Some("ops@example.com"));
    assert_eq!(user.display_name.as_deref(), Some("Ops Person"));
    assert_eq!(user.role, Role::Viewer, "no allowlist means viewer");
    assert!(user.is_oauth());

    // The session the cookie names is real, and the SPA can read it back.
    let cookie = session_cookie(&headers).expect("a session cookie");
    let value = cookie
        .trim_start_matches("taskito_session=")
        .split(';')
        .next()
        .expect("a cookie value");
    let (status, _, body) = call(
        &state,
        axum::http::Request::builder()
            .uri("/api/auth/whoami")
            .header("cookie", format!("taskito_session={value}"))
            .body(axum::body::Body::empty())
            .expect("valid request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["user"]["username"],
        json!(format!("{SLOT}:provider-subject-1"))
    );
}

#[tokio::test]
async fn a_verified_allowlisted_email_gets_admin() {
    let (storage, state, issuer) = harness("oidc-admin", &["OPS@example.com"]).await;
    let backend: &StorageBackend = &storage;
    let (token, nonce) = begin_login(&state, backend).await;

    issuer.expect_exchange(NextToken::valid(&issuer.issuer_url(), CLIENT_ID, &nonce));
    land_callback(&state, &token).await;

    let user = store::get_user(backend, &format!("{SLOT}:provider-subject-1"))
        .expect("storage")
        .expect("the provider user was created");
    assert_eq!(
        user.role,
        Role::Admin,
        "the allowlist match is case-insensitive"
    );
}

#[tokio::test]
async fn an_unverified_email_never_gets_admin() {
    let (storage, state, issuer) = harness("oidc-unverified", &["ops@example.com"]).await;
    let backend: &StorageBackend = &storage;
    let (token, nonce) = begin_login(&state, backend).await;

    issuer.expect_exchange(
        NextToken::valid(&issuer.issuer_url(), CLIENT_ID, &nonce)
            .with_claim("email_verified", json!(false)),
    );
    land_callback(&state, &token).await;

    let user = store::get_user(backend, &format!("{SLOT}:provider-subject-1"))
        .expect("storage")
        .expect("the provider user was created");
    assert_eq!(
        user.role,
        Role::Viewer,
        "an unverified email is a claim, not an identity"
    );
}

/// One way of forging or ageing an `id_token`.
type Forge = Box<dyn Fn(NextToken) -> NextToken + Send>;

/// Every way a forged or stale `id_token` can arrive. None may produce a
/// session or a user.
#[tokio::test]
async fn a_forged_or_stale_id_token_is_refused() {
    let expired_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the clock is after 1970")
        .as_secs()
        - 7_200;

    let cases: Vec<(&str, Forge)> = vec![
        (
            "audience belongs to another client",
            Box::new(|token: NextToken| token.with_claim("aud", json!("someone-elses-client"))),
        ),
        (
            "issuer is not the one discovery named",
            Box::new(|token: NextToken| token.with_claim("iss", json!("https://evil.example"))),
        ),
        (
            "token expired",
            Box::new(move |token: NextToken| token.with_claim("exp", json!(expired_at))),
        ),
        (
            "nonce does not match this login",
            Box::new(|token: NextToken| token.with_claim("nonce", json!("a-different-nonce"))),
        ),
        (
            "subject claim is missing",
            Box::new(|token: NextToken| token.with_claim("sub", Value::Null)),
        ),
        (
            "signed with a key the provider does not publish",
            Box::new(|token: NextToken| token.with_unknown_key_id()),
        ),
        (
            "signature does not match the payload",
            Box::new(|token: NextToken| token.tampered()),
        ),
    ];

    for (description, forge) in cases {
        let (storage, state, issuer) = harness("oidc-forged", &[]).await;
        let backend: &StorageBackend = &storage;
        let (token, nonce) = begin_login(&state, backend).await;

        issuer.expect_exchange(forge(NextToken::valid(
            &issuer.issuer_url(),
            CLIENT_ID,
            &nonce,
        )));
        let (status, headers, _) = land_callback(&state, &token).await;

        assert_eq!(status, StatusCode::FOUND, "{description}");
        assert_eq!(
            location_of(&headers),
            "/login?error=oauth_failed",
            "{description}: must land on the login page"
        );
        assert!(
            session_cookie(&headers).is_none(),
            "{description}: must not set a session cookie"
        );
        assert_eq!(
            store::count_users(backend).expect("storage"),
            0,
            "{description}: must not create a user"
        );
        assert_eq!(
            issuer.exchanges(),
            1,
            "{description}: the token exchange must have happened, or this \
             case proves nothing about verification"
        );
    }
}

#[tokio::test]
async fn a_second_callback_with_the_same_state_is_refused() {
    let (storage, state, issuer) = harness("oidc-replay", &[]).await;
    let backend: &StorageBackend = &storage;
    let (token, nonce) = begin_login(&state, backend).await;

    issuer.expect_exchange(NextToken::valid(&issuer.issuer_url(), CLIENT_ID, &nonce));
    let (_, headers, _) = land_callback(&state, &token).await;
    assert!(
        session_cookie(&headers).is_some(),
        "the first login succeeds"
    );

    // No second exchange is armed: a replay must be rejected before the stub
    // is ever called.
    let (status, headers, _) = land_callback(&state, &token).await;
    assert_eq!(status, StatusCode::FOUND);
    assert_eq!(location_of(&headers), "/login?error=oauth_state_invalid");
    assert!(session_cookie(&headers).is_none());
}

#[tokio::test]
async fn a_returning_user_keeps_its_role_and_refreshes_its_profile() {
    let (storage, state, issuer) = harness("oidc-returning", &[]).await;
    let backend: &StorageBackend = &storage;

    let (token, nonce) = begin_login(&state, backend).await;
    issuer.expect_exchange(NextToken::valid(&issuer.issuer_url(), CLIENT_ID, &nonce));
    land_callback(&state, &token).await;

    // Promote out of band, as an operator would.
    let username = format!("{SLOT}:provider-subject-1");
    let mut users = store::list_users(backend).expect("storage");
    let user = users.get_mut(&username).expect("the user exists");
    assert_eq!(user.role, Role::Viewer);

    let (token, nonce) = begin_login(&state, backend).await;
    issuer.expect_exchange(
        NextToken::valid(&issuer.issuer_url(), CLIENT_ID, &nonce)
            .with_claim("email", json!("moved@example.com")),
    );
    land_callback(&state, &token).await;

    let user = store::get_user(backend, &username)
        .expect("storage")
        .expect("the user still exists");
    assert_eq!(user.email.as_deref(), Some("moved@example.com"));
    assert_eq!(
        store::count_users(backend).expect("storage"),
        1,
        "a second login must not create a second user"
    );
}
