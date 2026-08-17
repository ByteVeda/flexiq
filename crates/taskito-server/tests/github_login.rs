//! The GitHub login flow end to end, against a stub GitHub.
//!
//! GitHub has no `id_token` to verify, so the identity rests entirely on what
//! the API says — which means the parts worth testing are the ones that decide
//! *whose* identity it is: the subject, the verified email, and org membership.

mod support;

use axum::http::{HeaderMap, StatusCode};
use taskito_core::{Storage, StorageBackend};

use support::github_stub::{GitHubStub, Scenario};
use support::{call, dashboard_state_with_oauth, get, temp_storage, TempStorage};
use taskito_server::dashboard::auth::model::Role;
use taskito_server::dashboard::auth::oauth::config::{
    GitHubEndpoints, OAuthConfig, ProviderConfig, ProviderKind,
};
use taskito_server::dashboard::auth::oauth::state::{OAuthState, STATE_PREFIX};
use taskito_server::dashboard::auth::store;
use taskito_server::dashboard::state::SharedState;

const CLIENT_ID: &str = "test-client-id";

/// A dashboard whose GitHub provider points at `stub`.
async fn harness(
    label: &str,
    scenario: Scenario,
    admin_emails: &[&str],
    allowed_orgs: &[&str],
) -> (TempStorage, SharedState, GitHubStub) {
    let storage = temp_storage(label);
    let stub = GitHubStub::start(scenario).await;
    let config = OAuthConfig {
        redirect_base_url: "http://127.0.0.1:8080".into(),
        password_auth_enabled: true,
        admin_emails: admin_emails.iter().map(|email| email.to_string()).collect(),
        providers: vec![ProviderConfig {
            slot: "github".into(),
            label: "GitHub".into(),
            kind: ProviderKind::GitHub,
            client_id: CLIENT_ID.into(),
            client_secret: "test-client-secret".into(),
            discovery_url: None,
            allowed_domains: vec![],
            allowed_orgs: allowed_orgs.iter().map(|org| org.to_string()).collect(),
            github: GitHubEndpoints::rooted_at(&stub.base_url),
        }],
    };
    let state = dashboard_state_with_oauth(&storage, config);
    (storage, state, stub)
}

/// Start a login and hand back the state token the callback needs.
async fn begin_login(state: &SharedState, storage: &StorageBackend) -> String {
    let (status, headers, _) = call(state, get("/api/auth/oauth/start/github")).await;
    assert_eq!(status, StatusCode::FOUND);
    let token = location_of(&headers)
        .split(['?', '&'])
        .find_map(|part| part.strip_prefix("state="))
        .expect("a state token")
        .to_string();
    // Proves the row landed before the callback consumes it.
    let raw = storage
        .get_setting(&format!("{STATE_PREFIX}{token}"))
        .expect("storage")
        .expect("a state row");
    let _: OAuthState = serde_json::from_str(&raw).expect("the row parses");
    token
}

async fn land_callback(state: &SharedState, token: &str) -> (StatusCode, HeaderMap) {
    let (status, headers, _) = call(state, callback_request(token, Some(token))).await;
    (status, headers)
}

/// A callback request presenting `cookie` as the browser's state marker.
fn callback_request(token: &str, cookie: Option<&str>) -> axum::http::Request<axum::body::Body> {
    let mut request = axum::http::Request::builder().uri(format!(
        "/api/auth/oauth/callback/github?code=stub-code&state={token}"
    ));
    if let Some(cookie) = cookie {
        request = request.header("cookie", format!("taskito_oauth_state={cookie}"));
    }
    request
        .body(axum::body::Body::empty())
        .expect("valid request")
}

fn location_of(headers: &HeaderMap) -> String {
    headers
        .get("location")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

fn has_session(headers: &HeaderMap) -> bool {
    headers
        .get_all("set-cookie")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .any(|cookie| cookie.starts_with("flexiq_session="))
}

#[tokio::test]
async fn a_github_login_creates_a_session_keyed_on_the_account_id() {
    let (storage, state, _stub) = harness("github-happy", Scenario::default(), &[], &[]).await;
    let backend: &StorageBackend = &storage;
    let token = begin_login(&state, backend).await;

    let (status, headers) = land_callback(&state, &token).await;
    assert_eq!(status, StatusCode::FOUND);
    assert_eq!(location_of(&headers), "/");
    assert!(has_session(&headers));

    // Keyed by GitHub's numeric id: a login rename must not orphan the account.
    let user = store::get_user(backend, "github:4242")
        .expect("storage")
        .expect("the provider user was created");
    assert_eq!(user.email.as_deref(), Some("ops@example.com"));
    assert_eq!(user.display_name.as_deref(), Some("Octo Cat"));
    assert_eq!(user.role, Role::Viewer);
    assert!(user.is_oauth());
}

#[tokio::test]
async fn the_primary_verified_email_is_what_grants_admin() {
    let (storage, state, _stub) = harness(
        "github-admin",
        Scenario::default(),
        &["ops@example.com"],
        &[],
    )
    .await;
    let backend: &StorageBackend = &storage;
    let token = begin_login(&state, backend).await;
    land_callback(&state, &token).await;

    let user = store::get_user(backend, "github:4242")
        .expect("storage")
        .expect("the user was created");
    assert_eq!(
        user.role,
        Role::Admin,
        "the allowlisted primary email must be the one that counts"
    );
}

#[tokio::test]
async fn an_unverified_email_is_not_an_identity() {
    let (storage, state, _stub) = harness(
        "github-unverified",
        Scenario::default().with_unverified_email(),
        &["ops@example.com"],
        &[],
    )
    .await;
    let backend: &StorageBackend = &storage;
    let token = begin_login(&state, backend).await;
    land_callback(&state, &token).await;

    let user = store::get_user(backend, "github:4242")
        .expect("storage")
        .expect("the user was created");
    assert_eq!(user.role, Role::Viewer, "unverified must never grant admin");
    assert_eq!(
        user.email, None,
        "an unverified address must not be recorded as the account's email"
    );
}

#[tokio::test]
async fn org_membership_is_checked_and_admits_a_member() {
    let (storage, state, stub) = harness(
        "github-org-member",
        Scenario::default().with_membership_status(204),
        &[],
        &["byteveda"],
    )
    .await;
    let backend: &StorageBackend = &storage;
    let token = begin_login(&state, backend).await;

    let (_, headers) = land_callback(&state, &token).await;
    assert!(has_session(&headers));
    assert_eq!(
        stub.membership_checks(),
        vec!["byteveda/octocat".to_string()],
        "membership is checked against the login, not the id"
    );
}

#[tokio::test]
async fn a_non_member_is_denied() {
    let (storage, state, _stub) = harness(
        "github-org-outsider",
        Scenario::default().with_membership_status(404),
        &[],
        &["byteveda", "other-org"],
    )
    .await;
    let backend: &StorageBackend = &storage;
    let token = begin_login(&state, backend).await;

    let (status, headers) = land_callback(&state, &token).await;
    assert_eq!(status, StatusCode::FOUND);
    assert_eq!(location_of(&headers), "/login?error=oauth_denied");
    assert!(!has_session(&headers));
    assert_eq!(
        store::count_users(backend).expect("storage"),
        0,
        "a denied login must not leave a user behind"
    );
}

#[tokio::test]
async fn the_org_check_failing_is_not_treated_as_a_denial() {
    let (storage, state, _stub) = harness(
        "github-org-error",
        Scenario::default().with_membership_status(500),
        &[],
        &["byteveda"],
    )
    .await;
    let backend: &StorageBackend = &storage;
    let token = begin_login(&state, backend).await;

    let (_, headers) = land_callback(&state, &token).await;
    assert_eq!(
        location_of(&headers),
        "/login?error=oauth_failed",
        "a broken check is a failure, not a verdict about the user"
    );
    assert!(!has_session(&headers));
    assert_eq!(store::count_users(backend).expect("storage"), 0);
}

#[tokio::test]
async fn a_rejected_or_unusable_exchange_creates_nothing() {
    for (label, scenario) in [
        ("github-token-error", Scenario::default().with_token_error()),
        (
            "github-no-token",
            Scenario::default().without_access_token(),
        ),
        ("github-bad-user", Scenario::default().with_unusable_user()),
    ] {
        let (storage, state, _stub) = harness(label, scenario, &[], &[]).await;
        let backend: &StorageBackend = &storage;
        let token = begin_login(&state, backend).await;

        let (status, headers) = land_callback(&state, &token).await;
        assert_eq!(status, StatusCode::FOUND, "{label}");
        assert_eq!(
            location_of(&headers),
            "/login?error=oauth_failed",
            "{label}"
        );
        assert!(!has_session(&headers), "{label}");
        assert_eq!(store::count_users(backend).expect("storage"), 0, "{label}");
    }
}

#[tokio::test]
async fn a_returning_account_is_refreshed_not_duplicated() {
    let (storage, state, _stub) = harness("github-returning", Scenario::default(), &[], &[]).await;
    let backend: &StorageBackend = &storage;

    for _ in 0..2 {
        let token = begin_login(&state, backend).await;
        let (_, headers) = land_callback(&state, &token).await;
        assert!(has_session(&headers));
    }

    assert_eq!(
        store::count_users(backend).expect("storage"),
        1,
        "the same GitHub account is one user, however often it logs in"
    );
}
