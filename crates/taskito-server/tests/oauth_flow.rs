//! Provider login, up to the point where a real identity provider is needed.
//!
//! Everything here is network-free: GitHub's authorize URL is built locally,
//! and every callback path under test is rejected before any token exchange —
//! which is exactly where the security-relevant checks live.

mod support;

use axum::http::StatusCode;
use serde_json::json;
use taskito_core::{Storage, StorageBackend};

use support::{call, dashboard_state, dashboard_state_with_oauth, get, temp_storage};
use taskito_server::config::dashboard::AuthMode;
use taskito_server::dashboard::auth::oauth::config::{
    GitHubEndpoints, OAuthConfig, ProviderConfig, ProviderKind,
};
use taskito_server::dashboard::auth::oauth::state;

fn github_config() -> OAuthConfig {
    OAuthConfig {
        redirect_base_url: "https://ops.example.com".into(),
        password_auth_enabled: true,
        admin_emails: vec!["ops@example.com".into()],
        providers: vec![ProviderConfig {
            slot: "github".into(),
            label: "GitHub".into(),
            kind: ProviderKind::GitHub,
            client_id: "client-id".into(),
            client_secret: "client-secret".into(),
            discovery_url: None,
            allowed_domains: vec![],
            allowed_orgs: vec![],
            github: GitHubEndpoints::default(),
        }],
    }
}

/// The `Location` of a redirect response.
fn location(headers: &axum::http::HeaderMap) -> String {
    headers
        .get("location")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

/// The `state` parameter carried in a redirect URL.
fn state_token(url: &str) -> String {
    url.split(['?', '&'])
        .find_map(|part| part.strip_prefix("state="))
        .expect("the authorize URL carries a state token")
        .to_string()
}

#[tokio::test]
async fn providers_are_listed_as_empty_when_none_are_configured() {
    let storage = temp_storage("oauth-none");
    let state = dashboard_state(&storage, AuthMode::Session);

    let (status, _, body) = call(&state, get("/api/auth/providers")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["password_enabled"], json!(true));
    assert_eq!(body["providers"], json!([]));

    // Starting a flow that was never configured is a 404, not a redirect.
    let (status, _, body) = call(&state, get("/api/auth/oauth/start/github")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], json!("oauth_not_configured"));
}

#[tokio::test]
async fn a_configured_provider_is_advertised_without_secrets() {
    let storage = temp_storage("oauth-listing");
    let state = dashboard_state_with_oauth(&storage, github_config());

    let (status, _, body) = call(&state, get("/api/auth/providers")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["providers"],
        json!([{ "slot": "github", "label": "GitHub", "type": "github" }])
    );
    assert!(!body.to_string().contains("client-secret"));
}

#[tokio::test]
async fn starting_a_flow_redirects_with_state_and_pkce() {
    let storage = temp_storage("oauth-start");
    let state = dashboard_state_with_oauth(&storage, github_config());

    let (status, headers, _) = call(&state, get("/api/auth/oauth/start/github")).await;
    assert_eq!(status, StatusCode::FOUND);

    let url = location(&headers);
    assert!(url.starts_with("https://github.com/login/oauth/authorize?"));
    assert!(url.contains("code_challenge_method=S256"));
    assert!(
        !url.contains("client-secret"),
        "the secret must never reach the browser"
    );
    assert_eq!(
        headers.get("cache-control").and_then(|v| v.to_str().ok()),
        Some("no-store")
    );

    // The state row exists server-side, and its verifier matches the challenge
    // that was sent.
    let backend: &StorageBackend = &storage;
    let row = state::consume(backend, &state_token(&url))
        .expect("storage")
        .expect("a state row was created");
    assert_eq!(row.slot, "github");
    assert_eq!(row.next_url, "/");
    assert!(url.contains(&format!(
        "code_challenge={}",
        state::s256_challenge(&row.code_verifier)
    )));
}

#[tokio::test]
async fn an_off_origin_next_target_is_replaced_with_the_root() {
    let storage = temp_storage("oauth-next");
    let state = dashboard_state_with_oauth(&storage, github_config());
    let backend: &StorageBackend = &storage;

    let (_, headers, _) = call(
        &state,
        get("/api/auth/oauth/start/github?next=https%3A%2F%2Fevil.example%2Fsteal"),
    )
    .await;
    let row = state::consume(backend, &state_token(&location(&headers)))
        .expect("storage")
        .expect("a state row");
    assert_eq!(row.next_url, "/", "an absolute URL must not survive");

    let (_, headers, _) = call(
        &state,
        get("/api/auth/oauth/start/github?next=%2F%2Fevil.example%2Fsteal"),
    )
    .await;
    let row = state::consume(backend, &state_token(&location(&headers)))
        .expect("storage")
        .expect("a state row");
    assert_eq!(
        row.next_url, "/",
        "a protocol-relative URL must not survive"
    );

    let (_, headers, _) = call(&state, get("/api/auth/oauth/start/github?next=%2Fjobs")).await;
    let row = state::consume(backend, &state_token(&location(&headers)))
        .expect("storage")
        .expect("a state row");
    assert_eq!(row.next_url, "/jobs", "a same-origin path is kept");
}

#[tokio::test]
async fn a_callback_without_valid_state_lands_on_the_login_page() {
    let storage = temp_storage("oauth-callback-state");
    let state = dashboard_state_with_oauth(&storage, github_config());

    // No parameters at all.
    let (status, headers, _) = call(&state, get("/api/auth/oauth/callback/github")).await;
    assert_eq!(status, StatusCode::FOUND);
    assert_eq!(location(&headers), "/login?error=oauth_state_invalid");

    // A state token nobody issued.
    let (_, headers, _) = call(
        &state,
        get("/api/auth/oauth/callback/github?code=abc&state=forged"),
    )
    .await;
    assert_eq!(location(&headers), "/login?error=oauth_state_invalid");

    // The provider itself reported a failure.
    let (_, headers, _) = call(
        &state,
        get("/api/auth/oauth/callback/github?error=access_denied"),
    )
    .await;
    assert_eq!(location(&headers), "/login?error=oauth_failed");
}

#[tokio::test]
async fn a_state_row_cannot_be_replayed_or_redeemed_on_another_slot() {
    let storage = temp_storage("oauth-replay");
    let state = dashboard_state_with_oauth(&storage, github_config());
    let backend: &StorageBackend = &storage;

    // A state minted for a different slot must not be accepted here.
    let (token, _) = state::create(backend, "acme-okta", "/").expect("create state");
    let (_, headers, _) = call(
        &state,
        get(&format!(
            "/api/auth/oauth/callback/github?code=abc&state={token}"
        )),
    )
    .await;
    assert_eq!(location(&headers), "/login?error=oauth_state_invalid");

    // Consuming it — even on a rejected path — must burn the row.
    assert!(
        state::consume(backend, &token).expect("storage").is_none(),
        "a state row is single-use even when the callback is rejected"
    );
}

#[tokio::test]
async fn the_oauth_endpoints_stay_public_before_setup() {
    let storage = temp_storage("oauth-public");
    let state = dashboard_state_with_oauth(&storage, github_config());

    // With no users yet, ordinary API routes report setup_required; the login
    // endpoints must not, or a provider-only deployment could never sign in.
    let (status, _, _) = call(&state, get("/api/jobs")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

    let (status, _, _) = call(&state, get("/api/auth/providers")).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, _) = call(&state, get("/api/auth/oauth/start/github")).await;
    assert_eq!(status, StatusCode::FOUND);
}

#[tokio::test]
async fn expired_state_rows_do_not_accumulate() {
    let storage = temp_storage("oauth-prune");
    let backend: &StorageBackend = &storage;

    let (token, _) = state::create(backend, "github", "/").expect("create state");
    // Rewrite it as already expired, as an abandoned flow would be.
    let key = format!("{}{token}", state::STATE_PREFIX);
    let expired = json!({
        "slot": "github",
        "nonce": "n",
        "code_verifier": "v",
        "next_url": "/",
        "created_at": 0,
        "expires_at": 1,
    });
    backend
        .set_setting(&key, &expired.to_string())
        .expect("rewrite the row");

    assert_eq!(state::prune_expired(backend).expect("prune"), 1);
    assert!(backend.get_setting(&key).expect("storage").is_none());
}
