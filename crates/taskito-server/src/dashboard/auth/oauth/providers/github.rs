//! GitHub's OAuth2 flow.
//!
//! GitHub does not implement OIDC, so there is no `id_token` to verify: the
//! identity comes from the API, read with the freshly issued access token.
//! PKCE is still used — GitHub supports it — and the org allowlist is enforced
//! here rather than afterwards, because it needs that same token.

use serde_json::Value;

use crate::dashboard::auth::oauth::config::ProviderConfig;
use crate::dashboard::auth::oauth::providers::{Identity, OAuthError, OAuthRuntime};

const AUTHORIZE_URL: &str = "https://github.com/login/oauth/authorize";
const TOKEN_URL: &str = "https://github.com/login/oauth/access_token";
const API_BASE: &str = "https://api.github.com";

/// Scopes needed to read the account and its verified email.
const BASE_SCOPE: &str = "read:user user:email";

/// Build GitHub's authorize URL. The OIDC nonce has no place here.
pub fn authorization_url(
    provider: &ProviderConfig,
    state: &str,
    code_challenge: &str,
    redirect_uri: &str,
) -> String {
    // `read:org` is only requested when an org allowlist exists, so a
    // deployment without one never asks for more access than it needs.
    let scope = if provider.allowed_orgs.is_empty() {
        BASE_SCOPE.to_string()
    } else {
        format!("{BASE_SCOPE} read:org")
    };
    let params = [
        ("client_id", provider.client_id.as_str()),
        ("redirect_uri", redirect_uri),
        ("scope", &scope),
        ("state", state),
        ("code_challenge", code_challenge),
        ("code_challenge_method", "S256"),
        ("allow_signup", "false"),
    ];
    format!(
        "{AUTHORIZE_URL}?{}",
        serde_urlencoded::to_string(params).unwrap_or_default()
    )
}

/// Exchange the code, read the account, and enforce the org allowlist.
pub async fn exchange_code(
    runtime: &OAuthRuntime,
    provider: &ProviderConfig,
    code: &str,
    code_verifier: &str,
    redirect_uri: &str,
) -> Result<Identity, OAuthError> {
    let access_token = fetch_token(runtime, provider, code, code_verifier, redirect_uri).await?;

    let user = api_get(runtime, "/user", &access_token).await?;
    let subject = user
        .get("id")
        .and_then(|id| id.as_i64().map(|id| id.to_string()))
        .ok_or_else(|| OAuthError::IdentityFetch("GitHub /user is missing 'id'".into()))?;
    let login = user
        .get("login")
        .and_then(Value::as_str)
        .filter(|login| !login.is_empty())
        .ok_or_else(|| OAuthError::IdentityFetch("GitHub /user is missing 'login'".into()))?;

    let (email, email_verified) = primary_email(runtime, &access_token).await;
    verify_org_membership(runtime, provider, login, &access_token).await?;

    Ok(Identity {
        slot: provider.slot.clone(),
        subject,
        email,
        email_verified,
        name: user
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
            .map(str::to_string)
            .or_else(|| Some(login.to_string())),
    })
}

async fn fetch_token(
    runtime: &OAuthRuntime,
    provider: &ProviderConfig,
    code: &str,
    code_verifier: &str,
    redirect_uri: &str,
) -> Result<String, OAuthError> {
    let form = [
        ("grant_type", "authorization_code"),
        ("code", code),
        ("code_verifier", code_verifier),
        ("redirect_uri", redirect_uri),
        ("client_id", &provider.client_id),
        ("client_secret", &provider.client_secret),
    ];
    let response = runtime
        .http()
        .post(TOKEN_URL)
        // GitHub answers form-encoded unless asked otherwise.
        .header("Accept", "application/json")
        .form(&form)
        .send()
        .await
        .map_err(|error| OAuthError::IdentityFetch(format!("token exchange failed: {error}")))?;
    if !response.status().is_success() {
        return Err(OAuthError::IdentityFetch(format!(
            "token endpoint returned {}",
            response.status()
        )));
    }

    let token: Value = response.json().await.map_err(|error| {
        OAuthError::IdentityFetch(format!("token response unreadable: {error}"))
    })?;
    // GitHub reports failures with a 200 and an `error` field.
    if let Some(error) = token.get("error").and_then(Value::as_str) {
        return Err(OAuthError::IdentityFetch(format!(
            "token exchange rejected: {error}"
        )));
    }
    token
        .get("access_token")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| OAuthError::IdentityFetch("no access_token in token response".into()))
}

/// The account's primary **verified** email, or none.
///
/// An unverified address is never returned: every downstream decision — the
/// domain allowlist, the admin list — treats an email as an identity claim.
async fn primary_email(runtime: &OAuthRuntime, access_token: &str) -> (Option<String>, bool) {
    let Ok(emails) = api_get(runtime, "/user/emails", access_token).await else {
        return (None, false);
    };
    let found = emails.as_array().and_then(|entries| {
        entries.iter().find(|entry| {
            entry
                .get("primary")
                .and_then(Value::as_bool)
                .unwrap_or(false)
                && entry
                    .get("verified")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
        })
    });
    match found
        .and_then(|entry| entry.get("email"))
        .and_then(Value::as_str)
    {
        Some(email) => (Some(email.to_string()), true),
        None => (None, false),
    }
}

async fn verify_org_membership(
    runtime: &OAuthRuntime,
    provider: &ProviderConfig,
    login: &str,
    access_token: &str,
) -> Result<(), OAuthError> {
    if provider.allowed_orgs.is_empty() {
        return Ok(());
    }
    for org in &provider.allowed_orgs {
        let response = runtime
            .http()
            .get(format!("{API_BASE}/orgs/{org}/members/{login}"))
            .headers(api_headers(access_token))
            .send()
            .await
            .map_err(|error| {
                OAuthError::IdentityFetch(format!("org membership check failed: {error}"))
            })?;
        match response.status().as_u16() {
            // 204 is a member; 302/404 mean "not visible to this token", which
            // is a no for this org but not an error.
            204 => return Ok(()),
            302 | 404 => continue,
            status => {
                return Err(OAuthError::IdentityFetch(format!(
                    "org membership check returned {status}"
                )))
            }
        }
    }
    Err(OAuthError::Denied(format!(
        "user is not a member of any allowed GitHub org ({})",
        provider.allowed_orgs.join(", ")
    )))
}

async fn api_get(
    runtime: &OAuthRuntime,
    path: &str,
    access_token: &str,
) -> Result<Value, OAuthError> {
    let response = runtime
        .http()
        .get(format!("{API_BASE}{path}"))
        .headers(api_headers(access_token))
        .send()
        .await
        .map_err(|error| OAuthError::IdentityFetch(format!("GET {path} failed: {error}")))?;
    if !response.status().is_success() {
        return Err(OAuthError::IdentityFetch(format!(
            "GET {path} returned {}",
            response.status()
        )));
    }
    response
        .json()
        .await
        .map_err(|error| OAuthError::IdentityFetch(format!("GET {path} was unreadable: {error}")))
}

fn api_headers(access_token: &str) -> reqwest::header::HeaderMap {
    let mut headers = reqwest::header::HeaderMap::new();
    for (name, value) in [
        ("authorization", format!("Bearer {access_token}")),
        ("accept", "application/vnd.github+json".to_string()),
        ("x-github-api-version", "2022-11-28".to_string()),
        // GitHub rejects API requests without one.
        ("user-agent", "taskito-server".to_string()),
    ] {
        if let (Ok(name), Ok(value)) = (
            reqwest::header::HeaderName::from_bytes(name.as_bytes()),
            reqwest::header::HeaderValue::from_str(&value),
        ) {
            headers.insert(name, value);
        }
    }
    headers
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dashboard::auth::oauth::config::ProviderKind;

    fn provider(allowed_orgs: &[&str]) -> ProviderConfig {
        ProviderConfig {
            slot: "github".into(),
            label: "GitHub".into(),
            kind: ProviderKind::GitHub,
            client_id: "client-id".into(),
            client_secret: "secret".into(),
            discovery_url: None,
            allowed_domains: vec![],
            allowed_orgs: allowed_orgs.iter().map(|org| org.to_string()).collect(),
        }
    }

    #[test]
    fn the_authorize_url_carries_pkce_and_state() {
        let url = authorization_url(
            &provider(&[]),
            "state-token",
            "challenge",
            "https://ops.example.com/api/auth/oauth/callback/github",
        );
        assert!(url.starts_with(AUTHORIZE_URL));
        assert!(url.contains("code_challenge=challenge"));
        assert!(url.contains("code_challenge_method=S256"));
        assert!(url.contains("state=state-token"));
        assert!(url.contains("allow_signup=false"));
        // The client secret must never appear in a browser-visible URL.
        assert!(!url.contains("secret"));
    }

    #[test]
    fn org_scope_is_only_requested_when_an_allowlist_exists() {
        let without = authorization_url(&provider(&[]), "s", "c", "https://ops.example.com/cb");
        assert!(!without.contains("read%3Aorg"));

        let with = authorization_url(
            &provider(&["byteveda"]),
            "s",
            "c",
            "https://ops.example.com/cb",
        );
        assert!(with.contains("read%3Aorg"));
    }
}
