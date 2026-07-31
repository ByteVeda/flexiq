//! The OIDC flow, shared by Google and any generic issuer.
//!
//! The identity comes from a signed `id_token`, verified against the issuer's
//! published JWKS. Signature, issuer, audience, expiry, and nonce are all
//! checked — dropping any one of them turns the token into an unauthenticated
//! claim anyone could mint.

use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{decode, decode_header, DecodingKey, Validation};
use serde::Deserialize;
use serde_json::Value;

use crate::dashboard::auth::oauth::config::ProviderConfig;
use crate::dashboard::auth::oauth::providers::{recover, Identity, OAuthError, OAuthRuntime};

/// Tolerance for clock skew between this host and the issuer.
const CLOCK_SKEW_SECONDS: u64 = 60;

/// The subset of a discovery document the flow uses.
#[derive(Debug, Clone, Deserialize)]
pub struct Discovery {
    /// Where the browser is sent to authenticate.
    pub authorization_endpoint: String,
    /// Where the code is exchanged.
    pub token_endpoint: String,
    /// Where the signing keys are published.
    pub jwks_uri: String,
    /// Expected `iss` claim.
    pub issuer: String,
}

/// Build the provider's authorize URL.
pub async fn authorization_url(
    runtime: &OAuthRuntime,
    provider: &ProviderConfig,
    state: &str,
    nonce: &str,
    code_challenge: &str,
    redirect_uri: &str,
) -> Result<String, OAuthError> {
    let discovery = discovery(runtime, provider).await?;
    let mut params = vec![
        ("response_type", "code".to_string()),
        ("client_id", provider.client_id.clone()),
        ("redirect_uri", redirect_uri.to_string()),
        ("scope", "openid email profile".to_string()),
        ("state", state.to_string()),
        ("nonce", nonce.to_string()),
        ("code_challenge", code_challenge.to_string()),
        ("code_challenge_method", "S256".to_string()),
    ];
    if provider.kind == crate::dashboard::auth::oauth::config::ProviderKind::Google {
        params.push(("prompt", "select_account".to_string()));
        // With exactly one allowed domain, Google can pre-select the right
        // account. A hint only — enforcement is still the allowlist check.
        if let [domain] = provider.allowed_domains.as_slice() {
            params.push(("hd", domain.clone()));
        }
    }
    Ok(format!(
        "{}?{}",
        discovery.authorization_endpoint,
        serde_urlencoded::to_string(&params).unwrap_or_default()
    ))
}

/// Exchange the code and verify the returned `id_token`.
pub async fn exchange_code(
    runtime: &OAuthRuntime,
    provider: &ProviderConfig,
    code: &str,
    code_verifier: &str,
    redirect_uri: &str,
    expected_nonce: &str,
) -> Result<Identity, OAuthError> {
    let discovery = discovery(runtime, provider).await?;

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
        .post(&discovery.token_endpoint)
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
    let id_token = token
        .get("id_token")
        .and_then(Value::as_str)
        .ok_or_else(|| OAuthError::IdentityFetch("no id_token in token response".into()))?;

    let claims = verify_id_token(runtime, provider, &discovery, id_token).await?;
    if claims.nonce.as_deref() != Some(expected_nonce) {
        return Err(OAuthError::IdentityFetch("id_token nonce mismatch".into()));
    }
    let subject = claims
        .sub
        .filter(|sub| !sub.is_empty())
        .ok_or_else(|| OAuthError::IdentityFetch("id_token missing 'sub' claim".into()))?;

    Ok(Identity {
        slot: provider.slot.clone(),
        subject,
        email: claims.email,
        email_verified: claims.email_verified.as_ref().is_some_and(truthy),
        name: claims.name,
    })
}

/// Claims the flow reads. Signature, `iss`, `aud`, and `exp` are enforced by
/// the decoder itself.
#[derive(Debug, Deserialize)]
struct Claims {
    sub: Option<String>,
    #[serde(default)]
    email: Option<String>,
    // Some issuers send this as the string "true" rather than a boolean.
    #[serde(default)]
    email_verified: Option<Value>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    nonce: Option<String>,
}

fn truthy(value: &Value) -> bool {
    match value {
        Value::Bool(flag) => *flag,
        Value::String(text) => text.eq_ignore_ascii_case("true"),
        _ => false,
    }
}

async fn verify_id_token(
    runtime: &OAuthRuntime,
    provider: &ProviderConfig,
    discovery: &Discovery,
    id_token: &str,
) -> Result<Claims, OAuthError> {
    let header = decode_header(id_token)
        .map_err(|error| OAuthError::IdentityFetch(format!("id_token is malformed: {error}")))?;
    // A token that names a `kid` must be verified with *that* key: falling back
    // to some other key would accept a token signed by a retired or unrelated
    // one. Only a token naming no `kid` may use a single-key set.
    let mut keys = jwks(runtime, provider, &discovery.jwks_uri, false).await?;
    if select_key(&keys, header.kid.as_deref()).is_none() {
        // Issuers rotate on a schedule, so a stale cache is normal operation
        // rather than an attack. Refetch once — and only once, so a token
        // naming a `kid` that never existed cannot drive a fetch per request.
        keys = jwks(runtime, provider, &discovery.jwks_uri, true).await?;
    }

    let jwk = select_key(&keys, header.kid.as_deref()).ok_or_else(|| {
        OAuthError::IdentityFetch("no signing key matches the id_token's kid".into())
    })?;
    let key = DecodingKey::from_jwk(jwk)
        .map_err(|error| OAuthError::IdentityFetch(format!("unusable signing key: {error}")))?;

    let mut validation = Validation::new(header.alg);
    validation.set_audience(&[&provider.client_id]);
    validation.set_issuer(&[&discovery.issuer]);
    validation.leeway = CLOCK_SKEW_SECONDS;
    validation.validate_exp = true;

    decode::<Claims>(id_token, &key, &validation)
        .map(|data| data.claims)
        .map_err(|error| OAuthError::IdentityFetch(format!("id_token validation failed: {error}")))
}

/// The provider's discovery document, cached after the first fetch.
async fn discovery(
    runtime: &OAuthRuntime,
    provider: &ProviderConfig,
) -> Result<Discovery, OAuthError> {
    if let Some(cached) = runtime
        .discovery
        .lock()
        .unwrap_or_else(recover)
        .get(&provider.slot)
    {
        return Ok(cached.clone());
    }

    let url = provider.discovery_url.as_deref().ok_or_else(|| {
        OAuthError::IdentityFetch(format!("provider '{}' has no discovery URL", provider.slot))
    })?;
    let document: Discovery = fetch_json(runtime, url, "discovery document").await?;
    runtime
        .discovery
        .lock()
        .unwrap_or_else(recover)
        .insert(provider.slot.clone(), document.clone());
    Ok(document)
}

/// The provider's signing keys, cached after the first fetch.
/// The key a token should be verified with, if the set holds it.
fn select_key<'a>(keys: &'a JwkSet, kid: Option<&str>) -> Option<&'a jsonwebtoken::jwk::Jwk> {
    match kid {
        Some(kid) => keys.find(kid),
        None => keys.keys.first().filter(|_| keys.keys.len() == 1),
    }
}

/// `refresh` bypasses the cache, for the one retry a `kid` miss earns.
async fn jwks(
    runtime: &OAuthRuntime,
    provider: &ProviderConfig,
    jwks_uri: &str,
    refresh: bool,
) -> Result<JwkSet, OAuthError> {
    if !refresh {
        if let Some(cached) = runtime
            .jwks
            .lock()
            .unwrap_or_else(recover)
            .get(&provider.slot)
        {
            return Ok(cached.clone());
        }
    }
    let keys: JwkSet = fetch_json(runtime, jwks_uri, "JWKS").await?;
    runtime
        .jwks
        .lock()
        .unwrap_or_else(recover)
        .insert(provider.slot.clone(), keys.clone());
    Ok(keys)
}

async fn fetch_json<T: serde::de::DeserializeOwned>(
    runtime: &OAuthRuntime,
    url: &str,
    what: &str,
) -> Result<T, OAuthError> {
    let response = runtime.http().get(url).send().await.map_err(|error| {
        OAuthError::IdentityFetch(format!("fetching the {what} failed: {error}"))
    })?;
    if !response.status().is_success() {
        return Err(OAuthError::IdentityFetch(format!(
            "fetching the {what} returned {}",
            response.status()
        )));
    }
    response
        .json()
        .await
        .map_err(|error| OAuthError::IdentityFetch(format!("the {what} is unreadable: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn email_verified_accepts_both_shapes_issuers_send() {
        assert!(truthy(&Value::Bool(true)));
        assert!(truthy(&Value::String("true".into())));
        assert!(truthy(&Value::String("TRUE".into())));
        assert!(!truthy(&Value::Bool(false)));
        assert!(!truthy(&Value::String("no".into())));
        assert!(!truthy(&Value::Null));
    }

    #[test]
    fn a_discovery_document_only_needs_the_fields_the_flow_uses() {
        let document: Discovery = serde_json::from_str(
            r#"{
                "issuer": "https://accounts.google.com",
                "authorization_endpoint": "https://accounts.google.com/o/oauth2/v2/auth",
                "token_endpoint": "https://oauth2.googleapis.com/token",
                "jwks_uri": "https://www.googleapis.com/oauth2/v3/certs",
                "unrelated": "ignored"
            }"#,
        )
        .expect("parses");
        assert_eq!(document.issuer, "https://accounts.google.com");
        assert_eq!(
            document.jwks_uri,
            "https://www.googleapis.com/oauth2/v3/certs"
        );
    }
}
