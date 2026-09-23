//! A Pub/Sub push subscription's OIDC token.
//!
//! A push subscription configured with a service account sends
//! `Authorization: Bearer <id token>`, signed by Google. The token proves the
//! request came from Pub/Sub acting as that account, for that audience — which
//! a query-string secret cannot, and without putting a credential in any URL.
//!
//! Checked: an RS256 signature by a key in Google's published set, the issuer,
//! the expiry, an audience that is exactly the configured one, and a verified
//! `email` that is exactly the configured service account. The algorithm comes
//! from this code, never from the token's own header, which whoever presents
//! the token wrote.

use std::time::Duration;

use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use serde_json::Value;

use super::fetch::KeyFetcher;
use super::{Inbound, Rejection};

/// Where Google publishes the keys its OIDC tokens are signed with.
pub const GOOGLE_JWKS_URL: &str = "https://www.googleapis.com/oauth2/v3/certs";

const ISSUERS: [&str; 2] = ["https://accounts.google.com", "accounts.google.com"];

/// Google rotates keys daily and publishes them well ahead, so an hour's
/// cache never serves a set missing the current key for long.
const JWKS_TTL: Duration = Duration::from_secs(3600);

/// A token naming an unknown key refetches the set at most this often.
const REFRESH_FLOOR: Duration = Duration::from_secs(60);

/// Clock skew allowed on `exp` and `nbf`.
const LEEWAY_SECS: u64 = 60;

/// Pub/Sub push OIDC token verification.
#[derive(Debug, Clone)]
pub struct GoogleOidc {
    audience: String,
    service_account: String,
}

#[derive(Deserialize)]
struct Claims {
    #[serde(default)]
    aud: Option<Value>,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    email_verified: Option<Value>,
}

impl GoogleOidc {
    /// Accept tokens for `audience`, minted for `service_account`.
    pub fn new(audience: String, service_account: String) -> Self {
        Self {
            audience,
            service_account,
        }
    }

    pub(super) async fn verify(
        &self,
        inbound: &Inbound<'_>,
        keys: &KeyFetcher,
    ) -> Result<(), Rejection> {
        let token = inbound
            .header("authorization")
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(str::trim)
            .filter(|token| !token.is_empty())
            .ok_or(Rejection("no bearer token"))?;
        let header = decode_header(token).map_err(|_| Rejection("the token is malformed"))?;
        if header.alg != Algorithm::RS256 {
            return Err(Rejection("the token is not RS256"));
        }
        let kid = header
            .kid
            .ok_or(Rejection("the token names no signing key"))?;

        let jwk = match self.key(keys, &kid, false).await? {
            Some(jwk) => jwk,
            // Google rotates keys; a miss is usually a stale cache, so it
            // earns one refresh — rate-limited by the fetcher's floor.
            None => self
                .key(keys, &kid, true)
                .await?
                .ok_or(Rejection("no Google key matches the token's kid"))?,
        };
        let key =
            DecodingKey::from_jwk(&jwk).map_err(|_| Rejection("the Google key is unusable"))?;

        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_audience(&[&self.audience]);
        validation.set_issuer(&ISSUERS);
        validation.set_required_spec_claims(&["exp", "iss", "aud"]);
        validation.leeway = LEEWAY_SECS;
        let claims = decode::<Claims>(token, &key, &validation)
            .map_err(|_| Rejection("the token failed validation"))?
            .claims;

        // The decoder accepts an audience list that merely *contains* ours;
        // a token meant for several services is not one meant for this one.
        let only_ours = match &claims.aud {
            Some(Value::String(aud)) => aud == &self.audience,
            Some(Value::Array(auds)) => auds.iter().all(|aud| aud == &self.audience),
            _ => false,
        };
        if !only_ours {
            return Err(Rejection("the token names another audience"));
        }
        if claims.email.as_deref() != Some(self.service_account.as_str()) {
            return Err(Rejection("the token is for another service account"));
        }
        let verified = matches!(&claims.email_verified, Some(Value::Bool(true)))
            || matches!(&claims.email_verified, Some(Value::String(text)) if text == "true");
        if !verified {
            return Err(Rejection("the token's email is not verified"));
        }
        Ok(())
    }

    async fn key(
        &self,
        keys: &KeyFetcher,
        kid: &str,
        refresh: bool,
    ) -> Result<Option<jsonwebtoken::jwk::Jwk>, Rejection> {
        let bytes = keys
            .cached(GOOGLE_JWKS_URL, JWKS_TTL, refresh, REFRESH_FLOOR)
            .await
            .map_err(|error| {
                log::warn!("[flexiq] Google signing keys could not be fetched: {error}");
                Rejection("Google's signing keys could not be fetched")
            })?;
        let set: JwkSet = serde_json::from_slice(&bytes)
            .map_err(|_| Rejection("Google's signing keys are unreadable"))?;
        Ok(set.find(kid).cloned())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use axum::http::{HeaderMap, HeaderValue};
    use jsonwebtoken::{encode, EncodingKey, Header};
    use serde_json::json;

    use super::*;
    use crate::trigger::auth::fetch::Fetch;

    const KEY_PEM: &[u8] = include_bytes!("../../../tests/fixtures/oidc_test_key.pem");
    const JWKS: &[u8] = include_bytes!("../../../tests/fixtures/oidc_test_jwks.json");
    const AUDIENCE: &str = "https://hooks.example.com/t/gcs";
    const ACCOUNT: &str = "pusher@project.iam.gserviceaccount.com";

    fn fixtures() -> (KeyFetcher, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let fetch: Fetch = Arc::new(move |_url: String| {
            counter.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(JWKS.to_vec()) })
        });
        (KeyFetcher::new(fetch), calls)
    }

    fn token(kid: &str, claims: Value) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(kid.to_string());
        encode(
            &header,
            &claims,
            &EncodingKey::from_rsa_pem(KEY_PEM).expect("a test key"),
        )
        .expect("signed")
    }

    fn claims() -> Value {
        let now = chrono::Utc::now().timestamp();
        json!({
            "iss": "https://accounts.google.com",
            "aud": AUDIENCE,
            "exp": now + 600,
            "iat": now,
            "email": ACCOUNT,
            "email_verified": true,
            "sub": "1234"
        })
    }

    async fn check(token: &str, keys: &KeyFetcher) -> Result<(), Rejection> {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_str(&format!("Bearer {token}")).expect("valid header"),
        );
        GoogleOidc::new(AUDIENCE.into(), ACCOUNT.into())
            .verify(
                &Inbound {
                    headers: &headers,
                    query: "",
                    body: b"{}",
                    now_secs: 0,
                },
                keys,
            )
            .await
    }

    #[tokio::test]
    async fn a_token_from_the_configured_account_verifies() {
        let (keys, _) = fixtures();
        assert_eq!(check(&token("test-key", claims()), &keys).await, Ok(()));
    }

    #[tokio::test]
    async fn every_claim_that_matters_is_checked() {
        let (keys, _) = fixtures();
        let cases = [
            ("aud", json!("https://elsewhere.example.com")),
            ("aud", json!([AUDIENCE, "https://elsewhere.example.com"])),
            ("iss", json!("https://evil.example.com")),
            ("email", json!("someone@else.iam.gserviceaccount.com")),
            ("email_verified", json!(false)),
            ("exp", json!(chrono::Utc::now().timestamp() - 3600)),
        ];
        for (claim, value) in cases {
            let mut forged = claims();
            forged[claim] = value.clone();
            assert!(
                check(&token("test-key", forged), &keys).await.is_err(),
                "{claim}={value} must be refused"
            );
        }
    }

    #[tokio::test]
    async fn an_unknown_kid_refetches_once_then_refuses() {
        let (keys, calls) = fixtures();
        assert!(check(&token("rotated-away", claims()), &keys)
            .await
            .is_err());
        // One fetch for the cache, and the refresh is inside the floor.
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(check(&token("rotated-away", claims()), &keys)
            .await
            .is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_missing_or_foreign_algorithm_token_is_refused() {
        let (keys, _) = fixtures();
        assert!(check("", &keys).await.is_err());
        let hs = encode(
            &Header::new(Algorithm::HS256),
            &claims(),
            &EncodingKey::from_secret(b"guess"),
        )
        .expect("signed");
        assert!(check(&hs, &keys).await.is_err());
    }
}
