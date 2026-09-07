//! The OIDC flow, shared by Google and any generic issuer.
//!
//! The identity comes from a signed `id_token`, verified against the issuer's
//! published JWKS. Signature, issuer, audience, expiry, and nonce are all
//! checked — dropping any one of them turns the token into an unauthenticated
//! claim anyone could mint. So is the signature *algorithm*, which comes from
//! the published key rather than from the token asking to be verified.

use jsonwebtoken::jwk::{AlgorithmParameters, Jwk, JwkSet, KeyAlgorithm, PublicKeyUse};
use jsonwebtoken::{decode, decode_header, Algorithm, AlgorithmFamily, DecodingKey, Validation};
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

/// Claims the flow reads. Signature, `iss`, and `exp` are enforced by the
/// decoder itself; `aud`, `azp` and `iat` are re-read here because the decoder
/// does not enforce them the way OIDC Core requires. See [`check_audience`].
#[derive(Debug, Deserialize)]
struct Claims {
    sub: Option<String>,
    #[serde(default)]
    aud: Option<Audience>,
    /// Authorized party. Present when the token was minted for a client other
    /// than the one that is meant to use it.
    #[serde(default)]
    azp: Option<String>,
    /// Issued-at. REQUIRED of an id_token, and not something
    /// `required_spec_claims` can ask for — see [`check_audience`]'s caller.
    #[serde(default)]
    iat: Option<u64>,
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

/// `aud` is one string or an array of them; both shapes are legal.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Audience {
    One(String),
    Many(Vec<String>),
}

impl Audience {
    fn values(&self) -> &[String] {
        match self {
            Audience::One(one) => std::slice::from_ref(one),
            Audience::Many(many) => many,
        }
    }
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
    let algorithms = permitted_algorithms(jwk)?;

    let mut validation = Validation::new(algorithms[0]);
    validation.algorithms = algorithms;
    validation.set_audience(&[&provider.client_id]);
    validation.set_issuer(&[&discovery.issuer]);
    // OIDC Core makes all four REQUIRED of an id_token, and naming them is also
    // what turns a malformed claim into a rejection: a claim that fails to
    // parse is indistinguishable from an absent one to the checks below, so
    // `validate_exp` alone would wave through an `exp` sent as a string.
    validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
    validation.leeway = CLOCK_SKEW_SECONDS;
    validation.validate_exp = true;
    validation.validate_nbf = true;

    let claims = decode::<Claims>(id_token, &key, &validation)
        .map(|data| data.claims)
        .map_err(|error| {
            OAuthError::IdentityFetch(format!("id_token validation failed: {error}"))
        })?;

    // `iat` is REQUIRED of an id_token, and `set_required_spec_claims` cannot
    // ask for it: the decoder recognises only exp/nbf/aud/iss/sub and skips
    // every other name silently, so this is the check.
    if claims.iat.is_none() {
        return Err(OAuthError::IdentityFetch(
            "id_token has no 'iat' claim".into(),
        ));
    }
    check_audience(&claims, &provider.client_id)?;

    Ok(claims)
}

/// Reject a token that names an audience this client does not trust.
///
/// The decoder's own `aud` check is satisfied by an *intersection*, so a token
/// carrying `["this-client", "someone-else"]` passes it. OIDC Core requires the
/// opposite: every audience must be one the client trusts, and this deployment
/// trusts exactly one — its own `client_id`. `azp` names the party a
/// multi-audience token was actually minted for, so where it is present it has
/// to be us as well.
fn check_audience(claims: &Claims, client_id: &str) -> Result<(), OAuthError> {
    let audiences = claims.aud.as_ref().map(Audience::values).unwrap_or(&[]);
    if let Some(untrusted) = audiences.iter().find(|audience| *audience != client_id) {
        return Err(OAuthError::IdentityFetch(format!(
            "id_token names an untrusted audience: {untrusted}"
        )));
    }

    match claims.azp.as_deref() {
        Some(azp) if azp != client_id => Err(OAuthError::IdentityFetch(format!(
            "id_token was authorized for another party: {azp}"
        ))),
        _ => Ok(()),
    }
}

/// The algorithms `jwk` may verify an `id_token` with.
///
/// Deliberately not the algorithm named in the token's own header: that header
/// is written by whoever presents the token, so a validator built from it can
/// only ever agree with itself. The issuer pins the algorithm when it publishes
/// one on the key; failing that the key type narrows it to a family.
fn permitted_algorithms(jwk: &Jwk) -> Result<Vec<Algorithm>, OAuthError> {
    if jwk.common.public_key_use == Some(PublicKeyUse::Encryption) {
        return Err(OAuthError::IdentityFetch(
            "the id_token's signing key is published for encryption, not signatures".into(),
        ));
    }

    if let Some(declared) = jwk.common.key_algorithm {
        return signing_algorithm(declared)
            .map(|algorithm| vec![algorithm])
            .ok_or_else(|| {
                OAuthError::IdentityFetch(
                    "the id_token's signing key declares an 'alg' that cannot verify a signature"
                        .into(),
                )
            });
    }

    let family = match &jwk.algorithm {
        AlgorithmParameters::RSA(_) => AlgorithmFamily::Rsa,
        AlgorithmParameters::EllipticCurve(_) => AlgorithmFamily::Ec,
        AlgorithmParameters::OctetKeyPair(_) => AlgorithmFamily::Ed,
        // A JWKS publishes public halves. A symmetric key has none — the whole
        // key is the signing secret — so anyone who can read the set could
        // mint tokens with it.
        AlgorithmParameters::OctetKey(_) => {
            return Err(OAuthError::IdentityFetch(
                "the id_token's signing key is symmetric, which a JWKS must never publish".into(),
            ))
        }
    };
    Ok(family.algorithms().to_vec())
}

/// The signature algorithm a key's `alg` names, if it names one at all.
///
/// `None` covers both halves of what must never reach the verifier: HMAC, where
/// the published key doubles as the signing secret, and the RSA encryption
/// algorithms, which do not sign at all.
fn signing_algorithm(declared: KeyAlgorithm) -> Option<Algorithm> {
    match declared {
        KeyAlgorithm::ES256 => Some(Algorithm::ES256),
        KeyAlgorithm::ES384 => Some(Algorithm::ES384),
        KeyAlgorithm::RS256 => Some(Algorithm::RS256),
        KeyAlgorithm::RS384 => Some(Algorithm::RS384),
        KeyAlgorithm::RS512 => Some(Algorithm::RS512),
        KeyAlgorithm::PS256 => Some(Algorithm::PS256),
        KeyAlgorithm::PS384 => Some(Algorithm::PS384),
        KeyAlgorithm::PS512 => Some(Algorithm::PS512),
        KeyAlgorithm::EdDSA => Some(Algorithm::EdDSA),
        KeyAlgorithm::HS256
        | KeyAlgorithm::HS384
        | KeyAlgorithm::HS512
        | KeyAlgorithm::RSA1_5
        | KeyAlgorithm::RSA_OAEP
        | KeyAlgorithm::RSA_OAEP_256
        | KeyAlgorithm::UNKNOWN_ALGORITHM => None,
    }
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

    /// A JWK is only ever built here from a literal, so a bad one is a bug in
    /// the test rather than a case the code must handle.
    fn jwk(json: serde_json::Value) -> Jwk {
        serde_json::from_value(json).expect("a well-formed JWK")
    }

    /// The modulus and exponent are never used — nothing in these tests
    /// verifies a signature, only decides which algorithms may attempt one.
    fn rsa_key(extra: serde_json::Value) -> Jwk {
        let mut json = serde_json::json!({
            "kty": "RSA",
            "kid": "test",
            "n": "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM64",
            "e": "AQAB",
        });
        let (Some(base), Some(extra)) = (json.as_object_mut(), extra.as_object()) else {
            panic!("both literals are objects");
        };
        base.extend(extra.clone());
        jwk(json)
    }

    #[test]
    fn an_rsa_key_verifies_with_rsa_algorithms_only() {
        let allowed = permitted_algorithms(&rsa_key(serde_json::json!({}))).expect("allowed");

        assert!(allowed.contains(&Algorithm::RS256));
        // The whole point: the token cannot talk the verifier into HMAC, where
        // the published modulus would serve as the shared secret.
        for symmetric in [Algorithm::HS256, Algorithm::HS384, Algorithm::HS512] {
            assert!(
                !allowed.contains(&symmetric),
                "must not allow {symmetric:?}"
            );
        }
    }

    #[test]
    fn a_key_that_pins_its_algorithm_allows_exactly_that_one() {
        let allowed =
            permitted_algorithms(&rsa_key(serde_json::json!({ "alg": "RS256" }))).expect("allowed");
        assert_eq!(allowed, vec![Algorithm::RS256]);
    }

    #[test]
    fn a_key_pinned_to_hmac_verifies_nothing() {
        let error = permitted_algorithms(&rsa_key(serde_json::json!({ "alg": "HS256" })))
            .expect_err("rejected");
        assert!(matches!(error, OAuthError::IdentityFetch(_)));
    }

    #[test]
    fn a_key_pinned_to_an_encryption_algorithm_verifies_nothing() {
        assert!(permitted_algorithms(&rsa_key(serde_json::json!({ "alg": "RSA-OAEP" }))).is_err());
    }

    #[test]
    fn a_key_published_for_encryption_verifies_nothing() {
        assert!(permitted_algorithms(&rsa_key(serde_json::json!({ "use": "enc" }))).is_err());
    }

    #[test]
    fn a_symmetric_key_in_a_jwks_verifies_nothing() {
        let symmetric = jwk(serde_json::json!({
            "kty": "oct",
            "kid": "test",
            "k": "c2VjcmV0",
        }));
        assert!(permitted_algorithms(&symmetric).is_err());
    }

    #[test]
    fn an_ec_key_verifies_with_ecdsa_algorithms_only() {
        let allowed = permitted_algorithms(&jwk(serde_json::json!({
            "kty": "EC",
            "kid": "test",
            "crv": "P-256",
            "x": "f83OJ3D2xF1Bg8vub9tLe1gHMzV76e8Tus9uPHvRVEU",
            "y": "x_FEzRu9m36HLN_tue659LNpXW6pCyStikYjKIWI5a0",
        })))
        .expect("allowed");

        assert_eq!(allowed, vec![Algorithm::ES256, Algorithm::ES384]);
    }

    /// Claims carrying only what [`check_audience`] reads.
    fn claims(json: serde_json::Value) -> Claims {
        serde_json::from_value(json).expect("well-formed claims")
    }

    #[test]
    fn the_only_trusted_audience_is_this_client() {
        assert!(check_audience(&claims(serde_json::json!({ "aud": "me" })), "me").is_ok());
        assert!(check_audience(&claims(serde_json::json!({ "aud": ["me"] })), "me").is_ok());
    }

    #[test]
    fn an_extra_audience_is_refused_even_though_this_client_is_named() {
        // The decoder is satisfied by an intersection, so this is the token
        // that would otherwise walk straight through.
        let error = check_audience(
            &claims(serde_json::json!({ "aud": ["me", "someone-else"] })),
            "me",
        )
        .expect_err("rejected");

        assert!(matches!(error, OAuthError::IdentityFetch(_)));
    }

    #[test]
    fn an_audience_that_is_not_this_client_is_refused() {
        assert!(
            check_audience(&claims(serde_json::json!({ "aud": "someone-else" })), "me").is_err()
        );
    }

    #[test]
    fn an_authorized_party_must_be_this_client_when_present() {
        assert!(check_audience(
            &claims(serde_json::json!({ "aud": "me", "azp": "me" })),
            "me"
        )
        .is_ok());
        assert!(check_audience(
            &claims(serde_json::json!({ "aud": "me", "azp": "someone-else" })),
            "me"
        )
        .is_err());
    }

    #[test]
    fn a_missing_authorized_party_is_not_itself_a_failure() {
        // `azp` is only required of a multi-audience token, and one of those
        // is already refused above.
        assert!(check_audience(&claims(serde_json::json!({ "aud": "me" })), "me").is_ok());
    }

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
