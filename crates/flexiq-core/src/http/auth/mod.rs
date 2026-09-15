//! The signing seam every outbound authentication scheme plugs into.
//!
//! GitHub issue #844: a push target reachable from the scheduler is reachable
//! by anything else that can reach it too, so the scheduler has to prove who
//! it is. [`Signer`] is the seam HMAC, OIDC and SigV4 each plug into, one
//! commit apiece. Two are shipped so far: a static bearer token needing no
//! machinery, in the private `bearer` submodule, and HMAC-SHA256 — the
//! replay-resistant scheme issue #844 names by "works everywhere" — in the
//! private `hmac` submodule.

mod bearer;
mod hmac;

use std::sync::Arc;

use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

use crate::worker::Secret;
use bearer::BearerSigner;
use hmac::HmacSigner;

pub use hmac::{
    string_to_sign, verify, HmacConfig, HmacRejection, DEFAULT_MAX_SKEW, HDR_KEY_ID, HDR_NONCE,
    HDR_SIGNATURE, HDR_TIMESTAMP,
};

/// Everything a signer may see, and nothing it may change.
///
/// Borrowed throughout, and deliberately: the signature has to cover the bytes
/// the client actually puts on the wire, so the caller owns them and lends
/// them rather than letting a signer rebuild a request it might build
/// differently. A signer that could replace the URL would also reopen the
/// rebinding hole the pinned resolver closes.
pub struct SigningRequest<'a> {
    /// Uppercase HTTP method, as it appears on the request line.
    pub method: &'a str,
    /// The absolute target, already parsed by the parser the client resolves with.
    pub url: &'a url::Url,
    /// The exact body bytes. Empty for a body-less request.
    pub body: &'a [u8],
    /// Headers the caller has already decided to send.
    ///
    /// A scheme that covers headers signs these and nothing else, so the
    /// caller must not add another header after signing — the receiver would
    /// canonicalise a different request and reject a correct signature.
    pub headers: &'a HeaderMap,
}

/// One outbound authentication scheme.
#[async_trait]
pub trait Signer: Send + Sync + 'static {
    /// Headers to merge into the request.
    ///
    /// Returns a `HeaderMap` rather than mutating a `RequestBuilder`: a
    /// returned map can be asserted byte-for-byte with no client, no runtime
    /// and no bound port, which is the entire test strategy in a repo that
    /// has no mocking crate, and `RequestBuilder::body` is write-only, so a
    /// signer handed a builder still could not read the body it must hash.
    ///
    /// Async, not sync: two of the three schemes this seam exists for fetch a
    /// credential over the network. A sync signature would force either
    /// `block_in_place` inside the dispatch loop — a deadlock on a
    /// current-thread runtime, and this crate builds both kinds — or a
    /// fetch-everything-up-front design that can never refresh mid-flight. A
    /// bearer signer pays one poll of an already-ready future for the same
    /// signature.
    ///
    /// Sign once per attempt and never cache the result: every scheme here
    /// binds a timestamp, so a retry after a timeout has to re-sign or the
    /// receiver's replay window refuses it.
    async fn sign(&self, request: &SigningRequest<'_>) -> Result<HeaderMap, AuthError>;

    /// Scheme name, for a log line or a metric label. Carries no
    /// configuration, so it is always safe to print.
    fn scheme(&self) -> &'static str;
}

/// Insert a header, whose value may or may not be credential material.
///
/// One helper for both the sensitive and the plain case, rather than two
/// near-identical functions, so the two header paths cannot drift apart.
/// `sensitive` is not advisory when `true`: it is what makes `{headers:?}`
/// in a dispatch log print `Sensitive` instead of the value.
pub(crate) fn insert_header(
    map: &mut HeaderMap,
    name: HeaderName,
    value: &str,
    sensitive: bool,
) -> Result<(), AuthError> {
    let mut header_value = HeaderValue::from_str(value)
        .map_err(|_| AuthError::InvalidHeaderValue(name.to_string()))?;
    header_value.set_sensitive(sensitive);
    map.insert(name, header_value);
    Ok(())
}

/// Why a request could not be signed.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    /// The scheme's configuration is unusable.
    #[error("outbound auth is misconfigured: {0}")]
    Config(String),
    /// A credential could not be rendered as a header value.
    #[error("outbound auth could not set header '{0}'")]
    InvalidHeaderValue(String),
}

impl AuthError {
    /// Whether signing is worth attempting again.
    ///
    /// Both variants today are `false`: a misconfiguration and a value that is
    /// not a legal header do not become true on a retry. The method exists
    /// because the dispatcher has to route a signing failure through the same
    /// retry decision as every other failure, and later variants — a metadata
    /// endpoint answering 429, a connection that never landed — are genuinely
    /// transient.
    pub fn retryable(&self) -> bool {
        match self {
            AuthError::Config(_) | AuthError::InvalidHeaderValue(_) => false,
        }
    }
}

/// How the scheduler proves to a push target that it is the scheduler.
///
/// Deliberately **not** `Serialize`/`Deserialize`. Every variant below the
/// first holds credential material, and a config type that round-trips through
/// serde is one `#[derive]` away from being written into a settings row or a
/// task definition. The operator's side of this is a *reference* — a token id,
/// an environment variable name — resolved into one of these at construction.
#[derive(Clone, Debug)]
pub enum OutboundAuth {
    /// Send nothing. Only sensible when the target is otherwise unreachable —
    /// a mesh with mTLS, a socket on the same host.
    None,
    /// A static `Authorization: Bearer <secret>`.
    Bearer(Secret),
    /// HMAC-SHA256 over the method, target, body digest, timestamp and
    /// nonce — the scheme with replay defence, for a target reachable from
    /// anywhere but the scheduler.
    Hmac(HmacConfig),
}

impl OutboundAuth {
    /// Build the signer, or `None` when there is nothing to sign with.
    ///
    /// Takes no client: nothing this commit ships needs one. A later commit
    /// adds a parameter when the generic OAuth2 source dials an
    /// operator-supplied token URL, which has to go through the same guarded
    /// client every other destination does.
    pub fn signer(self) -> Result<Option<Arc<dyn Signer>>, AuthError> {
        match self {
            OutboundAuth::None => Ok(None),
            OutboundAuth::Bearer(secret) => {
                // Checked here, not in `sign`: a `Bearer ` header with
                // nothing after it authenticates nobody, and failing at
                // construction beats every dispatch being refused by the
                // target with no explanation on our side.
                if secret.is_empty() {
                    return Err(AuthError::Config(
                        "bearer token must not be empty".to_string(),
                    ));
                }
                Ok(Some(Arc::new(BearerSigner::new(secret))))
            }
            OutboundAuth::Hmac(config) => {
                // Same reasoning and the same place as the bearer's check
                // above: an empty key signs nothing meaningful, and failing
                // here beats every dispatch being refused by the target with
                // no explanation on our side.
                if config.secret.is_empty() {
                    return Err(AuthError::Config(
                        "hmac secret must not be empty".to_string(),
                    ));
                }
                Ok(Some(Arc::new(HmacSigner::new(config)?)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_configured_scheme_yields_a_signer_and_none_yields_nothing() {
        assert!(OutboundAuth::None
            .signer()
            .expect("None never fails to build")
            .is_none());

        let signer = OutboundAuth::Bearer(Secret::new("token-value"))
            .signer()
            .expect("a non-empty secret builds")
            .expect("Bearer always yields a signer");
        assert_eq!(signer.scheme(), "bearer");
    }

    #[test]
    fn an_empty_bearer_secret_is_refused_at_construction() {
        // `Arc<dyn Signer>` carries no `Debug`, so `expect_err` cannot be
        // used here; `matches!` needs none.
        let result = OutboundAuth::Bearer(Secret::new("")).signer();
        assert!(matches!(result, Err(AuthError::Config(_))));
    }

    #[test]
    fn an_empty_hmac_secret_is_refused_at_construction() {
        let result = OutboundAuth::Hmac(HmacConfig {
            key_id: None,
            secret: Secret::new(""),
        })
        .signer();
        assert!(matches!(result, Err(AuthError::Config(_))));
    }

    #[test]
    fn the_config_never_reaches_a_formatter() {
        // Alternating letter/digit: every 4-character window below carries a
        // digit, so it cannot coincide with a purely alphabetic run inside
        // "Bearer(Secret(<redacted>))" itself (e.g. "cret" from `Secret`) and
        // produce a false positive.
        let secret_value = "a1b2c3d4e5f6g7h8";
        let auth = OutboundAuth::Bearer(Secret::new(secret_value));
        let rendered = format!("{auth:?}");

        assert!(!rendered.contains(secret_value));
        // Any substring, not just the whole value: a formatter that echoed
        // half the secret would still be a leak.
        for window in secret_value.as_bytes().windows(4) {
            let fragment = std::str::from_utf8(window).expect("secret is ascii");
            assert!(
                !rendered.contains(fragment),
                "rendered debug leaked fragment {fragment:?}: {rendered}"
            );
        }
    }

    #[test]
    fn a_signer_is_object_safe() {
        let _: Arc<dyn Signer> = Arc::new(BearerSigner::new(Secret::new("token-value")));
    }

    #[test]
    fn neither_error_is_retryable_yet() {
        assert!(!AuthError::Config("bad config".to_string()).retryable());
        assert!(!AuthError::InvalidHeaderValue("authorization".to_string()).retryable());
    }
}
