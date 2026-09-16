//! The signing seam every outbound authentication scheme plugs into.
//!
//! GitHub issue #844: a push target reachable from the scheduler is reachable
//! by anything else that can reach it too, so the scheduler has to prove who
//! it is. [`Signer`] is the seam HMAC, OIDC and SigV4 each plug into, one
//! commit apiece. All four named in #844 are shipped: a static bearer token
//! needing no machinery, in the private `bearer` submodule; HMAC-SHA256 —
//! the replay-resistant scheme issue #844 names by "works everywhere" — in
//! the private `hmac` submodule; OIDC identity tokens, five credential
//! sources deep, in the private `oidc` submodule — Cloud Run's and Azure
//! Functions' native answer to the same question; and AWS SigV4, its own
//! credential chain four sources deep, in the private `sigv4` submodule —
//! Lambda function URLs' and API Gateway's native answer.

mod bearer;
mod cache;
mod digest;
mod hmac;
mod metadata;
mod oidc;
mod sigv4;

use std::sync::Arc;

use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

use super::DispatchClient;
use crate::worker::Secret;
use bearer::BearerSigner;
use hmac::HmacSigner;
use oidc::OidcSigner;
use sigv4::SigV4Signer;

pub use hmac::{
    string_to_sign, verify, HmacConfig, HmacRejection, DEFAULT_MAX_SKEW, HDR_KEY_ID, HDR_NONCE,
    HDR_SIGNATURE, HDR_TIMESTAMP,
};
pub use oidc::{ClientAuthStyle, OidcConfig, OidcSource};
pub use sigv4::{AwsCredentialSource, SigV4Config};

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
    /// No credential source in the chain produced one.
    #[error("no usable credential: {0}")]
    NoCredentials(&'static str),
    /// A credential endpoint answered non-2xx.
    #[error("{endpoint} answered HTTP {status}")]
    CredentialEndpoint {
        /// Which endpoint, from `MetadataEndpoint::label` — never
        /// interpolated from anything the endpoint sent.
        endpoint: &'static str,
        /// The HTTP status it answered with.
        status: u16,
    },
    /// A credential endpoint answered 2xx with something this build cannot
    /// read.
    #[error("{endpoint} returned a response this build cannot read: {reason}")]
    CredentialShape {
        /// Which endpoint, from `MetadataEndpoint::label`.
        endpoint: &'static str,
        /// Why the response could not be read, from a closed set of literals
        /// (`"not valid JSON"`, `"expires_in is not an integer"`, and so on).
        reason: &'static str,
    },
    /// The credential fetch never produced a complete response: no response
    /// at all, or a body that broke part-way through being read.
    ///
    /// A partial body is this and not [`Self::CredentialShape`] on purpose. A
    /// truncated JWT still has three dot-separated segments and a readable
    /// `exp`, so reading one as a shape error would present a corrupt token
    /// instead of diagnosing it — and would make a retryable failure
    /// permanent.
    ///
    /// **Invariant every construction site must keep: build this from
    /// [`reqwest::Error::without_url`], never from the bare `Display`.**
    /// reqwest interpolates the URL it was dialling, query string included,
    /// and an operator's token URL is a URL nothing here promised was free
    /// of a credential. This value reaches a log — `cache.rs` prints it on a
    /// refresh that failed inside the window — and a stored job error, via
    /// the push dispatcher's `Refusal::Signing`. `OidcConfig`'s userinfo
    /// check guards one half of the same hole; this guards the other.
    #[error("credential fetch failed: {0}")]
    Transport(String),
}

impl AuthError {
    /// Whether signing is worth attempting again.
    ///
    /// `endpoint` and `reason` above are `&'static str`, not `String`, and
    /// that is the point: there is no interpolation site through which a
    /// credential endpoint's response body could reach an error that lands
    /// in a log. A response body is exactly the kind of thing that should
    /// never round-trip into a log line unexamined — it might carry a
    /// partial credential, or just be large — so `CredentialEndpoint` and
    /// `CredentialShape` can only ever say *which* endpoint and *which*
    /// closed-set reason, never *what the endpoint sent*.
    ///
    /// `Transport` is retryable: the request never got a response, and
    /// trying again is exactly what a transient network failure calls for.
    /// `CredentialEndpoint` is retryable for 404, 410, 429 and any 5xx —
    /// Microsoft's own IMDS guidance names exactly those as transient.
    /// `NoCredentials`, `CredentialShape`, `Config` and `InvalidHeaderValue`
    /// stay non-retryable: none of them becomes true on a retry alone.
    pub fn retryable(&self) -> bool {
        match self {
            AuthError::Config(_)
            | AuthError::InvalidHeaderValue(_)
            | AuthError::NoCredentials(_)
            | AuthError::CredentialShape { .. } => false,
            AuthError::Transport(_) => true,
            AuthError::CredentialEndpoint { status, .. } => {
                matches!(status, 404 | 410 | 429) || (500..=599).contains(status)
            }
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
    /// A signed identity token, refreshed before it expires — Cloud Run's
    /// and Azure Functions' native answer, verified by the receiver's own
    /// platform at the door.
    Oidc(OidcConfig),
    /// AWS Signature Version 4 — Lambda function URLs' and API Gateway's
    /// native answer, verified by AWS itself at the door.
    SigV4(SigV4Config),
}

impl OutboundAuth {
    /// Build the signer, or `None` when there is nothing to sign with.
    ///
    /// Takes `dispatch`, deferred from the commit that introduced this
    /// method precisely so it would arrive with the caller that needs it:
    /// [`OidcConfig`]'s generic OAuth2 source dials an operator-supplied
    /// token URL, and that has to go through the same guarded client every
    /// other operator-configured destination does — see `oidc/oauth2.rs`'s
    /// module doc for the full argument. Every other scheme, including
    /// every other OIDC source, ignores this parameter entirely.
    ///
    /// Takes `target` for the same kind of reason, added by the SigV4
    /// commit: [`SigV4Config`]'s region and service inference reads the
    /// dispatch target's own host, which `signer` had no way to see before.
    /// Every other scheme ignores it entirely.
    pub fn signer(
        self,
        dispatch: &DispatchClient,
        target: &url::Url,
    ) -> Result<Option<Arc<dyn Signer>>, AuthError> {
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
            OutboundAuth::Oidc(config) => Ok(Some(Arc::new(OidcSigner::new(config, dispatch)?))),
            OutboundAuth::SigV4(config) => Ok(Some(Arc::new(SigV4Signer::new(config, target)?))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::http::EgressPolicy;
    use crate::net::Allowlist;

    /// A permissive client for tests that need one only to satisfy
    /// `signer`'s parameter — every scheme but `Oidc`'s OAuth2 source
    /// ignores it entirely, so what the policy allows is irrelevant here.
    fn test_dispatch_client() -> DispatchClient {
        let policy = Arc::new(EgressPolicy::new(
            Allowlist::parse("0.0.0.0/0,::/0").expect("test allowlist parses"),
            true,
        ));
        DispatchClient::new(policy, Duration::from_secs(5)).expect("test client builds")
    }

    /// A target for schemes that ignore it entirely — every one but SigV4's,
    /// which has its own dedicated tests in `sigv4/mod.rs`.
    fn test_target() -> url::Url {
        url::Url::parse("https://push.example.com/hook").expect("test url parses")
    }

    #[test]
    fn a_configured_scheme_yields_a_signer_and_none_yields_nothing() {
        let dispatch = test_dispatch_client();
        let target = test_target();
        assert!(OutboundAuth::None
            .signer(&dispatch, &target)
            .expect("None never fails to build")
            .is_none());

        let signer = OutboundAuth::Bearer(Secret::new("token-value"))
            .signer(&dispatch, &target)
            .expect("a non-empty secret builds")
            .expect("Bearer always yields a signer");
        assert_eq!(signer.scheme(), "bearer");
    }

    #[test]
    fn an_empty_bearer_secret_is_refused_at_construction() {
        // `Arc<dyn Signer>` carries no `Debug`, so `expect_err` cannot be
        // used here; `matches!` needs none.
        let dispatch = test_dispatch_client();
        let result = OutboundAuth::Bearer(Secret::new("")).signer(&dispatch, &test_target());
        assert!(matches!(result, Err(AuthError::Config(_))));
    }

    #[test]
    fn an_empty_hmac_secret_is_refused_at_construction() {
        let dispatch = test_dispatch_client();
        let result = OutboundAuth::Hmac(HmacConfig {
            key_id: None,
            secret: Secret::new(""),
        })
        .signer(&dispatch, &test_target());
        assert!(matches!(result, Err(AuthError::Config(_))));
    }

    #[test]
    fn an_oidc_config_yields_a_signer_through_the_same_seam() {
        let dispatch = test_dispatch_client();
        let signer = OutboundAuth::Oidc(OidcConfig {
            source: OidcSource::File {
                path: std::path::PathBuf::from("/tmp/does-not-need-to-exist-for-this-check.jwt"),
            },
            audience: String::new(),
        })
        .signer(&dispatch, &test_target())
        .expect("a File source needs no audience and no network to construct")
        .expect("Oidc always yields a signer");
        assert_eq!(signer.scheme(), "oidc");
    }

    #[test]
    fn an_empty_oidc_audience_is_refused_at_construction() {
        let dispatch = test_dispatch_client();
        let result = OutboundAuth::Oidc(OidcConfig {
            source: OidcSource::GoogleMetadata,
            audience: String::new(),
        })
        .signer(&dispatch, &test_target());
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
    fn config_and_invalid_header_are_never_retryable() {
        assert!(!AuthError::Config("bad config".to_string()).retryable());
        assert!(!AuthError::InvalidHeaderValue("authorization".to_string()).retryable());
    }

    #[test]
    fn credential_fetch_errors_are_retryable_by_a_closed_set_of_codes() {
        assert!(AuthError::Transport("connection reset".to_string()).retryable());
        assert!(!AuthError::NoCredentials("no source configured").retryable());
        assert!(!AuthError::CredentialShape {
            endpoint: "gce metadata identity",
            reason: "not valid JSON",
        }
        .retryable());

        for status in [404, 410, 429, 500, 503, 599] {
            assert!(
                AuthError::CredentialEndpoint {
                    endpoint: "gce metadata identity",
                    status,
                }
                .retryable(),
                "{status} must be retryable"
            );
        }
        for status in [400, 401, 403, 451] {
            assert!(
                !AuthError::CredentialEndpoint {
                    endpoint: "gce metadata identity",
                    status,
                }
                .retryable(),
                "{status} must not be retryable"
            );
        }
    }
}
