//! OIDC identity tokens for outbound dispatch.
//!
//! GitHub issue #844 names OIDC as the outbound-auth scheme whose native
//! story this is: Cloud Run and Azure Functions both verify a signed
//! identity token at the door, so a push target on either needs nothing but
//! its own platform's verification to know the caller is the scheduler.
//!
//! Five credential sources, and they split on trust, not on cloud:
//!
//! - [`OidcSource::GoogleMetadata`], [`OidcSource::AzureImds`] and
//!   [`OidcSource::AzureAppService`] each reach a host that is either a
//!   compile-time constant or an environment-supplied loopback or
//!   link-local address — App Service's `IDENTITY_ENDPOINT` is ordinarily
//!   loopback, IMDS is the link-local `169.254.169.254` — never a value an
//!   operator typed in, so all three fetch through
//!   [`MetadataClient`], the type that is structurally incapable of being
//!   pointed at an arbitrary host (see `metadata.rs`'s module doc).
//! - [`OidcSource::OAuth2ClientCredentials`] dials a token URL the operator
//!   configured, exactly the same kind of input the dispatch target's own
//!   URL is, so it fetches through the guarded
//!   [`DispatchClient`](crate::http::DispatchClient) instead, subject to the
//!   same egress allowlist as the dispatch target itself — **including the
//!   construction-time host check**, because the pinned resolver alone never
//!   sees an IP-literal host. See `oauth2.rs`'s `validate_token_url` for the
//!   four rules. That asymmetry, three sources on one client and one source
//!   on the other, is the load-bearing design decision here.
//! - [`OidcSource::File`] touches no network at all: it re-reads a
//!   Kubernetes-projected service-account token off disk on every refresh.
//!
//! This module is the presenter, never the verifier: the receiver checks a
//! token's signature at its own door, and all this code ever needs is when
//! to fetch a replacement, via the `exp` claim (`jwt.rs`). There is no
//! `jsonwebtoken` dependency anywhere in this subsystem, and there should not
//! be one — verifying a token this process just fetched would prove nothing
//! about it, since the only key available to check it against is the
//! issuer's, which is exactly who this process already trusts to have signed
//! it correctly.

mod azure;
mod file;
mod google;
mod jwt;
mod oauth2;

use std::path::PathBuf;

use async_trait::async_trait;
use reqwest::header::{HeaderMap, AUTHORIZATION};

pub use oauth2::ClientAuthStyle;

use super::cache::{refresh_at_ms, CredentialCache, Expiring};
use super::metadata::MetadataClient;
use super::{insert_header, AuthError, Signer, SigningRequest};
use crate::http::DispatchClient;
use crate::job::now_millis;
use crate::worker::Secret;

/// Where the identity token comes from.
#[derive(Clone, Debug)]
pub enum OidcSource {
    /// GCE, GKE or Cloud Run's metadata server.
    GoogleMetadata,
    /// Azure IMDS — VMs, VMSS, AKS.
    AzureImds {
        /// A user-assigned identity's client id. At most one of
        /// `client_id`, `object_id` and `msi_res_id` may be set — more than
        /// one is [`AuthError::Config`] at construction, not at fetch time —
        /// see `azure.rs`'s `validate_imds_selector`.
        client_id: Option<String>,
        /// A user-assigned identity's object id. See `client_id`'s doc.
        object_id: Option<String>,
        /// A user-assigned identity's Azure resource id. See `client_id`'s
        /// doc.
        msi_res_id: Option<String>,
    },
    /// Azure App Service, Functions or Container Apps' per-instance sidecar
    /// — the source that actually covers "Azure Functions" in issue #844,
    /// as opposed to [`OidcSource::AzureImds`], which only ever answers on a
    /// VM. `IDENTITY_ENDPOINT` and `IDENTITY_HEADER` are read from the
    /// process environment at construction; a missing one is
    /// [`AuthError::NoCredentials`].
    AzureAppService,
    /// Generic OAuth2 client-credentials — the only off-cloud source, and
    /// the one that dials an operator-supplied host. See this module's doc.
    OAuth2ClientCredentials {
        /// The token endpoint the operator configured. Parsed and validated
        /// once at construction, by `oauth2::validate_token_url`: a URL,
        /// `https` (or `http` to loopback), no userinfo, and **a host the
        /// egress allowlist names** — this endpoint is dialled through the
        /// same guard as the dispatch target, so it has to be allowlisted
        /// alongside it.
        token_url: String,
        /// The client id sent in the form body under
        /// [`ClientAuthStyle::ClientSecretPost`], or urlencoded into the
        /// `Authorization: Basic` header under
        /// [`ClientAuthStyle::ClientSecretBasic`].
        client_id: String,
        /// The client secret. Never sent in the clear on the wire without
        /// going through form- or Basic-encoding first — see `oauth2.rs`.
        client_secret: Secret,
        /// Sent as the `scope` form field when set; omitted otherwise.
        scope: Option<String>,
        /// Which of the two RFC 6749 §2.3 client authentication styles to
        /// use.
        style: ClientAuthStyle,
    },
    /// A Kubernetes projected service-account token on disk, re-read on
    /// every refresh — the kubelet rotates the file in place, so a token
    /// cached from the first read would outlive its own file.
    File {
        /// Where the projected token file lives.
        path: PathBuf,
    },
}

/// Outbound OIDC configuration.
#[derive(Clone, Debug)]
pub struct OidcConfig {
    /// Which of the five credential sources to fetch from.
    pub source: OidcSource,
    /// The value the receiver will check the token's `aud` against — for
    /// Cloud Run, the target's base URL.
    ///
    /// Required for [`OidcSource::GoogleMetadata`] and both Azure sources —
    /// their endpoints reject a request with no `audience`/`resource` at
    /// all. Optional for the other two, for two different reasons:
    /// [`OidcSource::File`] has no wire request of its own to attach it to
    /// (a projected token was already minted, `aud` and all, by whoever
    /// issues it), and [`OidcSource::OAuth2ClientCredentials`] sends
    /// `audience` only as the Auth0/Okta convention it is, not an RFC 6749
    /// field — a standards-compliant authorization server may reject an
    /// unrecognised parameter, so a target we could not otherwise reach at
    /// all must stay reachable with none sent. This three-way asymmetry is
    /// enforced in `OidcSigner::new`, not allowed silently by every source.
    pub audience: String,
}

/// A signed identity token, refreshed before it expires.
pub struct OidcSigner {
    audience: String,
    resolved: Resolved,
    cache: CredentialCache<String>,
}

/// Everything one configured source needs to fetch a fresh token, with every
/// construction-time validation already done and every per-source client
/// already built.
///
/// [`OidcSigner::fetch_token`] matches on this, not on [`OidcSource`] again:
/// every branch here already carries what it fetches with, so there is no
/// `Option` to find unexpectedly `None` in at fetch time for a source that
/// should always have built one at construction.
enum Resolved {
    Google {
        metadata: MetadataClient,
    },
    AzureImds {
        metadata: MetadataClient,
        client_id: Option<String>,
        object_id: Option<String>,
        msi_res_id: Option<String>,
    },
    AzureAppService {
        metadata: MetadataClient,
        endpoint: url::Url,
        identity_header: Secret,
    },
    OAuth2(Box<OAuth2Resolved>),
    File {
        path: PathBuf,
    },
}

/// [`Resolved::OAuth2`]'s fields, boxed so the largest variant does not set
/// the size of every other one — `File`, the smallest, is a bare `PathBuf`.
struct OAuth2Resolved {
    client: reqwest::Client,
    token_url: url::Url,
    client_id: String,
    client_secret: Secret,
    scope: Option<String>,
    style: ClientAuthStyle,
}

impl OidcSigner {
    /// Build a signer from `config`.
    ///
    /// Takes `dispatch` only to clone the guarded `reqwest::Client` out of it
    /// for [`OidcSource::OAuth2ClientCredentials`] — see this module's doc
    /// for why that is the one source that needs it. Every other source
    /// builds its own [`MetadataClient`], which needs no policy at all: it
    /// cannot be pointed anywhere but the closed set of metadata endpoints.
    pub(super) fn new(config: OidcConfig, dispatch: &DispatchClient) -> Result<Self, AuthError> {
        let OidcConfig { source, audience } = config;

        // `audience` is required for Google and Azure — their endpoints
        // reject a request with no `audience`/`resource` outright — but
        // optional for the other two sources, for two different reasons:
        // `File` has no wire request of its own to attach it to, and
        // `OAuth2ClientCredentials` sends `audience` only as the
        // Auth0/Okta extension it is, not an RFC 6749 field, so a
        // standards-compliant authorization server that rejects an
        // unrecognised parameter must still be reachable with none sent at
        // all.
        let audience_required = !matches!(
            source,
            OidcSource::File { .. } | OidcSource::OAuth2ClientCredentials { .. }
        );
        if audience_required && audience.is_empty() {
            return Err(AuthError::Config(
                "oidc audience must not be empty".to_string(),
            ));
        }

        let resolved = match source {
            OidcSource::GoogleMetadata => Resolved::Google {
                metadata: MetadataClient::new()?,
            },
            OidcSource::AzureImds {
                client_id,
                object_id,
                msi_res_id,
            } => {
                azure::validate_imds_selector(
                    client_id.as_deref(),
                    object_id.as_deref(),
                    msi_res_id.as_deref(),
                )?;
                Resolved::AzureImds {
                    metadata: MetadataClient::new()?,
                    client_id,
                    object_id,
                    msi_res_id,
                }
            }
            OidcSource::AzureAppService => {
                // `app_service_from_env`'s closure parameter is the seam:
                // here it is `std::env::var`, and its own tests hand it a
                // fixed closure instead — the same `from_env`/`from_map`
                // split `flexiq-server`'s `Config` uses, so a test never
                // mutates the process environment to exercise this path.
                let (endpoint, identity_header) =
                    azure::app_service_from_env(|name| std::env::var(name).ok())?;
                Resolved::AzureAppService {
                    metadata: MetadataClient::new()?,
                    endpoint,
                    identity_header,
                }
            }
            OidcSource::OAuth2ClientCredentials {
                token_url,
                client_id,
                client_secret,
                scope,
                style,
            } => {
                // Parsed and vetted once, here, rather than on every fetch: a
                // bad URL, a cleartext one or one carrying userinfo are all
                // configuration mistakes, and failing at construction beats
                // every dispatch being refused with no explanation on our
                // side — the same argument `OutboundAuth::signer`'s
                // empty-secret checks make. The rules are `oauth2.rs`'s, next
                // to the code that puts the client secret on the wire.
                let token_url = oauth2::validate_token_url(&token_url, dispatch.policy())?;
                Resolved::OAuth2(Box::new(OAuth2Resolved {
                    client: dispatch.inner().clone(),
                    token_url,
                    client_id,
                    client_secret,
                    scope,
                    style,
                }))
            }
            OidcSource::File { path } => Resolved::File { path },
        };

        Ok(Self {
            audience,
            resolved,
            cache: CredentialCache::new(),
        })
    }

    async fn fetch_token(&self) -> Result<Expiring<String>, AuthError> {
        match &self.resolved {
            Resolved::Google { metadata } => google::fetch(metadata, &self.audience).await,
            Resolved::AzureImds {
                metadata,
                client_id,
                object_id,
                msi_res_id,
            } => {
                azure::fetch_imds(
                    metadata,
                    &self.audience,
                    client_id.as_deref(),
                    object_id.as_deref(),
                    msi_res_id.as_deref(),
                )
                .await
            }
            Resolved::AzureAppService {
                metadata,
                endpoint,
                identity_header,
            } => {
                azure::fetch_app_service(metadata, endpoint, identity_header, &self.audience).await
            }
            Resolved::OAuth2(fields) => {
                oauth2::fetch(
                    &fields.client,
                    &fields.token_url,
                    &fields.client_id,
                    &fields.client_secret,
                    fields.scope.as_deref(),
                    fields.style,
                    &self.audience,
                )
                .await
            }
            Resolved::File { path } => file::fetch(path).await,
        }
    }
}

#[async_trait]
impl Signer for OidcSigner {
    async fn sign(&self, _request: &SigningRequest<'_>) -> Result<HeaderMap, AuthError> {
        let token = self.cache.get_or_refresh(|| self.fetch_token()).await?;
        let mut headers = HeaderMap::with_capacity(1);
        insert_header(
            &mut headers,
            AUTHORIZATION,
            &format!("Bearer {token}"),
            true,
        )?;
        Ok(headers)
    }

    fn scheme(&self) -> &'static str {
        "oidc"
    }
}

/// Build an [`Expiring`] from a token and a TTL in seconds, measuring the
/// issue time as now: the caller just received the response, so "now" is as
/// close to the issuer's own clock as this process gets without trusting a
/// field like Azure's `expires_on` against its own possibly-skewed clock.
fn expiring_from_ttl_seconds(value: String, ttl_seconds: i64) -> Expiring<String> {
    let issued_at_ms = now_millis();
    let expires_at_ms = issued_at_ms.saturating_add(ttl_seconds.saturating_mul(1000));
    Expiring {
        value,
        refresh_at_ms: refresh_at_ms(issued_at_ms, expires_at_ms),
        expires_at_ms,
    }
}

/// Build an [`Expiring`] from a token whose expiry is already known
/// absolutely — a JWT's own `exp` claim — rather than as a TTL measured from
/// now.
fn expiring_from_expiry_ms(value: String, expires_at_ms: i64) -> Expiring<String> {
    Expiring {
        value,
        refresh_at_ms: refresh_at_ms(now_millis(), expires_at_ms),
        expires_at_ms,
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Arc;
    use std::time::Duration;

    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;

    use super::*;
    use crate::http::testing::StubServer;
    use crate::http::EgressPolicy;
    use crate::net::Allowlist;

    fn permissive_dispatch_client() -> DispatchClient {
        let policy = Arc::new(EgressPolicy::new(
            Allowlist::parse("0.0.0.0/0,::/0").expect("test allowlist parses"),
            true,
        ));
        DispatchClient::new(policy, Duration::from_secs(5))
            .expect("a permissive policy and a timeout are enough to build a client")
    }

    fn base64url(bytes: &[u8]) -> String {
        URL_SAFE_NO_PAD.encode(bytes)
    }

    /// An unsigned three-segment JWT whose `exp` is already in the past —
    /// which forces `CredentialCache` to treat it as due for refresh the
    /// very next time it is read, with no sleep and no clock mocking.
    fn expired_jwt(subject: &str) -> String {
        format!(
            "{}.{}.{}",
            base64url(br#"{"alg":"none"}"#),
            base64url(format!(r#"{{"sub":"{subject}","exp":1000000000}}"#).as_bytes()),
            base64url(b"sig"),
        )
    }

    fn empty_request() -> (url::Url, HeaderMap) {
        (
            url::Url::parse("https://push.example.com/hook").expect("test url parses"),
            HeaderMap::new(),
        )
    }

    #[tokio::test]
    async fn a_token_is_fetched_once_for_two_signings() {
        let stub = StubServer::start(200, r#"{"access_token":"tok","expires_in":3600}"#).await;
        let dispatch = permissive_dispatch_client();
        let config = OidcConfig {
            source: OidcSource::OAuth2ClientCredentials {
                token_url: format!("{}/token", stub.base_url()),
                client_id: "id".to_string(),
                client_secret: Secret::new("secret"),
                scope: None,
                style: ClientAuthStyle::ClientSecretPost,
            },
            audience: "https://push.example.com".to_string(),
        };
        let signer = OidcSigner::new(config, &dispatch).expect("construction succeeds");

        let (url, headers) = empty_request();
        let request = SigningRequest {
            method: "POST",
            url: &url,
            body: b"",
            headers: &headers,
        };

        signer.sign(&request).await.expect("first sign fetches");
        signer
            .sign(&request)
            .await
            .expect("second sign reads the cache");

        assert_eq!(stub.request_count(), 1);
    }

    #[tokio::test]
    async fn the_authorization_header_is_bearer_and_sensitive() {
        let mut file = tempfile::NamedTempFile::new().expect("temp file creates");
        write!(file, "{}", expired_jwt("scheduler")).expect("temp file writes");

        let dispatch = permissive_dispatch_client();
        let config = OidcConfig {
            source: OidcSource::File {
                path: file.path().to_path_buf(),
            },
            audience: String::new(),
        };
        let signer = OidcSigner::new(config, &dispatch).expect("construction succeeds");

        let (url, headers) = empty_request();
        let request = SigningRequest {
            method: "POST",
            url: &url,
            body: b"",
            headers: &headers,
        };
        let signed = signer.sign(&request).await.expect("a readable file signs");

        assert_eq!(signed.len(), 1);
        let value = signed
            .get(AUTHORIZATION)
            .expect("authorization header present");
        assert!(value.is_sensitive());
        assert!(value
            .to_str()
            .expect("header value is ascii")
            .starts_with("Bearer "));

        let rendered = format!("{signed:?}");
        assert!(rendered.contains("Sensitive"));
        assert!(!rendered.contains(&expired_jwt("scheduler")));
    }

    #[test]
    fn the_config_never_reaches_a_formatter() {
        // Alternating letter/digit, matching the sliding-window technique
        // `auth/mod.rs`'s own `the_config_never_reaches_a_formatter` uses:
        // every 4-character window carries a digit, so it cannot coincide
        // with a purely alphabetic run elsewhere in the rendered `Debug`.
        let secret_value = "q1w2e3r4t5y6u7i8";
        let config = OidcConfig {
            source: OidcSource::OAuth2ClientCredentials {
                token_url: "https://issuer.example.com/token".to_string(),
                client_id: "client-id".to_string(),
                client_secret: Secret::new(secret_value),
                scope: Some("push:dispatch".to_string()),
                style: ClientAuthStyle::ClientSecretBasic,
            },
            audience: "https://push.example.com".to_string(),
        };
        let rendered = format!("{config:?}");

        assert!(!rendered.contains(secret_value));
        for window in secret_value.as_bytes().windows(4) {
            let fragment = std::str::from_utf8(window).expect("secret is ascii");
            assert!(
                !rendered.contains(fragment),
                "rendered debug leaked fragment {fragment:?}: {rendered}"
            );
        }
        // The rest of the config is not itself sensitive and should still
        // print, so a redaction that swallowed the whole struct would not
        // be caught by the assertions above alone.
        assert!(rendered.contains("client-id"));
        assert!(rendered.contains("push.example.com"));
    }

    #[test]
    fn an_empty_audience_is_refused_at_construction() {
        let dispatch = permissive_dispatch_client();
        let config = OidcConfig {
            source: OidcSource::GoogleMetadata,
            audience: String::new(),
        };
        // `OidcSigner` carries no `Debug`, so `expect_err` cannot be used
        // here; `matches!` needs none.
        let result = OidcSigner::new(config, &dispatch);
        assert!(matches!(result, Err(AuthError::Config(_))));
    }

    #[test]
    fn a_token_url_with_userinfo_is_refused_at_construction() {
        let dispatch = permissive_dispatch_client();

        for token_url in [
            "https://id:s3cret@issuer.example.com/token",
            "https://id@issuer.example.com/token",
        ] {
            let config = OidcConfig {
                source: OidcSource::OAuth2ClientCredentials {
                    token_url: token_url.to_string(),
                    client_id: "id".to_string(),
                    client_secret: Secret::new("secret"),
                    scope: None,
                    style: ClientAuthStyle::ClientSecretPost,
                },
                audience: "https://push.example.com".to_string(),
            };
            // `OidcSigner` carries no `Debug`, so `expect_err` cannot be
            // used here; `matches!` needs none.
            let result = OidcSigner::new(config, &dispatch);
            assert!(
                matches!(result, Err(AuthError::Config(_))),
                "{token_url} must be refused"
            );
        }
    }

    #[test]
    fn two_azure_imds_selectors_are_refused_through_oidc_signer_new() {
        // `azure::validate_imds_selector` has its own direct test; this one
        // guards the call site at `OidcSigner::new` itself — a refactor
        // that dropped that call would still pass the direct test but must
        // fail this one.
        let dispatch = permissive_dispatch_client();
        let config = OidcConfig {
            source: OidcSource::AzureImds {
                client_id: Some("client-id".to_string()),
                object_id: Some("object-id".to_string()),
                msi_res_id: None,
            },
            audience: "https://management.azure.com/".to_string(),
        };
        // `OidcSigner` carries no `Debug`, so `expect_err` cannot be used
        // here; `matches!` needs none.
        let result = OidcSigner::new(config, &dispatch);
        assert!(matches!(result, Err(AuthError::Config(_))));
    }

    #[test]
    fn an_empty_audience_is_allowed_for_the_file_source() {
        let dispatch = permissive_dispatch_client();
        let config = OidcConfig {
            source: OidcSource::File {
                path: PathBuf::from("/tmp/does-not-need-to-exist-for-this-check.jwt"),
            },
            audience: String::new(),
        };
        assert!(OidcSigner::new(config, &dispatch).is_ok());
    }

    /// A sanity check on the fixture technique
    /// `the_authorization_header_is_bearer_and_sensitive` relies on: without
    /// this, a change to `expired_jwt` that stopped producing a parseable
    /// token would make that test fail somewhere unrelated to what it means
    /// to check. `file.rs`'s own test module keeps an independent copy of
    /// this same check, guarding its own separate `expired_jwt`.
    #[test]
    fn the_expired_jwt_fixture_is_actually_expired() {
        assert!(jwt::expiry_ms(&expired_jwt("x")).expect("fixture parses") < now_millis());
    }
}
