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
//!   compile-time constant or an environment-supplied link-local address —
//!   never a value an operator typed in — so all three fetch through
//!   [`MetadataClient`], the type that is structurally incapable of being
//!   pointed at an arbitrary host (see `metadata.rs`'s module doc).
//! - [`OidcSource::OAuth2ClientCredentials`] dials a token URL the operator
//!   configured, exactly the same kind of input the dispatch target's own
//!   URL is, so it fetches through the guarded
//!   [`DispatchClient`](crate::http::DispatchClient) instead, subject to the
//!   same egress allowlist as the dispatch target itself — see `oauth2.rs`'s
//!   module doc for the full argument. That asymmetry, three sources on one
//!   client and one source on the other, is the load-bearing design decision
//!   in this commit.
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
        /// once at construction.
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
    /// Required for every source but [`OidcSource::File`]: a projected
    /// token was already minted, `aud` and all, by whoever issues it, and
    /// there is no wire request here for this field to attach to. That
    /// asymmetry is deliberate — enforced in `OidcSigner::new` rather than
    /// allowed silently for every source.
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

        if !matches!(source, OidcSource::File { .. }) && audience.is_empty() {
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
                // Parsed once, here, rather than on every fetch: an invalid
                // URL is a configuration mistake, and failing at
                // construction beats every dispatch being refused with no
                // explanation on our side — the same argument
                // `OutboundAuth::signer`'s empty-secret checks make.
                let token_url = url::Url::parse(&token_url).map_err(|_| {
                    AuthError::Config("oauth2 token_url is not a valid URL".to_string())
                })?;
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
            Resolved::File { path } => fetch_file(path).await,
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

/// Read the file at `path`, trimming trailing whitespace — a kubelet's
/// projected token file ends in a newline — and re-reading it in full on
/// every refresh: the kubelet rotates the file in place, so a token cached
/// from the first read would outlive its own file.
async fn fetch_file(path: &std::path::Path) -> Result<Expiring<String>, AuthError> {
    let raw = tokio::fs::read_to_string(path).await.map_err(|error| {
        AuthError::Config(format!(
            "could not read projected token at {}: {error}",
            path.display()
        ))
    })?;
    let token = raw.trim_end().to_string();
    let expires_at_ms = jwt::expiry_ms(&token)?;
    Ok(expiring_from_expiry_ms(token, expires_at_ms))
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

    #[tokio::test]
    async fn a_file_source_rereads_on_refresh() {
        let mut file = tempfile::NamedTempFile::new().expect("temp file creates");
        write!(file, "{}", expired_jwt("first")).expect("temp file writes");

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

        let first = signer
            .sign(&request)
            .await
            .expect("first sign reads the file");
        let first_header = first
            .get(AUTHORIZATION)
            .expect("authorization header present")
            .to_str()
            .expect("header value is ascii")
            .to_string();
        assert!(first_header.contains(&expired_jwt("first")));

        // The token above is already expired (`exp` is in the past), so the
        // cache is due for refresh on the very next call — no sleep needed.
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(file.path())
            .expect("temp file reopens for overwrite");
        write!(file, "{}", expired_jwt("second")).expect("temp file overwrites");

        let second = signer
            .sign(&request)
            .await
            .expect("second sign re-reads the file");
        let second_header = second
            .get(AUTHORIZATION)
            .expect("authorization header present")
            .to_str()
            .expect("header value is ascii")
            .to_string();
        assert!(second_header.contains(&expired_jwt("second")));
        assert_ne!(first_header, second_header);
    }

    /// A sanity check on the fixture technique
    /// `the_authorization_header_is_bearer_and_sensitive` and
    /// `a_file_source_rereads_on_refresh` both rely on: without this, a
    /// change to `expired_jwt` that stopped producing an already-expired
    /// token would make `a_file_source_rereads_on_refresh` flaky rather than
    /// fail outright.
    #[test]
    fn the_expired_jwt_fixture_is_actually_expired() {
        assert!(jwt::expiry_ms(&expired_jwt("x")).expect("fixture parses") < now_millis());
    }
}
