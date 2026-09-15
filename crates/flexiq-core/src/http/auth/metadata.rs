//! The credential client, and the closed set of endpoints it may reach.
//!
//! [`DispatchClient`](super::super::DispatchClient) refuses link-local and
//! the cloud metadata literals unconditionally — that is the guard the last
//! two commits on this branch built. A credential client must reach exactly
//! those addresses to do its job: OIDC and SigV4 both start with a call to a
//! cloud metadata server. Those two facts coexist only because the
//! separation between them is structural, not configurable: [`MetadataClient`]
//! is a different type from `DispatchClient`, one that cannot be handed an
//! operator's URL at all. If an operator could turn the guard off with a
//! flag, it would not be a guard.

use std::net::IpAddr;
use std::time::Duration;

use super::AuthError;
use crate::net::{is_loopback_address, is_never_routable};

/// The name Google blesses for its metadata server, resolvable only inside
/// GCE/GKE.
const GOOGLE_METADATA_HOST: &str = "metadata.google.internal";

/// Azure IMDS and AWS IMDSv2 both answer here — the exact literal
/// `DispatchClient`'s guard refuses unconditionally, and the address this
/// client exists to reach anyway.
const CLOUD_METADATA_IP: &str = "169.254.169.254";

/// A credential document or a JWT comfortably fits in this many bytes; a
/// response larger than this is not one this client was built to read.
const MAX_BODY_BYTES: usize = 16 * 1024;

/// The client credential fetches use, and the only one in this crate that may
/// reach a link-local address.
///
/// `pub(crate)`: nothing outside this crate can hold one. It takes an
/// endpoint *variant*, never a URL, and its [`Self::fetch`] has no body
/// parameter — so an operator-supplied host has no way in, and a job payload
/// has no way out. That separation is what lets `DispatchClient` refuse the
/// metadata addresses unconditionally while a credential still reaches them.
// No caller yet: the OIDC and SigV4 commits are the first to hold one.
#[allow(dead_code)]
pub(crate) struct MetadataClient {
    client: reqwest::Client,
    /// Set only by [`Self::with_base_url`]: redirects every [`Self::fetch`]
    /// to a test stub instead of the endpoint's real host. Always `None` in
    /// any build an operator runs — `with_base_url` is `#[cfg(test)]`, so it
    /// is the only place that can ever produce `Some`. Kept as a plain field
    /// rather than a `#[cfg(test)]` one so `fetch` needs no conditional
    /// compilation of its own to read it.
    base_override: Option<url::Url>,
}

/// The closed set of places a credential may be fetched from.
///
/// A variant rather than a URL: there is no parameter on this API through
/// which an operator-supplied host could arrive. Five of these are
/// compile-time constants; the two read from the process environment carry
/// their already-vetted [`url::Url`], produced by [`accept_env_endpoint`],
/// which refuses everything the egress guard accepts.
// No caller yet: the OIDC and SigV4 commits are the first to construct one.
#[allow(dead_code)]
pub(crate) enum MetadataEndpoint {
    /// GCE/GKE's identity token endpoint, by name.
    GoogleIdentity,
    /// The same endpoint, by its link-local IP — the fallback for a hardened
    /// resolver that will not resolve `metadata.google.internal`.
    GoogleIdentityByIp,
    /// Azure IMDS's managed-identity token endpoint.
    AzureImdsToken,
    /// AWS IMDSv2's session-token endpoint (the `PUT` that precedes every
    /// other IMDSv2 call).
    AwsImdsApiToken,
    /// AWS IMDSv2's security-credentials listing.
    AwsImdsSecurityCredentials,
    /// Azure App Service's managed-identity endpoint, read from
    /// `IDENTITY_ENDPOINT` and vetted by [`accept_env_endpoint`]. Ordinarily
    /// a loopback high port.
    AzureAppServiceToken(url::Url),
    /// ECS task role or EKS pod identity credentials, read from
    /// `AWS_CONTAINER_CREDENTIALS_FULL_URI` (or the relative-URI variant
    /// resolved against `169.254.170.2`) and vetted by
    /// [`accept_env_endpoint`].
    AwsContainerCredentials(url::Url),
}

impl MetadataEndpoint {
    /// The string that goes into [`AuthError::CredentialEndpoint`]'s
    /// `endpoint` field: safe to log, and independent of whatever the
    /// endpoint answered.
    // No caller yet: see the enum's doc.
    #[allow(dead_code)]
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::GoogleIdentity => "gce metadata identity",
            Self::GoogleIdentityByIp => "gce metadata identity (ip fallback)",
            Self::AzureImdsToken => "azure imds token",
            Self::AwsImdsApiToken => "aws imds token",
            Self::AwsImdsSecurityCredentials => "aws imds security credentials",
            Self::AzureAppServiceToken(_) => "azure app service token",
            Self::AwsContainerCredentials(_) => "aws container credentials",
        }
    }

    /// The URL this variant fetches from: a freshly parsed constant for the
    /// five built-in variants, or the already-vetted URL carried by the two
    /// environment-derived ones.
    fn url(&self) -> Result<url::Url, AuthError> {
        let built = match self {
            Self::GoogleIdentity => format!(
                "http://{GOOGLE_METADATA_HOST}/computeMetadata/v1/instance/service-accounts/default/identity"
            ),
            Self::GoogleIdentityByIp => format!(
                "http://{CLOUD_METADATA_IP}/computeMetadata/v1/instance/service-accounts/default/identity"
            ),
            Self::AzureImdsToken => {
                format!("http://{CLOUD_METADATA_IP}/metadata/identity/oauth2/token")
            }
            Self::AwsImdsApiToken => format!("http://{CLOUD_METADATA_IP}/latest/api/token"),
            Self::AwsImdsSecurityCredentials => {
                format!("http://{CLOUD_METADATA_IP}/latest/meta-data/iam/security-credentials/")
            }
            Self::AzureAppServiceToken(url) | Self::AwsContainerCredentials(url) => {
                return Ok(url.clone())
            }
        };
        // Every built variant above is a fixed, crate-authored string, so
        // this can never actually fail; propagated through `Result` rather
        // than `expect`-ed all the same, matching this crate's rule against
        // `unwrap`/`expect` in library code. Covered by
        // `every_built_in_endpoint_url_parses` below.
        url::Url::parse(&built)
            .map_err(|_| AuthError::Config("built-in metadata URL failed to parse".to_string()))
    }
}

impl MetadataClient {
    /// Build the client credential fetches use.
    // No caller yet: see the struct's doc.
    #[allow(dead_code)]
    pub(crate) fn new() -> Result<Self, AuthError> {
        Ok(Self {
            client: build_client()?,
            base_override: None,
        })
    }

    /// Point the client at a test stub.
    ///
    /// `#[cfg(test)]`: unreachable from any build an operator runs, so this
    /// is a test seam and not the configuration flag this design
    /// deliberately refuses. The whole argument for [`MetadataClient`] being
    /// a distinct, URL-less type falls apart if there is a way to hand it an
    /// arbitrary host — this constructor is not that way, because it does
    /// not exist outside `cargo test`. If it ever became reachable from a
    /// non-test build, it would be exactly the flag this module's doc warns
    /// about.
    #[cfg(test)]
    pub(crate) fn with_base_url(base: url::Url) -> Result<Self, AuthError> {
        Ok(Self {
            client: build_client()?,
            base_override: Some(base),
        })
    }

    /// No host parameter and no body parameter, deliberately: see the
    /// struct's doc for why.
    // No caller yet: see the struct's doc.
    #[allow(dead_code)]
    pub(crate) async fn fetch(
        &self,
        endpoint: &MetadataEndpoint,
        method: reqwest::Method,
        query: &[(&str, &str)],
        headers: &[(&str, &str)],
    ) -> Result<String, AuthError> {
        let mut url = match &self.base_override {
            Some(base) => base.clone(),
            None => endpoint.url()?,
        };

        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query);
        }

        let mut request = self.client.request(method, url);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }

        let response = request
            .send()
            .await
            .map_err(|error| AuthError::Transport(error.to_string()))?;

        let status = response.status();
        if !status.is_success() {
            // Never the body: see `AuthError::CredentialEndpoint`'s doc for
            // why `endpoint` is the only thing this error is allowed to say.
            return Err(AuthError::CredentialEndpoint {
                endpoint: endpoint.label(),
                status: status.as_u16(),
            });
        }

        let body = read_capped(response, MAX_BODY_BYTES).await;
        Ok(String::from_utf8_lossy(&body).into_owned())
    }
}

/// The `reqwest::Client` shared by both of [`MetadataClient`]'s constructors.
fn build_client() -> Result<reqwest::Client, AuthError> {
    reqwest::Client::builder()
        // IMDS is explicitly unsupported behind a proxy, and an HTTP_PROXY in
        // the environment would otherwise send an IMDS session token to
        // whoever runs the proxy.
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        // Off-cloud these endpoints are a black hole or a refusal. Failing
        // fast makes a misconfigured source a clear error rather than a
        // stalled dispatch.
        .connect_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(2))
        .build()
        .map_err(|error| AuthError::Config(error.to_string()))
    // Deliberately no `.dns_resolver(..)`: `PinnedResolver` exists to refuse
    // link-local and the metadata literals, and this client's entire job is
    // to reach them. Pinning it here would refuse every request this type
    // exists to make. That asymmetry with `DispatchClient` is the whole
    // design, not an oversight — see the module doc.
}

/// Reads at most `cap` bytes of `response`'s body and drops the rest.
///
/// A cap of its own, separate from `client::read_bounded`'s: a credential
/// document or a JWT is at most a few KB, and this module has no
/// dispatcher-sized response to plan around.
async fn read_capped(response: reqwest::Response, cap: usize) -> Vec<u8> {
    let mut response = response;
    let mut buffered = Vec::new();
    while buffered.len() < cap {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                let room = cap - buffered.len();
                buffered.extend_from_slice(&chunk[..chunk.len().min(room)]);
            }
            // A clean end of body and a broken connection are both "stop
            // reading" here: `fetch` only ever returns the bytes gathered so
            // far, never a distinct truncation signal, so the two need no
            // separate handling.
            _ => break,
        }
    }
    buffered
}

/// Accept an endpoint URL that arrived from the process environment.
///
/// Deliberately the inverse of the egress allowlist: `http`, and a host that
/// is loopback or link-local — `169.254.170.2` and `169.254.170.23` for ECS
/// and EKS pod identity, `169.254.169.254` for IMDS, and a loopback high port
/// for Azure App Service. Anything routable is refused: an `IDENTITY_ENDPOINT`
/// pointing at the internet is not a credential endpoint.
// No caller yet: the OIDC and SigV4 commits are the first to read
// `IDENTITY_ENDPOINT` / `AWS_CONTAINER_CREDENTIALS_FULL_URI` and hand the
// result here.
#[allow(dead_code)]
pub(crate) fn accept_env_endpoint(raw: &str) -> Result<url::Url, AuthError> {
    let url = url::Url::parse(raw)
        .map_err(|_| AuthError::Config("credential endpoint is not a URL".to_string()))?;

    if url.scheme() != "http" {
        return Err(AuthError::Config(format!(
            "credential endpoint must use http, not {}",
            url.scheme()
        )));
    }

    // `Url::host()` returns a typed, already-unbracketed address for an IP
    // literal; `host_str()` would keep the brackets on an IPv6 literal
    // (`"[::1]"`), which is not a shape `IpAddr::parse` accepts. Using the
    // typed accessor sidesteps that pitfall entirely rather than working
    // around it.
    let address = match url.host() {
        Some(url::Host::Ipv4(v4)) => IpAddr::V4(v4),
        Some(url::Host::Ipv6(v6)) => IpAddr::V6(v6),
        // A name needs DNS, which is not one of the two shapes a credential
        // endpoint may take: it is either the host's own loopback or a
        // link-local address the container network assigned it, never a
        // name that resolves to either.
        Some(url::Host::Domain(name)) => {
            return Err(AuthError::Config(format!(
                "credential endpoint '{name}' is a name, not loopback or link-local"
            )))
        }
        None => {
            return Err(AuthError::Config(
                "credential endpoint has no host".to_string(),
            ))
        }
    };

    // `is_never_routable` already covers loopback; `is_loopback_address` is
    // named here too so the code reads as the two shapes the doc above
    // names, rather than silently folding one into the other. Anything this
    // rejects is rejected for being routable — the metadata literals and the
    // rest of `is_never_routable`'s set (multicast, broadcast, unspecified)
    // are accepted too, but no variant in this file ever puts one of those
    // in the environment.
    if is_loopback_address(address) || is_never_routable(address) {
        Ok(url)
    } else {
        Err(AuthError::Config(format!(
            "credential endpoint '{address}' is routable, not loopback or link-local"
        )))
    }
}

#[cfg(test)]
mod tests {
    use crate::http::EgressPolicy;
    use crate::net::Allowlist;

    use super::*;

    fn host_string(url: &url::Url) -> String {
        match url.host().expect("test url has a host") {
            url::Host::Domain(domain) => domain.to_string(),
            url::Host::Ipv4(v4) => v4.to_string(),
            url::Host::Ipv6(v6) => v6.to_string(),
        }
    }

    #[test]
    fn every_built_in_endpoint_url_parses() {
        for endpoint in [
            MetadataEndpoint::GoogleIdentity,
            MetadataEndpoint::GoogleIdentityByIp,
            MetadataEndpoint::AzureImdsToken,
            MetadataEndpoint::AwsImdsApiToken,
            MetadataEndpoint::AwsImdsSecurityCredentials,
        ] {
            assert!(
                endpoint.url().is_ok(),
                "{} must build a valid URL",
                endpoint.label()
            );
        }
    }

    #[test]
    fn an_env_endpoint_on_loopback_is_accepted() {
        assert!(accept_env_endpoint("http://127.0.0.1:9000/token").is_ok());
        assert!(accept_env_endpoint("http://[::1]:9000/token").is_ok());
    }

    #[test]
    fn an_env_endpoint_on_link_local_is_accepted() {
        for raw in [
            "http://169.254.169.254/latest/api/token",
            "http://169.254.170.2/v2/credentials/abc",
            "http://169.254.170.23/v1/credentials",
        ] {
            assert!(accept_env_endpoint(raw).is_ok(), "{raw} must be accepted");
        }
    }

    #[test]
    fn an_env_endpoint_on_a_routable_address_is_refused() {
        let error = accept_env_endpoint("http://93.184.216.34/token")
            .expect_err("a public address is not a credential endpoint");
        assert!(matches!(error, AuthError::Config(_)));
    }

    #[test]
    fn an_env_endpoint_that_is_a_name_is_refused() {
        let error = accept_env_endpoint("http://metadata.internal/token")
            .expect_err("a name needs DNS, which is not one of the two accepted shapes");
        assert!(matches!(error, AuthError::Config(_)));
    }

    #[test]
    fn an_env_endpoint_with_an_https_scheme_is_refused() {
        let error = accept_env_endpoint("https://169.254.169.254/token")
            .expect_err("only http is accepted");
        assert!(matches!(error, AuthError::Config(_)));
    }

    #[test]
    fn a_bracketed_ipv6_env_endpoint_is_read_correctly() {
        let url = accept_env_endpoint("http://[::1]:1234/token")
            .expect("a bracketed IPv6 loopback literal is accepted");
        assert_eq!(host_string(&url), "::1");
        assert_eq!(url.port(), Some(1234));
    }

    #[test]
    fn the_guards_are_inverses() {
        // Permissive on the allowlist dimension (every address, both
        // families), but not on the loopback relaxation: this is the "an
        // operator would allow anything" policy the invariant below is
        // measured against.
        let permissive = EgressPolicy::new(
            Allowlist::parse("0.0.0.0/0,::/0").expect("test allowlist parses"),
            false,
        );

        // IP-literal hosts only: a name is refused by both functions for
        // different reasons (one has no allowlist rule that matches a bare
        // name, the other refuses DNS outright), which would make both sides
        // false and break the invariant below. That combination is already
        // covered by `an_env_endpoint_that_is_a_name_is_refused`.
        let rows = [
            "http://127.0.0.1:9000/token",
            "http://[::1]:9000/token",
            "http://169.254.169.254/latest/api/token",
            "http://169.254.170.2/v2/credentials/abc",
            "http://169.254.170.23/v1/credentials",
            "http://93.184.216.34/token",
            "https://93.184.216.34/token",
            "http://8.8.8.8/token",
        ];

        for raw in rows {
            let url = url::Url::parse(raw).expect("test url parses");
            let egress_permits = permissive.permits_host(&host_string(&url));
            let credential_accepts = accept_env_endpoint(raw).is_ok();
            assert_ne!(
                egress_permits, credential_accepts,
                "{raw}: egress permits={egress_permits}, credential accepts={credential_accepts} — exactly one must be true"
            );
        }
    }

    #[tokio::test]
    async fn a_non_2xx_names_the_endpoint_and_not_the_body() {
        let stub = crate::http::testing::StubServer::start(500, "leaked-secret-body").await;
        let client = MetadataClient::with_base_url(
            url::Url::parse(&stub.base_url()).expect("stub url parses"),
        )
        .expect("test client builds");

        let error = client
            .fetch(
                &MetadataEndpoint::GoogleIdentity,
                reqwest::Method::GET,
                &[],
                &[],
            )
            .await
            .expect_err("a 500 must be refused");

        match &error {
            AuthError::CredentialEndpoint { endpoint, status } => {
                assert_eq!(*status, 500);
                assert_eq!(*endpoint, MetadataEndpoint::GoogleIdentity.label());
            }
            other => panic!("expected CredentialEndpoint, got {other:?}"),
        }
        let message = error.to_string();
        assert!(message.contains(MetadataEndpoint::GoogleIdentity.label()));
        assert!(!message.contains("leaked-secret-body"));
    }

    #[tokio::test]
    async fn a_fetch_sends_the_query_and_headers_it_was_given() {
        let stub = crate::http::testing::StubServer::start(200, "token-body").await;
        let client = MetadataClient::with_base_url(
            url::Url::parse(&stub.base_url()).expect("stub url parses"),
        )
        .expect("test client builds");

        let body = client
            .fetch(
                &MetadataEndpoint::AzureImdsToken,
                reqwest::Method::GET,
                &[
                    ("api-version", "2018-02-01"),
                    ("resource", "https://example.com"),
                ],
                &[("metadata", "true")],
            )
            .await
            .expect("a 200 succeeds");
        assert_eq!(body, "token-body");

        let received = stub.received();
        assert_eq!(received.len(), 1);
        let request = &received[0];
        assert_eq!(request.method, "GET");
        assert!(request.target.contains("api-version=2018-02-01"));
        assert!(request.target.contains("resource=https"));
        assert!(request
            .headers
            .iter()
            .any(|(name, value)| name == "metadata" && value == "true"));
    }
}
