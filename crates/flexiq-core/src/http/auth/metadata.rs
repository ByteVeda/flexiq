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
use crate::net::{is_link_local_address, is_loopback_address};

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
pub(crate) struct MetadataClient {
    client: reqwest::Client,
    /// Set only by [`Self::with_base_url`]: redirects every [`Self::fetch`]'s
    /// scheme, host and port to a test stub, while keeping the endpoint's
    /// real path — so a test can still assert *which* endpoint a call
    /// reached, not just that some request landed. Always `None` in any
    /// build an operator runs — `with_base_url` is `#[cfg(test)]`, so it is
    /// the only place that can ever produce `Some`. Kept as a plain field
    /// rather than a `#[cfg(test)]` one so `fetch` needs no conditional
    /// compilation of its own to read it.
    base_override: Option<url::Url>,
}

/// The label used for [`MetadataEndpoint::AwsImdsRoleCredentials`], both in
/// [`MetadataEndpoint::label`] and in the one error
/// [`MetadataEndpoint::aws_imds_role_credentials`] can raise before a value
/// of that variant exists to read the label from. One constant so the two
/// can never drift apart.
const AWS_IMDS_ROLE_CREDENTIALS_LABEL: &str = "aws imds role credentials";

/// The closed set of places a credential may be fetched from.
///
/// A variant rather than a URL: there is no parameter on this API through
/// which an operator-supplied host could arrive. Five of these are
/// compile-time constants; three carry an owned [`url::Url`] built at fetch
/// time — two of them, [`Self::AzureAppServiceToken`] and
/// [`Self::AwsContainerCredentials`], from an environment variable already
/// vetted by [`accept_env_endpoint`], which refuses everything the egress
/// guard accepts. The third, [`Self::AwsImdsRoleCredentials`], is not
/// environment-derived at all: [`Self::aws_imds_role_credentials`] builds it
/// from the link-local constant every other AWS/Azure variant here already
/// uses, plus a role name that arrives over the network in IMDS's own
/// response body. That role name cannot move the request off this host —
/// the scheme and authority are fixed before it is ever interpolated, so a
/// hostile value can only choose a different path on the same link-local
/// address — but it is still not an operator- or environment-vetted value,
/// and a reader relying on this doc to answer "can an untrusted host reach
/// this client" should not come away thinking all three are.
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
    /// AWS IMDSv2's per-role credentials call — the third of `imds.rs`'s
    /// three requests. Its path embeds the role name the second request just
    /// returned, so — like the two variants directly above — it carries an
    /// owned [`url::Url`] built at fetch time rather than a compile-time
    /// constant.
    AwsImdsRoleCredentials(url::Url),
}

impl MetadataEndpoint {
    /// The string that goes into [`AuthError::CredentialEndpoint`]'s
    /// `endpoint` field: safe to log, and independent of whatever the
    /// endpoint answered.
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::GoogleIdentity => "gce metadata identity",
            Self::GoogleIdentityByIp => "gce metadata identity (ip fallback)",
            Self::AzureImdsToken => "azure imds token",
            Self::AwsImdsApiToken => "aws imds token",
            Self::AwsImdsSecurityCredentials => "aws imds security credentials",
            Self::AzureAppServiceToken(_) => "azure app service token",
            Self::AwsContainerCredentials(_) => "aws container credentials",
            Self::AwsImdsRoleCredentials(_) => AWS_IMDS_ROLE_CREDENTIALS_LABEL,
        }
    }

    /// Build the per-role IMDSv2 credentials endpoint from a role name that
    /// arrived over the network in IMDS's security-credentials listing.
    ///
    /// Kept here rather than in `sigv4/imds.rs`, alongside
    /// [`CLOUD_METADATA_IP`] and [`AWS_IMDS_ROLE_CREDENTIALS_LABEL`]: the
    /// host, the path and the label all belong to this type, so a change to
    /// any of the three can never leave the others behind.
    pub(crate) fn aws_imds_role_credentials(role: &str) -> Result<Self, AuthError> {
        let built =
            format!("http://{CLOUD_METADATA_IP}/latest/meta-data/iam/security-credentials/{role}");
        url::Url::parse(&built)
            .map(Self::AwsImdsRoleCredentials)
            .map_err(|_| AuthError::CredentialShape {
                endpoint: AWS_IMDS_ROLE_CREDENTIALS_LABEL,
                reason: "role name is not usable in a URL",
            })
    }

    /// The URL this variant fetches from: a freshly parsed constant for the
    /// five built-in variants, or the owned [`url::Url`] carried by the
    /// three that build one at fetch time — see this enum's own doc for how
    /// those three differ from each other.
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
            Self::AzureAppServiceToken(url)
            | Self::AwsContainerCredentials(url)
            | Self::AwsImdsRoleCredentials(url) => return Ok(url.clone()),
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
    ///
    /// `headers` carries a sensitivity flag per entry, routed through the
    /// shared [`insert_header`](super::insert_header) helper exactly like a
    /// signer's own output headers: a credential source that must send a
    /// secret to its metadata endpoint (Azure App Service's
    /// `X-IDENTITY-HEADER`, for one) needs that value kept out of a
    /// `Debug` render of the outgoing request, not just out of this
    /// module's own errors.
    pub(crate) async fn fetch(
        &self,
        endpoint: &MetadataEndpoint,
        method: reqwest::Method,
        query: &[(&str, &str)],
        headers: &[(&str, &str, bool)],
    ) -> Result<String, AuthError> {
        let real = endpoint.url()?;
        let mut url = match &self.base_override {
            // Scheme, host and port come from the stub; the path comes from
            // the endpoint's real URL, or a test could never tell one
            // endpoint's request apart from another's — every variant would
            // resolve to the override's bare `/`.
            Some(base) => {
                let mut overridden = base.clone();
                overridden.set_path(real.path());
                overridden
            }
            None => real,
        };

        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query);
        }

        let response = self
            .client
            .request(method, url)
            .headers(build_headers(headers)?)
            .send()
            .await
            // `without_url` for the same reason the query above is built
            // separately from the endpoint: see `AuthError::Transport`'s doc.
            .map_err(|error| AuthError::Transport(error.without_url().to_string()))?;

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

/// Build the `HeaderMap` [`MetadataClient::fetch`] sends: one call to
/// [`insert_header`](super::insert_header) per entry, so an entry marked
/// sensitive here renders as `Sensitive` in any `Debug` of the outgoing
/// request — the same guarantee a signer's own output headers get.
/// Extracted from `fetch` itself so a test can assert on the built
/// `HeaderMap` directly, with no network round trip.
fn build_headers(headers: &[(&str, &str, bool)]) -> Result<reqwest::header::HeaderMap, AuthError> {
    let mut header_map = reqwest::header::HeaderMap::new();
    for (name, value, sensitive) in headers {
        let header_name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| AuthError::InvalidHeaderValue((*name).to_string()))?;
        super::insert_header(&mut header_map, header_name, value, *sensitive)?;
    }
    Ok(header_map)
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

    // Exactly the two shapes the doc above names — not `is_never_routable`,
    // which is a broader set that also accepts the Alibaba and EC2-v6
    // metadata literals (CGNAT and unique-local respectively, both
    // reachable, not just link-local), multicast, broadcast and
    // unspecified. This function's caller is untrusted input (an operator's
    // environment variable), and accepting any of those would hand a
    // container-network-adjacent host a route to whatever credential this
    // client fetches.
    if is_loopback_address(address) || is_link_local_address(address) {
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
        for raw in [
            "http://93.184.216.34/token",
            "http://10.0.0.5/token",
            "http://192.168.1.1/token",
        ] {
            let error = accept_env_endpoint(raw)
                .expect_err("a routable address is not a credential endpoint");
            assert!(matches!(error, AuthError::Config(_)));
        }
    }

    #[test]
    fn an_env_endpoint_on_a_never_routable_but_non_link_local_address_is_refused() {
        // `is_never_routable` is a *broader* set than "loopback or
        // link-local": it also refuses the Alibaba and EC2-v6 metadata
        // literals (CGNAT and unique-local respectively — both reachable,
        // not just link-local), multicast, broadcast and unspecified. None
        // of those is a credential endpoint. Pinned here so a narrowing of
        // `accept_env_endpoint` back to `is_never_routable` alone — which
        // reads as a harmless simplification, since loopback is already
        // inside it — is caught immediately rather than silently widening
        // what an operator's environment variable can point at.
        for raw in [
            "http://100.100.100.200/token",
            "http://[fd00:ec2::254]/token",
            "http://0.0.0.0/token",
            "http://224.0.0.1/token",
            "http://255.255.255.255/token",
        ] {
            let error = accept_env_endpoint(raw)
                .expect_err(&format!("{raw} is neither loopback nor link-local"));
            assert!(matches!(error, AuthError::Config(_)));
        }
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
    fn an_accepted_credential_endpoint_is_never_egress_permitted() {
        // One-directional and non-circular: every address this function
        // accepts as a credential source must be refused by the egress
        // guard an operator's dispatch target goes through, even under a
        // maximally permissive allowlist. This does not assert the converse
        // — see `an_env_endpoints_accepted_set_matches_the_briefs_enumeration`
        // below for that half, checked against an independently written
        // table rather than against this function's own predicate. (An
        // earlier version of this test asserted "exactly one of the two
        // succeeds" both ways; that is false in general — `https://127.0.0.1/`
        // is refused by both guards — and the false symmetry is what let
        // `accept_env_endpoint` accept more than the doc above promises
        // without this test catching it.)
        let permissive = EgressPolicy::new(
            Allowlist::parse("0.0.0.0/0,::/0").expect("test allowlist parses"),
            false,
        );

        for raw in [
            "http://127.0.0.1:9000/token",
            "http://[::1]:9000/token",
            "http://169.254.169.254/latest/api/token",
            "http://169.254.170.2/v2/credentials/abc",
            "http://169.254.170.23/v1/credentials",
        ] {
            let url = url::Url::parse(raw).expect("test url parses");
            assert!(
                accept_env_endpoint(raw).is_ok(),
                "{raw} is expected to be an accepted credential endpoint"
            );
            assert!(
                !permissive.permits_host(&host_string(&url)),
                "{raw}: accepted as a credential source but also egress-permitted"
            );
        }
    }

    #[test]
    fn an_env_endpoints_accepted_set_matches_the_briefs_enumeration() {
        // Independently written against the brief's stated set (loopback ∪
        // link-local) rather than against `accept_env_endpoint`'s own
        // predicate — the point is to catch the implementation accepting a
        // broader or narrower set than the doc comment above promises, not
        // to restate whatever the implementation already does.
        let cases: &[(&str, bool)] = &[
            // Accept: the real credential endpoints this crate's variants
            // resolve to.
            ("http://127.0.0.1:40000/token", true),
            ("http://169.254.169.254/latest/api/token", true),
            ("http://169.254.170.2/v2/credentials/abc", true),
            ("http://169.254.170.23/v1/credentials", true),
            // Refuse: routable and private.
            ("http://10.0.0.5/token", false),
            ("http://192.168.1.1/token", false),
            // Refuse: routable and public.
            ("http://93.184.216.34/token", false),
            // Refuse: never-routable, but neither loopback nor link-local.
            ("http://100.100.100.200/token", false),
            ("http://[fd00:ec2::254]/token", false),
            ("http://0.0.0.0/token", false),
            ("http://224.0.0.1/token", false),
            ("http://255.255.255.255/token", false),
        ];

        for (raw, should_accept) in cases {
            let accepted = accept_env_endpoint(raw).is_ok();
            assert_eq!(
                accepted, *should_accept,
                "{raw}: expected accept={should_accept}, got {accepted}"
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
                &[("metadata", "true", false)],
            )
            .await
            .expect("a 200 succeeds");
        assert_eq!(body, "token-body");

        let received = stub.received();
        assert_eq!(received.len(), 1);
        let request = &received[0];
        assert_eq!(request.method, "GET");
        // The endpoint's real path, not the override base's bare `/` — this
        // is what lets a test tell `AzureImdsToken` apart from any other
        // variant under the test seam.
        assert!(request
            .target
            .starts_with("/metadata/identity/oauth2/token?"));
        assert!(request.target.contains("api-version=2018-02-01"));
        assert!(request.target.contains("resource=https"));
        assert!(request
            .headers
            .iter()
            .any(|(name, value)| name == "metadata" && value == "true"));
    }

    #[test]
    fn a_sensitive_header_entry_is_marked_sensitive_and_a_plain_one_is_not() {
        let map = build_headers(&[
            ("x-identity-header", "super-secret-value", true),
            ("metadata", "true", false),
        ])
        .expect("headers build");

        let sensitive_value = map.get("x-identity-header").expect("header present");
        assert!(sensitive_value.is_sensitive());

        let plain_value = map.get("metadata").expect("header present");
        assert!(!plain_value.is_sensitive());

        // The `Debug` impl, not the accessor above: this is what actually
        // guards a request-headers dump, and it is the assertion that would
        // catch a caller passing `sensitive: false` for a credential by
        // mistake.
        let rendered = format!("{map:?}");
        assert!(rendered.contains("Sensitive"));
        assert!(!rendered.contains("super-secret-value"));
    }
}
