//! Generic OAuth2 client-credentials — the only off-cloud OIDC source, and
//! the reason [`OutboundAuth::signer`](crate::http::auth::OutboundAuth::signer)
//! grew a `&DispatchClient` parameter in this commit.
//!
//! Google, Azure IMDS and Azure App Service all reach a host that is either a
//! compile-time constant (`metadata.google.internal`, `169.254.169.254`) or
//! read from a trusted environment variable and vetted by
//! [`accept_env_endpoint`](crate::http::auth::metadata::accept_env_endpoint).
//! This source's `token_url` is neither: it is a value the *operator* typed
//! into their push-dispatch configuration, exactly the same kind of input
//! the dispatch target's own URL is. It therefore goes through the same
//! guarded [`DispatchClient`](crate::http::DispatchClient) and the same
//! egress allowlist as the dispatch target — never through
//! [`MetadataClient`](crate::http::auth::metadata::MetadataClient), which by
//! design cannot be pointed at an arbitrary host at all. That asymmetry,
//! three sources on `MetadataClient` and this one on `DispatchClient`, is
//! the load-bearing design decision in this commit.

use url::form_urlencoded;

use crate::http::auth::cache::Expiring;
use crate::http::auth::AuthError;
use crate::worker::Secret;

/// The label every [`AuthError::CredentialEndpoint`]/[`AuthError::CredentialShape`]
/// produced here carries. Not a [`MetadataEndpoint`](crate::http::auth::metadata::MetadataEndpoint)
/// variant — there is no closed set of operator-supplied token URLs to
/// enumerate — so this is a fixed literal rather than the URL itself, which
/// would otherwise put an operator-chosen value into a log line.
const LABEL: &str = "oidc oauth2 token endpoint";

/// How the client authenticates itself to the token endpoint, per RFC 6749
/// §2.3.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientAuthStyle {
    /// `client_id`/`client_secret` in the form body.
    ClientSecretPost,
    /// `Authorization: Basic base64(urlencode(id) + ":" + urlencode(secret))`;
    /// the form body carries neither.
    ClientSecretBasic,
}

#[derive(serde::Deserialize)]
struct TokenResponse {
    access_token: String,
    /// A number here, unlike Azure's quoted `expires_in` — see `azure.rs`'s
    /// `AzureTokenResponse` for the contrast this module deliberately does
    /// not share a type with.
    expires_in: i64,
}

/// Parse and vet the operator-supplied token URL.
///
/// Every check here is a construction-time one, so a misconfiguration stops
/// the process at boot rather than failing the first dispatch an hour later —
/// the same argument [`OutboundAuth::signer`](crate::http::auth::OutboundAuth::signer)'s
/// empty-secret checks make.
///
/// Four rules:
///
/// - **A URL at all.** Anything `url::Url` cannot parse is a typo.
/// - **`https`, or `http` only to a loopback host *with the loopback
///   relaxation enabled*.** The client secret goes to this endpoint in a form
///   body (or base64 in an `Authorization: Basic` header, which is encoding,
///   not encryption), and the access token comes back the same way, so
///   cleartext to anywhere but the host this process is already running on
///   puts a credential on the wire. Both halves are load-bearing: `localhost`
///   is matched as a *name* here, and a name rule on the allowlist vouches for
///   whatever it resolves to, so without `allow_loopback` a resolver answering
///   a public address for `localhost` would turn this into cleartext to a
///   remote host. This is the same rule the push dispatch target's own URL
///   obeys, for the same reason, and it is deliberately *not* the rule
///   [`accept_env_endpoint`](crate::http::auth::metadata::accept_env_endpoint)
///   applies: that one vets a credential endpoint the *platform* placed in the
///   environment, which is `http` on loopback or link-local by construction,
///   whereas this is a URL an operator typed.
/// - **No userinfo.** RFC 6749 §2.3.1 has no place for credentials in the URL
///   itself. Refused rather than merely discouraged, because a `Transport`
///   error on a failed fetch carries reqwest's `Display` of the request URL
///   verbatim — userinfo included — into `AuthError`, and from there into
///   whatever logs that error. The credential is never actually put on the
///   wire this way (reqwest does not turn URL userinfo into an
///   `Authorization` header), so this is a log-leak guard, not a
///   transport-security one.
/// - **A host the egress allowlist permits.** The fetch dials through the
///   guarded client, whose pinned resolver vets whatever a *name* resolves to
///   — but an IP-literal host never reaches a resolver at all, because the
///   connector recognizes it as an address and dials it directly. Without a
///   check here, `https://10.1.2.3/token` would reach a host the operator
///   never allowlisted. This is the same gate, for the same reason, that
///   `worker::http_target::validate_target_url` applies to the dispatch URL,
///   and it means **an OAuth2 token endpoint has to be on the same allowlist
///   as the dispatch target**.
///
/// `localhost` counts as loopback without being resolved: RFC 6761 §6.3
/// reserves the name for the loopback interface, and there is nothing to
/// resolve at construction time. Errors name no part of the URL — an
/// operator's token URL may carry a tenant key in its query — which is the
/// same discipline [`LABEL`] exists for; the allowlist refusal is the one
/// exception and names the **host only**, because an operator cannot fix an
/// allowlist they are not told the missing entry for, and a host is not the
/// part of a URL a credential hides in.
pub(super) fn validate_token_url(
    raw: &str,
    policy: &crate::http::EgressPolicy,
) -> Result<url::Url, AuthError> {
    let url = url::Url::parse(raw)
        .map_err(|_| AuthError::Config("oauth2 token_url is not a valid URL".to_string()))?;

    match url.scheme() {
        "https" => {}
        // Both halves, not just the host — the same pair
        // `worker::http_target`'s `cleartext_permitted` requires. The host
        // alone is not enough because `localhost` is taken at its *name* here
        // (nothing is resolved at construction), and a name rule on the
        // allowlist vouches for whatever that name resolves to. With the
        // relaxation off but `localhost` allowlisted, a resolver answering a
        // public address would put the client secret on the wire in the clear.
        // Requiring the knob is what makes "cleartext is only ever reaching
        // this host" true rather than merely likely.
        "http" if is_loopback_host(&url) && policy.allows_loopback() => {}
        _ => {
            return Err(AuthError::Config(
                "oauth2 token_url must use https; http is accepted only for a loopback host \
                 (127.0.0.0/8, ::1, or the name 'localhost') and only with the loopback \
                 relaxation enabled"
                    .to_string(),
            ))
        }
    }

    if !url.username().is_empty() || url.password().is_some() {
        return Err(AuthError::Config(
            "oauth2 token_url must not carry userinfo".to_string(),
        ));
    }

    // Read through `url::Host` rather than `host_str`: the latter keeps an
    // IPv6 literal's brackets, and `permits_host` expects the unbracketed form
    // every other caller gives it. The same reading `validate_target_url` does.
    let host = match url.host() {
        Some(url::Host::Domain(domain)) => domain.to_string(),
        Some(url::Host::Ipv4(v4)) => v4.to_string(),
        Some(url::Host::Ipv6(v6)) => v6.to_string(),
        None => {
            return Err(AuthError::Config(
                "oauth2 token_url must include a hostname".to_string(),
            ))
        }
    };
    if !policy.permits_host(&host) {
        return Err(AuthError::Config(format!(
            "oauth2 token_url host '{host}' is not on the egress allowlist — a token \
             endpoint is dialled through the same guard as the dispatch target, so it \
             has to be named there too"
        )));
    }

    Ok(url)
}

/// Whether `url`'s host is the loopback interface.
///
/// Read through `Url::host()` rather than `host_str()`: the typed accessor
/// hands back an already-unbracketed address, and `"[::1]"` is not a shape
/// `IpAddr::parse` accepts.
fn is_loopback_host(url: &url::Url) -> bool {
    match url.host() {
        Some(url::Host::Ipv4(v4)) => crate::net::is_loopback_address(std::net::IpAddr::V4(v4)),
        Some(url::Host::Ipv6(v6)) => crate::net::is_loopback_address(std::net::IpAddr::V6(v6)),
        Some(url::Host::Domain(name)) => {
            let normalized = name.strip_suffix('.').unwrap_or(name);
            normalized.eq_ignore_ascii_case("localhost")
        }
        None => false,
    }
}

/// POST a client-credentials grant to `token_url` through `client` — the
/// guarded client's own `reqwest::Client`, per this module's doc — and
/// return the access token it answers with.
pub(super) async fn fetch(
    client: &reqwest::Client,
    token_url: &url::Url,
    client_id: &str,
    client_secret: &Secret,
    scope: Option<&str>,
    style: ClientAuthStyle,
    audience: &str,
) -> Result<Expiring<String>, AuthError> {
    let body = build_form(style, client_id, client_secret, scope, audience);

    let mut request = client
        .post(token_url.clone())
        .header(
            reqwest::header::CONTENT_TYPE,
            "application/x-www-form-urlencoded",
        )
        .body(body);
    if style == ClientAuthStyle::ClientSecretBasic {
        request = request.header(
            reqwest::header::AUTHORIZATION,
            basic_auth_header(client_id, client_secret),
        );
    }

    let response = request
        .send()
        .await
        // `without_url`, never the bare `Display`: reqwest interpolates the
        // URL it was dialling, and the operator's token URL may carry a
        // credential in its query. See `AuthError::Transport`'s own doc —
        // the invariant is the variant's, not this call site's.
        .map_err(|error| AuthError::Transport(error.without_url().to_string()))?;

    let status = response.status();
    if !status.is_success() {
        return Err(AuthError::CredentialEndpoint {
            endpoint: LABEL,
            status: status.as_u16(),
        });
    }

    let body = read_capped(response).await?;
    let parsed: TokenResponse =
        serde_json::from_slice(&body).map_err(|_| AuthError::CredentialShape {
            endpoint: LABEL,
            reason: "not valid JSON",
        })?;

    Ok(super::expiring_from_ttl_seconds(
        parsed.access_token,
        parsed.expires_in,
    ))
}

/// A token response comfortably fits in this many bytes — the same limit
/// `metadata.rs`'s own credential fetch enforces (`MAX_BODY_BYTES`), and for
/// an analogous reason: `response.text()` alone would buffer whatever the
/// token endpoint chose to send with no cap at all, and this is still a
/// credential document, not a dispatch payload with its own
/// operator-configured cap.
const MAX_BODY_BYTES: usize = 16 * 1024;

/// Reads at most [`MAX_BODY_BYTES`] of `response`'s body and drops the rest.
///
/// A separate copy of `metadata.rs`'s own `read_capped` rather than a shared
/// one: that one is private to `MetadataClient`, and this path reaches an
/// operator's token URL through the guarded `DispatchClient` instead — a
/// different trust boundary, sharing only the cap's rationale, not its
/// implementation.
///
/// **A clean end of body and a broken connection are not the same thing
/// here**, mirroring `metadata.rs`'s own `read_capped` for the same reason:
/// returning the bytes gathered so far would let a half-read token response
/// be parsed as if it were whole, and would report a retryable network
/// failure as a permanent [`AuthError::CredentialShape`].
async fn read_capped(response: reqwest::Response) -> Result<Vec<u8>, AuthError> {
    let mut response = response;
    let mut buffered = Vec::new();
    while buffered.len() < MAX_BODY_BYTES {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                let room = MAX_BODY_BYTES - buffered.len();
                buffered.extend_from_slice(&chunk[..chunk.len().min(room)]);
            }
            Ok(None) => break,
            // `without_url`, never the bare `Display`, for the reason
            // `AuthError::Transport`'s own doc gives: the operator's token
            // URL may carry a credential in its query.
            Err(error) => {
                return Err(AuthError::Transport(error.without_url().to_string()));
            }
        }
    }
    Ok(buffered)
}

/// The client-credentials form body: `grant_type` always, `client_id` and
/// `client_secret` only under [`ClientAuthStyle::ClientSecretPost`] — RFC
/// 6749 §2.3.1 is explicit that a client authenticating with `Basic` must
/// not also present the secret in the body — `scope` when configured, and
/// `audience` when configured (the Auth0/Okta convention for an aud-bound
/// token, sent only when the caller asked for one).
fn build_form(
    style: ClientAuthStyle,
    client_id: &str,
    client_secret: &Secret,
    scope: Option<&str>,
    audience: &str,
) -> String {
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    serializer.append_pair("grant_type", "client_credentials");
    if style == ClientAuthStyle::ClientSecretPost {
        // Unwrapped only for the duration of this call, the same discipline
        // `BearerSigner::sign` applies to its own token.
        let secret = String::from_utf8_lossy(client_secret.expose_secret());
        serializer.append_pair("client_id", client_id);
        serializer.append_pair("client_secret", secret.as_ref());
    }
    if let Some(scope) = scope {
        serializer.append_pair("scope", scope);
    }
    if !audience.is_empty() {
        serializer.append_pair("audience", audience);
    }
    serializer.finish()
}

/// `Basic base64(urlencode(id) + ":" + urlencode(secret))`.
///
/// Both halves are form-urlencoded *before* concatenation and base64, per
/// RFC 6749 §2.3.1 — skipping the urlencode step only breaks for a secret
/// containing a byte outside the unreserved set (`+`, `/`, and friends),
/// which is exactly why `basic_form_urlencodes_both_halves_before_base64`
/// below uses one that does.
fn basic_auth_header(client_id: &str, client_secret: &Secret) -> String {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;

    let encoded_id: String = form_urlencoded::byte_serialize(client_id.as_bytes()).collect();
    let encoded_secret: String =
        form_urlencoded::byte_serialize(client_secret.expose_secret()).collect();
    let credentials = format!("{encoded_id}:{encoded_secret}");
    format!("Basic {}", STANDARD.encode(credentials))
}

#[cfg(test)]
mod tests {
    use reqwest::header::HeaderMap;

    use super::*;
    use crate::http::auth::{Signer, SigningRequest};
    use crate::http::testing::StubServer;
    use crate::http::{DispatchClient, EgressPolicy};
    use crate::net::Allowlist;
    use std::sync::Arc;
    use std::time::Duration;

    fn token_url(stub: &StubServer) -> url::Url {
        url::Url::parse(&format!("{}/token", stub.base_url())).expect("stub url parses")
    }

    /// A policy that permits every host the transport tests use, so those
    /// tests fail on the rule they are about rather than on the allowlist.
    fn permissive_policy() -> EgressPolicy {
        EgressPolicy::new(
            Allowlist::parse(
                "issuer.example.com,10.0.0.0/8,2606:4700::/32,127.0.0.0/8,::1,localhost",
            )
            .expect("test allowlist parses"),
            true,
        )
    }

    #[test]
    fn a_cleartext_token_url_is_refused_unless_it_is_loopback() {
        // The client secret goes to this endpoint in a form body and the
        // access token comes back in the response; neither may cross a
        // network in the clear.
        let policy = permissive_policy();
        for refused in [
            "http://issuer.example.com/token",
            "http://10.1.2.3:8080/token",
            "http://[2606:4700::1111]/token",
            "ftp://issuer.example.com/token",
            "not a url",
        ] {
            assert!(
                matches!(
                    validate_token_url(refused, &policy),
                    Err(AuthError::Config(_))
                ),
                "{refused} must be refused at construction"
            );
        }

        for accepted in [
            "https://issuer.example.com/token",
            "http://127.0.0.1:8080/token",
            "http://[::1]:8080/token",
            "http://localhost:8080/token",
            "http://LocalHost.:8080/token",
        ] {
            assert!(
                validate_token_url(accepted, &policy).is_ok(),
                "{accepted} is https or a loopback sidecar, both of which stay reachable"
            );
        }

        // Without the loopback relaxation, *every* cleartext URL is refused —
        // loopback included. The host alone does not buy it, because
        // `localhost` is matched as a name and a name rule vouches for
        // whatever it resolves to: with the knob off and `localhost`
        // allowlisted, a resolver answering a public address would otherwise
        // have put the client secret on the wire in the clear.
        let strict = EgressPolicy::new(
            Allowlist::parse("issuer.example.com,127.0.0.0/8,::1,localhost")
                .expect("test allowlist parses"),
            false,
        );
        for refused in [
            "http://127.0.0.1:8080/token",
            "http://[::1]:8080/token",
            "http://localhost:8080/token",
        ] {
            let error = match validate_token_url(refused, &strict) {
                Err(error) => error,
                Ok(_) => panic!("{refused} is cleartext without the loopback relaxation"),
            };
            assert!(
                error.to_string().contains("relaxation"),
                "the refusal must say which half is missing: {error}"
            );
        }
        assert!(
            validate_token_url("https://issuer.example.com/token", &strict).is_ok(),
            "the knob gates cleartext only; https is unaffected"
        );
    }

    /// The token URL is the second operator-supplied host in this subsystem,
    /// and it used to pass no allowlist check at all. A *name* would still be
    /// caught at fetch time by the pinned resolver, but an IP literal never
    /// reaches a resolver — the connector dials it directly — so nothing
    /// vetted `https://10.1.2.3/token` before the client secret went to it.
    #[test]
    fn a_token_url_host_off_the_allowlist_is_refused_at_construction() {
        let policy = EgressPolicy::new(
            Allowlist::parse("issuer.example.com").expect("test allowlist parses"),
            false,
        );

        for refused in [
            "https://elsewhere.example.com/token",
            // The literal: the case the resolver can never catch.
            "https://10.1.2.3/token",
            "https://[2606:4700::1111]/token",
            // Unconditionally refused whatever the allowlist says, and the
            // loopback relaxation is off here.
            "https://169.254.169.254/token",
            "https://127.0.0.1/token",
        ] {
            let error = match validate_token_url(refused, &policy) {
                Err(error) => error,
                Ok(_) => panic!("{refused} must be refused at construction"),
            };
            assert!(matches!(error, AuthError::Config(_)), "{error:?}");
            assert!(
                error.to_string().contains("allowlist"),
                "the refusal must say which guard refused it: {error}"
            );
        }

        assert!(
            validate_token_url("https://issuer.example.com/token", &policy).is_ok(),
            "the host the operator did allowlist stays reachable"
        );
    }

    #[test]
    fn a_refused_token_url_never_appears_in_the_error() {
        // The same invariant `LABEL` exists for: an operator's token URL may
        // carry a tenant key in its query, and this error reaches a log. The
        // allowlist refusal names the host deliberately (an operator cannot
        // fix a list they are not told the missing entry for) and is checked
        // separately below.
        let error = validate_token_url(
            "http://issuer.example.com/token?tenant=q1w2e3r4",
            &permissive_policy(),
        )
        .expect_err("cleartext off loopback is refused");

        let rendered = error.to_string();
        assert!(!rendered.contains("q1w2e3r4"), "{rendered}");
        assert!(!rendered.contains("issuer.example.com"), "{rendered}");

        // The one refusal that does name a host still names only the host —
        // never the path, never the query.
        let policy = EgressPolicy::new(
            Allowlist::parse("elsewhere.example.com").expect("test allowlist parses"),
            false,
        );
        let rendered =
            validate_token_url("https://issuer.example.com/token?tenant=q1w2e3r4", &policy)
                .expect_err("a host off the allowlist is refused")
                .to_string();
        assert!(rendered.contains("issuer.example.com"), "{rendered}");
        assert!(!rendered.contains("q1w2e3r4"), "{rendered}");
        assert!(!rendered.contains("/token"), "{rendered}");
    }

    fn permissive_dispatch_client() -> DispatchClient {
        let policy = Arc::new(EgressPolicy::new(
            Allowlist::parse("0.0.0.0/0,::/0").expect("test allowlist parses"),
            true,
        ));
        DispatchClient::new(policy, Duration::from_secs(5))
            .expect("a permissive policy and a timeout are enough to build a client")
    }

    fn empty_request() -> (url::Url, HeaderMap) {
        (
            url::Url::parse("https://push.example.com/hook").expect("test url parses"),
            HeaderMap::new(),
        )
    }

    /// Regression, the twin of `metadata.rs`'s own: the loop used to read a
    /// mid-body break as the end of the body, so a half-delivered token
    /// response was parsed as if it were whole and a retryable network
    /// failure was reported as a permanent `CredentialShape`.
    #[tokio::test]
    async fn a_token_body_that_breaks_mid_read_is_a_transport_error() {
        let stub = StubServer::start_truncating(200, r#"{"access_token":"tok","expire"#, 64).await;

        // `Expiring<String>` carries no `Debug` — it holds a credential — so
        // `expect_err` cannot be used here; the `Ok` arm is unwrapped by hand.
        let result = fetch(
            &reqwest::Client::new(),
            &token_url(&stub),
            "id",
            &Secret::new("secret"),
            None,
            ClientAuthStyle::ClientSecretPost,
            "",
        )
        .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("a body that ends before its Content-Length is not a complete answer"),
        };

        assert!(
            matches!(error, AuthError::Transport(_)),
            "expected Transport, got {error:?}"
        );
        assert!(
            error.retryable(),
            "a broken connection is worth another attempt"
        );
    }

    #[tokio::test]
    async fn the_form_body_is_client_credentials() {
        let stub = StubServer::start(200, r#"{"access_token":"tok","expires_in":3600}"#).await;

        fetch(
            &reqwest::Client::new(),
            &token_url(&stub),
            "my-client-id",
            &Secret::new("my-client-secret"),
            Some("push:dispatch"),
            ClientAuthStyle::ClientSecretPost,
            "",
        )
        .await
        .expect("a well-formed response succeeds");

        let received = stub.received();
        assert_eq!(received.len(), 1);
        let request = &received[0];
        assert_eq!(request.target, "/token");
        assert_eq!(
            String::from_utf8(request.body.clone()).expect("body is utf8"),
            "grant_type=client_credentials&client_id=my-client-id&client_secret=my-client-secret&scope=push%3Adispatch"
        );
        assert!(request
            .headers
            .iter()
            .any(|(name, value)| name == "content-type"
                && value == "application/x-www-form-urlencoded"));
    }

    #[tokio::test]
    async fn client_secret_basic_sends_a_basic_header_and_no_secret_in_the_body() {
        let stub = StubServer::start(200, r#"{"access_token":"tok","expires_in":3600}"#).await;

        fetch(
            &reqwest::Client::new(),
            &token_url(&stub),
            "basic-client-id",
            &Secret::new("basic-client-secret"),
            None,
            ClientAuthStyle::ClientSecretBasic,
            "",
        )
        .await
        .expect("a well-formed response succeeds");

        let received = stub.received();
        assert_eq!(received.len(), 1);
        let request = &received[0];
        let body = String::from_utf8(request.body.clone()).expect("body is utf8");
        assert_eq!(body, "grant_type=client_credentials");
        assert!(!body.contains("basic-client-secret"));
        assert!(!body.contains("client_secret"));

        let auth_header = request
            .headers
            .iter()
            .find(|(name, _)| name == "authorization")
            .map(|(_, value)| value.clone())
            .expect("authorization header present");
        assert!(auth_header.starts_with("Basic "));
    }

    #[tokio::test]
    async fn basic_form_urlencodes_both_halves_before_base64() {
        // Contains both `+` and `/` — neither is in the unreserved set, so an
        // implementation that skips the urlencode step and only base64s the
        // raw bytes produces a different (and wrong) header value.
        let stub = StubServer::start(200, r#"{"access_token":"tok","expires_in":3600}"#).await;

        fetch(
            &reqwest::Client::new(),
            &token_url(&stub),
            "cli+ent/id",
            &Secret::new("sec+ret/val"),
            None,
            ClientAuthStyle::ClientSecretBasic,
            "",
        )
        .await
        .expect("a well-formed response succeeds");

        let received = stub.received();
        let auth_header = received[0]
            .headers
            .iter()
            .find(|(name, _)| name == "authorization")
            .map(|(_, value)| value.clone())
            .expect("authorization header present");

        // Computed independently: urlencode("cli+ent/id") = "cli%2Bent%2Fid",
        // urlencode("sec+ret/val") = "sec%2Bret%2Fval", joined with ':' and
        // base64-standard-encoded — cross-checked with Python's
        // `urllib.parse.quote_plus` + `base64.b64encode` outside this crate.
        assert_eq!(
            auth_header,
            "Basic Y2xpJTJCZW50JTJGaWQ6c2VjJTJCcmV0JTJGdmFs"
        );
    }

    /// Built through the public constructor, not `fetch` directly: an empty
    /// `audience` on an `OAuth2ClientCredentials` source is exactly what
    /// `OidcSigner::new` is supposed to allow (see `OidcConfig::audience`'s
    /// doc for the three-way asymmetry), so this exercises that the
    /// omission is actually reachable from the public API, not merely true
    /// of `fetch` in isolation.
    #[tokio::test]
    async fn the_audience_is_sent_only_when_configured() {
        // Each call gets its own stub, so the body it reads back is
        // unambiguously the request this call's signer sent.
        async fn sign_once(audience: &str) -> Vec<u8> {
            let stub = StubServer::start(200, r#"{"access_token":"tok","expires_in":3600}"#).await;
            let dispatch = permissive_dispatch_client();
            let config = super::super::OidcConfig {
                source: super::super::OidcSource::OAuth2ClientCredentials {
                    token_url: format!("{}/token", stub.base_url()),
                    client_id: "id".to_string(),
                    client_secret: Secret::new("secret"),
                    scope: None,
                    style: ClientAuthStyle::ClientSecretBasic,
                },
                audience: audience.to_string(),
            };
            let signer =
                super::super::OidcSigner::new(config, &dispatch).expect("construction succeeds");

            let (url, headers) = empty_request();
            let request = SigningRequest {
                method: "POST",
                url: &url,
                body: b"",
                headers: &headers,
            };
            signer
                .sign(&request)
                .await
                .expect("a well-formed response succeeds");

            stub.received()[0].body.clone()
        }

        let without_audience = String::from_utf8(sign_once("").await).expect("body is utf8");
        assert!(!without_audience.contains("audience"));

        let with_audience =
            String::from_utf8(sign_once("https://push.example.com").await).expect("body is utf8");
        assert_eq!(
            with_audience,
            "grant_type=client_credentials&audience=https%3A%2F%2Fpush.example.com"
        );
    }

    /// The policy governs the token URL, and it does so at **construction**.
    ///
    /// This used to prove the point with a runtime refusal: a token URL the
    /// policy disallowed, refused inside resolution. That path no longer
    /// exists, and its absence is the fix — every way a token URL can be
    /// refused is now a construction-time check, so an operator sees it at
    /// boot rather than on the first dispatch. What is left to show is the
    /// pair: a host the policy names is dialled through the client that was
    /// handed in, and a host it does not name never produces a signer at all.
    #[tokio::test]
    async fn the_policy_gates_the_token_url_at_construction() {
        let stub = StubServer::start(200, r#"{"access_token":"tok","expires_in":3600}"#).await;
        let port = stub
            .base_url()
            .rsplit(':')
            .next()
            .expect("stub base url has a port")
            .to_string();
        let token_url = format!("http://localhost:{port}/token");

        let build = |entries: &str, allow_loopback: bool| {
            let policy = Arc::new(EgressPolicy::new(
                Allowlist::parse(entries).expect("test allowlist parses"),
                allow_loopback,
            ));
            let dispatch = DispatchClient::new(policy, Duration::from_secs(1))
                .expect("a policy and a timeout are enough to build a client");
            let config = super::super::OidcConfig {
                source: super::super::OidcSource::OAuth2ClientCredentials {
                    token_url: token_url.clone(),
                    client_id: "id".to_string(),
                    client_secret: Secret::new("secret"),
                    scope: None,
                    style: ClientAuthStyle::ClientSecretPost,
                },
                audience: "https://push.example.com".to_string(),
            };
            super::super::OidcSigner::new(config, &dispatch)
        };

        // A policy that does not name the host: no signer, and nothing dialled.
        // `OidcSigner` carries no `Debug`, so the `Ok` arm is unwrapped by hand.
        match build("elsewhere.example.com", true) {
            Err(AuthError::Config(reason)) => {
                assert!(reason.contains("allowlist"), "{reason}")
            }
            Err(other) => panic!("expected a Config refusal, got {other:?}"),
            Ok(_) => panic!("a host the policy does not name must not produce a signer"),
        }
        assert_eq!(
            stub.request_count(),
            0,
            "a token URL refused at construction is never dialled"
        );

        // The same URL, named by the policy and with the relaxation the
        // cleartext rule also requires: the fetch goes out through the client
        // the signer was handed.
        let signer = build("localhost", true).expect("an allowlisted loopback host builds");
        let target_url = url::Url::parse("https://push.example.com/hook").expect("test url parses");
        let request_headers = reqwest::header::HeaderMap::new();
        let request = crate::http::auth::SigningRequest {
            method: "POST",
            url: &target_url,
            body: b"",
            headers: &request_headers,
        };
        signer
            .sign(&request)
            .await
            .expect("an allowlisted token endpoint answers");

        assert_eq!(
            stub.request_count(),
            1,
            "the token fetch is dialled through the client handed to the signer"
        );
    }

    /// `AuthError::Transport`'s invariant, guarded where it is built: this
    /// string reaches `cache.rs`'s refresh `warn!` and — through the push
    /// dispatcher's `Refusal::Signing` — a stored job error. RFC 6749 has no
    /// place for a query on a token URL, but nothing refuses one, and an
    /// operator who puts a tenant key there must not find it in a log.
    ///
    /// Port 1 on loopback: allowlisted, so construction passes, and nothing
    /// listens there, so the fetch fails without needing DNS or a network.
    #[tokio::test]
    async fn a_transport_failure_never_carries_the_token_url() {
        let policy = Arc::new(EgressPolicy::new(
            Allowlist::parse("127.0.0.0/8").expect("test allowlist parses"),
            true,
        ));
        let dispatch = DispatchClient::new(policy, Duration::from_secs(1))
            .expect("a policy and a timeout are enough to build a client");

        let config = super::super::OidcConfig {
            source: super::super::OidcSource::OAuth2ClientCredentials {
                token_url: "http://127.0.0.1:1/token?tenant=q1w2e3r4".to_string(),
                client_id: "id".to_string(),
                client_secret: Secret::new("secret"),
                scope: None,
                style: ClientAuthStyle::ClientSecretPost,
            },
            audience: "https://push.example.com".to_string(),
        };
        let signer = super::super::OidcSigner::new(config, &dispatch)
            .expect("an allowlisted loopback token URL builds");

        let target_url = url::Url::parse("https://push.example.com/hook").expect("test url parses");
        let request_headers = reqwest::header::HeaderMap::new();
        let request = crate::http::auth::SigningRequest {
            method: "POST",
            url: &target_url,
            body: b"",
            headers: &request_headers,
        };

        let error = signer
            .sign(&request)
            .await
            .expect_err("nothing listens on port 1");

        assert!(matches!(error, AuthError::Transport(_)), "{error:?}");
        let rendered = error.to_string();
        assert!(
            !rendered.contains("q1w2e3r4"),
            "the token URL's query must not survive into the error: {rendered}"
        );
        assert!(
            !rendered.contains("127.0.0.1"),
            "nor the host it was dialling: {rendered}"
        );
    }
}
