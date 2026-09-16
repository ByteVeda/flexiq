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
/// Three rules:
///
/// - **A URL at all.** Anything `url::Url` cannot parse is a typo.
/// - **`https`, or `http` only to a loopback host.** The client secret goes to
///   this endpoint in a form body (or base64 in an `Authorization: Basic`
///   header, which is encoding, not encryption), and the access token comes
///   back the same way, so cleartext to anywhere but the host this process is
///   already running on puts a credential on the wire. This is the same rule
///   the push dispatch target's own URL obeys, for the same reason, and it is
///   deliberately *not* the rule
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
///
/// `localhost` counts as loopback without being resolved: RFC 6761 §6.3
/// reserves the name for the loopback interface, and there is nothing to
/// resolve at construction time. Errors name no part of the URL — an
/// operator's token URL may carry a tenant key in its query — which is the
/// same discipline [`LABEL`] exists for.
pub(super) fn validate_token_url(raw: &str) -> Result<url::Url, AuthError> {
    let url = url::Url::parse(raw)
        .map_err(|_| AuthError::Config("oauth2 token_url is not a valid URL".to_string()))?;

    match url.scheme() {
        "https" => {}
        "http" if is_loopback_host(&url) => {}
        _ => {
            return Err(AuthError::Config(
                "oauth2 token_url must use https; http is accepted only for a loopback host \
                 (127.0.0.0/8, ::1, or the name 'localhost')"
                    .to_string(),
            ))
        }
    }

    if !url.username().is_empty() || url.password().is_some() {
        return Err(AuthError::Config(
            "oauth2 token_url must not carry userinfo".to_string(),
        ));
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

    #[test]
    fn a_cleartext_token_url_is_refused_unless_it_is_loopback() {
        // The client secret goes to this endpoint in a form body and the
        // access token comes back in the response; neither may cross a
        // network in the clear.
        for refused in [
            "http://issuer.example.com/token",
            "http://10.1.2.3:8080/token",
            "http://[2606:4700::1111]/token",
            "ftp://issuer.example.com/token",
            "not a url",
        ] {
            assert!(
                matches!(validate_token_url(refused), Err(AuthError::Config(_))),
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
                validate_token_url(accepted).is_ok(),
                "{accepted} is https or a loopback sidecar, both of which stay reachable"
            );
        }
    }

    #[test]
    fn a_refused_token_url_never_appears_in_the_error() {
        // The same invariant `LABEL` exists for: an operator's token URL may
        // carry a tenant key in its query, and this error reaches a log.
        let error = validate_token_url("http://issuer.example.com/token?tenant=q1w2e3r4")
            .expect_err("cleartext off loopback is refused");

        let rendered = error.to_string();
        assert!(!rendered.contains("q1w2e3r4"), "{rendered}");
        assert!(!rendered.contains("issuer.example.com"), "{rendered}");
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

    #[tokio::test]
    async fn an_oauth2_token_url_goes_through_the_guarded_client() {
        let stub = StubServer::start(200, r#"{"access_token":"tok","expires_in":3600}"#).await;
        // `127.0.0.1` is an IP literal and never reaches the pinned
        // resolver at all (see `egress.rs`'s `permits_host` doc) — using
        // `localhost` here is what actually exercises the guard, the same
        // technique `resolver.rs`'s own
        // `localhost_is_refused_by_a_policy_that_does_not_allow_it` uses.
        let port = stub
            .base_url()
            .rsplit(':')
            .next()
            .expect("stub base url has a port")
            .to_string();
        // The query is the threat model for the `without_url` assertion
        // below: RFC 6749 has no place for one, but nothing refuses it, and
        // an operator who puts a tenant key there must not find it in a log.
        let disallowed_url = format!("http://localhost:{port}/token?tenant=q1w2e3r4");

        // A policy that permits nothing at all — not even loopback.
        let policy = Arc::new(EgressPolicy::new(
            Allowlist::parse("93.184.216.34").expect("test allowlist parses"),
            false,
        ));
        let dispatch = DispatchClient::new(policy, Duration::from_secs(1))
            .expect("a policy and a timeout are enough to build a client");

        let config = super::super::OidcConfig {
            source: super::super::OidcSource::OAuth2ClientCredentials {
                token_url: disallowed_url,
                client_id: "id".to_string(),
                client_secret: Secret::new("secret"),
                scope: None,
                style: ClientAuthStyle::ClientSecretPost,
            },
            audience: "https://push.example.com".to_string(),
        };
        let signer = super::super::OidcSigner::new(config, &dispatch)
            .expect("construction does not need network access");

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
            .expect_err("a policy that disallows loopback must refuse the token fetch");
        assert!(matches!(error, AuthError::Transport(_)));
        // `AuthError::Transport`'s invariant, guarded where it is built: this
        // string reaches `cache.rs`'s refresh `warn!` and — through the push
        // dispatcher's `Refusal::Signing` — a stored job error.
        let rendered = error.to_string();
        assert!(
            !rendered.contains("q1w2e3r4"),
            "the token URL's query must not survive into the error: {rendered}"
        );
        assert_eq!(
            stub.request_count(),
            0,
            "a refused fetch must never reach the stub at all"
        );
    }
}
