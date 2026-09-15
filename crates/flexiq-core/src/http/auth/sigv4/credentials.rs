//! Where AWS SigV4 credentials come from: a source enum, and one fetch
//! function per kind.
//!
//! Deliberately not implemented: `~/.aws/config` profiles, SSO,
//! `AssumeRole`/STS, web-identity federation, `credential_process`. Each of
//! those is either a file format this crate has no reader for, or a second
//! network protocol on top of SigV4 itself; none of them is present inside a
//! container, which is where every real target of this feature runs, and an
//! operator who genuinely needs one can mint short-lived keys with it
//! elsewhere and feed them in as [`AwsCredentialSource::Static`] from a
//! sidecar. Listing that here is worth more than the questions it prevents.

use chrono::DateTime;
use serde::Deserialize;

use super::imds;
use crate::http::auth::cache::{refresh_at_ms, CredentialCache, Expiring};
use crate::http::auth::metadata::{accept_env_endpoint, MetadataClient, MetadataEndpoint};
use crate::http::auth::AuthError;
use crate::job::now_millis;
use crate::worker::Secret;

/// ECS's and EKS Pod Identity's link-local credentials host — the address
/// `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` is joined onto.
const ECS_CONTAINER_CREDENTIALS_HOST: &str = "169.254.170.2";

/// Static and environment credentials never expire. Far enough past any real
/// clock reading that [`CredentialCache`] never refetches a value carrying
/// this as both its `refresh_at_ms` and `expires_at_ms` — which is correct
/// for a credential with no expiry at all, not a workaround for one that has
/// one.
const NEVER_EXPIRES_MS: i64 = i64::MAX / 2;

/// A set of AWS credentials, with the instant they stop working.
///
/// `Debug` is derived, not banned: [`Secret`]'s own `Debug` already redacts
/// both `secret_access_key` and `session_token`, so deriving here costs
/// nothing and leaves `access_key_id` printing in the clear, which is the
/// useful half for diagnosis — an AKID is not itself a secret. Proved, not
/// merely asserted, by `credentials_never_reach_a_formatter` below.
#[derive(Clone, Debug)]
pub(crate) struct AwsCredentials {
    pub(crate) access_key_id: String,
    pub(crate) secret_access_key: Secret,
    /// Present for every source but static keys.
    pub(crate) session_token: Option<Secret>,
}

/// Where SigV4 credentials come from.
#[derive(Clone, Debug)]
pub enum AwsCredentialSource {
    /// Keys given directly by the operator, never expiring.
    Static {
        /// The access key id. Not itself secret — an AKID is safe to log.
        access_key_id: String,
        /// The secret access key.
        secret_access_key: Secret,
        /// A session token, when the keys are temporary (e.g. minted by
        /// `AssumeRole` elsewhere and fed in here).
        session_token: Option<Secret>,
    },
    /// `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`.
    Environment,
    /// ECS and EKS Pod Identity.
    ContainerCredentials,
    /// AWS's own EC2 instance metadata service, version 2 only — see
    /// `imds.rs`'s module doc for why there is no version 1 fallback.
    Imdsv2,
    /// Environment → container → IMDSv2: the AWS SDK's own default order,
    /// minus the parts a server process in a container does not have.
    DefaultChain,
}

/// The wire shape AWS's container-credentials endpoint and IMDSv2's
/// role-credentials call both answer with.
///
/// Fields stay private to this module: `imds.rs` receives this type only
/// opaquely, through [`parse_credential_response`], [`require_success_code`]
/// and [`to_expiring`], never by reading a field directly.
#[derive(Deserialize)]
pub(super) struct AwsCredentialResponse {
    #[serde(rename = "AccessKeyId")]
    access_key_id: String,
    #[serde(rename = "SecretAccessKey")]
    secret_access_key: String,
    #[serde(rename = "Token")]
    token: Option<String>,
    #[serde(rename = "Expiration")]
    expiration: String,
    /// Only IMDSv2's role-credentials call sets this; the container
    /// endpoint's response carries no such field. Parsed here regardless so
    /// both callers share one deserializer, and checked only by the caller
    /// that needs it — see [`require_success_code`].
    #[serde(rename = "Code")]
    code: Option<String>,
}

/// Parse a container- or IMDS-shaped credentials response.
pub(super) fn parse_credential_response(
    endpoint: &MetadataEndpoint,
    body: &str,
) -> Result<AwsCredentialResponse, AuthError> {
    serde_json::from_str(body).map_err(|_| AuthError::CredentialShape {
        endpoint: endpoint.label(),
        reason: "not valid JSON",
    })
}

/// IMDSv2's role-credentials call alone sets `Code`, and answers `"Success"`
/// when the credentials in the same body are usable — anything else,
/// including its absence, means this build cannot trust the rest of the
/// response.
pub(super) fn require_success_code(
    response: &AwsCredentialResponse,
    endpoint: &MetadataEndpoint,
) -> Result<(), AuthError> {
    match response.code.as_deref() {
        Some("Success") => Ok(()),
        _ => Err(AuthError::CredentialShape {
            endpoint: endpoint.label(),
            reason: "Code was not Success",
        }),
    }
}

/// Turn a parsed response into a cache entry, parsing `Expiration` as RFC
/// 3339 — the format both ECS/EKS Pod Identity and IMDSv2 answer with.
pub(super) fn to_expiring(
    response: AwsCredentialResponse,
    endpoint: &MetadataEndpoint,
) -> Result<Expiring<AwsCredentials>, AuthError> {
    let expires_at_ms = DateTime::parse_from_rfc3339(&response.expiration)
        .map_err(|_| AuthError::CredentialShape {
            endpoint: endpoint.label(),
            reason: "Expiration is not RFC 3339",
        })?
        .timestamp_millis();
    Ok(Expiring {
        value: AwsCredentials {
            access_key_id: response.access_key_id,
            secret_access_key: Secret::new(response.secret_access_key),
            session_token: response.token.map(Secret::new),
        },
        refresh_at_ms: refresh_at_ms(now_millis(), expires_at_ms),
        expires_at_ms,
    })
}

/// Static and environment credentials share this: never expire, so the cache
/// never refetches them.
fn never_expires(value: AwsCredentials) -> Expiring<AwsCredentials> {
    Expiring {
        value,
        refresh_at_ms: NEVER_EXPIRES_MS,
        expires_at_ms: NEVER_EXPIRES_MS,
    }
}

/// `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN`, read
/// through `read` — the same seam `oidc/azure.rs`'s `app_service_from_env`
/// uses, so a test never has to mutate the process environment. Neither of
/// the first two being set is [`AuthError::NoCredentials`]: this source
/// simply is not configured, not misconfigured.
fn fetch_environment(
    read: &impl Fn(&str) -> Option<String>,
) -> Result<Expiring<AwsCredentials>, AuthError> {
    match (read("AWS_ACCESS_KEY_ID"), read("AWS_SECRET_ACCESS_KEY")) {
        (Some(access_key_id), Some(secret_access_key)) => Ok(never_expires(AwsCredentials {
            access_key_id,
            secret_access_key: Secret::new(secret_access_key),
            session_token: read("AWS_SESSION_TOKEN").map(Secret::new),
        })),
        _ => Err(AuthError::NoCredentials(
            "AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY are not both set in the environment",
        )),
    }
}

/// Resolve the container-credentials endpoint: a relative URI joined onto
/// the ECS/EKS Pod Identity link-local host, or a full URI vetted by
/// [`accept_env_endpoint`] — a routable one is refused there, not here.
/// Neither variable set is [`AuthError::NoCredentials`]: this source simply
/// is not configured, not misconfigured.
fn container_credentials_endpoint(
    read: &impl Fn(&str) -> Option<String>,
) -> Result<url::Url, AuthError> {
    if let Some(relative) = read("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") {
        let joined = format!("http://{ECS_CONTAINER_CREDENTIALS_HOST}{relative}");
        return url::Url::parse(&joined).map_err(|_| {
            AuthError::Config(format!(
                "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI '{relative}' does not join into a usable URL"
            ))
        });
    }
    if let Some(full) = read("AWS_CONTAINER_CREDENTIALS_FULL_URI") {
        return accept_env_endpoint(&full);
    }
    Err(AuthError::NoCredentials(
        "neither AWS_CONTAINER_CREDENTIALS_RELATIVE_URI nor AWS_CONTAINER_CREDENTIALS_FULL_URI is set",
    ))
}

/// The container credentials endpoint's `Authorization` header value, sent
/// verbatim — never `Bearer`-prefixed, unlike the OIDC schemes in this crate:
/// AWS's container-credentials endpoints check the header for an exact
/// match, and a `Bearer ` prefix would make every request a 403 with no
/// explanation — or `None` for the classic ECS task role, whose endpoint
/// needs no header at all.
///
/// `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` wins when both variables are
/// set, and is re-read here on every call — never cached — because EKS Pod
/// Identity rotates that file in place; treating the static variable as an
/// override would silently pin a token that stops working the moment it
/// does.
async fn container_authorization_token(
    read: &impl Fn(&str) -> Option<String>,
) -> Result<Option<Secret>, AuthError> {
    if let Some(path) = read("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE") {
        let raw = tokio::fs::read_to_string(&path).await.map_err(|error| {
            AuthError::Config(format!(
                "could not read AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE at {path}: {error}"
            ))
        })?;
        return Ok(Some(Secret::new(raw.trim_end().to_string())));
    }
    Ok(read("AWS_CONTAINER_AUTHORIZATION_TOKEN").map(Secret::new))
}

/// The header entries [`fetch_container`] sends: the `Authorization` value
/// above alone, marked sensitive, or none at all. Extracted so a test can
/// assert the sensitivity flag directly, with no network round trip — the
/// same shape as `oidc/azure.rs`'s `app_service_header_entries`.
fn container_authorization_header_entries(
    token: &Option<Secret>,
) -> Vec<(&'static str, String, bool)> {
    match token {
        Some(token) => vec![(
            reqwest::header::AUTHORIZATION.as_str(),
            String::from_utf8_lossy(token.expose_secret()).into_owned(),
            true,
        )],
        None => Vec::new(),
    }
}

/// Fetch fresh credentials from ECS or EKS Pod Identity.
async fn fetch_container(
    read: &impl Fn(&str) -> Option<String>,
    metadata: &MetadataClient,
) -> Result<Expiring<AwsCredentials>, AuthError> {
    let url = container_credentials_endpoint(read)?;
    let token = container_authorization_token(read).await?;
    let endpoint = MetadataEndpoint::AwsContainerCredentials(url);

    let entries = container_authorization_header_entries(&token);
    let headers: Vec<(&str, &str, bool)> = entries
        .iter()
        .map(|(name, value, sensitive)| (*name, value.as_str(), *sensitive))
        .collect();

    let body = metadata
        .fetch(&endpoint, reqwest::Method::GET, &[], &headers)
        .await?;
    let response = parse_credential_response(&endpoint, &body)?;
    to_expiring(response, &endpoint)
}

/// Environment → container → IMDSv2, in that order — the AWS SDK's own
/// default chain, minus the sources this module's doc explains are out of
/// scope entirely. A source's own [`AuthError::NoCredentials`] means only
/// that source is unconfigured, so the chain moves on; any other error means
/// a configured source is broken, and propagates immediately rather than
/// being papered over by silently trying the next one.
async fn fetch_default_chain(
    read: &impl Fn(&str) -> Option<String>,
    metadata: &MetadataClient,
    imds_token_cache: &CredentialCache<String>,
) -> Result<Expiring<AwsCredentials>, AuthError> {
    match fetch_environment(read) {
        Ok(expiring) => return Ok(expiring),
        Err(AuthError::NoCredentials(_)) => {}
        Err(other) => return Err(other),
    }
    match fetch_container(read, metadata).await {
        Ok(expiring) => return Ok(expiring),
        Err(AuthError::NoCredentials(_)) => {}
        Err(other) => return Err(other),
    }
    imds::fetch(metadata, imds_token_cache).await
}

/// Resolve `source` into fresh credentials, reading the environment through
/// `read` and IMDSv2 through `metadata`/`imds_token_cache`.
///
/// Separated from [`fetch`] so a test can inject a fixed map instead of the
/// real process environment — the same split `oidc/azure.rs`'s
/// `app_service_from_env` uses, and for the same reason: no test here ever
/// mutates `std::env`.
async fn fetch_from(
    source: &AwsCredentialSource,
    metadata: &MetadataClient,
    imds_token_cache: &CredentialCache<String>,
    read: &impl Fn(&str) -> Option<String>,
) -> Result<Expiring<AwsCredentials>, AuthError> {
    match source {
        AwsCredentialSource::Static {
            access_key_id,
            secret_access_key,
            session_token,
        } => Ok(never_expires(AwsCredentials {
            access_key_id: access_key_id.clone(),
            secret_access_key: secret_access_key.clone(),
            session_token: session_token.clone(),
        })),
        AwsCredentialSource::Environment => fetch_environment(read),
        AwsCredentialSource::ContainerCredentials => fetch_container(read, metadata).await,
        AwsCredentialSource::Imdsv2 => imds::fetch(metadata, imds_token_cache).await,
        AwsCredentialSource::DefaultChain => {
            fetch_default_chain(read, metadata, imds_token_cache).await
        }
    }
}

/// Resolve `source` into fresh credentials, reading the real process
/// environment.
pub(crate) async fn fetch(
    source: &AwsCredentialSource,
    metadata: &MetadataClient,
    imds_token_cache: &CredentialCache<String>,
) -> Result<Expiring<AwsCredentials>, AuthError> {
    fetch_from(source, metadata, imds_token_cache, &|name| {
        std::env::var(name).ok()
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;
    use crate::http::testing::StubServer;

    fn client_pointed_at(stub: &StubServer) -> MetadataClient {
        MetadataClient::with_base_url(url::Url::parse(&stub.base_url()).expect("stub url parses"))
            .expect("test client builds")
    }

    /// A `read` closure over a fixed table, never the real process
    /// environment — matching `oidc/azure.rs`'s own test seam.
    /// Owns its data (rather than borrowing `pairs`) so the returned closure
    /// carries no lifetime of its own and can be bound with `let` and used
    /// across several statements, including ones built from a locally-owned
    /// `String` such as a temp-file path.
    fn fixed_env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let owned: Vec<(String, String)> = pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        move |name| {
            owned
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.clone())
        }
    }

    fn container_credentials_json() -> String {
        r#"{"AccessKeyId":"AKIDEXAMPLE","SecretAccessKey":"secret-value","Token":"session-token-value","Expiration":"2099-01-01T00:00:00Z"}"#
            .to_string()
    }

    #[tokio::test]
    async fn static_credentials_never_expire() {
        let source = AwsCredentialSource::Static {
            access_key_id: "AKIDEXAMPLE".to_string(),
            secret_access_key: Secret::new("secret-value"),
            session_token: None,
        };
        let metadata = MetadataClient::new().expect("test client builds");
        let token_cache = CredentialCache::new();

        let expiring = fetch_from(&source, &metadata, &token_cache, &fixed_env(&[]))
            .await
            .expect("static credentials always resolve, with no network at all");

        // A century out: comfortably past any real clock reading, which is
        // the whole point of `NEVER_EXPIRES_MS` — a cache built from this
        // never sees a reason to refetch.
        let century_out = now_millis() + 100 * 365 * 24 * 60 * 60 * 1000;
        assert!(expiring.expires_at_ms > century_out);
        assert!(expiring.refresh_at_ms > century_out);
    }

    #[test]
    fn no_environment_credentials_is_no_credentials() {
        // `Expiring<AwsCredentials>` carries no `Debug`, so `expect_err`
        // cannot be used here; `matches!` needs none.
        let result = fetch_environment(&fixed_env(&[]));
        assert!(matches!(result, Err(AuthError::NoCredentials(_))));
    }

    #[test]
    fn environment_credentials_are_read_and_the_session_token_is_optional() {
        let with_token = fixed_env(&[
            ("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE"),
            ("AWS_SECRET_ACCESS_KEY", "secret-value"),
            ("AWS_SESSION_TOKEN", "token-value"),
        ]);
        let expiring = fetch_environment(&with_token).expect("both required vars are set");
        assert_eq!(expiring.value.access_key_id, "AKIDEXAMPLE");
        assert!(expiring.value.session_token.is_some());

        let without_token = fixed_env(&[
            ("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE"),
            ("AWS_SECRET_ACCESS_KEY", "secret-value"),
        ]);
        let expiring = fetch_environment(&without_token).expect("session token is optional");
        assert!(expiring.value.session_token.is_none());
    }

    #[test]
    fn a_relative_container_uri_is_joined_onto_the_link_local_host() {
        let read = fixed_env(&[(
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
            "/v2/credentials/abc-123",
        )]);
        let url = container_credentials_endpoint(&read).expect("a relative URI joins");
        assert_eq!(url.as_str(), "http://169.254.170.2/v2/credentials/abc-123");
    }

    #[test]
    fn a_full_container_uri_goes_through_accept_env_endpoint() {
        let loopback = fixed_env(&[(
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "http://169.254.170.23/v1/credentials",
        )]);
        assert!(container_credentials_endpoint(&loopback).is_ok());

        // A routable one is refused: `accept_env_endpoint` is the guard, not
        // this function's own logic.
        let routable = fixed_env(&[(
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "http://93.184.216.34/v1/credentials",
        )]);
        let error =
            container_credentials_endpoint(&routable).expect_err("a routable URI is refused");
        assert!(matches!(error, AuthError::Config(_)));
    }

    #[tokio::test]
    async fn the_container_auth_token_is_sent_verbatim_not_as_a_bearer() {
        let stub = StubServer::start(200, container_credentials_json()).await;
        let metadata = client_pointed_at(&stub);
        let read = fixed_env(&[
            (
                "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
                "/v2/credentials/x",
            ),
            ("AWS_CONTAINER_AUTHORIZATION_TOKEN", "raw-token-value"),
        ]);

        fetch_container(&read, &metadata)
            .await
            .expect("a well-formed response succeeds");

        let received = stub.received();
        assert_eq!(received.len(), 1);
        let auth_header = received[0]
            .headers
            .iter()
            .find(|(name, _)| name == "authorization")
            .map(|(_, value)| value.clone())
            .expect("authorization header present");
        assert_eq!(auth_header, "raw-token-value");
        assert!(!auth_header.starts_with("Bearer "));
    }

    #[tokio::test]
    async fn the_container_auth_token_file_is_reread_on_every_fetch() {
        let mut file = tempfile::NamedTempFile::new().expect("temp file creates");
        write!(file, "first-token").expect("temp file writes");
        let path = file.path().to_str().expect("path is utf8").to_string();

        let stub = StubServer::start(200, container_credentials_json()).await;
        let metadata = client_pointed_at(&stub);
        let read = fixed_env(&[
            (
                "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
                "/v2/credentials/x",
            ),
            ("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", &path),
        ]);

        fetch_container(&read, &metadata)
            .await
            .expect("first fetch succeeds");
        let first_auth = received_authorization(&stub, 0);
        assert_eq!(first_auth, "first-token");

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .expect("temp file reopens for overwrite");
        write!(file, "second-token").expect("temp file overwrites");

        fetch_container(&read, &metadata)
            .await
            .expect("second fetch succeeds");
        let second_auth = received_authorization(&stub, 1);
        assert_eq!(second_auth, "second-token");
        assert_ne!(first_auth, second_auth);
    }

    fn received_authorization(stub: &StubServer, index: usize) -> String {
        stub.received()[index]
            .headers
            .iter()
            .find(|(name, _)| name == "authorization")
            .map(|(_, value)| value.clone())
            .expect("authorization header present")
    }

    #[test]
    fn the_container_auth_token_is_sent_sensitive() {
        let entries = container_authorization_header_entries(&Some(Secret::new("tok")));
        assert_eq!(entries.len(), 1);
        let (name, value, sensitive) = &entries[0];
        assert_eq!(*name, reqwest::header::AUTHORIZATION.as_str());
        assert_eq!(value, "tok");
        assert!(
            sensitive,
            "the container auth token entry must be marked sensitive"
        );
    }

    #[test]
    fn an_expiration_is_parsed_from_rfc3339() {
        let response = AwsCredentialResponse {
            access_key_id: "AKIDEXAMPLE".to_string(),
            secret_access_key: "secret-value".to_string(),
            token: None,
            expiration: "2024-01-01T00:00:00Z".to_string(),
            code: None,
        };
        let endpoint = MetadataEndpoint::AwsContainerCredentials(
            url::Url::parse("http://169.254.170.2/v2/credentials/x").expect("test url parses"),
        );
        let expiring =
            to_expiring(response, &endpoint).expect("a well-formed RFC 3339 expiration parses");
        let expected_ms = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .expect("fixture parses")
            .timestamp_millis();
        assert_eq!(expiring.expires_at_ms, expected_ms);
    }

    #[tokio::test]
    async fn the_default_chain_falls_through_environment_then_container_then_imds() {
        // No AWS_* vars at all: environment and container both report
        // `NoCredentials`, so the chain falls all the way to IMDSv2, whose
        // three-call sequence this one stub answers in order.
        let stub = StubServer::start_scripted(vec![
            (200, "imds-session-token".to_string()),
            (200, "the-only-role".to_string()),
            (
                200,
                r#"{"Code":"Success","AccessKeyId":"AKIDEXAMPLE","SecretAccessKey":"secret-value","Token":"session-token-value","Expiration":"2099-01-01T00:00:00Z"}"#
                    .to_string(),
            ),
        ])
        .await;
        let metadata = client_pointed_at(&stub);
        let token_cache = CredentialCache::new();

        let expiring = fetch_from(
            &AwsCredentialSource::DefaultChain,
            &metadata,
            &token_cache,
            &fixed_env(&[]),
        )
        .await
        .expect("the chain falls through to imds and succeeds");

        assert_eq!(expiring.value.access_key_id, "AKIDEXAMPLE");
        let received = stub.received();
        assert_eq!(received.len(), 3, "environment and container add no calls");
        assert_eq!(received[0].method, "PUT");
        assert_eq!(received[1].method, "GET");
        assert_eq!(received[2].method, "GET");
    }

    #[test]
    fn credentials_never_reach_a_formatter() {
        // Alternating letter/digit, matching the sliding-window technique
        // `auth/mod.rs`'s own `the_config_never_reaches_a_formatter` uses:
        // every 4-character window carries a digit, so it cannot coincide
        // with a purely alphabetic run elsewhere in the rendered `Debug`.
        let secret_value = "z9y8x7w6v5u4t3s2";
        let token_value = "r1q2p3o4n5m6l7k8";
        let credentials = AwsCredentials {
            access_key_id: "AKIDEXAMPLE".to_string(),
            secret_access_key: Secret::new(secret_value),
            session_token: Some(Secret::new(token_value)),
        };
        let rendered = format!("{credentials:?}");

        for secret in [secret_value, token_value] {
            assert!(!rendered.contains(secret));
            for window in secret.as_bytes().windows(4) {
                let fragment = std::str::from_utf8(window).expect("secret is ascii");
                assert!(
                    !rendered.contains(fragment),
                    "rendered debug leaked fragment {fragment:?}: {rendered}"
                );
            }
        }
        // The access key id is not itself secret and must still print, or a
        // redaction that swallowed the whole struct would not be caught by
        // the assertions above alone.
        assert!(rendered.contains("AKIDEXAMPLE"));
    }
}
