//! AWS IMDSv2: the session-token `PUT`, then two `GET`s carrying it.
//!
//! IMDSv1 answers an unauthenticated `GET` to these same paths with no
//! session token at all — the exact primitive GitHub issue #844 exists to
//! close, by putting a signature between the scheduler and every push target
//! it talks to. Falling back to it here, even only when the `PUT` above is
//! refused, would reopen that hole inside the credential path of the very
//! feature meant to close it. So there is no fallback: a refused `PUT` is
//! this module's own refusal, full stop, and nothing below ever issues a
//! `GET` without the token that `PUT` returned.

use reqwest::Method;

use super::credentials::{self, AwsCredentials};
use crate::http::auth::cache::{CredentialCache, Expiring};
use crate::http::auth::metadata::{MetadataClient, MetadataEndpoint};
use crate::http::auth::AuthError;
use crate::job::now_millis;

/// IMDSv2 issues a session token good for this many seconds.
const TOKEN_TTL_SECONDS: i64 = 21600;
/// Sent on the `PUT` that requests a session token.
const TOKEN_TTL_HEADER: &str = "X-aws-ec2-metadata-token-ttl-seconds";
/// Sent on both subsequent `GET`s, carrying the token the `PUT` returned.
const TOKEN_HEADER: &str = "X-aws-ec2-metadata-token";

/// Run the full three-call sequence and return usable credentials.
///
/// `token_cache` is the caller's — see `credentials.rs`'s doc for why it
/// outlives any one call here: the session token is good for
/// [`TOKEN_TTL_SECONDS`], far longer than the role credentials it is used to
/// fetch are usually good for, so refetching it on every call here would
/// throw away most of its useful life.
pub(super) async fn fetch(
    metadata: &MetadataClient,
    token_cache: &CredentialCache<String>,
) -> Result<Expiring<AwsCredentials>, AuthError> {
    let token = token_cache
        .get_or_refresh(|| fetch_session_token(metadata))
        .await?;
    let role = fetch_role_name(metadata, &token).await?;
    fetch_role_credentials(metadata, &token, &role).await
}

async fn fetch_session_token(metadata: &MetadataClient) -> Result<Expiring<String>, AuthError> {
    let ttl_value = TOKEN_TTL_SECONDS.to_string();
    let token = metadata
        .fetch(
            &MetadataEndpoint::AwsImdsApiToken,
            Method::PUT,
            &[],
            &[(TOKEN_TTL_HEADER, &ttl_value, false)],
        )
        .await?;

    let issued_at_ms = now_millis();
    let ttl_ms = TOKEN_TTL_SECONDS * 1000;
    Ok(Expiring {
        value: token.trim().to_string(),
        // Half the TTL, not the usual cache-wide skew: this token outlives
        // several role-credential refreshes, so refreshing it only shortly
        // before its own expiry would still risk a signing attempt starting
        // just before that point and finishing after it.
        refresh_at_ms: issued_at_ms + ttl_ms / 2,
        expires_at_ms: issued_at_ms + ttl_ms,
    })
}

/// The one header entry both of IMDSv2's `GET`s send: the session token,
/// marked sensitive. `fetch_role_name` and `fetch_role_credentials` both
/// build their headers through this single function, so proving it sensitive
/// once — see `the_session_token_is_sent_sensitive_on_both_gets` below —
/// covers both call sites; they cannot drift apart from each other.
fn token_header_entries(token: &str) -> [(&'static str, &str, bool); 1] {
    [(TOKEN_HEADER, token, true)]
}

async fn fetch_role_name(metadata: &MetadataClient, token: &str) -> Result<String, AuthError> {
    let headers = token_header_entries(token);
    let body = metadata
        .fetch(
            &MetadataEndpoint::AwsImdsSecurityCredentials,
            Method::GET,
            &[],
            &headers,
        )
        .await?;
    body.lines()
        .next()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .ok_or(AuthError::CredentialShape {
            endpoint: MetadataEndpoint::AwsImdsSecurityCredentials.label(),
            reason: "role list is empty",
        })
}

async fn fetch_role_credentials(
    metadata: &MetadataClient,
    token: &str,
    role: &str,
) -> Result<Expiring<AwsCredentials>, AuthError> {
    let endpoint = MetadataEndpoint::aws_imds_role_credentials(role)?;
    let headers = token_header_entries(token);

    let body = metadata
        .fetch(&endpoint, Method::GET, &[], &headers)
        .await?;
    let response = credentials::parse_credential_response(&endpoint, &body)?;
    credentials::require_success_code(&response, &endpoint)?;
    credentials::to_expiring(response, &endpoint)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::testing::StubServer;

    fn client_pointed_at(stub: &StubServer) -> MetadataClient {
        MetadataClient::with_base_url(url::Url::parse(&stub.base_url()).expect("stub url parses"))
            .expect("test client builds")
    }

    #[tokio::test]
    async fn the_session_token_is_requested_with_a_put_and_a_ttl_header() {
        let stub = StubServer::start(200, "a-session-token").await;
        let metadata = client_pointed_at(&stub);

        let expiring = fetch_session_token(&metadata)
            .await
            .expect("a 200 succeeds");
        assert_eq!(expiring.value, "a-session-token");

        let received = stub.received();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].method, "PUT");
        assert!(received[0].headers.iter().any(|(name, value)| name
            == "x-aws-ec2-metadata-token-ttl-seconds"
            && value == "21600"));
    }

    #[tokio::test]
    async fn the_role_name_is_the_first_line() {
        let stub = StubServer::start(200, "the-only-role\nsomething-else").await;
        let metadata = client_pointed_at(&stub);

        let role = fetch_role_name(&metadata, "tok")
            .await
            .expect("a non-empty listing succeeds");
        assert_eq!(role, "the-only-role");
    }

    #[tokio::test]
    async fn a_code_that_is_not_success_is_a_shape_error() {
        let stub = StubServer::start(
            200,
            r#"{"Code":"Failure","AccessKeyId":"AKIDEXAMPLE","SecretAccessKey":"s","Token":"t","Expiration":"2099-01-01T00:00:00Z"}"#,
        )
        .await;
        let metadata = client_pointed_at(&stub);

        // `Expiring<AwsCredentials>` carries no `Debug`, so `expect_err`
        // cannot be used here; `matches!` needs none.
        let result = fetch_role_credentials(&metadata, "tok", "some-role").await;
        assert!(matches!(result, Err(AuthError::CredentialShape { .. })));

        // Absent entirely is refused the same way.
        let stub = StubServer::start(
            200,
            r#"{"AccessKeyId":"AKIDEXAMPLE","SecretAccessKey":"s","Token":"t","Expiration":"2099-01-01T00:00:00Z"}"#,
        )
        .await;
        let metadata = client_pointed_at(&stub);
        let result = fetch_role_credentials(&metadata, "tok", "some-role").await;
        assert!(matches!(result, Err(AuthError::CredentialShape { .. })));
    }

    #[test]
    fn the_session_token_is_sent_sensitive_on_both_gets() {
        let entries = token_header_entries("tok");
        assert_eq!(entries.len(), 1);
        let (name, value, sensitive) = entries[0];
        assert_eq!(name, TOKEN_HEADER);
        assert_eq!(value, "tok");
        assert!(
            sensitive,
            "the session token entry must be marked sensitive"
        );
    }

    #[tokio::test]
    async fn there_is_no_v1_fallback() {
        let stub = StubServer::start(401, "token endpoint refused").await;
        let metadata = client_pointed_at(&stub);
        let token_cache = CredentialCache::new();

        // `Expiring<AwsCredentials>` carries no `Debug`, so `expect_err`
        // cannot be used here; a plain match needs none.
        let result = fetch(&metadata, &token_cache).await;
        assert!(matches!(
            result,
            Err(AuthError::CredentialEndpoint { status: 401, .. })
        ));

        // The whole point: no GET was ever attempted after the PUT refusal —
        // that would be the IMDSv1 shape this module's doc refuses to offer.
        let received = stub.received();
        assert_eq!(received.len(), 1, "exactly one request: the refused PUT");
        for request in &received {
            assert_eq!(request.method, "PUT");
        }
    }
}
