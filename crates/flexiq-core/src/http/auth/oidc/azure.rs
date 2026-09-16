//! Azure managed identity — two sources, one response shape.
//!
//! IMDS (`169.254.169.254`, VMs/VMSS/AKS) and App Service's per-instance
//! sidecar (Functions, Container Apps) answer the same JSON envelope, so one
//! deserializer and one query/response contract serve both; only the
//! endpoint, the extra header, and where the identity comes from differ.

use reqwest::Method;
use serde::Deserialize;

use crate::http::auth::cache::Expiring;
use crate::http::auth::metadata::{accept_env_endpoint, MetadataClient, MetadataEndpoint};
use crate::http::auth::AuthError;
use crate::worker::Secret;

const API_VERSION_IMDS: &str = "2018-02-01";
const API_VERSION_APP_SERVICE: &str = "2019-08-01";
const IDENTITY_HEADER_NAME: &str = "X-IDENTITY-HEADER";

/// Azure's managed-identity token response — the same shape from IMDS and
/// App Service.
///
/// Real Azure responses also carry `expires_on`, `not_before`, `resource`
/// and `token_type`; those are not named here and serde drops them silently
/// rather than erroring, so this struct is not a full mirror of the wire
/// response, on purpose — it reads only what this module uses.
#[derive(Deserialize)]
struct AzureTokenResponse {
    access_token: String,
    /// A JSON *string*, not a number — Microsoft's own IMDS sample response
    /// quotes it. A field typed `i64` here would fail to deserialize every
    /// real response while still passing against a hand-written unquoted
    /// test fixture; see `imds_expires_in_is_a_json_string` below, whose
    /// fixture is deliberately quoted for exactly that reason.
    expires_in: String,
}

fn parse_response(endpoint: &MetadataEndpoint, body: &str) -> Result<(String, i64), AuthError> {
    let parsed: AzureTokenResponse =
        serde_json::from_str(body).map_err(|_| AuthError::CredentialShape {
            endpoint: endpoint.label(),
            reason: "not valid JSON",
        })?;
    let expires_in_seconds: i64 =
        parsed
            .expires_in
            .parse()
            .map_err(|_| AuthError::CredentialShape {
                endpoint: endpoint.label(),
                reason: "expires_in is not an integer",
            })?;
    Ok((parsed.access_token, expires_in_seconds))
}

/// Fetch a fresh token from IMDS.
///
/// At most one of `client_id`, `object_id`, `msi_res_id` may be set —
/// [`validate_imds_selector`] enforces that at construction, not here, so a
/// caller reaching this function has already been checked.
pub(super) async fn fetch_imds(
    metadata: &MetadataClient,
    audience: &str,
    client_id: Option<&str>,
    object_id: Option<&str>,
    msi_res_id: Option<&str>,
) -> Result<Expiring<String>, AuthError> {
    let endpoint = MetadataEndpoint::AzureImdsToken;

    let mut query: Vec<(&str, &str)> =
        vec![("api-version", API_VERSION_IMDS), ("resource", audience)];
    if let Some(value) = client_id {
        query.push(("client_id", value));
    }
    if let Some(value) = object_id {
        query.push(("object_id", value));
    }
    if let Some(value) = msi_res_id {
        query.push(("msi_res_id", value));
    }
    // Not sensitive: `Metadata: true` is a fixed literal, not credential
    // material.
    let headers = [("Metadata", "true", false)];

    let body = metadata
        .fetch(&endpoint, Method::GET, &query, &headers)
        .await?;
    let (token, expires_in_seconds) = parse_response(&endpoint, &body)?;
    Ok(super::expiring_from_ttl_seconds(token, expires_in_seconds))
}

/// The one header entry `fetch_app_service` sends, marked sensitive so
/// `MetadataClient::fetch` never lets it reach a `Debug` of the outgoing
/// request — the per-instance sidecar secret is exactly the kind of value
/// that must not round-trip into a log unexamined.
///
/// Extracted from `fetch_app_service` so a test can assert the sensitivity
/// flag directly, with no network round trip: see
/// `the_identity_header_entry_is_marked_sensitive` below.
fn app_service_header_entries(identity_header: &Secret) -> [(&'static str, String, bool); 1] {
    // Unwrapped only at the point of use and held nowhere else, the same
    // discipline `BearerSigner::sign` applies to its own token: `Secret` is
    // always built from a `String`, so this is never lossy in practice.
    let header_value = String::from_utf8_lossy(identity_header.expose_secret()).into_owned();
    [(IDENTITY_HEADER_NAME, header_value, true)]
}

/// Fetch a fresh token from the App Service / Functions / Container Apps
/// sidecar — the source that actually covers "Azure Functions" in issue
/// #844, as opposed to IMDS, which only ever answers on a VM.
pub(super) async fn fetch_app_service(
    metadata: &MetadataClient,
    identity_endpoint: &url::Url,
    identity_header: &Secret,
    audience: &str,
) -> Result<Expiring<String>, AuthError> {
    let endpoint = MetadataEndpoint::AzureAppServiceToken(identity_endpoint.clone());
    let query = [
        ("resource", audience),
        ("api-version", API_VERSION_APP_SERVICE),
    ];

    let entries = app_service_header_entries(identity_header);
    let headers: Vec<(&str, &str, bool)> = entries
        .iter()
        .map(|(name, value, sensitive)| (*name, value.as_str(), *sensitive))
        .collect();

    let body = metadata
        .fetch(&endpoint, Method::GET, &query, &headers)
        .await?;
    let (token, expires_in_seconds) = parse_response(&endpoint, &body)?;
    Ok(super::expiring_from_ttl_seconds(token, expires_in_seconds))
}

/// At most one user-assigned identity selector may be set — more than one is
/// ambiguous to IMDS, and refusing it at construction beats a dispatch
/// finding out from a 400 every time it signs.
pub(super) fn validate_imds_selector(
    client_id: Option<&str>,
    object_id: Option<&str>,
    msi_res_id: Option<&str>,
) -> Result<(), AuthError> {
    let selected = [client_id, object_id, msi_res_id]
        .into_iter()
        .filter(|value| value.is_some())
        .count();
    if selected > 1 {
        return Err(AuthError::Config(
            "at most one of client_id, object_id or msi_res_id may be set".to_string(),
        ));
    }
    Ok(())
}

/// Read `IDENTITY_ENDPOINT` and `IDENTITY_HEADER` through `read`, which the
/// real caller fills with [`std::env::var`] and a test fills with a fixed
/// map — the same split `flexiq-server`'s own `Config::from_env`/`from_map`
/// uses, so a test never has to mutate the process environment to exercise
/// this path.
///
/// `IDENTITY_ENDPOINT` is vetted by [`accept_env_endpoint`]: it is normally a
/// loopback URL on a random high port, but nothing here trusts that without
/// checking it. A missing variable, either one, is
/// [`AuthError::NoCredentials`] — this source simply is not configured, not
/// misconfigured.
pub(super) fn app_service_from_env(
    read: impl Fn(&str) -> Option<String>,
) -> Result<(url::Url, Secret), AuthError> {
    let endpoint_raw = read("IDENTITY_ENDPOINT")
        .ok_or(AuthError::NoCredentials("IDENTITY_ENDPOINT is not set"))?;
    let header_raw =
        read("IDENTITY_HEADER").ok_or(AuthError::NoCredentials("IDENTITY_HEADER is not set"))?;

    let endpoint = accept_env_endpoint(&endpoint_raw)?;
    Ok((endpoint, Secret::new(header_raw)))
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
    async fn imds_sends_the_metadata_header_and_the_api_version() {
        let stub =
            StubServer::start(200, r#"{"access_token":"imds-token","expires_in":"3599"}"#).await;
        let client = client_pointed_at(&stub);

        fetch_imds(&client, "https://management.azure.com/", None, None, None)
            .await
            .expect("a well-formed response succeeds");

        let received = stub.received();
        assert_eq!(received.len(), 1);
        let request = &received[0];
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

    #[tokio::test]
    async fn imds_expires_in_is_a_json_string() {
        // Deliberately quoted, matching Microsoft's own sample response — a
        // fixture built from an unquoted `3599` would pass against a struct
        // typed `expires_in: i64`, which is the wrong implementation.
        let stub = StubServer::start(
            200,
            r#"{"access_token":"imds-token","expires_in":"3599","expires_on":"1700003599","not_before":"1700000000","resource":"https://management.azure.com/","token_type":"Bearer"}"#,
        )
        .await;
        let client = client_pointed_at(&stub);

        let before = crate::job::now_millis();
        let fetched = fetch_imds(&client, "https://management.azure.com/", None, None, None)
            .await
            .expect("a quoted expires_in must parse");
        let after = crate::job::now_millis();

        assert_eq!(fetched.value, "imds-token");
        assert!(
            fetched.expires_at_ms >= before + 3599 * 1000
                && fetched.expires_at_ms <= after + 3599 * 1000,
            "expected an expiry roughly 3599s out, got {}",
            fetched.expires_at_ms
        );
    }

    #[test]
    fn more_than_one_user_assigned_identity_selector_is_refused_at_construction() {
        let error = validate_imds_selector(Some("client-id"), Some("object-id"), None)
            .expect_err("two selectors set at once is ambiguous");
        assert!(matches!(error, AuthError::Config(_)));

        assert!(validate_imds_selector(Some("client-id"), None, None).is_ok());
        assert!(validate_imds_selector(None, None, None).is_ok());
    }

    #[tokio::test]
    async fn app_service_sends_the_identity_header_and_never_logs_it() {
        let secret_value = "sidecar-secret-9f8e7d6c";
        let identity_header = Secret::new(secret_value);

        // First: the value actually reaches the sidecar in cleartext — that
        // is the credential channel itself, not a leak.
        let ok_stub =
            StubServer::start(200, r#"{"access_token":"app-token","expires_in":"120"}"#).await;
        let ok_client = client_pointed_at(&ok_stub);
        let endpoint = url::Url::parse(&ok_stub.base_url()).expect("stub url parses");

        fetch_app_service(&ok_client, &endpoint, &identity_header, "aud")
            .await
            .expect("a well-formed response succeeds");

        let received = ok_stub.received();
        assert_eq!(received.len(), 1);
        assert!(received[0]
            .headers
            .iter()
            .any(|(name, value)| name == "x-identity-header" && value == secret_value));

        // Second: a refused fetch's error never carries the value, in
        // either its Display or its Debug rendering.
        let err_stub = StubServer::start(403, "forbidden").await;
        let err_client = client_pointed_at(&err_stub);
        let err_endpoint = url::Url::parse(&err_stub.base_url()).expect("stub url parses");

        // `Expiring<String>` carries no `Debug`, so `expect_err` cannot be
        // used here; a plain match needs none.
        let error =
            match fetch_app_service(&err_client, &err_endpoint, &identity_header, "aud").await {
                Err(error) => error,
                Ok(_) => panic!("a 403 must be refused"),
            };

        assert!(!error.to_string().contains(secret_value));
        assert!(!format!("{error:?}").contains(secret_value));
    }

    #[test]
    fn the_identity_header_entry_is_marked_sensitive() {
        // The regression this guards: `fetch_app_service` handing
        // `MetadataClient::fetch` a `sensitive: false` entry for a
        // per-instance secret, which would render in cleartext in any
        // `Debug` of the outgoing request headers.
        let secret_value = "sidecar-secret-9f8e7d6c";
        let entries = app_service_header_entries(&Secret::new(secret_value));

        assert_eq!(entries.len(), 1);
        let (name, value, sensitive) = &entries[0];
        assert_eq!(*name, IDENTITY_HEADER_NAME);
        assert_eq!(value, secret_value);
        assert!(
            sensitive,
            "the identity header entry must be marked sensitive"
        );
    }

    #[test]
    fn a_missing_identity_endpoint_is_no_credentials() {
        let error = app_service_from_env(|name| match name {
            "IDENTITY_HEADER" => Some("some-header-value".to_string()),
            _ => None,
        })
        .expect_err("no IDENTITY_ENDPOINT means no credential source");

        assert!(matches!(error, AuthError::NoCredentials(_)));
    }
}
