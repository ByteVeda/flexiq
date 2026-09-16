//! Google Cloud's metadata-server identity token — Cloud Run and GKE's
//! answer to "prove you are the scheduler."
//!
//! The response body *is* the JWT, sent as `text/plain`: no envelope, no
//! `expires_in`, nothing to parse but the token itself. Expiry comes from
//! [`jwt::expiry_ms`](super::jwt::expiry_ms).

use reqwest::Method;

use super::jwt;
use crate::http::auth::cache::Expiring;
use crate::http::auth::metadata::{MetadataClient, MetadataEndpoint};
use crate::http::auth::AuthError;

/// Every GCE/GKE metadata call must carry this header or the metadata server
/// refuses it, treating the request as accidental rather than deliberate.
const METADATA_FLAVOR_HEADER: &str = "Metadata-Flavor";
const METADATA_FLAVOR_VALUE: &str = "Google";

/// `format=full` adds instance and project claims to the token beyond the
/// bare identity — exactly what an audit trail on the receiving side wants,
/// and free to ask for since the endpoint already requires an audience.
const FORMAT_FULL: &str = "full";

/// Fetch a fresh Google-signed identity token bound to `audience`.
///
/// Tries [`MetadataEndpoint::GoogleIdentity`] by name first — Cloud Run and
/// GKE both resolve `metadata.google.internal` — and falls back to
/// [`MetadataEndpoint::GoogleIdentityByIp`] only when the name fails to
/// resolve or connect ([`AuthError::Transport`]): a hardened resolver inside
/// some clusters will not resolve the name at all. A non-2xx
/// ([`AuthError::CredentialEndpoint`]) is a real answer from the real
/// metadata server — a service account missing a grant, say — and retrying
/// it against the same server by IP would just ask the same question twice,
/// not route around anything.
pub(super) async fn fetch(
    metadata: &MetadataClient,
    audience: &str,
) -> Result<Expiring<String>, AuthError> {
    let query = [("audience", audience), ("format", FORMAT_FULL)];
    // Not sensitive: `Metadata-Flavor: Google` is a fixed literal, not
    // credential material.
    let headers = [(METADATA_FLAVOR_HEADER, METADATA_FLAVOR_VALUE, false)];

    let body = match metadata
        .fetch(
            &MetadataEndpoint::GoogleIdentity,
            Method::GET,
            &query,
            &headers,
        )
        .await
    {
        Ok(body) => body,
        Err(AuthError::Transport(_)) => {
            metadata
                .fetch(
                    &MetadataEndpoint::GoogleIdentityByIp,
                    Method::GET,
                    &query,
                    &headers,
                )
                .await?
        }
        Err(other) => return Err(other),
    };

    // `MetadataClient::fetch` silently truncates at its own cap (16 KiB;
    // see `metadata.rs`'s `MAX_BODY_BYTES`) and gives this caller no way to
    // tell a truncated body from a complete one. That is safe for the
    // structured JSON responses the other three sources read — truncation
    // there almost always breaks JSON parsing outright — but this is the
    // one source whose whole response *is* the credential: a body cut off
    // partway through the signature segment would still split into three
    // syntactically valid parts, still carry a readable `exp`, and still
    // look like success here, only to be refused by the receiver's
    // signature check with nothing in this process's own logs pointing at
    // truncation as the cause. Accepted rather than guarded against: a
    // GCE/Cloud Run identity token, even with `format=full`, is nowhere
    // near 16 KiB in practice.
    let token = body.trim().to_string();
    let expires_at_ms = jwt::expiry_ms(&token)?;
    Ok(super::expiring_from_expiry_ms(token, expires_at_ms))
}

#[cfg(test)]
mod tests {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;

    use super::*;
    use crate::http::testing::StubServer;

    fn base64url(bytes: &[u8]) -> String {
        URL_SAFE_NO_PAD.encode(bytes)
    }

    /// A hand-built, unsigned three-segment token — `google::fetch` never
    /// verifies it, only reads `exp` off the payload via `jwt::expiry_ms`.
    fn sample_jwt(exp_seconds: i64) -> String {
        format!(
            "{}.{}.{}",
            base64url(br#"{"alg":"none","typ":"JWT"}"#),
            base64url(format!(r#"{{"exp":{exp_seconds}}}"#).as_bytes()),
            base64url(b"not-a-real-signature"),
        )
    }

    fn client_pointed_at(stub: &StubServer) -> MetadataClient {
        MetadataClient::with_base_url(url::Url::parse(&stub.base_url()).expect("stub url parses"))
            .expect("test client builds")
    }

    #[tokio::test]
    async fn the_identity_request_carries_the_audience_and_the_flavour_header() {
        let stub = StubServer::start(200, sample_jwt(4_000_000_000)).await;
        let client = client_pointed_at(&stub);

        fetch(&client, "https://push.example.com")
            .await
            .expect("a 200 with a bare JWT body succeeds");

        let received = stub.received();
        assert_eq!(received.len(), 1);
        let request = &received[0];
        assert!(request
            .target
            .starts_with("/computeMetadata/v1/instance/service-accounts/default/identity?"));
        assert!(request.target.contains("audience=https"));
        assert!(request.target.contains("format=full"));
        assert!(request
            .headers
            .iter()
            .any(|(name, value)| name == "metadata-flavor" && value == "Google"));
    }

    #[tokio::test]
    async fn a_bare_jwt_body_is_accepted_and_its_expiry_read() {
        let stub = StubServer::start(200, sample_jwt(1_700_000_000)).await;
        let client = client_pointed_at(&stub);

        let fetched = fetch(&client, "https://push.example.com")
            .await
            .expect("a bare JWT body is a valid response");

        assert_eq!(fetched.expires_at_ms, 1_700_000_000_000);
        assert_eq!(fetched.value, sample_jwt(1_700_000_000));
    }

    #[tokio::test]
    async fn a_non_2xx_does_not_fall_back_to_the_ip_endpoint() {
        let stub = StubServer::start(403, "forbidden").await;
        let client = client_pointed_at(&stub);

        // `Expiring<String>` carries no `Debug`, so `expect_err` cannot be
        // used here; a plain match needs none.
        let error = match fetch(&client, "https://push.example.com").await {
            Err(error) => error,
            Ok(_) => panic!("a 403 is a real answer, not a reason to retry by IP"),
        };

        assert!(matches!(
            error,
            AuthError::CredentialEndpoint { status: 403, .. }
        ));
        // A retry-by-IP would be a second request against the same stub —
        // there must be exactly one.
        assert_eq!(stub.request_count(), 1);
    }
}
