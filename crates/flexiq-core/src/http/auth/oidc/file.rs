//! A Kubernetes projected service-account token on disk — the one source
//! with no network at all.
//!
//! The kubelet rotates the file in place well before the token inside it
//! expires, so the only correctness rule this module has is: never cache
//! bytes read once and hand them out again after a rotation. Re-reading in
//! full on every refresh is what [`CredentialCache`](super::cache::CredentialCache)'s
//! own refresh window already buys for free — this module just has to not
//! get clever and skip the read.

use std::path::Path;

use super::jwt;
use crate::http::auth::cache::Expiring;
use crate::http::auth::AuthError;

/// Read the file at `path`, trimming trailing whitespace — a kubelet's
/// projected token file ends in a newline — and re-reading it in full on
/// every refresh: the kubelet rotates the file in place, so a token cached
/// from the first read would outlive its own file.
pub(super) async fn fetch(path: &Path) -> Result<Expiring<String>, AuthError> {
    let raw = tokio::fs::read_to_string(path).await.map_err(|error| {
        AuthError::Config(format!(
            "could not read projected token at {}: {error}",
            path.display()
        ))
    })?;
    let token = raw.trim_end().to_string();
    let expires_at_ms = jwt::expiry_ms(&token)?;
    Ok(super::expiring_from_expiry_ms(token, expires_at_ms))
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Arc;
    use std::time::Duration;

    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;
    use reqwest::header::{HeaderMap, AUTHORIZATION};

    use super::*;
    use crate::http::auth::{Signer, SigningRequest};
    use crate::http::{DispatchClient, EgressPolicy};
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

    /// A sanity check on the fixture `a_file_source_rereads_on_refresh`
    /// relies on: without this, a change to `expired_jwt` that stopped
    /// producing an already-expired token would make that test flaky
    /// rather than fail outright.
    #[test]
    fn the_expired_jwt_fixture_is_actually_expired() {
        assert!(
            jwt::expiry_ms(&expired_jwt("x")).expect("fixture parses") < crate::job::now_millis()
        );
    }

    #[tokio::test]
    async fn a_file_source_rereads_on_refresh() {
        let mut file = tempfile::NamedTempFile::new().expect("temp file creates");
        write!(file, "{}", expired_jwt("first")).expect("temp file writes");

        let dispatch = permissive_dispatch_client();
        let config = super::super::OidcConfig {
            source: super::super::OidcSource::File {
                path: file.path().to_path_buf(),
            },
            audience: String::new(),
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
}
