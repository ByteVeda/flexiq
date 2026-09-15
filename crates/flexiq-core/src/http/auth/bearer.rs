//! The one scheme that needs no machinery: a token, sent as-is.

use async_trait::async_trait;
use reqwest::header::{HeaderMap, AUTHORIZATION};

use super::{insert_header, AuthError, Signer, SigningRequest};
use crate::worker::Secret;

/// A static bearer token, sent unchanged on every dispatch.
///
/// The floor: it proves to the target that the caller holds a secret the
/// operator gave it, and nothing more. It covers no part of the request, so a
/// captured request replays forever — which is why the HMAC scheme exists
/// beside it and is what the docs recommend for a target reachable from
/// anywhere but the scheduler.
pub struct BearerSigner {
    token: Secret,
}

impl BearerSigner {
    /// Wrap `secret` for use as a static bearer token.
    ///
    /// Never validates: [`super::OutboundAuth::signer`] is where an empty
    /// secret is refused, so every `BearerSigner` that exists already holds a
    /// non-empty token.
    pub fn new(secret: Secret) -> Self {
        Self { token: secret }
    }
}

#[async_trait]
impl Signer for BearerSigner {
    async fn sign(&self, _request: &SigningRequest<'_>) -> Result<HeaderMap, AuthError> {
        // `Secret` is always built from a `String`, so this is never lossy in
        // practice; `from_utf8_lossy` over an `expect` keeps that an
        // invariant rather than a panic if it is ever wrong.
        let token = String::from_utf8_lossy(self.token.expose_secret());
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
        "bearer"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_request_parts() -> (url::Url, HeaderMap) {
        (
            url::Url::parse("https://example.com/hook").expect("test url parses"),
            HeaderMap::new(),
        )
    }

    #[tokio::test]
    async fn a_bearer_signs_with_one_authorization_header() {
        let signer = BearerSigner::new(Secret::new("tok-abc123"));
        let (url, headers) = empty_request_parts();
        let request = SigningRequest {
            method: "POST",
            url: &url,
            body: b"",
            headers: &headers,
        };

        let signed = signer
            .sign(&request)
            .await
            .expect("a non-empty token signs");

        assert_eq!(signed.len(), 1);
        let (name, value) = signed.iter().next().expect("exactly one header");
        assert_eq!(name.as_str(), "authorization");
        assert_eq!(
            value.to_str().expect("header value is ascii"),
            "Bearer tok-abc123"
        );
    }

    #[tokio::test]
    async fn the_authorization_header_is_marked_sensitive() {
        let signer = BearerSigner::new(Secret::new("tok-abc123"));
        let (url, headers) = empty_request_parts();
        let request = SigningRequest {
            method: "POST",
            url: &url,
            body: b"",
            headers: &headers,
        };

        let signed = signer
            .sign(&request)
            .await
            .expect("a non-empty token signs");
        let value = signed
            .get(AUTHORIZATION)
            .expect("authorization header present");

        assert!(value.is_sensitive());
        // The `Debug` impl, not the accessor above: this is what actually
        // guards a dispatch log, and it is the assertion that would catch
        // `insert_header`'s `sensitive` argument being flipped to `false`
        // later.
        let rendered = format!("{signed:?}");
        assert!(rendered.contains("Sensitive"));
        assert!(!rendered.contains("tok-abc123"));
    }

    #[tokio::test]
    async fn a_header_value_with_a_control_character_is_refused_without_echoing_it() {
        let signer = BearerSigner::new(Secret::new("bad\ntoken"));
        let (url, headers) = empty_request_parts();
        let request = SigningRequest {
            method: "POST",
            url: &url,
            body: b"",
            headers: &headers,
        };

        let error = signer
            .sign(&request)
            .await
            .expect_err("a control character is not a legal header value");

        match &error {
            AuthError::InvalidHeaderValue(header) => assert_eq!(header, "authorization"),
            other => panic!("expected InvalidHeaderValue naming the header, got {other:?}"),
        }
        let message = error.to_string();
        assert!(!message.contains("bad"));
        assert!(!message.contains("token"));
    }

    #[test]
    fn the_scheme_name_is_stable() {
        let signer = BearerSigner::new(Secret::new("tok-abc123"));
        assert_eq!(signer.scheme(), "bearer");
    }
}
