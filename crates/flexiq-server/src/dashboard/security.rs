//! Response hardening applied to every route.

use axum::extract::Request;
use axum::http::header::{HeaderName, HeaderValue};
use axum::middleware::Next;
use axum::response::Response;

/// Defence-in-depth headers, matching the SDK dashboards. The CSP assumes a
/// self-contained bundle with no inline scripts; inline style attributes are
/// what force `'unsafe-inline'` in `style-src`.
const SECURITY_HEADERS: &[(&str, &str)] = &[
    (
        "content-security-policy",
        "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; \
         img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; \
         base-uri 'self'; form-action 'self'; object-src 'none'",
    ),
    ("x-content-type-options", "nosniff"),
    ("x-frame-options", "DENY"),
    ("referrer-policy", "same-origin"),
];

/// One choke point so JSON, assets, probes, and redirects all carry the
/// headers — a per-handler approach reliably misses an error path.
pub async fn headers(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    let map = response.headers_mut();
    for (name, value) in SECURITY_HEADERS {
        // Both sides are compile-time constants, so neither parse can fail.
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            map.insert(name, value);
        }
    }
    response
}

/// A fresh URL-safe token with 256 bits of entropy — session tokens, CSRF
/// tokens, and webhook signing secrets all come from here so none of them can
/// quietly end up weaker than the others.
pub fn random_token() -> String {
    use base64::Engine;

    let bytes: [u8; 32] = rand::random();
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Compare two secrets without leaking their common prefix through timing.
pub fn constant_time_eq(left: &str, right: &str) -> bool {
    let (left, right) = (left.as_bytes(), right.as_bytes());
    // Length is not secret — a differing length is already observable from the
    // token that produced it — but the byte comparison must not short-circuit.
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |difference, (a, b)| difference | (a ^ b))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_are_url_safe_and_unique() {
        let token = random_token();
        assert_eq!(token.len(), 43, "32 bytes, base64url, unpadded");
        assert!(token
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
        assert_ne!(token, random_token());
    }

    #[test]
    fn constant_time_eq_matches_ordinary_equality() {
        assert!(constant_time_eq("token", "token"));
        assert!(!constant_time_eq("token", "tokeN"));
        assert!(!constant_time_eq("token", "token-longer"));
        assert!(constant_time_eq("", ""));
    }
}
