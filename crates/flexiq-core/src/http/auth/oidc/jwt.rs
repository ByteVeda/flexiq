//! `exp` extraction from a JWT payload — nothing else, and deliberately.
//!
//! [`OidcSigner`](super::OidcSigner) is a presenter, never a verifier: the
//! receiver (Cloud Run, Azure Functions, or whatever else sits behind the
//! push target) checks the token's signature at its own door, and all this
//! crate needs is when to fetch a replacement. Reading `exp` off the payload
//! is enough for that. Do not add a JWT-verification crate here —
//! `jsonwebtoken` or otherwise — this process holds no key that could verify
//! a signature meaningfully, and verifying a token against a key we minted
//! ourselves (or worse, against no key at all) would prove nothing about it.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;

use super::super::AuthError;
use crate::job::now_millis;

/// Used when a token parses as a JWT but carries no readable `exp`.
///
/// Conservative rather than fatal: the token is probably fine, we just cannot
/// plan around it, so we re-fetch a minute later instead of failing a
/// dispatch.
pub(crate) const FALLBACK_TTL_MS: i64 = 60_000;

/// The label every [`AuthError::CredentialShape`] produced here carries.
///
/// [`expiry_ms`] takes no endpoint parameter — every source hands it a bare
/// token string — so it cannot report which credential endpoint the token
/// came from. That is a real loss of precision against
/// [`MetadataEndpoint::label`](crate::http::auth::metadata::MetadataEndpoint::label),
/// accepted deliberately: `AuthError::CredentialShape`'s job is to never let
/// a response body reach a log, not to pinpoint its source, and this label
/// still says *which layer* rejected the token.
const LABEL: &str = "oidc identity token";

/// The `exp` claim of a JWT, in Unix milliseconds.
///
/// Reads the payload without verifying anything. We are the presenter, not
/// the receiver: the target verifies the signature, and all we need is when
/// to fetch a replacement. Do not add a JWT library here — verifying our own
/// token would prove nothing about it.
pub(crate) fn expiry_ms(token: &str) -> Result<i64, AuthError> {
    let mut segments = token.split('.');
    let header = segments.next();
    let payload = segments.next();
    let signature = segments.next();
    let extra = segments.next();

    // A two-segment answer is an error page, not a token; a fourth segment
    // is some other wire format entirely. Both are shape errors, not
    // "missing exp" — this crate never even gets to look for a claim.
    let (Some(_header), Some(payload), Some(_signature), None) =
        (header, payload, signature, extra)
    else {
        return Err(shape_error("not three base64url segments"));
    };

    let decoded = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| shape_error("not three base64url segments"))?;

    let claims: serde_json::Value =
        serde_json::from_slice(&decoded).map_err(|_| shape_error("not valid JSON"))?;

    // `as_i64` rather than a typed `Claims { exp: Option<i64> }` struct: a
    // present-but-wrong-shaped `exp` (a string, a float, absent entirely)
    // must fall back exactly like a missing one, not turn into a shape
    // error — only the segment count and the JSON parse above are fatal;
    // a malformed or missing `exp` is not.
    match claims.get("exp").and_then(serde_json::Value::as_i64) {
        Some(exp_seconds) => Ok(exp_seconds.saturating_mul(1000)),
        None => Ok(now_millis().saturating_add(FALLBACK_TTL_MS)),
    }
}

fn shape_error(reason: &'static str) -> AuthError {
    AuthError::CredentialShape {
        endpoint: LABEL,
        reason,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base64url(bytes: &[u8]) -> String {
        URL_SAFE_NO_PAD.encode(bytes)
    }

    /// A hand-built, unsigned three-segment token: header and signature are
    /// both arbitrary bytes, since `expiry_ms` never inspects either — that
    /// is the whole point of this module.
    fn token_with_payload(payload_json: &str) -> String {
        format!(
            "{}.{}.{}",
            base64url(br#"{"alg":"none","typ":"JWT"}"#),
            base64url(payload_json.as_bytes()),
            base64url(b"not-a-real-signature"),
        )
    }

    #[test]
    fn the_exp_claim_is_read_without_verifying_anything() {
        let token = token_with_payload(r#"{"sub":"scheduler","exp":1700000000}"#);
        assert_eq!(
            expiry_ms(&token).expect("a valid exp reads"),
            1_700_000_000_000
        );
    }

    #[test]
    fn a_token_that_is_not_three_segments_is_a_shape_error() {
        for token in ["only-one-segment", "two.segments", "a.b.c.d"] {
            let error = expiry_ms(token).expect_err(&format!("{token:?} is not three segments"));
            assert!(matches!(
                error,
                AuthError::CredentialShape {
                    reason: "not three base64url segments",
                    ..
                }
            ));
        }
    }

    #[test]
    fn a_token_without_a_readable_exp_falls_back_rather_than_failing() {
        let before = now_millis();
        let token = token_with_payload(r#"{"sub":"scheduler"}"#);

        let result = expiry_ms(&token).expect("a missing exp is not an error");

        let after = now_millis();
        // The literal, not `FALLBACK_TTL_MS`: asserting against the constant
        // under test would let a change to its value pass silently.
        assert!(
            result >= before + 60_000 && result <= after + 60_000,
            "expected roughly now + 60_000ms, got {result}"
        );
    }

    #[test]
    fn a_payload_that_is_not_json_is_a_shape_error() {
        let token = format!(
            "{}.{}.{}",
            base64url(b"header"),
            base64url(b"not-json-at-all"),
            base64url(b"sig"),
        );

        let error = expiry_ms(&token).expect_err("a non-JSON payload is a shape error");
        assert!(matches!(
            error,
            AuthError::CredentialShape {
                reason: "not valid JSON",
                ..
            }
        ));
    }

    #[test]
    fn an_exp_in_seconds_becomes_milliseconds() {
        let token = token_with_payload(r#"{"exp":1735689600}"#);
        assert_eq!(
            expiry_ms(&token).expect("a valid exp reads"),
            1_735_689_600_000
        );
    }
}
