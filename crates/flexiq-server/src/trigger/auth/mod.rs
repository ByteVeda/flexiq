//! How a trigger decides a request came from the sender it was built for.
//!
//! Every trigger carries exactly one [`Verifier`] — there is no anonymous kind,
//! because a public URL that enqueues without proof of origin is a public URL
//! anyone can fill a queue through. The signature kinds verify the **raw**
//! body, the bytes as they arrived, never a re-serialisation of the parsed
//! document: a sender signs what it sent, and re-encoding JSON is not a
//! byte-for-byte round trip.
//!
//! A [`Rejection`] names the check that failed for the log line. It is never
//! sent back: a caller probing the door learns `401` and nothing about which
//! part of its forgery was wrong.

pub mod shared;
pub mod signature;
pub mod standard;
pub mod stripe;
pub mod twilio;

use std::fmt;

use axum::http::HeaderMap;
use hmac::{Hmac, Mac};
use sha2::Sha256;

pub use shared::{SecretLocation, SharedSecret};
pub use signature::{Encoding, HeaderHmac};
pub use standard::StandardWebhooks;
pub use stripe::Stripe;
pub use twilio::Twilio;

/// Default window, in seconds, a timestamped signature stays acceptable.
///
/// Five minutes is what Stripe and Standard Webhooks both document. It bounds
/// how long a captured request can be replayed, while leaving room for the
/// clock skew between the sender and this process.
pub const DEFAULT_TOLERANCE_SECS: i64 = 300;

/// What a verifier reads from one request.
#[derive(Debug, Clone, Copy)]
pub struct Inbound<'a> {
    /// Request headers.
    pub headers: &'a HeaderMap,
    /// The raw query string, without the leading `?`. Empty when absent.
    pub query: &'a str,
    /// The body exactly as it arrived.
    pub body: &'a [u8],
    /// The current Unix time in seconds, passed in so a test can pin it.
    pub now_secs: i64,
}

impl Inbound<'_> {
    /// A header's value, when present and valid visible ASCII.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name)?.to_str().ok()
    }

    /// The first query parameter called `name`, percent-decoded.
    pub fn query_param(&self, name: &str) -> Option<String> {
        url::form_urlencoded::parse(self.query.as_bytes())
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.into_owned())
    }
}

/// Why a request was refused. Logged, never returned to the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rejection(pub &'static str);

impl fmt::Display for Rejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

/// Secret key material. Its `Debug` prints the length and never the bytes, so
/// a definition can be logged whole without leaking what it verifies with.
#[derive(Clone, PartialEq, Eq)]
pub struct Key(Vec<u8>);

impl Key {
    /// Wrap raw key bytes.
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    /// The key bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Key(<{} bytes redacted>)", self.0.len())
    }
}

/// The proof of origin one trigger demands.
#[derive(Debug, Clone)]
pub enum Verifier {
    /// A secret the sender presents verbatim, in a header or the query.
    SharedSecret(SharedSecret),
    /// HMAC-SHA256 of the body in one header — the generic form and GitHub's.
    HeaderHmac(HeaderHmac),
    /// Stripe's timestamped `Stripe-Signature`.
    Stripe(Stripe),
    /// The Standard Webhooks (`webhook-*`) headers.
    StandardWebhooks(StandardWebhooks),
    /// Twilio's `X-Twilio-Signature`.
    Twilio(Twilio),
}

impl Verifier {
    /// Check `inbound` against this verifier.
    pub fn verify(&self, inbound: &Inbound<'_>) -> Result<(), Rejection> {
        match self {
            Self::SharedSecret(verifier) => verifier.verify(inbound),
            Self::HeaderHmac(verifier) => verifier.verify(inbound),
            Self::Stripe(verifier) => verifier.verify(inbound),
            Self::StandardWebhooks(verifier) => verifier.verify(inbound),
            Self::Twilio(verifier) => verifier.verify(inbound),
        }
    }

    /// The kind's configuration name, for a log line.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::SharedSecret(_) => "shared_secret",
            Self::HeaderHmac(verifier) => verifier.kind(),
            Self::Stripe(_) => "stripe",
            Self::StandardWebhooks(_) => "standard_webhooks",
            Self::Twilio(_) => "twilio",
        }
    }
}

/// HMAC-SHA256 keyed by `key` over `parts`, concatenated.
fn sha256_mac(key: &Key, parts: &[&[u8]]) -> Result<Hmac<Sha256>, Rejection> {
    // HMAC takes a key of any length, so this cannot fail in practice; the
    // `Result` is the trait's, and mapping it keeps a panic out of the door.
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key.as_bytes())
        .map_err(|_| Rejection("the signing key was refused by HMAC"))?;
    for part in parts {
        mac.update(part);
    }
    Ok(mac)
}

/// Whether any candidate is `mac`'s tag, each compared in constant time.
///
/// Senders list several signatures while they rotate a secret, and one match
/// is enough.
fn any_tag_matches<M: Mac + Clone>(mac: &M, candidates: &[Vec<u8>]) -> bool {
    candidates
        .iter()
        .any(|candidate| mac.clone().verify_slice(candidate).is_ok())
}

/// Whether `timestamp` is within `tolerance` seconds of `now`, either side.
fn within_tolerance(timestamp: i64, now: i64, tolerance: i64) -> bool {
    timestamp.abs_diff(now) <= tolerance.unsigned_abs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_never_prints_its_bytes() {
        let key = Key::new(b"hunter2-hunter2-hunter2".to_vec());
        let printed = format!("{key:?}");
        assert!(!printed.contains("hunter2"), "{printed}");
        assert!(printed.contains("23 bytes"), "{printed}");
    }

    #[test]
    fn tolerance_is_symmetric_and_inclusive() {
        assert!(within_tolerance(1_000, 1_300, 300));
        assert!(within_tolerance(1_300, 1_000, 300));
        assert!(!within_tolerance(1_000, 1_301, 300));
        // A timestamp that overflows a naive subtraction is simply refused.
        assert!(!within_tolerance(i64::MIN, i64::MAX, 300));
    }

    #[test]
    fn query_params_are_percent_decoded() {
        let headers = HeaderMap::new();
        let inbound = Inbound {
            headers: &headers,
            query: "a=1&token=abc%2Bdef&token=second",
            body: b"",
            now_secs: 0,
        };
        assert_eq!(inbound.query_param("token").as_deref(), Some("abc+def"));
        assert_eq!(inbound.query_param("missing"), None);
    }
}
