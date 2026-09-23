//! Stripe's `Stripe-Signature: t=<unix>,v1=<hex>[,v1=<hex>…]`.
//!
//! The tag is HMAC-SHA256 over `"{t}.{body}"`, keyed by the endpoint's signing
//! secret used as-is (`whsec_…` included — Stripe does not decode it). The
//! timestamp is inside the signed bytes, so checking it against the clock is
//! what turns a captured request into one that expires.

use super::{any_tag_matches, sha256_mac, within_tolerance, Inbound, Key, Rejection};

const HEADER: &str = "stripe-signature";

/// Stripe webhook signature verification.
#[derive(Debug, Clone)]
pub struct Stripe {
    key: Key,
    tolerance_secs: i64,
}

impl Stripe {
    /// A verifier for the endpoint signing secret `secret`.
    pub fn new(secret: &str, tolerance_secs: i64) -> Self {
        Self {
            key: Key::new(secret.as_bytes().to_vec()),
            tolerance_secs,
        }
    }

    pub(super) fn verify(&self, inbound: &Inbound<'_>) -> Result<(), Rejection> {
        let header = inbound
            .header(HEADER)
            .ok_or(Rejection("the Stripe-Signature header is missing"))?;

        let mut timestamp = None;
        let mut tags = Vec::new();
        for item in header.split(',') {
            match item.trim().split_once('=') {
                Some(("t", value)) if timestamp.is_none() => timestamp = Some(value),
                // Only v1 is a real signature; v0 is Stripe's test-mode scheme
                // and verifying it would accept what live mode would refuse.
                Some(("v1", value)) => {
                    if let Ok(tag) = hex::decode(value) {
                        tags.push(tag);
                    }
                }
                _ => {}
            }
        }

        let timestamp = timestamp.ok_or(Rejection("the Stripe-Signature has no timestamp"))?;
        let seconds: i64 = timestamp
            .parse()
            .map_err(|_| Rejection("the Stripe-Signature timestamp is not an integer"))?;
        if tags.is_empty() {
            return Err(Rejection("the Stripe-Signature has no v1 signature"));
        }
        if !within_tolerance(seconds, inbound.now_secs, self.tolerance_secs) {
            return Err(Rejection(
                "the Stripe-Signature timestamp is outside the tolerance",
            ));
        }

        let mac = sha256_mac(&self.key, &[timestamp.as_bytes(), b".", inbound.body])?;
        if any_tag_matches(&mac, &tags) {
            Ok(())
        } else {
            Err(Rejection("no Stripe v1 signature matches the body"))
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue};

    use super::*;

    const BODY: &[u8] = b"{\"id\":\"evt_1\"}";
    /// HMAC-SHA256(`whsec_test_secret`, `1700000000.{"id":"evt_1"}`), computed
    /// outside this crate with Python's `hmac` module.
    const TAG: &str = "248a374f50f943a28b0f6ab50faf9a7e7e29b710fa26df9fb1618b9bf8ea9c9a";

    fn check(header: &str, now: i64) -> Result<(), Rejection> {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER, HeaderValue::from_str(header).expect("valid header"));
        Stripe::new("whsec_test_secret", 300).verify(&Inbound {
            headers: &headers,
            query: "",
            body: BODY,
            now_secs: now,
        })
    }

    #[test]
    fn a_fresh_signature_verifies() {
        assert_eq!(
            check(&format!("t=1700000000,v1={TAG}"), 1_700_000_100),
            Ok(())
        );
    }

    #[test]
    fn any_v1_may_match_while_a_secret_rotates() {
        let header = format!("t=1700000000,v1={},v1={TAG},v0=abc", "00".repeat(32));
        assert_eq!(check(&header, 1_700_000_000), Ok(()));
    }

    #[test]
    fn a_stale_signature_is_refused() {
        assert!(check(&format!("t=1700000000,v1={TAG}"), 1_700_000_301).is_err());
    }

    #[test]
    fn a_v0_signature_alone_is_not_enough() {
        assert!(check(&format!("t=1700000000,v0={TAG}"), 1_700_000_000).is_err());
    }

    #[test]
    fn a_moved_timestamp_breaks_the_signature() {
        assert!(check(&format!("t=1700000001,v1={TAG}"), 1_700_000_000).is_err());
    }
}
