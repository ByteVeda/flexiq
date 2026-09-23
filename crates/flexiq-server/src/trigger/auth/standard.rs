//! Standard Webhooks: `webhook-id`, `webhook-timestamp`, `webhook-signature`.
//!
//! The tag is HMAC-SHA256 over `"{id}.{timestamp}.{body}"`, base64-encoded and
//! written as `v1,<tag>`; several may be listed, space-separated. Unlike
//! Stripe, the secret is **decoded** before use — `whsec_` followed by base64 —
//! so a secret that does not decode is a configuration error, caught at boot.

use base64::Engine;

use super::{any_tag_matches, sha256_mac, within_tolerance, Inbound, Key, Rejection};

const SECRET_PREFIX: &str = "whsec_";

/// Standard Webhooks signature verification.
#[derive(Debug, Clone)]
pub struct StandardWebhooks {
    key: Key,
    tolerance_secs: i64,
}

impl StandardWebhooks {
    /// A verifier for `secret`, with or without its `whsec_` prefix.
    pub fn new(secret: &str, tolerance_secs: i64) -> Result<Self, String> {
        let encoded = secret.strip_prefix(SECRET_PREFIX).unwrap_or(secret);
        let key = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|_| {
                "a standard_webhooks secret is base64 after its optional whsec_ prefix, \
                 and this one does not decode"
                    .to_string()
            })?;
        Ok(Self {
            key: Key::new(key),
            tolerance_secs,
        })
    }

    pub(super) fn verify(&self, inbound: &Inbound<'_>) -> Result<(), Rejection> {
        let id = inbound
            .header("webhook-id")
            .ok_or(Rejection("the webhook-id header is missing"))?;
        let timestamp = inbound
            .header("webhook-timestamp")
            .ok_or(Rejection("the webhook-timestamp header is missing"))?;
        let signatures = inbound
            .header("webhook-signature")
            .ok_or(Rejection("the webhook-signature header is missing"))?;

        let seconds: i64 = timestamp
            .parse()
            .map_err(|_| Rejection("the webhook-timestamp is not an integer"))?;
        if !within_tolerance(seconds, inbound.now_secs, self.tolerance_secs) {
            return Err(Rejection("the webhook-timestamp is outside the tolerance"));
        }

        let tags: Vec<Vec<u8>> = signatures
            .split_whitespace()
            .filter_map(|entry| entry.strip_prefix("v1,"))
            .filter_map(|tag| base64::engine::general_purpose::STANDARD.decode(tag).ok())
            .collect();
        if tags.is_empty() {
            return Err(Rejection("the webhook-signature has no v1 signature"));
        }

        let mac = sha256_mac(
            &self.key,
            &[
                id.as_bytes(),
                b".",
                timestamp.as_bytes(),
                b".",
                inbound.body,
            ],
        )?;
        if any_tag_matches(&mac, &tags) {
            Ok(())
        } else {
            Err(Rejection("no webhook-signature matches the body"))
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue};

    use super::*;

    /// The reference vector the Standard Webhooks libraries test against.
    const SECRET: &str = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw";
    const ID: &str = "msg_p5jXN8AQM9LWM0D4loKWxJek";
    const TIMESTAMP: i64 = 1_614_265_330;
    const BODY: &[u8] = b"{\"test\": 2432232314}";
    const SIGNATURE: &str = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=";

    fn check(signature: &str, now: i64) -> Result<(), Rejection> {
        let mut headers = HeaderMap::new();
        headers.insert("webhook-id", HeaderValue::from_static(ID));
        headers.insert(
            "webhook-timestamp",
            HeaderValue::from_str(&TIMESTAMP.to_string()).expect("valid header"),
        );
        headers.insert(
            "webhook-signature",
            HeaderValue::from_str(signature).expect("valid header"),
        );
        StandardWebhooks::new(SECRET, 300)
            .expect("a valid secret")
            .verify(&Inbound {
                headers: &headers,
                query: "",
                body: BODY,
                now_secs: now,
            })
    }

    #[test]
    fn the_reference_vector_verifies() {
        assert_eq!(check(SIGNATURE, TIMESTAMP), Ok(()));
    }

    #[test]
    fn one_of_several_signatures_is_enough() {
        let listed = format!("v1,AAAA {SIGNATURE} v2,ignored");
        assert_eq!(check(&listed, TIMESTAMP + 10), Ok(()));
    }

    #[test]
    fn an_expired_timestamp_is_refused() {
        assert!(check(SIGNATURE, TIMESTAMP + 301).is_err());
    }

    #[test]
    fn a_secret_that_does_not_decode_is_a_config_error() {
        assert!(StandardWebhooks::new("whsec_not base64!", 300).is_err());
    }
}
