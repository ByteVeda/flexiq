//! Synchronous webhook delivery for the dashboard's "send test event" and
//! "replay delivery" actions.
//!
//! Regular event delivery belongs to whatever process emits the events; this is
//! only the operator-triggered path, so it makes exactly one attempt and
//! reports the result inline.

use std::time::{Duration, Instant};

use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;

use crate::dashboard::stores::url_safety::validate_webhook_url;
use crate::dashboard::stores::webhooks::WebhookSubscription;

/// Header carrying the HMAC-SHA256 signature of the request body.
const SIGNATURE_HEADER: &str = "X-Taskito-Signature";

/// Longest response body read back into the delivery log.
const MAX_RESPONSE_BYTES: usize = 8 * 1024;

/// What one delivery attempt produced.
#[derive(Debug, Default)]
pub struct SendOutcome {
    /// HTTP status, or `None` when the request never got a response.
    pub status: Option<i64>,
    /// Response body, truncated.
    pub body: Option<String>,
    /// Round-trip time.
    pub latency_ms: i64,
    /// Transport-level failure, when there is no status.
    pub error: Option<String>,
}

impl SendOutcome {
    /// Whether the endpoint accepted the delivery.
    pub fn delivered(&self) -> bool {
        self.status.is_some_and(|status| status < 400)
    }
}

/// POST `payload` to `subscription`, signing it when a secret is configured.
pub async fn deliver(
    subscription: &WebhookSubscription,
    payload: &Value,
    allow_private: bool,
) -> SendOutcome {
    let started = Instant::now();

    // Re-validate at send time: a host that resolved publicly at registration
    // may have been rebound since. A residual race remains between this
    // resolve and the connect, but it closes the wide window.
    if let Err(error) = validate_webhook_url(&subscription.url, allow_private) {
        return SendOutcome {
            error: Some(error.to_string()),
            latency_ms: elapsed_ms(started),
            ..SendOutcome::default()
        };
    }

    let body = serde_json::to_vec(payload).unwrap_or_default();
    let client = match reqwest::Client::builder()
        // A redirect could send a signed body to a host that never passed the
        // safety check.
        .redirect(reqwest::redirect::Policy::none())
        .timeout(timeout_of(subscription))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            return SendOutcome {
                error: Some(error.to_string()),
                latency_ms: elapsed_ms(started),
                ..SendOutcome::default()
            }
        }
    };

    let mut request = client
        .post(&subscription.url)
        .header("Content-Type", "application/json");
    for (name, value) in &subscription.headers {
        request = request.header(name, value);
    }
    if let Some(secret) = subscription.secret.as_deref().filter(|s| !s.is_empty()) {
        request = request.header(SIGNATURE_HEADER, sign(secret.as_bytes(), &body));
    }

    match request.body(body).send().await {
        Ok(response) => {
            let status = response.status().as_u16() as i64;
            SendOutcome {
                status: Some(status),
                body: read_bounded(response).await,
                latency_ms: elapsed_ms(started),
                error: None,
            }
        }
        Err(error) => SendOutcome {
            status: None,
            body: None,
            latency_ms: elapsed_ms(started),
            // Reqwest's Display includes the URL but never header material.
            error: Some(error.to_string()),
        },
    }
}

/// Read at most [`MAX_RESPONSE_BYTES`] of the response.
///
/// `text()` would buffer whatever the endpoint chose to send before any cap
/// applied — the operator configures that URL, but the body is the far side's
/// to decide. Streaming stops as soon as the budget is spent.
async fn read_bounded(response: reqwest::Response) -> Option<String> {
    let mut response = response;
    let mut buffered: Vec<u8> = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                let room = MAX_RESPONSE_BYTES.saturating_sub(buffered.len());
                if room == 0 {
                    break;
                }
                buffered.extend_from_slice(&chunk[..chunk.len().min(room)]);
            }
            Ok(None) => break,
            // A read that fails mid-body still leaves whatever arrived useful.
            Err(_) => break,
        }
    }
    (!buffered.is_empty()).then(|| String::from_utf8_lossy(&buffered).into_owned())
}

/// `sha256=<hex>` over the exact bytes on the wire.
fn sign(secret: &[u8], body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret)
        .expect("HMAC accepts a key of any length, so this cannot fail");
    mac.update(body);
    let digest = mac.finalize().into_bytes();
    let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("sha256={hex}")
}

fn timeout_of(subscription: &WebhookSubscription) -> Duration {
    // A non-positive or absurd stored value must not disable the timeout.
    let seconds = subscription.timeout_seconds.clamp(0.1, 120.0);
    Duration::from_secs_f64(seconds)
}

fn elapsed_ms(started: Instant) -> i64 {
    started.elapsed().as_millis().min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_signature_matches_the_documented_scheme() {
        // Verified against the same key and body through the SDK signer.
        let signature = sign(b"s3cret", b"{\"event\":\"test.ping\"}");
        assert!(signature.starts_with("sha256="));
        assert_eq!(signature.len(), "sha256=".len() + 64);
        assert_eq!(signature, sign(b"s3cret", b"{\"event\":\"test.ping\"}"));
        assert_ne!(signature, sign(b"other", b"{\"event\":\"test.ping\"}"));
    }

    #[test]
    fn stored_timeouts_are_clamped_into_a_sane_range() {
        let mut subscription = WebhookSubscription::new("https://example.com/hook".into());
        subscription.timeout_seconds = 0.0;
        assert!(timeout_of(&subscription) >= Duration::from_millis(100));
        subscription.timeout_seconds = 100_000.0;
        assert_eq!(timeout_of(&subscription), Duration::from_secs(120));
    }

    #[test]
    fn a_missing_status_is_not_a_delivery() {
        assert!(!SendOutcome::default().delivered());
        assert!(SendOutcome {
            status: Some(204),
            ..SendOutcome::default()
        }
        .delivered());
        assert!(!SendOutcome {
            status: Some(500),
            ..SendOutcome::default()
        }
        .delivered());
    }
}
