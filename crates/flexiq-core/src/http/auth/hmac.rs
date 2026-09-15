//! HMAC-SHA256 signing for outbound dispatch — the replay-resistant scheme
//! GitHub issue #844 names by "works everywhere".
//!
//! Deliberately **not** wire-compatible with the shipped `X-Flexiq-Signature`
//! webhook contract (`crates/flexiq-server/src/dashboard/webhook_sender.rs`,
//! four implementations deep across the SDKs). That scheme means "HMAC over
//! the body, no replay defence" to every one of them. Reusing its header name
//! with different signed bytes would make a correct webhook verifier reject
//! every dispatch — and worse, would let someone point an existing webhook
//! verifier at a dispatch endpoint and get a verifier that ignores the
//! timestamp entirely, accepting unbounded replays: same name, different
//! semantics, failing open. So this scheme gets new header names, a new
//! scheme string, and a timestamp and nonce bound into the signature.

use std::time::Duration;

use async_trait::async_trait;
use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use sha2::{Digest, Sha256};

use super::{insert_header, AuthError, Signer, SigningRequest};
use crate::job::now_millis;
use crate::worker::auth::constant_time_eq;
use crate::worker::Secret;

/// Leads the string to sign, so a signature minted under a future scheme can
/// never be replayed as a v1 one even under the same key.
const SCHEME: &str = "FLEXIQ-HMAC-SHA256";

/// 128 bits: large enough that a receiver's nonce set never collides, small
/// enough to stay a short header.
const NONCE_BYTES: usize = 16;

/// The receiver-side default this crate's verifier uses. Five minutes is the
/// smallest window that survives ordinary container clock drift.
pub const DEFAULT_MAX_SKEW: Duration = Duration::from_secs(300);

/// Header carrying `v1=<64 lowercase hex chars>`.
///
/// Marked sensitive when sent: not because it is credential material — a
/// signature authorises one request, not the holder of a key — but because
/// nothing is lost by keeping it out of a `{headers:?}` dump either.
pub const HDR_SIGNATURE: &str = "x-flexiq-dispatch-signature";
/// Header carrying the signed timestamp, Unix seconds, decimal.
pub const HDR_TIMESTAMP: &str = "x-flexiq-dispatch-timestamp";
/// Header carrying the signed nonce, lowercase hex.
pub const HDR_NONCE: &str = "x-flexiq-dispatch-nonce";
/// Header naming which secret signed the request. Public, unlike the other
/// three: it is what lets a receiver pick between two secrets mid-rotation,
/// so it is a plain header and may appear in logs.
pub const HDR_KEY_ID: &str = "x-flexiq-dispatch-key-id";

/// Shared-secret signing for one push target.
#[derive(Clone, Debug)]
pub struct HmacConfig {
    /// Sent as `x-flexiq-dispatch-key-id` so a receiver can hold two secrets
    /// during a rotation. Public; it appears in logs on purpose. Not itself
    /// signed: a receiver reads it *before* verifying, to pick which secret
    /// to check the signature against, so a tampered id just makes the
    /// signature fail to match the wrong secret — the same
    /// [`HmacRejection::Mismatch`] as any other forged request, not a
    /// bypass.
    pub key_id: Option<String>,
    /// The shared secret the signature is computed with.
    pub secret: Secret,
}

/// HMAC-SHA256 over a canonical description of the request.
pub struct HmacSigner {
    key_id: Option<String>,
    secret: Secret,
}

impl HmacSigner {
    /// Build a signer from `config`.
    ///
    /// Empty-secret validation lives in [`super::OutboundAuth::signer`], the
    /// same place the bearer's does. What this constructor checks is that
    /// `key_id`, if set, is usable as a header value: a control character in
    /// it would otherwise fail every dispatch attempt at [`Self::sign_at`]
    /// instead of failing once, here, before the first one — the same
    /// argument the empty-secret check upstream makes.
    pub fn new(config: HmacConfig) -> Result<Self, AuthError> {
        if let Some(key_id) = &config.key_id {
            HeaderValue::from_str(key_id)
                .map_err(|_| AuthError::InvalidHeaderValue(HDR_KEY_ID.to_string()))?;
        }
        Ok(Self {
            key_id: config.key_id,
            secret: config.secret,
        })
    }

    /// The pure core, with the two impure inputs injected.
    ///
    /// Exists so the whole scheme is a function of its arguments and can be
    /// pinned to a vector: [`Signer::sign`] is this plus a clock and a random
    /// nonce.
    fn sign_at(
        &self,
        request: &SigningRequest<'_>,
        unix_seconds: i64,
        nonce: [u8; NONCE_BYTES],
    ) -> Result<HeaderMap, AuthError> {
        let nonce_hex = hex_lower(&nonce);
        let target = request_target(request.url);
        let body_hex = sha256_hex(request.body);
        let to_sign = string_to_sign(request.method, &target, unix_seconds, &nonce_hex, &body_hex);

        let signature = hmac_sha256_hex(self.secret.expose_secret(), &to_sign)
            .map_err(|_| AuthError::Config("hmac secret could not be used as a key".to_string()))?;

        let mut headers = HeaderMap::with_capacity(if self.key_id.is_some() { 4 } else { 3 });
        insert_header(
            &mut headers,
            HeaderName::from_static(HDR_SIGNATURE),
            &format!("v1={signature}"),
            true,
        )?;
        insert_header(
            &mut headers,
            HeaderName::from_static(HDR_TIMESTAMP),
            &unix_seconds.to_string(),
            false,
        )?;
        insert_header(
            &mut headers,
            HeaderName::from_static(HDR_NONCE),
            &nonce_hex,
            false,
        )?;
        if let Some(key_id) = &self.key_id {
            insert_header(
                &mut headers,
                HeaderName::from_static(HDR_KEY_ID),
                key_id,
                false,
            )?;
        }
        Ok(headers)
    }
}

#[async_trait]
impl Signer for HmacSigner {
    async fn sign(&self, request: &SigningRequest<'_>) -> Result<HeaderMap, AuthError> {
        // Unix seconds, not milliseconds: the wire format is decimal seconds,
        // and the verifier's skew window is granular to the second anyway.
        let unix_seconds = now_millis() / 1000;
        let nonce: [u8; NONCE_BYTES] = rand::random();
        self.sign_at(request, unix_seconds, nonce)
    }

    fn scheme(&self) -> &'static str {
        "hmac-sha256"
    }
}

/// The origin-form request target a client actually sends: `url.path()`,
/// plus `?` and `url.query()` when there is one. Not the full URL — the host
/// is deliberately not signed (see [`verify`]'s doc for why).
fn request_target(url: &url::Url) -> String {
    match url.query() {
        Some(query) => format!("{}?{}", url.path(), query),
        None => url.path().to_string(),
    }
}

/// `sha256(body)`, lowercase hex.
///
/// A digest, not the body inline: it keeps the string to sign fixed-size and
/// printable, so a mismatch is diagnosable — logged, pasted into an issue —
/// without dumping a job payload anywhere.
fn sha256_hex(body: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(body);
    hex_lower(&hasher.finalize())
}

/// Lowercase hex, the same `format!("{byte:02x}")` fold
/// `flexiq-server`'s `webhook_sender.rs` uses for its own HMAC digest.
fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// `hex(HMAC-SHA256(secret, message))`, lowercase — used by both the signer
/// and the reference verifier, so the two can never compute the digest two
/// different ways.
///
/// `Hmac::<Sha256>::new_from_slice` returns a `Result` only because the
/// `Mac` trait also covers MACs that require a fixed key length; HMAC itself
/// accepts a key of any length, so the `Err` arm is unreachable here. It is
/// still propagated through `Result` rather than `expect`-ed: this repo
/// already carries one `expect` resting on that exact fact
/// (`flexiq-server/src/dashboard/webhook_sender.rs`'s `sign`), and library
/// code in this crate does not add a second instance of it.
fn hmac_sha256_hex(secret: &[u8], message: &str) -> Result<String, ()> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).map_err(|_| ())?;
    mac.update(message.as_bytes());
    Ok(hex_lower(&mac.finalize().into_bytes()))
}

/// The string a signature covers, exposed so the contract has one definition
/// that both the signer (via [`Signer::sign`]) and [`verify`] read.
///
/// Six fields, joined with `\n`, no trailing newline. Field injection across
/// the `\n` delimiter is impossible: `unix_seconds` is rendered from an
/// integer (digits only), `nonce_hex`/`body_sha256_hex` are hex (`[0-9a-f]`
/// only), `method` is uppercase ASCII, and `target` is a `url::Url` path and
/// query — `url::Url` strips tab/CR/LF while parsing, so it cannot carry a
/// newline (see the `a_url_path_cannot_contain_a_newline` test below, which
/// asserts that fact about another crate rather than merely commenting it).
pub fn string_to_sign(
    method: &str,
    target: &str,
    unix_seconds: i64,
    nonce_hex: &str,
    body_sha256_hex: &str,
) -> String {
    [
        SCHEME,
        &unix_seconds.to_string(),
        nonce_hex,
        method,
        target,
        body_sha256_hex,
    ]
    .join("\n")
}

/// Why a presented signature was refused.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum HmacRejection {
    /// One of the three required headers was absent.
    #[error("missing the {0} header")]
    Missing(&'static str),
    /// The signature header did not carry the `v1=` prefix this verifier
    /// understands — including a future `v2=`, which must fail closed rather
    /// than be compared as if it were hex under the v1 scheme.
    #[error("unsupported signature scheme version")]
    Version,
    /// The timestamp is further from `now` than `max_skew` permits.
    #[error("timestamp is outside the permitted skew")]
    Skew,
    /// The computed signature does not match the one presented.
    #[error("signature does not match")]
    Mismatch,
    /// A header was present but could not be read as the shape it must have.
    #[error("malformed {0} header")]
    Malformed(&'static str),
}

/// Reference verifier: the stateless half of a receiver's job.
///
/// Checks the signature and the clock window. It deliberately does **not**
/// keep a nonce set — that is durable, receiver-scoped state this crate has
/// no place owning. A receiver that skips it has replay protection only to
/// the width of `max_skew`.
///
/// The host is not part of what is checked, because it is not part of what
/// is signed: proxies and ingress rewrite `Host`, and a per-target secret
/// already binds target identity, so signing it would produce failures that
/// look like key failures. That target-binding is an assumption this module
/// takes on faith, not one it enforces: nothing here stops an operator from
/// reusing one [`HmacConfig`]'s secret across two targets, and doing so
/// would let a request valid for one be replayed at the other. Enforcing
/// "one secret, one target" belongs to whatever loads `HmacConfig` from
/// operator configuration, not to the signer or this verifier.
pub fn verify(
    secret: &Secret,
    request: &SigningRequest<'_>,
    presented: &HeaderMap,
    now_unix: i64,
    max_skew: Duration,
) -> Result<(), HmacRejection> {
    let signature_header = header_str(presented, HDR_SIGNATURE)?;
    let timestamp_header = header_str(presented, HDR_TIMESTAMP)?;
    let nonce_header = header_str(presented, HDR_NONCE)?;

    let presented_signature = signature_header
        .strip_prefix("v1=")
        .ok_or(HmacRejection::Version)?;
    // Caught here rather than left to `constant_time_eq`'s length
    // short-circuit: a wrong-shape value is a different failure than a
    // wrong-value one, and folding it into `Mismatch` would hide that a
    // presented header is not even well-formed.
    if !is_lowercase_hex64(presented_signature) {
        return Err(HmacRejection::Malformed(HDR_SIGNATURE));
    }

    // Digits only, no leading `+`/`-` and no leading zero (unless the value
    // is exactly `0`): `i64::from_str` accepts all of those and would parse
    // `+1735689600` or `01735689600` to the same integer as the canonical
    // `1735689600` the signer emits. This verifier reconstructs the string
    // to sign from the *parsed* integer, so accepting them would make it
    // more permissive than a receiver comparing the header's raw bytes —
    // exactly the kind of contract drift this scheme exists to avoid.
    let is_canonical_decimal = !timestamp_header.is_empty()
        && timestamp_header.bytes().all(|byte| byte.is_ascii_digit())
        && (timestamp_header.len() == 1 || !timestamp_header.starts_with('0'));
    if !is_canonical_decimal {
        return Err(HmacRejection::Malformed(HDR_TIMESTAMP));
    }
    let unix_seconds: i64 = timestamp_header
        .parse()
        .map_err(|_| HmacRejection::Malformed(HDR_TIMESTAMP))?;

    // `i128` so a hostile timestamp anywhere in `i64`'s range cannot overflow
    // the subtraction — an out-of-range value must fail the skew check, not
    // panic the process that checks it.
    let skew = (i128::from(now_unix) - i128::from(unix_seconds)).unsigned_abs();
    if skew > u128::from(max_skew.as_secs()) {
        return Err(HmacRejection::Skew);
    }

    let target = request_target(request.url);
    let body_hex = sha256_hex(request.body);
    let to_sign = string_to_sign(
        request.method,
        &target,
        unix_seconds,
        nonce_header,
        &body_hex,
    );

    // Construction cannot fail (see `hmac_sha256_hex`'s doc); a caller that
    // somehow reached that arm anyway could not have produced a matching
    // digest, so folding it to `Mismatch` refuses rather than panics.
    let expected_signature =
        hmac_sha256_hex(secret.expose_secret(), &to_sign).map_err(|_| HmacRejection::Mismatch)?;

    if constant_time_eq(
        expected_signature.as_bytes(),
        presented_signature.as_bytes(),
    ) {
        Ok(())
    } else {
        Err(HmacRejection::Mismatch)
    }
}

/// Whether `value` is exactly 64 lowercase hex characters — the shape
/// [`hmac_sha256_hex`] always produces and the only shape a genuine `v1`
/// signature can have.
fn is_lowercase_hex64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Read one required header as `&str`, naming it in whichever way it failed.
fn header_str<'a>(headers: &'a HeaderMap, name: &'static str) -> Result<&'a str, HmacRejection> {
    headers
        .get(name)
        .ok_or(HmacRejection::Missing(name))?
        .to_str()
        .map_err(|_| HmacRejection::Malformed(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signer_with(secret: &str) -> HmacSigner {
        HmacSigner::new(HmacConfig {
            key_id: None,
            secret: Secret::new(secret),
        })
        .expect("a non-empty secret builds")
    }

    fn url(raw: &str) -> url::Url {
        url::Url::parse(raw).expect("test url parses")
    }

    fn extract_signature(headers: &HeaderMap) -> String {
        headers
            .get(HDR_SIGNATURE)
            .expect("signature header present")
            .to_str()
            .expect("signature header is ascii")
            .strip_prefix("v1=")
            .expect("signature header carries the v1 prefix")
            .to_string()
    }

    fn sign_fixture(
        secret: &str,
        method: &str,
        target_url: &url::Url,
        body: &[u8],
        unix_seconds: i64,
        nonce: [u8; NONCE_BYTES],
    ) -> HeaderMap {
        let request_headers = HeaderMap::new();
        let request = SigningRequest {
            method,
            url: target_url,
            body,
            headers: &request_headers,
        };
        signer_with(secret)
            .sign_at(&request, unix_seconds, nonce)
            .expect("a well-formed request signs")
    }

    #[test]
    fn the_signature_matches_a_pinned_vector() {
        // Computed outside this code:
        //
        //   python3 -c "
        //   import hmac, hashlib
        //   secret = b'pinned-test-secret-do-not-rotate'
        //   nonce = bytes([0x01]*16)
        //   nonce_hex = nonce.hex()
        //   body = b'{\"job_id\":\"42\",\"attempt\":1}'
        //   body_hex = hashlib.sha256(body).hexdigest()
        //   sts = '\n'.join(['FLEXIQ-HMAC-SHA256', '1735689600', nonce_hex,
        //                     'POST', '/dispatch/42?attempt=1', body_hex])
        //   print(hmac.new(secret, sts.encode(), hashlib.sha256).hexdigest())
        //   "
        //
        // printed: ae8e04b171f581a8d602ac9b2c074c06993423f7ebf8932c70bd5af2bdc30933
        //
        // Cross-checked with (note `printf '%s'`, not a `<<<` here-string —
        // a here-string appends a newline, which would hash a different
        // string and silently pin the wrong value):
        //
        //   printf '%s' "$STS" \
        //     | openssl dgst -sha256 -hmac 'pinned-test-secret-do-not-rotate'
        //
        // which printed the same digest.
        let target_url = url("https://push.example.com/dispatch/42?attempt=1");
        let body: &[u8] = br#"{"job_id":"42","attempt":1}"#;
        let headers = sign_fixture(
            "pinned-test-secret-do-not-rotate",
            "POST",
            &target_url,
            body,
            1735689600,
            [0x01u8; NONCE_BYTES],
        );

        assert_eq!(
            extract_signature(&headers),
            "ae8e04b171f581a8d602ac9b2c074c06993423f7ebf8932c70bd5af2bdc30933"
        );
    }

    #[test]
    fn the_string_to_sign_is_exactly_six_newline_joined_fields() {
        let result = string_to_sign(
            "POST",
            "/dispatch/42?attempt=1",
            1735689600,
            "01010101010101010101010101010101",
            "270c575859e3bc6f2fd9b0bb7348b36c9af9542adb5e56807654144cfe3e9b77",
        );

        assert_eq!(
            result,
            "FLEXIQ-HMAC-SHA256\n\
             1735689600\n\
             01010101010101010101010101010101\n\
             POST\n\
             /dispatch/42?attempt=1\n\
             270c575859e3bc6f2fd9b0bb7348b36c9af9542adb5e56807654144cfe3e9b77"
        );
        assert!(!result.ends_with('\n'));
    }

    #[test]
    fn changing_any_signed_field_changes_the_signature() {
        let base_secret = "correct-horse-battery-staple";
        let base_method = "POST";
        let base_url = url("https://push.example.com/dispatch/42?job=abc123");
        let base_body: &[u8] = br#"{"job_id":"42"}"#;
        let base_unix_seconds: i64 = 1_700_000_000;
        let base_nonce = [0x02u8; NONCE_BYTES];

        let sign_with = |secret: &str,
                         method: &str,
                         target_url: &url::Url,
                         body: &[u8],
                         unix_seconds: i64,
                         nonce: [u8; NONCE_BYTES]| {
            extract_signature(&sign_fixture(
                secret,
                method,
                target_url,
                body,
                unix_seconds,
                nonce,
            ))
        };

        let base = sign_with(
            base_secret,
            base_method,
            &base_url,
            base_body,
            base_unix_seconds,
            base_nonce,
        );

        let different_path = url("https://push.example.com/dispatch/43?job=abc123");
        let different_query = url("https://push.example.com/dispatch/42?job=xyz999");

        let rows: Vec<(&str, String)> = vec![
            (
                "secret",
                sign_with(
                    "a-different-secret-value",
                    base_method,
                    &base_url,
                    base_body,
                    base_unix_seconds,
                    base_nonce,
                ),
            ),
            (
                "body",
                sign_with(
                    base_secret,
                    base_method,
                    &base_url,
                    br#"{"job_id":"43"}"#,
                    base_unix_seconds,
                    base_nonce,
                ),
            ),
            (
                "timestamp",
                sign_with(
                    base_secret,
                    base_method,
                    &base_url,
                    base_body,
                    base_unix_seconds + 1,
                    base_nonce,
                ),
            ),
            (
                "nonce",
                sign_with(
                    base_secret,
                    base_method,
                    &base_url,
                    base_body,
                    base_unix_seconds,
                    [0x03u8; NONCE_BYTES],
                ),
            ),
            (
                "method",
                sign_with(
                    base_secret,
                    "PUT",
                    &base_url,
                    base_body,
                    base_unix_seconds,
                    base_nonce,
                ),
            ),
            (
                "path",
                sign_with(
                    base_secret,
                    base_method,
                    &different_path,
                    base_body,
                    base_unix_seconds,
                    base_nonce,
                ),
            ),
            (
                "query",
                sign_with(
                    base_secret,
                    base_method,
                    &different_query,
                    base_body,
                    base_unix_seconds,
                    base_nonce,
                ),
            ),
        ];

        let mut all: Vec<(&str, String)> = vec![("base", base)];
        all.extend(rows);

        for i in 0..all.len() {
            for j in (i + 1)..all.len() {
                assert_ne!(
                    all[i].1, all[j].1,
                    "'{}' and '{}' produced the same signature",
                    all[i].0, all[j].0
                );
            }
        }
    }

    #[test]
    fn the_host_is_not_signed() {
        let secret = "shared-secret";
        let method = "POST";
        let body: &[u8] = b"payload";
        let unix_seconds = 1_700_000_000;
        let nonce = [0x04u8; NONCE_BYTES];

        let signed_a = sign_fixture(
            secret,
            method,
            &url("https://a.example.com/dispatch/1"),
            body,
            unix_seconds,
            nonce,
        );
        let signed_b = sign_fixture(
            secret,
            method,
            &url("https://b.example.com/dispatch/1"),
            body,
            unix_seconds,
            nonce,
        );

        assert_eq!(extract_signature(&signed_a), extract_signature(&signed_b));
    }

    #[test]
    fn a_signature_round_trips_through_verify() {
        let secret = Secret::new("round-trip-secret");
        let target_url = url("https://push.example.com/dispatch/7");
        let body: &[u8] = b"job-payload";
        let unix_seconds = 1_700_000_500;
        let nonce = [0x05u8; NONCE_BYTES];

        let signed = sign_fixture(
            "round-trip-secret",
            "POST",
            &target_url,
            body,
            unix_seconds,
            nonce,
        );

        let request_headers = HeaderMap::new();
        let request = SigningRequest {
            method: "POST",
            url: &target_url,
            body,
            headers: &request_headers,
        };

        let result = verify(&secret, &request, &signed, unix_seconds, DEFAULT_MAX_SKEW);
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn a_timestamp_outside_the_window_is_refused() {
        let secret = Secret::new("skew-secret");
        let target_url = url("https://push.example.com/dispatch/9");
        let body: &[u8] = b"payload";
        let unix_seconds = 1_700_000_000;
        let nonce = [0x06u8; NONCE_BYTES];
        let max_skew = Duration::from_secs(60);

        let signed = sign_fixture(
            "skew-secret",
            "POST",
            &target_url,
            body,
            unix_seconds,
            nonce,
        );
        let request_headers = HeaderMap::new();
        let request = SigningRequest {
            method: "POST",
            url: &target_url,
            body,
            headers: &request_headers,
        };

        assert_eq!(
            verify(&secret, &request, &signed, unix_seconds + 61, max_skew),
            Err(HmacRejection::Skew)
        );
        assert_eq!(
            verify(&secret, &request, &signed, unix_seconds - 61, max_skew),
            Err(HmacRejection::Skew)
        );
        assert_eq!(
            verify(&secret, &request, &signed, unix_seconds + 60, max_skew),
            Ok(()),
            "exactly the positive skew boundary must be accepted"
        );
        assert_eq!(
            verify(&secret, &request, &signed, unix_seconds - 60, max_skew),
            Ok(()),
            "exactly the negative skew boundary must be accepted"
        );
    }

    #[test]
    fn a_tampered_body_is_refused() {
        let secret = Secret::new("tamper-secret");
        let target_url = url("https://push.example.com/dispatch/11");
        let unix_seconds = 1_700_000_100;
        let nonce = [0x07u8; NONCE_BYTES];

        let signed = sign_fixture(
            "tamper-secret",
            "POST",
            &target_url,
            b"original-body",
            unix_seconds,
            nonce,
        );

        let request_headers = HeaderMap::new();
        let tampered_request = SigningRequest {
            method: "POST",
            url: &target_url,
            body: b"tampered-body",
            headers: &request_headers,
        };

        assert_eq!(
            verify(
                &secret,
                &tampered_request,
                &signed,
                unix_seconds,
                DEFAULT_MAX_SKEW,
            ),
            Err(HmacRejection::Mismatch)
        );
    }

    #[test]
    fn a_v2_signature_is_refused_rather_than_compared() {
        let secret = Secret::new("version-secret");
        let target_url = url("https://push.example.com/dispatch/13");
        let body: &[u8] = b"payload";
        let unix_seconds = 1_700_000_200;
        let nonce = [0x08u8; NONCE_BYTES];

        let mut signed = sign_fixture(
            "version-secret",
            "POST",
            &target_url,
            body,
            unix_seconds,
            nonce,
        );
        let presented_hex = extract_signature(&signed);
        insert_header(
            &mut signed,
            HeaderName::from_static(HDR_SIGNATURE),
            &format!("v2={presented_hex}"),
            false,
        )
        .expect("test header value is legal");

        let request_headers = HeaderMap::new();
        let request = SigningRequest {
            method: "POST",
            url: &target_url,
            body,
            headers: &request_headers,
        };

        assert_eq!(
            verify(&secret, &request, &signed, unix_seconds, DEFAULT_MAX_SKEW),
            Err(HmacRejection::Version)
        );
    }

    #[test]
    fn every_missing_header_is_named() {
        let secret = Secret::new("missing-secret");
        let target_url = url("https://push.example.com/dispatch/17");
        let body: &[u8] = b"payload";
        let unix_seconds = 1_700_000_300;
        let nonce = [0x09u8; NONCE_BYTES];

        let request_headers = HeaderMap::new();
        let request = SigningRequest {
            method: "POST",
            url: &target_url,
            body,
            headers: &request_headers,
        };

        for missing in [HDR_SIGNATURE, HDR_TIMESTAMP, HDR_NONCE] {
            let mut signed = sign_fixture(
                "missing-secret",
                "POST",
                &target_url,
                body,
                unix_seconds,
                nonce,
            );
            signed.remove(missing);

            assert_eq!(
                verify(&secret, &request, &signed, unix_seconds, DEFAULT_MAX_SKEW),
                Err(HmacRejection::Missing(missing)),
                "removing {missing} must be reported by name"
            );
        }
    }

    #[test]
    fn a_malformed_timestamp_is_refused() {
        let secret = Secret::new("malformed-timestamp-secret");
        let target_url = url("https://push.example.com/dispatch/21");
        let body: &[u8] = b"payload";
        let unix_seconds = 1_700_000_400;
        let nonce = [0x0au8; NONCE_BYTES];

        let request_headers = HeaderMap::new();
        let request = SigningRequest {
            method: "POST",
            url: &target_url,
            body,
            headers: &request_headers,
        };

        // Each parses to the same `i64` as the canonical form the signer
        // emits, so if `verify` compared raw bytes only after parsing it
        // would accept all three — the guard has to run before the parse.
        for malformed in ["+1700000400", "01700000400", "not-a-number"] {
            let mut signed = sign_fixture(
                "malformed-timestamp-secret",
                "POST",
                &target_url,
                body,
                unix_seconds,
                nonce,
            );
            insert_header(
                &mut signed,
                HeaderName::from_static(HDR_TIMESTAMP),
                malformed,
                false,
            )
            .expect("test header value is legal");

            assert_eq!(
                verify(&secret, &request, &signed, unix_seconds, DEFAULT_MAX_SKEW),
                Err(HmacRejection::Malformed(HDR_TIMESTAMP)),
                "{malformed:?} must be refused as malformed, not parsed loosely"
            );
        }
    }

    #[test]
    fn a_malformed_signature_is_refused() {
        let secret = Secret::new("malformed-signature-secret");
        let target_url = url("https://push.example.com/dispatch/23");
        let body: &[u8] = b"payload";
        let unix_seconds = 1_700_000_500;
        let nonce = [0x0bu8; NONCE_BYTES];

        let request_headers = HeaderMap::new();
        let request = SigningRequest {
            method: "POST",
            url: &target_url,
            body,
            headers: &request_headers,
        };

        // Too short, and uppercase hex: both are the wrong shape for a v1
        // signature and must be refused before ever reaching a comparison,
        // not folded into `Mismatch`.
        for malformed in [
            "v1=abcd",
            "v1=AE8E04B171F581A8D602AC9B2C074C06993423F7EBF8932C70BD5AF2BDC30933",
        ] {
            let mut signed = sign_fixture(
                "malformed-signature-secret",
                "POST",
                &target_url,
                body,
                unix_seconds,
                nonce,
            );
            insert_header(
                &mut signed,
                HeaderName::from_static(HDR_SIGNATURE),
                malformed,
                false,
            )
            .expect("test header value is legal");

            assert_eq!(
                verify(&secret, &request, &signed, unix_seconds, DEFAULT_MAX_SKEW),
                Err(HmacRejection::Malformed(HDR_SIGNATURE)),
                "{malformed:?} must be refused as malformed, not compared"
            );
        }
    }

    #[tokio::test]
    async fn two_signatures_of_one_request_differ() {
        let signer = signer_with("nonce-freshness-secret");
        let target_url = url("https://push.example.com/dispatch/19");
        let request_headers = HeaderMap::new();
        let request = SigningRequest {
            method: "POST",
            url: &target_url,
            body: b"payload",
            headers: &request_headers,
        };

        assert_eq!(signer.scheme(), "hmac-sha256");

        let first = signer.sign(&request).await.expect("first signs");
        let second = signer.sign(&request).await.expect("second signs");

        assert_ne!(
            first.get(HDR_NONCE).expect("nonce present"),
            second.get(HDR_NONCE).expect("nonce present"),
            "two signing calls must draw two different nonces"
        );
        assert_ne!(
            extract_signature(&first),
            extract_signature(&second),
            "a fresh nonce must produce a fresh signature"
        );
    }

    #[test]
    fn the_config_never_reaches_a_formatter() {
        let secret_value = "a1b2c3d4e5f6g7h8";
        let config = HmacConfig {
            key_id: Some("rotation-2026".to_string()),
            secret: Secret::new(secret_value),
        };
        let rendered = format!("{config:?}");

        assert!(!rendered.contains(secret_value));
        for window in secret_value.as_bytes().windows(4) {
            let fragment = std::str::from_utf8(window).expect("secret is ascii");
            assert!(
                !rendered.contains(fragment),
                "rendered debug leaked fragment {fragment:?}: {rendered}"
            );
        }
        assert!(rendered.contains("rotation-2026"));
    }

    #[test]
    fn a_url_path_cannot_contain_a_newline() {
        // The delimiter-injection assumption `string_to_sign`'s doc relies
        // on, asserted against the `url` crate rather than merely commented.
        // The signed target is `path` plus `?query`, so both halves are
        // checked — one clean and one with an embedded newline would leave
        // the other half untested.
        let parsed = url::Url::parse("https://h/a\nb?c\nd=e")
            .expect("url::Url strips control characters rather than rejecting them");
        assert!(!parsed.path().contains('\n'));
        assert!(!parsed.query().unwrap_or_default().contains('\n'));
    }
}
