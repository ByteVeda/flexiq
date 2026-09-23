//! Amazon SNS message signatures.
//!
//! SNS signs every message it delivers over HTTPS with RSA, over a canonical
//! string built from the message's own fields, and names the certificate that
//! verifies it in `SigningCertURL`. That URL is part of the request, so it is
//! only trusted once its host is exactly the SNS endpoint of the allowlisted
//! topic's region — `sns.<region>.amazonaws.com` — reached over HTTPS; the
//! certificate is then trusted because that host served it.
//!
//! Beyond the signature: the topic must be one this trigger names, the
//! message must be recent, and `SignatureVersion` 1 (SHA1withRSA) can be
//! refused in favour of 2 (SHA256withRSA) for a topic configured to use it.

use std::time::Duration;

use base64::Engine;
use rsa::pkcs1v15::{Signature, VerifyingKey};
use rsa::pkcs8::DecodePublicKey;
use rsa::signature::Verifier;
use rsa::RsaPublicKey;
use serde_json::Value;
use sha1::Sha1;
use sha2::Sha256;
use url::Url;
use x509_cert::der::{Decode, Encode};
use x509_cert::Certificate;

use super::fetch::KeyFetcher;
use super::{within_tolerance, Inbound, Rejection};

/// How long a message's `Timestamp` stays acceptable by default. SNS retries
/// an HTTP endpoint for about an hour under its default delivery policy.
pub const DEFAULT_SNS_TOLERANCE_SECS: i64 = 3600;

/// SNS rotates its signing certificate rarely and publishes the new one under
/// a new URL, so a day's cache costs nothing.
const CERT_TTL: Duration = Duration::from_secs(24 * 3600);

/// The fields a notification's signature covers, in signing order. `Subject`
/// is signed only when the message has one.
const NOTIFICATION_FIELDS: [&str; 6] = [
    "Message",
    "MessageId",
    "Subject",
    "Timestamp",
    "TopicArn",
    "Type",
];

/// The fields a subscription or unsubscription confirmation's signature covers.
const CONFIRMATION_FIELDS: [&str; 7] = [
    "Message",
    "MessageId",
    "SubscribeURL",
    "Timestamp",
    "Token",
    "TopicArn",
    "Type",
];

/// What an SNS-supplied URL is for, which decides what else it must look like.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AwsUrl {
    /// `SigningCertURL`: a `.pem` file.
    Certificate,
    /// `SubscribeURL`: the `ConfirmSubscription` action.
    Subscribe,
}

/// SNS signature verification.
#[derive(Debug, Clone)]
pub struct Sns {
    topic_arns: Vec<String>,
    require_v2: bool,
    tolerance_secs: i64,
}

impl Sns {
    /// Accept messages from `topic_arns` only.
    pub fn new(topic_arns: Vec<String>, require_v2: bool, tolerance_secs: i64) -> Self {
        Self {
            topic_arns,
            require_v2,
            tolerance_secs,
        }
    }

    pub(super) async fn verify(
        &self,
        inbound: &Inbound<'_>,
        keys: &KeyFetcher,
    ) -> Result<(), Rejection> {
        let message: Value = serde_json::from_slice(inbound.body)
            .map_err(|_| Rejection("the body is not an SNS message"))?;
        let field = |name: &str| message.get(name).and_then(Value::as_str);

        let topic = field("TopicArn").ok_or(Rejection("the message names no topic"))?;
        if !self.topic_arns.iter().any(|allowed| allowed == topic) {
            return Err(Rejection(
                "the message is from a topic this trigger does not accept",
            ));
        }

        let signed: &[&str] = match field("Type") {
            Some("Notification") => &NOTIFICATION_FIELDS,
            Some("SubscriptionConfirmation" | "UnsubscribeConfirmation") => &CONFIRMATION_FIELDS,
            _ => return Err(Rejection("the message has no type SNS sends")),
        };
        let mut canonical = String::new();
        for name in signed {
            match field(name) {
                Some(value) => {
                    canonical.push_str(name);
                    canonical.push('\n');
                    canonical.push_str(value);
                    canonical.push('\n');
                }
                None if *name == "Subject" => {}
                None => return Err(Rejection("the message lacks a signed field")),
            }
        }

        let timestamp = field("Timestamp")
            .and_then(|text| chrono::DateTime::parse_from_rfc3339(text).ok())
            .ok_or(Rejection("the message timestamp is unreadable"))?;
        if !within_tolerance(timestamp.timestamp(), inbound.now_secs, self.tolerance_secs) {
            return Err(Rejection("the message timestamp is outside the tolerance"));
        }

        let version = field("SignatureVersion");
        if version == Some("1") && self.require_v2 {
            return Err(Rejection(
                "the message uses signature version 1, which is refused",
            ));
        }
        let signature = field("Signature")
            .and_then(|text| base64::engine::general_purpose::STANDARD.decode(text).ok())
            .and_then(|bytes| Signature::try_from(bytes.as_slice()).ok())
            .ok_or(Rejection("the message signature is unreadable"))?;

        let cert_url = aws_url(
            field("SigningCertURL").ok_or(Rejection("the message names no certificate"))?,
            AwsUrl::Certificate,
            topic,
        )?;
        let key = certificate_key(keys, cert_url.as_str(), inbound.now_secs).await?;

        let verified = match version {
            Some("1") => VerifyingKey::<Sha1>::new(key).verify(canonical.as_bytes(), &signature),
            Some("2") => VerifyingKey::<Sha256>::new(key).verify(canonical.as_bytes(), &signature),
            _ => return Err(Rejection("the message has no signature version SNS uses")),
        };
        verified.map_err(|_| Rejection("the SNS signature does not match the message"))
    }
}

/// The SNS endpoint host of the region and partition `topic_arn` names.
///
/// `arn:<partition>:sns:<region>:<account>:<name>`. The topic is one this
/// trigger allowlists, so the host derived from it is one the operator chose.
pub fn endpoint_host(topic_arn: &str) -> Option<String> {
    let mut parts = topic_arn.split(':');
    let (Some("arn"), Some(partition), Some("sns"), Some(region)) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return None;
    };
    let domain = match partition {
        "aws" | "aws-us-gov" => "amazonaws.com",
        "aws-cn" => "amazonaws.com.cn",
        _ => return None,
    };
    is_region(region).then(|| format!("sns.{region}.{domain}"))
}

/// Whether `region` has the shape every AWS region has: a two-letter area,
/// one or more words, and a number — `us-east-1`, `us-gov-west-1`,
/// `cn-north-1`. What it rules out is a name like `s3` or `s3-us-west-2`,
/// which would turn `sns.<region>.amazonaws.com` into an S3 bucket host.
fn is_region(region: &str) -> bool {
    let parts: Vec<&str> = region.split('-').collect();
    let letters = |part: &&str| !part.is_empty() && part.chars().all(|c| c.is_ascii_lowercase());
    match parts.as_slice() {
        [area, words @ .., number] => {
            area.len() == 2
                && letters(area)
                && !words.is_empty()
                && words.iter().all(letters)
                && !number.is_empty()
                && number.chars().all(|c| c.is_ascii_digit())
        }
        _ => false,
    }
}

/// `raw` as a URL SNS could have issued for `topic_arn`, or a refusal.
///
/// HTTPS on the default port, no credentials, and **exactly** the SNS
/// endpoint of the topic's own region. A shape match is not enough: a host
/// like `sns.s3.amazonaws.com` fits `sns.<anything>.amazonaws.com` and is an
/// S3 bucket someone else can own. This is the whole of the trust in the
/// certificate — and what keeps a forged message from making this process
/// fetch an arbitrary URL.
pub fn aws_url(raw: &str, kind: AwsUrl, topic_arn: &str) -> Result<Url, Rejection> {
    let url = Url::parse(raw).map_err(|_| Rejection("an SNS URL is not a URL"))?;
    let expected =
        endpoint_host(topic_arn).ok_or(Rejection("the topic ARN names no SNS region"))?;
    if url.scheme() != "https"
        || url.host_str() != Some(expected.as_str())
        || url.port().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(Rejection("an SNS URL does not point at an SNS endpoint"));
    }
    let shaped = match kind {
        AwsUrl::Certificate => url.path().ends_with(".pem"),
        AwsUrl::Subscribe => url
            .query_pairs()
            .any(|(key, value)| key == "Action" && value == "ConfirmSubscription"),
    };
    if !shaped {
        return Err(Rejection("an SNS URL is not the kind its field names"));
    }
    Ok(url)
}

/// The RSA key in the certificate at `url`, fetched once a day at most.
async fn certificate_key(
    keys: &KeyFetcher,
    url: &str,
    now_secs: i64,
) -> Result<RsaPublicKey, Rejection> {
    let pem = keys
        .cached(url, CERT_TTL, false, CERT_TTL)
        .await
        .map_err(|error| {
            log::warn!("[flexiq] the SNS signing certificate could not be fetched: {error}");
            Rejection("the SNS signing certificate could not be fetched")
        })?;
    public_key(&pem, now_secs)
}

/// The RSA public key of a PEM certificate valid at `now_secs`.
pub fn public_key(pem: &[u8], now_secs: i64) -> Result<RsaPublicKey, Rejection> {
    let der = pem_body(pem).ok_or(Rejection("the SNS certificate is not PEM"))?;
    let certificate =
        Certificate::from_der(&der).map_err(|_| Rejection("the SNS certificate is malformed"))?;

    let validity = &certificate.tbs_certificate.validity;
    let now = u64::try_from(now_secs).unwrap_or_default();
    if now < validity.not_before.to_unix_duration().as_secs()
        || now > validity.not_after.to_unix_duration().as_secs()
    {
        return Err(Rejection("the SNS certificate is not valid now"));
    }

    let spki = certificate
        .tbs_certificate
        .subject_public_key_info
        .to_der()
        .map_err(|_| Rejection("the SNS certificate's key is malformed"))?;
    RsaPublicKey::from_public_key_der(&spki)
        .map_err(|_| Rejection("the SNS certificate's key is not RSA"))
}

/// The DER inside the first `CERTIFICATE` block.
fn pem_body(pem: &[u8]) -> Option<Vec<u8>> {
    let text = std::str::from_utf8(pem).ok()?;
    let start = text.find("-----BEGIN CERTIFICATE-----")? + "-----BEGIN CERTIFICATE-----".len();
    let end = start + text[start..].find("-----END CERTIFICATE-----")?;
    let body: String = text[start..end]
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    base64::engine::general_purpose::STANDARD.decode(body).ok()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::http::HeaderMap;
    use rsa::pkcs1v15::SigningKey;
    use rsa::pkcs8::DecodePrivateKey;
    use rsa::signature::{SignatureEncoding, Signer};
    use rsa::RsaPrivateKey;
    use serde_json::json;

    use super::*;
    use crate::trigger::auth::fetch::Fetch;

    const CERT: &[u8] = include_bytes!("../../../tests/fixtures/sns_test_cert.pem");
    const KEY: &[u8] = include_bytes!("../../../tests/fixtures/sns_test_key.der");
    const TOPIC: &str = "arn:aws:sns:us-east-1:123456789012:uploads";
    const CERT_URL: &str = "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-test.pem";
    const NOW: i64 = 1_800_000_000;

    fn keys() -> KeyFetcher {
        let fetch: Fetch = Arc::new(|_url: String| Box::pin(async { Ok(CERT.to_vec()) }));
        KeyFetcher::new(fetch)
    }

    /// A notification signed the way SNS signs one.
    fn signed(version: &str, mut message: Value) -> Value {
        message["SignatureVersion"] = json!(version);
        message["SigningCertURL"] = json!(CERT_URL);
        let fields: &[&str] = if message["Type"] == "Notification" {
            &NOTIFICATION_FIELDS
        } else {
            &CONFIRMATION_FIELDS
        };
        let canonical: String = fields
            .iter()
            .filter_map(|name| {
                message
                    .get(*name)
                    .and_then(Value::as_str)
                    .map(|value| format!("{name}\n{value}\n"))
            })
            .collect();
        let key = RsaPrivateKey::from_pkcs8_der(KEY).expect("a test key");
        let signature = match version {
            "1" => SigningKey::<Sha1>::new(key)
                .sign(canonical.as_bytes())
                .to_vec(),
            _ => SigningKey::<Sha256>::new(key)
                .sign(canonical.as_bytes())
                .to_vec(),
        };
        message["Signature"] = json!(base64::engine::general_purpose::STANDARD.encode(signature));
        message
    }

    fn notification() -> Value {
        json!({
            "Type": "Notification",
            "MessageId": "22b80b92-fdea-4c2c-8f9d-bdfb0c7bf324",
            "TopicArn": TOPIC,
            "Subject": "Amazon S3 Notification",
            "Message": "{\"Records\":[]}",
            "Timestamp": "2027-01-15T08:00:00.000Z",
        })
    }

    async fn check(sns: &Sns, message: &Value) -> Result<(), Rejection> {
        let body = message.to_string();
        let headers = HeaderMap::new();
        sns.verify(
            &Inbound {
                headers: &headers,
                query: "",
                body: body.as_bytes(),
                now_secs: NOW,
            },
            &keys(),
        )
        .await
    }

    fn sns() -> Sns {
        Sns::new(vec![TOPIC.into()], false, DEFAULT_SNS_TOLERANCE_SECS)
    }

    #[tokio::test]
    async fn both_signature_versions_verify() {
        assert_eq!(check(&sns(), &signed("1", notification())).await, Ok(()));
        assert_eq!(check(&sns(), &signed("2", notification())).await, Ok(()));
    }

    #[tokio::test]
    async fn a_message_without_a_subject_still_verifies() {
        let mut message = notification();
        message
            .as_object_mut()
            .expect("an object")
            .remove("Subject");
        assert_eq!(check(&sns(), &signed("2", message)).await, Ok(()));
    }

    #[tokio::test]
    async fn a_changed_field_breaks_the_signature() {
        let mut message = signed("2", notification());
        message["Message"] = json!("{\"Records\":[{\"forged\":true}]}");
        assert!(check(&sns(), &message).await.is_err());
    }

    #[tokio::test]
    async fn another_topic_is_refused_before_anything_is_fetched() {
        let mut message = notification();
        message["TopicArn"] = json!("arn:aws:sns:us-east-1:999999999999:other");
        let message = signed("2", message);
        let fetch: Fetch =
            Arc::new(|_url: String| Box::pin(async { panic!("must not fetch a certificate") }));
        let body = message.to_string();
        let headers = HeaderMap::new();
        let refused = sns()
            .verify(
                &Inbound {
                    headers: &headers,
                    query: "",
                    body: body.as_bytes(),
                    now_secs: NOW,
                },
                &KeyFetcher::new(fetch),
            )
            .await;
        assert!(refused.is_err());
    }

    #[tokio::test]
    async fn version_one_can_be_refused_and_a_stale_message_is() {
        let strict = Sns::new(vec![TOPIC.into()], true, DEFAULT_SNS_TOLERANCE_SECS);
        assert!(check(&strict, &signed("1", notification())).await.is_err());
        assert_eq!(check(&strict, &signed("2", notification())).await, Ok(()));

        let mut stale = notification();
        stale["Timestamp"] = json!("2020-01-01T00:00:00.000Z");
        assert!(check(&sns(), &signed("2", stale)).await.is_err());
    }

    #[tokio::test]
    async fn a_certificate_url_off_an_sns_host_is_refused() {
        let mut message = signed("2", notification());
        message["SigningCertURL"] = json!("https://evil.example.com/cert.pem");
        assert!(check(&sns(), &message).await.is_err());
    }

    #[test]
    fn only_the_topics_own_sns_endpoint_passes_the_url_check() {
        let good = [
            (CERT_URL, AwsUrl::Certificate, TOPIC),
            (
                "https://sns.cn-north-1.amazonaws.com.cn/SimpleNotificationService-x.pem",
                AwsUrl::Certificate,
                "arn:aws-cn:sns:cn-north-1:123456789012:uploads",
            ),
            (
                "https://sns.eu-west-1.amazonaws.com/?Action=ConfirmSubscription&TopicArn=a&Token=t",
                AwsUrl::Subscribe,
                "arn:aws:sns:eu-west-1:123456789012:uploads",
            ),
        ];
        for (url, kind, topic) in good {
            assert!(aws_url(url, kind, topic).is_ok(), "{url}");
        }

        // Both are S3 virtual-hosted buckets that fit `sns.<x>.amazonaws.com`.
        for s3_bucket in [
            "https://sns.s3.amazonaws.com/cert.pem",
            "https://sns.s3-us-west-2.amazonaws.com/cert.pem",
        ] {
            assert!(
                aws_url(s3_bucket, AwsUrl::Certificate, TOPIC).is_err(),
                "{s3_bucket}"
            );
        }
        // Not even a topic ARN claiming those "regions" makes them reachable.
        for region in ["s3", "s3-us-west-2"] {
            let topic = format!("arn:aws:sns:{region}:123456789012:uploads");
            let url = format!("https://sns.{region}.amazonaws.com/cert.pem");
            assert!(
                aws_url(&url, AwsUrl::Certificate, &topic).is_err(),
                "{region}"
            );
        }
        // A real SNS endpoint, but another region than the topic's.
        assert!(aws_url(
            "https://sns.eu-west-1.amazonaws.com/SimpleNotificationService-x.pem",
            AwsUrl::Certificate,
            TOPIC
        )
        .is_err());
        // A topic ARN that names no partition or region yields no endpoint.
        for topic in [
            "arn:aws:sqs:us-east-1:1:q",
            "arn:evil:sns:us-east-1:1:t",
            "nonsense",
        ] {
            assert!(
                aws_url(CERT_URL, AwsUrl::Certificate, topic).is_err(),
                "{topic}"
            );
        }

        let bad = [
            (
                "http://sns.us-east-1.amazonaws.com/x.pem",
                AwsUrl::Certificate,
            ),
            (
                "https://sns.us-east-1.amazonaws.com.evil.com/x.pem",
                AwsUrl::Certificate,
            ),
            (
                "https://evil.com/sns.us-east-1.amazonaws.com/x.pem",
                AwsUrl::Certificate,
            ),
            ("https://sns..amazonaws.com/x.pem", AwsUrl::Certificate),
            (
                "https://sns.us-east-1.amazonaws.com:8443/x.pem",
                AwsUrl::Certificate,
            ),
            (
                "https://user@sns.us-east-1.amazonaws.com/x.pem",
                AwsUrl::Certificate,
            ),
            (
                "https://sns.us-east-1.amazonaws.com/x.txt",
                AwsUrl::Certificate,
            ),
            (
                "https://sns.us-east-1.amazonaws.com/?Action=Publish",
                AwsUrl::Subscribe,
            ),
        ];
        for (url, kind) in bad {
            assert!(aws_url(url, kind, TOPIC).is_err(), "{url}");
        }
    }

    #[test]
    fn an_expired_certificate_is_refused() {
        assert!(public_key(CERT, NOW).is_ok());
        // Long after the fixture's hundred-year validity ends.
        assert!(public_key(CERT, 5_000_000_000).is_err());
        assert!(public_key(b"not a certificate", NOW).is_err());
    }
}
