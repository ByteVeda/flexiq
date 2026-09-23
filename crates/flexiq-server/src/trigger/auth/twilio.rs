//! Twilio's `X-Twilio-Signature`.
//!
//! Base64 HMAC-SHA1, keyed by the account's auth token, over the URL Twilio
//! was configured to call. For a form body, every parameter follows the URL,
//! sorted by name, as `name` + `value` with no separator. For any other body,
//! Twilio signs only the URL and puts the body's SHA-256 in a `bodySHA256`
//! query parameter, which is then checked here too.
//!
//! The URL is the **public** one, as the sender saw it — behind an ingress the
//! host, scheme and path this process sees are not it — so a Twilio trigger
//! has to be told its own address.

use base64::Engine;
use hmac::{Hmac, Mac};
use sha1::Sha1;
use sha2::{Digest, Sha256};

use super::{any_tag_matches, Inbound, Key, Rejection};
use crate::dashboard::security::constant_time_eq;

const HEADER: &str = "x-twilio-signature";
const FORM: &str = "application/x-www-form-urlencoded";

/// Twilio webhook signature verification.
#[derive(Debug, Clone)]
pub struct Twilio {
    key: Key,
    public_url: String,
}

impl Twilio {
    /// A verifier for `auth_token`, reached by Twilio at `public_url` (no
    /// query string — the request's own is appended).
    pub fn new(auth_token: &str, public_url: String) -> Self {
        Self {
            key: Key::new(auth_token.as_bytes().to_vec()),
            public_url,
        }
    }

    pub(super) fn verify(&self, inbound: &Inbound<'_>) -> Result<(), Rejection> {
        let tag = inbound
            .header(HEADER)
            .ok_or(Rejection("the X-Twilio-Signature header is missing"))?;
        let tag = base64::engine::general_purpose::STANDARD
            .decode(tag.trim())
            .map_err(|_| Rejection("the X-Twilio-Signature is not base64"))?;

        let mut mac = <Hmac<Sha1> as Mac>::new_from_slice(self.key.as_bytes())
            .map_err(|_| Rejection("the signing key was refused by HMAC"))?;
        mac.update(self.public_url.as_bytes());
        if !inbound.query.is_empty() {
            mac.update(b"?");
            mac.update(inbound.query.as_bytes());
        }

        if is_form(inbound) {
            let mut params: Vec<(String, String)> = url::form_urlencoded::parse(inbound.body)
                .into_owned()
                .collect();
            params.sort();
            for (name, value) in &params {
                mac.update(name.as_bytes());
                mac.update(value.as_bytes());
            }
        } else {
            let declared = inbound
                .query_param("bodySHA256")
                .ok_or(Rejection("a non-form Twilio body needs its bodySHA256"))?;
            let actual = hex::encode(Sha256::digest(inbound.body));
            if !constant_time_eq(&declared.to_ascii_lowercase(), &actual) {
                return Err(Rejection("the bodySHA256 does not match the body"));
            }
        }

        if any_tag_matches(&mac, &[tag]) {
            Ok(())
        } else {
            Err(Rejection("the X-Twilio-Signature does not match"))
        }
    }
}

fn is_form(inbound: &Inbound<'_>) -> bool {
    inbound
        .header("content-type")
        .and_then(|value| value.split(';').next())
        .is_some_and(|mime| mime.trim().eq_ignore_ascii_case(FORM))
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue};

    use super::*;

    /// The worked example in Twilio's "Webhooks security" guide.
    #[test]
    fn twilios_published_vector_verifies() {
        let verifier = Twilio::new("12345", "https://mycompany.com/myapp.php".into());
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static(FORM));
        headers.insert(
            HEADER,
            HeaderValue::from_static("0/KCTR6DLpKmkAf8muzZqo1nDgQ="),
        );
        // Written out of order on purpose: the signature sorts them.
        let body = b"To=%2B18005551212&CallSid=CA1234567890ABCDE&Digits=1234\
                     &From=%2B12349013030&Caller=%2B12349013030";
        let inbound = Inbound {
            headers: &headers,
            query: "foo=1&bar=2",
            body,
            now_secs: 0,
        };
        assert_eq!(verifier.verify(&inbound), Ok(()));

        let tampered = Inbound {
            query: "foo=1&bar=3",
            ..inbound
        };
        assert!(verifier.verify(&tampered).is_err());
    }

    #[test]
    fn a_json_body_is_bound_through_its_digest() {
        let token = "twilio-auth-token";
        let url = "https://hooks.example.com/t/twilio";
        let body = b"{\"event\":\"x\"}";
        let digest = hex::encode(Sha256::digest(body));
        let query = format!("bodySHA256={digest}");

        let mut mac = <Hmac<Sha1> as Mac>::new_from_slice(token.as_bytes()).expect("key");
        mac.update(format!("{url}?{query}").as_bytes());
        let tag = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());

        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        headers.insert(HEADER, HeaderValue::from_str(&tag).expect("valid header"));
        let verifier = Twilio::new(token, url.into());

        let inbound = Inbound {
            headers: &headers,
            query: &query,
            body,
            now_secs: 0,
        };
        assert_eq!(verifier.verify(&inbound), Ok(()));
        assert!(verifier
            .verify(&Inbound {
                body: b"{\"event\":\"y\"}",
                ..inbound
            })
            .is_err());
    }
}
