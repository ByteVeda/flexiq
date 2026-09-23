//! HMAC-SHA256 of the raw body, carried in one header.
//!
//! The generic shape most webhook senders use, and GitHub's exactly: GitHub is
//! this verifier with its header, prefix and encoding filled in, so it is a
//! constructor rather than a second implementation.

use base64::Engine;

use super::{any_tag_matches, sha256_mac, Inbound, Key, Rejection};

/// How the tag is written into the header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    /// Lowercase or uppercase hexadecimal.
    Hex,
    /// Standard base64, padded.
    Base64,
}

impl Encoding {
    /// Decode a tag as written, or `None` when it is not this encoding.
    pub fn decode(self, text: &str) -> Option<Vec<u8>> {
        match self {
            Self::Hex => hex::decode(text).ok(),
            Self::Base64 => base64::engine::general_purpose::STANDARD.decode(text).ok(),
        }
    }
}

/// HMAC-SHA256 of the body in one header.
#[derive(Debug, Clone)]
pub struct HeaderHmac {
    kind: &'static str,
    header: String,
    prefix: String,
    encoding: Encoding,
    key: Key,
}

impl HeaderHmac {
    /// The generic form: any header, any prefix, either encoding.
    pub fn new(header: String, prefix: String, encoding: Encoding, key: Key) -> Self {
        Self {
            kind: "hmac_sha256",
            header,
            prefix,
            encoding,
            key,
        }
    }

    /// GitHub's `X-Hub-Signature-256: sha256=<hex>`.
    pub fn github(key: Key) -> Self {
        Self {
            kind: "github",
            header: "x-hub-signature-256".into(),
            prefix: "sha256=".into(),
            encoding: Encoding::Hex,
            key,
        }
    }

    pub(super) fn kind(&self) -> &'static str {
        self.kind
    }

    pub(super) fn verify(&self, inbound: &Inbound<'_>) -> Result<(), Rejection> {
        let tag = inbound
            .header(&self.header)
            .ok_or(Rejection("the signature header is missing"))?
            .trim()
            .strip_prefix(self.prefix.as_str())
            .ok_or(Rejection("the signature header lacks its prefix"))?;
        let tag = self
            .encoding
            .decode(tag)
            .ok_or(Rejection("the signature is not validly encoded"))?;
        let mac = sha256_mac(&self.key, &[inbound.body])?;
        if any_tag_matches(&mac, &[tag]) {
            Ok(())
        } else {
            Err(Rejection("the signature does not match the body"))
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue};
    use hmac::Mac;

    use super::*;

    fn inbound<'a>(headers: &'a HeaderMap, body: &'a [u8]) -> Inbound<'a> {
        Inbound {
            headers,
            query: "",
            body,
            now_secs: 0,
        }
    }

    /// The worked example in GitHub's "Validating webhook deliveries" guide.
    #[test]
    fn githubs_published_vector_verifies() {
        let verifier = HeaderHmac::github(Key::new(b"It's a Secret to Everybody".to_vec()));
        let mut headers = HeaderMap::new();
        headers.insert(
            "X-Hub-Signature-256",
            HeaderValue::from_static(
                "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17",
            ),
        );
        assert_eq!(
            verifier.verify(&inbound(&headers, b"Hello, World!")),
            Ok(())
        );
        assert!(verifier
            .verify(&inbound(&headers, b"Hello, World?"))
            .is_err());
    }

    #[test]
    fn a_base64_tag_without_a_prefix() {
        let key = Key::new(b"generic-signing-key".to_vec());
        let body = b"{\"ok\":true}";
        let tag = sha256_mac(&key, &[body])
            .expect("mac")
            .finalize()
            .into_bytes();
        let encoded = base64::engine::general_purpose::STANDARD.encode(tag);

        let verifier = HeaderHmac::new("x-signature".into(), String::new(), Encoding::Base64, key);
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-signature",
            HeaderValue::from_str(&encoded).expect("valid header"),
        );
        assert_eq!(verifier.verify(&inbound(&headers, body)), Ok(()));
    }

    #[test]
    fn garbage_and_absence_are_both_refusals() {
        let verifier = HeaderHmac::github(Key::new(b"k".to_vec()));
        let empty = HeaderMap::new();
        assert!(verifier.verify(&inbound(&empty, b"x")).is_err());

        let mut headers = HeaderMap::new();
        headers.insert("x-hub-signature-256", HeaderValue::from_static("sha256=zz"));
        assert!(verifier.verify(&inbound(&headers, b"x")).is_err());
    }
}
