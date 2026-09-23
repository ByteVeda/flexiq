//! A secret the sender presents verbatim.
//!
//! The weakest kind, and the one every object-store eventing platform offers:
//! an EventBridge connection sends it as a header, a Pub/Sub push subscription
//! and an Event Grid endpoint carry it in the URL's query. It proves only that
//! the caller knows the value, so it is only as private as the channel — which
//! is why the listener is meant to sit behind TLS.

use super::{Inbound, Rejection};
use crate::dashboard::security::constant_time_eq;

/// Where the sender puts the secret.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecretLocation {
    /// A request header, after an optional prefix such as `Bearer `.
    Header {
        /// Header name, compared case-insensitively.
        name: String,
        /// Text the value must start with, stripped before comparing.
        prefix: String,
    },
    /// A query parameter.
    Query {
        /// Parameter name.
        name: String,
    },
}

/// A verbatim shared secret.
#[derive(Clone)]
pub struct SharedSecret {
    location: SecretLocation,
    secret: String,
}

impl std::fmt::Debug for SharedSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedSecret")
            .field("location", &self.location)
            .field("secret", &"<redacted>")
            .finish()
    }
}

impl SharedSecret {
    /// A verifier expecting `secret` at `location`.
    pub fn new(location: SecretLocation, secret: String) -> Self {
        Self { location, secret }
    }

    pub(super) fn verify(&self, inbound: &Inbound<'_>) -> Result<(), Rejection> {
        let presented = match &self.location {
            SecretLocation::Header { name, prefix } => inbound
                .header(name)
                .ok_or(Rejection("the secret header is missing"))?
                .strip_prefix(prefix.as_str())
                .ok_or(Rejection("the secret header lacks its prefix"))?
                .to_string(),
            SecretLocation::Query { name } => inbound
                .query_param(name)
                .ok_or(Rejection("the secret query parameter is missing"))?,
        };
        if constant_time_eq(&presented, &self.secret) {
            Ok(())
        } else {
            Err(Rejection("the shared secret does not match"))
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue};

    use super::*;

    const SECRET: &str = "s3cret-s3cret-s3cret";

    fn inbound<'a>(headers: &'a HeaderMap, query: &'a str) -> Inbound<'a> {
        Inbound {
            headers,
            query,
            body: b"{}",
            now_secs: 0,
        }
    }

    #[test]
    fn a_header_secret_behind_a_prefix() {
        let verifier = SharedSecret::new(
            SecretLocation::Header {
                name: "authorization".into(),
                prefix: "Bearer ".into(),
            },
            SECRET.into(),
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {SECRET}")).expect("valid header"),
        );
        assert_eq!(verifier.verify(&inbound(&headers, "")), Ok(()));

        headers.insert("Authorization", HeaderValue::from_static(SECRET));
        assert!(verifier.verify(&inbound(&headers, "")).is_err());
    }

    #[test]
    fn a_query_secret() {
        let verifier = SharedSecret::new(
            SecretLocation::Query {
                name: "token".into(),
            },
            SECRET.into(),
        );
        let headers = HeaderMap::new();
        let good = format!("token={SECRET}");
        assert_eq!(verifier.verify(&inbound(&headers, &good)), Ok(()));
        assert!(verifier.verify(&inbound(&headers, "token=wrong")).is_err());
        assert!(verifier.verify(&inbound(&headers, "")).is_err());
    }

    #[test]
    fn debug_hides_the_secret() {
        let verifier = SharedSecret::new(
            SecretLocation::Query {
                name: "token".into(),
            },
            SECRET.into(),
        );
        assert!(!format!("{verifier:?}").contains(SECRET));
    }
}
