//! The shared secret an executor presents when it attaches.
//!
//! An attach connection receives job frames, so the handshake has to prove who
//! is on the other end. The token is a bearer credential: it proves the peer
//! knows it, it does not encrypt the connection. Over an untrusted network,
//! terminate mTLS in front of the listener and treat the token as the second
//! factor.

use std::fmt;

use serde::{Deserialize, Serialize};

/// A shared secret, redacted everywhere it could be printed.
///
/// `Debug` and `Display` render a placeholder, so a struct holding one stays
/// safe to log. Comparison goes through [`Secret::matches`], which does not
/// short-circuit on the first differing byte.
#[derive(Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    /// Wrap a secret value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Whether `presented` is this secret, compared without leaking the common
    /// prefix through timing.
    pub fn matches(&self, presented: &Secret) -> bool {
        constant_time_eq(self.0.as_bytes(), presented.0.as_bytes())
    }

    /// Number of characters. Length is not sensitive, and config validation
    /// needs it to reject a token too short to be one.
    pub fn len(&self) -> usize {
        self.0.chars().count()
    }

    /// Whether the secret is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

impl fmt::Display for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// Compare two byte strings without short-circuiting on the first difference.
/// Length is allowed to short-circuit — it is already observable.
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |difference, (a, b)| difference | (a ^ b))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matching_secrets_compare_equal() {
        let expected = Secret::new("s3cret-value");
        assert!(expected.matches(&Secret::new("s3cret-value")));
    }

    #[test]
    fn a_differing_byte_or_length_does_not_match() {
        let expected = Secret::new("s3cret-value");
        assert!(!expected.matches(&Secret::new("s3cret-valuE")));
        assert!(!expected.matches(&Secret::new("s3cret-value-longer")));
        assert!(!expected.matches(&Secret::new("")));
    }

    #[test]
    fn the_value_never_reaches_a_formatter() {
        let secret = Secret::new("s3cret-value");
        assert_eq!(format!("{secret:?}"), "Secret(<redacted>)");
        assert_eq!(format!("{secret}"), "<redacted>");
        // The struct that carries it into logs must be safe too.
        #[derive(Debug)]
        struct Holder {
            #[allow(dead_code)]
            token: Secret,
        }
        assert!(!format!("{:?}", Holder { token: secret }).contains("s3cret"));
    }

    #[test]
    fn a_secret_round_trips_as_a_plain_json_string() {
        let json = serde_json::to_string(&Secret::new("s3cret-value")).expect("serialize");
        assert_eq!(json, r#""s3cret-value""#);
        let parsed: Secret = serde_json::from_str(&json).expect("deserialize");
        assert!(parsed.matches(&Secret::new("s3cret-value")));
    }

    #[test]
    fn length_counts_characters() {
        assert_eq!(Secret::new("abcd").len(), 4);
        assert!(Secret::new("").is_empty());
    }
}
