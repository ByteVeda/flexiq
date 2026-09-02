//! Reading the credential out of request metadata.
//!
//! One place, because every authenticator wants the same thing and the failure
//! modes are the interesting part: a missing header, a header that is not
//! UTF-8, and the wrong scheme must all be indistinguishable from a wrong
//! credential by the time a caller sees an answer.

use tonic::metadata::MetadataMap;

/// The metadata key the credential arrives under.
///
/// gRPC's own name for a bearer credential, so a generic client's
/// call-credentials plumbing sets it without being told, and so a proxy in
/// front of the door strips or rewrites the header it already knows about.
pub const AUTHORIZATION: &str = "authorization";

/// The scheme, matched case-insensitively as RFC 7235 requires.
const BEARER: &str = "bearer ";

/// The credential in `metadata`, if there is a well-formed one.
pub fn presented(metadata: &MetadataMap) -> Option<&str> {
    let raw = metadata.get(AUTHORIZATION)?.to_str().ok()?;
    // Slicing on a fixed ASCII prefix is safe once the prefix has matched, and
    // matching it case-insensitively costs nothing on a string this short.
    if raw.len() < BEARER.len() || !raw[..BEARER.len()].eq_ignore_ascii_case(BEARER) {
        return None;
    }
    Some(&raw[BEARER.len()..])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with(value: &str) -> MetadataMap {
        let mut metadata = MetadataMap::new();
        metadata.insert(AUTHORIZATION, value.parse().expect("an ASCII header value"));
        metadata
    }

    #[test]
    fn the_scheme_is_case_insensitive() {
        for scheme in ["Bearer", "bearer", "BEARER", "BeArEr"] {
            assert_eq!(presented(&with(&format!("{scheme} tok"))), Some("tok"));
        }
    }

    #[test]
    fn nothing_else_is_a_credential() {
        assert_eq!(presented(&MetadataMap::new()), None);
        for value in ["Basic tok", "tok", "Bearer", "bearertok", ""] {
            assert_eq!(presented(&with(value)), None, "value: {value:?}");
        }
    }

    #[test]
    fn the_scheme_and_nothing_after_it_is_an_empty_credential() {
        // Not `None`: it is a well-formed header carrying an empty credential,
        // and an empty credential matches nothing. The distinction never
        // reaches a caller — both end as one refusal.
        assert_eq!(presented(&with("Bearer ")), Some(""));
    }
}
