//! Password hashing, in the encoding every SDK dashboard reads and writes.
//!
//! `pbkdf2_sha256$<iterations>$<salt_hex>$<hash_hex>` — self-describing, so a
//! hash written with different parameters still verifies, and a user created on
//! one SDK's dashboard can log in on another.

use sha2::Sha256;

use crate::dashboard::auth::model::OAUTH_PASSWORD_MARKER;
use crate::dashboard::security::constant_time_eq;

/// OWASP's PBKDF2-HMAC-SHA256 baseline.
const ITERATIONS: u32 = 600_000;
const SALT_BYTES: usize = 16;
const HASH_BYTES: usize = 32;

const SCHEME: &str = "pbkdf2_sha256";

/// Hash `password` with a fresh salt.
///
/// Deliberately expensive: call it from a blocking context, never from an
/// async handler, or a login stalls the runtime for hundreds of milliseconds.
pub fn hash(password: &str) -> String {
    let salt: [u8; SALT_BYTES] = rand::random();
    let digest = derive(password, &salt, ITERATIONS, HASH_BYTES);
    format!(
        "{SCHEME}${ITERATIONS}${}${}",
        to_hex(&salt),
        to_hex(&digest)
    )
}

/// Verify `password` against an encoded hash, in constant time.
pub fn verify(password: &str, encoded: &str) -> bool {
    // A provider-backed user has no password; a password attempt against one
    // must never succeed, whatever was submitted.
    if encoded.starts_with(OAUTH_PASSWORD_MARKER) {
        return false;
    }
    let mut parts = encoded.split('$');
    let (Some(scheme), Some(iterations), Some(salt), Some(expected), None) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return false;
    };
    if scheme != SCHEME {
        return false;
    }
    let (Ok(iterations), Some(salt), Some(expected)) = (
        iterations.parse::<u32>(),
        from_hex(salt),
        from_hex(expected),
    ) else {
        return false;
    };
    if iterations == 0 || expected.is_empty() {
        return false;
    }

    let candidate = derive(password, &salt, iterations, expected.len());
    constant_time_eq(&to_hex(&candidate), &to_hex(&expected))
}

/// A fixed hash used to keep verification timing constant for unknown users.
/// Never matches any password — the digest is all zeroes.
pub fn dummy_hash() -> String {
    format!(
        "{SCHEME}${ITERATIONS}${}${}",
        "0".repeat(SALT_BYTES * 2),
        "0".repeat(HASH_BYTES * 2)
    )
}

fn derive(password: &str, salt: &[u8], iterations: u32, length: usize) -> Vec<u8> {
    let mut output = vec![0u8; length];
    pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut output);
    output
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn from_hex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&text[index..index + 2], 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Produced by CPython's `hashlib.pbkdf2_hmac("sha256", b"correct horse",
    /// bytes(range(1, 17)), 1000, 32)` — the exact primitive the SDK
    /// dashboards hash with. Verifying it here is the cross-SDK interop check:
    /// encoding, salt handling, and KDF must all agree. A low iteration count
    /// keeps the test fast; `verify` reads the count from the hash itself.
    const PYTHON_HASH: &str = "pbkdf2_sha256$1000$0102030405060708090a0b0c0d0e0f10$\
63029f34cae71cb733eb39646121c236d84c994566464a6566dc4274066057c3";

    #[test]
    fn a_hash_verifies_against_its_own_password() {
        // 600k iterations twice is slow but this is the property that matters.
        let encoded = hash("correct horse battery staple");
        assert!(verify("correct horse battery staple", &encoded));
        assert!(!verify("Correct horse battery staple", &encoded));
    }

    #[test]
    fn the_encoding_is_the_cross_sdk_format() {
        let encoded = hash("whatever");
        let parts: Vec<&str> = encoded.split('$').collect();
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0], SCHEME);
        assert_eq!(parts[1], ITERATIONS.to_string());
        assert_eq!(parts[2].len(), SALT_BYTES * 2);
        assert_eq!(parts[3].len(), HASH_BYTES * 2);
    }

    #[test]
    fn a_hash_written_by_another_sdk_verifies_here() {
        assert!(verify("correct horse", PYTHON_HASH));
        assert!(!verify("wrong horse", PYTHON_HASH));
    }

    #[test]
    fn malformed_hashes_never_verify() {
        for encoded in [
            "",
            "plaintext",
            "pbkdf2_sha256$notanumber$aabb$ccdd",
            "argon2$600000$aabb$ccdd",
            "pbkdf2_sha256$600000$zz$ccdd",
            "pbkdf2_sha256$600000$aabb$ccdd$extra",
            "pbkdf2_sha256$0$aabb$ccdd",
        ] {
            assert!(!verify("anything", encoded), "must reject '{encoded}'");
        }
    }

    #[test]
    fn an_oauth_user_rejects_every_password() {
        assert!(!verify("", "oauth:google"));
        assert!(!verify("guess", "oauth:google"));
    }

    #[test]
    fn the_dummy_hash_is_well_formed_and_never_matches() {
        let dummy = dummy_hash();
        assert!(!verify("", &dummy));
        assert!(!verify("password", &dummy));
    }
}
