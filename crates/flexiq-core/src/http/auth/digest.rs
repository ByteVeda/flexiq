//! Digest primitives shared by every outbound signing scheme.
//!
//! `sha256_hex` and `hmac_sha256_hex` were HMAC's alone (`hmac.rs`) until
//! SigV4 needed both, plus a third — `hmac_sha256` — for its four-step
//! signing-key derivation. One module owns all three so a digest is never
//! computed two different ways by two schemes that both claim to trust it.

use hmac::digest::core_api::BlockSizeUser;
use hmac::digest::generic_array::GenericArray;
use hmac::digest::typenum::Unsigned;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

/// `Sha256`'s own block size (64 bytes), read from the type rather than
/// hard-coded: a hash swap that changed it would otherwise hit
/// `GenericArray::from_slice`'s length-mismatch panic in [`hmac_sha256`] at
/// runtime instead of a size mismatch caught here at compile time.
const BLOCK_SIZE: usize = <<Sha256 as BlockSizeUser>::BlockSize as Unsigned>::USIZE;

/// `sha256(body)`, lowercase hex.
///
/// A digest, not the body inline: it keeps the string to sign fixed-size and
/// printable, so a mismatch is diagnosable — logged, pasted into an issue —
/// without dumping a job payload anywhere.
pub(crate) fn sha256_hex(body: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(body);
    hex_lower(&hasher.finalize())
}

/// Lowercase hex, the same `format!("{byte:02x}")` fold
/// `flexiq-server`'s `webhook_sender.rs` uses for its own HMAC digest.
pub(crate) fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// HMAC-SHA256 returning raw bytes.
///
/// SigV4's signing key is four chained HMACs where each step's raw output is
/// the next step's key. Hex-encoding an intermediate is the classic way to get
/// a signature that is wrong in a way nothing explains.
///
/// Keys the block-sized buffer directly, via the infallible [`Mac::new`],
/// rather than [`Hmac::new_from_slice`]. That constructor performs the exact
/// same RFC 2104 §2 key normalisation (hash a key longer than the block size
/// down first, zero-pad a shorter one) — confirmed unreachable-`Err` by
/// reading `HmacCore::new_from_slice`'s body upstream, which has no path that
/// returns anything but `Ok` — but its signature is still `Result`, for MACs
/// in general that *do* require a fixed key length. This crate does not
/// carry a second `expect` resting on that unreachable arm (there is exactly
/// one, elsewhere, under a recorded ruling). [`block_sized_key`] does only
/// the normalisation — the actual HMAC construction (the inner double hash)
/// is still entirely the `hmac` crate's, via the exact-size [`Mac::new`],
/// which takes no `Result` at all because a matching key length cannot be
/// wrong.
pub(crate) fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let block_key = block_sized_key(key);
    let mut mac = Hmac::<Sha256>::new(GenericArray::from_slice(&block_key));
    mac.update(message);
    mac.finalize().into_bytes().into()
}

/// `hex(HMAC-SHA256(secret, message))`, lowercase — used by both the HMAC
/// signer and its reference verifier, so the two can never compute the
/// digest two different ways.
///
/// A thin wrapper over [`hmac_sha256`] now, rather than a second HMAC call
/// site. The `Result` return stays for its existing callers — both already
/// route it through `?`/`map_err` rather than unwrapping — but it can no
/// longer actually fail: [`hmac_sha256`] is infallible.
pub(crate) fn hmac_sha256_hex(secret: &[u8], message: &str) -> Result<String, ()> {
    Ok(hex_lower(&hmac_sha256(secret, message.as_bytes())))
}

/// HMAC's inner double hash keys on a block-sized buffer — [`BLOCK_SIZE`]
/// bytes — per RFC 2104 §2: a key longer than the block is hashed down
/// first, a shorter one is zero-padded. Both arms are infallible:
/// `Sha256::digest` cannot fail, and both `copy_from_slice` calls copy a
/// source no longer than the `BLOCK_SIZE`-byte destination by construction.
fn block_sized_key(key: &[u8]) -> [u8; BLOCK_SIZE] {
    let mut block = [0u8; BLOCK_SIZE];
    if key.len() > BLOCK_SIZE {
        let hashed = Sha256::digest(key);
        block[..hashed.len()].copy_from_slice(&hashed);
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    block
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_hex_of_empty_matches_the_well_known_constant() {
        // The single most-quoted SHA-256 test vector there is; verified
        // independently rather than trusted from memory:
        //
        //   python3 -c "import hashlib; print(hashlib.sha256(b'').hexdigest())"
        //
        // printed: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn hmac_sha256_matches_a_short_key_vector_from_rfc_4231() {
        // RFC 4231 §4.2 "Test Case 1" — a 20-byte key, well under the
        // 64-byte SHA-256 block size, so this exercises `block_sized_key`'s
        // zero-pad arm.
        let key = [0x0bu8; 20];
        let data = b"Hi There";
        let expected = "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7";
        assert_eq!(hex_lower(&hmac_sha256(&key, data)), expected);
    }

    #[test]
    fn hmac_sha256_matches_a_key_longer_than_the_block_size() {
        // RFC 4231 §4.7 "Test Case 6": a 131-byte key, longer than SHA-256's
        // 64-byte block, so this exercises `block_sized_key`'s hash-down arm
        // — the branch none of the SigV4 key-derivation vectors reach, since
        // every chained step's key there is at most a 44-byte secret or a
        // 32-byte prior HMAC output.
        let key = [0xaau8; 131];
        let data = b"Test Using Larger Than Block-Size Key - Hash Key First";
        let expected = "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54";
        assert_eq!(hex_lower(&hmac_sha256(&key, data)), expected);
    }

    #[test]
    fn hmac_sha256_hex_is_a_thin_wrapper_over_the_raw_function() {
        let key = b"pinned-test-secret-do-not-rotate";
        let message = "message to authenticate";
        assert_eq!(
            hmac_sha256_hex(key, message).expect("infallible in practice"),
            hex_lower(&hmac_sha256(key, message.as_bytes()))
        );
    }
}
