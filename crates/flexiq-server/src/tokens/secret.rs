//! The token string: how one is minted, how one is read back, and what is
//! stored instead of it.
//!
//! A token is `fqt_<id>.<secret>`.
//!
//! - **`fqt_`** makes a leaked token greppable, and gives a secret scanner
//!   something to match on. Every credential this project hands out should be
//!   recognisable on sight.
//! - **`<id>`** is public. It is the settings key the row lives under, the
//!   handle a listing shows, the argument `revoke` takes, and the thing a log
//!   line may name — which is the audit trail the shared secret it replaces
//!   could not have. Because it is in the token, a lookup is a point read
//!   rather than a scan over every row's hash.
//! - **`.`** separates them because it is *not* in the base64url alphabet, so
//!   splitting is unambiguous. `_` would not be: base64url uses it, and a
//!   `rsplit_once('_')` would guess.
//! - **`<secret>`** is 256 bits from [`random_token`], the one generator every
//!   credential in this server draws from, so none of them can quietly end up
//!   weaker than the others.
//!
//! **What is stored is `sha256(secret)`, hex-encoded — not the token, and not a
//! password hash.** `dashboard::auth::password` runs 600k PBKDF2 iterations
//! because a password is low-entropy and an attacker who steals the rows will
//! guess it. There is nothing to guess here: the input is 256 uniformly random
//! bits, so a slow KDF would defend against nothing while adding its cost to
//! *every RPC*. It is also why there is no salt — a rainbow table over a space
//! that size cannot exist, and a per-row salt would make the digest unindexable
//! and the lookup a scan.

use sha2::{Digest, Sha256};

use crate::dashboard::security::random_token;

/// The prefix every token carries.
const PREFIX: &str = "fqt_";

/// Separator between the public id and the secret. Not in base64url.
const SEPARATOR: char = '.';

/// How many bytes of randomness the public id carries, hex-encoded.
///
/// Eight bytes is 2^64 ids: enough that two mints never collide in a store an
/// operator manages by hand, and short enough to paste into a `revoke`.
const ID_BYTES: usize = 8;

/// The id's length once hex-encoded, which is what a parse checks.
const ID_LEN: usize = ID_BYTES * 2;

/// A freshly minted token: the string to show once, and what to store.
///
/// The plaintext is deliberately not `Debug`-printable through this struct
/// reaching a log by accident — it is carried out of here, shown, and dropped.
pub struct MintedToken {
    /// Public identifier, also the store key.
    pub id: String,
    /// The full `fqt_<id>.<secret>` string. Shown once, never stored.
    pub plaintext: String,
    /// `sha256(secret)`, hex. This is what the row keeps.
    pub hash: String,
}

impl std::fmt::Debug for MintedToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The id and the hash are not secret; the plaintext is the whole
        // credential, and a struct that prints it is one `dbg!` from a log.
        f.debug_struct("MintedToken")
            .field("id", &self.id)
            .field("plaintext", &"<redacted>")
            .field("hash", &self.hash)
            .finish()
    }
}

/// A token as a caller presented it, already reduced to what a lookup needs.
///
/// The secret does not survive parsing: what comes out is its digest, so no
/// caller of this module ever holds a credential it could log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresentedToken {
    /// The id to look the row up by.
    pub id: String,
    /// `sha256(secret)`, hex, to compare against the stored one.
    pub hash: String,
}

/// Mint a new token.
pub fn mint() -> MintedToken {
    let id_bytes: [u8; ID_BYTES] = rand::random();
    let id = hex(&id_bytes);
    let secret = random_token();
    MintedToken {
        hash: digest(&secret),
        plaintext: format!("{PREFIX}{id}{SEPARATOR}{secret}"),
        id,
    }
}

/// Read a presented token, or `None` if it is not one of ours.
///
/// A malformed token is not distinguished from an unknown one anywhere above
/// this function: telling them apart tells a caller whether an id exists.
pub fn parse(raw: &str) -> Option<PresentedToken> {
    let body = raw.strip_prefix(PREFIX)?;
    let (id, secret) = body.split_once(SEPARATOR)?;
    // The id is checked for shape before it becomes a settings key: an
    // arbitrary string here would let a caller probe for other keys under the
    // `auth:` prefix — sessions live there too.
    if id.len() != ID_LEN || !id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    if secret.is_empty() {
        return None;
    }
    Some(PresentedToken {
        id: id.to_string(),
        hash: digest(secret),
    })
}

/// Whether `id` could have come out of [`mint`]. Used by the revoke paths,
/// which take an id rather than a token.
pub fn is_token_id(id: &str) -> bool {
    id.len() == ID_LEN && id.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// `sha256(input)`, lowercase hex.
fn digest(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hex(&hasher.finalize())
}

/// Lowercase hex, the encoding both the id and the digest are stored in.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut out, byte| {
        use std::fmt::Write;
        // Writing to a String cannot fail; the result is discarded rather than
        // unwrapped so this stays panic-free in library code.
        let _ = write!(out, "{byte:02x}");
        out
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_minted_token_parses_back_to_its_own_row() {
        let minted = mint();
        let presented = parse(&minted.plaintext).expect("a minted token must parse");
        assert_eq!(presented.id, minted.id);
        assert_eq!(
            presented.hash, minted.hash,
            "the digest a lookup compares must be the digest that was stored"
        );
    }

    #[test]
    fn a_token_carries_its_prefix_and_its_id() {
        let minted = mint();
        assert!(minted.plaintext.starts_with("fqt_"));
        assert!(minted.plaintext.contains(&minted.id));
        assert!(
            !minted.plaintext.contains(&minted.hash),
            "the stored digest must not be recoverable from the token"
        );
    }

    #[test]
    fn two_mints_never_share_an_id_or_a_secret() {
        let (first, second) = (mint(), mint());
        assert_ne!(first.id, second.id);
        assert_ne!(first.hash, second.hash);
    }

    #[test]
    fn the_id_is_fixed_width_hex() {
        let minted = mint();
        assert_eq!(minted.id.len(), ID_LEN);
        assert!(is_token_id(&minted.id));
        assert!(!is_token_id("nothex0000000000"));
        assert!(!is_token_id("abc"));
        // A path separator must never survive into a settings key.
        assert!(!is_token_id("../../auth:users"));
    }

    #[test]
    fn a_secret_containing_the_base64url_underscore_still_parses() {
        // The reason the separator is `.` and not `_`: base64url uses `-` and
        // `_`, so a secret may contain either.
        let token = format!("fqt_{}.{}", "a".repeat(ID_LEN), "ab_cd-ef");
        let presented = parse(&token).expect("an underscore in the secret is legal");
        assert_eq!(presented.id, "a".repeat(ID_LEN));
        assert_eq!(presented.hash, digest("ab_cd-ef"));
    }

    #[test]
    fn a_dot_inside_the_secret_belongs_to_the_secret() {
        // `split_once` takes the first separator, so everything after it is the
        // secret — a token is never truncated at a later dot.
        let token = format!("fqt_{}.{}", "b".repeat(ID_LEN), "one.two");
        let presented = parse(&token).expect("parse");
        assert_eq!(presented.hash, digest("one.two"));
    }

    #[test]
    fn nothing_that_is_not_a_token_parses() {
        let id = "c".repeat(ID_LEN);
        for raw in [
            "",
            "fqt_",
            "fqt_.",
            // No prefix.
            &format!("{id}.secret"),
            // The wrong prefix.
            &format!("ghp_{id}.secret"),
            // No separator.
            &format!("fqt_{id}secret"),
            // An id that is not hex.
            "fqt_zzzzzzzzzzzzzzzz.secret",
            // An id of the wrong width.
            "fqt_abc.secret",
            // A secret of nothing.
            &format!("fqt_{id}."),
        ] {
            assert!(parse(raw).is_none(), "must not parse: {raw:?}");
        }
    }

    #[test]
    fn the_digest_is_sha256_and_stable() {
        // Pinned against a value computed outside this code, so a change of
        // algorithm cannot pass as a refactor: every stored row would stop
        // matching its token.
        assert_eq!(
            digest("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn the_plaintext_stays_out_of_the_debug_output() {
        let minted = mint();
        let rendered = format!("{minted:?}");
        assert!(rendered.contains("<redacted>"));
        assert!(
            !rendered.contains(&minted.plaintext),
            "a token must not reach a log through Debug"
        );
    }
}
