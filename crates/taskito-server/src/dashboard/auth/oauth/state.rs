//! Short-lived server-side state for an in-flight login.
//!
//! Holding the nonce and PKCE verifier server-side — keyed by an opaque state
//! token — is what makes the callback verifiable: a forged callback carries no
//! state row, and a replayed one finds the row already consumed.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use taskito_core::{now_millis, Result, Storage};

use crate::dashboard::security::random_token;
use crate::dashboard::stores::kv;

/// Settings-key prefix for a state row, suffixed by the state token.
pub const STATE_PREFIX: &str = "auth:oauth_state:";

/// Long enough for a consent screen, short enough to bound replay exposure.
const STATE_TTL_SECONDS: i64 = 5 * 60;

/// One in-flight login.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthState {
    /// Provider slot the flow started on.
    pub slot: String,
    /// OIDC nonce, echoed back in the id_token.
    pub nonce: String,
    /// PKCE verifier, sent at token exchange.
    pub code_verifier: String,
    /// Where to send the browser afterwards.
    pub next_url: String,
    /// Unix seconds.
    #[serde(default)]
    pub created_at: i64,
    /// Unix seconds.
    pub expires_at: i64,
}

impl OAuthState {
    fn is_expired(&self, now: i64) -> bool {
        now >= self.expires_at
    }
}

/// Mint and persist a state row. Returns `(state token, row)`.
pub fn create(storage: &impl Storage, slot: &str, next_url: &str) -> Result<(String, OAuthState)> {
    let now = now_seconds();
    let token = random_token();
    let state = OAuthState {
        slot: slot.to_string(),
        nonce: random_token(),
        code_verifier: random_token(),
        next_url: next_url.to_string(),
        created_at: now,
        expires_at: now + STATE_TTL_SECONDS,
    };
    kv::write(storage, &format!("{STATE_PREFIX}{token}"), &state)?;
    Ok((token, state))
}

/// Read a state row and delete it, whatever happens next.
///
/// The delete comes first so a replayed state finds nothing even if parsing or
/// validation fails afterwards — single use is the property that matters.
pub fn consume(storage: &impl Storage, token: &str) -> Result<Option<OAuthState>> {
    if token.is_empty() {
        return Ok(None);
    }
    let key = format!("{STATE_PREFIX}{token}");
    let Some(raw) = storage.get_setting(&key)? else {
        return Ok(None);
    };
    storage.delete_setting(&key)?;

    let Ok(state) = serde_json::from_str::<OAuthState>(&raw) else {
        return Ok(None);
    };
    Ok((!state.is_expired(now_seconds())).then_some(state))
}

/// Drop rows whose flow was abandoned. Returns how many were removed.
pub fn prune_expired(storage: &impl Storage) -> Result<usize> {
    let now = now_seconds();
    let mut removed = 0;
    for (token, raw) in kv::scan_prefix(storage, STATE_PREFIX)? {
        let expired = serde_json::from_str::<OAuthState>(&raw)
            .map(|state| state.is_expired(now))
            .unwrap_or(true);
        if expired && storage.delete_setting(&format!("{STATE_PREFIX}{token}"))? {
            removed += 1;
        }
    }
    Ok(removed)
}

/// PKCE S256 challenge: `base64url(sha256(verifier))`, unpadded.
pub fn s256_challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

fn now_seconds() -> i64 {
    now_millis() / 1_000
}

#[cfg(test)]
mod tests {
    use super::*;
    use taskito_core::{SqliteStorage, StorageBackend};

    fn storage() -> StorageBackend {
        StorageBackend::Sqlite(SqliteStorage::new(":memory:").expect("in-memory SQLite"))
    }

    #[test]
    fn the_pkce_challenge_matches_the_rfc_vector() {
        // RFC 7636 appendix B.
        assert_eq!(
            s256_challenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }

    #[test]
    fn a_state_row_is_single_use() {
        let storage = storage();
        let (token, created) = create(&storage, "google", "/jobs").expect("create");

        let consumed = consume(&storage, &token)
            .expect("storage")
            .expect("live row");
        assert_eq!(consumed.slot, "google");
        assert_eq!(consumed.next_url, "/jobs");
        assert_eq!(consumed.nonce, created.nonce);

        // Replaying the same state finds nothing.
        assert!(consume(&storage, &token).expect("storage").is_none());
    }

    #[test]
    fn an_unknown_or_expired_state_is_refused_and_cleaned_up() {
        let storage = storage();
        assert!(consume(&storage, "never-issued")
            .expect("storage")
            .is_none());
        assert!(consume(&storage, "").expect("storage").is_none());

        let expired = OAuthState {
            slot: "google".into(),
            nonce: "n".into(),
            code_verifier: "v".into(),
            next_url: "/".into(),
            created_at: 0,
            expires_at: 1,
        };
        kv::write(&storage, &format!("{STATE_PREFIX}stale"), &expired).expect("write");
        assert!(consume(&storage, "stale").expect("storage").is_none());
        // Consuming removed it, so the prune has nothing left to do.
        assert_eq!(prune_expired(&storage).expect("prune"), 0);
    }

    #[test]
    fn pruning_removes_abandoned_rows() {
        let storage = storage();
        let expired = OAuthState {
            slot: "google".into(),
            nonce: "n".into(),
            code_verifier: "v".into(),
            next_url: "/".into(),
            created_at: 0,
            expires_at: 1,
        };
        kv::write(&storage, &format!("{STATE_PREFIX}abandoned"), &expired).expect("write");
        let (live, _) = create(&storage, "google", "/").expect("create");

        assert_eq!(prune_expired(&storage).expect("prune"), 1);
        assert!(consume(&storage, &live).expect("storage").is_some());
    }
}
