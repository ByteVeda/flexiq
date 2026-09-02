//! Reading and writing token rows.
//!
//! **One settings row per token, keyed by the token's public id** — not one
//! document holding every token. Two reasons, both on hot paths: a lookup is a
//! point read rather than a parse of the whole store, and writing one token's
//! `last_used_at` cannot lose a race against another token's revocation,
//! because they are different rows.
//!
//! The prefix lives under `auth:`, which `flexiq_core::RESERVED_SETTING_PREFIXES`
//! already reserves — its comment already says "dashboard sessions, OAuth state,
//! API tokens". So the dashboard's generic key/value surface neither lists nor
//! writes these rows, and a token cannot be forged through it.

use flexiq_core::{now_millis, QueueError, Result, Storage};

use super::model::{ApiToken, NewToken};
use super::secret;

/// Settings-key prefix every token row lives under.
pub const KEY_PREFIX: &str = "auth:grpc_token:";

/// How often a token's `last_used_at` may be rewritten.
///
/// Writing it per RPC would double the write load of the door for a field read
/// by a human at admin frequency. A minute is fine enough to answer "is this
/// credential still in use" and coarse enough to disappear against real
/// traffic.
pub const TOUCH_INTERVAL_MS: i64 = 60_000;

/// How many times a conditional write is re-read and retried.
///
/// Matches the settings store's own bound: a losing writer only loses to one
/// that won, and these writes are admin-frequency plus a coalesced touch.
const MAX_ATTEMPTS: usize = 25;

/// The settings key `id`'s row lives under.
fn key(id: &str) -> String {
    format!("{KEY_PREFIX}{id}")
}

/// Mint a token, store its row, and return the row with the string to show
/// once.
///
/// The plaintext is the second element rather than a field on the row so that
/// no code path can pass it along by accident: a caller has to name it.
pub fn create(storage: &impl Storage, request: NewToken) -> Result<(ApiToken, String)> {
    let minted = secret::mint();
    let row = request.into_row(minted.id.clone(), minted.hash);
    let encoded = serde_json::to_string(&row)?;
    // Conditional on the key being absent. An id collision is a 2^64 event, but
    // the difference between "unlikely" and "impossible" here is one argument,
    // and the thing it would overwrite is somebody's live credential.
    if !storage.set_setting_if(&key(&row.id), None, &encoded)? {
        return Err(QueueError::SettingConflict(key(&row.id)));
    }
    Ok((row, minted.plaintext))
}

/// One token by id, or `None` when there is no such row.
///
/// A row this build cannot parse reads as absent. That is the fail-closed
/// answer: an unreadable credential must not authenticate, and the log is how
/// the operator finds out the store holds something this build does not
/// understand.
pub fn get(storage: &impl Storage, id: &str) -> Result<Option<ApiToken>> {
    if !secret::is_token_id(id) {
        return Ok(None);
    }
    Ok(storage
        .get_setting(&key(id))?
        .and_then(|raw| parse(id, &raw)))
}

/// Every token, newest first, optionally narrowed to one namespace.
///
/// `namespace` is `Some` whenever the process serves one: the settings KV is a
/// single global keyspace, so a dashboard scoped to a namespace must not list
/// another's credentials just because they share a database.
pub fn list(storage: &impl Storage, namespace: Option<&str>) -> Result<Vec<ApiToken>> {
    let mut tokens: Vec<ApiToken> = storage
        .list_settings()?
        .into_iter()
        .filter_map(|(key, raw)| {
            let id = key.strip_prefix(KEY_PREFIX)?;
            parse(id, &raw)
        })
        .filter(|token| namespace.is_none_or(|name| token.namespace == name))
        .collect();
    // Newest first, then by id so a listing does not reshuffle between polls
    // when two tokens were minted in the same millisecond.
    tokens.sort_by(|left, right| {
        right
            .created_at
            .cmp(&left.created_at)
            .then_with(|| left.id.cmp(&right.id))
    });
    Ok(tokens)
}

/// Revoke a token, returning whether there was one to revoke.
///
/// Revoking an already-revoked token writes nothing and still answers `true`:
/// the answer describes the token's state, so a retry says the same thing as
/// the call it is retrying.
pub fn revoke(storage: &impl Storage, id: &str) -> Result<bool> {
    update(storage, id, |token| {
        if token.revoked_at.is_none() {
            token.revoked_at = Some(now_millis());
        }
    })
}

/// Record that a token was used at `now`.
///
/// Best-effort: a token that was revoked between the authentication and this
/// write is simply not there any more, and a failed touch must never fail the
/// call that succeeded.
pub fn touch(storage: &impl Storage, id: &str, now: i64) -> Result<bool> {
    update(storage, id, |token| token.last_used_at = Some(now))
}

/// Read, mutate and conditionally write one row, retrying a lost race.
///
/// Conditional rather than a plain write because the two mutations here race
/// each other by design: a touch happens on the hot path and a revoke happens
/// while it does. A read-then-write would let the touch resurrect a credential
/// an operator had just revoked.
fn update(storage: &impl Storage, id: &str, mut mutate: impl FnMut(&mut ApiToken)) -> Result<bool> {
    if !secret::is_token_id(id) {
        return Ok(false);
    }
    let key = key(id);
    for _ in 0..MAX_ATTEMPTS {
        let Some(stored) = storage.get_setting(&key)? else {
            return Ok(false);
        };
        let Some(mut token) = parse(id, &stored) else {
            return Ok(false);
        };
        mutate(&mut token);
        let encoded = serde_json::to_string(&token)?;
        // A mutation that changed nothing needs no write — which is what makes
        // revoking an already-revoked token free rather than a lost race.
        if encoded == stored {
            return Ok(true);
        }
        if storage.set_setting_if(&key, Some(&stored), &encoded)? {
            return Ok(true);
        }
    }
    Err(QueueError::SettingConflict(key))
}

/// Decode one row, logging and discarding what cannot be read.
fn parse(id: &str, raw: &str) -> Option<ApiToken> {
    match serde_json::from_str::<ApiToken>(raw) {
        Ok(token) if token.id == id => Some(token),
        Ok(token) => {
            // The key is the identity. A row whose body disagrees with it was
            // not written by this code, and honouring the body would let the
            // two disagree about which credential was just used.
            log::warn!(
                "gRPC token row '{id}' carries id '{}'; ignoring it",
                token.id
            );
            None
        }
        Err(error) => {
            log::warn!("gRPC token row '{id}' is not readable ({error}); ignoring it");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokens::model::{TokenStatus, MAX_LIFETIME_DAYS};
    use crate::tokens::scope::{Scope, ScopeSet};
    use flexiq_core::storage::sqlite::SqliteStorage;

    fn storage() -> SqliteStorage {
        SqliteStorage::in_memory().expect("in-memory sqlite")
    }

    fn request(name: &str, namespace: &str) -> NewToken {
        NewToken::new(
            name,
            ScopeSet::of(&[Scope::Produce]),
            namespace,
            None,
            Some("tests".to_string()),
        )
        .expect("a valid request")
    }

    #[test]
    fn a_created_token_is_readable_by_its_id_and_by_its_secret() {
        let storage = storage();
        let (row, plaintext) = create(&storage, request("ci", "prod")).expect("create");

        let stored = get(&storage, &row.id).expect("read").expect("present");
        assert_eq!(stored.name, "ci");
        assert_eq!(stored.namespace, "prod");
        assert_eq!(stored.created_by.as_deref(), Some("tests"));
        assert!(stored.last_used_at.is_none());

        let presented = secret::parse(&plaintext).expect("the returned token parses");
        assert_eq!(presented.id, row.id);
        assert_eq!(presented.hash, stored.hash);
    }

    #[test]
    fn the_row_holds_no_plaintext() {
        let storage = storage();
        let (row, plaintext) = create(&storage, request("ci", "prod")).expect("create");
        let raw = storage
            .get_setting(&key(&row.id))
            .expect("read")
            .expect("present");
        assert!(
            !raw.contains(&plaintext),
            "the stored row must not contain the token"
        );
        // Nor the secret half on its own.
        let secret_half = plaintext.split_once('.').expect("separated").1;
        assert!(!raw.contains(secret_half));
    }

    #[test]
    fn a_token_row_lives_under_a_reserved_prefix() {
        // The generic settings API hides these rows because of this, so a
        // change to the prefix that left the reservation behind would expose
        // every credential to it.
        assert!(flexiq_core::is_reserved_setting_key(&key(
            "abcdef0123456789"
        )));
    }

    #[test]
    fn revoking_takes_effect_on_the_next_read_and_is_idempotent() {
        let storage = storage();
        let (row, _) = create(&storage, request("ci", "prod")).expect("create");
        assert!(get(&storage, &row.id)
            .expect("read")
            .expect("present")
            .is_usable(now_millis()));

        assert!(revoke(&storage, &row.id).expect("revoke"));
        let revoked = get(&storage, &row.id).expect("read").expect("present");
        assert_eq!(revoked.status(now_millis()), TokenStatus::Revoked);
        assert!(!revoked.is_usable(now_millis()));

        // A second revoke describes the same state rather than failing.
        assert!(revoke(&storage, &row.id).expect("revoke again"));
        let again = get(&storage, &row.id).expect("read").expect("present");
        assert_eq!(again.revoked_at, revoked.revoked_at, "the instant is kept");
    }

    #[test]
    fn revoking_or_touching_an_unknown_id_says_so_rather_than_creating_one() {
        let storage = storage();
        assert!(!revoke(&storage, "0123456789abcdef").expect("revoke"));
        assert!(!touch(&storage, "0123456789abcdef", now_millis()).expect("touch"));
        assert_eq!(
            storage.get_setting(&key("0123456789abcdef")).expect("read"),
            None,
            "a miss must leave no row behind"
        );
    }

    /// An id that is not one of ours must never become a settings key: sessions
    /// live under the same reserved prefix.
    #[test]
    fn a_malformed_id_reaches_no_key_at_all() {
        let storage = storage();
        storage.set_setting("auth:users", "{}").expect("write");
        assert!(get(&storage, "../users").expect("read").is_none());
        assert!(!revoke(&storage, "users").expect("revoke"));
        assert_eq!(
            storage.get_setting("auth:users").expect("read").as_deref(),
            Some("{}"),
            "an unrelated row must be untouched"
        );
    }

    #[test]
    fn touching_records_the_instant() {
        let storage = storage();
        let (row, _) = create(&storage, request("ci", "prod")).expect("create");
        assert!(touch(&storage, &row.id, 1_700_000_000_000).expect("touch"));
        assert_eq!(
            get(&storage, &row.id)
                .expect("read")
                .expect("present")
                .last_used_at,
            Some(1_700_000_000_000)
        );
    }

    /// The race the conditional write exists for: a touch must not write back a
    /// row it read before a revoke landed.
    #[test]
    fn a_touch_cannot_resurrect_a_revoked_token() {
        let storage = storage();
        let (row, _) = create(&storage, request("ci", "prod")).expect("create");
        assert!(revoke(&storage, &row.id).expect("revoke"));
        assert!(touch(&storage, &row.id, now_millis()).expect("touch"));
        assert!(get(&storage, &row.id)
            .expect("read")
            .expect("present")
            .revoked_at
            .is_some());
    }

    #[test]
    fn a_listing_is_newest_first_and_scoped_to_one_namespace() {
        let storage = storage();
        let (first, _) = create(&storage, request("one", "prod")).expect("create");
        let (other, _) = create(&storage, request("elsewhere", "staging")).expect("create");
        let (last, _) = create(&storage, request("two", "prod")).expect("create");

        let prod = list(&storage, Some("prod")).expect("list");
        let ids: Vec<&str> = prod.iter().map(|token| token.id.as_str()).collect();
        assert!(ids.contains(&last.id.as_str()) && ids.contains(&first.id.as_str()));
        assert!(
            !ids.contains(&other.id.as_str()),
            "another namespace's credentials must not be listed"
        );
        assert!(
            prod[0].created_at >= prod[1].created_at,
            "newest first: {prod:?}"
        );

        assert_eq!(list(&storage, None).expect("list").len(), 3);
    }

    #[test]
    fn a_listing_skips_a_row_it_cannot_read_rather_than_failing() {
        let storage = storage();
        let (row, _) = create(&storage, request("ci", "prod")).expect("create");
        storage
            .set_setting(&key("ffffffffffffffff"), "not json")
            .expect("write");

        let tokens = list(&storage, None).expect("a bad row must not fail the listing");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].id, row.id);
        assert!(
            get(&storage, "ffffffffffffffff").expect("read").is_none(),
            "an unreadable credential must not authenticate"
        );
    }

    /// The key is the identity, so a row whose body claims another id is not a
    /// credential this build will honour.
    #[test]
    fn a_row_whose_body_disagrees_with_its_key_is_ignored() {
        let storage = storage();
        let (row, _) = create(&storage, request("ci", "prod")).expect("create");
        let raw = storage
            .get_setting(&key(&row.id))
            .expect("read")
            .expect("present");
        storage
            .set_setting(&key("aaaaaaaaaaaaaaaa"), &raw)
            .expect("write");
        assert!(get(&storage, "aaaaaaaaaaaaaaaa").expect("read").is_none());
    }

    #[test]
    fn the_maximum_lifetime_survives_a_round_trip() {
        let storage = storage();
        let request = NewToken::new("long", ScopeSet::ALL, "prod", Some(MAX_LIFETIME_DAYS), None)
            .expect("valid");
        let (row, _) = create(&storage, request).expect("create");
        let stored = get(&storage, &row.id).expect("read").expect("present");
        assert_eq!(stored.expires_at, row.expires_at);
        assert_eq!(stored.scopes, ScopeSet::ALL);
    }
}
