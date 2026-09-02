//! Authenticating against the stored API tokens.
//!
//! This is the credential the door has. It replaces #716's shared secret, which
//! was one string every client presented: it could not be revoked for one of
//! them, it carried no scope of its own, and it left no record of which client
//! called. A token here is a named row with its own scopes, its own namespace
//! and an expiry, and revoking it is a write.
//!
//! **There is no cache.** A cached allow decision is a revocation that has not
//! taken effect, and "a revoked token fails on the next RPC, with no restart"
//! is the point of the issue this closes. Every call is one point read, keyed by
//! the id inside the token itself, on a path that is about to do a database
//! write anyway.
//!
//! **An empty store authenticates nobody.** A gRPC listener with no credential
//! provisioned is a misconfiguration, not a permission grant — Temporal's
//! self-hosted default ships a `noopAuthorizer` that allows everything, and that
//! is the shape of failure this refuses to have.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use flexiq_core::{now_millis, StorageBackend};
use tonic::metadata::MetadataMap;
use tonic::Status;

use super::authenticator::Authenticator;
use super::bearer;
use super::principal::Principal;
use crate::grpc::blocking;
use crate::grpc::status::WireError;
use crate::tokens::model::{ApiToken, EXPIRY_WARNING_DAYS};
use crate::tokens::{secret, store};

/// Checks presented credentials against the token store.
pub struct TokenStore {
    /// The rows live here; the same handle the handlers use.
    storage: StorageBackend,
    /// The one namespace this listener serves.
    namespace: Arc<str>,
    /// Lowest expiry threshold already warned about, per token.
    ///
    /// Per process rather than persisted: a restart repeating one warning is
    /// harmless, and a stored "already warned" flag would be a write on the
    /// authentication path.
    warned: Mutex<HashMap<String, i64>>,
    /// When each token's `last_used_at` was last written.
    touched: Mutex<HashMap<String, i64>>,
}

impl std::fmt::Debug for TokenStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenStore")
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

impl TokenStore {
    /// Authenticate against `storage`, for the namespace this listener serves.
    pub fn new(storage: StorageBackend, namespace: impl Into<Arc<str>>) -> Self {
        Self {
            storage,
            namespace: namespace.into(),
            warned: Mutex::new(HashMap::new()),
            touched: Mutex::new(HashMap::new()),
        }
    }

    /// Whether this token's `last_used_at` is due a write, recording that it is
    /// about to happen.
    ///
    /// Coalesced because writing it per RPC would double the write load of the
    /// door for a field read at admin frequency. The map is keyed by id and
    /// never pruned within a process's life: it holds one timestamp per token
    /// that has been *used*, which is bounded by the number of tokens minted.
    fn touch_due(&self, id: &str, now: i64) -> bool {
        let mut touched = match self.touched.lock() {
            Ok(guard) => guard,
            // A poisoned lock means another thread panicked while holding it.
            // The consequence here is a stale `last_used_at`, which must never
            // be a reason to refuse a valid credential.
            Err(poisoned) => poisoned.into_inner(),
        };
        match touched.get(id) {
            Some(last) if now - *last < store::TOUCH_INTERVAL_MS => false,
            _ => {
                touched.insert(id.to_string(), now);
                true
            }
        }
    }

    /// Warn once per threshold about a credential that is running out.
    ///
    /// On use rather than on a timer: a sweep would need a background task and
    /// would shout about tokens nobody holds any more, while this tells the
    /// operator about the credentials that are actually carrying traffic — which
    /// are the ones whose expiry will be an outage.
    fn warn_if_expiring(&self, token: &ApiToken, now: i64) {
        let remaining = token.days_remaining(now);
        // Reversed, because the thresholds descend and the one that matters is
        // the *tightest* one crossed. Searching forwards would answer 30 at ten
        // days remaining, and the token would never warn past the first.
        let Some(threshold) = EXPIRY_WARNING_DAYS
            .into_iter()
            .rev()
            .find(|days| remaining <= *days)
        else {
            return;
        };
        let mut warned = match self.warned.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        // Thresholds descend, so a token only re-warns as it crosses a lower
        // one. `<=` would repeat the same warning on every call.
        if warned.get(&token.id).is_some_and(|last| *last <= threshold) {
            return;
        }
        warned.insert(token.id.clone(), threshold);
        log::warn!(
            "gRPC token '{}' (id {}) expires in {remaining} days. Mint a replacement \
             and revoke this one before it lapses; callers presenting it will start \
             receiving UNAUTHENTICATED.",
            token.name,
            token.id,
        );
    }
}

#[async_trait::async_trait]
impl Authenticator for TokenStore {
    async fn authenticate(&self, metadata: &MetadataMap) -> Result<Principal, Status> {
        // Every way of being wrong past this point is one answer. A caller must
        // not be able to tell "no header" from "an id that does not exist" from
        // "the right id and the wrong secret" — each distinction is a step in
        // guessing a credential.
        let refused = || -> Status { WireError::unauthenticated().into() };

        let Some(presented) = bearer::presented(metadata).and_then(secret::parse) else {
            return Err(refused());
        };

        // Reaching storage is the one failure that is *not* the caller's fault,
        // so it keeps its own answer: `UNAVAILABLE`, sanitised. Telling a client
        // with a valid token to go and rotate it would be a worse lie than
        // saying the server is having trouble.
        let id = presented.id.clone();
        let found =
            blocking::on_storage(&self.storage, move |storage| store::get(storage, &id)).await?;

        let Some(token) = found else {
            return Err(refused());
        };
        if !crate::dashboard::security::constant_time_eq(&token.hash, &presented.hash) {
            return Err(refused());
        }
        // The credential carries the namespace (§5.1), and the settings store is
        // one global keyspace — two listeners serving different namespaces read
        // the same rows. A token minted elsewhere is refused here as
        // `UNAUTHENTICATED` rather than `PERMISSION_DENIED`, because the latter
        // would confirm that the id exists.
        if *token.namespace != *self.namespace {
            log::warn!(
                "gRPC token '{}' is bound to namespace '{}' and was presented to a \
                 listener serving '{}'; refusing it",
                token.id,
                token.namespace,
                self.namespace,
            );
            return Err(refused());
        }
        let now = now_millis();
        if !token.is_usable(now) {
            return Err(refused());
        }

        self.warn_if_expiring(&token, now);
        if self.touch_due(&token.id, now) {
            let id = token.id.clone();
            // A failed touch must never fail the call that succeeded: the
            // credential was valid, and `last_used_at` is a diagnostic.
            if let Err(error) = blocking::on_storage(&self.storage, move |storage| {
                store::touch(storage, &id, now)
            })
            .await
            {
                log::warn!("could not record use of gRPC token '{}': {error}", token.id);
            }
        }

        // The namespace is the listener's `Arc`, not a fresh allocation from the
        // row: the check above proved they are the same string.
        Ok(Principal::new(Arc::clone(&self.namespace), token.scopes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::status::reason;
    use crate::tokens::model::{NewToken, DEFAULT_LIFETIME_DAYS, MAX_LIFETIME_DAYS};
    use crate::tokens::scope::{Scope, ScopeSet};
    use flexiq_core::storage::sqlite::SqliteStorage;
    use flexiq_core::Storage;
    use tonic::Code;

    fn backend() -> StorageBackend {
        StorageBackend::Sqlite(SqliteStorage::in_memory().expect("in-memory sqlite"))
    }

    fn mint(storage: &StorageBackend, namespace: &str, scopes: ScopeSet) -> (String, String) {
        let request = NewToken::new("test", scopes, namespace, None, None).expect("valid");
        let (row, plaintext) = store::create(storage, request).expect("create");
        (row.id, plaintext)
    }

    fn with_bearer(token: &str) -> MetadataMap {
        let mut metadata = MetadataMap::new();
        metadata.insert(
            bearer::AUTHORIZATION,
            format!("Bearer {token}").parse().expect("ASCII"),
        );
        metadata
    }

    #[tokio::test]
    async fn a_stored_token_authenticates_with_its_own_scopes() {
        let storage = backend();
        let (_, plaintext) = mint(&storage, "prod", ScopeSet::of(&[Scope::Produce]));
        let principal = TokenStore::new(storage, "prod")
            .authenticate(&with_bearer(&plaintext))
            .await
            .expect("a valid token must be accepted");
        assert_eq!(&**principal.namespace(), "prod");
        assert!(principal.grants(Scope::Produce));
        assert!(
            !principal.grants(Scope::Execute),
            "a token grants what it was minted with, not everything"
        );
    }

    /// The acceptance criterion the issue leads with, at the unit level; the
    /// wire-level version lives in `tests/grpc_auth.rs`.
    #[tokio::test]
    async fn a_revoked_token_stops_working_immediately() {
        let storage = backend();
        let (id, plaintext) = mint(&storage, "prod", ScopeSet::ALL);
        let authenticator = TokenStore::new(storage.clone(), "prod");
        assert!(authenticator
            .authenticate(&with_bearer(&plaintext))
            .await
            .is_ok());

        assert!(store::revoke(&storage, &id).expect("revoke"));

        let status = authenticator
            .authenticate(&with_bearer(&plaintext))
            .await
            .expect_err("the same authenticator must refuse it now");
        assert_eq!(status.code(), Code::Unauthenticated);
    }

    #[tokio::test]
    async fn an_expired_token_is_refused() {
        let storage = backend();
        let (id, plaintext) = mint(&storage, "prod", ScopeSet::ALL);
        // Reach past the store rather than waiting 90 days: the row is the
        // contract, and expiry is read off it.
        let mut row = store::get(&storage, &id).expect("read").expect("present");
        row.expires_at = now_millis() - 1;
        storage
            .set_setting(
                &format!("{}{id}", store::KEY_PREFIX),
                &serde_json::to_string(&row).expect("encode"),
            )
            .expect("write");

        let status = TokenStore::new(storage, "prod")
            .authenticate(&with_bearer(&plaintext))
            .await
            .expect_err("an expired credential must not authenticate");
        assert_eq!(status.code(), Code::Unauthenticated);
    }

    /// §11: a token bound to one namespace must not reach another's data. One
    /// database can carry two listeners, and the settings store is not scoped.
    #[tokio::test]
    async fn a_token_for_another_namespace_is_refused() {
        let storage = backend();
        let (_, plaintext) = mint(&storage, "staging", ScopeSet::ALL);
        let status = TokenStore::new(storage, "prod")
            .authenticate(&with_bearer(&plaintext))
            .await
            .expect_err("a credential for another namespace must not be believed");
        assert_eq!(
            status.code(),
            Code::Unauthenticated,
            "not PermissionDenied: that would confirm the token exists"
        );
    }

    /// The fail-closed default #716 could not assert, because it had an
    /// anonymous authenticator to fall back on.
    #[tokio::test]
    async fn an_empty_store_authenticates_nobody() {
        let storage = backend();
        let authenticator = TokenStore::new(storage, "prod");
        for metadata in [MetadataMap::new(), with_bearer("fqt_0123456789abcdef.x")] {
            let status = authenticator
                .authenticate(&metadata)
                .await
                .expect_err("an unprovisioned door serves nothing");
            assert_eq!(status.code(), Code::Unauthenticated);
        }
    }

    #[tokio::test]
    async fn every_way_of_being_wrong_is_the_same_answer() {
        let storage = backend();
        let (id, plaintext) = mint(&storage, "prod", ScopeSet::ALL);
        let authenticator = TokenStore::new(storage, "prod");
        let secret_half = plaintext.split_once('.').expect("separated").1;

        let mut refusals = Vec::new();
        // No credential at all.
        refusals.push(authenticator.authenticate(&MetadataMap::new()).await);
        for raw in [
            // A well-formed token for an id that does not exist.
            "fqt_ffffffffffffffff.whatever".to_string(),
            // The right id, the wrong secret.
            format!("fqt_{id}.wrong"),
            // The right id, a prefix of the right secret.
            format!("fqt_{id}.{}", &secret_half[..secret_half.len() - 1]),
            // The right secret under the wrong id.
            format!("fqt_ffffffffffffffff.{secret_half}"),
            // Not one of our tokens at all.
            "some-other-credential".to_string(),
            // The secret with no envelope.
            secret_half.to_string(),
        ] {
            refusals.push(authenticator.authenticate(&with_bearer(&raw)).await);
        }
        // The right token under the wrong scheme.
        let mut basic = MetadataMap::new();
        basic.insert(
            bearer::AUTHORIZATION,
            format!("Basic {plaintext}").parse().expect("ASCII"),
        );
        refusals.push(authenticator.authenticate(&basic).await);

        for refusal in refusals {
            let status = refusal.expect_err("must refuse");
            assert_eq!(status.code(), Code::Unauthenticated);
            assert_eq!(
                status.message(),
                WireError::unauthenticated().message(),
                "the message must not say which way the credential was wrong"
            );
        }
        assert_eq!(
            WireError::unauthenticated().reason(),
            reason::UNAUTHENTICATED
        );
    }

    #[tokio::test]
    async fn using_a_token_records_when_it_was_used_at_most_once_a_minute() {
        let storage = backend();
        let (id, plaintext) = mint(&storage, "prod", ScopeSet::ALL);
        let authenticator = TokenStore::new(storage.clone(), "prod");

        authenticator
            .authenticate(&with_bearer(&plaintext))
            .await
            .expect("accepted");
        let first = store::get(&storage, &id)
            .expect("read")
            .expect("present")
            .last_used_at
            .expect("the first use is recorded");

        // A second call inside the window must not write again.
        authenticator
            .authenticate(&with_bearer(&plaintext))
            .await
            .expect("accepted");
        assert_eq!(
            store::get(&storage, &id)
                .expect("read")
                .expect("present")
                .last_used_at,
            Some(first),
            "the write is coalesced, not repeated per call"
        );
    }

    #[test]
    fn a_token_warns_once_per_threshold_as_it_runs_out() {
        let storage = backend();
        let request = NewToken::new("ci", ScopeSet::ALL, "prod", Some(MAX_LIFETIME_DAYS), None)
            .expect("valid");
        let (row, _) = store::create(&storage, request).expect("create");
        let authenticator = TokenStore::new(storage, "prod");

        let day = 24 * 60 * 60 * 1000;
        let at = |days: i64| row.expires_at - days * day;

        // Far out: nothing to say.
        authenticator.warn_if_expiring(&row, at(40));
        assert!(authenticator.warned.lock().expect("lock").is_empty());

        // Each threshold is recorded once, and a second call at the same
        // distance does not re-record it.
        for (days, expected) in [(30, 30), (25, 30), (20, 20), (11, 20), (10, 10), (1, 10)] {
            authenticator.warn_if_expiring(&row, at(days));
            assert_eq!(
                authenticator.warned.lock().expect("lock").get(&row.id),
                Some(&expected),
                "at {days} days remaining"
            );
        }
    }

    #[test]
    fn the_default_lifetime_is_inside_the_first_warning_threshold() {
        // Otherwise a token minted with the default would warn on its first use.
        assert!(DEFAULT_LIFETIME_DAYS > EXPIRY_WARNING_DAYS[0]);
    }
}
