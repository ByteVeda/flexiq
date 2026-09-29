//! A short-lived, per-handle cache of quota documents.
//!
//! Every enqueue and every dispatch consults its namespace's quota, so reading
//! the settings row each time would add a round trip to the hottest paths. A
//! cached document is trusted for [`QUOTA_CACHE_TTL`]: an admin write reaches
//! every running process within that window, where runtime overrides only
//! reach the next start. A write through this same handle invalidates at once.
//!
//! A read that fails, or a document that does not decode, is an error — never
//! "unlimited" — and is never cached, so the next call retries the read.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use super::document::NamespaceQuota;
use super::key::quota_key;
use crate::error::{QueueError, Result};
use crate::storage::Storage;

/// How long a read quota document is trusted before it is read again.
pub const QUOTA_CACHE_TTL: Duration = Duration::from_secs(2);

/// One namespace's cached read: when it happened, and what it found. `None`
/// caches "no quota", which is most namespaces and the case worth caching.
type Entry = (Instant, Option<Arc<NamespaceQuota>>);

/// Quota documents by namespace, each with the instant it was read.
#[derive(Default)]
pub struct QuotaCache {
    entries: Mutex<HashMap<Option<String>, Entry>>,
}

impl QuotaCache {
    /// `namespace`'s quota, read through `storage` when the cached copy is
    /// missing or older than [`QUOTA_CACHE_TTL`].
    pub fn get<S: Storage>(
        &self,
        storage: &S,
        namespace: Option<&str>,
    ) -> Result<Option<Arc<NamespaceQuota>>> {
        let slot = namespace.map(str::to_string);
        if let Some((read_at, quota)) = self.lock().get(&slot) {
            if read_at.elapsed() < QUOTA_CACHE_TTL {
                return Ok(quota.clone());
            }
        }

        let quota = read_quota(storage, namespace)?.map(Arc::new);
        self.lock().insert(slot, (Instant::now(), quota.clone()));
        Ok(quota)
    }

    /// Forget `namespace`'s cached document, so the next read sees a write.
    pub fn invalidate(&self, namespace: Option<&str>) {
        self.lock().remove(&namespace.map(str::to_string));
    }

    /// The map, recovered from a poisoned lock: every entry is a complete
    /// value, so a panic elsewhere cannot have left one half-written.
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<Option<String>, Entry>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// `namespace`'s quota straight from the settings KV. `None` only when no
/// document exists; an unreadable one is an error naming the namespace.
pub fn read_quota<S: Storage>(
    storage: &S,
    namespace: Option<&str>,
) -> Result<Option<NamespaceQuota>> {
    let Some(raw) = storage.get_setting(&quota_key(namespace))? else {
        return Ok(None);
    };
    NamespaceQuota::from_json(&raw).map(Some).map_err(|e| {
        QueueError::Config(format!(
            "quota for namespace {} is unreadable, refusing rather than running unlimited: {e}",
            namespace.unwrap_or(crate::scheduler::retention::DEFAULT_NAMESPACE)
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::sqlite::SqliteStorage;

    #[test]
    fn a_missing_document_is_no_quota() {
        let storage = SqliteStorage::in_memory().unwrap();
        let cache = QuotaCache::default();
        assert!(cache.get(&storage, Some("t")).unwrap().is_none());
    }

    #[test]
    fn a_cached_document_survives_a_write_until_invalidated() {
        let storage = SqliteStorage::in_memory().unwrap();
        let cache = QuotaCache::default();
        storage
            .set_setting(&quota_key(Some("t")), r#"{"max_pending":1}"#)
            .unwrap();
        assert_eq!(
            cache.get(&storage, Some("t")).unwrap().unwrap().max_pending,
            Some(1)
        );

        storage
            .set_setting(&quota_key(Some("t")), r#"{"max_pending":2}"#)
            .unwrap();
        assert_eq!(
            cache.get(&storage, Some("t")).unwrap().unwrap().max_pending,
            Some(1)
        );
        cache.invalidate(Some("t"));
        assert_eq!(
            cache.get(&storage, Some("t")).unwrap().unwrap().max_pending,
            Some(2)
        );
    }

    #[test]
    fn an_unreadable_document_fails_closed_and_is_not_cached() {
        let storage = SqliteStorage::in_memory().unwrap();
        let cache = QuotaCache::default();
        storage
            .set_setting(&quota_key(None), r#"{"max_pending":-1}"#)
            .unwrap();
        let error = cache.get(&storage, None).unwrap_err().to_string();
        assert!(error.contains("namespace default is unreadable"), "{error}");

        storage.set_setting(&quota_key(None), "{}").unwrap();
        assert!(
            cache.get(&storage, None).unwrap().is_some(),
            "the error was cached"
        );
    }
}
