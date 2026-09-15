//! A credential cache: refreshed before it expires, fetched once even when
//! many dispatches want it at the same moment.
//!
//! [`CredentialCache`] is the piece the OIDC and SigV4 signers share: neither
//! wants a fresh network round trip on every dispatch, and neither may let
//! several concurrent dispatches at the refresh boundary each call a
//! metadata server that rate-limits. Azure IMDS answers 429 and throttles a
//! caller that hammers it; GCE's metadata server is likewise not built for
//! high QPS. Without the single-flight gate below, every dispatch in flight
//! at the exact moment a token goes stale would call out at once.

use tokio::sync::{Mutex, RwLock};

use super::AuthError;
use crate::job::now_millis;

/// Refresh this long before expiry: room for one failed fetch and a retry
/// against a metadata server answering 429, without ever handing a signer a
/// token that dies in flight.
pub(crate) const REFRESH_SKEW_MS: i64 = 5 * 60 * 1000;

/// A credential whose whole life is shorter than twice the skew refreshes at
/// its midpoint instead, or the skew alone would mean refreshing on every
/// call.
///
/// A pure function of its two arguments, with no clock read, so it can be
/// tested without one.
pub(crate) fn refresh_at_ms(issued_at_ms: i64, expires_at_ms: i64) -> i64 {
    let lifetime_ms = expires_at_ms - issued_at_ms;
    if lifetime_ms < 2 * REFRESH_SKEW_MS {
        issued_at_ms + lifetime_ms / 2
    } else {
        expires_at_ms - REFRESH_SKEW_MS
    }
}

/// A credential and the two instants that govern it.
pub(crate) struct Expiring<T> {
    pub(crate) value: T,
    /// When to start trying to replace it.
    pub(crate) refresh_at_ms: i64,
    /// When it stops being usable at all.
    pub(crate) expires_at_ms: i64,
}

/// One cached credential, refreshed before it expires and fetched once even
/// when many dispatches want it at the same moment.
///
/// Two locks doing two different jobs: the [`RwLock`] lets any number of
/// dispatches read a fresh value with no contention between them, and the
/// [`Mutex`] is a single-flight gate so only one task at a time ever calls
/// the caller's `fetch`. See the module doc for why that gate is not
/// optional.
pub(crate) struct CredentialCache<T: Clone + Send + Sync> {
    value: RwLock<Option<Expiring<T>>>,
    /// Held for the duration of one fetch. Its value carries nothing; it
    /// exists only to be waited on.
    refresh_gate: Mutex<()>,
}

impl<T: Clone + Send + Sync> CredentialCache<T> {
    pub(crate) fn new() -> Self {
        Self {
            value: RwLock::new(None),
            refresh_gate: Mutex::new(()),
        }
    }

    /// The cached value, refreshing it first if it is due.
    pub(crate) async fn get_or_refresh<F, Fut>(&self, fetch: F) -> Result<T, AuthError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<Expiring<T>, AuthError>>,
    {
        if let Some(value) = self.fresh(now_millis()).await {
            return Ok(value);
        }

        // Past this point, only one task at a time ever reaches `fetch`.
        let _single_flight = self.refresh_gate.lock().await;

        // The whole point of this second check: another task may have
        // already refreshed while this one waited for the mutex above.
        // Skipping it would mean every waiter refetches the instant it gets
        // the mutex, which is exactly the stampede the mutex exists to stop.
        // The clock is read again here rather than reused from above: the
        // wait for the mutex is unbounded from this task's point of view,
        // and comparing against a "now" from before a long wait is the same
        // mistake as not re-checking at all.
        if let Some(value) = self.fresh(now_millis()).await {
            return Ok(value);
        }

        match fetch().await {
            Ok(expiring) => {
                let value = expiring.value.clone();
                *self.value.write().await = Some(expiring);
                Ok(value)
            }
            Err(error) => {
                // A fetch that failed inside the refresh window is not a
                // reason to fail a dispatch that has a working credential —
                // only a value that is now past its hard expiry is. The
                // clock is read again here too: `fetch().await` can itself
                // run for as long as the caller's own network timeout, and
                // the still-usable check has to reflect the time the
                // decision is actually made at.
                let now = now_millis();
                let guard = self.value.read().await;
                match guard.as_ref() {
                    Some(existing) if now < existing.expires_at_ms => {
                        let value = existing.value.clone();
                        drop(guard);
                        log::warn!(
                            "credential refresh failed, reusing the still-usable cached value: {error}"
                        );
                        Ok(value)
                    }
                    _ => Err(error),
                }
            }
        }
    }

    /// `Some` when a value exists and has not yet reached its refresh point.
    async fn fresh(&self, now: i64) -> Option<T> {
        let guard = self.value.read().await;
        match guard.as_ref() {
            Some(existing) if now < existing.refresh_at_ms => Some(existing.value.clone()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;

    fn expiring(value: u32, issued_at_ms: i64, ttl_ms: i64) -> Expiring<u32> {
        let expires_at_ms = issued_at_ms + ttl_ms;
        Expiring {
            value,
            refresh_at_ms: refresh_at_ms(issued_at_ms, expires_at_ms),
            expires_at_ms,
        }
    }

    /// One hour: long enough that none of the fixed-value tests below stray
    /// anywhere near their own refresh point mid-assertion.
    const LONG_TTL_MS: i64 = 60 * 60 * 1000;

    #[tokio::test]
    async fn a_fresh_value_is_returned_without_fetching() {
        let cache = CredentialCache::new();
        let calls = Arc::new(AtomicUsize::new(0));

        let make_fetch = || {
            let calls = calls.clone();
            move || {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, AuthError>(expiring(1, now_millis(), LONG_TTL_MS))
                }
            }
        };

        let first = cache
            .get_or_refresh(make_fetch())
            .await
            .expect("first call fetches");
        let second = cache
            .get_or_refresh(make_fetch())
            .await
            .expect("second call reads the cache");

        assert_eq!(first, 1);
        assert_eq!(second, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_value_past_its_refresh_point_is_refetched() {
        let cache = CredentialCache::new();
        let now = now_millis();
        *cache.value.write().await = Some(Expiring {
            value: 7u32,
            refresh_at_ms: now - 1,
            expires_at_ms: now + LONG_TTL_MS,
        });

        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_fetch = calls.clone();
        let value = cache
            .get_or_refresh(move || {
                let calls = calls_for_fetch.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, AuthError>(expiring(9, now_millis(), LONG_TTL_MS))
                }
            })
            .await
            .expect("a due value is refetched");

        assert_eq!(value, 9);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_callers_refresh_once() {
        let cache = Arc::new(CredentialCache::new());
        let calls = Arc::new(AtomicUsize::new(0));

        // A value due for refresh but not yet expired: every task starts
        // past the fast path in step 1 and into the single-flight gate.
        let now = now_millis();
        *cache.value.write().await = Some(Expiring {
            value: 0u32,
            refresh_at_ms: now - 1,
            expires_at_ms: now + LONG_TTL_MS,
        });

        // Every task is spawned before any of them is awaited below: that is
        // what makes this a genuine race rather than 16 sequential calls
        // that happen to share one executor.
        let mut handles = Vec::with_capacity(16);
        for _ in 0..16 {
            let cache = cache.clone();
            let calls = calls.clone();
            handles.push(tokio::spawn(async move {
                cache
                    .get_or_refresh(move || {
                        let calls = calls.clone();
                        async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            // Widens the window every other task has to pile
                            // up on the mutex, rather than this one finishing
                            // before the next has even started.
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            Ok::<_, AuthError>(expiring(1, now_millis(), LONG_TTL_MS))
                        }
                    })
                    .await
            }));
        }

        for handle in handles {
            handle
                .await
                .expect("task did not panic")
                .expect("get_or_refresh succeeds");
        }

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "16 concurrent callers against one due value must fetch exactly once"
        );
    }

    #[tokio::test]
    async fn a_failed_refresh_keeps_a_still_usable_value() {
        let cache = CredentialCache::new();
        let now = now_millis();
        *cache.value.write().await = Some(Expiring {
            value: 5u32,
            refresh_at_ms: now - 1,
            expires_at_ms: now + LONG_TTL_MS,
        });

        let value = cache
            .get_or_refresh(|| async {
                Err::<Expiring<u32>, AuthError>(AuthError::Transport("boom".to_string()))
            })
            .await
            .expect("a still-usable value is returned instead of the error");

        assert_eq!(value, 5);
    }

    #[tokio::test]
    async fn a_failed_refresh_past_expiry_is_an_error() {
        let cache = CredentialCache::new();
        let now = now_millis();
        *cache.value.write().await = Some(Expiring {
            value: 5u32,
            refresh_at_ms: now - 2,
            expires_at_ms: now - 1,
        });

        let error = cache
            .get_or_refresh(|| async {
                Err::<Expiring<u32>, AuthError>(AuthError::Transport("boom".to_string()))
            })
            .await
            .expect_err("a value past expiry with a failing refresh must error");

        assert!(matches!(error, AuthError::Transport(_)));
    }

    #[test]
    fn a_short_lived_credential_refreshes_at_its_midpoint() {
        // Shorter than twice the skew: the midpoint branch.
        let issued = 0;
        let short_expiry = 4 * 60 * 1000; // 4 minutes < 2 * 5 minutes
        assert_eq!(
            refresh_at_ms(issued, short_expiry),
            issued + short_expiry / 2
        );

        // Longer than twice the skew: `REFRESH_SKEW_MS` before expiry.
        let long_expiry = 60 * 60 * 1000; // 1 hour
        assert_eq!(
            refresh_at_ms(issued, long_expiry),
            long_expiry - REFRESH_SKEW_MS
        );

        // Exactly twice the skew: the condition is strict `<`, so this stays
        // on the skew-before-expiry branch, not the midpoint one.
        let boundary_expiry = 2 * REFRESH_SKEW_MS;
        assert_eq!(
            refresh_at_ms(issued, boundary_expiry),
            boundary_expiry - REFRESH_SKEW_MS
        );
    }
}
