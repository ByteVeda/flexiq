use crate::error::Result;
use crate::storage::{Storage, StorageBackend};

/// Token bucket rate limiter backed by SQLite for persistence. In-crate only:
/// consumed solely by `Scheduler`; the config contract is `RateLimitConfig`.
pub(crate) struct RateLimiter {
    storage: StorageBackend,
}

/// Parsed rate limit configuration (e.g., "100/m" → 100 tokens, refill 1.667/s).
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Bucket capacity.
    pub max_tokens: f64,
    /// Tokens added per second.
    pub refill_rate: f64,
}

impl RateLimitConfig {
    /// Parse a rate limit string like "100/s", "100/m", "1000/h".
    pub fn parse(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.split('/').collect();
        if parts.len() != 2 {
            return None;
        }

        let count: f64 = parts[0].trim().parse().ok()?;
        let refill_rate = match parts[1].trim() {
            "s" | "sec" | "second" => count,
            "m" | "min" | "minute" => count / 60.0,
            "h" | "hr" | "hour" => count / 3600.0,
            _ => return None,
        };

        Some(Self {
            max_tokens: count,
            refill_rate,
        })
    }
}

impl RateLimiter {
    /// Build a rate limiter over `storage`.
    pub(crate) fn new(storage: StorageBackend) -> Self {
        Self { storage }
    }

    /// Try to acquire a token for the given key.
    /// Returns `true` if the token was acquired, `false` if rate limited.
    /// Uses an atomic transaction to prevent race conditions.
    pub(crate) fn try_acquire(&self, key: &str, config: &RateLimitConfig) -> Result<bool> {
        self.storage
            .try_acquire_token(key, config.max_tokens, config.refill_rate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_rate_limit() {
        let config = RateLimitConfig::parse("100/m").unwrap();
        assert!((config.max_tokens - 100.0).abs() < f64::EPSILON);
        assert!((config.refill_rate - 100.0 / 60.0).abs() < 0.01);

        let config = RateLimitConfig::parse("50/s").unwrap();
        assert!((config.refill_rate - 50.0).abs() < f64::EPSILON);

        let config = RateLimitConfig::parse("3600/h").unwrap();
        assert!((config.refill_rate - 1.0).abs() < f64::EPSILON);

        assert!(RateLimitConfig::parse("invalid").is_none());
    }

    #[test]
    fn test_token_bucket() {
        let storage =
            StorageBackend::Sqlite(crate::storage::sqlite::SqliteStorage::in_memory().unwrap());
        let limiter = RateLimiter::new(storage);
        let config = RateLimitConfig {
            max_tokens: 3.0,
            refill_rate: 1.0,
        };

        // Should allow 3 acquires (bucket starts full)
        assert!(limiter.try_acquire("test", &config).unwrap());
        assert!(limiter.try_acquire("test", &config).unwrap());
        assert!(limiter.try_acquire("test", &config).unwrap());

        // 4th should be denied
        assert!(!limiter.try_acquire("test", &config).unwrap());
    }

    #[test]
    fn test_concurrent_token_acquisition() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::sync::Barrier;

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("rate_limit_test.db");
        let storage = StorageBackend::Sqlite(
            crate::storage::sqlite::SqliteStorage::new(db_path.to_str().unwrap()).unwrap(),
        );
        let limiter = Arc::new(RateLimiter::new(storage));
        let config = RateLimitConfig {
            max_tokens: 10.0,
            refill_rate: 0.0, // no refill so we can count exactly
        };

        let num_threads = 20;
        let acquired = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(num_threads));
        let mut handles = vec![];

        for _ in 0..num_threads {
            let limiter = limiter.clone();
            let config = config.clone();
            let acquired = acquired.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                // Retry on lock contention (SQLite busy)
                let mut last_err = None;
                for _ in 0..10 {
                    match limiter.try_acquire("concurrent_test", &config) {
                        Ok(true) => {
                            acquired.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                        Ok(false) => return,
                        Err(e) => {
                            last_err = Some(e.to_string());
                            std::thread::sleep(std::time::Duration::from_millis(10));
                            continue;
                        }
                    }
                }
                panic!("exhausted retries in SQLite rate-limit test: {last_err:?}");
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // With 10 tokens and no refill, exactly 10 should succeed
        assert_eq!(acquired.load(Ordering::Relaxed), 10);
    }

    /// Postgres lost-update guard: under concurrency the row is seeded once and
    /// read `FOR UPDATE`, so a fresh bucket must admit exactly `max_tokens` — not
    /// more. Skips unless `FLEXIQ_POSTGRES_TEST_URL` is set (CI Postgres job).
    #[cfg(feature = "postgres")]
    #[test]
    fn test_postgres_concurrent_token_acquisition_no_over_admit() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc, Barrier};

        let url = match std::env::var("FLEXIQ_POSTGRES_TEST_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("Skipping Postgres rate-limit test (FLEXIQ_POSTGRES_TEST_URL not set)");
                return;
            }
        };
        let num_threads = 12;
        let storage = match crate::storage::postgres::PostgresStorage::with_pool_size(
            &url,
            num_threads as u32 + 4,
        ) {
            Ok(s) => StorageBackend::Postgres(s),
            Err(e) => {
                eprintln!("Skipping Postgres rate-limit test (cannot connect): {e}");
                return;
            }
        };
        let limiter = Arc::new(RateLimiter::new(storage));
        let config = RateLimitConfig {
            max_tokens: 5.0,
            refill_rate: 0.0,
        };
        // Unique key so repeated runs against the same DB start from a full bucket.
        let key = format!("pg-conc-{}", crate::job::now_millis());

        let acquired = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(num_threads));
        let mut handles = vec![];
        for _ in 0..num_threads {
            let limiter = limiter.clone();
            let config = config.clone();
            let acquired = acquired.clone();
            let barrier = barrier.clone();
            let key = key.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                let mut last_err = None;
                for _ in 0..10 {
                    match limiter.try_acquire(&key, &config) {
                        Ok(true) => {
                            acquired.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                        Ok(false) => return,
                        Err(e) => {
                            last_err = Some(e.to_string());
                            std::thread::sleep(std::time::Duration::from_millis(10));
                            continue;
                        }
                    }
                }
                panic!("exhausted retries in Postgres rate-limit test: {last_err:?}");
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(
            acquired.load(Ordering::Relaxed),
            5,
            "FOR UPDATE + seed must admit exactly max_tokens, never over-admit"
        );
    }
}
