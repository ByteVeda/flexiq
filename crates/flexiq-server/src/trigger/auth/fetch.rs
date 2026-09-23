//! The outbound half of verification: the key documents a verifier checks a
//! request against, fetched and cached.
//!
//! Two verifiers need it. A Pub/Sub push token is checked against Google's
//! published signing keys; an SNS message against the certificate its own
//! `SigningCertURL` names. Both are public documents that change rarely, so
//! each is cached and a request costs a fetch only on a miss.
//!
//! Every fetch is HTTPS-only, follows no redirect, gives up after ten seconds
//! and reads at most [`MAX_DOCUMENT_BYTES`]. The URL an SNS message names is
//! sender-supplied, so the verifier checks its host before it gets here; the
//! redirect refusal is what keeps a checked host from handing the request on
//! to an unchecked one.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

/// Largest key document read. A JWKS or a PEM certificate is a few kilobytes.
pub const MAX_DOCUMENT_BYTES: usize = 64 * 1024;

/// Most documents held at once. SNS names one certificate per region and
/// rotation, so this is generous; it bounds the memory an attacker-chosen
/// (if host-checked) URL can occupy.
const MAX_CACHED: usize = 32;

const FETCH_TIMEOUT: Duration = Duration::from_secs(10);

/// A pending fetch.
pub type FetchFuture = Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send>>;

/// How a URL becomes bytes. Injectable, so a test serves fixtures instead of
/// reaching the network.
pub type Fetch = Arc<dyn Fn(String) -> FetchFuture + Send + Sync>;

struct Cached {
    fetched: Instant,
    bytes: Arc<Vec<u8>>,
}

/// Fetches key documents and caches them by URL.
pub struct KeyFetcher {
    fetch: Fetch,
    cache: Mutex<HashMap<String, Cached>>,
}

impl Default for KeyFetcher {
    fn default() -> Self {
        Self::new(http_fetch())
    }
}

impl KeyFetcher {
    /// A fetcher over `fetch`.
    pub fn new(fetch: Fetch) -> Self {
        Self {
            fetch,
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// Fetch `url` without caching — for a one-off call such as confirming a
    /// subscription.
    pub async fn get(&self, url: &str) -> Result<Vec<u8>, String> {
        (self.fetch)(url.to_string()).await
    }

    /// `url`'s document, from the cache while it is younger than `ttl`.
    ///
    /// `refresh` asks for a fresh copy — the one retry a key-id miss earns —
    /// but is honoured only once the cached copy is older than `floor`, so a
    /// request naming a key that never existed cannot drive a fetch each.
    pub async fn cached(
        &self,
        url: &str,
        ttl: Duration,
        refresh: bool,
        floor: Duration,
    ) -> Result<Arc<Vec<u8>>, String> {
        if let Some(bytes) = self.lookup(url, ttl, refresh, floor) {
            return Ok(bytes);
        }
        let bytes = Arc::new(self.get(url).await?);
        let mut cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        if cache.len() >= MAX_CACHED && !cache.contains_key(url) {
            if let Some(oldest) = cache
                .iter()
                .min_by_key(|(_, entry)| entry.fetched)
                .map(|(key, _)| key.clone())
            {
                cache.remove(&oldest);
            }
        }
        cache.insert(
            url.to_string(),
            Cached {
                fetched: Instant::now(),
                bytes: bytes.clone(),
            },
        );
        Ok(bytes)
    }

    fn lookup(
        &self,
        url: &str,
        ttl: Duration,
        refresh: bool,
        floor: Duration,
    ) -> Option<Arc<Vec<u8>>> {
        let cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        let entry = cache.get(url)?;
        let age = entry.fetched.elapsed();
        let fresh = age < ttl && !(refresh && age >= floor);
        fresh.then(|| entry.bytes.clone())
    }
}

/// The production fetch: HTTPS only, no redirects, bounded in time and size.
fn http_fetch() -> Fetch {
    let client = reqwest::Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .timeout(FETCH_TIMEOUT)
        .build()
        .map_err(|error| log::error!("[flexiq] trigger key fetcher has no HTTP client: {error}"))
        .ok();
    Arc::new(move |url: String| {
        let client = client.clone();
        Box::pin(async move {
            let client = client.ok_or("the trigger key fetcher has no HTTP client")?;
            let mut response = client
                .get(&url)
                .send()
                .await
                .map_err(|error| format!("fetching {url} failed: {error}"))?;
            if !response.status().is_success() {
                return Err(format!("fetching {url} returned {}", response.status()));
            }
            let mut body = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|error| format!("reading {url} failed: {error}"))?
            {
                if body.len() + chunk.len() > MAX_DOCUMENT_BYTES {
                    return Err(format!("{url} is larger than {MAX_DOCUMENT_BYTES} bytes"));
                }
                body.extend_from_slice(&chunk);
            }
            Ok(body)
        })
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn counting() -> (KeyFetcher, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let fetch: Fetch = Arc::new(move |url: String| {
            counter.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Ok(url.into_bytes()) })
        });
        (KeyFetcher::new(fetch), calls)
    }

    const HOUR: Duration = Duration::from_secs(3600);

    #[tokio::test]
    async fn a_cached_document_is_not_fetched_again() {
        let (fetcher, calls) = counting();
        for _ in 0..3 {
            let bytes = fetcher
                .cached("https://a/keys", HOUR, false, HOUR)
                .await
                .expect("fetched");
            assert_eq!(bytes.as_slice(), b"https://a/keys");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_refresh_inside_the_floor_is_served_from_the_cache() {
        let (fetcher, calls) = counting();
        fetcher
            .cached("https://a/keys", HOUR, false, HOUR)
            .await
            .expect("fetched");
        fetcher
            .cached("https://a/keys", HOUR, true, HOUR)
            .await
            .expect("fetched");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "refresh inside the floor");

        fetcher
            .cached("https://a/keys", HOUR, true, Duration::ZERO)
            .await
            .expect("fetched");
        assert_eq!(calls.load(Ordering::SeqCst), 2, "refresh past the floor");
    }

    #[tokio::test]
    async fn the_cache_is_bounded() {
        let (fetcher, _) = counting();
        for n in 0..(MAX_CACHED + 5) {
            fetcher
                .cached(&format!("https://a/{n}"), HOUR, false, HOUR)
                .await
                .expect("fetched");
        }
        let held = fetcher
            .cache
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len();
        assert_eq!(held, MAX_CACHED);
    }
}
