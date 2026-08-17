mod archival;
mod circuit_breakers;
mod dashboard_settings;
mod dead_letter;
mod jobs;
mod locks;
mod logs;
mod metrics;
mod periodic;
mod pubsub;
mod queue_state;
mod rate_limits;
mod trait_impl;
mod workers;

#[cfg(feature = "push-dispatch")]
#[doc(hidden)]
pub mod listener;

use crate::error::{QueueError, Result};

/// Redis-backed storage for the task queue.
#[derive(Clone)]
pub struct RedisStorage {
    client: redis::Client,
    prefix: String,
}

impl RedisStorage {
    /// Connect to Redis at the given URL with default prefix `"flexiq:"`.
    pub fn new(redis_url: &str) -> Result<Self> {
        Self::with_prefix(redis_url, "flexiq:")
    }

    /// Connect with a custom key prefix.
    pub fn with_prefix(redis_url: &str, prefix: &str) -> Result<Self> {
        let client = redis::Client::open(redis_url)
            .map_err(|e| QueueError::Config(format!("Redis connection error: {e}")))?;

        // Validate the connection works
        let mut conn = client
            .get_connection()
            .map_err(|e| QueueError::Config(format!("Redis connection error: {e}")))?;
        redis::cmd("PING")
            .query::<String>(&mut conn)
            .map_err(|e| QueueError::Config(format!("Redis ping failed: {e}")))?;

        Ok(Self {
            client,
            prefix: prefix.to_string(),
        })
    }

    /// Report that there is nothing to migrate.
    ///
    /// Redis stores no schema — keys are written on first use and every read is
    /// already defensive about fields an older writer never set — so an
    /// explicit migrate is a successful no-op rather than an error.
    pub fn migrate(&self) -> Result<crate::storage::migrate::MigrationReport> {
        Ok(crate::storage::migrate::MigrationReport::schemaless())
    }

    /// Always migrated: Redis stores no schema, so there is never a state where
    /// its keys are "not yet applied".
    pub fn is_migrated(&self) -> Result<bool> {
        Ok(true)
    }

    /// Build a Redis key from parts: `"{prefix}{part1}:{part2}:..."`.
    fn key(&self, parts: &[&str]) -> String {
        let mut k = self.prefix.clone();
        for (i, part) in parts.iter().enumerate() {
            if i > 0 {
                k.push(':');
            }
            k.push_str(part);
        }
        k
    }

    /// Get a Redis connection.
    pub fn conn(&self) -> Result<redis::Connection> {
        self.client
            .get_connection()
            .map_err(|e| QueueError::Other(format!("Redis connection error: {e}")))
    }

    /// The key prefix used for every Redis key this storage writes.
    ///
    /// Exposed so adjacent stores (e.g. the workflow store) can namespace
    /// their own keys under the same prefix without re-parsing the URL.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The single list key push-dispatch uses to signal ready jobs. The
    /// enqueue side `LPUSH`es a sentinel here; the listener `BLPOP`s it.
    #[cfg(feature = "push-dispatch")]
    pub(crate) fn notify_key(&self) -> String {
        self.key(&["notify", "_ready"])
    }

    /// A raw client clone, for the listener's dedicated blocking connection.
    #[cfg(feature = "push-dispatch")]
    pub fn client(&self) -> &redis::Client {
        &self.client
    }
}

#[cfg(feature = "push-dispatch")]
impl crate::storage::notify::StorageNotifier for RedisStorage {
    fn notify_job_ready(&self, _queue: &str, _scheduled_at: i64) {
        // Best-effort LPUSH of a sentinel onto the notify list. A failure only
        // costs the latency improvement — the fallback poll still dispatches.
        let mut conn = match self.conn() {
            Ok(c) => c,
            Err(e) => {
                log::warn!("push-dispatch: redis notify conn failed: {e}");
                return;
            }
        };
        let key = self.notify_key();
        // Keep the list short — a single pending sentinel is enough to wake the
        // listener; trim so it can't grow unbounded under bursty enqueues.
        let res: redis::RedisResult<()> = redis::pipe()
            .lpush(&key, 1)
            .ltrim(&key, 0, 15)
            .query(&mut conn);
        if let Err(e) = res {
            log::warn!("push-dispatch: redis LPUSH notify failed: {e}");
        }
    }
}

fn map_err(e: redis::RedisError) -> QueueError {
    QueueError::Redis(e)
}

/// Batch size for the bounded history scans (SSCAN/ZSCAN COUNT hint and the
/// ZRANGEBYSCORE LIMIT window). Caps how many ids a purge/list holds in memory
/// per round trip so a sweep over millions of rows never loads the whole set.
const SCAN_BATCH: isize = 500;

/// Drop the `payload`/`result` blobs from a job before it enters a listing.
/// Redis loads the whole job JSON in one read, so this saves no I/O; it exists
/// only to match the Diesel backends' narrow-projection contract — list results
/// are blob-free on every backend (fetch the full job via `get_job`).
fn strip_list_blobs(job: &mut crate::job::Job) {
    job.payload = Vec::new();
    job.result = None;
}

/// DLQ analogue of [`strip_list_blobs`]: a dead-letter entry carries only the
/// `payload` blob, dropped from listings (requeue re-reads the entry by id).
fn strip_dead_blob(dead: &mut crate::storage::DeadJob) {
    dead.payload = Vec::new();
}

/// Keyset page of member ids from a ZSET scored so that a higher score is newer
/// (e.g. `archived:all` by `completed_at`, `dlq:all` by `failed_at`), in
/// `(score, member)` **descending** order. `after` is the `(score, id)` of the
/// previous page's last row. Matches the Diesel `(sort_key, id) < cursor`
/// keyset: Redis orders equal-score members by reverse-lexicographic id under
/// `ZREVRANGEBYSCORE`, which is exactly `id DESC`.
fn zset_keyset_page(
    conn: &mut redis::Connection,
    zkey: &str,
    after: Option<(i64, &str)>,
    limit: i64,
) -> Result<Vec<String>> {
    use redis::Commands;
    if limit <= 0 {
        return Ok(Vec::new());
    }
    let Some((score, cursor_id)) = after else {
        // First page: the newest `limit` members overall.
        return conn
            .zrevrangebyscore_limit(zkey, "+inf", "-inf", 0, limit as isize)
            .map_err(map_err);
    };

    // Seek by rank while the cursor row is still indexed. `ZREVRANK` orders by
    // (score DESC, member DESC) — exactly the page order — so the next page is
    // the `limit` members that follow it, read in one bounded range. The rank is
    // re-derived every call, so concurrent inserts shift it without skipping or
    // repeating rows below the cursor.
    let rank: Option<isize> = conn.zrevrank(zkey, cursor_id).map_err(map_err)?;
    if let Some(rank) = rank {
        return conn
            .zrevrange(zkey, rank + 1, rank + limit as isize)
            .map_err(map_err);
    }

    // The cursor row was purged between pages, so there is no rank to seek from.
    // Fall back to the score bounds: the tie bucket (same score, id < cursor_id)
    // first, then everything strictly below that score.
    let same_score: Vec<String> = conn
        .zrangebyscore(zkey, score as f64, score as f64)
        .map_err(map_err)?;
    let mut page: Vec<String> = same_score
        .into_iter()
        .filter(|m| m.as_str() < cursor_id)
        .collect();
    page.reverse(); // ascending lex → descending id
    page.truncate(limit as usize);

    if (page.len() as i64) < limit {
        let remaining = limit - page.len() as i64;
        let lower: Vec<String> = conn
            .zrevrangebyscore_limit(zkey, format!("({score}"), "-inf", 0, remaining as isize)
            .map_err(map_err)?;
        page.extend(lower);
    }

    Ok(page)
}
