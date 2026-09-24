mod archival;
mod circuit_breakers;
mod dashboard_settings;
mod dead_letter;
mod jobs;
mod locks;
mod logs;
mod metrics;
mod periodic;
mod pool;
mod pubsub;
mod queue_state;
mod rate_limits;
mod steps;
mod trait_impl;
mod workers;

#[cfg(feature = "push-dispatch")]
#[doc(hidden)]
pub mod listener;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::error::{QueueError, Result};

pub use pool::RedisConnection;

/// Redis-backed storage for the task queue.
#[derive(Clone)]
pub struct RedisStorage {
    pool: Arc<pool::ConnectionPool>,
    prefix: String,
    /// Set once an enqueue's wake `PUBLISH` has been refused and warned about,
    /// so a denied channel warns once per storage, not once per enqueue.
    wake_refusal_warned: Arc<AtomicBool>,
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

        // Validate the connection works; it then seeds the pool.
        let mut conn = client
            .get_connection()
            .map_err(|e| QueueError::Config(format!("Redis connection error: {e}")))?;
        redis::cmd("PING")
            .query::<String>(&mut conn)
            .map_err(|e| QueueError::Config(format!("Redis ping failed: {e}")))?;

        Ok(Self {
            pool: pool::ConnectionPool::new(client, conn),
            prefix: prefix.to_string(),
            wake_refusal_warned: Arc::new(AtomicBool::new(false)),
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

    /// Reuse an idle connection, or dial one; it returns to the pool when
    /// dropped. No `PING` on checkout — that would put back the round trip
    /// reuse removes; a dead socket fails its command and is not returned.
    ///
    /// Hold it only for the commands at hand, and never check out a second
    /// while holding one: each would be a connection of its own.
    pub fn conn(&self) -> Result<RedisConnection> {
        self.pool
            .get()
            .map_err(|e| QueueError::Other(format!("Redis connection error: {e}")))
    }

    /// The key prefix used for every Redis key this storage writes.
    ///
    /// Exposed so adjacent stores (e.g. the workflow store) can namespace
    /// their own keys under the same prefix without re-parsing the URL.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The pub/sub channel push-dispatch signals ready jobs of `(namespace,
    /// queue)` on. The enqueue side `PUBLISH`es here; every scheduler serving
    /// that queue in that namespace listens. A scheduler only claims its own
    /// namespace's jobs, so a shared channel would wake it for nothing.
    #[cfg(feature = "push-dispatch")]
    pub(crate) fn notify_channel(&self, namespace: Option<&str>, queue: &str) -> String {
        self.key(&["notify", &Self::namespace_segment(namespace), queue])
    }

    /// The client, for the listener's dedicated blocking connection — a
    /// `SUBSCRIBE`d connection must never return to the pool.
    #[cfg(feature = "push-dispatch")]
    pub fn client(&self) -> &redis::Client {
        self.pool.client()
    }

    /// Append `PUBLISH <notify-channel> <scheduled_at>` for `(namespace, queue)`
    /// onto `pipe`, `.ignore()`d so it never changes the pipe's reply shape.
    /// Every enqueue write path folds its notify in here instead of paying
    /// `notify_job_ready`'s own connection checkout + round trip (see
    /// `jobs/enqueue.rs`). The payload is the job's `scheduled_at` in ms: a
    /// listener drains when it is due and arms a timer for it otherwise.
    ///
    /// Run the pipe with [`exec_enqueue_pipe`](Self::exec_enqueue_pipe), and
    /// never fold into an atomic (`MULTI`/`EXEC`) pipe: an ACL that denies the
    /// channel rejects the `PUBLISH` at queue time, and `EXEC` then discards
    /// the whole transaction — a lost wake would become a lost enqueue.
    #[cfg(feature = "push-dispatch")]
    pub(crate) fn fold_notify(
        &self,
        pipe: &mut redis::Pipeline,
        namespace: Option<&str>,
        queue: &str,
        scheduled_at: i64,
    ) {
        pipe.publish(self.notify_channel(namespace, queue), scheduled_at)
            .ignore();
    }

    /// Run a non-atomic enqueue `pipe`, treating an error reply on a folded
    /// wake `PUBLISH` as a lost wake rather than a failed enqueue.
    ///
    /// Redis runs every command of a plain pipeline, so when only `PUBLISH`
    /// slots failed (say, an ACL user without channel permissions) the writes
    /// are committed and reporting `Err` would invite a duplicating retry. The
    /// wake is best-effort: the scheduler's fallback poll still finds the job.
    pub(crate) fn exec_enqueue_pipe(
        &self,
        pipe: &redis::Pipeline,
        conn: &mut RedisConnection,
    ) -> Result<()> {
        let err = match pipe.query::<()>(conn) {
            Ok(()) => return Ok(()),
            Err(err) => err,
        };
        // redis-rs reports every failing slot by its index in `pipe`, ignored
        // slots included, so a write's error can never hide behind a wake's.
        let only_wakes_failed = !pipe.is_transaction()
            && err.clone().into_server_errors().is_some_and(|failed| {
                !failed.is_empty() && failed.iter().all(|(slot, _)| is_publish(pipe, *slot))
            });
        if !only_wakes_failed {
            return Err(map_err(err));
        }
        if self.wake_refusal_warned.swap(true, Ordering::Relaxed) {
            log::debug!("push-dispatch: enqueue wake PUBLISH refused: {err}");
        } else {
            log::warn!(
                "push-dispatch: enqueue wake PUBLISH refused ({err}); jobs are still \
                 enqueued and dispatch on the fallback poll. Grant the connection's \
                 ACL user PUBLISH on the notify channels to restore push latency; \
                 further refusals log at debug"
            );
        }
        Ok(())
    }
}

/// Whether command `slot` of `pipe` is a `PUBLISH` — the only command an
/// enqueue pipe carries that is not a write.
fn is_publish(pipe: &redis::Pipeline, slot: usize) -> bool {
    pipe.cmd_iter()
        .nth(slot)
        .and_then(|cmd| cmd.args_iter().next())
        .is_some_and(
            |name| matches!(name, redis::Arg::Simple(n) if n.eq_ignore_ascii_case(b"PUBLISH")),
        )
}

#[cfg(feature = "push-dispatch")]
impl crate::storage::notify::StorageNotifier for RedisStorage {
    fn notify_job_ready(&self, namespace: Option<&str>, queue: &str, scheduled_at: i64) {
        // Best-effort PUBLISH: a broadcast, so every scheduler serving `queue`
        // wakes, not whichever popped a shared signal first. A failure only
        // costs the latency improvement — the fallback poll still dispatches.
        let mut conn = match self.conn() {
            Ok(c) => c,
            Err(e) => {
                log::warn!("push-dispatch: redis notify conn failed: {e}");
                return;
            }
        };
        let res: redis::RedisResult<()> = redis::cmd("PUBLISH")
            .arg(self.notify_channel(namespace, queue))
            .arg(scheduled_at)
            .query(&mut conn);
        if let Err(e) = res {
            log::warn!("push-dispatch: redis PUBLISH notify failed: {e}");
        }
    }
}

fn map_err(e: redis::RedisError) -> QueueError {
    QueueError::Redis(e)
}

/// [`redis::transaction`] that never hands a connection back to the pool
/// still `WATCH`ing: the helper `UNWATCH`es only on success, and a stale watch
/// would make the next borrower's `MULTI`/`EXEC` abort for a key it never read.
fn watched_transaction<T, F>(
    conn: &mut RedisConnection,
    keys: &[&str],
    func: F,
) -> redis::RedisResult<T>
where
    F: FnMut(&mut RedisConnection, &mut redis::Pipeline) -> redis::RedisResult<Option<T>>,
{
    let result = redis::transaction(conn, keys, func);
    if result.is_err() {
        // Best-effort: a failure here means the socket is gone or out of step,
        // and such a connection is dropped by the pool rather than reused.
        if let Err(e) = redis::cmd("UNWATCH").exec(conn) {
            log::debug!("redis UNWATCH after a failed transaction: {e}");
        }
    }
    result
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
    conn: &mut RedisConnection,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A storage over the hosted test Redis under a fresh prefix, or `None`
    /// (with a skip line) when none is configured.
    fn test_storage() -> Option<RedisStorage> {
        let Ok(url) = std::env::var("FLEXIQ_REDIS_TEST_URL") else {
            eprintln!("Skipping: FLEXIQ_REDIS_TEST_URL unset");
            return None;
        };
        let prefix = format!("flexiq-wake-test-{}:", uuid::Uuid::now_v7());
        Some(RedisStorage::with_prefix(&url, &prefix).unwrap())
    }

    /// A malformed `PUBLISH` (wrong arity) stands in for an ACL-denied one:
    /// both are an error reply on the wake's slot after the writes ran.
    fn failing_wake(pipe: &mut redis::Pipeline) {
        pipe.cmd("PUBLISH").arg("only-a-channel").ignore();
    }

    #[test]
    fn a_refused_wake_does_not_fail_the_enqueue_pipe() {
        let Some(s) = test_storage() else { return };
        let key = s.key(&["written"]);
        let mut conn = s.conn().unwrap();
        let pipe = &mut redis::pipe();
        pipe.set(&key, "v");
        failing_wake(pipe);

        s.exec_enqueue_pipe(pipe, &mut conn).unwrap();
        let written: Option<String> = redis::Commands::get(&mut conn, &key).unwrap();
        let _: () = redis::Commands::del(&mut conn, &key).unwrap();
        assert_eq!(written.as_deref(), Some("v"), "the write must land");
        assert!(s.wake_refusal_warned.load(Ordering::Relaxed));
    }

    #[test]
    fn a_failed_write_still_fails_the_enqueue_pipe() {
        let Some(s) = test_storage() else { return };
        let key = s.key(&["a-string"]);
        let mut conn = s.conn().unwrap();
        let pipe = &mut redis::pipe();
        pipe.set(&key, "v").ignore();
        // WRONGTYPE on an ignored write, beside a failing wake: still an error.
        pipe.sadd(&key, "member").ignore();
        failing_wake(pipe);

        let result = s.exec_enqueue_pipe(pipe, &mut conn);
        let _: () = redis::Commands::del(&mut conn, &key).unwrap();
        assert!(result.is_err(), "a write's error must surface");
    }

    #[test]
    fn a_transaction_never_hides_its_wake_failure() {
        let Some(s) = test_storage() else { return };
        let key = s.key(&["in-multi"]);
        let mut conn = s.conn().unwrap();
        let pipe = &mut redis::pipe();
        pipe.atomic().set(&key, "v");
        failing_wake(pipe);

        // A queue-time rejection aborts the EXEC, so nothing was written and
        // the error is the honest answer.
        assert!(s.exec_enqueue_pipe(pipe, &mut conn).is_err());
        let written: Option<String> = redis::Commands::get(&mut conn, &key).unwrap();
        assert_eq!(written, None, "EXECABORT discards the whole transaction");
    }
}
