//! Connection reuse for [`RedisStorage`](super::RedisStorage).
//!
//! Dialling per call costs a TCP connect plus the `HELLO` handshake before the
//! command runs — on a remote Redis, more than the command itself. Idle
//! connections are kept here and handed back out instead.
//!
//! Deliberately not a threaded pool: a pool that dials from background threads
//! leaves a process that `fork`s mid-dial (a pre-forking web server, a
//! `multiprocessing` child) with a wedged copy of that machinery. Here the
//! caller dials on its own thread, and nothing runs between calls.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use redis::{Cmd, ConnectionLike, ErrorKind, RedisError, RedisResult, Value};

/// Most idle connections kept per storage — sized for a scheduler, a worker
/// pool's threads and result handling issuing commands at once. A connection
/// returned beyond it is closed; checkouts are never capped, so none waits.
const MAX_IDLE: usize = 16;

/// An idle connection older than this is closed rather than reused, well under
/// the idle timeouts hosted Redis services apply, so a checkout rarely meets a
/// socket the server already closed (which would fail that one command).
const MAX_IDLE_AGE: Duration = Duration::from_secs(60);

struct IdleConnection {
    conn: redis::Connection,
    since: Instant,
}

/// The idle connections of one storage (shared by its clones).
pub(super) struct ConnectionPool {
    client: redis::Client,
    idle: Mutex<Vec<IdleConnection>>,
    /// The process whose sockets `idle` holds. A forked child inherits them
    /// still open on the parent's side too, so it must never use them.
    owner_pid: u32,
}

impl ConnectionPool {
    /// A pool seeded with `warm`, the connection construction validated with.
    pub(super) fn new(client: redis::Client, warm: redis::Connection) -> Arc<Self> {
        Self::owned_by(client, warm, std::process::id())
    }

    fn owned_by(client: redis::Client, warm: redis::Connection, owner_pid: u32) -> Arc<Self> {
        let pool = Arc::new(Self {
            client,
            idle: Mutex::new(Vec::with_capacity(MAX_IDLE)),
            owner_pid,
        });
        pool.put_back(warm, false);
        pool
    }

    /// The client connections are dialled from.
    #[cfg(feature = "push-dispatch")]
    pub(super) fn client(&self) -> &redis::Client {
        &self.client
    }

    /// Reuse the most recently returned live connection, or dial a new one.
    pub(super) fn get(self: &Arc<Self>) -> redis::RedisResult<RedisConnection> {
        if std::process::id() != self.owner_pid {
            // Inherited across a fork: dial per call, as before pooling, and
            // never touch the parent's sockets or its lock.
            return Ok(RedisConnection {
                conn: Some(self.client.get_connection()?),
                pool: None,
                poisoned: false,
            });
        }
        loop {
            let Some(idle) = self.lock_idle().pop() else {
                break;
            };
            if idle.conn.is_open() && idle.since.elapsed() < MAX_IDLE_AGE {
                return Ok(self.lend(idle.conn));
            }
        }
        Ok(self.lend(self.client.get_connection()?))
    }

    fn lend(self: &Arc<Self>, conn: redis::Connection) -> RedisConnection {
        RedisConnection {
            conn: Some(conn),
            pool: Some(Arc::clone(self)),
            poisoned: false,
        }
    }

    /// Keep `conn` for reuse unless it is broken (`poisoned`, or closed) or the
    /// pool is full.
    ///
    /// A broken connection empties the whole idle stack: whatever killed it (a
    /// server restart, a failover, a proxy dropping idle clients) most likely
    /// killed its siblings too, and each would otherwise fail one caller's
    /// command before being found out. Not retried — the dead socket may have
    /// accepted the write, and an enqueue is not idempotent.
    fn put_back(&self, conn: redis::Connection, poisoned: bool) {
        let mut idle = self.lock_idle();
        if poisoned || !conn.is_open() {
            idle.clear();
            return;
        }
        // Aged entries sit at the bottom of the stack, where a busy pool never
        // reaches them; close them here rather than leave the sockets open.
        idle.retain(|entry| entry.since.elapsed() < MAX_IDLE_AGE);
        if idle.len() < MAX_IDLE {
            idle.push(IdleConnection {
                conn,
                since: Instant::now(),
            });
        }
    }

    /// The list is only pushed and popped under the lock, so a panic elsewhere
    /// cannot leave it inconsistent — a poisoned lock is safe to reuse.
    fn lock_idle(&self) -> MutexGuard<'_, Vec<IdleConnection>> {
        self.idle.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A connection checked out of a [`RedisStorage`](super::RedisStorage),
/// returned to the pool on drop — unless a command on it failed in a way that
/// can leave it out of step with the server.
///
/// It is a [`ConnectionLike`] itself rather than a `Deref` to
/// [`redis::Connection`], so every command passes through it and no failure
/// goes unseen.
pub struct RedisConnection {
    /// `Some` until drop.
    conn: Option<redis::Connection>,
    /// `None` for a connection dialled in a forked child, which is not pooled.
    pool: Option<Arc<ConnectionPool>>,
    /// A failed read may have left part of a reply unread; the next borrower
    /// would take it for its own. redis-rs only marks the connection closed on
    /// EOF, so the guard remembers any such failure itself.
    poisoned: bool,
}

/// Whether `err` can leave the connection's reply stream out of step: any I/O
/// or protocol failure, as opposed to a server error reply, which is read in
/// full.
fn desyncs(err: &RedisError) -> bool {
    matches!(err.kind(), ErrorKind::Io | ErrorKind::Parse)
        || err.is_unrecoverable_error()
        || err.is_connection_dropped()
}

impl RedisConnection {
    fn inner(&mut self) -> &mut redis::Connection {
        match &mut self.conn {
            Some(conn) => conn,
            None => unreachable!("a RedisConnection holds its connection until drop"),
        }
    }

    fn inner_ref(&self) -> &redis::Connection {
        match &self.conn {
            Some(conn) => conn,
            None => unreachable!("a RedisConnection holds its connection until drop"),
        }
    }

    fn track<T>(&mut self, result: RedisResult<T>) -> RedisResult<T> {
        if let Err(err) = &result {
            self.poisoned |= desyncs(err);
        }
        result
    }
}

impl ConnectionLike for RedisConnection {
    fn req_packed_command(&mut self, cmd: &[u8]) -> RedisResult<Value> {
        let result = self.inner().req_packed_command(cmd);
        self.track(result)
    }

    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        count: usize,
    ) -> RedisResult<Vec<Value>> {
        let result = self.inner().req_packed_commands(cmd, offset, count);
        self.track(result)
    }

    fn req_command(&mut self, cmd: &Cmd) -> RedisResult<Value> {
        let result = self.inner().req_command(cmd);
        self.track(result)
    }

    fn get_db(&self) -> i64 {
        self.inner_ref().get_db()
    }

    fn check_connection(&mut self) -> bool {
        self.inner().check_connection()
    }

    fn is_open(&self) -> bool {
        self.inner_ref().is_open()
    }
}

impl Drop for RedisConnection {
    fn drop(&mut self) {
        if let (Some(conn), Some(pool)) = (self.conn.take(), self.pool.as_ref()) {
            pool.put_back(conn, self.poisoned);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A pool over the hosted test Redis, or `None` (with a skip line) when
    /// none is configured. A configured Redis that cannot be reached fails.
    fn test_pool(owner_pid: u32) -> Option<Arc<ConnectionPool>> {
        let Ok(url) = std::env::var("FLEXIQ_REDIS_TEST_URL") else {
            eprintln!("Skipping: FLEXIQ_REDIS_TEST_URL unset");
            return None;
        };
        let client = redis::Client::open(url).unwrap();
        let warm = client.get_connection().unwrap();
        Some(ConnectionPool::owned_by(client, warm, owner_pid))
    }

    fn client_id(conn: &mut impl ConnectionLike) -> i64 {
        redis::cmd("CLIENT").arg("ID").query(conn).unwrap()
    }

    #[test]
    fn a_returned_connection_is_reused() {
        let Some(pool) = test_pool(std::process::id()) else {
            return;
        };
        let first = client_id(&mut pool.get().unwrap());
        let second = client_id(&mut pool.get().unwrap());
        assert_eq!(first, second);
        assert_eq!(pool.lock_idle().len(), 1);
    }

    /// A forked child inherits the parent's idle sockets; using one would
    /// interleave two processes' replies on it.
    #[test]
    fn another_process_never_takes_the_idle_connections() {
        let Some(pool) = test_pool(std::process::id().wrapping_add(1)) else {
            return;
        };
        let mut conn = pool.get().unwrap();
        assert!(
            conn.pool.is_none(),
            "a foreign process's checkout is pooled"
        );
        client_id(&mut conn);
        drop(conn);
        // Still exactly the one seeded connection: nothing taken or added.
        assert_eq!(pool.lock_idle().len(), 1);
    }

    #[test]
    fn an_old_idle_connection_is_redialled() {
        let Some(pool) = test_pool(std::process::id()) else {
            return;
        };
        let stale = client_id(&mut pool.get().unwrap());
        pool.lock_idle()[0].since -= MAX_IDLE_AGE;
        let fresh = client_id(&mut pool.get().unwrap());
        assert_ne!(stale, fresh);
        assert_eq!(pool.lock_idle().len(), 1);
    }

    #[test]
    fn aged_idle_connections_are_pruned_on_return() {
        let Some(pool) = test_pool(std::process::id()) else {
            return;
        };
        let held: Vec<_> = (0..3).map(|_| pool.get().unwrap()).collect();
        drop(held);
        pool.lock_idle()
            .iter_mut()
            .for_each(|entry| entry.since -= MAX_IDLE_AGE);
        drop(pool.lock_idle().pop());
        let fresh = pool.get().unwrap();
        drop(fresh);
        assert_eq!(pool.lock_idle().len(), 1);
    }

    /// One server-side event kills every idle connection; only the first
    /// caller to meet a dead one fails, and the rest dial fresh.
    #[test]
    fn a_dead_connection_empties_the_idle_stack() {
        let Some(pool) = test_pool(std::process::id()) else {
            return;
        };
        let mut held: Vec<_> = (0..4).map(|_| pool.get().unwrap()).collect();
        let ids: Vec<i64> = held.iter_mut().map(client_id).collect();
        drop(held);
        assert_eq!(pool.lock_idle().len(), 4);

        let mut killer = pool.client.get_connection().unwrap();
        for id in &ids {
            let _: i64 = redis::cmd("CLIENT")
                .arg("KILL")
                .arg("ID")
                .arg(id)
                .query(&mut killer)
                .unwrap();
        }

        let mut failures = 0;
        for _ in 0..ids.len() {
            let mut conn = pool.get().unwrap();
            let pong: redis::RedisResult<String> = redis::cmd("PING").query(&mut conn);
            if pong.is_err() {
                failures += 1;
            }
        }
        assert!(failures <= 1, "{failures} callers met a killed connection");
        let mut conn = pool.get().unwrap();
        assert!(!ids.contains(&client_id(&mut conn)));
    }

    /// A read that times out leaves the reply in flight; redis-rs still calls
    /// the connection open, so only the guard's own bookkeeping keeps the next
    /// borrower from reading that stale reply as its own.
    #[test]
    fn a_connection_whose_read_failed_is_not_pooled() {
        let Some(pool) = test_pool(std::process::id()) else {
            return;
        };
        let mut conn = pool.get().unwrap();
        let first = client_id(&mut conn);
        conn.inner()
            .set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();
        let key = format!("pool_poison_{}", std::process::id());
        let popped: RedisResult<Option<(String, String)>> =
            redis::cmd("BLPOP").arg(&key).arg(2).query(&mut conn);
        assert!(popped.is_err(), "BLPOP returned before the read timeout");
        assert!(conn.is_open(), "redis-rs now closes on a read timeout");
        assert!(conn.poisoned);
        drop(conn);

        assert!(pool.lock_idle().is_empty());
        let mut next = pool.get().unwrap();
        assert_ne!(client_id(&mut next), first);
        let pong: String = redis::cmd("PING").query(&mut next).unwrap();
        assert_eq!(pong, "PONG");
    }

    #[test]
    fn idle_connections_are_capped() {
        let Some(pool) = test_pool(std::process::id()) else {
            return;
        };
        let held: Vec<_> = (0..MAX_IDLE + 2).map(|_| pool.get().unwrap()).collect();
        drop(held);
        assert_eq!(pool.lock_idle().len(), MAX_IDLE);
    }
}
