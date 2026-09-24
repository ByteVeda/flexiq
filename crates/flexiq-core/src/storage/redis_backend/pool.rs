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
//!
//! A forked child that inherits a storage (a module-level queue in a
//! pre-forking server) adopts its pool: it discards the parent's idle sockets
//! and pools its own from then on.

use std::sync::atomic::{AtomicU32, Ordering};
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
    owner_pid: AtomicU32,
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
            owner_pid: AtomicU32::new(owner_pid),
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
        let pid = std::process::id();
        let Some(mut idle) = self.lock_as(pid) else {
            // The stack was locked when this process was forked, so its lock
            // may never be released here: dial per call, pooling nothing.
            return Ok(RedisConnection {
                conn: Some(self.client.get_connection()?),
                pool: None,
                pid,
                poisoned: false,
            });
        };
        while let Some(entry) = idle.pop() {
            if entry.conn.is_open() && entry.since.elapsed() < MAX_IDLE_AGE {
                drop(idle);
                return Ok(self.lend(entry.conn, pid));
            }
        }
        drop(idle);
        Ok(self.lend(self.client.get_connection()?, pid))
    }

    /// Lock the idle stack on behalf of process `pid`, adopting the pool first
    /// if it was built in a parent. `None` when an inherited lock cannot be
    /// taken without waiting — a thread that no longer exists may hold it.
    fn lock_as(&self, pid: u32) -> Option<MutexGuard<'_, Vec<IdleConnection>>> {
        if self.owner_pid.load(Ordering::Acquire) == pid {
            return Some(self.lock_idle());
        }
        // Never block here: a lock held at fork time is never released.
        let mut idle = self.idle.try_lock().ok()?;
        if self.owner_pid.load(Ordering::Acquire) != pid {
            // The parent's sockets. Dropping them closes only this process's
            // descriptors and sends nothing, so the parent keeps its own.
            idle.clear();
            self.owner_pid.store(pid, Ordering::Release);
        }
        Some(idle)
    }

    fn lend(self: &Arc<Self>, conn: redis::Connection, pid: u32) -> RedisConnection {
        RedisConnection {
            conn: Some(conn),
            pool: Some(Arc::clone(self)),
            pid,
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
    /// `None` for a connection dialled in a forked child that could not adopt
    /// the pool, which is not pooled.
    pool: Option<Arc<ConnectionPool>>,
    /// The process that checked it out. A guard carried across a fork holds
    /// the parent's socket, which must not enter the child's pool.
    pid: u32,
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
        let Some(conn) = self.conn.take() else {
            return;
        };
        match &self.pool {
            // Not across a fork: that socket is the parent's, and the pool's
            // lock may be one the parent held when it forked.
            Some(pool) if self.pid == std::process::id() => pool.put_back(conn, self.poisoned),
            _ => drop(conn),
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
    /// interleave two processes' replies on it. It drops them and pools its own.
    #[test]
    fn a_forked_child_adopts_the_pool() {
        let parent = std::process::id().wrapping_add(1);
        let Some(pool) = test_pool(parent) else {
            return;
        };
        let inherited = client_id(&mut pool.lock_idle()[0].conn);

        let mut conn = pool.get().unwrap();
        assert!(
            conn.pool.is_some(),
            "the adopted pool lent an unpooled connection"
        );
        let own = client_id(&mut conn);
        assert_ne!(own, inherited, "the child reused the parent's socket");
        drop(conn);

        assert_eq!(pool.owner_pid.load(Ordering::Acquire), std::process::id());
        assert_eq!(client_id(&mut pool.get().unwrap()), own);
    }

    /// A lock held when the process forked is never released in the child;
    /// the child dials per call rather than wait on it.
    #[test]
    fn a_child_forked_mid_lock_dials_without_waiting() {
        let parent = std::process::id().wrapping_add(1);
        let Some(pool) = test_pool(parent) else {
            return;
        };
        let held_at_fork = pool.lock_idle();
        let mut conn = pool.get().unwrap();
        assert!(
            conn.pool.is_none(),
            "a connection was pooled past a held lock"
        );
        client_id(&mut conn);
        drop(conn);
        assert_eq!(held_at_fork.len(), 1);
        drop(held_at_fork);
        assert_eq!(pool.owner_pid.load(Ordering::Acquire), parent);
    }

    /// A guard carried across a fork holds the parent's socket: dropping it in
    /// the child must not hand that socket to the child's pool.
    #[test]
    fn a_guard_dropped_after_a_fork_is_not_pooled() {
        let Some(pool) = test_pool(std::process::id()) else {
            return;
        };
        let mut conn = pool.get().unwrap();
        conn.pid = conn.pid.wrapping_add(1);
        drop(conn);
        assert!(pool.lock_idle().is_empty());
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
