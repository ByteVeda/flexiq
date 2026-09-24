//! Wake sources for the scheduler loop.
//!
//! Entirely behind the `push-dispatch` feature. When the feature is off this
//! module is not compiled and the scheduler keeps its original adaptive-poll
//! loop unchanged.
//!
//! A [`WakeSource`] lets an enqueue (or retry, or periodic enqueue) wake the
//! scheduler immediately instead of waiting for the next poll tick, and lets a
//! delayed enqueue announce its `scheduled_at` so the loop arms a timer for it.
//! Polling is retained as a safety-net fallback so a missed notification can
//! never strand a job.

#![cfg(feature = "push-dispatch")]

use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, Notify};

use crate::storage::StorageBackend;

/// Where the scheduler gets its "a job is ready" signals from.
pub enum WakeSource {
    /// SQLite, single-process: an in-memory [`Notify`] shared with the
    /// storage layer, plus the delayed-job deadlines enqueue announced since
    /// the last wake. Enqueue calls `notify_one()` on the same handle.
    InProcess(Arc<Notify>, DelayedHints),
    /// Postgres `LISTEN` / Redis pub/sub: a background listener forwards one
    /// value per notification into this channel — `Some(scheduled_at)` (ms)
    /// when the message announced one, `None` for a plain wake.
    Channel(mpsc::Receiver<Option<i64>>),
    /// Feature compiled in but no wake source available — behaves like the
    /// fallback timer alone (the loop still dispatches on its periodic tick).
    Polling,
}

/// What one [`WakeSource::wait`] heard.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Wake {
    /// A job may be ready now, so the loop should drain.
    pub ready: bool,
    /// Future `scheduled_at`s (ms) announced; the loop arms a timer for each.
    pub delayed: Vec<i64>,
}

impl Wake {
    /// Fold one channel message in: a future deadline arms a timer, anything
    /// else — a due deadline, or none at all — is a plain wake.
    fn absorb(&mut self, announced: Option<i64>, now: i64) {
        match announced {
            Some(at) if at > now => self.delayed.push(at),
            _ => self.ready = true,
        }
    }
}

/// Delayed-job deadlines held between two wakes; past this a burst's extras
/// fall back to the timer rather than growing without bound.
const MAX_PENDING_HINTS: usize = 1024;

/// What in-process (SQLite) enqueues announced since the push loop's last
/// wake: delayed-job deadlines, and whether any job was ready. A `Notify` only
/// says "something happened", so this is what lets a delayed-only wake skip
/// the drain. Cloning shares the buffer.
#[derive(Clone, Default)]
pub struct DelayedHints(Arc<Mutex<Wake>>);

impl DelayedHints {
    fn lock(&self) -> std::sync::MutexGuard<'_, Wake> {
        self.0.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Record a delayed job's `scheduled_at` (ms) for the next wake.
    pub(crate) fn push(&self, scheduled_at: i64) {
        let mut pending = self.lock();
        if pending.delayed.len() < MAX_PENDING_HINTS {
            pending.delayed.push(scheduled_at);
        }
    }

    /// Record that a ready job was enqueued, so the next wake drains.
    pub(crate) fn mark_ready(&self) {
        self.lock().ready = true;
    }

    /// Everything recorded since the last take. A wake that recorded nothing
    /// (a dropped deadline, a foreign `notify_one`) drains, as before hints.
    fn take(&self) -> Wake {
        let mut heard = std::mem::take(&mut *self.lock());
        heard.ready |= heard.delayed.is_empty();
        heard
    }
}

impl WakeSource {
    /// Build the wake source that matches `storage`'s backend: SQLite shares
    /// the storage's in-process [`Notify`], Postgres and Redis each spawn their
    /// listener and take its channel. `namespace` and `queues` are what the
    /// scheduler serves: Redis subscribes to exactly those `(namespace, queue)`
    /// channels; the others ignore both.
    ///
    /// Must be called from inside a Tokio runtime context — the Postgres and
    /// Redis arms spawn a listener task. SQLite needs no runtime, but every
    /// binding shell installs wakeups through this one entry point so the
    /// backend mapping lives in a single place.
    #[cfg_attr(not(feature = "redis"), allow(unused_variables))]
    pub fn for_storage(
        storage: &StorageBackend,
        namespace: Option<&str>,
        queues: &[String],
    ) -> Self {
        match storage {
            StorageBackend::Sqlite(s) => {
                WakeSource::InProcess(s.notify_handle().clone(), s.delayed_hints().clone())
            }
            #[cfg(feature = "postgres")]
            StorageBackend::Postgres(s) => {
                WakeSource::Channel(crate::storage::postgres::listener::spawn(s.clone()))
            }
            // No queues means no channel to subscribe to (SUBSCRIBE needs one).
            #[cfg(feature = "redis")]
            StorageBackend::Redis(_) if queues.is_empty() => WakeSource::Polling,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => WakeSource::Channel(
                crate::storage::redis_backend::listener::spawn(s.clone(), namespace, queues),
            ),
        }
    }

    /// Wait for the next wake signal, then collect every signal already
    /// queued behind it so one pass sees them all.
    ///
    /// For [`WakeSource::Polling`] this never resolves on its own — the
    /// scheduler's fallback `sleep` timer drives dispatch instead, so this
    /// arm must simply yield the loop's `select!` to the timer.
    pub async fn wait(&mut self) -> Wake {
        match self {
            WakeSource::InProcess(notify, hints) => {
                notify.notified().await;
                hints.take()
            }
            WakeSource::Channel(rx) => {
                // A closed channel (listener gone) must not busy-loop the
                // select!; fall back to never-resolving so the timer drives.
                let Some(first) = rx.recv().await else {
                    return std::future::pending().await;
                };
                let now = crate::job::now_millis();
                let mut heard = Wake::default();
                heard.absorb(first, now);
                while let Ok(next) = rx.try_recv() {
                    heard.absorb(next, now);
                }
                heard
            }
            WakeSource::Polling => std::future::pending().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::sqlite::SqliteStorage;

    /// The SQLite source must be the *same* handle the storage notifies on. A
    /// fresh `Notify` here would compile and run, but the loop would then wait
    /// on something no enqueue ever signals.
    #[test]
    fn sqlite_source_shares_the_storage_notify_handle() {
        let sqlite = SqliteStorage::in_memory().unwrap();
        let notify = sqlite.notify_handle().clone();
        match WakeSource::for_storage(&StorageBackend::Sqlite(sqlite), None, &[]) {
            WakeSource::InProcess(handle, _) => assert!(Arc::ptr_eq(&handle, &notify)),
            _ => panic!("sqlite must wake in-process"),
        }
    }

    /// Every queued message is folded into one wake: a plain or due one
    /// drains, future deadlines are kept for the timer, and nothing queued
    /// behind the first message is lost.
    #[tokio::test]
    async fn channel_wait_collects_every_queued_signal() {
        let (tx, rx) = mpsc::channel(8);
        let mut source = WakeSource::Channel(rx);
        let later = crate::job::now_millis() + 60_000;
        tx.send(Some(later)).await.unwrap();
        tx.send(Some(later + 1)).await.unwrap();
        assert_eq!(
            source.wait().await,
            Wake {
                ready: false,
                delayed: vec![later, later + 1],
            },
            "future-only announcements arm timers without a drain"
        );

        tx.send(Some(1)).await.unwrap();
        tx.send(None).await.unwrap();
        assert_eq!(
            source.wait().await,
            Wake {
                ready: true,
                delayed: vec![],
            }
        );
    }
}
