//! Postgres `LISTEN`-based wake listener for push-dispatch.
//!
//! Entirely behind the `push-dispatch` feature.
//!
//! ## Status: conservative stub
//!
//! True event-driven `LISTEN`/`NOTIFY` requires retrieving libpq notifications
//! (`PQnotifies`) from the underlying connection. Diesel's safe `PgConnection`
//! API does not expose the raw libpq handle or a notification-poll primitive,
//! so a correct, non-`unsafe` `LISTEN` loop cannot be built on top of Diesel
//! alone here. Rather than ship a broken or `unsafe` FFI path, this listener
//! forwards a periodic tick: push-dispatch on Postgres therefore degrades to a
//! faster, bounded fallback poll (driven by [`LISTEN_POLL_INTERVAL`]) instead
//! of instantaneous wakeups. The enqueue side still issues `pg_notify` (see
//! [`super::JOB_READY_CHANNEL`]) so a future libpq-backed listener can become
//! fully event-driven without any enqueue-path change.
//!
//! The default (feature-off) build never compiles this module.

use std::time::Duration;

use tokio::sync::mpsc;

use super::PostgresStorage;

/// Interval at which the stub listener forwards a wake while a true
/// `LISTEN`/`NOTIFY` retrieval path is unavailable.
pub const LISTEN_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Spawn the Postgres wake listener and return the receiver end for the
/// scheduler's [`crate::scheduler::wake::WakeSource::Channel`].
///
/// The `_storage` handle is retained for the eventual libpq-backed
/// implementation (it carries [`PostgresStorage::database_url`]); the current
/// stub does not open a dedicated connection.
pub fn spawn(_storage: PostgresStorage) -> mpsc::Receiver<()> {
    let (tx, rx) = mpsc::channel(1);
    tokio::task::spawn_blocking(move || loop {
        std::thread::sleep(LISTEN_POLL_INTERVAL);
        // A full channel already has a pending wake — dropping this one is
        // fine. A closed channel means the scheduler is gone; stop.
        match tx.try_send(()) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {}
            Err(mpsc::error::TrySendError::Closed(_)) => break,
        }
    });
    rx
}
