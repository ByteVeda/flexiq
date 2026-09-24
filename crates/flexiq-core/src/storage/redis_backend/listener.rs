//! Redis pub/sub wake listener for push-dispatch.
//!
//! Entirely behind the `push-dispatch` feature. The default (feature-off)
//! build never compiles this module.
//!
//! A dedicated blocking connection `SUBSCRIBE`s to the notify channel of every
//! queue the scheduler serves; each message forwards a unit wake into the
//! scheduler's [`crate::scheduler::wake::WakeSource::Channel`]. Pub/sub is a
//! broadcast, so every scheduler serving a queue wakes — a consumed signal
//! (a list pop) would wake one, possibly one that cannot take the job. A read
//! timeout bounds each wait so the loop re-checks the forward channel and
//! stops promptly on shutdown. Connection errors back off, then reconnect and
//! resubscribe. Every successful subscribe forwards one wake, so a message
//! published while no subscription existed is drained on arrival anyway.

use std::time::Duration;

use tokio::sync::mpsc;

use super::RedisStorage;

/// Socket read deadline, and so the cadence at which an idle listener notices
/// a dropped forward channel. Also bounds a half-dead connection's read.
const READ_TIMEOUT: Duration = Duration::from_secs(1);

/// Backoff after a connection error before reconnecting.
const RECONNECT_BACKOFF: Duration = Duration::from_millis(500);

/// Connect deadline for the listener's connection. A black-holed connect would
/// otherwise block the blocking task — and so `Runtime::drop` — for the OS
/// TCP timeout (minutes).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// Spawn the Redis wake listener for `queues` and return the receiver end for
/// the scheduler's [`crate::scheduler::wake::WakeSource::Channel`].
///
/// The task ends once that receiver is dropped (the push loop returned),
/// within one read timeout; every other wait in the loop is bounded too.
pub fn spawn(storage: RedisStorage, queues: &[String]) -> mpsc::Receiver<()> {
    let (tx, rx) = mpsc::channel(1);
    let channels: Vec<String> = queues.iter().map(|q| storage.notify_channel(q)).collect();

    tokio::task::spawn_blocking(move || {
        while !tx.is_closed() {
            let mut conn = match connect(&storage) {
                Ok(c) => c,
                Err(e) => {
                    log::warn!("push-dispatch: redis listener connect failed: {e}");
                    backoff(&tx);
                    continue;
                }
            };
            let mut pubsub = conn.as_pubsub();
            if let Err(e) = pubsub.subscribe(&channels) {
                log::warn!("push-dispatch: redis SUBSCRIBE failed: {e}");
                backoff(&tx);
                continue;
            }
            // Pub/sub keeps nothing for an absent subscriber: wake once so a
            // publish before this (re)subscribe — startup or a reconnect gap —
            // is still drained now rather than at the fallback.
            let _ = tx.try_send(());

            while !tx.is_closed() {
                match pubsub.get_message() {
                    Ok(_) => match tx.try_send(()) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {}
                        Err(mpsc::error::TrySendError::Closed(_)) => return,
                    },
                    // Idle past the read deadline — just re-check shutdown.
                    Err(e) if e.is_timeout() => {}
                    Err(e) => {
                        log::warn!("push-dispatch: redis pub/sub read failed: {e}");
                        backoff(&tx);
                        break; // reconnect and resubscribe
                    }
                }
            }
        }
    });

    rx
}

/// Open the listener's dedicated connection with both deadlines set.
fn connect(storage: &RedisStorage) -> redis::RedisResult<redis::Connection> {
    let conn = storage
        .client()
        .get_connection_with_timeout(CONNECT_TIMEOUT)?;
    conn.set_read_timeout(Some(READ_TIMEOUT))?;
    Ok(conn)
}

/// Sleep out the reconnect backoff, skipping it when the push loop is already
/// gone so a failing Redis cannot stretch shutdown by a backoff.
fn backoff(tx: &mpsc::Sender<()>) {
    if !tx.is_closed() {
        std::thread::sleep(RECONNECT_BACKOFF);
    }
}
