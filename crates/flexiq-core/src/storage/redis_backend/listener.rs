//! Redis `BLPOP`-based wake listener for push-dispatch.
//!
//! Entirely behind the `push-dispatch` feature. The default (feature-off)
//! build never compiles this module.
//!
//! A dedicated blocking connection loops on `BLPOP <notify-key> 1`. The 1s
//! timeout keeps shutdown responsive (the loop re-checks the forward channel
//! between blocks). Each popped sentinel forwards a unit wake into the
//! scheduler's [`crate::scheduler::wake::WakeSource::Channel`]. Connection
//! errors back off before reconnecting.

use std::time::Duration;

use tokio::sync::mpsc;

use super::RedisStorage;

/// `BLPOP` block timeout. Short enough to notice a dropped forward channel and
/// stop promptly on shutdown.
const BLPOP_TIMEOUT_SECS: f64 = 1.0;

/// Backoff after a connection error before reconnecting.
const RECONNECT_BACKOFF: Duration = Duration::from_millis(500);

/// Connect deadline for the listener's connection. A black-holed connect would
/// otherwise block the blocking task — and so `Runtime::drop` — for the OS
/// TCP timeout (minutes).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// Socket read deadline: the `BLPOP` block plus grace for a slow reply. Without
/// it a half-dead connection blocks `BLPOP` forever and shutdown never returns.
const READ_TIMEOUT: Duration = Duration::from_secs(3);

/// Spawn the Redis wake listener and return the receiver end for the
/// scheduler's [`crate::scheduler::wake::WakeSource::Channel`].
///
/// The task ends once that receiver is dropped (the push loop returned),
/// within one `BLPOP` block; every other wait in the loop is bounded too, so
/// shutdown never waits on Redis for longer than `READ_TIMEOUT`.
pub fn spawn(storage: RedisStorage) -> mpsc::Receiver<()> {
    let (tx, rx) = mpsc::channel(1);
    let key = storage.notify_key();

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

            while !tx.is_closed() {
                let popped: redis::RedisResult<Option<(String, i64)>> = redis::cmd("BLPOP")
                    .arg(&key)
                    .arg(BLPOP_TIMEOUT_SECS)
                    .query(&mut conn);

                match popped {
                    // Timed out with no element — just re-check shutdown.
                    Ok(None) => {}
                    Ok(Some(_)) => match tx.try_send(()) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {}
                        Err(mpsc::error::TrySendError::Closed(_)) => return,
                    },
                    Err(e) => {
                        log::warn!("push-dispatch: redis BLPOP failed: {e}");
                        backoff(&tx);
                        break; // reconnect
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
