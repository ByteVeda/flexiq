//! One shutdown signal shared by the async HTTP server and the blocking
//! listener threads.
//!
//! The flag is what a blocking accept loop polls between non-blocking accepts;
//! the notify is what the axum graceful-shutdown future awaits. Both are
//! driven by the same `trigger`, so SIGTERM stops every part of the process.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::Notify;

/// A cloneable handle to the process-wide shutdown signal.
#[derive(Clone, Default)]
pub struct Shutdown {
    triggered: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl Shutdown {
    /// Signal shutdown. Idempotent — a second SIGTERM changes nothing.
    pub fn trigger(&self) {
        self.triggered.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Whether shutdown has been signalled.
    pub fn is_triggered(&self) -> bool {
        self.triggered.load(Ordering::SeqCst)
    }

    /// Resolve once shutdown is signalled, immediately if it already was.
    pub async fn wait(&self) {
        // Subscribe before the check: `notify_waiters` only wakes waiters that
        // already registered, so testing first would lose a trigger landing in
        // the check-to-await window.
        let notified = self.notify.notified();
        if self.is_triggered() {
            return;
        }
        notified.await;
    }
}

/// Resolve on SIGINT or SIGTERM. SIGTERM is what a container runtime sends, so
/// ignoring it would mean every deploy ends in SIGKILL with jobs mid-flight.
pub async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut terminate = match signal(SignalKind::terminate()) {
            Ok(stream) => stream,
            Err(error) => {
                log::warn!("cannot listen for SIGTERM ({error}); falling back to SIGINT only");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn waiting_after_a_trigger_returns_immediately() {
        let shutdown = Shutdown::default();
        shutdown.trigger();
        shutdown.wait().await;
        assert!(shutdown.is_triggered());
    }

    #[tokio::test]
    async fn a_waiter_is_woken_by_a_later_trigger() {
        let shutdown = Shutdown::default();
        let waiter = tokio::spawn({
            let shutdown = shutdown.clone();
            async move { shutdown.wait().await }
        });
        // Yield so the waiter registers before the trigger fires.
        tokio::task::yield_now().await;
        shutdown.trigger();
        waiter.await.expect("waiter task");
    }
}
