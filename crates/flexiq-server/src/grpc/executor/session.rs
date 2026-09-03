//! Which attach stream a unary call belongs to.
//!
//! `Heartbeat` is off the dispatch stream on purpose, which leaves it with
//! nothing to say *which* stream it reports on. A session token answers that,
//! and it is the scheduler's own value handed back — the same principle as the
//! lease.
//!
//! It is deliberately not the executor id. An id is a name the executor chose,
//! so one authenticated peer could shrink another's advertised capacity simply
//! by claiming it. The token is 16 random bytes the scheduler mints at attach
//! and returns in the response's `flexiq-attach-session-bin` metadata; knowing
//! one is indistinguishable from having been handed one.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use flexiq_core::worker::FrameEndpoint;

/// Bytes in a session token. 128 bits of randomness, so a peer cannot find
/// another's by trying.
const SESSION_BYTES: usize = 16;

/// The live attach streams, by session token.
#[derive(Default)]
pub struct SessionRegistry {
    sessions: Mutex<HashMap<Vec<u8>, Arc<FrameEndpoint>>>,
}

impl SessionRegistry {
    /// Mint a token for `endpoint` and register it.
    pub fn open(&self, endpoint: Arc<FrameEndpoint>) -> Vec<u8> {
        let token: Vec<u8> = (0..SESSION_BYTES).map(|_| rand::random::<u8>()).collect();
        self.sessions
            .lock()
            .unwrap_or_else(recover)
            .insert(token.clone(), endpoint);
        token
    }

    /// The stream a token names, if it is still attached.
    pub fn get(&self, token: &[u8]) -> Option<Arc<FrameEndpoint>> {
        self.sessions
            .lock()
            .unwrap_or_else(recover)
            .get(token)
            .cloned()
    }

    /// Forget a token. Called when its stream ends, so the map cannot grow for
    /// the life of the process across a reconnect loop.
    pub fn close(&self, token: &[u8]) {
        self.sessions.lock().unwrap_or_else(recover).remove(token);
    }

    /// How many streams are registered. For tests and for a leak check.
    pub fn len(&self) -> usize {
        self.sessions.lock().unwrap_or_else(recover).len()
    }

    /// Whether any stream is registered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn recover<T>(poisoned: std::sync::PoisonError<T>) -> T {
    poisoned.into_inner()
}

#[cfg(test)]
mod tests {
    use super::*;
    use flexiq_core::worker::FrameTransport;

    fn endpoint() -> Arc<FrameEndpoint> {
        let (transport, endpoint) = FrameTransport::new("grpc:test", true);
        // The transport is dropped: nothing here drives an attach, and the
        // registry only ever holds the endpoint.
        drop(transport);
        Arc::new(endpoint)
    }

    #[test]
    fn a_token_finds_its_own_stream_and_no_other() {
        let registry = SessionRegistry::default();
        let first = registry.open(endpoint());
        let second = registry.open(endpoint());

        assert_ne!(first, second, "two streams must not share a token");
        assert!(registry.get(&first).is_some());
        assert!(registry.get(&second).is_some());
        assert!(
            registry.get(b"guessed").is_none(),
            "a token nobody was handed names nothing"
        );
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn a_closed_stream_leaves_nothing_behind() {
        let registry = SessionRegistry::default();
        let token = registry.open(endpoint());
        registry.close(&token);

        assert!(registry.get(&token).is_none());
        assert!(
            registry.is_empty(),
            "a reconnect loop must not grow the map"
        );
        // Idempotent: the stream's end and an error path may both close it.
        registry.close(&token);
    }

    #[test]
    fn a_token_is_long_enough_not_to_be_found_by_trying() {
        let registry = SessionRegistry::default();
        assert_eq!(registry.open(endpoint()).len(), SESSION_BYTES);
    }
}
