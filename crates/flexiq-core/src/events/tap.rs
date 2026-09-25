//! [`EventTap`]: an in-process observer of every event a hub is handed.

use super::event::JobEvent;

/// Sees every event the hub is handed, before any sink's filter.
///
/// For a consumer in the same process — a server's watch streams — that wants
/// the transitions the scheduler and the doors already announce, without a
/// sink, a buffer thread, or a second emit site.
pub trait EventTap: Send + Sync {
    /// Called on the emitting thread. Must not block or do I/O: the scheduler's
    /// dispatch and settle paths wait on it. The payload is present only when
    /// the emitter held it for a sink that asked.
    fn observe(&self, event: &JobEvent);
}
