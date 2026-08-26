//! Whether a queue's backend can memoize a step at all.
//!
//! Opening a session is a *worker's* business, not a queue's — the fence is
//! `(owner, attempt)`, and the owner is the id one worker claims execution
//! under. See [`JsWorker::open_step_session`](crate::JsWorker).

use flexiq_core::storage::Storage;
use napi_derive::napi;

use super::JsQueue;

#[napi]
impl JsQueue {
    /// Whether this backend implements a step store at all.
    ///
    /// Exposed so the shell can answer "can a task here use steps" without
    /// opening a session, which costs a job read. A backend that answers `false`
    /// refuses every step rather than degrading to "no steps recorded" — that
    /// answer re-runs a charge.
    #[napi]
    pub fn supports_steps(&self) -> bool {
        self.storage.supports_steps()
    }
}
