//! What the attribute macro implements.

use flexiq_core::{Job, TaskConfig};

use crate::{EnqueueOptions, Outcome};

/// A registered task: its name, its dispatch policy, its enqueue defaults, and
/// how to run one encoded job.
///
/// Implemented by `#[flexiq::task]`. Implementing it by hand is supported and is
/// what this crate's own tests do — the macro is ergonomics, not plumbing, and
/// a task with an unusual shape can skip it.
pub trait Task: Send + Sync + 'static {
    /// The name a job carries, and the name a producer in any language
    /// enqueues.
    const NAME: &'static str;

    /// Dispatch policy: retries, rate limits, breaker, concurrency caps.
    ///
    /// Everything here is read by the scheduler, never by the job row. The
    /// split is core's: [`TaskConfig`] carries no `queue`, `priority` or
    /// `timeout`, because those are properties of a job rather than of a task.
    fn config() -> TaskConfig;

    /// The enqueue defaults this task was declared with, seeded into every
    /// [`crate::TaskCall`] before a caller's per-call overrides.
    fn defaults() -> EnqueueOptions;

    /// Decode the job's payload, run the body, encode the result.
    ///
    /// The `Option<Vec<u8>>` is the archived result: `None` when the body
    /// returned `()`, so a unit task stores nothing rather than storing an
    /// encoded nothing.
    fn run_encoded(job: &Job, step: &mut StepHandle) -> Outcome<Option<Vec<u8>>>;
}

/// The durable-step handle a running task holds.
///
/// Reached through [`crate::current_step`] rather than a parameter, because a
/// macro-expanded body is the caller's own function and cannot grow an argument
/// it did not declare.
pub struct StepHandle {
    // Temporary: written by nothing until the dispatcher can open a session.
    // Remove with the one in `detached` when `pool.rs` lands.
    #[allow(dead_code)]
    pub(crate) inner: Option<crate::steps::Session>,
}

impl StepHandle {
    /// A handle with no session behind it.
    ///
    /// What [`crate::current_step`] returns outside a running task. Every
    /// method on it fails rather than panicking: a panic here would take down a
    /// pool thread over a caller's mistake in their own code.
    #[allow(dead_code)]
    pub(crate) fn detached() -> Self {
        Self { inner: None }
    }
}
