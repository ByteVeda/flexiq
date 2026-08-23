//! Shared rules for durable inline steps.
//!
//! Everything a step is allowed to be, in one place, so the storage backends,
//! the workflow crate and every SDK shell answer the question the same way.
//! The rules — the limits, the failure taxonomy, key derivation and the
//! sequence check — are pure and I/O-free.

mod failure;
mod key;
mod limits;
mod sequence;
mod session;

pub use failure::{classify_step_failure, StepFailure};
pub use key::StepKey;
pub use limits::{
    StepLimits, DEFAULT_MAX_STEPS, DEFAULT_MAX_STEP_BYTES, DEFAULT_MAX_TOTAL_BYTES,
    MAX_STEPS_CEILING, MAX_STEP_BYTES_CEILING, MAX_TOTAL_BYTES_CEILING,
};
pub use sequence::{PendingStep, SleepDecision, StepDecision, StepSequence, DEFAULT_SLEEP_NAME};
pub use session::{StepSession, StepSleep};
