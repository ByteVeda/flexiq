//! Shared rules for durable inline steps.
//!
//! Everything a step is allowed to be, in one place, so the storage backends,
//! the workflow crate and every SDK shell answer the question the same way.
//! The rules — the limits, the failure taxonomy, key derivation, the
//! downstream idempotency key and the sequence check — are pure and I/O-free.

mod failure;
mod idempotency;
mod key;
mod limits;
mod sequence;
mod session;
mod store;

pub use failure::{classify_step_failure, StepFailure};
pub use idempotency::{idempotency_key, run_key, ORIGIN_JOB_ID_KEY};
// Written only by `retry_dead`, the one path that changes a run's job id.
pub(crate) use idempotency::stamp_origin_job_id;
pub use key::StepKey;
pub use limits::{
    StepLimits, DEFAULT_MAX_STEPS, DEFAULT_MAX_STEP_BYTES, DEFAULT_MAX_TOTAL_BYTES,
    MAX_STEPS_CEILING, MAX_STEP_BYTES_CEILING, MAX_TOTAL_BYTES_CEILING,
};
pub use sequence::{PendingStep, SleepDecision, StepDecision, StepSequence, DEFAULT_SLEEP_NAME};
pub use session::{StepSession, StepSleep, StorageStepSession};
pub use store::{StepStore, StorageSteps};
