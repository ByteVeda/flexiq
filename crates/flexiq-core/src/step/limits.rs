//! The caps a job's committed steps are held to.

/// Caps a job's committed steps are held to.
///
/// Enforced twice on purpose: in the shell, where the error can name the value
/// the caller passed, and again in `record_step_result`, which is the check that
/// holds when a shell forgets. Both measure the *encoded* bytes — post
/// serializer, post codec — because that is what is stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StepLimits {
    /// Largest encoded result one step may commit.
    pub max_step_bytes: usize,
    /// Largest total across every committed step of one job. The snapshot is
    /// loaded whole at attempt start, which the per-step cap alone does not
    /// bound once a loop runs ten thousand times.
    pub max_total_bytes: usize,
    /// Most steps one job may commit. A loop of cheap steps returning nothing
    /// slips past a byte cap.
    pub max_steps: usize,
}

/// Default per-step cap: one checkpoint, not a data payload.
pub const DEFAULT_MAX_STEP_BYTES: usize = 256 * 1024;
/// Default per-job cap across every committed step.
pub const DEFAULT_MAX_TOTAL_BYTES: usize = 4 * 1024 * 1024;
/// Default cap on how many steps one job may commit.
pub const DEFAULT_MAX_STEPS: usize = 1_000;

/// Hard ceiling on [`StepLimits::max_step_bytes`], whatever a caller configures.
/// Above this the answer is not a bigger cap — it is storing the value elsewhere
/// and memoizing the handle.
pub const MAX_STEP_BYTES_CEILING: usize = 1024 * 1024;
/// Hard ceiling on [`StepLimits::max_total_bytes`].
pub const MAX_TOTAL_BYTES_CEILING: usize = 64 * 1024 * 1024;
/// Hard ceiling on [`StepLimits::max_steps`].
pub const MAX_STEPS_CEILING: usize = 100_000;

impl Default for StepLimits {
    fn default() -> Self {
        Self {
            max_step_bytes: DEFAULT_MAX_STEP_BYTES,
            max_total_bytes: DEFAULT_MAX_TOTAL_BYTES,
            max_steps: DEFAULT_MAX_STEPS,
        }
    }
}

impl StepLimits {
    /// The limits with every field brought inside its hard ceiling.
    ///
    /// The storage boundary clamps what it is handed rather than trusting it: a
    /// configurable cap that a caller can raise without bound is not a cap.
    pub fn clamped(self) -> Self {
        Self {
            max_step_bytes: self.max_step_bytes.min(MAX_STEP_BYTES_CEILING),
            max_total_bytes: self.max_total_bytes.min(MAX_TOTAL_BYTES_CEILING),
            max_steps: self.max_steps.min(MAX_STEPS_CEILING),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_the_documented_limits() {
        let limits = StepLimits::default();
        assert_eq!(limits.max_step_bytes, 256 * 1024);
        assert_eq!(limits.max_total_bytes, 4 * 1024 * 1024);
        assert_eq!(limits.max_steps, 1_000);
    }

    #[test]
    fn clamping_caps_a_caller_that_asks_for_more() {
        let limits = StepLimits {
            max_step_bytes: usize::MAX,
            max_total_bytes: usize::MAX,
            max_steps: usize::MAX,
        }
        .clamped();
        assert_eq!(limits.max_step_bytes, MAX_STEP_BYTES_CEILING);
        assert_eq!(limits.max_total_bytes, MAX_TOTAL_BYTES_CEILING);
        assert_eq!(limits.max_steps, MAX_STEPS_CEILING);
    }

    #[test]
    fn clamping_leaves_a_smaller_configuration_alone() {
        let limits = StepLimits {
            max_step_bytes: 1024,
            max_total_bytes: 4096,
            max_steps: 8,
        };
        assert_eq!(limits.clamped(), limits);
    }
}
