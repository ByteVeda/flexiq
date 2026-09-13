package step

// Mirrors crates/flexiq-core/src/step/limits.rs.

// Default caps a job's committed steps are held to.
//
// The same three numbers the scheduler defaults to. Checking them here buys
// the good error message — one that can name the step and the value the caller
// passed — while the check that actually holds is the scheduler's, inside the
// write's own transaction.
const (
	// DefaultMaxStepBytes is the largest encoded result one step may commit:
	// one checkpoint, not a data payload.
	DefaultMaxStepBytes = 256 * 1024
	// DefaultMaxTotalBytes is the largest total across every committed step of
	// one job. The snapshot is loaded whole at attempt start, which a per-step
	// cap alone does not bound once a loop runs ten thousand times.
	DefaultMaxTotalBytes = 4 * 1024 * 1024
	// DefaultMaxSteps caps how many steps one job may commit. A loop of cheap
	// steps returning nothing slips past a byte cap.
	DefaultMaxSteps = 1000
)

// Hard ceilings, whatever a caller configures. Above these the answer is not a
// bigger cap — it is storing the value elsewhere and memoizing the handle.
const (
	// MaxStepBytesCeiling bounds [Limits.MaxStepBytes].
	MaxStepBytesCeiling = 1024 * 1024
	// MaxTotalBytesCeiling bounds [Limits.MaxTotalBytes].
	MaxTotalBytesCeiling = 64 * 1024 * 1024
	// MaxStepsCeiling bounds [Limits.MaxSteps].
	MaxStepsCeiling = 100_000
)

// Limits are the caps a job's committed steps are held to. All three measure
// the encoded bytes — post serializer, post codec — because that is what is
// stored.
type Limits struct {
	// MaxStepBytes is the largest encoded result one step may commit.
	MaxStepBytes int
	// MaxTotalBytes is the largest total across every committed step of a job.
	MaxTotalBytes int
	// MaxSteps is the most steps one job may commit.
	MaxSteps int
}

// DefaultLimits are the caps the scheduler itself defaults to.
func DefaultLimits() Limits {
	return Limits{
		MaxStepBytes:  DefaultMaxStepBytes,
		MaxTotalBytes: DefaultMaxTotalBytes,
		MaxSteps:      DefaultMaxSteps,
	}
}

// Clamped brings every field inside its hard ceiling, and a zero or negative
// one back to the default.
//
// A configurable cap a caller can raise without bound is not a cap; a cap a
// caller can zero by leaving the struct empty is worse, because it refuses
// every step rather than none.
func (l Limits) Clamped() Limits {
	return Limits{
		MaxStepBytes:  clamp(l.MaxStepBytes, DefaultMaxStepBytes, MaxStepBytesCeiling),
		MaxTotalBytes: clamp(l.MaxTotalBytes, DefaultMaxTotalBytes, MaxTotalBytesCeiling),
		MaxSteps:      clamp(l.MaxSteps, DefaultMaxSteps, MaxStepsCeiling),
	}
}

func clamp(value, fallback, ceiling int) int {
	if value <= 0 {
		return fallback
	}
	return min(value, ceiling)
}
