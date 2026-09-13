package executor

import "github.com/ByteVeda/flexiq/sdks/go/v2/internal/step"

// Caps a job's committed steps are held to, by default.
//
// The scheduler's own numbers, copied rather than invented. Checking them here
// buys the good error message — one that names the step and the value the
// caller passed — while the check that actually holds is the scheduler's,
// inside the write's own transaction. Configure them only to match a scheduler
// configured away from these.
const (
	// DefaultMaxStepBytes is the largest encoded result one step may commit: a
	// checkpoint, not a data payload.
	DefaultMaxStepBytes = step.DefaultMaxStepBytes
	// DefaultMaxTotalBytes is the largest total across every committed step of
	// one job. The snapshot is loaded whole at attempt start, which a per-step
	// cap alone does not bound once a loop runs ten thousand times.
	DefaultMaxTotalBytes = step.DefaultMaxTotalBytes
	// DefaultMaxSteps caps how many steps one job may commit. A loop of cheap
	// steps returning nothing slips past a byte cap.
	DefaultMaxSteps = step.DefaultMaxSteps
)

// StepLimits are the caps this client refuses a step commit against before the
// round trip.
//
// A zero field takes its default; anything above the scheduler's hard ceiling
// is brought back to it, because a cap a caller can raise without bound is not
// a cap.
type StepLimits struct {
	// MaxStepBytes is the largest encoded result one step may commit.
	MaxStepBytes int
	// MaxTotalBytes is the largest total across every committed step of a job.
	MaxTotalBytes int
	// MaxSteps is the most steps one job may commit.
	MaxSteps int
}

// DefaultStepLimits are the caps a worker uses when none are configured.
func DefaultStepLimits() StepLimits {
	limits := step.DefaultLimits()
	return StepLimits{
		MaxStepBytes:  limits.MaxStepBytes,
		MaxTotalBytes: limits.MaxTotalBytes,
		MaxSteps:      limits.MaxSteps,
	}
}

func (l StepLimits) internal() step.Limits {
	return step.Limits{
		MaxStepBytes:  l.MaxStepBytes,
		MaxTotalBytes: l.MaxTotalBytes,
		MaxSteps:      l.MaxSteps,
	}.Clamped()
}
