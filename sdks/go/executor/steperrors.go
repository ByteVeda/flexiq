package executor

import (
	"errors"
	"fmt"

	executorv1 "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/executor/v1"
)

// The verdicts a refused step carries, matched with [errors.Is].
//
// A refusal is classified by the side that holds storage, because only it can
// see the error — so the verdict crosses the wire as an enum and the message
// crosses as prose. Never read the verdict back out of the message.
var (
	// ErrStepRetryable means the backend failed, not the request. The attempt
	// fails and the job's retry policy has it. A commit that was never
	// acknowledged is one of these: nothing confirmed the write landed, so a
	// replay is safe.
	ErrStepRetryable = errors.New("flexiq: the step commit failed and the attempt should retry")
	// ErrStepPermanent means the commit will never succeed — a divergence, a
	// cap, a bad encoding. Retrying only wastes an attempt, so it dead-letters.
	ErrStepPermanent = errors.New("flexiq: the step commit will never succeed")
	// ErrStepSuperseded means another attempt owns this job now. This one must
	// stop **without writing**: no result frame is sent at all, because the job
	// is proceeding correctly somewhere else.
	ErrStepSuperseded = errors.New("flexiq: another attempt owns this job")
	// ErrStepUnavailable means the scheduler offers no step store. Retryable, so
	// a fleet mid-rollout can still place the next attempt somewhere that
	// commits. It is never a silent un-memoized run — there is no version of
	// "your charge step lost its memo" that beats a failure naming the reason.
	ErrStepUnavailable = errors.New("flexiq: the scheduler offers no step store")
)

// ErrStepSlept ends the attempt in a durable sleep.
//
// Returned by [Job.Sleep] and [Job.SleepUntil] once the deadline is committed:
// the row is written, the claim is released and the job is already scheduled to
// wake. Propagate it. Anything the task body does past this point runs
// unclaimed and will run again when the job wakes.
//
// Swallowing it does not resume the attempt — the claim is gone either way, and
// this client writes the slept frame regardless.
var ErrStepSlept = errors.New("flexiq: the attempt ended in a durable sleep")

// ErrStepSwallowed is settled when a step failed and the task body returned
// successfully anyway.
//
// A Go task can ignore an error, and a step that did not commit is a memo that
// is not there: recording a success would record one for an attempt whose
// sequence has a hole in it. The original verdict is preserved, so a swallowed
// permanent failure still dead-letters and a swallowed retryable one still
// retries.
var ErrStepSwallowed = errors.New("flexiq: a step failure was swallowed by the task body")

// StepError is a step that could not be committed.
//
// Test it with [errors.Is] against one of the verdicts above. A permanent one
// also answers to [ErrFatal], so a task body that simply returns it settles the
// job the way the verdict says without the caller having to translate.
type StepError struct {
	// JobID is the attempt the step belongs to.
	JobID string
	// StepKey is the step's identity, "name#occurrence" or "name:key". Empty
	// when the failure happened before one could be derived.
	StepKey string
	// Message is the reason, in whichever side's own words saw it.
	Message string

	causes []error
}

// Error renders the refusal, naming the step where there is one.
func (e *StepError) Error() string {
	if e.StepKey == "" {
		return fmt.Sprintf("flexiq: step failed on job %s: %s", e.JobID, e.Message)
	}
	return fmt.Sprintf("flexiq: step %q of job %s failed: %s", e.StepKey, e.JobID, e.Message)
}

// Unwrap reports every sentinel this refusal answers to: its verdict, plus the
// narrower tag where there is one.
func (e *StepError) Unwrap() []error { return e.causes }

// retryableStep is the verdict for anything that did not confirm a write.
func retryableStep(jobID, stepKey, message string) *StepError {
	return &StepError{JobID: jobID, StepKey: stepKey, Message: message, causes: []error{ErrStepRetryable}}
}

// permanentStep also answers to ErrFatal: the commit will never succeed, and a
// task body that returns one dead-letters the job rather than burning its
// retries on a sequence that cannot line up.
func permanentStep(jobID, stepKey, message string) *StepError {
	return &StepError{
		JobID: jobID, StepKey: stepKey, Message: message,
		causes: []error{ErrStepPermanent, ErrFatal},
	}
}

func supersededStep(jobID, stepKey, message string) *StepError {
	return &StepError{JobID: jobID, StepKey: stepKey, Message: message, causes: []error{ErrStepSuperseded}}
}

func unavailableStep(jobID string) *StepError {
	return &StepError{
		JobID: jobID,
		Message: "this job uses durable steps, but the scheduler it is attached to " +
			"offers no step store",
		causes: []error{ErrStepUnavailable, ErrStepRetryable},
	}
}

// swallowed re-raises a latched refusal the task body returned past.
func swallowed(cause *StepError) *StepError {
	return &StepError{
		JobID:   cause.JobID,
		StepKey: cause.StepKey,
		Message: cause.Message + " (the task body returned successfully past this failure)",
		causes:  append([]error{ErrStepSwallowed}, cause.causes...),
	}
}

// refusalFor rebuilds the verdict an ack carried.
//
// From the enum, never the message. An unrecognised verdict reads as retryable:
// nothing confirmed the write landed, so a replay is the safe reading, and a
// scheduler that grows a fourth verdict must not turn into a dead letter here.
func refusalFor(jobID, stepKey string, ack *executorv1.StepAckFrame) *StepError {
	message := ack.GetError()
	if message == "" {
		message = "the scheduler refused the commit without saying why"
	}
	switch ack.GetFailure() {
	case executorv1.StepFailure_STEP_FAILURE_PERMANENT:
		return permanentStep(jobID, stepKey, message)
	case executorv1.StepFailure_STEP_FAILURE_SUPERSEDED:
		return supersededStep(jobID, stepKey, message)
	default:
		return retryableStep(jobID, stepKey, message)
	}
}

// localStepError classifies a refusal the rules made before the wire.
//
// Every one of them is permanent: a divergence, a duplicate key, an over-cap
// result and a malformed name all fail identically on the next attempt. The one
// exception is a snapshot that would not decode, which is a fact about this
// dispatch rather than about the code — see snapshotStepError.
func localStepError(jobID, stepKey string, err error) *StepError {
	return permanentStep(jobID, stepKey, err.Error())
}

// snapshotStepError is the one local refusal that is *not* permanent.
//
// A snapshot that did not decode says nothing about whether the code and the
// recorded sequence agree — only that this dispatch arrived damaged. The next
// one may not. It must never read as "no steps recorded": that answer re-runs
// a charge.
func snapshotStepError(jobID string, err error) *StepError {
	return retryableStep(jobID, "", err.Error())
}
