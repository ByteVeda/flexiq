package executor

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	executorv1 "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/executor/v1"
)

// outcome is everything the settling frame is built from.
type outcome struct {
	value     any
	err       error
	timedOut  bool
	cancelled bool
	wall      time.Duration
	// slept is the deadline a durable sleep committed, and latched is the last
	// step refusal this attempt saw. Both are read once the handler has
	// returned, because a Go task body can ignore an error and three of these
	// outcomes are not the body's to decide: an attempt that slept is over, one
	// that was superseded must write nothing at all, and one that diverged
	// cannot write into a sequence that no longer lines up. Every other refusal
	// is the body's, and it is taken at its word.
	slept   *time.Time
	latched *StepError
}

// settle turns a handler's return into exactly one settling frame, or into no
// frame at all for the one case that must send none.
//
// Otherwise exactly one, always: a dispatched job the scheduler never hears
// about again is a slot it believes is busy until the reaper notices. The lease
// is not stamped here — session.send does that, so there is no path that builds
// one of these and forgets.
func settle(job *Job, o outcome) *executorv1.AttachRequest {
	wall := durationpb.New(o.wall)

	switch {
	// Another attempt owns this job now, so this one must stop without writing.
	// Not a failure and not a success: a frame either way would be this attempt
	// writing over the one that replaced it.
	case errors.Is(o.latchedErr(), ErrStepSuperseded):
		return nil

	// The attempt ended in a durable sleep: the row is committed, the claim is
	// released and the job is already scheduled to wake. It outranks whatever
	// the body returned on its way out, including a body that swallowed the
	// signal — the claim is gone either way.
	case o.slept != nil:
		return sleptFrame(job, *o.slept, wall)

	// A divergence the body returned normally past. It is the one refusal that
	// is not the body's to handle: the deployed code and the recorded rows
	// disagree, so an attempt that carried on wrote into a sequence that no
	// longer lines up. Every other refusal is taken at the body's word.
	//
	// The other SDKs put their control signals in an exception tier `catch`
	// cannot reach. Go has no such tier, so the check happens here.
	case o.err == nil && errors.Is(o.latchedErr(), ErrStepDiverged):
		return failureFrame(job, failure{
			error: flexiq.EncodeTaskError("StepDivergedError",
				o.latched.Error()+" (the task body returned successfully past this)", nil),
			// Permanent. The next attempt reads the same rows and runs the same
			// code, and would diverge identically.
			shouldRetry: false,
			wall:        wall,
		})

	case o.err == nil:
		result, err := encodeResult(o.value)
		if err != nil {
			return failureFrame(job, failure{
				error: flexiq.EncodeTaskError("ResultEncodeError",
					fmt.Sprintf("task %q returned a value that does not encode: %v", job.TaskName, err), nil),
				// Fatal, not retryable. The handler ran and its side effects
				// happened; only the value cannot be written, and the next
				// attempt would run them again and fail on the same type.
				// Recording a success with an empty result instead would be a
				// different answer from the one the task gave, and one no
				// reader can decode.
				shouldRetry: false,
				wall:        wall,
			})
		}
		return &executorv1.AttachRequest{
			Frame: &executorv1.AttachRequest_Success{Success: &executorv1.SuccessFrame{
				JobId:    job.ID,
				TaskName: job.TaskName,
				Result:   result,
				WallTime: wall,
			}},
		}

	// A deadline that fired outranks whatever the handler returned on its way
	// out: the attempt ran out of time, and that is what the scheduler's own
	// reaper would have recorded had the handler never returned at all.
	case o.timedOut:
		return failureFrame(job, failure{
			error:       flexiq.EncodeTaskError("TimeoutError", timeoutMessage(job.Timeout), nil),
			shouldRetry: true,
			timedOut:    true,
			wall:        wall,
		})

	case o.cancelled && isCancellation(o.err):
		return &executorv1.AttachRequest{
			Frame: &executorv1.AttachRequest_Cancelled{Cancelled: &executorv1.CancelledFrame{
				JobId:    job.ID,
				TaskName: job.TaskName,
				WallTime: wall,
			}},
		}

	default:
		errType, message, traceback := describe(o.err)
		return failureFrame(job, failure{
			error:       flexiq.EncodeTaskError(errType, message, traceback),
			shouldRetry: !errors.Is(o.err, ErrFatal),
			timedOut:    false,
			wall:        wall,
		})
	}
}

// latchedErr is the latched refusal as an error, and nil when there is none.
//
// Spelled out rather than passed straight to errors.Is: a nil *StepError in an
// error interface is not a nil error, and unwrapping one panics.
func (o outcome) latchedErr() error {
	if o.latched == nil {
		return nil
	}
	return o.latched
}

type failure struct {
	error       string
	shouldRetry bool
	timedOut    bool
	wall        *durationpb.Duration
}

func failureFrame(job *Job, f failure) *executorv1.AttachRequest {
	return &executorv1.AttachRequest{
		Frame: &executorv1.AttachRequest_Failure{Failure: &executorv1.FailureFrame{
			JobId:       job.ID,
			TaskName:    job.TaskName,
			Error:       f.error,
			RetryCount:  int32(job.RetryCount),
			MaxRetries:  int32(job.MaxRetries),
			WallTime:    f.wall,
			ShouldRetry: f.shouldRetry,
			TimedOut:    f.timedOut,
		}},
	}
}

// encodeResult keeps "returned nothing" and "returned an empty value" apart.
// They are different answers, and the frame has a way to say each: nil for the
// first, a non-nil slice for the second.
//
// A value that does not encode is an error for the caller to settle on, not
// something to paper over — see the fatal branch in settle.
func encodeResult(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	return flexiq.EncodeResult(value)
}

// isCancellation reports whether the handler stopped because it was asked to.
//
// A handler that ignores its cancel and finishes normally settles normally:
// cancellation is cooperative, and nothing here can stop a running goroutine.
func isCancellation(err error) bool {
	return errors.Is(err, ErrCancelled) || errors.Is(err, context.Canceled)
}

func timeoutMessage(timeout time.Duration) string {
	// The reaper's own wording, so a timeout the executor reported and one the
	// scheduler synthesized read identically in the same list.
	return fmt.Sprintf("job timed out after %dms", timeout.Milliseconds())
}

// describe renders a Go error as the three parts of the canonical failure JSON.
//
// The errtype is the error's own type name where it has a useful one. The
// standard library's wrappers do not — *errors.errorString says nothing a
// reader can act on — so those become the neutral "Error" rather than leaking
// an implementation detail into a field other runtimes match on.
func describe(err error) (errType, message string, traceback []string) {
	var panicked *panicError
	if errors.As(err, &panicked) {
		return "panic", panicked.Error(), panicked.traceback
	}

	// Unwrap Fatal so the failure names the error the task actually raised.
	var fatal fatalError
	if errors.As(err, &fatal) {
		err = fatal.err
	}

	return typeName(err), err.Error(), nil
}

func typeName(err error) string {
	typ := reflect.TypeOf(err)
	if typ == nil {
		return "Error"
	}
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	name := typ.Name()
	if name == "" || strings.HasPrefix(typ.PkgPath(), "errors") || strings.HasPrefix(typ.PkgPath(), "fmt") {
		return "Error"
	}
	return name
}

// panicError is a recovered panic, carried as an error so it settles through
// the same path as any other failure.
type panicError struct {
	value     any
	traceback []string
}

func (e *panicError) Error() string { return fmt.Sprintf("panic: %v", e.value) }

func recovered(value any, stack []byte) *panicError {
	lines := strings.Split(strings.TrimRight(string(stack), "\n"), "\n")
	return &panicError{value: value, traceback: lines}
}

// capture is called from a deferred recover in the job runner.
func capture(value any) *panicError { return recovered(value, debug.Stack()) }
