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
}

// settle turns a handler's return into exactly one settling frame.
//
// Exactly one, always: a dispatched job the scheduler never hears about again
// is a slot it believes is busy until the reaper notices. The lease is not
// stamped here — session.send does that, so there is no path that builds one of
// these and forgets.
func settle(job *Job, o outcome) *executorv1.AttachRequest {
	wall := durationpb.New(o.wall)

	switch {
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
