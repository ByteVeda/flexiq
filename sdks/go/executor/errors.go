package executor

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrNoToken is returned by [New] when no credential was supplied. There is no
// anonymous path on this door.
var ErrNoToken = errors.New("flexiq: no token: every call to this door carries one, use WithToken")

// ErrFatal marks a task failure the scheduler must not retry.
//
// Match it with [errors.Is]; produce it with [Fatal]. Retrying is the default
// because most failures are transient and the scheduler never inspects an
// error — only the executor can see the exception, so only the executor can say
// that running the task again would fail the same way.
var ErrFatal = errors.New("flexiq: fatal task error")

// ErrCancelled settles a job as cancelled rather than failed.
//
// A handler returns it, or its context's error, after observing a cancel. A
// handler that ignores the cancel and finishes normally settles normally —
// cancellation is cooperative, and nothing here stops a running goroutine.
var ErrCancelled = errors.New("flexiq: job cancelled")

// Fatal wraps an error so the job dead-letters instead of retrying.
//
//	return nil, executor.Fatal(fmt.Errorf("no such customer: %q", id))
//
// The wrapped error keeps its type and message: the type becomes the errtype in
// the canonical failure JSON, and [errors.Is] and [errors.As] still reach it.
func Fatal(err error) error {
	if err == nil {
		return ErrFatal
	}
	return fatalError{err: err}
}

type fatalError struct{ err error }

func (e fatalError) Error() string { return e.err.Error() }

// Unwrap returns the wrapped error, so errors.As reaches the original type.
func (e fatalError) Unwrap() error { return e.err }

// Is answers for ErrFatal as well as anything the wrapped error answers for,
// so a caller can test either.
func (e fatalError) Is(target error) bool { return target == ErrFatal }

// AttachError is a refused attach.
//
// Some refusals are worth retrying and some are not, and the difference is the
// status code rather than the message. [Worker.Run] returns a permanent one and
// reconnects through the rest.
type AttachError struct {
	// Code is the gRPC status code the scheduler refused with.
	Code codes.Code
	// Message is the scheduler's own text, written for humans.
	Message string
	// Permanent reports whether reconnecting could ever succeed.
	Permanent bool
}

// Error renders the refusal.
func (e *AttachError) Error() string {
	return fmt.Sprintf("flexiq: attach refused (%s): %s", e.Code, e.Message)
}

// GRPCStatus keeps status.Code and status.Convert working on this error.
func (e *AttachError) GRPCStatus() *status.Status {
	return status.New(e.Code, e.Message)
}

// attachError classifies a failed Attach RPC.
//
// The four permanent codes each name a refusal that reconnecting repeats
// verbatim: another stream holds this id, the two ends do not speak the same
// frame protocol, the credential was revoked, or it carries the wrong scope. A
// client that retries those turns one misconfiguration into a hot loop against
// a server that is answering correctly.
//
// Everything else — UNAVAILABLE while the scheduler winds down, a broken
// connection, a proxy in the way — is worth another attempt.
func attachError(err error) *AttachError {
	st, _ := status.FromError(err)
	code := st.Code()
	permanent := false
	switch code {
	case codes.AlreadyExists, codes.FailedPrecondition, codes.Unauthenticated, codes.PermissionDenied:
		permanent = true
	}
	return &AttachError{Code: code, Message: st.Message(), Permanent: permanent}
}

// versionMismatch is the refusal this client raises itself, having read an
// acknowledgement whose protocol version is not the one it speaks.
//
// The scheduler sends the acknowledgement before refusing, exactly so both ends
// can log both numbers. Reading it and reporting only our own would throw away
// the half of the answer that says what to upgrade.
func versionMismatch(theirs uint32) *AttachError {
	return &AttachError{
		Code: codes.FailedPrecondition,
		Message: fmt.Sprintf(
			"protocol version mismatch: we speak %d, the scheduler speaks %d",
			ProtocolVersion, theirs,
		),
		Permanent: true,
	}
}
