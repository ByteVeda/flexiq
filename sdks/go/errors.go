package flexiq

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrorDomain scopes the ErrorInfo this client trusts. A detail from another
// domain belongs to a proxy or a mesh in the path, not to FlexiQ, and reading
// its reason as one of ours would branch on somebody else's vocabulary.
//
// It is exported because a caller writing its own interceptor needs the same
// test.
const ErrorDomain = "flexiq.byteveda.org"

// Reason is the machine-readable half of a failed request, and the only part
// of it a program should branch on.
//
// The gRPC code is a category — INVALID_ARGUMENT covers both a malformed
// request and a step over its limit — and the message is written for humans and
// may be reworded in any release. The reason may not be.
//
// A Reason is itself an error, so a caller branches with the standard library:
//
//	if errors.Is(err, flexiq.ReasonQueueFull) { ... }
type Reason string

// Error makes a Reason usable as an [errors.Is] target.
func (r Reason) Error() string { return string(r) }

// The closed list. A server that grows a new one sends a Reason not named here;
// treat an unrecognised reason as "not this build" and fall back to the code,
// exactly as with an unknown enum value.
const (
	ReasonUnauthenticated              Reason = "UNAUTHENTICATED"
	ReasonScopeDenied                  Reason = "SCOPE_DENIED"
	ReasonInvalidRequest               Reason = "INVALID_REQUEST"
	ReasonMalformedPayload             Reason = "MALFORMED_PAYLOAD"
	ReasonNoSuchMethod                 Reason = "NO_SUCH_METHOD"
	ReasonJobNotFound                  Reason = "JOB_NOT_FOUND"
	ReasonDependencyNotFound           Reason = "DEPENDENCY_NOT_FOUND"
	ReasonQueueFull                    Reason = "QUEUE_FULL"
	ReasonRateLimited                  Reason = "RATE_LIMITED"
	ReasonTaskNotRegistered            Reason = "TASK_NOT_REGISTERED"
	ReasonWorkflowConstructUnsupported Reason = "WORKFLOW_CONSTRUCT_UNSUPPORTED"
	ReasonContractTooOld               Reason = "CONTRACT_TOO_OLD"
	ReasonJobTimeout                   Reason = "JOB_TIMEOUT"
	ReasonClaimLost                    Reason = "CLAIM_LOST"
	ReasonStepDiverged                 Reason = "STEP_DIVERGED"
	ReasonStepLimitExceeded            Reason = "STEP_LIMIT_EXCEEDED"
	ReasonStepRefused                  Reason = "STEP_REFUSED"
	ReasonLockHeld                     Reason = "LOCK_HELD"
	ReasonSettingConflict              Reason = "SETTING_CONFLICT"
	ReasonStorageUnavailable           Reason = "STORAGE_UNAVAILABLE"
	ReasonStorageConstraint            Reason = "STORAGE_CONSTRAINT"
	ReasonServerMisconfigured          Reason = "SERVER_MISCONFIGURED"
	ReasonInternal                     Reason = "INTERNAL"
	ReasonUnknown                      Reason = "UNKNOWN"
)

// Error is a failed request. It is not a failed job: a job that raised comes
// back inside a response that succeeded, as [Job.TaskError].
type Error struct {
	// Code is the gRPC status code. A category, never sufficient on its own.
	Code codes.Code
	// Reason is the value to branch on. Empty when the server sent no
	// ErrorInfo — a transport failure, or a proxy answering on its own.
	Reason Reason
	// Message is written for humans and may be reworded in any release.
	Message string
	// Metadata carries the per-reason facts, base-10 ASCII for every numeric
	// one. Read it through the typed accessors below rather than by hand.
	Metadata map[string]string
	// RetryAfter is how long the server asked the caller to wait. Set on every
	// RESOURCE_EXHAUSTED, zero elsewhere.
	RetryAfter time.Duration

	status *status.Status
}

func (e *Error) Error() string {
	if e.Reason == "" {
		return fmt.Sprintf("flexiq: %s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("flexiq: %s (%s): %s", e.Reason, e.Code, e.Message)
}

// Unwrap exposes the reason so that errors.Is(err, ReasonQueueFull) works.
func (e *Error) Unwrap() error {
	if e.Reason == "" {
		return nil
	}
	return e.Reason
}

// GRPCStatus returns the underlying status, so this error keeps working with
// status.Code and status.FromError.
func (e *Error) GRPCStatus() *status.Status { return e.status }

// BatchIndex is the 0-based position of the failing item in an
// [Client.EnqueueBatch] request.
//
// It is present when the whole RPC failed because one item did — the shape a
// backend takes when a batch is one transaction, where returning the earlier
// items as enqueued would report jobs that do not exist.
func (e *Error) BatchIndex() (int, bool) {
	index, ok := e.MetaInt64("index")
	return int(index), ok
}

// Meta reads one metadata value.
func (e *Error) Meta(key string) (string, bool) {
	value, ok := e.Metadata[key]
	return value, ok
}

// MetaInt64 reads a numeric metadata value.
//
// A value that will not parse is reported absent rather than as a failure. It
// is a server bug, and the code and the reason already carry the decision — a
// client that failed the whole response over one unreadable number would lose
// the part of the answer that was fine.
func (e *Error) MetaInt64(key string) (int64, bool) {
	raw, ok := e.Metadata[key]
	if !ok {
		return 0, false
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return value, true
}

// MetaUint64 reads an unsigned numeric metadata value. Unparsable is absent,
// for the reason given on [Error.MetaInt64].
func (e *Error) MetaUint64(key string) (uint64, bool) {
	raw, ok := e.Metadata[key]
	if !ok {
		return 0, false
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return value, true
}

// QueueFullInfo is the admission decision behind a [ReasonQueueFull].
type QueueFullInfo struct {
	Queue   string
	Pending int64
	Cap     int64
}

// QueueFull reports the queue that refused the enqueue. The second return is
// false unless this error is a [ReasonQueueFull] carrying all three values.
func (e *Error) QueueFull() (QueueFullInfo, bool) {
	if e.Reason != ReasonQueueFull {
		return QueueFullInfo{}, false
	}
	queue, hasQueue := e.Meta("queue")
	pending, hasPending := e.MetaInt64("pending")
	capacity, hasCap := e.MetaInt64("cap")
	if !hasQueue || !hasPending || !hasCap {
		return QueueFullInfo{}, false
	}
	return QueueFullInfo{Queue: queue, Pending: pending, Cap: capacity}, true
}

// Scope reports which scope the credential was missing — "produce" or
// "execute" — on a [ReasonScopeDenied].
func (e *Error) Scope() (string, bool) {
	if e.Reason != ReasonScopeDenied {
		return "", false
	}
	return e.Meta("scope")
}

// Retryable reports whether the server described a condition that clears on its
// own: a full queue, a rate limit, a storage blip, a lock another writer holds.
//
// It says nothing about whether it is safe to retry. On a write, UNAVAILABLE
// and DEADLINE_EXCEEDED may both mean the write landed and the connection
// dropped afterwards, and no field on the wire distinguishes them. Retry a
// write only with a unique key set, and reuse the same value.
func (e *Error) Retryable() bool {
	switch e.Reason {
	case ReasonQueueFull, ReasonRateLimited, ReasonStorageUnavailable,
		ReasonLockHeld, ReasonSettingConflict:
		return true
	}
	return e.Reason == "" && (e.Code == codes.Unavailable || e.Code == codes.DeadlineExceeded)
}

// AsError extracts the FlexiQ error behind err, if there is one.
func AsError(err error) (*Error, bool) {
	var wireErr *Error
	ok := errors.As(err, &wireErr)
	return wireErr, ok
}

// fromRPC converts what a gRPC call returned into an [*Error].
//
// A non-status error — a context deadline the caller set, a dial failure — is
// returned as it is: dressing it up as a wire error would claim the server said
// something it never said.
func fromRPC(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	return fromStatus(st)
}

func fromStatus(st *status.Status) *Error {
	wireErr := &Error{
		Code:    st.Code(),
		Message: st.Message(),
		status:  st,
	}

	// Unknown detail types come back from Details() as errors. They are a
	// newer server describing something this build has no name for, so they
	// are skipped rather than reported.
	for _, detail := range st.Details() {
		switch info := detail.(type) {
		case *errdetails.ErrorInfo:
			if info.GetDomain() != ErrorDomain {
				continue
			}
			wireErr.Reason = Reason(info.GetReason())
			wireErr.Metadata = info.GetMetadata()
		case *errdetails.RetryInfo:
			wireErr.RetryAfter = info.GetRetryDelay().AsDuration()
		}
	}
	return wireErr
}
