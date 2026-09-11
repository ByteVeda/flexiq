package flexiq

import (
	"strconv"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/v1"
)

// JobStatus is a job's lifecycle state.
//
// The zero value is [StatusUnspecified], which a server never sends: it is the
// value a status this build has no name for decodes to nothing better than.
type JobStatus int32

const (
	// StatusUnspecified is the zero value, which a server never sends. See
	// [JobStatus.IsKnown].
	StatusUnspecified JobStatus = 0
	// StatusPending means waiting to be dequeued, or scheduled for the future.
	StatusPending JobStatus = 1
	// StatusRunning means claimed by a worker and executing.
	StatusRunning JobStatus = 2
	// StatusComplete means finished successfully.
	StatusComplete JobStatus = 3
	// StatusFailed means the attempt failed. Not terminal: the job may still
	// be retried.
	StatusFailed JobStatus = 4
	// StatusDead means retries were exhausted and the job moved to the
	// dead-letter queue.
	StatusDead JobStatus = 5
	// StatusCancelled means cancelled before or during execution.
	StatusCancelled JobStatus = 6
)

// IsTerminal reports whether the job has reached a state it will not leave.
//
// A status this build does not recognise is never terminal. A newer server may
// grow one, and reading an unknown state as finished would have a poller stop
// watching a job that is still running.
func (s JobStatus) IsTerminal() bool {
	switch s {
	case StatusComplete, StatusDead, StatusCancelled:
		return true
	default:
		return false
	}
}

// IsKnown reports whether the status is one this build can reason about.
//
// [StatusUnspecified] is not, and that is deliberate rather than an omission:
// a server never sends it. It has a name here because proto3 spends zero on
// one, so receiving it means either a value from a build newer than this one
// or a job that arrived without a status at all — and neither is a state a
// caller can act on. Both answer false, which is the answer that keeps a
// caller from trusting the value.
func (s JobStatus) IsKnown() bool {
	switch s {
	case StatusPending, StatusRunning, StatusComplete, StatusFailed, StatusDead, StatusCancelled:
		return true
	default:
		return false
	}
}

func (s JobStatus) String() string {
	switch s {
	case StatusUnspecified:
		return nameUnspecified
	case StatusPending:
		return namePending
	case StatusRunning:
		return nameRunning
	case StatusComplete:
		return "COMPLETE"
	case StatusFailed:
		return nameFailed
	case StatusDead:
		return "DEAD"
	case StatusCancelled:
		return nameCancelled
	default:
		// A status from a newer server. Naming the number is more use than
		// "unknown" when it turns up in a log.
		return "JobStatus(" + strconv.FormatInt(int64(s), 10) + ")"
	}
}

// Job is a job as a reader sees it.
//
// A time that the server did not set is the zero [time.Time]; test with
// IsZero, not against a sentinel.
type Job struct {
	// ID is opaque. A UUIDv7 today; read it as a string, never as a UUID.
	ID       string
	Queue    string
	TaskName string
	Status   JobStatus
	// Priority: higher runs first.
	Priority   int32
	RetryCount int32
	MaxRetries int32
	// Timeout is how long one attempt may run before the scheduler reclaims it.
	Timeout time.Duration
	// ResultTTL is how long the result is kept after completion.
	ResultTTL time.Duration
	// CancelRequested means a cancel arrived while the job was running; the
	// task stops at its next check.
	CancelRequested bool
	// HasDeps means the job waits on at least one dependency.
	HasDeps bool
	// Namespace is output-only and always the caller's own. It is here so a
	// client logging a job record has it, not so a client can select one.
	Namespace   string
	CreatedAt   time.Time
	ScheduledAt time.Time
	StartedAt   time.Time
	CompletedAt time.Time
	// ExpiresAt: after this instant the job is cancelled instead of dispatched.
	ExpiresAt time.Time

	// Payload is the tagged envelope, opaque and never re-encoded.
	//
	// Nil means the caller did not ask for it — reads leave it out by default,
	// and list rows never carry one. Non-nil and empty means the job carries no
	// body. Decode it with [Job.DecodePayload].
	Payload []byte
	// Result is the task's return value, opaque. Nil means the job returned
	// nothing, or the caller did not ask. Decode it with [Job.DecodeResult].
	Result []byte
	// Error is the failure a job recorded, canonical JSON where whatever wrote
	// it produced one and free prose otherwise. Read it with [Job.TaskError].
	Error string
	// Progress is 0-100, reported by the running task. Nil when the task
	// reported none — which is not the same as reporting zero.
	Progress *int32
	// Metadata is opaque JSON text, byte-preserved.
	Metadata    string
	Notes       string
	UniqueKey   string
	DebounceKey string
}

// TaskError parses the failure the job recorded. The second return is false
// when the job recorded no error at all.
func (j Job) TaskError() (TaskError, bool) {
	if j.Error == "" {
		return TaskError{}, false
	}
	return ParseTaskError(j.Error), true
}

// DecodePayload reads the job's payload back into the call it was enqueued
// with. It fails when the payload was not requested.
func (j Job) DecodePayload() (Call, error) {
	return DecodeCall(j.Payload)
}

// DecodeResult reads the task's return value into v. It fails when the result
// was not requested, and when the job returned nothing.
func (j Job) DecodeResult(v any) error {
	return DecodeResult(j.Result, v)
}

// jobFromProto maps the wire message onto the read model.
//
// An enum value this build has no name for is carried through as its number
// rather than rejected — see [JobStatus.IsKnown].
//
// A *field* this build has no name for is dropped here. It survives protobuf
// decoding, but this mapping names every field it copies, so a field a newer
// server adds reaches the wire message and stops. That is the trade for a
// hand-written read model, and it is why a client is generated against a
// version of the contract rather than discovering one.
func jobFromProto(msg *pb.Job) Job {
	if msg == nil {
		return Job{}
	}
	return Job{
		ID:              msg.GetId(),
		Queue:           msg.GetQueue(),
		TaskName:        msg.GetTaskName(),
		Status:          JobStatus(msg.GetStatus()),
		Priority:        msg.GetPriority(),
		RetryCount:      msg.GetRetryCount(),
		MaxRetries:      msg.GetMaxRetries(),
		Timeout:         asDuration(msg.GetTimeout()),
		ResultTTL:       asDuration(msg.GetResultTtl()),
		CancelRequested: msg.GetCancelRequested(),
		HasDeps:         msg.GetHasDeps(),
		Namespace:       msg.GetNamespace(),
		CreatedAt:       asTime(msg.GetCreatedAt()),
		ScheduledAt:     asTime(msg.GetScheduledAt()),
		StartedAt:       asTime(msg.GetStartedAt()),
		CompletedAt:     asTime(msg.GetCompletedAt()),
		ExpiresAt:       asTime(msg.GetExpiresAt()),
		Payload:         msg.GetPayload(),
		Result:          msg.GetResult(),
		Error:           msg.GetError(),
		Progress:        msg.Progress,
		Metadata:        msg.GetMetadata(),
		Notes:           msg.GetNotes(),
		UniqueKey:       msg.GetUniqueKey(),
		DebounceKey:     msg.GetDebounceKey(),
	}
}

// asTime keeps "the server did not set this" distinguishable from "the server
// set the epoch": an absent Timestamp becomes the zero time, not 1970.
func asTime(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

func asDuration(d *durationpb.Duration) time.Duration {
	if d == nil {
		return 0
	}
	return d.AsDuration()
}

// timestampOf is asTime's inverse, for the request side.
func timestampOf(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// durationOf is asDuration's inverse. A zero duration means "unset", which is
// what every duration on the request side already means.
func durationOf(d time.Duration) *durationpb.Duration {
	if d == 0 {
		return nil
	}
	return durationpb.New(d)
}
