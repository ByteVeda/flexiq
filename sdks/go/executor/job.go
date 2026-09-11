package executor

import (
	"encoding/json"
	"fmt"
	"time"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	executorv1 "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/executor/v1"
)

// Log levels a task log frame carries. Any string is accepted by the scheduler;
// these are the ones the rest of the system reads.
const (
	// LevelInfo is an ordinary log line.
	LevelInfo = "info"
	// LevelWarn is a log line worth an operator's attention.
	LevelWarn = "warn"
	// LevelError is a log line describing something that went wrong.
	LevelError = "error"
	// LevelResult marks a published partial: the message is empty and the value
	// rides in the frame's extra. [Job.Publish] writes one.
	LevelResult = "result"
)

// Job is one dispatched attempt.
//
// Everything on it was resolved by the scheduler from the dispatch it recorded.
// Nothing a client sends names a namespace, an owner, an attempt or a resource
// cap — anything a client could name is something a client could forge.
type Job struct {
	// ID is the job's identifier. It is not permanent: retention archives and
	// then deletes, so an id that reads NOT_FOUND later did still exist.
	ID string
	// TaskName is the task this attempt runs.
	TaskName string
	// Queue is the queue the job was drawn from.
	Queue string
	// Namespace the job belongs to, told to the executor and never accepted
	// from one. Empty when the server sent none.
	Namespace string
	// RetryCount is how many attempts already failed.
	RetryCount int
	// MaxRetries is how many this job is allowed.
	MaxRetries int
	// Timeout is how long this attempt may run. Zero means no limit.
	Timeout time.Duration
	// DisabledMiddleware names the middleware an operator has turned off for
	// this task, resolved at dispatch. It rides the dispatch because an
	// executor has no settings store to read it from.
	DisabledMiddleware []string
	// Metadata is the job's metadata blob as stored: opaque JSON text,
	// byte-preserved. It has no schema this package knows; parse it only if
	// your own code wrote it.
	Metadata string

	payload []byte
	session *session
}

// Payload returns the raw wire envelope, which is a tag byte followed by the
// call body. It is opaque and must not be re-encoded.
func (j *Job) Payload() []byte { return j.payload }

// Call decodes the whole payload: the positional and keyword arguments the task
// was enqueued with.
//
// Values arrive as `any`, so a number is whatever CBOR's widest form for it is.
// Use [Job.Bind] to decode into your own types instead.
func (j *Job) Call() (flexiq.Call, error) { return flexiq.DecodeCall(j.payload) }

// Bind decodes the call's positional arguments into targets, in order.
//
//	var charge Charge
//	if err := job.Bind(&charge); err != nil { ... }
//
// The cross-SDK convention is a single object argument, so one target is the
// usual case. A call carrying keyword arguments is refused rather than bound by
// position — read those with [Job.Call].
func (j *Job) Bind(targets ...any) error {
	return flexiq.DecodeCallInto(j.payload, targets...)
}

// Progress reports how far along this attempt is, as a percentage.
//
// Fire and forget: nothing answers it, and it never settles the job. It is a
// no-op when the scheduler did not advertise the side-channel capability, which
// is that capability's documented degradation.
//
// The value is clamped to 0-100. The frame's range is part of the contract and
// the scheduler does not enforce it, so a value outside it would be stored and
// read back wrong rather than refused.
func (j *Job) Progress(percent int) {
	if j.session == nil || !j.session.sideChannelOn() {
		return
	}

	clamped := percent
	if clamped < 0 {
		clamped = 0
	}
	if clamped > 100 {
		clamped = 100
	}

	j.session.pushSide(j.ID, true, &executorv1.AttachRequest{
		Frame: &executorv1.AttachRequest_Progress{Progress: &executorv1.ProgressFrame{
			JobId:    j.ID,
			Progress: int32(clamped),
		}},
	})
}

// Log records one structured line against this attempt.
//
// extra is marshalled as JSON and may be nil. Fire and forget, and a no-op
// without the side-channel capability, exactly like [Job.Progress].
func (j *Job) Log(level, message string, extra any) error {
	return j.log(level, message, extra)
}

// Publish records a partial result for this attempt: a log line at level
// "result" whose message is empty and whose value rides in the frame's extra.
//
// Logging and publishing share one frame, so a subscriber reading partials and
// an operator reading logs are reading the same stream.
func (j *Job) Publish(value any) error {
	return j.log(LevelResult, "", value)
}

func (j *Job) log(level, message string, extra any) error {
	var encoded []byte
	if extra != nil {
		marshalled, err := json.Marshal(extra)
		if err != nil {
			return fmt.Errorf("flexiq: encode log extra: %w", err)
		}
		encoded = marshalled
	}

	if j.session == nil || !j.session.sideChannelOn() {
		return nil
	}

	frame := &executorv1.TaskLogFrame{
		JobId:    j.ID,
		TaskName: j.TaskName,
		Level:    level,
		Message:  message,
	}
	if encoded != nil {
		frame.Extra = encoded
	}

	j.session.pushSide(j.ID, false, &executorv1.AttachRequest{
		Frame: &executorv1.AttachRequest_TaskLog{TaskLog: frame},
	})
	return nil
}

// jobFromFrame converts a dispatch into the job a handler sees.
func jobFromFrame(frame *executorv1.JobFrame, s *session) *Job {
	job := &Job{
		ID:                 frame.GetId(),
		TaskName:           frame.GetTaskName(),
		Queue:              frame.GetQueue(),
		Namespace:          frame.GetNamespace(),
		RetryCount:         int(frame.GetRetryCount()),
		MaxRetries:         int(frame.GetMaxRetries()),
		DisabledMiddleware: frame.GetDisabledMiddleware(),
		Metadata:           frame.GetMetadata(),
		payload:            frame.GetPayload(),
		session:            s,
	}
	// Unset means no limit, which is also what the frame protocol's
	// non-positive timeout means. Both arrive here as a zero Duration.
	if timeout := frame.GetTimeout(); timeout != nil && timeout.AsDuration() > 0 {
		job.Timeout = timeout.AsDuration()
	}
	return job
}
