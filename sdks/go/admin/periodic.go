package admin

import (
	"context"
	"errors"
	"time"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	adminv1 "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/admin/v1"
)

// PeriodicTask is a schedule that enqueues one task on a cron expression.
//
// A time the server did not set is the zero [time.Time]; test with IsZero.
type PeriodicTask struct {
	// Name is unique within the namespace, and the handle every other periodic
	// method takes.
	Name string
	// TaskName is the task each firing enqueues.
	TaskName string
	// Cron has six fields, seconds first: "0 */5 * * * *" is every five
	// minutes.
	Cron string
	// Queue is the queue each firing enqueues onto. A task put with an empty
	// queue reads back as "default", the name the server stores.
	Queue string
	// Timezone is the IANA name the cron expression is read in. Empty is UTC.
	Timezone string
	// Enabled is false while paused.
	Enabled bool
	NextRun time.Time
	// LastRun is zero until the task has fired once.
	LastRun time.Time
	// Payload is the tagged envelope each firing enqueues, opaque and never
	// re-encoded. Nil unless asked for with [GetPeriodicTaskOptions]; decode it
	// with [PeriodicTask.DecodePayload].
	Payload []byte
}

// DecodePayload reads the payload back into the call each firing makes. It
// fails when the payload was not requested.
func (t PeriodicTask) DecodePayload() (flexiq.Call, error) {
	return flexiq.DecodeCall(t.Payload)
}

// PeriodicTaskSpec is a periodic task's definition, as [Client.PutPeriodicTask]
// writes it.
//
// The arguments are Args and Kwargs, encoded with [flexiq.EncodeCall] — the
// envelope [flexiq.Client.Enqueue] sends, so a firing carries exactly what an
// enqueue of the same call would. Raw replaces both with a pre-encoded
// envelope.
type PeriodicTaskSpec struct {
	Name string
	// Task is the task each firing enqueues. Not validated here: the server
	// holds no task registry.
	Task string
	// Cron has six fields, seconds first. Refused if it does not parse.
	Cron string
	// Queue is empty for the default queue.
	Queue string
	// Timezone is the IANA name to read Cron in. Empty is UTC; an unknown name
	// is refused.
	Timezone string
	// Args are the positional arguments each firing passes.
	Args []any
	// Kwargs are the keyword arguments, for tasks written in a language that
	// has them.
	Kwargs map[string]any
	// Raw is a pre-encoded payload envelope. When set, Args and Kwargs are
	// ignored.
	Raw []byte
	// StartPaused creates the task paused. Ignored when the task already
	// exists: a replace never changes whether a task is paused.
	StartPaused bool
}

// ListPeriodicTasksQuery narrows [Client.ListPeriodicTasks]. The zero value
// lists every task. A credential narrowed to some queues or tasks must name one
// it reaches.
type ListPeriodicTasksQuery struct {
	// Queue keeps only the tasks firing onto this queue.
	Queue string
	// TaskName keeps only the schedules of this task.
	TaskName string
}

// GetPeriodicTaskOptions are what [Client.GetPeriodicTask] reads beyond the
// definition.
type GetPeriodicTaskOptions struct {
	// IncludePayload sends [PeriodicTask.Payload] back — the way to tell
	// whether a task's arguments still match a definition.
	IncludePayload bool
}

// ListPeriodicTasks answers the namespace's periodic tasks, by name, never
// carrying a payload.
func (c *Client) ListPeriodicTasks(ctx context.Context, query ListPeriodicTasksQuery) ([]PeriodicTask, error) {
	resp, err := c.admin.ListPeriodicTasks(ctx, &adminv1.ListPeriodicTasksRequest{
		Queue:    optionalString(query.Queue),
		TaskName: optionalString(query.TaskName),
	})
	if err != nil {
		return nil, rpcError(err)
	}
	tasks := make([]PeriodicTask, 0, len(resp.GetPeriodicTasks()))
	for _, msg := range resp.GetPeriodicTasks() {
		tasks = append(tasks, periodicTaskFromProto(msg))
	}
	return tasks, nil
}

// GetPeriodicTask reads one periodic task. One that does not exist is
// [flexiq.ReasonPeriodicTaskNotFound].
func (c *Client) GetPeriodicTask(ctx context.Context, name string, opts GetPeriodicTaskOptions) (PeriodicTask, error) {
	resp, err := c.admin.GetPeriodicTask(ctx, &adminv1.GetPeriodicTaskRequest{
		Name:           name,
		IncludePayload: opts.IncludePayload,
	})
	if err != nil {
		return PeriodicTask{}, rpcError(err)
	}
	return periodicTaskFromProto(resp.GetPeriodicTask()), nil
}

// PutPeriodicTask creates a periodic task, or replaces an existing one's
// definition, and answers it without its payload.
//
// A replace keeps what an operator set and a definition does not own: whether
// the task is paused, and when it last ran. Its next run moves only when the
// cron expression or the timezone changed. A task also declared in a worker's
// code is declared again when that worker starts.
func (c *Client) PutPeriodicTask(ctx context.Context, spec PeriodicTaskSpec) (PeriodicTask, error) {
	if spec.Name == "" {
		return PeriodicTask{}, errors.New("flexiq: put periodic task: name is empty")
	}
	if spec.Task == "" {
		return PeriodicTask{}, errors.New("flexiq: put periodic task: task name is empty")
	}
	payload, err := spec.payload()
	if err != nil {
		return PeriodicTask{}, err
	}

	resp, err := c.admin.PutPeriodicTask(ctx, &adminv1.PutPeriodicTaskRequest{
		Name:        spec.Name,
		TaskName:    spec.Task,
		Cron:        spec.Cron,
		Queue:       spec.Queue,
		Body:        &adminv1.PutPeriodicTaskRequest_Raw{Raw: payload},
		StartPaused: spec.StartPaused,
		Timezone:    optionalString(spec.Timezone),
	})
	if err != nil {
		return PeriodicTask{}, rpcError(err)
	}
	return periodicTaskFromProto(resp.GetPeriodicTask()), nil
}

// DeletePeriodicTask deletes a periodic task. Jobs it already fired are
// untouched. One that does not exist is [flexiq.ReasonPeriodicTaskNotFound].
func (c *Client) DeletePeriodicTask(ctx context.Context, name string) error {
	if _, err := c.admin.DeletePeriodicTask(ctx, &adminv1.DeletePeriodicTaskRequest{Name: name}); err != nil {
		return rpcError(err)
	}
	return nil
}

// PausePeriodicTask stops a periodic task firing, keeping its definition.
func (c *Client) PausePeriodicTask(ctx context.Context, name string) (PeriodicTask, error) {
	resp, err := c.admin.PausePeriodicTask(ctx, &adminv1.PausePeriodicTaskRequest{Name: name})
	if err != nil {
		return PeriodicTask{}, rpcError(err)
	}
	return periodicTaskFromProto(resp.GetPeriodicTask()), nil
}

// ResumePeriodicTask lets a paused periodic task fire again.
func (c *Client) ResumePeriodicTask(ctx context.Context, name string) (PeriodicTask, error) {
	resp, err := c.admin.ResumePeriodicTask(ctx, &adminv1.ResumePeriodicTaskRequest{Name: name})
	if err != nil {
		return PeriodicTask{}, rpcError(err)
	}
	return periodicTaskFromProto(resp.GetPeriodicTask()), nil
}

// payload is the envelope each firing passes. Always set, as on an enqueue: a
// spec with no arguments is a call with no arguments, encoded as one.
func (s PeriodicTaskSpec) payload() ([]byte, error) {
	if s.Raw != nil {
		return s.Raw, nil
	}
	return flexiq.EncodeCall(s.Args, s.Kwargs)
}

func periodicTaskFromProto(msg *adminv1.PeriodicTask) PeriodicTask {
	return PeriodicTask{
		Name:     msg.GetName(),
		TaskName: msg.GetTaskName(),
		Cron:     msg.GetCron(),
		Queue:    msg.GetQueue(),
		Timezone: msg.GetTimezone(),
		Enabled:  msg.GetEnabled(),
		NextRun:  asTime(msg.GetNextRun()),
		LastRun:  asTime(msg.GetLastRun()),
		Payload:  msg.GetPayload(),
	}
}
