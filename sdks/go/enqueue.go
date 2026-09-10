package flexiq

import (
	"context"
	"fmt"
	"time"

	pb "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/v1"
	"google.golang.org/grpc/status"
)

// EnqueueRequest is one job to submit.
//
// The body is Args and Kwargs, encoded into the cross-SDK envelope before the
// call goes out. Raw replaces both for a caller that already has an encoded
// envelope — a payload read off another job, or one built by hand.
type EnqueueRequest struct {
	// Task is the task to run. It is not validated here: the server holds no
	// task registry, so enqueuing a name nobody implements succeeds and the
	// job dead-letters later.
	Task string
	// Args are the positional arguments.
	Args []any
	// Kwargs are the keyword arguments, for tasks written in a language that
	// has them. The envelope carries the map either way.
	Kwargs map[string]any
	// Raw is a pre-encoded payload envelope. When set, Args and Kwargs are
	// ignored and these bytes reach storage untouched.
	Raw []byte
	// Options are the job's knobs. The zero value is a job on the default
	// queue at the server's defaults.
	Options EnqueueOptions
}

// EnqueueOptions are the producer-settable knobs on a job.
//
// Every field's zero value means "the server's default", so a caller sets only
// what it cares about. There is deliberately no namespace: a namespace is a
// property of the credential, and anything a client could name is something a
// client could forge.
type EnqueueOptions struct {
	// Queue is empty for the default queue.
	Queue string
	// Priority: higher runs first.
	Priority int32
	// MaxRetries is attempts after the first. Zero means the job is not
	// retried.
	MaxRetries int32
	// ScheduledAt is when the job becomes eligible to run. Zero means as soon
	// as possible.
	ScheduledAt time.Time
	// Timeout is how long one attempt may run. Zero takes the server's
	// default.
	Timeout time.Duration
	// UniqueKey dedupes against the *active* job carrying the same key.
	//
	// It is not an idempotency key: once the original completes or
	// dead-letters the key is released, and the same request enqueues a second
	// job. A retry loop's total deadline belongs inside the job's own life.
	UniqueKey string
	// Metadata is opaque JSON text, byte-preserved.
	Metadata string
	Notes    string
	// DependsOn are job ids this job waits on. An id in another namespace is
	// refused exactly as a missing one is.
	DependsOn []string
	// ExpiresAt: after this instant the job is cancelled instead of
	// dispatched.
	ExpiresAt time.Time
	// ResultTTL is how long to keep the result after completion.
	ResultTTL time.Duration
	// Debounce collapses a burst of enqueues carrying one key into a single
	// run. Nil for no debouncing.
	Debounce *Debounce
}

// Debounce collapses a burst of enqueues carrying one key into a single run.
//
// While a job with this key is still pending and unclaimed, its scheduled time
// slides forward instead of a second job being inserted.
type Debounce struct {
	// Key is non-empty. Jobs sharing it share a window.
	Key string
	// Window is how far ahead of now each enqueue pushes the run. Positive.
	Window time.Duration
	// MaxWait is the ceiling on the total delay, measured from the pending
	// job's creation. Mandatory, and at least Window: without it a producer
	// that never stops enqueuing starves the job forever.
	MaxWait time.Duration
	// ReplacePayload overwrites the pending job's payload with this one. False
	// keeps the payload the window opened with.
	ReplacePayload bool
	// MaxPending is the producer's own admission cap for the target queue. Nil
	// means uncapped, and then nothing is counted.
	MaxPending *int64
}

// EnqueueResult is one submitted job.
type EnqueueResult struct {
	Job Job
	// Deduplicated means the unique key matched a job that was already active,
	// and Job is that job rather than a new one.
	//
	// It is the one response field that describes what the call did rather
	// than the state it left behind, because it is the whole reason a producer
	// sets a unique key: without it the two calls are indistinguishable.
	Deduplicated bool
}

// BatchResult is one item's outcome from [Client.EnqueueBatch], in the position
// the item was sent in. Exactly one of Result and Err is set.
type BatchResult struct {
	Result *EnqueueResult
	// Err is an [*Error] describing why this item alone did not land.
	Err error
}

// Enqueue submits one job.
//
// A failure is not a signal to try again blindly. UNAVAILABLE,
// DEADLINE_EXCEEDED and CANCELLED may each mean the write landed and the
// connection dropped afterwards, and no field on the wire distinguishes them.
// A caller that retries sets [EnqueueOptions.UniqueKey] and reuses the same
// value.
func (c *Client) Enqueue(ctx context.Context, req EnqueueRequest) (EnqueueResult, error) {
	msg, err := req.toProto()
	if err != nil {
		return EnqueueResult{}, err
	}

	resp, err := c.producer.Enqueue(ctx, msg)
	if err != nil {
		return EnqueueResult{}, fromRPC(err)
	}
	return enqueueResultFromProto(resp), nil
}

// EnqueueBatch submits many jobs and answers one result per item, in input
// order.
//
// No atomicity is promised, and the call fails in two shapes a caller handles
// separately:
//
//   - Where a batch is one transaction, an item failure fails the whole RPC —
//     returning the earlier items as enqueued would report jobs that do not
//     exist. The returned error is an [*Error] whose [Error.BatchIndex] names
//     the item, and the results slice is nil.
//   - Where a batch can partially apply, the call succeeds and an item's Err
//     means that item alone did not land.
//
// Under both shapes a result with no Err is durable.
func (c *Client) EnqueueBatch(ctx context.Context, reqs []EnqueueRequest) ([]BatchResult, error) {
	items := make([]*pb.EnqueueRequest, 0, len(reqs))
	for i, req := range reqs {
		msg, err := req.toProto()
		if err != nil {
			return nil, fmt.Errorf("flexiq: batch item %d: %w", i, err)
		}
		items = append(items, msg)
	}

	resp, err := c.producer.EnqueueBatch(ctx, &pb.EnqueueBatchRequest{Items: items})
	if err != nil {
		return nil, fromRPC(err)
	}

	results := make([]BatchResult, 0, len(resp.GetResults()))
	for _, item := range resp.GetResults() {
		results = append(results, batchResultFromProto(item))
	}
	return results, nil
}

func batchResultFromProto(item *pb.EnqueueBatchItemResult) BatchResult {
	switch outcome := item.GetOutcome().(type) {
	case *pb.EnqueueBatchItemResult_Enqueued:
		result := enqueueResultFromProto(outcome.Enqueued)
		return BatchResult{Result: &result}
	case *pb.EnqueueBatchItemResult_Error:
		return BatchResult{Err: fromStatus(status.FromProto(outcome.Error))}
	default:
		// An arm this build has no name for. Reporting it as an error is the
		// honest answer: the item's outcome is genuinely unknown here, and
		// treating it as enqueued would claim a job that may not exist.
		return BatchResult{Err: fmt.Errorf("flexiq: batch item carries an outcome this client does not recognise")}
	}
}

func enqueueResultFromProto(resp *pb.EnqueueResponse) EnqueueResult {
	return EnqueueResult{
		Job:          jobFromProto(resp.GetJob()),
		Deduplicated: resp.GetDeduplicated(),
	}
}

func (r EnqueueRequest) toProto() (*pb.EnqueueRequest, error) {
	if r.Task == "" {
		return nil, fmt.Errorf("flexiq: enqueue: task name is empty")
	}

	body, err := r.body()
	if err != nil {
		return nil, err
	}
	return &pb.EnqueueRequest{
		TaskName: r.Task,
		Body:     body,
		Options:  r.Options.toProto(),
	}, nil
}

// body picks the request's payload arm. There is always one: an absent body is
// not an empty one, and the server refuses a request that sets neither.
func (r EnqueueRequest) body() (*pb.EnqueueRequest_Raw, error) {
	if r.Raw != nil {
		return &pb.EnqueueRequest_Raw{Raw: r.Raw}, nil
	}
	encoded, err := EncodeCall(r.Args, r.Kwargs)
	if err != nil {
		return nil, err
	}
	return &pb.EnqueueRequest_Raw{Raw: encoded}, nil
}

func (o EnqueueOptions) toProto() *pb.EnqueueOptions {
	opts := &pb.EnqueueOptions{
		Queue:       o.Queue,
		Priority:    o.Priority,
		MaxRetries:  o.MaxRetries,
		ScheduledAt: timestampOf(o.ScheduledAt),
		Timeout:     durationOf(o.Timeout),
		DependsOn:   o.DependsOn,
		ExpiresAt:   timestampOf(o.ExpiresAt),
		ResultTtl:   durationOf(o.ResultTTL),
		Debounce:    o.Debounce.toProto(),
	}
	if o.UniqueKey != "" {
		opts.UniqueKey = &o.UniqueKey
	}
	if o.Metadata != "" {
		opts.Metadata = &o.Metadata
	}
	if o.Notes != "" {
		opts.Notes = &o.Notes
	}
	return opts
}

func (d *Debounce) toProto() *pb.Debounce {
	if d == nil {
		return nil
	}
	return &pb.Debounce{
		Key:            d.Key,
		Window:         durationOf(d.Window),
		MaxWait:        durationOf(d.MaxWait),
		ReplacePayload: d.ReplacePayload,
		MaxPending:     d.MaxPending,
	}
}
