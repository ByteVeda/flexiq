package flexiq

import (
	"context"
	"errors"
	"io"
	"iter"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"

	pb "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/v1"
)

// TransitionKind is what a [Transition] reports.
type TransitionKind int32

const (
	// TransitionUnspecified is the zero value, which a server never sends.
	TransitionUnspecified TransitionKind = 0
	// TransitionSnapshot is the job's current state, read from storage when
	// the watch opened or when the server's re-read found it had moved on.
	TransitionSnapshot TransitionKind = 1
	// TransitionEnqueued means the job was written to the queue.
	TransitionEnqueued TransitionKind = 2
	// TransitionStarted means the job was claimed and handed to a worker.
	TransitionStarted TransitionKind = 3
	// TransitionCompleted means the job finished successfully.
	TransitionCompleted TransitionKind = 4
	// TransitionFailed means an attempt failed. A retry or a dead-letter
	// always follows.
	TransitionFailed TransitionKind = 5
	// TransitionRetrying means the job was rescheduled for another attempt.
	TransitionRetrying TransitionKind = 6
	// TransitionDead means the job was dead-lettered.
	TransitionDead TransitionKind = 7
	// TransitionCancelled means the job was cancelled.
	TransitionCancelled TransitionKind = 8
	// TransitionSleeping means an attempt ended in a durable step sleep; the
	// job is pending again.
	TransitionSleeping TransitionKind = 9
)

func (k TransitionKind) String() string {
	switch k {
	case TransitionUnspecified:
		return nameUnspecified
	case TransitionSnapshot:
		return "SNAPSHOT"
	case TransitionEnqueued:
		return "ENQUEUED"
	case TransitionStarted:
		return "STARTED"
	case TransitionCompleted:
		return nameCompleted
	case TransitionFailed:
		return nameFailed
	case TransitionRetrying:
		return "RETRYING"
	case TransitionDead:
		return "DEAD"
	case TransitionCancelled:
		return nameCancelled
	case TransitionSleeping:
		return "SLEEPING"
	default:
		return "TransitionKind(" + strconv.FormatInt(int64(k), 10) + ")"
	}
}

// Transition is one item of a watch: a job's state, or a change to it.
//
// A time the server did not set is the zero [time.Time].
type Transition struct {
	JobID    string
	Queue    string
	TaskName string
	Kind     TransitionKind
	// Status is the status the job is in after this transition.
	Status JobStatus
	// Attempt is the job's retry count for the attempt this transition belongs
	// to, zero for the first run.
	Attempt int32
	// Time is when the server observed the transition.
	Time time.Time
	// Terminal marks the job's last item. Decide that a job is finished by this,
	// never by Status: a failed attempt is not final while a retry or a
	// dead-letter is still to come.
	Terminal bool
	// Error is a failed attempt's error message, empty when there is none.
	Error string
	// Reason says why a job was dead-lettered without running out of retries,
	// or cancelled without running. Empty when there is none.
	Reason string
	// TimedOut marks a failed attempt that was an execution timeout.
	TimedOut bool
	// WakeAt is the instant a sleeping job is rescheduled to.
	WakeAt time.Time
	// NotFound means JobID names no job this credential can see — it does not
	// exist, or is in another namespace, which the server does not
	// distinguish. Every other field but Cursor is empty. It is terminal.
	NotFound bool
	// Checkpoint marks a queue watch's opening item: no job, only the Cursor
	// the watch starts from. Keep that cursor like any other, so a stream lost
	// before its first transition resumes with no gap.
	Checkpoint bool
	// Cursor is this item's position on a queue watch; pass it to
	// [Client.WatchQueue] to resume. Opaque, and empty on an id watch.
	Cursor string
}

// WatchJobs follows jobs by id, until every one is finished.
//
// It yields one item per id first — a [TransitionSnapshot] of the job's current
// state, or a NotFound item — then each live transition. When every job has
// sent its Terminal item the stream ends and so does the iteration. A job that
// is already terminal yields that one item.
//
// The iteration stops at the first error, yielded with a zero Transition. One
// watch names at most 100 ids. To resume after an error, call again: the
// snapshot catches anything that happened meanwhile. [Client.Wait] does that
// for one job.
func (c *Client) WatchJobs(ctx context.Context, jobIDs ...string) iter.Seq2[Transition, error] {
	return c.watch(ctx, &pb.WatchJobsRequest{
		Target: &pb.WatchJobsRequest_JobIds{JobIds: &pb.WatchJobIds{JobIds: jobIDs}},
	})
}

// WatchQueue follows every job in a queue, until ctx ends or the stream fails.
//
// No snapshot: a Checkpoint item carrying the starting Cursor, then live
// transitions, each carrying a Cursor. Pass the last one — checkpoint included
// — back as resumeCursor to replay what a dropped stream missed; an empty one
// starts from now. The server keeps a bounded window of transitions per
// process, so a cursor it no longer holds fails with
// [ReasonWatchCursorExpired]. The gap's transitions are gone: watch again from
// now, and read current state with [Client.ListJobs] if needed.
//
// A queue watch sees only the transitions the server process it reaches
// handles itself. An empty queue means "default".
func (c *Client) WatchQueue(ctx context.Context, queue, resumeCursor string) iter.Seq2[Transition, error] {
	return c.watch(ctx, &pb.WatchJobsRequest{
		Target:       &pb.WatchJobsRequest_Queue{Queue: queue},
		ResumeCursor: resumeCursor,
	})
}

func (c *Client) watch(ctx context.Context, req *pb.WatchJobsRequest) iter.Seq2[Transition, error] {
	return func(yield func(Transition, error) bool) {
		// Cancelled on return, so a caller that breaks early ends the stream
		// rather than leaving it open on the server.
		streamCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		stream, err := c.producer.WatchJobs(streamCtx, req)
		if err != nil {
			yield(Transition{}, fromRPC(err))
			return
		}
		for {
			msg, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return
			}
			if err != nil {
				yield(Transition{}, fromRPC(err))
				return
			}
			transition, known := transitionFromProto(msg)
			if !known {
				// An arm a newer server added: not this build.
				continue
			}
			if !yield(transition, nil) {
				return
			}
		}
	}
}

// The first wait before [Client.Wait] reopens a dropped watch, and the most it
// ever waits between tries.
const (
	waitBackoffStart = 250 * time.Millisecond
	waitBackoffCap   = 5 * time.Second
)

// Wait blocks until a job is finished, then reads it back with its result.
//
// It watches the job rather than polling it, and reopens the watch on its own
// when the stream drops for a reason that clears — the server restarting, the
// connection failing, the client falling behind. A job that does not exist, or
// is in another namespace, fails with [ReasonJobNotFound]. Bound it with ctx.
func (c *Client) Wait(ctx context.Context, jobID string) (Job, error) {
	backoff := waitBackoffStart
	opened := false
	for {
		finished, err := c.waitOnce(ctx, jobID, &backoff, &opened)
		if finished {
			return c.readFinished(ctx, jobID)
		}
		if err != nil && !reopenable(err, opened) {
			return Job{}, err
		}
		if err := sleep(ctx, backoff); err != nil {
			return Job{}, err
		}
		backoff = min(backoff*2, waitBackoffCap)
	}
}

// readFinished reads a finished job with its result, retrying a failure that
// clears on its own: the job is done, so only the read is left to go wrong.
//
// The result is read, not streamed: a watch carries no payload or result.
// GetJob also answers the not-found case with the server's own error.
func (c *Client) readFinished(ctx context.Context, jobID string) (Job, error) {
	backoff := waitBackoffStart
	for {
		job, err := c.GetJob(ctx, jobID, GetJobOptions{IncludeResult: true})
		if err == nil || !transient(err) {
			return job, err
		}
		if err := sleep(ctx, backoff); err != nil {
			return Job{}, err
		}
		backoff = min(backoff*2, waitBackoffCap)
	}
}

// sleep waits d, or returns ctx's error if it ends first.
func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// waitOnce follows one stream until the job finishes or the stream ends.
// opened records that a watch delivered an item at least once.
func (c *Client) waitOnce(ctx context.Context, jobID string, backoff *time.Duration, opened *bool) (bool, error) {
	for transition, err := range c.WatchJobs(ctx, jobID) {
		if err != nil {
			return false, err
		}
		// An item arrived, so the connection works again.
		*backoff = waitBackoffStart
		*opened = true
		if transition.Terminal || transition.NotFound {
			return true, nil
		}
	}
	// Ended OK without a terminal item: the server ended it early.
	return false, nil
}

// reopenable reports whether a watch that failed with err is worth opening
// again. Reopening is always safe — the RPC writes nothing.
//
// [ReasonWatchLimit] counts only once a watch has opened: after a dropped
// connection the server may still hold the old stream's slot until it
// notices. Before that, the credential really is at its cap.
func reopenable(err error, openedBefore bool) bool {
	wireErr, ok := AsError(err)
	if !ok {
		return false
	}
	switch wireErr.Reason {
	case ReasonWatchOverflow, ReasonShuttingDown:
		return true
	case ReasonWatchLimit:
		return openedBefore
	}
	return transient(err)
}

// transient reports UNAVAILABLE, or a reason-free DEADLINE_EXCEEDED — the
// transport or a proxy's own deadline, not the server's answer. A caller's own
// expired ctx also surfaces as DEADLINE_EXCEEDED; the loops catch that when
// they next wait on ctx.
func transient(err error) bool {
	wireErr, ok := AsError(err)
	if !ok {
		return false
	}
	return wireErr.Code == codes.Unavailable ||
		(wireErr.Reason == "" && wireErr.Code == codes.DeadlineExceeded)
}

// EnqueueAndWait submits one job and waits for it to finish — "run this and
// give me the answer". See [Client.Enqueue] for what a failed enqueue means,
// and [Client.Wait] for the wait.
//
// A deduplicated enqueue waits for the job the unique key matched.
func (c *Client) EnqueueAndWait(ctx context.Context, req EnqueueRequest) (Job, error) {
	enqueued, err := c.Enqueue(ctx, req)
	if err != nil {
		return Job{}, err
	}
	return c.Wait(ctx, enqueued.Job.ID)
}

// transitionFromProto reads one item, or reports false for an arm this build
// does not know.
func transitionFromProto(msg *pb.WatchJobsResponse) (Transition, bool) {
	switch item := msg.GetItem().(type) {
	case nil:
		// No arm this build knows. With a cursor it is still a position a
		// queue watch must keep: the opening checkpoint, or an arm a newer
		// server added.
		if msg.GetCursor() == "" {
			return Transition{}, false
		}
		return Transition{Checkpoint: true, Cursor: msg.GetCursor()}, true
	case *pb.WatchJobsResponse_NotFoundJobId:
		return Transition{
			JobID:    item.NotFoundJobId,
			NotFound: true,
			Terminal: true,
			Cursor:   msg.GetCursor(),
		}, true
	case *pb.WatchJobsResponse_Transition:
		t := item.Transition
		return Transition{
			JobID:    t.GetJobId(),
			Queue:    t.GetQueue(),
			TaskName: t.GetTaskName(),
			Kind:     TransitionKind(t.GetKind()),
			Status:   JobStatus(t.GetStatus()),
			Attempt:  t.GetAttempt(),
			Time:     asTime(t.GetTime()),
			Terminal: t.GetTerminal(),
			Error:    t.GetError(),
			Reason:   t.GetReason(),
			TimedOut: t.GetTimedOut(),
			WakeAt:   asTime(t.GetWakeAt()),
			Cursor:   msg.GetCursor(),
		}, true
	default:
		return Transition{}, false
	}
}
