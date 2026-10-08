package admin

import (
	"context"

	adminv1 "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/admin/v1"
)

// Queue is a queue as an operator sees it: whether it dispatches, and what it
// holds.
type Queue struct {
	Name string
	// Paused in this namespace. A pause in another namespace does not show.
	Paused    bool
	Pending   int64
	Running   int64
	Completed int64
	Failed    int64
	Dead      int64
	Cancelled int64
}

// ListQueues answers every queue the namespace has a job, a pause or an
// override for, by name.
func (c *Client) ListQueues(ctx context.Context) ([]Queue, error) {
	resp, err := c.admin.ListQueues(ctx, &adminv1.ListQueuesRequest{})
	if err != nil {
		return nil, rpcError(err)
	}
	return queuesFromProto(resp.GetQueues()), nil
}

// GetQueue reads one queue. The second return is false when the namespace has
// no job, pause or override for it — a queue exists only through those.
//
// A credential narrowed to some queues can read those; [Client.ListQueues]
// needs a whole grant.
func (c *Client) GetQueue(ctx context.Context, name string) (Queue, bool, error) {
	resp, err := c.admin.ListQueues(ctx, &adminv1.ListQueuesRequest{Queue: &name})
	if err != nil {
		return Queue{}, false, rpcError(err)
	}
	for _, queue := range resp.GetQueues() {
		if queue.GetName() == name {
			return queueFromProto(queue), true, nil
		}
	}
	return Queue{}, false, nil
}

// PauseQueue stops dispatching from a queue; jobs already running finish. A
// queue with no jobs yet may be paused, and the pause holds once jobs arrive.
// Pausing a paused queue is not an error.
func (c *Client) PauseQueue(ctx context.Context, name string) (Queue, error) {
	resp, err := c.admin.PauseQueue(ctx, &adminv1.PauseQueueRequest{Queue: name})
	if err != nil {
		return Queue{}, rpcError(err)
	}
	return queueFromProto(resp.GetQueue()), nil
}

// ResumeQueue resumes dispatching from a paused queue. Resuming a queue that is
// not paused is not an error.
func (c *Client) ResumeQueue(ctx context.Context, name string) (Queue, error) {
	resp, err := c.admin.ResumeQueue(ctx, &adminv1.ResumeQueueRequest{Queue: name})
	if err != nil {
		return Queue{}, rpcError(err)
	}
	return queueFromProto(resp.GetQueue()), nil
}

func queuesFromProto(msgs []*adminv1.Queue) []Queue {
	queues := make([]Queue, 0, len(msgs))
	for _, msg := range msgs {
		queues = append(queues, queueFromProto(msg))
	}
	return queues
}

func queueFromProto(msg *adminv1.Queue) Queue {
	return Queue{
		Name:      msg.GetName(),
		Paused:    msg.GetPaused(),
		Pending:   msg.GetPending(),
		Running:   msg.GetRunning(),
		Completed: msg.GetCompleted(),
		Failed:    msg.GetFailed(),
		Dead:      msg.GetDead(),
		Cancelled: msg.GetCancelled(),
	}
}
