package admin

import (
	"context"
	"time"

	adminv1 "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/admin/v1"
)

// QueueOverride is what an operator overrides on a queue. An unset field is
// not overridden.
//
// There is no Paused: [Client.PauseQueue] is the one way to pause a queue, and
// [Queue.Paused] the one place to read it.
//
// An override reaches workers when they start. A worker already running keeps
// the values it started with.
type QueueOverride struct {
	// RateLimit is `<count>/<unit>`, unit one of s, m or h and count at least
	// one: "100/m". Empty is not overridden.
	RateLimit string
	// MaxConcurrent caps how many of the queue's jobs run at once. Nil is not
	// overridden; zero is a real cap.
	MaxConcurrent *int32
	// UpdatedAt is when the override last changed. Output only: ignored by
	// [Client.SetQueueOverride].
	UpdatedAt time.Time
}

// IsZero reports whether nothing is overridden.
func (o QueueOverride) IsZero() bool {
	return o.RateLimit == "" && o.MaxConcurrent == nil
}

// ListQueueOverrides answers every queue override in the namespace, by queue
// name.
func (c *Client) ListQueueOverrides(ctx context.Context) (map[string]QueueOverride, error) {
	resp, err := c.admin.ListOverrides(ctx, &adminv1.ListOverridesRequest{})
	if err != nil {
		return nil, rpcError(err)
	}
	overrides := make(map[string]QueueOverride, len(resp.GetQueues()))
	for name, msg := range resp.GetQueues() {
		overrides[name] = queueOverrideFromProto(msg)
	}
	return overrides, nil
}

// GetQueueOverride reads one queue's override. The second return is false when
// the queue has none.
func (c *Client) GetQueueOverride(ctx context.Context, queue string) (QueueOverride, bool, error) {
	resp, err := c.admin.ListOverrides(ctx, &adminv1.ListOverridesRequest{Queue: &queue})
	if err != nil {
		return QueueOverride{}, false, rpcError(err)
	}
	msg, ok := resp.GetQueues()[queue]
	if !ok {
		return QueueOverride{}, false, nil
	}
	return queueOverrideFromProto(msg), true, nil
}

// SetQueueOverride replaces one queue's override and answers it as stored.
//
// It is a replace, not a merge: a field left unset is no longer overridden, and
// an override with every field unset is the same as [Client.ClearQueueOverride].
// The server validates the values; a malformed rate limit is refused with
// [flexiq.ReasonInvalidRequest].
func (c *Client) SetQueueOverride(ctx context.Context, queue string, override QueueOverride) (QueueOverride, error) {
	resp, err := c.admin.SetQueueOverride(ctx, &adminv1.SetQueueOverrideRequest{
		Queue: queue,
		QueueOverride: &adminv1.QueueOverride{
			RateLimit:     optionalString(override.RateLimit),
			MaxConcurrent: override.MaxConcurrent,
		},
	})
	if err != nil {
		return QueueOverride{}, rpcError(err)
	}
	return queueOverrideFromProto(resp.GetQueueOverride()), nil
}

// ClearQueueOverride removes one queue's override. Clearing a queue that has
// none is not an error: "no override" is the state asked for.
func (c *Client) ClearQueueOverride(ctx context.Context, queue string) error {
	if _, err := c.admin.ClearQueueOverride(ctx, &adminv1.ClearQueueOverrideRequest{Queue: queue}); err != nil {
		return rpcError(err)
	}
	return nil
}

func queueOverrideFromProto(msg *adminv1.QueueOverride) QueueOverride {
	override := QueueOverride{
		RateLimit: msg.GetRateLimit(),
		UpdatedAt: asTime(msg.GetUpdateTime()),
	}
	if msg != nil && msg.MaxConcurrent != nil {
		maxConcurrent := msg.GetMaxConcurrent()
		override.MaxConcurrent = &maxConcurrent
	}
	return override
}
