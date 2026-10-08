package admin

import (
	"context"
	"strconv"

	adminv1 "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/admin/v1"
)

// QuotaOverflow is what an enqueue over a quota's MaxPending or EnqueueRate
// does.
type QuotaOverflow int32

const (
	// OverflowUnspecified is the zero value. The server stores it as
	// [OverflowReject] and reads back OverflowReject, so a declarative caller
	// sends OverflowReject explicitly or sees drift on its next read.
	OverflowUnspecified QuotaOverflow = 0
	// OverflowReject refuses the enqueue: [flexiq.ReasonQueueFull] for depth,
	// [flexiq.ReasonRateLimited] for rate. Nothing of the call is written.
	OverflowReject QuotaOverflow = 1
	// OverflowDrop accepts the call but dead-letters its jobs as shed. They
	// never run, and the dead-letter sweep never retries them.
	OverflowDrop QuotaOverflow = 2
)

func (o QuotaOverflow) String() string {
	switch o {
	case OverflowUnspecified:
		return "UNSPECIFIED"
	case OverflowReject:
		return "REJECT"
	case OverflowDrop:
		return "DROP"
	default:
		// A value from a newer server; the number is more use in a log.
		return "QuotaOverflow(" + strconv.FormatInt(int64(o), 10) + ")"
	}
}

// NamespaceQuota is the limits the token's namespace is held to, enforced by
// every process serving it.
//
// A nil limit is unlimited; zero is a real limit.
type NamespaceQuota struct {
	// MaxPending caps pending jobs, delayed ones included. A call that would
	// pass it is over as a whole: a batch never half-lands.
	MaxPending *int64
	// OnExcess is what an enqueue over MaxPending or EnqueueRate does.
	OnExcess QuotaOverflow
	// EnqueueRate is enqueues per interval, `<count>/<unit>` with unit one of
	// s, m or h. Empty is unlimited.
	EnqueueRate string
	// MaxRunning caps jobs running at once, gated at dispatch.
	MaxRunning *int64
	// MaxArchivedRows is a row ceiling on the archive; the retention sweep
	// deletes the oldest rows over it.
	MaxArchivedRows *int64
	// MaxDeadRows is a row ceiling on the dead-letter queue, trimmed the same
	// way.
	MaxDeadRows *int64
}

// IsZero reports whether every limit is unset: an unlimited namespace.
func (q NamespaceQuota) IsZero() bool {
	return q.MaxPending == nil && q.EnqueueRate == "" && q.MaxRunning == nil &&
		q.MaxArchivedRows == nil && q.MaxDeadRows == nil
}

// GetNamespaceQuota reads the namespace's quota. A namespace with none answers
// every limit unset.
func (c *Client) GetNamespaceQuota(ctx context.Context) (NamespaceQuota, error) {
	resp, err := c.admin.GetNamespaceQuota(ctx, &adminv1.GetNamespaceQuotaRequest{})
	if err != nil {
		return NamespaceQuota{}, rpcError(err)
	}
	return quotaFromProto(resp.GetQuota()), nil
}

// SetNamespaceQuota replaces the namespace's quota and answers it as stored.
//
// It is a replace, not a merge: a limit left unset is lifted, and a quota with
// every limit unset is the same as [Client.ClearNamespaceQuota]. Every running
// process enforces the new limits within a couple of seconds.
func (c *Client) SetNamespaceQuota(ctx context.Context, quota NamespaceQuota) (NamespaceQuota, error) {
	resp, err := c.admin.SetNamespaceQuota(ctx, &adminv1.SetNamespaceQuotaRequest{
		Quota: &adminv1.NamespaceQuota{
			MaxPending:      quota.MaxPending,
			OnExcess:        adminv1.QuotaOverflow(quota.OnExcess),
			EnqueueRate:     optionalString(quota.EnqueueRate),
			MaxRunning:      quota.MaxRunning,
			MaxArchivedRows: quota.MaxArchivedRows,
			MaxDeadRows:     quota.MaxDeadRows,
		},
	})
	if err != nil {
		return NamespaceQuota{}, rpcError(err)
	}
	return quotaFromProto(resp.GetQuota()), nil
}

// ClearNamespaceQuota removes the namespace's quota, lifting every limit.
func (c *Client) ClearNamespaceQuota(ctx context.Context) error {
	if _, err := c.admin.ClearNamespaceQuota(ctx, &adminv1.ClearNamespaceQuotaRequest{}); err != nil {
		return rpcError(err)
	}
	return nil
}

func quotaFromProto(msg *adminv1.NamespaceQuota) NamespaceQuota {
	if msg == nil {
		return NamespaceQuota{}
	}
	return NamespaceQuota{
		MaxPending:      copyInt64(msg.MaxPending),
		OnExcess:        QuotaOverflow(msg.GetOnExcess()),
		EnqueueRate:     msg.GetEnqueueRate(),
		MaxRunning:      copyInt64(msg.MaxRunning),
		MaxArchivedRows: copyInt64(msg.MaxArchivedRows),
		MaxDeadRows:     copyInt64(msg.MaxDeadRows),
	}
}

// copyInt64 detaches an optional from the response message it was read from.
func copyInt64(v *int64) *int64 {
	if v == nil {
		return nil
	}
	out := *v
	return &out
}
