package tests

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	"github.com/ByteVeda/flexiq/sdks/go/v2/admin"
	adminv1 "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/admin/v1"
)

// TestListQueueOverridesMapsEveryQueue: the unfiltered read names no queue,
// and every entry comes back under its own name.
func TestListQueueOverridesMapsEveryQueue(t *testing.T) {
	var got *adminv1.ListOverridesRequest
	client := serveAdmin(t, &fakeAdmin{
		listOverrides: func(_ context.Context, req *adminv1.ListOverridesRequest) (*adminv1.ListOverridesResponse, error) {
			got = req
			return &adminv1.ListOverridesResponse{Queues: map[string]*adminv1.QueueOverride{
				"payments": {RateLimit: strPtr("5/s")},
				"emails":   {MaxConcurrent: int32Ptr(3)},
			}}, nil
		},
	})

	overrides, err := client.ListQueueOverrides(context.Background())
	if err != nil {
		t.Fatalf("ListQueueOverrides: %v", err)
	}
	if got.Queue != nil || got.TaskName != nil {
		t.Errorf("an unfiltered list narrowed: %v", got)
	}
	if len(overrides) != 2 || overrides["payments"].RateLimit != "5/s" ||
		overrides["emails"].MaxConcurrent == nil || *overrides["emails"].MaxConcurrent != 3 {
		t.Errorf("overrides are %+v", overrides)
	}
}

// TestClearCallsNameTheirTarget: a clear carries the queue it is about, and
// the quota clear carries nothing.
func TestClearCallsNameTheirTarget(t *testing.T) {
	var cleared string
	quotaCleared := false
	client := serveAdmin(t, &fakeAdmin{
		clearQueueOverride: func(_ context.Context, req *adminv1.ClearQueueOverrideRequest) (*adminv1.ClearQueueOverrideResponse, error) {
			cleared = req.GetQueue()
			return &adminv1.ClearQueueOverrideResponse{}, nil
		},
		clearQuota: func(context.Context, *adminv1.ClearNamespaceQuotaRequest) (*adminv1.ClearNamespaceQuotaResponse, error) {
			quotaCleared = true
			return &adminv1.ClearNamespaceQuotaResponse{}, nil
		},
	})
	ctx := context.Background()

	if err := client.ClearQueueOverride(ctx, "payments"); err != nil {
		t.Fatalf("ClearQueueOverride: %v", err)
	}
	if cleared != "payments" {
		t.Errorf("cleared queue %q, want payments", cleared)
	}
	if err := client.ClearNamespaceQuota(ctx); err != nil {
		t.Fatalf("ClearNamespaceQuota: %v", err)
	}
	if !quotaCleared {
		t.Error("ClearNamespaceQuota did not reach the server")
	}
}

// TestResumeCallsAnswerTheStateAfter: a resume answers the queue or task as
// the call left it.
func TestResumeCallsAnswerTheStateAfter(t *testing.T) {
	client := serveAdmin(t, &fakeAdmin{
		resumeQueue: func(_ context.Context, req *adminv1.ResumeQueueRequest) (*adminv1.ResumeQueueResponse, error) {
			return &adminv1.ResumeQueueResponse{Queue: &adminv1.Queue{Name: req.GetQueue(), Pending: 4}}, nil
		},
		resumePeriodic: func(_ context.Context, req *adminv1.ResumePeriodicTaskRequest) (*adminv1.ResumePeriodicTaskResponse, error) {
			return &adminv1.ResumePeriodicTaskResponse{PeriodicTask: &adminv1.PeriodicTask{Name: req.GetName(), Enabled: true}}, nil
		},
	})
	ctx := context.Background()

	queue, err := client.ResumeQueue(ctx, "payments")
	if err != nil || queue.Name != "payments" || queue.Paused || queue.Pending != 4 {
		t.Errorf("ResumeQueue = %+v, %v", queue, err)
	}
	task, err := client.ResumePeriodicTask(ctx, "nightly")
	if err != nil || task.Name != "nightly" || !task.Enabled {
		t.Errorf("ResumePeriodicTask = %+v, %v", task, err)
	}
}

// TestListTokensKeepsTheServerOrder: newest first is the server's promise,
// so the client must not reorder.
func TestListTokensKeepsTheServerOrder(t *testing.T) {
	client := serveAdmin(t, &fakeAdmin{
		listTokens: func(context.Context, *adminv1.ListTokensRequest) (*adminv1.ListTokensResponse, error) {
			return &adminv1.ListTokensResponse{Tokens: []*adminv1.ApiToken{
				{Id: "new", Status: adminv1.TokenStatus_TOKEN_STATUS_ACTIVE},
				{Id: "old", Status: adminv1.TokenStatus_TOKEN_STATUS_EXPIRED},
			}}, nil
		},
	})

	tokens, err := client.ListTokens(context.Background())
	if err != nil {
		t.Fatalf("ListTokens: %v", err)
	}
	if len(tokens) != 2 || tokens[0].ID != "new" || tokens[1].ID != "old" ||
		tokens[1].Status != admin.TokenStatusExpired {
		t.Errorf("tokens are %+v", tokens)
	}
}

// TestUnhandledAdminCallIsUnimplemented: a double with no handler answers a
// status the client reports, not a panic in the server goroutine.
func TestUnhandledAdminCallIsUnimplemented(t *testing.T) {
	client := serveAdmin(t, &fakeAdmin{})

	_, err := client.ListTokens(context.Background())
	wireErr, ok := flexiq.AsError(err)
	if !ok || wireErr.Code != codes.Unimplemented {
		t.Fatalf("want an Unimplemented *flexiq.Error, got %T: %v", err, err)
	}
}
