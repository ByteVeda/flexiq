package tests

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ByteVeda/flexiq/sdks/go/v2/admin"
	adminv1 "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/admin/v1"
)

func int32Ptr(v int32) *int32 { return &v }
func int64Ptr(v int64) *int64 { return &v }

// TestSetQueueOverrideSendsOnlyWhatIsSet: the call is a replace, so an unset
// field must reach the wire unset — an empty rate limit sent as "" would be
// refused, and a nil cap sent as 0 would hold the queue.
func TestSetQueueOverrideSendsOnlyWhatIsSet(t *testing.T) {
	var got *adminv1.SetQueueOverrideRequest
	updated := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	client := serveAdmin(t, &fakeAdmin{
		setQueueOverride: func(_ context.Context, req *adminv1.SetQueueOverrideRequest) (*adminv1.SetQueueOverrideResponse, error) {
			got = req
			stored := req.GetQueueOverride()
			stored.UpdateTime = timestamppb.New(updated)
			return &adminv1.SetQueueOverrideResponse{QueueOverride: stored}, nil
		},
	})
	ctx := context.Background()

	stored, err := client.SetQueueOverride(ctx, "payments", admin.QueueOverride{MaxConcurrent: int32Ptr(0)})
	if err != nil {
		t.Fatalf("SetQueueOverride: %v", err)
	}
	override := got.GetQueueOverride()
	if got.GetQueue() != "payments" || override.RateLimit != nil {
		t.Errorf("request is %v; an empty rate limit must stay unset", got)
	}
	if override.MaxConcurrent == nil || override.GetMaxConcurrent() != 0 {
		t.Errorf("a zero cap did not reach the wire as a set zero: %v", override)
	}
	if stored.MaxConcurrent == nil || *stored.MaxConcurrent != 0 || !stored.UpdatedAt.Equal(updated) {
		t.Errorf("answered override is %+v", stored)
	}

	if _, err := client.SetQueueOverride(ctx, "payments", admin.QueueOverride{RateLimit: "100/m"}); err != nil {
		t.Fatalf("SetQueueOverride: %v", err)
	}
	if got.GetQueueOverride().GetRateLimit() != "100/m" || got.GetQueueOverride().MaxConcurrent != nil {
		t.Errorf("request is %v", got)
	}
}

// TestGetQueueOverrideReportsAnAbsentOne: a queue with no override is not a
// failure, and the read narrows to the queue so a narrowed grant can make it.
func TestGetQueueOverrideReportsAnAbsentOne(t *testing.T) {
	var got *adminv1.ListOverridesRequest
	client := serveAdmin(t, &fakeAdmin{
		listOverrides: func(_ context.Context, req *adminv1.ListOverridesRequest) (*adminv1.ListOverridesResponse, error) {
			got = req
			if req.GetQueue() == "payments" {
				return &adminv1.ListOverridesResponse{Queues: map[string]*adminv1.QueueOverride{
					"payments": {RateLimit: strPtr("5/s")},
				}}, nil
			}
			return &adminv1.ListOverridesResponse{}, nil
		},
	})
	ctx := context.Background()

	if _, found, err := client.GetQueueOverride(ctx, "other"); err != nil || found {
		t.Fatalf("GetQueueOverride(other) = found %v, err %v; want absent", found, err)
	}
	if got.Queue == nil || got.GetQueue() != "other" || got.TaskName != nil {
		t.Errorf("the read did not narrow to the queue: %v", got)
	}

	override, found, err := client.GetQueueOverride(ctx, "payments")
	if err != nil || !found {
		t.Fatalf("GetQueueOverride(payments) = found %v, err %v", found, err)
	}
	if override.RateLimit != "5/s" || override.MaxConcurrent != nil || override.IsZero() {
		t.Errorf("override is %+v", override)
	}
}

// TestNamespaceQuotaKeepsUnsetAndZeroApart: a nil limit is unlimited and a zero
// one is a real limit, in both directions.
func TestNamespaceQuotaKeepsUnsetAndZeroApart(t *testing.T) {
	var got *adminv1.SetNamespaceQuotaRequest
	client := serveAdmin(t, &fakeAdmin{
		setQuota: func(_ context.Context, req *adminv1.SetNamespaceQuotaRequest) (*adminv1.SetNamespaceQuotaResponse, error) {
			got = req
			return &adminv1.SetNamespaceQuotaResponse{Quota: req.GetQuota()}, nil
		},
		getQuota: func(context.Context, *adminv1.GetNamespaceQuotaRequest) (*adminv1.GetNamespaceQuotaResponse, error) {
			return &adminv1.GetNamespaceQuotaResponse{Quota: &adminv1.NamespaceQuota{}}, nil
		},
	})
	ctx := context.Background()

	stored, err := client.SetNamespaceQuota(ctx, admin.NamespaceQuota{
		MaxPending:  int64Ptr(0),
		OnExcess:    admin.OverflowDrop,
		EnqueueRate: "10/s",
	})
	if err != nil {
		t.Fatalf("SetNamespaceQuota: %v", err)
	}
	quota := got.GetQuota()
	if quota.MaxPending == nil || quota.GetMaxPending() != 0 {
		t.Errorf("a zero max_pending did not reach the wire as a set zero: %v", quota)
	}
	if quota.MaxRunning != nil || quota.MaxArchivedRows != nil || quota.MaxDeadRows != nil {
		t.Errorf("an unset limit went out set: %v", quota)
	}
	if quota.GetOnExcess() != adminv1.QuotaOverflow_QUOTA_OVERFLOW_DROP || quota.GetEnqueueRate() != "10/s" {
		t.Errorf("request is %v", quota)
	}
	if stored.OnExcess != admin.OverflowDrop || stored.OnExcess.String() != "DROP" ||
		stored.MaxPending == nil || *stored.MaxPending != 0 {
		t.Errorf("answered quota is %+v", stored)
	}

	none, err := client.GetNamespaceQuota(ctx)
	if err != nil {
		t.Fatalf("GetNamespaceQuota: %v", err)
	}
	if !none.IsZero() {
		t.Errorf("a namespace with no quota read as %+v", none)
	}
}
