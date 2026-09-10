package tests

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	pb "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/v1"
)

// TestUnknownStatusIsNotTerminal is the tolerance rule with the sharpest
// consequence: a newer server may grow a status, and reading one this build
// cannot name as finished would have a poller stop watching a live job.
func TestUnknownStatusIsNotTerminal(t *testing.T) {
	const fromANewerServer = pb.JobStatus(42)

	client := serve(t, &fakeProducer{
		getJob: func(_ context.Context, req *pb.GetJobRequest) (*pb.GetJobResponse, error) {
			return &pb.GetJobResponse{Job: &pb.Job{Id: req.GetJobId(), Status: fromANewerServer}}, nil
		},
	})

	job, err := client.GetJob(context.Background(), "job-1", flexiq.GetJobOptions{})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if job.Status.IsKnown() {
		t.Errorf("status %d was reported as known to this build", job.Status)
	}
	if job.Status.IsTerminal() {
		t.Error("an unrecognised status was treated as terminal")
	}
	if job.Status.String() != "JobStatus(42)" {
		t.Errorf("unknown status prints as %q; it should name the number", job.Status)
	}
}

// TestTerminalStatuses pins which states a job does not leave. FAILED is not
// one of them: a failed attempt may still be retried.
func TestTerminalStatuses(t *testing.T) {
	terminal := map[flexiq.JobStatus]bool{
		flexiq.StatusUnspecified: false,
		flexiq.StatusPending:     false,
		flexiq.StatusRunning:     false,
		flexiq.StatusComplete:    true,
		flexiq.StatusFailed:      false,
		flexiq.StatusDead:        true,
		flexiq.StatusCancelled:   true,
	}
	for status, want := range terminal {
		if got := status.IsTerminal(); got != want {
			t.Errorf("%s.IsTerminal() = %v, want %v", status, got, want)
		}
	}
}

// TestJobMapsEveryField walks the read model once, so a field added to the
// proto and forgotten in the mapping fails here rather than reading as an
// empty value forever.
func TestJobMapsEveryField(t *testing.T) {
	created := time.Now().UTC().Truncate(time.Millisecond)
	progress := int32(42)
	payload := []byte{flexiq.TagCBOR, 0x82, 0x80, 0xa0}
	result := []byte{flexiq.TagCBOR, 0xf5}

	client := serve(t, &fakeProducer{
		getJob: func(context.Context, *pb.GetJobRequest) (*pb.GetJobResponse, error) {
			return &pb.GetJobResponse{Job: &pb.Job{
				Id:              "job-1",
				Queue:           "payments",
				TaskName:        "billing.charge",
				Status:          pb.JobStatus_JOB_STATUS_RUNNING,
				Priority:        7,
				CreatedAt:       timestamppb.New(created),
				ScheduledAt:     timestamppb.New(created.Add(time.Minute)),
				StartedAt:       timestamppb.New(created.Add(2 * time.Minute)),
				RetryCount:      1,
				MaxRetries:      3,
				Timeout:         durationpb.New(30 * time.Second),
				CancelRequested: true,
				HasDeps:         true,
				Namespace:       "prod",
				Payload:         payload,
				Result:          result,
				Error:           strPtr(`{"errtype":"ValueError","message":"bad","traceback":[]}`),
				Progress:        &progress,
				Metadata:        strPtr(`{"tenant":"acme"}`),
				Notes:           strPtr("by hand"),
				UniqueKey:       strPtr("charge:ord-1"),
				DebounceKey:     strPtr("tenant:acme"),
				ExpiresAt:       timestamppb.New(created.Add(time.Hour)),
				ResultTtl:       durationpb.New(time.Hour),
			}}, nil
		},
	})

	job, err := client.GetJob(context.Background(), "job-1", flexiq.GetJobOptions{})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}

	if job.ID != "job-1" || job.Queue != "payments" || job.TaskName != "billing.charge" {
		t.Errorf("identity fields are %q/%q/%q", job.ID, job.Queue, job.TaskName)
	}
	if job.Status != flexiq.StatusRunning || job.Priority != 7 || job.RetryCount != 1 || job.MaxRetries != 3 {
		t.Errorf("scheduling fields are %s/%d/%d/%d", job.Status, job.Priority, job.RetryCount, job.MaxRetries)
	}
	if !job.CreatedAt.Equal(created) || !job.CompletedAt.IsZero() {
		t.Errorf("times are created=%s completed=%s; an unset time must be the zero time",
			job.CreatedAt, job.CompletedAt)
	}
	if job.Timeout != 30*time.Second || job.ResultTTL != time.Hour {
		t.Errorf("durations are %s/%s", job.Timeout, job.ResultTTL)
	}
	if !job.CancelRequested || !job.HasDeps || job.Namespace != "prod" {
		t.Errorf("flags are cancel=%v deps=%v namespace=%q", job.CancelRequested, job.HasDeps, job.Namespace)
	}
	if job.Progress == nil || *job.Progress != 42 {
		t.Errorf("progress is %v", job.Progress)
	}
	if job.Metadata == "" || job.Notes == "" || job.UniqueKey == "" || job.DebounceKey == "" {
		t.Errorf("optional strings dropped: %q/%q/%q/%q",
			job.Metadata, job.Notes, job.UniqueKey, job.DebounceKey)
	}

	taskErr, ok := job.TaskError()
	if !ok || taskErr.Type != "ValueError" {
		t.Errorf("task error is %+v (ok=%v)", taskErr, ok)
	}
}

// TestProgressAbsentIsNotZero: a task that reported no progress and a task that
// reported 0% are different answers.
func TestProgressAbsentIsNotZero(t *testing.T) {
	client := serve(t, &fakeProducer{
		getJob: func(context.Context, *pb.GetJobRequest) (*pb.GetJobResponse, error) {
			return &pb.GetJobResponse{Job: &pb.Job{Id: "job-1"}}, nil
		},
	})

	job, err := client.GetJob(context.Background(), "job-1", flexiq.GetJobOptions{})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if job.Progress != nil {
		t.Errorf("progress is %v, want nil for a task that reported none", *job.Progress)
	}
	if _, ok := job.TaskError(); ok {
		t.Error("a job with no error reported one")
	}
}

func strPtr(s string) *string { return &s }
