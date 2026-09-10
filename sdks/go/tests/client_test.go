package tests

import (
	"context"
	"errors"
	"strings"
	"testing"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	pb "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/v1"
	"google.golang.org/grpc/codes"
)

// TestEveryCallCarriesTheBearerToken covers the rule with no exception on this
// door: there is no anonymous path, so the credential rides on the call rather
// than on a call site that can forget it.
func TestEveryCallCarriesTheBearerToken(t *testing.T) {
	fake := &fakeProducer{}
	client := serve(t, fake)
	ctx := context.Background()

	if _, err := client.Enqueue(ctx, flexiq.EnqueueRequest{Task: "t"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	assertBearer(t, fake)

	if _, err := client.GetJob(ctx, "job-1", flexiq.GetJobOptions{}); err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	assertBearer(t, fake)
}

func assertBearer(t *testing.T, fake *fakeProducer) {
	t.Helper()

	got := fake.metadata.Get("authorization")
	if len(got) != 1 {
		t.Fatalf("want exactly one authorization header, got %v", got)
	}
	if want := "Bearer " + testToken; got[0] != want {
		t.Errorf("authorization header is %q, want %q", got[0], want)
	}
}

// TestClientNamesItselfInTheUserAgent keeps a server log able to say which
// client build called it.
func TestClientNamesItselfInTheUserAgent(t *testing.T) {
	fake := &fakeProducer{}
	client := serve(t, fake)

	if _, err := client.Enqueue(context.Background(), flexiq.EnqueueRequest{Task: "t"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	agents := fake.metadata.Get("user-agent")
	if len(agents) != 1 || !strings.Contains(agents[0], "flexiq-go/"+flexiq.Version) {
		t.Errorf("user-agent is %v, want it to name flexiq-go/%s", agents, flexiq.Version)
	}
}

// TestNewRequiresAToken fails the caller at construction rather than on the
// first call, where the refusal would look like a server problem.
func TestNewRequiresAToken(t *testing.T) {
	_, err := flexiq.New("localhost:50051")
	if !errors.Is(err, flexiq.ErrNoToken) {
		t.Fatalf("want ErrNoToken, got %v", err)
	}
}

// TestOversizePayloadFailsBeforeItIsSent pins the reason this client caps the
// send direction: grpc-go leaves it unbounded, so without the cap an oversized
// payload would travel the network to be refused at the far end.
func TestOversizePayloadFailsBeforeItIsSent(t *testing.T) {
	fake := &fakeProducer{}
	client := serve(t, fake)

	oversize := make([]byte, flexiq.MaxMessageBytes+1024)
	oversize[0] = flexiq.TagCBOR
	_, err := client.Enqueue(context.Background(), flexiq.EnqueueRequest{Task: "t", Raw: oversize})
	if err == nil {
		t.Fatal("an oversized payload was accepted")
	}

	wireErr, ok := flexiq.AsError(err)
	if !ok {
		t.Fatalf("want a *flexiq.Error, got %T: %v", err, err)
	}
	if wireErr.Code != codes.ResourceExhausted {
		t.Errorf("code is %s, want ResourceExhausted", wireErr.Code)
	}
	if fake.calls != 0 {
		t.Errorf("the request reached the server %d times; it should have failed locally", fake.calls)
	}
}

// TestCancelJobReportsResultingState covers the shape that makes a second call
// safe: the response describes where the job stands, not what the call did.
func TestCancelJobReportsResultingState(t *testing.T) {
	fake := &fakeProducer{
		cancelJob: func(_ context.Context, req *pb.CancelJobRequest) (*pb.CancelJobResponse, error) {
			return &pb.CancelJobResponse{Job: &pb.Job{
				Id:              req.GetJobId(),
				Status:          pb.JobStatus_JOB_STATUS_RUNNING,
				CancelRequested: true,
			}}, nil
		},
	}
	client := serve(t, fake)

	job, err := client.CancelJob(context.Background(), "job-7")
	if err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	if job.Status != flexiq.StatusRunning || !job.CancelRequested {
		t.Errorf("got status %s cancelRequested=%v, want RUNNING with a cancel requested",
			job.Status, job.CancelRequested)
	}
	if job.Status.IsTerminal() {
		t.Error("a running job with a cancel requested is not terminal yet")
	}
}

func TestQueueStats(t *testing.T) {
	var gotQueue *string
	fake := &fakeProducer{
		queueStats: func(_ context.Context, req *pb.QueueStatsRequest) (*pb.QueueStatsResponse, error) {
			gotQueue = req.Queue
			return &pb.QueueStatsResponse{Pending: 3, Running: 1, Dead: 2}, nil
		},
	}
	client := serve(t, fake)
	ctx := context.Background()

	stats, err := client.QueueStats(ctx, "payments")
	if err != nil {
		t.Fatalf("QueueStats: %v", err)
	}
	if stats.Pending != 3 || stats.Running != 1 || stats.Dead != 2 {
		t.Errorf("counts are %+v, want pending 3, running 1, dead 2", stats)
	}
	if gotQueue == nil || *gotQueue != "payments" {
		t.Errorf("queue filter is %v, want payments", gotQueue)
	}

	if _, err := client.QueueStats(ctx, ""); err != nil {
		t.Fatalf("QueueStats: %v", err)
	}
	if gotQueue != nil {
		// An empty queue counts the namespace, and the way to ask for that is
		// to leave the field unset — not to send "".
		t.Errorf("queue filter is %q, want it left unset", *gotQueue)
	}
}
