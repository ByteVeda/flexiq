package tests

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	pb "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/v1"
)

// TestEnqueueSendsTheTaggedEnvelope proves the client encodes rather than
// deferring to the server's `structured` arm: the bytes on the wire are the
// same envelope every other SDK writes, pinned here against the vector the
// contract states.
func TestEnqueueSendsTheTaggedEnvelope(t *testing.T) {
	var got *pb.EnqueueRequest
	fake := &fakeProducer{
		enqueue: func(_ context.Context, req *pb.EnqueueRequest) (*pb.EnqueueResponse, error) {
			got = req
			return &pb.EnqueueResponse{Job: &pb.Job{Id: "job-1"}}, nil
		},
	}
	client := serve(t, fake)

	if _, err := client.Enqueue(context.Background(), flexiq.EnqueueRequest{
		Task: "billing.charge",
		Args: []any{1, "a"},
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if got.GetTaskName() != "billing.charge" {
		t.Errorf("task name is %q", got.GetTaskName())
	}
	// The vector from BINDING_CONTRACT.md: f(1, "a") with no kwargs.
	if want := "028282016161a0"; hex.EncodeToString(got.GetRaw()) != want {
		t.Errorf("payload is %s, want %s", hex.EncodeToString(got.GetRaw()), want)
	}
}

// TestEnqueueRawBodyReachesStorageUntouched covers the escape hatch: a caller
// that already holds an envelope — one read off another job — sends it as it
// is.
func TestEnqueueRawBodyReachesStorageUntouched(t *testing.T) {
	var got []byte
	fake := &fakeProducer{
		enqueue: func(_ context.Context, req *pb.EnqueueRequest) (*pb.EnqueueResponse, error) {
			got = req.GetRaw()
			return &pb.EnqueueResponse{Job: &pb.Job{Id: "job-1"}}, nil
		},
	}
	client := serve(t, fake)

	raw := mustHex(t, "028280a1616bf5")
	if _, err := client.Enqueue(context.Background(), flexiq.EnqueueRequest{
		Task: "t",
		Args: []any{"ignored"},
		Raw:  raw,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if hex.EncodeToString(got) != hex.EncodeToString(raw) {
		t.Errorf("payload is %s, want the raw bytes %s", hex.EncodeToString(got), hex.EncodeToString(raw))
	}
}

// TestEnqueueOptionsMapOntoTheWire walks the knobs a producer sets, including
// the three that carry explicit presence — a zero value has to arrive unset,
// or the server reads "" as a unique key nobody asked for.
func TestEnqueueOptionsMapOntoTheWire(t *testing.T) {
	var got *pb.EnqueueOptions
	fake := &fakeProducer{
		enqueue: func(_ context.Context, req *pb.EnqueueRequest) (*pb.EnqueueResponse, error) {
			got = req.GetOptions()
			return &pb.EnqueueResponse{Job: &pb.Job{Id: "job-1"}}, nil
		},
	}
	client := serve(t, fake)
	ctx := context.Background()

	runAt := time.Now().Add(time.Hour).UTC().Truncate(time.Millisecond)
	maxPending := int64(500)
	if _, err := client.Enqueue(ctx, flexiq.EnqueueRequest{
		Task: "t",
		Options: flexiq.EnqueueOptions{
			Queue:       "payments",
			Priority:    5,
			MaxRetries:  3,
			ScheduledAt: runAt,
			Timeout:     30 * time.Second,
			UniqueKey:   "charge:ord-1",
			Metadata:    `{"tenant":"acme"}`,
			Notes:       "retried by hand",
			DependsOn:   []string{"job-a", "job-b"},
			ResultTTL:   time.Hour,
			Debounce: &flexiq.Debounce{
				Key:            "tenant:acme",
				Window:         5 * time.Second,
				MaxWait:        time.Minute,
				ReplacePayload: true,
				MaxPending:     &maxPending,
			},
		},
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if got.GetQueue() != "payments" || got.GetPriority() != 5 || got.GetMaxRetries() != 3 {
		t.Errorf("queue/priority/retries are %q/%d/%d", got.GetQueue(), got.GetPriority(), got.GetMaxRetries())
	}
	if !got.GetScheduledAt().AsTime().Equal(runAt) {
		t.Errorf("scheduled_at is %s, want %s", got.GetScheduledAt().AsTime(), runAt)
	}
	if got.GetTimeout().AsDuration() != 30*time.Second || got.GetResultTtl().AsDuration() != time.Hour {
		t.Errorf("timeout/result_ttl are %s/%s", got.GetTimeout().AsDuration(), got.GetResultTtl().AsDuration())
	}
	if got.GetUniqueKey() != "charge:ord-1" || got.GetNotes() != "retried by hand" {
		t.Errorf("unique_key/notes are %q/%q", got.GetUniqueKey(), got.GetNotes())
	}
	if len(got.GetDependsOn()) != 2 {
		t.Errorf("depends_on is %v", got.GetDependsOn())
	}
	debounce := got.GetDebounce()
	if debounce.GetKey() != "tenant:acme" || debounce.GetWindow().AsDuration() != 5*time.Second ||
		debounce.GetMaxWait().AsDuration() != time.Minute || !debounce.GetReplacePayload() ||
		debounce.GetMaxPending() != 500 {
		t.Errorf("debounce is %+v", debounce)
	}

	// And the same call with nothing set: the optional fields must be absent,
	// not empty.
	if _, err := client.Enqueue(ctx, flexiq.EnqueueRequest{Task: "t"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if got.UniqueKey != nil || got.Metadata != nil || got.Notes != nil {
		t.Errorf("unset options arrived as empty values: unique_key=%v metadata=%v notes=%v",
			got.UniqueKey, got.Metadata, got.Notes)
	}
	if got.ScheduledAt != nil || got.Timeout != nil || got.ExpiresAt != nil ||
		got.ResultTtl != nil || got.Debounce != nil {
		t.Error("unset times, durations and debounce arrived set")
	}
}

// TestEnqueueReportsDeduplication covers the one response field that describes
// what the call did rather than the state it left: without it, a deduplicated
// enqueue and a fresh one are indistinguishable.
func TestEnqueueReportsDeduplication(t *testing.T) {
	fake := &fakeProducer{
		enqueue: func(context.Context, *pb.EnqueueRequest) (*pb.EnqueueResponse, error) {
			return &pb.EnqueueResponse{
				Job:          &pb.Job{Id: "job-1", Status: pb.JobStatus_JOB_STATUS_PENDING},
				Deduplicated: true,
			}, nil
		},
	}
	client := serve(t, fake)

	result, err := client.Enqueue(context.Background(), flexiq.EnqueueRequest{
		Task:    "t",
		Options: flexiq.EnqueueOptions{UniqueKey: "k"},
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if !result.Deduplicated {
		t.Error("the response said deduplicated and the client did not")
	}
	if result.Job.ID != "job-1" {
		t.Errorf("job id is %q", result.Job.ID)
	}
}

func TestEnqueueRejectsAnEmptyTaskName(t *testing.T) {
	fake := &fakeProducer{}
	client := serve(t, fake)

	if _, err := client.Enqueue(context.Background(), flexiq.EnqueueRequest{}); err == nil {
		t.Fatal("an empty task name was accepted")
	}
	if fake.calls != 0 {
		t.Error("the request went out anyway")
	}
}

// TestBatchPartialFailure is the shape where the backend could apply part of
// the batch: the RPC succeeds and the failure is per item.
func TestBatchPartialFailure(t *testing.T) {
	itemErr, err := status.New(codes.ResourceExhausted, "queue `payments` is full").
		WithDetails(&errdetails.ErrorInfo{
			Domain:   flexiq.ErrorDomain,
			Reason:   string(flexiq.ReasonQueueFull),
			Metadata: map[string]string{"queue": "payments", "pending": "1001", "cap": "1000"},
		})
	if err != nil {
		t.Fatalf("build item error: %v", err)
	}

	fake := &fakeProducer{
		enqueueBatch: func(_ context.Context, req *pb.EnqueueBatchRequest) (*pb.EnqueueBatchResponse, error) {
			if len(req.GetItems()) != 3 {
				t.Errorf("server received %d items, want 3", len(req.GetItems()))
			}
			return &pb.EnqueueBatchResponse{Results: []*pb.EnqueueBatchItemResult{
				{Outcome: &pb.EnqueueBatchItemResult_Enqueued{
					Enqueued: &pb.EnqueueResponse{Job: &pb.Job{Id: "job-1"}},
				}},
				{Outcome: &pb.EnqueueBatchItemResult_Error{Error: itemErr.Proto()}},
				// An outcome this build has no name for.
				{},
			}}, nil
		},
	}
	client := serve(t, fake)

	results, err := client.EnqueueBatch(context.Background(), []flexiq.EnqueueRequest{
		{Task: "t"}, {Task: "t"}, {Task: "t"},
	})
	if err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}

	if results[0].Err != nil || results[0].Result == nil || results[0].Result.Job.ID != "job-1" {
		t.Errorf("item 0 should be durable, got %+v", results[0])
	}

	if results[1].Result != nil {
		t.Error("item 1 reported a job and an error at once")
	}
	failed, ok := flexiq.AsError(results[1].Err)
	if !ok {
		t.Fatalf("item 1 error is %T, want *flexiq.Error", results[1].Err)
	}
	if failed.Reason != flexiq.ReasonQueueFull {
		t.Errorf("item 1 reason is %q", failed.Reason)
	}
	if info, ok := failed.QueueFull(); !ok || info.Queue != "payments" || info.Cap != 1000 {
		t.Errorf("item 1 queue-full detail is %+v (ok=%v)", info, ok)
	}

	if results[2].Err == nil {
		t.Error("an unrecognised outcome arm was reported as a durable enqueue")
	}
}

// TestBatchRefusesAMiscountedResponse: a BatchResult carries no id of its own,
// so position is the whole mapping between a request and its outcome. A
// response of the wrong length has to fail rather than be paired up anyway,
// which would report one item's outcome against another item's request.
func TestBatchRefusesAMiscountedResponse(t *testing.T) {
	for _, tc := range []struct {
		name    string
		results []*pb.EnqueueBatchItemResult
	}{
		{"short", []*pb.EnqueueBatchItemResult{
			{Outcome: &pb.EnqueueBatchItemResult_Enqueued{
				Enqueued: &pb.EnqueueResponse{Job: &pb.Job{Id: "job-1"}},
			}},
		}},
		{"long", []*pb.EnqueueBatchItemResult{
			{Outcome: &pb.EnqueueBatchItemResult_Enqueued{
				Enqueued: &pb.EnqueueResponse{Job: &pb.Job{Id: "job-1"}},
			}},
			{Outcome: &pb.EnqueueBatchItemResult_Enqueued{
				Enqueued: &pb.EnqueueResponse{Job: &pb.Job{Id: "job-2"}},
			}},
			{Outcome: &pb.EnqueueBatchItemResult_Enqueued{
				Enqueued: &pb.EnqueueResponse{Job: &pb.Job{Id: "job-3"}},
			}},
		}},
		{"empty", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := serve(t, &fakeProducer{
				enqueueBatch: func(context.Context, *pb.EnqueueBatchRequest) (*pb.EnqueueBatchResponse, error) {
					return &pb.EnqueueBatchResponse{Results: tc.results}, nil
				},
			})

			results, err := client.EnqueueBatch(context.Background(), []flexiq.EnqueueRequest{
				{Task: "t"}, {Task: "t"},
			})
			if err == nil {
				t.Fatalf("a %s response was accepted as %d results", tc.name, len(results))
			}
			if results != nil {
				t.Error("results were returned beside the error; none of them are attributable")
			}
		})
	}
}

// TestBatchWholeRPCFailureNamesTheItem is the other shape: where the batch is
// one transaction, an item failure fails the call, because returning the
// earlier items as enqueued would report jobs that do not exist.
func TestBatchWholeRPCFailureNamesTheItem(t *testing.T) {
	failure, err := status.New(codes.InvalidArgument, "item 1 is not a shape this service accepts").
		WithDetails(&errdetails.ErrorInfo{
			Domain:   flexiq.ErrorDomain,
			Reason:   string(flexiq.ReasonInvalidRequest),
			Metadata: map[string]string{"index": "1"},
		})
	if err != nil {
		t.Fatalf("build error: %v", err)
	}

	client := serve(t, &fakeProducer{
		enqueueBatch: func(context.Context, *pb.EnqueueBatchRequest) (*pb.EnqueueBatchResponse, error) {
			return nil, failure.Err()
		},
	})

	results, err := client.EnqueueBatch(context.Background(), []flexiq.EnqueueRequest{{Task: "t"}, {Task: "t"}})
	if results != nil {
		t.Error("a failed batch returned results; none of them landed")
	}
	wireErr, ok := flexiq.AsError(err)
	if !ok {
		t.Fatalf("error is %T, want *flexiq.Error", err)
	}
	index, ok := wireErr.BatchIndex()
	if !ok || index != 1 {
		t.Errorf("batch index is %d (ok=%v), want 1", index, ok)
	}
}
