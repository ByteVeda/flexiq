package tests

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	pb "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/v1"
)

func transitionItem(id string, status pb.JobStatus, terminal bool) *pb.WatchJobsResponse {
	return &pb.WatchJobsResponse{
		Item: &pb.WatchJobsResponse_Transition{Transition: &pb.JobTransition{
			JobId:    id,
			Queue:    "q",
			TaskName: "t",
			Kind:     pb.JobTransitionKind_JOB_TRANSITION_KIND_SNAPSHOT,
			Status:   status,
			Terminal: terminal,
		}},
	}
}

func refusal(t *testing.T, code codes.Code, reason flexiq.Reason) error {
	t.Helper()
	st, err := status.New(code, "refused").WithDetails(&errdetails.ErrorInfo{
		Domain: flexiq.ErrorDomain,
		Reason: string(reason),
	})
	if err != nil {
		t.Fatalf("attach details: %v", err)
	}
	return st.Err()
}

// TestWatchJobsYieldsEachItemAndEndsWithTheStream: the ids go out as the
// request, a snapshot and a not-found come back, and the iteration ends when
// the server ends the stream.
func TestWatchJobsYieldsEachItemAndEndsWithTheStream(t *testing.T) {
	var asked []string
	client := serve(t, &fakeProducer{
		watchJobs: func(req *pb.WatchJobsRequest, stream pb.ProducerService_WatchJobsServer) error {
			asked = req.GetJobIds().GetJobIds()
			if err := stream.Send(transitionItem("a", pb.JobStatus_JOB_STATUS_COMPLETE, true)); err != nil {
				return err
			}
			return stream.Send(&pb.WatchJobsResponse{
				Item: &pb.WatchJobsResponse_NotFoundJobId{NotFoundJobId: "b"},
			})
		},
	})

	var got []flexiq.Transition
	for transition, err := range client.WatchJobs(context.Background(), "a", "b") {
		if err != nil {
			t.Fatalf("WatchJobs: %v", err)
		}
		got = append(got, transition)
	}

	if len(asked) != 2 || asked[0] != "a" || asked[1] != "b" {
		t.Errorf("request named %v, want [a b]", asked)
	}
	if len(got) != 2 {
		t.Fatalf("got %d items, want 2", len(got))
	}
	if got[0].Kind != flexiq.TransitionSnapshot || got[0].Status != flexiq.StatusComplete || !got[0].Terminal {
		t.Errorf("first item is %+v", got[0])
	}
	if !got[1].NotFound || !got[1].Terminal || got[1].JobID != "b" {
		t.Errorf("second item is %+v, want a terminal not-found for b", got[1])
	}
}

// TestWatchQueueSendsItsCursorAndExposesEachItems: resuming is the caller's
// job on a queue watch, so the cursor has to make the round trip.
func TestWatchQueueSendsItsCursorAndExposesEachItems(t *testing.T) {
	var req *pb.WatchJobsRequest
	client := serve(t, &fakeProducer{
		watchJobs: func(r *pb.WatchJobsRequest, stream pb.ProducerService_WatchJobsServer) error {
			req = r
			item := transitionItem("a", pb.JobStatus_JOB_STATUS_PENDING, false)
			item.Cursor = "c2"
			if err := stream.Send(item); err != nil {
				return err
			}
			return refusal(t, codes.FailedPrecondition, flexiq.ReasonWatchCursorExpired)
		},
	})

	var cursor string
	var last error
	for transition, err := range client.WatchQueue(context.Background(), "orders", "c1") {
		if err != nil {
			last = err
			break
		}
		cursor = transition.Cursor
	}
	if req.GetQueue() != "orders" || req.GetResumeCursor() != "c1" {
		t.Errorf("request is %v, want queue orders from c1", req)
	}
	if cursor != "c2" {
		t.Errorf("cursor is %q, want c2", cursor)
	}
	if !errors.Is(last, flexiq.ReasonWatchCursorExpired) {
		t.Errorf("the stream's error is %v, want WATCH_CURSOR_EXPIRED", last)
	}
}

// TestWaitReopensADroppedWatchThenReadsTheResult is the helper the issue asks
// for: a dropped stream is reopened, and the finished job comes back with its
// result from GetJob.
func TestWaitReopensADroppedWatchThenReadsTheResult(t *testing.T) {
	var opened atomic.Int32
	var includedResult atomic.Bool
	client := serve(t, &fakeProducer{
		watchJobs: func(_ *pb.WatchJobsRequest, stream pb.ProducerService_WatchJobsServer) error {
			if opened.Add(1) == 1 {
				if err := stream.Send(transitionItem("j", pb.JobStatus_JOB_STATUS_RUNNING, false)); err != nil {
					return err
				}
				return refusal(t, codes.Unavailable, flexiq.ReasonShuttingDown)
			}
			return stream.Send(transitionItem("j", pb.JobStatus_JOB_STATUS_COMPLETE, true))
		},
		getJob: func(_ context.Context, req *pb.GetJobRequest) (*pb.GetJobResponse, error) {
			includedResult.Store(req.GetIncludeResult())
			return &pb.GetJobResponse{Job: &pb.Job{
				Id:     req.GetJobId(),
				Status: pb.JobStatus_JOB_STATUS_COMPLETE,
				Result: []byte{0xf6},
			}}, nil
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	job, err := client.Wait(ctx, "j")
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if opened.Load() != 2 {
		t.Errorf("opened %d watches, want 2", opened.Load())
	}
	if !includedResult.Load() || job.Status != flexiq.StatusComplete || len(job.Result) != 1 {
		t.Errorf("job is %+v, want the completed job with its result", job)
	}
}

// TestWaitGivesUpOnARefusalThatWillNotClear: retrying a bad request forever
// would hang the caller on a mistake.
func TestWaitGivesUpOnARefusalThatWillNotClear(t *testing.T) {
	client := serve(t, &fakeProducer{
		watchJobs: func(*pb.WatchJobsRequest, pb.ProducerService_WatchJobsServer) error {
			return refusal(t, codes.InvalidArgument, flexiq.ReasonInvalidRequest)
		},
	})
	_, err := client.Wait(context.Background(), "j")
	if !errors.Is(err, flexiq.ReasonInvalidRequest) {
		t.Fatalf("Wait returned %v, want INVALID_REQUEST", err)
	}
}

// TestWaitTreatsTheWatchCapAsTransientOnlyAfterAWatchOpened: the cap refusing
// the first open is real, but refusing a reopen can be the dropped stream's
// slot the server has not released yet.
func TestWaitTreatsTheWatchCapAsTransientOnlyAfterAWatchOpened(t *testing.T) {
	first := serve(t, &fakeProducer{
		watchJobs: func(*pb.WatchJobsRequest, pb.ProducerService_WatchJobsServer) error {
			return refusal(t, codes.ResourceExhausted, flexiq.ReasonWatchLimit)
		},
	})
	if _, err := first.Wait(context.Background(), "j"); !errors.Is(err, flexiq.ReasonWatchLimit) {
		t.Fatalf("Wait returned %v, want WATCH_LIMIT at once", err)
	}

	var opened atomic.Int32
	reopened := serve(t, &fakeProducer{
		watchJobs: func(_ *pb.WatchJobsRequest, stream pb.ProducerService_WatchJobsServer) error {
			switch opened.Add(1) {
			case 1:
				if err := stream.Send(transitionItem("j", pb.JobStatus_JOB_STATUS_RUNNING, false)); err != nil {
					return err
				}
				return status.Error(codes.Unavailable, "connection dropped")
			case 2:
				return refusal(t, codes.ResourceExhausted, flexiq.ReasonWatchLimit)
			default:
				return stream.Send(transitionItem("j", pb.JobStatus_JOB_STATUS_COMPLETE, true))
			}
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := reopened.Wait(ctx, "j"); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if opened.Load() != 3 {
		t.Errorf("opened %d watches, want 3", opened.Load())
	}
}

// TestEnqueueAndWaitWaitsForTheJobItEnqueued.
func TestEnqueueAndWaitWaitsForTheJobItEnqueued(t *testing.T) {
	var watched string
	client := serve(t, &fakeProducer{
		watchJobs: func(req *pb.WatchJobsRequest, stream pb.ProducerService_WatchJobsServer) error {
			watched = req.GetJobIds().GetJobIds()[0]
			return stream.Send(transitionItem(watched, pb.JobStatus_JOB_STATUS_COMPLETE, true))
		},
	})

	job, err := client.EnqueueAndWait(context.Background(), flexiq.EnqueueRequest{Task: "t"})
	if err != nil {
		t.Fatalf("EnqueueAndWait: %v", err)
	}
	if watched != "job-1" || job.ID != "job-1" {
		t.Errorf("watched %q and read %q, want job-1 for both", watched, job.ID)
	}
}
