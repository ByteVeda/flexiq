package tests

import (
	"context"
	"testing"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	pb "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/v1"
	"google.golang.org/protobuf/proto"
)

// TestGetJobLeavesThePayloadOutByDefault pins the default a reader relies on: a
// payload is the largest thing a job carries, so it comes back only when asked
// for.
func TestGetJobLeavesThePayloadOutByDefault(t *testing.T) {
	var got *pb.GetJobRequest
	fake := &fakeProducer{
		getJob: func(_ context.Context, req *pb.GetJobRequest) (*pb.GetJobResponse, error) {
			got = req
			return &pb.GetJobResponse{Job: &pb.Job{Id: req.GetJobId()}}, nil
		},
	}
	client := serve(t, fake)
	ctx := context.Background()

	job, err := client.GetJob(ctx, "job-1", flexiq.GetJobOptions{})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.GetIncludePayload() || got.GetIncludeResult() {
		t.Error("a plain read asked for the payload or the result")
	}
	if job.Payload != nil {
		t.Error("a job that carries no payload came back with one")
	}

	if _, err := client.GetJob(ctx, "job-1", flexiq.GetJobOptions{
		IncludePayload: true,
		IncludeResult:  true,
	}); err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if !got.GetIncludePayload() || !got.GetIncludeResult() {
		t.Error("asking for the payload and result did not reach the wire")
	}
}

// TestGetJobDecodesPayloadAndResult covers the shape difference that catches
// people: a payload is the tag then an array, a result is the tag then a bare
// value.
func TestGetJobDecodesPayloadAndResult(t *testing.T) {
	payload := mustHex(t, "028282016161a0")
	result := mustHex(t, "02f5")
	client := serve(t, &fakeProducer{
		getJob: func(_ context.Context, req *pb.GetJobRequest) (*pb.GetJobResponse, error) {
			return &pb.GetJobResponse{Job: &pb.Job{
				Id:      req.GetJobId(),
				Status:  pb.JobStatus_JOB_STATUS_COMPLETE,
				Payload: payload,
				Result:  result,
			}}, nil
		},
	})

	job, err := client.GetJob(context.Background(), "job-1", flexiq.GetJobOptions{
		IncludePayload: true,
		IncludeResult:  true,
	})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}

	call, err := job.DecodePayload()
	if err != nil {
		t.Fatalf("DecodePayload: %v", err)
	}
	if len(call.Args) != 2 || call.Args[1] != "a" {
		t.Errorf("decoded args are %#v", call.Args)
	}
	if call.Kwargs == nil {
		t.Error("kwargs decoded to nil; the call body always carries the map")
	}

	var ok bool
	if err := job.DecodeResult(&ok); err != nil {
		t.Fatalf("DecodeResult: %v", err)
	}
	if !ok {
		t.Error("decoded result is false, want true")
	}
}

// TestListJobsLeavesUnsetFiltersUnset: an unset status lists every status,
// where sending JOB_STATUS_UNSPECIFIED would filter for a status no job has.
func TestListJobsLeavesUnsetFiltersUnset(t *testing.T) {
	var got *pb.ListJobsRequest
	fake := &fakeProducer{
		listJobs: func(_ context.Context, req *pb.ListJobsRequest) (*pb.ListJobsResponse, error) {
			got = req
			return &pb.ListJobsResponse{}, nil
		},
	}
	client := serve(t, fake)
	ctx := context.Background()

	if _, err := client.ListJobs(ctx, flexiq.ListJobsQuery{}); err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if got.Status != nil || got.Queue != nil || got.TaskName != nil {
		t.Errorf("an unfiltered listing sent filters: %v", got)
	}

	if _, err := client.ListJobs(ctx, flexiq.ListJobsQuery{
		Status:   flexiq.StatusDead,
		Queue:    "payments",
		TaskName: "billing.charge",
		PageSize: 25,
	}); err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if got.GetStatus() != pb.JobStatus_JOB_STATUS_DEAD || got.GetQueue() != "payments" ||
		got.GetTaskName() != "billing.charge" || got.GetPageSize() != 25 {
		t.Errorf("filters did not reach the wire: %v", got)
	}
}

// TestAllJobsPagesWithTheServersToken proves the token is passed back verbatim
// and never built: it is opaque, and one that does not decode is refused.
func TestAllJobsPagesWithTheServersToken(t *testing.T) {
	pages := map[string]*pb.ListJobsResponse{
		"": {
			Jobs:          []*pb.Job{{Id: "job-1"}, {Id: "job-2"}},
			NextPageToken: "opaque-cursor",
		},
		"opaque-cursor": {
			Jobs: []*pb.Job{{Id: "job-3"}},
		},
	}
	var tokensSeen []string
	client := serve(t, &fakeProducer{
		listJobs: func(_ context.Context, req *pb.ListJobsRequest) (*pb.ListJobsResponse, error) {
			tokensSeen = append(tokensSeen, req.GetPageToken())
			page, ok := pages[req.GetPageToken()]
			if !ok {
				t.Errorf("client invented a page token: %q", req.GetPageToken())
				return &pb.ListJobsResponse{}, nil
			}
			return proto.Clone(page).(*pb.ListJobsResponse), nil
		},
	})

	var ids []string
	for job, err := range client.AllJobs(context.Background(), flexiq.ListJobsQuery{}) {
		if err != nil {
			t.Fatalf("AllJobs: %v", err)
		}
		ids = append(ids, job.ID)
	}

	if len(ids) != 3 || ids[0] != "job-1" || ids[2] != "job-3" {
		t.Errorf("iterated %v, want all three jobs in order", ids)
	}
	if len(tokensSeen) != 2 || tokensSeen[0] != "" || tokensSeen[1] != "opaque-cursor" {
		t.Errorf("page tokens sent were %v", tokensSeen)
	}
}

// TestAllJobsStopsEarlyWithoutFetchingMore: breaking out of the range stops the
// paging, rather than reading the whole listing into memory first.
func TestAllJobsStopsEarlyWithoutFetchingMore(t *testing.T) {
	calls := 0
	client := serve(t, &fakeProducer{
		listJobs: func(context.Context, *pb.ListJobsRequest) (*pb.ListJobsResponse, error) {
			calls++
			return &pb.ListJobsResponse{
				Jobs:          []*pb.Job{{Id: "job-1"}, {Id: "job-2"}},
				NextPageToken: "more",
			}, nil
		},
	})

	for range client.AllJobs(context.Background(), flexiq.ListJobsQuery{}) {
		break
	}
	if calls != 1 {
		t.Errorf("fetched %d pages after an early break, want 1", calls)
	}
}
