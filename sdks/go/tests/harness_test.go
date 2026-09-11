// Package tests holds the client's test suite.
//
// It sits beside the package rather than inside it on purpose: everything here
// reaches the client through its exported API, the same way a caller does, so
// a surface that is awkward to use is awkward to test. Nothing here can reach
// an unexported helper, and nothing should need to.
package tests

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	pb "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/v1"
)

// The harness: a ProducerService double on an in-process connection.
//
// It is a double rather than a real flexiq-server because what these tests are
// about is this client's half of the contract — the metadata it sends, the
// requests it builds, the errors it branches on. The bytes it puts on the wire
// are pinned against the cross-SDK vectors in wire_test.go, and the server's
// half is pinned by the server's own suite.

const testToken = "fqt_0123456789abcdef.secret"

// fakeProducer answers whatever a test tells it to. Every RPC is a function
// field, so a test states only the call it is about, and any other call fails
// loudly through the unimplemented embed.
type fakeProducer struct {
	pb.UnimplementedProducerServiceServer

	enqueue      func(context.Context, *pb.EnqueueRequest) (*pb.EnqueueResponse, error)
	enqueueBatch func(context.Context, *pb.EnqueueBatchRequest) (*pb.EnqueueBatchResponse, error)
	getJob       func(context.Context, *pb.GetJobRequest) (*pb.GetJobResponse, error)
	listJobs     func(context.Context, *pb.ListJobsRequest) (*pb.ListJobsResponse, error)
	cancelJob    func(context.Context, *pb.CancelJobRequest) (*pb.CancelJobResponse, error)
	queueStats   func(context.Context, *pb.QueueStatsRequest) (*pb.QueueStatsResponse, error)

	submitWorkflow func(context.Context, *pb.SubmitWorkflowRequest) (*pb.SubmitWorkflowResponse, error)
	getWorkflowRun func(context.Context, *pb.GetWorkflowRunRequest) (*pb.GetWorkflowRunResponse, error)

	// calls counts every RPC that reached the server, so a test can prove a
	// request never went out.
	calls int
	// metadata is what the last call carried.
	metadata metadata.MD
}

func (f *fakeProducer) record(ctx context.Context) {
	f.calls++
	f.metadata, _ = metadata.FromIncomingContext(ctx)
}

func (f *fakeProducer) Enqueue(ctx context.Context, req *pb.EnqueueRequest) (*pb.EnqueueResponse, error) {
	f.record(ctx)
	if f.enqueue == nil {
		return &pb.EnqueueResponse{Job: &pb.Job{Id: "job-1"}}, nil
	}
	return f.enqueue(ctx, req)
}

func (f *fakeProducer) EnqueueBatch(ctx context.Context, req *pb.EnqueueBatchRequest) (*pb.EnqueueBatchResponse, error) {
	f.record(ctx)
	return f.enqueueBatch(ctx, req)
}

func (f *fakeProducer) GetJob(ctx context.Context, req *pb.GetJobRequest) (*pb.GetJobResponse, error) {
	f.record(ctx)
	if f.getJob == nil {
		return &pb.GetJobResponse{Job: &pb.Job{Id: req.GetJobId()}}, nil
	}
	return f.getJob(ctx, req)
}

func (f *fakeProducer) ListJobs(ctx context.Context, req *pb.ListJobsRequest) (*pb.ListJobsResponse, error) {
	f.record(ctx)
	return f.listJobs(ctx, req)
}

func (f *fakeProducer) CancelJob(ctx context.Context, req *pb.CancelJobRequest) (*pb.CancelJobResponse, error) {
	f.record(ctx)
	return f.cancelJob(ctx, req)
}

func (f *fakeProducer) QueueStats(ctx context.Context, req *pb.QueueStatsRequest) (*pb.QueueStatsResponse, error) {
	f.record(ctx)
	return f.queueStats(ctx, req)
}

func (f *fakeProducer) SubmitWorkflow(ctx context.Context, req *pb.SubmitWorkflowRequest) (*pb.SubmitWorkflowResponse, error) {
	f.record(ctx)
	if f.submitWorkflow == nil {
		return &pb.SubmitWorkflowResponse{RunId: "run-1"}, nil
	}
	return f.submitWorkflow(ctx, req)
}

func (f *fakeProducer) GetWorkflowRun(ctx context.Context, req *pb.GetWorkflowRunRequest) (*pb.GetWorkflowRunResponse, error) {
	f.record(ctx)
	if f.getWorkflowRun == nil {
		return &pb.GetWorkflowRunResponse{Run: &pb.WorkflowRun{Id: req.GetRunId()}}, nil
	}
	return f.getWorkflowRun(ctx, req)
}

// serve starts the double and returns a client connected to it. Both are torn
// down when the test ends.
func serve(t *testing.T, fake *fakeProducer, opts ...flexiq.Option) *flexiq.Client {
	t.Helper()

	listener := bufconn.Listen(64 * 1024)
	server := grpc.NewServer()
	pb.RegisterProducerServiceServer(server, fake)
	go func() {
		// Serve returns when the listener closes, which is the teardown path.
		_ = server.Serve(listener)
	}()

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}
	base := []flexiq.Option{
		flexiq.WithToken(testToken),
		flexiq.WithInsecureTransport(),
		flexiq.WithGRPCDialOptions(grpc.WithContextDialer(dialer)),
	}
	client, err := flexiq.New("passthrough:///bufnet", append(base, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	t.Cleanup(func() {
		_ = client.Close()
		server.Stop()
		_ = listener.Close()
	})
	return client
}
