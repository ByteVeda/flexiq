package tests

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	"github.com/ByteVeda/flexiq/sdks/go/v2/admin"
	adminv1 "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/admin/v1"
)

// fakeAdmin is an AdminService double, built like fakeProducer: every RPC a
// test is about is a function field. A field a test left nil answers
// UNIMPLEMENTED, as does any RPC with no field, through the embed.
type fakeAdmin struct {
	adminv1.UnimplementedAdminServiceServer

	listQueues  func(context.Context, *adminv1.ListQueuesRequest) (*adminv1.ListQueuesResponse, error)
	pauseQueue  func(context.Context, *adminv1.PauseQueueRequest) (*adminv1.PauseQueueResponse, error)
	resumeQueue func(context.Context, *adminv1.ResumeQueueRequest) (*adminv1.ResumeQueueResponse, error)

	listOverrides      func(context.Context, *adminv1.ListOverridesRequest) (*adminv1.ListOverridesResponse, error)
	setQueueOverride   func(context.Context, *adminv1.SetQueueOverrideRequest) (*adminv1.SetQueueOverrideResponse, error)
	clearQueueOverride func(context.Context, *adminv1.ClearQueueOverrideRequest) (*adminv1.ClearQueueOverrideResponse, error)

	listPeriodic   func(context.Context, *adminv1.ListPeriodicTasksRequest) (*adminv1.ListPeriodicTasksResponse, error)
	getPeriodic    func(context.Context, *adminv1.GetPeriodicTaskRequest) (*adminv1.GetPeriodicTaskResponse, error)
	putPeriodic    func(context.Context, *adminv1.PutPeriodicTaskRequest) (*adminv1.PutPeriodicTaskResponse, error)
	deletePeriodic func(context.Context, *adminv1.DeletePeriodicTaskRequest) (*adminv1.DeletePeriodicTaskResponse, error)
	pausePeriodic  func(context.Context, *adminv1.PausePeriodicTaskRequest) (*adminv1.PausePeriodicTaskResponse, error)
	resumePeriodic func(context.Context, *adminv1.ResumePeriodicTaskRequest) (*adminv1.ResumePeriodicTaskResponse, error)

	getQuota   func(context.Context, *adminv1.GetNamespaceQuotaRequest) (*adminv1.GetNamespaceQuotaResponse, error)
	setQuota   func(context.Context, *adminv1.SetNamespaceQuotaRequest) (*adminv1.SetNamespaceQuotaResponse, error)
	clearQuota func(context.Context, *adminv1.ClearNamespaceQuotaRequest) (*adminv1.ClearNamespaceQuotaResponse, error)

	createToken func(context.Context, *adminv1.CreateTokenRequest) (*adminv1.CreateTokenResponse, error)
	getToken    func(context.Context, *adminv1.GetTokenRequest) (*adminv1.GetTokenResponse, error)
	listTokens  func(context.Context, *adminv1.ListTokensRequest) (*adminv1.ListTokensResponse, error)
	revokeToken func(context.Context, *adminv1.RevokeTokenRequest) (*adminv1.RevokeTokenResponse, error)

	// calls counts every RPC that reached the server, so a test can prove a
	// request never went out.
	calls int
	// metadata is what the last call carried.
	metadata metadata.MD
}

func (f *fakeAdmin) record(ctx context.Context) {
	f.calls++
	f.metadata, _ = metadata.FromIncomingContext(ctx)
}

// answer records the call and runs fn, or answers UNIMPLEMENTED when the test
// left fn nil — a status the client can report, where a nil call would panic
// inside the server goroutine.
func answer[Req, Resp any](ctx context.Context, f *fakeAdmin, fn func(context.Context, Req) (Resp, error), req Req) (Resp, error) {
	f.record(ctx)
	if fn == nil {
		var zero Resp
		return zero, status.Error(codes.Unimplemented, "fakeAdmin: no handler for this RPC")
	}
	return fn(ctx, req)
}

func (f *fakeAdmin) ListQueues(ctx context.Context, req *adminv1.ListQueuesRequest) (*adminv1.ListQueuesResponse, error) {
	return answer(ctx, f, f.listQueues, req)
}

func (f *fakeAdmin) PauseQueue(ctx context.Context, req *adminv1.PauseQueueRequest) (*adminv1.PauseQueueResponse, error) {
	return answer(ctx, f, f.pauseQueue, req)
}

func (f *fakeAdmin) ResumeQueue(ctx context.Context, req *adminv1.ResumeQueueRequest) (*adminv1.ResumeQueueResponse, error) {
	return answer(ctx, f, f.resumeQueue, req)
}

func (f *fakeAdmin) ListOverrides(ctx context.Context, req *adminv1.ListOverridesRequest) (*adminv1.ListOverridesResponse, error) {
	return answer(ctx, f, f.listOverrides, req)
}

func (f *fakeAdmin) SetQueueOverride(ctx context.Context, req *adminv1.SetQueueOverrideRequest) (*adminv1.SetQueueOverrideResponse, error) {
	return answer(ctx, f, f.setQueueOverride, req)
}

func (f *fakeAdmin) ClearQueueOverride(ctx context.Context, req *adminv1.ClearQueueOverrideRequest) (*adminv1.ClearQueueOverrideResponse, error) {
	return answer(ctx, f, f.clearQueueOverride, req)
}

func (f *fakeAdmin) ListPeriodicTasks(ctx context.Context, req *adminv1.ListPeriodicTasksRequest) (*adminv1.ListPeriodicTasksResponse, error) {
	return answer(ctx, f, f.listPeriodic, req)
}

func (f *fakeAdmin) GetPeriodicTask(ctx context.Context, req *adminv1.GetPeriodicTaskRequest) (*adminv1.GetPeriodicTaskResponse, error) {
	return answer(ctx, f, f.getPeriodic, req)
}

func (f *fakeAdmin) PutPeriodicTask(ctx context.Context, req *adminv1.PutPeriodicTaskRequest) (*adminv1.PutPeriodicTaskResponse, error) {
	return answer(ctx, f, f.putPeriodic, req)
}

func (f *fakeAdmin) DeletePeriodicTask(ctx context.Context, req *adminv1.DeletePeriodicTaskRequest) (*adminv1.DeletePeriodicTaskResponse, error) {
	return answer(ctx, f, f.deletePeriodic, req)
}

func (f *fakeAdmin) PausePeriodicTask(ctx context.Context, req *adminv1.PausePeriodicTaskRequest) (*adminv1.PausePeriodicTaskResponse, error) {
	return answer(ctx, f, f.pausePeriodic, req)
}

func (f *fakeAdmin) ResumePeriodicTask(ctx context.Context, req *adminv1.ResumePeriodicTaskRequest) (*adminv1.ResumePeriodicTaskResponse, error) {
	return answer(ctx, f, f.resumePeriodic, req)
}

func (f *fakeAdmin) GetNamespaceQuota(ctx context.Context, req *adminv1.GetNamespaceQuotaRequest) (*adminv1.GetNamespaceQuotaResponse, error) {
	return answer(ctx, f, f.getQuota, req)
}

func (f *fakeAdmin) SetNamespaceQuota(ctx context.Context, req *adminv1.SetNamespaceQuotaRequest) (*adminv1.SetNamespaceQuotaResponse, error) {
	return answer(ctx, f, f.setQuota, req)
}

func (f *fakeAdmin) ClearNamespaceQuota(ctx context.Context, req *adminv1.ClearNamespaceQuotaRequest) (*adminv1.ClearNamespaceQuotaResponse, error) {
	return answer(ctx, f, f.clearQuota, req)
}

func (f *fakeAdmin) CreateToken(ctx context.Context, req *adminv1.CreateTokenRequest) (*adminv1.CreateTokenResponse, error) {
	return answer(ctx, f, f.createToken, req)
}

func (f *fakeAdmin) GetToken(ctx context.Context, req *adminv1.GetTokenRequest) (*adminv1.GetTokenResponse, error) {
	return answer(ctx, f, f.getToken, req)
}

func (f *fakeAdmin) ListTokens(ctx context.Context, req *adminv1.ListTokensRequest) (*adminv1.ListTokensResponse, error) {
	return answer(ctx, f, f.listTokens, req)
}

func (f *fakeAdmin) RevokeToken(ctx context.Context, req *adminv1.RevokeTokenRequest) (*adminv1.RevokeTokenResponse, error) {
	return answer(ctx, f, f.revokeToken, req)
}

// serveAdmin starts the double and returns an admin client connected to it.
// Both are torn down when the test ends.
func serveAdmin(t *testing.T, fake *fakeAdmin) *admin.Client {
	t.Helper()

	listener := bufconn.Listen(64 * 1024)
	server := grpc.NewServer()
	adminv1.RegisterAdminServiceServer(server, fake)
	go func() {
		// Serve returns when the listener closes, which is the teardown path.
		_ = server.Serve(listener)
	}()

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}
	client, err := admin.New("passthrough:///bufnet",
		flexiq.WithToken(testToken),
		flexiq.WithInsecureTransport(),
		flexiq.WithGRPCDialOptions(grpc.WithContextDialer(dialer)),
	)
	if err != nil {
		t.Fatalf("admin.New: %v", err)
	}

	t.Cleanup(func() {
		_ = client.Close()
		server.Stop()
		_ = listener.Close()
	})
	return client
}
