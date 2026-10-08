package tests

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	"github.com/ByteVeda/flexiq/sdks/go/v2/admin"
	adminv1 "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/admin/v1"
)

// fakeAdmin is an AdminService double, built like fakeProducer: every RPC a
// test is about is a function field, and any other call fails loudly through
// the unimplemented embed.
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

	getQuota func(context.Context, *adminv1.GetNamespaceQuotaRequest) (*adminv1.GetNamespaceQuotaResponse, error)
	setQuota func(context.Context, *adminv1.SetNamespaceQuotaRequest) (*adminv1.SetNamespaceQuotaResponse, error)

	createToken func(context.Context, *adminv1.CreateTokenRequest) (*adminv1.CreateTokenResponse, error)
	getToken    func(context.Context, *adminv1.GetTokenRequest) (*adminv1.GetTokenResponse, error)
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

func (f *fakeAdmin) ListQueues(ctx context.Context, req *adminv1.ListQueuesRequest) (*adminv1.ListQueuesResponse, error) {
	f.record(ctx)
	return f.listQueues(ctx, req)
}

func (f *fakeAdmin) PauseQueue(ctx context.Context, req *adminv1.PauseQueueRequest) (*adminv1.PauseQueueResponse, error) {
	f.record(ctx)
	return f.pauseQueue(ctx, req)
}

func (f *fakeAdmin) ResumeQueue(ctx context.Context, req *adminv1.ResumeQueueRequest) (*adminv1.ResumeQueueResponse, error) {
	f.record(ctx)
	return f.resumeQueue(ctx, req)
}

func (f *fakeAdmin) ListOverrides(ctx context.Context, req *adminv1.ListOverridesRequest) (*adminv1.ListOverridesResponse, error) {
	f.record(ctx)
	return f.listOverrides(ctx, req)
}

func (f *fakeAdmin) SetQueueOverride(ctx context.Context, req *adminv1.SetQueueOverrideRequest) (*adminv1.SetQueueOverrideResponse, error) {
	f.record(ctx)
	return f.setQueueOverride(ctx, req)
}

func (f *fakeAdmin) ClearQueueOverride(ctx context.Context, req *adminv1.ClearQueueOverrideRequest) (*adminv1.ClearQueueOverrideResponse, error) {
	f.record(ctx)
	return f.clearQueueOverride(ctx, req)
}

func (f *fakeAdmin) ListPeriodicTasks(ctx context.Context, req *adminv1.ListPeriodicTasksRequest) (*adminv1.ListPeriodicTasksResponse, error) {
	f.record(ctx)
	return f.listPeriodic(ctx, req)
}

func (f *fakeAdmin) GetPeriodicTask(ctx context.Context, req *adminv1.GetPeriodicTaskRequest) (*adminv1.GetPeriodicTaskResponse, error) {
	f.record(ctx)
	return f.getPeriodic(ctx, req)
}

func (f *fakeAdmin) PutPeriodicTask(ctx context.Context, req *adminv1.PutPeriodicTaskRequest) (*adminv1.PutPeriodicTaskResponse, error) {
	f.record(ctx)
	return f.putPeriodic(ctx, req)
}

func (f *fakeAdmin) DeletePeriodicTask(ctx context.Context, req *adminv1.DeletePeriodicTaskRequest) (*adminv1.DeletePeriodicTaskResponse, error) {
	f.record(ctx)
	return f.deletePeriodic(ctx, req)
}

func (f *fakeAdmin) PausePeriodicTask(ctx context.Context, req *adminv1.PausePeriodicTaskRequest) (*adminv1.PausePeriodicTaskResponse, error) {
	f.record(ctx)
	return f.pausePeriodic(ctx, req)
}

func (f *fakeAdmin) GetNamespaceQuota(ctx context.Context, req *adminv1.GetNamespaceQuotaRequest) (*adminv1.GetNamespaceQuotaResponse, error) {
	f.record(ctx)
	return f.getQuota(ctx, req)
}

func (f *fakeAdmin) SetNamespaceQuota(ctx context.Context, req *adminv1.SetNamespaceQuotaRequest) (*adminv1.SetNamespaceQuotaResponse, error) {
	f.record(ctx)
	return f.setQuota(ctx, req)
}

func (f *fakeAdmin) CreateToken(ctx context.Context, req *adminv1.CreateTokenRequest) (*adminv1.CreateTokenResponse, error) {
	f.record(ctx)
	return f.createToken(ctx, req)
}

func (f *fakeAdmin) GetToken(ctx context.Context, req *adminv1.GetTokenRequest) (*adminv1.GetTokenResponse, error) {
	f.record(ctx)
	return f.getToken(ctx, req)
}

func (f *fakeAdmin) RevokeToken(ctx context.Context, req *adminv1.RevokeTokenRequest) (*adminv1.RevokeTokenResponse, error) {
	f.record(ctx)
	return f.revokeToken(ctx, req)
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
