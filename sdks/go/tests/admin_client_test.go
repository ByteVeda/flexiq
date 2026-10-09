package tests

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	"github.com/ByteVeda/flexiq/sdks/go/v2/admin"
	adminv1 "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/admin/v1"
)

// notFound is the status the server answers a missing handle with: NOT_FOUND
// and an ErrorInfo naming the reason.
func notFound(t *testing.T, reason flexiq.Reason) error {
	t.Helper()

	st, err := status.New(codes.NotFound, "not found").WithDetails(&errdetails.ErrorInfo{
		Domain: flexiq.ErrorDomain,
		Reason: string(reason),
	})
	if err != nil {
		t.Fatalf("attach details: %v", err)
	}
	return st.Err()
}

// TestAdminDialsWithTheRootOptions: the admin client takes the producer
// client's options, so the token, the transport and the user agent are the
// ones a caller already configured.
func TestAdminDialsWithTheRootOptions(t *testing.T) {
	fake := &fakeAdmin{
		listQueues: func(context.Context, *adminv1.ListQueuesRequest) (*adminv1.ListQueuesResponse, error) {
			return &adminv1.ListQueuesResponse{}, nil
		},
	}
	client := serveAdmin(t, fake)

	if _, err := client.ListQueues(context.Background()); err != nil {
		t.Fatalf("ListQueues: %v", err)
	}

	if got := fake.metadata.Get("authorization"); len(got) != 1 || got[0] != "Bearer "+testToken {
		t.Errorf("authorization header is %v, want the bearer token", got)
	}
	agents := fake.metadata.Get("user-agent")
	if len(agents) != 1 || !strings.Contains(agents[0], "flexiq-go-admin/"+flexiq.Version) {
		t.Errorf("user-agent is %v, want it to name flexiq-go-admin/%s", agents, flexiq.Version)
	}
}

// TestAdminNewRequiresAToken fails at construction, as the producer client
// does, and with the same sentinel.
func TestAdminNewRequiresAToken(t *testing.T) {
	_, err := admin.New("localhost:50051")
	if !errors.Is(err, flexiq.ErrNoToken) {
		t.Fatalf("want ErrNoToken, got %v", err)
	}
}

// TestAdminFailuresAreFlexiqErrors: an admin failure is the root package's
// error, so a caller branches on it with the same reasons.
func TestAdminFailuresAreFlexiqErrors(t *testing.T) {
	client := serveAdmin(t, &fakeAdmin{
		pauseQueue: func(context.Context, *adminv1.PauseQueueRequest) (*adminv1.PauseQueueResponse, error) {
			st, err := status.New(codes.PermissionDenied, "needs admin").WithDetails(&errdetails.ErrorInfo{
				Domain:   flexiq.ErrorDomain,
				Reason:   string(flexiq.ReasonScopeDenied),
				Metadata: map[string]string{"scope": "admin"},
			})
			if err != nil {
				return nil, err
			}
			return nil, st.Err()
		},
	})

	_, err := client.PauseQueue(context.Background(), "q")
	if !errors.Is(err, flexiq.ReasonScopeDenied) {
		t.Fatalf("want ReasonScopeDenied, got %v", err)
	}
	wireErr, ok := flexiq.AsError(err)
	if !ok {
		t.Fatalf("want a *flexiq.Error, got %T", err)
	}
	if scope, ok := wireErr.Scope(); !ok || scope != "admin" {
		t.Errorf("scope is %q (%v), want admin", scope, ok)
	}
}

// TestAdminCapsTheSendDirection: the 4 MiB cap reaches this client too, so an
// oversized payload fails locally as a *flexiq.Error.
func TestAdminCapsTheSendDirection(t *testing.T) {
	fake := &fakeAdmin{}
	client := serveAdmin(t, fake)

	oversize := make([]byte, flexiq.MaxMessageBytes+1024)
	oversize[0] = flexiq.TagCBOR
	_, err := client.PutPeriodicTask(context.Background(), admin.PeriodicTaskSpec{
		Name: "p", Task: "t", Cron: "0 * * * * *", Raw: oversize,
	})
	wireErr, ok := flexiq.AsError(err)
	if !ok || wireErr.Code != codes.ResourceExhausted {
		t.Fatalf("want a ResourceExhausted *flexiq.Error, got %T: %v", err, err)
	}
	if fake.calls != 0 {
		t.Errorf("the request reached the server %d times; it should have failed locally", fake.calls)
	}
}

// TestGetQueueReportsAnAbsentQueue: a queue exists only through a job, a pause
// or an override, so a read of one with none is "absent", not an error.
func TestGetQueueReportsAnAbsentQueue(t *testing.T) {
	var got *adminv1.ListQueuesRequest
	client := serveAdmin(t, &fakeAdmin{
		listQueues: func(_ context.Context, req *adminv1.ListQueuesRequest) (*adminv1.ListQueuesResponse, error) {
			got = req
			if req.GetQueue() == "busy" {
				return &adminv1.ListQueuesResponse{Queues: []*adminv1.Queue{
					{Name: "busy", Paused: true, Pending: 3},
				}}, nil
			}
			return &adminv1.ListQueuesResponse{}, nil
		},
	})
	ctx := context.Background()

	if _, found, err := client.GetQueue(ctx, "idle"); err != nil || found {
		t.Fatalf("GetQueue(idle) = found %v, err %v; want absent", found, err)
	}
	if got.Queue == nil || got.GetQueue() != "idle" {
		t.Errorf("the read did not narrow to the queue: %v", got)
	}

	queue, found, err := client.GetQueue(ctx, "busy")
	if err != nil || !found {
		t.Fatalf("GetQueue(busy) = found %v, err %v", found, err)
	}
	if !queue.Paused || queue.Pending != 3 {
		t.Errorf("queue is %+v", queue)
	}
}
