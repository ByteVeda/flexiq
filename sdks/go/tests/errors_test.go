package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	pb "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/types/known/durationpb"
)

// failWith makes the fake answer every enqueue with one status.
func failWith(t *testing.T, code codes.Code, message string, details ...protoadapt.MessageV1) *flexiq.Client {
	t.Helper()

	st := status.New(code, message)
	if len(details) > 0 {
		withDetails, err := st.WithDetails(details...)
		if err != nil {
			t.Fatalf("attach details: %v", err)
		}
		st = withDetails
	}

	return serve(t, &fakeProducer{
		enqueue: func(context.Context, *pb.EnqueueRequest) (*pb.EnqueueResponse, error) {
			return nil, st.Err()
		},
	})
}

// TestErrorBranchesOnReasonNotCode is the rule a client cannot get away with
// skipping: INVALID_ARGUMENT covers both a malformed request and a step over
// its limit, and only the reason separates them.
func TestErrorBranchesOnReasonNotCode(t *testing.T) {
	client := failWith(t, codes.InvalidArgument, "step exceeded its byte limit",
		&errdetails.ErrorInfo{
			Domain: flexiq.ErrorDomain,
			Reason: string(flexiq.ReasonStepLimitExceeded),
			Metadata: map[string]string{
				"limit":   "step bytes",
				"actual":  "70000",
				"allowed": "65536",
			},
		})

	_, err := client.Enqueue(context.Background(), flexiq.EnqueueRequest{Task: "t"})
	if !errors.Is(err, flexiq.ReasonStepLimitExceeded) {
		t.Fatalf("errors.Is did not match the reason: %v", err)
	}
	if errors.Is(err, flexiq.ReasonInvalidRequest) {
		t.Error("matched a different reason under the same code")
	}

	wireErr, ok := flexiq.AsError(err)
	if !ok {
		t.Fatalf("error is %T, want *flexiq.Error", err)
	}
	if wireErr.Code != codes.InvalidArgument {
		t.Errorf("code is %s", wireErr.Code)
	}
	if actual, ok := wireErr.MetaUint64("actual"); !ok || actual != 70000 {
		t.Errorf("actual is %d (ok=%v), want 70000", actual, ok)
	}
	if limit, ok := wireErr.Meta("limit"); !ok || limit != "step bytes" {
		t.Errorf("limit is %q (ok=%v)", limit, ok)
	}
}

// TestResourceExhaustedCarriesItsOwnBackoff: every RESOURCE_EXHAUSTED carries
// a RetryInfo, so a client waits the interval the server named rather than
// inventing one.
func TestResourceExhaustedCarriesItsOwnBackoff(t *testing.T) {
	client := failWith(t, codes.ResourceExhausted, "queue `payments` is full",
		&errdetails.ErrorInfo{
			Domain:   flexiq.ErrorDomain,
			Reason:   string(flexiq.ReasonQueueFull),
			Metadata: map[string]string{"queue": "payments", "pending": "1001", "cap": "1000"},
		},
		&errdetails.RetryInfo{RetryDelay: durationpb.New(time.Second)},
	)

	_, err := client.Enqueue(context.Background(), flexiq.EnqueueRequest{Task: "t"})
	wireErr, ok := flexiq.AsError(err)
	if !ok {
		t.Fatalf("error is %T, want *flexiq.Error", err)
	}
	if wireErr.RetryAfter != time.Second {
		t.Errorf("retry after is %s, want 1s", wireErr.RetryAfter)
	}
	if !wireErr.Retryable() {
		t.Error("a full queue was not reported as retryable")
	}

	info, ok := wireErr.QueueFull()
	if !ok {
		t.Fatal("QueueFull reported nothing on a QUEUE_FULL error")
	}
	if info.Queue != "payments" || info.Pending != 1001 || info.Cap != 1000 {
		t.Errorf("queue-full detail is %+v", info)
	}
}

// TestUnparsableMetadataIsAbsentNotFatal: a metadata value that will not parse
// is a server bug, and the code and reason already carry the decision. Failing
// the whole response over one unreadable number would lose the part that was
// fine.
func TestUnparsableMetadataIsAbsentNotFatal(t *testing.T) {
	client := failWith(t, codes.ResourceExhausted, "queue is full",
		&errdetails.ErrorInfo{
			Domain:   flexiq.ErrorDomain,
			Reason:   string(flexiq.ReasonQueueFull),
			Metadata: map[string]string{"queue": "payments", "pending": "1_001", "cap": "1000"},
		})

	_, err := client.Enqueue(context.Background(), flexiq.EnqueueRequest{Task: "t"})
	wireErr, ok := flexiq.AsError(err)
	if !ok {
		t.Fatalf("error is %T, want *flexiq.Error", err)
	}
	if wireErr.Reason != flexiq.ReasonQueueFull {
		t.Errorf("reason is %q", wireErr.Reason)
	}
	if _, ok := wireErr.MetaInt64("pending"); ok {
		t.Error("an unparsable number was reported as a value")
	}
	if _, ok := wireErr.QueueFull(); ok {
		t.Error("QueueFull reported a detail it could not read in full")
	}
}

// TestForeignErrorDomainIsIgnored: an ErrorInfo from a proxy or a mesh in the
// path is not FlexiQ's, and reading its reason as one of ours would branch on
// somebody else's vocabulary.
func TestForeignErrorDomainIsIgnored(t *testing.T) {
	client := failWith(t, codes.PermissionDenied, "denied by the mesh",
		&errdetails.ErrorInfo{
			Domain: "mesh.example.com",
			Reason: "QUEUE_FULL",
		})

	_, err := client.Enqueue(context.Background(), flexiq.EnqueueRequest{Task: "t"})
	if errors.Is(err, flexiq.ReasonQueueFull) {
		t.Fatal("a foreign domain's reason was read as FlexiQ's")
	}
	wireErr, ok := flexiq.AsError(err)
	if !ok {
		t.Fatalf("error is %T, want *flexiq.Error", err)
	}
	if wireErr.Reason != "" {
		t.Errorf("reason is %q, want empty", wireErr.Reason)
	}
	if wireErr.Code != codes.PermissionDenied {
		t.Errorf("code is %s, want PermissionDenied", wireErr.Code)
	}
}

// TestScopeDeniedNamesTheScope: the two scopes are not a hierarchy, so the
// refusal has to say which one was missing.
func TestScopeDeniedNamesTheScope(t *testing.T) {
	client := failWith(t, codes.PermissionDenied, "this credential does not open flexiq.v1",
		&errdetails.ErrorInfo{
			Domain:   flexiq.ErrorDomain,
			Reason:   string(flexiq.ReasonScopeDenied),
			Metadata: map[string]string{"scope": "produce"},
		})

	_, err := client.Enqueue(context.Background(), flexiq.EnqueueRequest{Task: "t"})
	wireErr, ok := flexiq.AsError(err)
	if !ok {
		t.Fatalf("error is %T, want *flexiq.Error", err)
	}
	scope, ok := wireErr.Scope()
	if !ok || scope != "produce" {
		t.Errorf("scope is %q (ok=%v), want produce", scope, ok)
	}
	if wireErr.Retryable() {
		t.Error("a scope refusal is never retryable")
	}
}

// TestErrorWithoutDetailsStillReports: not every failure has an ErrorInfo — a
// dropped connection has none — and the client still has to hand back
// something a caller can act on.
func TestErrorWithoutDetailsStillReports(t *testing.T) {
	client := failWith(t, codes.Unavailable, "the storage backend is unavailable")

	_, err := client.Enqueue(context.Background(), flexiq.EnqueueRequest{Task: "t"})
	wireErr, ok := flexiq.AsError(err)
	if !ok {
		t.Fatalf("error is %T, want *flexiq.Error", err)
	}
	if wireErr.Reason != "" {
		t.Errorf("reason is %q, want empty", wireErr.Reason)
	}
	if status.Code(err) != codes.Unavailable {
		t.Errorf("status.Code says %s; the error stopped being a gRPC status", status.Code(err))
	}
	if errors.Is(err, flexiq.ReasonQueueFull) {
		t.Error("an error carrying no reason matched one")
	}
}
