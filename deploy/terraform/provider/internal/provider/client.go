package provider

import (
	"context"

	"github.com/ByteVeda/flexiq/sdks/go/v2/admin"
)

// adminAPI is the slice of [admin.Client] the resources call. An interface so
// unit tests can stand a fake in for the server.
type adminAPI interface {
	GetQueue(ctx context.Context, name string) (admin.Queue, bool, error)
	PauseQueue(ctx context.Context, name string) (admin.Queue, error)
	ResumeQueue(ctx context.Context, name string) (admin.Queue, error)

	GetQueueOverride(ctx context.Context, queue string) (admin.QueueOverride, bool, error)
	SetQueueOverride(ctx context.Context, queue string, override admin.QueueOverride) (admin.QueueOverride, error)
	ClearQueueOverride(ctx context.Context, queue string) error

	GetNamespaceQuota(ctx context.Context) (admin.NamespaceQuota, error)
	SetNamespaceQuota(ctx context.Context, quota admin.NamespaceQuota) (admin.NamespaceQuota, error)
	ClearNamespaceQuota(ctx context.Context) error
}

var _ adminAPI = (*admin.Client)(nil)

// providerData is what Configure hands every resource.
type providerData struct {
	client adminAPI
	// namespace is the provider's label for the token's namespace; the server
	// reads the real one from the token.
	namespace string
}
