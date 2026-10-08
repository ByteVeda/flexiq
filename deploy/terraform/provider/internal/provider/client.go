package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/diag"

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

	GetPeriodicTask(ctx context.Context, name string, opts admin.GetPeriodicTaskOptions) (admin.PeriodicTask, error)
	PutPeriodicTask(ctx context.Context, spec admin.PeriodicTaskSpec) (admin.PeriodicTask, error)
	DeletePeriodicTask(ctx context.Context, name string) error
	PausePeriodicTask(ctx context.Context, name string) (admin.PeriodicTask, error)
	ResumePeriodicTask(ctx context.Context, name string) (admin.PeriodicTask, error)

	CreateToken(ctx context.Context, req admin.CreateTokenRequest) (admin.CreatedToken, error)
	GetToken(ctx context.Context, id string) (admin.Token, error)
	RevokeToken(ctx context.Context, id string) (admin.Token, error)
}

var _ adminAPI = (*admin.Client)(nil)

// nameAttribute is the attribute every resource is named by.
const nameAttribute = "name"

// providerData is what Configure hands every resource.
type providerData struct {
	client adminAPI
	// namespace is the provider's label for the token's namespace; the server
	// reads the real one from the token.
	namespace string
}

// providerDataFrom unpacks a resource's ProviderData. Nil is not an error:
// Terraform configures resources once before the provider itself is.
func providerDataFrom(data any, diags *diag.Diagnostics) *providerData {
	if data == nil {
		return nil
	}
	pd, ok := data.(*providerData)
	if !ok {
		diags.AddError("Unexpected provider data", fmt.Sprintf("want *providerData, got %T", data))
		return nil
	}
	return pd
}
