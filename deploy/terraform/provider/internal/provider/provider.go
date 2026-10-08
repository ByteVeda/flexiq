// Package provider implements the flexiq Terraform provider: queue overrides,
// namespace quotas, periodic tasks and API tokens over a flexiq-server's
// admin door.
package provider

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/path"
	tfprovider "github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	"github.com/ByteVeda/flexiq/sdks/go/v2/admin"
)

// RegistryAddress is the provider's source address.
const RegistryAddress = "registry.terraform.io/byteveda/flexiq"

const (
	addressEnv = "FLEXIQ_ADDRESS"
	tokenEnv   = "FLEXIQ_TOKEN"
)

var _ tfprovider.Provider = (*flexiqProvider)(nil)

type flexiqProvider struct {
	version string
}

// New answers a factory for the provider at the given version, the shape
// providerserver.Serve and the acceptance tests both take.
func New(version string) func() tfprovider.Provider {
	return func() tfprovider.Provider {
		return &flexiqProvider{version: version}
	}
}

type providerModel struct {
	Address   types.String `tfsdk:"address"`
	Token     types.String `tfsdk:"token"`
	Namespace types.String `tfsdk:"namespace"`
	TLS       *tlsModel    `tfsdk:"tls"`
}

type tlsModel struct {
	CACert   types.String `tfsdk:"ca_cert"`
	Insecure types.Bool   `tfsdk:"insecure"`
}

func (p *flexiqProvider) Metadata(_ context.Context, _ tfprovider.MetadataRequest, resp *tfprovider.MetadataResponse) {
	resp.TypeName = "flexiq"
	resp.Version = p.version
}

func (p *flexiqProvider) Schema(_ context.Context, _ tfprovider.SchemaRequest, resp *tfprovider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages a flexiq-server through its gRPC admin door. " +
			"Every call acts in the namespace the token was minted for; use one provider alias per namespace.",
		Attributes: map[string]schema.Attribute{
			"address": schema.StringAttribute{
				Description: "The server's gRPC address, host:port. Falls back to " + addressEnv + ".",
				Optional:    true,
			},
			"token": schema.StringAttribute{
				Description: "Bearer token with the admin scope. Falls back to " + tokenEnv + ".",
				Optional:    true,
				Sensitive:   true,
			},
			"namespace": schema.StringAttribute{
				Description: "A label for the token's namespace, recorded as flexiq_namespace's name. " +
					"The server takes the namespace from the token, never from this.",
				Optional: true,
			},
		},
		Blocks: map[string]schema.Block{
			"tls": schema.SingleNestedBlock{
				Description: "Transport security. Omitted, TLS is verified against the system roots.",
				Attributes: map[string]schema.Attribute{
					"ca_cert": schema.StringAttribute{
						Description: "PEM bundle to verify the server against instead of the system roots.",
						Optional:    true,
					},
					"insecure": schema.BoolAttribute{
						Description: "Plaintext, no TLS: the token crosses the wire readable. Loopback and tests only.",
						Optional:    true,
					},
				},
			},
		},
	}
}

func (p *flexiqProvider) Configure(ctx context.Context, req tfprovider.ConfigureRequest, resp *tfprovider.ConfigureResponse) {
	var cfg providerModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &cfg)...)
	if resp.Diagnostics.HasError() {
		return
	}

	address, known := setting(cfg.Address, addressEnv)
	if !known || address == "" {
		resp.Diagnostics.AddAttributeError(path.Root("address"), "Missing flexiq address",
			"Set address, or "+addressEnv+", to the server's gRPC host:port.")
	}
	token, known := setting(cfg.Token, tokenEnv)
	if !known || token == "" {
		resp.Diagnostics.AddAttributeError(path.Root("token"), "Missing flexiq token",
			"Set token, or "+tokenEnv+", to a token with the admin scope.")
	}
	if resp.Diagnostics.HasError() {
		return
	}

	opts, err := dialOptions(token, cfg.TLS)
	if err != nil {
		resp.Diagnostics.AddAttributeError(path.Root("tls"), "Invalid flexiq TLS settings", err.Error())
		return
	}
	client, err := admin.New(address, opts...)
	if err != nil {
		resp.Diagnostics.AddError("Cannot build the flexiq admin client", err.Error())
		return
	}

	// The connection lives as long as the provider process; Terraform ends
	// that process when the run is done.
	data := &providerData{client: client, namespace: cfg.Namespace.ValueString()}
	resp.ResourceData = data
	resp.DataSourceData = data
}

func (p *flexiqProvider) Resources(context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		newQueueResource,
		newNamespaceResource,
		newPeriodicResource,
	}
}

func (p *flexiqProvider) DataSources(context.Context) []func() datasource.DataSource {
	return nil
}

// setting reads a provider attribute, falling back to an environment variable
// when it is null. The bool is false when the value is unknown at plan time.
func setting(v types.String, env string) (string, bool) {
	if v.IsUnknown() {
		return "", false
	}
	if v.IsNull() {
		return os.Getenv(env), true
	}
	return v.ValueString(), true
}

// dialOptions turns the provider's credentials into admin client options.
func dialOptions(token string, settings *tlsModel) ([]flexiq.Option, error) {
	opts := []flexiq.Option{flexiq.WithToken(token)}
	if settings == nil {
		return opts, nil
	}
	if settings.CACert.IsUnknown() || settings.Insecure.IsUnknown() {
		return nil, errors.New("tls settings must be known when the provider is configured")
	}

	caCert := settings.CACert.ValueString()
	switch {
	case settings.Insecure.ValueBool() && caCert != "":
		return nil, errors.New("insecure and ca_cert contradict each other: set one")
	case settings.Insecure.ValueBool():
		opts = append(opts, flexiq.WithInsecureTransport())
	case caCert != "":
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(caCert)) {
			return nil, errors.New("ca_cert holds no PEM certificate")
		}
		opts = append(opts, flexiq.WithTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}))
	}
	return opts, nil
}
