package flexiq

import (
	"crypto/tls"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/ByteVeda/flexiq/sdks/go/v2/internal/door"
)

// MaxMessageBytes is the server's per-message cap, 4 MiB in each direction. It
// is gRPC's own default for receiving and the same cap the JSON facade applies
// to a request body.
//
// grpc-go leaves sending unbounded by default, which would turn an oversized
// payload into a error from the server halfway through a request. This client
// applies the cap to both directions so the send fails locally, before the
// bytes go out.
const MaxMessageBytes = door.MaxMessageBytes

// Option configures a [Client], and the admin client in the
// [github.com/ByteVeda/flexiq/sdks/go/v2/admin] subpackage, which dials the
// same way.
type Option func(*config)

// config is an alias so the admin subpackage can apply these options to the
// same struct without this package exporting it.
type config = door.Config

// WithToken sets the bearer credential. Required.
//
// The whole string is opaque — it has a public id before the "." and a secret
// after it, and neither is a client's business to parse.
func WithToken(token string) Option {
	return func(c *config) { c.Token = token }
}

// WithTLS replaces the default TLS configuration, for a private CA or a pinned
// certificate.
//
// It clears a prior [WithInsecureTransport], so the last transport option a
// caller passes is the one that holds. Options are applied in order, and
// leaving the insecure flag set behind a later TLS option would put the token
// on a plaintext wire while the call site says otherwise.
func WithTLS(cfg *tls.Config) Option {
	return func(c *config) {
		c.Creds = credentials.NewTLS(cfg)
		c.Insecure = false
	}
}

// WithTransportCredentials sets the transport credentials directly, for a mesh
// or a credential type TLS does not cover. Like [WithTLS], it clears a prior
// [WithInsecureTransport].
func WithTransportCredentials(creds credentials.TransportCredentials) Option {
	return func(c *config) {
		c.Creds = creds
		c.Insecure = false
	}
}

// WithInsecureTransport sends the token over an unencrypted connection.
//
// For TLS, either flexiq-server terminates it (FLEXIQ_GRPC_TLS_CERT) or a proxy
// or a mesh in front of it does. This option is for the two hops that have no
// network to observe — a Unix-domain socket, and a loopback bind whose peers
// are on the same host — and for tests. On any other hop it publishes a
// credential anyone on the path can replay.
func WithInsecureTransport() Option {
	return func(c *config) { c.Insecure = true }
}

// WithMaxMessageBytes overrides the 4 MiB per-message cap in both directions.
//
// Raising it past the server's own cap does not raise the server's: an
// oversized request is refused there with OUT_OF_RANGE either way. Lowering it
// is the useful direction, for a caller that wants to fail earlier.
func WithMaxMessageBytes(n int) Option {
	return func(c *config) { c.MaxMessageBytes = n }
}

// WithUserAgent replaces the gRPC user agent, which by default names this
// client and its version.
func WithUserAgent(agent string) Option {
	return func(c *config) { c.UserAgent = agent }
}

// WithGRPCDialOptions appends raw dial options, for interceptors, a custom
// resolver, keepalive tuning, or anything else this package does not wrap.
// They are applied last and win over the options above.
func WithGRPCDialOptions(opts ...grpc.DialOption) Option {
	return func(c *config) { c.Extra = append(c.Extra, opts...) }
}
