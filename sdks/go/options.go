package flexiq

import (
	"crypto/tls"
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// MaxMessageBytes is the server's per-message cap, 4 MiB in each direction. It
// is gRPC's own default for receiving and the same cap the JSON facade applies
// to a request body.
//
// grpc-go leaves sending unbounded by default, which would turn an oversized
// payload into a error from the server halfway through a request. This client
// applies the cap to both directions so the send fails locally, before the
// bytes go out.
const MaxMessageBytes = 4 * 1024 * 1024

// Option configures a [Client].
type Option func(*config)

type config struct {
	token           string
	creds           credentials.TransportCredentials
	insecure        bool
	maxMessageBytes int
	userAgent       string
	extra           []grpc.DialOption
}

func defaultConfig() config {
	return config{
		// Verify the peer by default. The token is a bearer credential:
		// anything that observes one can replay it, and nothing on the wire
		// tells a replay from the original.
		creds:           credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12}),
		maxMessageBytes: MaxMessageBytes,
		userAgent:       "flexiq-go/" + Version,
	}
}

// WithToken sets the bearer credential. Required.
//
// The whole string is opaque — it has a public id before the "." and a secret
// after it, and neither is a client's business to parse.
func WithToken(token string) Option {
	return func(c *config) { c.token = token }
}

// WithTLS replaces the default TLS configuration, for a private CA or a pinned
// certificate.
func WithTLS(cfg *tls.Config) Option {
	return func(c *config) { c.creds = credentials.NewTLS(cfg) }
}

// WithTransportCredentials sets the transport credentials directly, for a mesh
// or a credential type TLS does not cover.
func WithTransportCredentials(creds credentials.TransportCredentials) Option {
	return func(c *config) { c.creds = creds }
}

// WithInsecureTransport sends the token over an unencrypted connection.
//
// flexiq-server terminates no TLS, so a deployment puts a proxy or a mesh in
// front of it. This option is for the two hops that have no network to observe
// — a Unix-domain socket, and a loopback bind whose peers are on the same host
// — and for tests. On any other hop it publishes a credential anyone on the
// path can replay.
func WithInsecureTransport() Option {
	return func(c *config) { c.insecure = true }
}

// WithMaxMessageBytes overrides the 4 MiB per-message cap in both directions.
//
// Raising it past the server's own cap does not raise the server's: an
// oversized request is refused there with OUT_OF_RANGE either way. Lowering it
// is the useful direction, for a caller that wants to fail earlier.
func WithMaxMessageBytes(n int) Option {
	return func(c *config) { c.maxMessageBytes = n }
}

// WithUserAgent replaces the gRPC user agent, which by default names this
// client and its version.
func WithUserAgent(agent string) Option {
	return func(c *config) { c.userAgent = agent }
}

// WithGRPCDialOptions appends raw dial options, for interceptors, a custom
// resolver, keepalive tuning, or anything else this package does not wrap.
// They are applied last and win over the options above.
func WithGRPCDialOptions(opts ...grpc.DialOption) Option {
	return func(c *config) { c.extra = append(c.extra, opts...) }
}

func (c config) dialOptions() ([]grpc.DialOption, error) {
	if c.token == "" {
		return nil, ErrNoToken
	}
	if c.maxMessageBytes <= 0 {
		return nil, errors.New("flexiq: max message bytes must be positive")
	}

	transport := c.creds
	if c.insecure {
		transport = insecureCredentials()
	}
	if transport == nil {
		return nil, fmt.Errorf("flexiq: no transport credentials: pass WithTLS, WithTransportCredentials or WithInsecureTransport")
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(transport),
		grpc.WithPerRPCCredentials(bearerToken{token: c.token, overInsecure: c.insecure}),
		grpc.WithUserAgent(c.userAgent),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(c.maxMessageBytes),
			grpc.MaxCallSendMsgSize(c.maxMessageBytes),
		),
	}
	return append(opts, c.extra...), nil
}
