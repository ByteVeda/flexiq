// Package door is the plumbing every client of a flexiq-server door shares:
// how a connection is dialled, and how a failed call is read.
//
// It exists so the producer client and the admin client dial the same way
// from one set of options, without either exporting its internals to the
// other. The options a caller passes are still the root package's; this is
// only what they write into.
package door

import (
	"crypto/tls"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// MaxMessageBytes is the server's per-message cap, 4 MiB in each direction.
const MaxMessageBytes = 4 * 1024 * 1024

// ErrNoToken is the refusal to dial without a credential. The root package
// re-exports it, so it is one value however a caller reaches it.
var ErrNoToken = errors.New("flexiq: no token: every call to this door carries one, use WithToken")

// Config is what the root package's options write into.
type Config struct {
	Token           string
	Creds           credentials.TransportCredentials
	Insecure        bool
	MaxMessageBytes int
	UserAgent       string
	Extra           []grpc.DialOption
}

// Defaults is the configuration before any option: a verified TLS peer, the
// 4 MiB cap, and the given user agent.
func Defaults(userAgent string) Config {
	return Config{
		// Verify the peer by default. The token is a bearer credential:
		// anything that observes one can replay it, and nothing on the wire
		// tells a replay from the original.
		Creds:           credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12}),
		MaxMessageBytes: MaxMessageBytes,
		UserAgent:       userAgent,
	}
}

// DialOptions turns the configuration into gRPC dial options, refusing one
// that would dial without a token or a transport.
func (c Config) DialOptions() ([]grpc.DialOption, error) {
	if c.Token == "" {
		return nil, ErrNoToken
	}
	if c.MaxMessageBytes <= 0 {
		return nil, errors.New("flexiq: max message bytes must be positive")
	}

	transport := c.Creds
	if c.Insecure {
		transport = insecure.NewCredentials()
	}
	if transport == nil {
		return nil, errors.New("flexiq: no transport credentials: pass WithTLS, WithTransportCredentials or WithInsecureTransport")
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(transport),
		grpc.WithPerRPCCredentials(bearerToken{token: c.Token, overInsecure: c.Insecure}),
		grpc.WithUserAgent(c.UserAgent),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(c.MaxMessageBytes),
			grpc.MaxCallSendMsgSize(c.MaxMessageBytes),
		),
	}
	return append(opts, c.Extra...), nil
}
