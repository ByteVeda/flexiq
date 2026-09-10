package flexiq

import (
	"context"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// bearerToken puts `authorization: Bearer <token>` on every call.
//
// It is per-RPC credentials rather than a header set at each call site because
// there is no call on this door that does not carry one, and a header a caller
// can forget is a header a caller will forget.
type bearerToken struct {
	token string
	// overInsecure records that the caller knowingly chose an unencrypted hop.
	// grpc-go refuses to attach per-RPC credentials to one otherwise, which is
	// the check that keeps a token off a plaintext wire by accident.
	overInsecure bool
}

var _ credentials.PerRPCCredentials = bearerToken{}

func (b bearerToken) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}

func (b bearerToken) RequireTransportSecurity() bool { return !b.overInsecure }

func insecureCredentials() credentials.TransportCredentials {
	return insecure.NewCredentials()
}
