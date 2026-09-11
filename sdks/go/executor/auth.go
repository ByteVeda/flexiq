package executor

import (
	"context"

	"google.golang.org/grpc/credentials"
)

// bearerToken puts `authorization: Bearer <token>` on every call.
//
// The same credential the producer door takes, and a copy rather than a shared
// symbol: it is twelve lines of the gRPC interface, and exporting it from the
// producer package to save them would put an option nobody calls on that
// package's public surface forever.
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
