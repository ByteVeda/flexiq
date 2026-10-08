package admin

import (
	"fmt"

	"google.golang.org/grpc"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	"github.com/ByteVeda/flexiq/sdks/go/v2/internal/door"
	adminv1 "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/admin/v1"
)

// Client talks to the admin door of a running flexiq-server.
//
// A Client is safe for concurrent use and holds one gRPC connection, so build
// one per server and share it. Close it when the program is done with it.
type Client struct {
	conn  *grpc.ClientConn
	admin adminv1.AdminServiceClient
}

// New builds an admin client for the server at target, configured by the same
// options as [flexiq.New]: a token is required, TLS is verified by default,
// and messages are capped at [flexiq.MaxMessageBytes] in both directions.
//
// No connection is made here — the first call opens one, so New failing means
// the arguments were wrong, never that the server is down.
func New(target string, opts ...flexiq.Option) (*Client, error) {
	cfg := door.Defaults("flexiq-go-admin/" + flexiq.Version)
	for _, opt := range opts {
		opt(&cfg)
	}
	dialOptions, err := cfg.DialOptions()
	if err != nil {
		return nil, err
	}

	conn, err := grpc.NewClient(target, dialOptions...)
	if err != nil {
		return nil, fmt.Errorf("flexiq: dial %q: %w", target, err)
	}
	return &Client{conn: conn, admin: adminv1.NewAdminServiceClient(conn)}, nil
}

// Close releases the connection.
func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("flexiq: close: %w", err)
	}
	return nil
}
