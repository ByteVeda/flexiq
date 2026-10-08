package flexiq

import (
	"fmt"

	"google.golang.org/grpc"

	"github.com/ByteVeda/flexiq/sdks/go/v2/internal/door"
	pb "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/v1"
)

// Client talks to the producer door of a running flexiq-server.
//
// A Client is safe for concurrent use and holds one gRPC connection, so build
// one per server and share it. Close it when the program is done with it.
type Client struct {
	conn     *grpc.ClientConn
	producer pb.ProducerServiceClient
}

// New builds a client for the server at target.
//
// The target is a gRPC name: "host:port", or "unix:///run/flexiq.sock" for a
// Unix socket. No connection is made here — the first call opens one, so New
// failing means the arguments were wrong, never that the server is down.
//
// A token is required: there is no anonymous path on this door.
func New(target string, opts ...Option) (*Client, error) {
	cfg := door.Defaults("flexiq-go/" + Version)
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
	return &Client{conn: conn, producer: pb.NewProducerServiceClient(conn)}, nil
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

// ErrNoToken is returned by [New], and by the admin subpackage's New, when no
// credential was supplied: every call on these doors carries a bearer token,
// so pass [WithToken]. Its message is "flexiq: no token: every call to this
// door carries one, use WithToken". Match it with [errors.Is].
var ErrNoToken = door.ErrNoToken
