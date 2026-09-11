package executor

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"runtime"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
)

// MaxMessageBytes is the executor door's per-message cap: the 64 MiB the worker
// frame protocol allows a payload, plus 4 MiB of envelope headroom.
//
// It is deliberately not the producer door's 4 MiB. The two doors carry
// different things, and grpc-go caps receiving at 4 MiB while leaving sending
// unbounded — so a client that leaves either alone attaches cleanly and fails
// on its first large job, which is the worst time to find out.
const MaxMessageBytes = 68 * 1024 * 1024

// ProtocolVersion is the worker frame format this client speaks. Both ends
// announce it in the handshake and both reject a mismatch; it is not the
// package version and not the contract level.
const ProtocolVersion uint32 = 1

// The optional behaviours an executor can take part in. A client sends no frame
// for a behaviour it did not advertise, and the scheduler sends none for one it
// did not acknowledge.
const (
	// CapSideChannel unlocks progress and log frames. Absent, they are no-ops.
	CapSideChannel = "side_channel"
	// CapLease unlocks a lease on every dispatch, echoed on every frame about
	// it. Absent, frames carry none.
	CapLease = "lease"
	// CapSteps unlocks durable steps. This client does not implement them and
	// never advertises it.
	CapSteps = "steps"
)

// Default timings. The first three are the reference executor's own numbers,
// copied rather than invented: a budget is a failure deadline, and one chosen
// tighter than its neighbour's fails on a slow morning for no reason.
const (
	defaultHandshakeTimeout  = 10 * time.Second
	defaultHeartbeatInterval = 5 * time.Second
	defaultShutdownDrain     = 30 * time.Second
	defaultBackoffFloor      = 250 * time.Millisecond
	defaultBackoffCeiling    = 30 * time.Second
)

// Option configures a [Worker].
type Option func(*config)

type config struct {
	token           string
	creds           credentials.TransportCredentials
	insecure        bool
	maxMessageBytes int
	userAgent       string
	extra           []grpc.DialOption

	id      string
	sdk     string
	version string
	slots   int

	handshakeTimeout  time.Duration
	heartbeatInterval time.Duration
	shutdownDrain     time.Duration
	backoffMin        time.Duration
	backoffMax        time.Duration

	logger *slog.Logger
}

func defaultConfig() config {
	return config{
		// Verify the peer by default, for the reason the producer client
		// states: a bearer token is replayable by anything that observes one.
		creds:             credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12}),
		maxMessageBytes:   MaxMessageBytes,
		userAgent:         "flexiq-go-executor/" + flexiq.Version,
		id:                defaultID(),
		sdk:               "go",
		version:           flexiq.Version,
		slots:             runtime.GOMAXPROCS(0),
		handshakeTimeout:  defaultHandshakeTimeout,
		heartbeatInterval: defaultHeartbeatInterval,
		shutdownDrain:     defaultShutdownDrain,
		backoffMin:        defaultBackoffFloor,
		backoffMax:        defaultBackoffCeiling,
		logger:            slog.Default(),
	}
}

// defaultID is unique per process rather than stable per deployment, because
// the scheduler refuses a second stream under an id already attached. Two
// replicas sharing a hostname must not share an id; a restart taking a new one
// is the safer failure.
func defaultID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("go-%s-%d", host, os.Getpid())
}

// WithToken sets the bearer credential. Required, and it must carry the
// "execute" scope: a produce-scoped token cannot open an executor stream.
func WithToken(token string) Option {
	return func(c *config) { c.token = token }
}

// WithID sets this executor's identity.
//
// One live stream per id: a second attach under an id already attached is
// refused with ALREADY_EXISTS, and [Worker.Run] treats that as permanent rather
// than reconnecting into the same refusal. The default is unique per process,
// which is safe but tells an operator nothing; name your deployment instead.
func WithID(id string) Option {
	return func(c *config) { c.id = id }
}

// WithSlots sets how many jobs this executor runs at once. Defaults to
// GOMAXPROCS.
//
// The scheduler reserves a slot before it writes a job frame, so it is designed
// never to oversend. A job that arrives with nothing free anyway is refused
// retryably rather than dropped.
func WithSlots(slots int) Option {
	return func(c *config) { c.slots = slots }
}

// WithSDK overrides the SDK name and version reported in the handshake, for a
// framework that wraps this package and wants its own name in the server's
// inventory.
func WithSDK(name, version string) Option {
	return func(c *config) {
		c.sdk = name
		c.version = version
	}
}

// WithTLS replaces the default TLS configuration, for a private CA or a pinned
// certificate. It clears a prior [WithInsecureTransport], so the last transport
// option a caller passes is the one that holds.
func WithTLS(cfg *tls.Config) Option {
	return func(c *config) {
		c.creds = credentials.NewTLS(cfg)
		c.insecure = false
	}
}

// WithTransportCredentials sets the transport credentials directly, for a mesh
// or a credential type TLS does not cover. Like [WithTLS], it clears a prior
// [WithInsecureTransport].
func WithTransportCredentials(creds credentials.TransportCredentials) Option {
	return func(c *config) {
		c.creds = creds
		c.insecure = false
	}
}

// WithInsecureTransport sends the token over an unencrypted connection.
//
// flexiq-server terminates no TLS, so a deployment puts a proxy or a mesh in
// front of it. This option is for the two hops that have no network to observe
// — a Unix-domain socket, and a loopback bind whose peers are on the same host
// — and for tests.
func WithInsecureTransport() Option {
	return func(c *config) { c.insecure = true }
}

// WithMaxMessageBytes overrides the [MaxMessageBytes] cap in both directions.
//
// Lowering it is the useful direction, for an executor that would rather fail
// early than buffer a large payload. Raising it past the server's own cap does
// not raise the server's.
func WithMaxMessageBytes(n int) Option {
	return func(c *config) { c.maxMessageBytes = n }
}

// WithUserAgent replaces the gRPC user agent, which by default names this
// client and its version.
func WithUserAgent(agent string) Option {
	return func(c *config) { c.userAgent = agent }
}

// WithHeartbeatInterval sets how often free capacity is reported. Heartbeats
// stop while the executor is draining.
func WithHeartbeatInterval(d time.Duration) Option {
	return func(c *config) { c.heartbeatInterval = d }
}

// WithHandshakeTimeout bounds the wait for a hello acknowledgement. A stream
// whose handshake does not complete inside it is torn down and retried.
func WithHandshakeTimeout(d time.Duration) Option {
	return func(c *config) { c.handshakeTimeout = d }
}

// WithShutdownDrain bounds how long a stream ending waits for the jobs it is
// already running.
//
// Past it the connection closes anyway and whatever is still running is left to
// the scheduler's reaper — a handler that ignores its context must not be able
// to hang the process.
func WithShutdownDrain(d time.Duration) Option {
	return func(c *config) { c.shutdownDrain = d }
}

// WithReconnectBackoff sets the reconnect schedule used after a transport
// failure. It doubles from min to max with jitter and resets on a completed
// handshake.
//
// It does not apply to a stream the scheduler ended cleanly: that is a
// rotation, not a failure, and reconnecting is immediate.
func WithReconnectBackoff(minDelay, maxDelay time.Duration) Option {
	return func(c *config) {
		c.backoffMin = minDelay
		c.backoffMax = maxDelay
	}
}

// WithLogger sets where this package logs. Defaults to [slog.Default].
//
// It logs a rotation, a reconnect, a refused attach and a frame it does not
// recognise. All four are ordinary, and an executor that cannot say which one
// happened leaves an operator guessing.
func WithLogger(logger *slog.Logger) Option {
	return func(c *config) { c.logger = logger }
}

// WithGRPCDialOptions appends raw dial options, for interceptors, a custom
// resolver, keepalive tuning, or anything else this package does not wrap. They
// are applied last and win over the options above.
func WithGRPCDialOptions(opts ...grpc.DialOption) Option {
	return func(c *config) { c.extra = append(c.extra, opts...) }
}

func (c config) validate() error {
	if c.token == "" {
		return ErrNoToken
	}
	if c.id == "" {
		return errors.New("flexiq: executor id must not be empty")
	}
	if c.slots <= 0 || c.slots > math.MaxUint32 {
		return fmt.Errorf("flexiq: slots must be between 1 and %d", uint32(math.MaxUint32))
	}
	if c.maxMessageBytes <= 0 {
		return errors.New("flexiq: max message bytes must be positive")
	}
	if c.heartbeatInterval <= 0 || c.handshakeTimeout <= 0 || c.shutdownDrain <= 0 {
		return errors.New("flexiq: handshake, heartbeat and drain durations must be positive")
	}
	if c.backoffMin <= 0 || c.backoffMax < c.backoffMin {
		return errors.New("flexiq: reconnect backoff must be positive and non-decreasing")
	}
	return nil
}

// wireSlots is the slot count as the handshake carries it. config.validate has
// already refused anything outside a uint32.
func (c config) wireSlots() uint32 { return uint32(c.slots) }

func (c config) dialOptions() ([]grpc.DialOption, error) {
	transport := c.creds
	if c.insecure {
		transport = insecure.NewCredentials()
	}
	if transport == nil {
		return nil, errors.New("flexiq: no transport credentials: pass WithTLS, WithTransportCredentials or WithInsecureTransport")
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
