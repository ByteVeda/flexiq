package provider

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	"github.com/ByteVeda/flexiq/sdks/go/v2/admin"
)

// The acceptance harness: a real flexiq-server process the provider is
// pointed at, adapted from sdks/go/tests/e2e_harness_test.go. It runs only
// under TF_ACC, so `go test ./...` stays double-only and needs no Rust build.

const (
	// binaryVar names the flexiq-server build to drive. Unset, the harness
	// looks in the workspace target directory.
	binaryVar = "FLEXIQ_SERVER_BIN"

	buildCommand = "cargo build -j1 -p flexiq-server --features grpc"

	// accNamespace is the namespace the server serves and the token is minted
	// for; the provider's namespace label repeats it.
	accNamespace = "tf-acc"

	// boundMarker is what the listener logs once it knows its port. The port
	// is asked for as :0 and read back, never chosen here.
	boundMarker = "gRPC listener on tcp://"

	// readyBudget is a failure deadline, not a delay.
	readyBudget = 60 * time.Second
	stopGrace   = 10 * time.Second
	// drainBudget outlives stopGrace so cmd.Wait never closes the stderr pipe
	// under the log reader.
	drainBudget = stopGrace + 5*time.Second
)

// accServer is the server the acceptance tests share, with an operator token
// for it and an admin client to check what the provider did out of band.
var accServer *server

type server struct {
	binary string
	dsn    string
	addr   string
	token  string
	client *admin.Client

	cmd     *exec.Cmd
	cancel  context.CancelFunc
	drained chan struct{}

	mu  sync.Mutex
	log []string
}

func TestMain(m *testing.M) {
	// os.Exit runs no deferred function, so the work lives in runTests.
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	if os.Getenv("TF_ACC") == "" {
		return m.Run()
	}

	binary, err := serverBinary()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	dir, err := os.MkdirTemp("", "flexiq-tf-acc-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "temp directory:", err)
		return 1
	}
	defer func() { _ = os.RemoveAll(dir) }()

	accServer, err = start(binary, filepath.Join(dir, "flexiq.db"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "start flexiq-server:", err)
		return 1
	}
	defer accServer.stop()

	// Only the scopes the provider's resources need (tokens for flexiq_token);
	// the harness's own reads need nothing more.
	if accServer.token, err = accServer.mint("tf-acc-operator", "admin", "inspect", "tokens"); err != nil {
		fmt.Fprintln(os.Stderr, "mint an operator token:", err)
		return 1
	}
	accServer.client, err = admin.New(accServer.addr, flexiq.WithToken(accServer.token), flexiq.WithInsecureTransport())
	if err != nil {
		fmt.Fprintln(os.Stderr, "admin client:", err)
		return 1
	}
	defer func() { _ = accServer.client.Close() }()

	code := m.Run()
	if code != 0 {
		fmt.Fprintln(os.Stderr, accServer.logTail())
	}
	return code
}

// start launches a server against a fresh database and returns once it
// answers SERVING.
func start(binary, dsn string) (*server, error) {
	ctx, cancel := context.WithCancel(context.Background())

	cmd := exec.CommandContext(ctx, binary)
	cmd.Env = serverEnv(map[string]string{
		"FLEXIQ_DSN":         dsn,
		"FLEXIQ_NAMESPACE":   accNamespace,
		"FLEXIQ_GRPC_LISTEN": "127.0.0.1:0",
		// Nothing here enqueues, so nothing needs polling or retention.
		"FLEXIQ_QUEUES":      "unpolled",
		"FLEXIQ_MAINTENANCE": "off",
	})
	// SIGKILL would skip the drain and leave the SQLite file mid-write.
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = stopGrace

	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("stderr pipe: %w", err)
	}

	srv := &server{binary: binary, dsn: dsn, cmd: cmd, cancel: cancel, drained: make(chan struct{})}
	if err := cmd.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("start %s: %w", binary, err)
	}

	bound := make(chan string, 1)
	go srv.readLog(bufio.NewScanner(stderr), bound)

	if err := srv.awaitAddress(bound); err != nil {
		srv.stop()
		return nil, err
	}
	if err := srv.awaitServing(); err != nil {
		srv.stop()
		return nil, err
	}
	return srv, nil
}

// readLog keeps every line the server wrote and reports the first that names
// a bound address.
func (s *server) readLog(scanner *bufio.Scanner, bound chan<- string) {
	defer close(s.drained)

	reported := false
	for scanner.Scan() {
		line := scanner.Text()

		s.mu.Lock()
		s.log = append(s.log, line)
		s.mu.Unlock()

		if _, addr, found := strings.Cut(line, boundMarker); found && !reported {
			bound <- strings.TrimSpace(addr)
			reported = true
		}
	}
	if err := scanner.Err(); err != nil {
		s.mu.Lock()
		s.log = append(s.log, "[harness] stopped reading the server log: "+err.Error())
		s.mu.Unlock()
	}
}

func (s *server) awaitAddress(bound <-chan string) error {
	select {
	case addr := <-bound:
		s.addr = addr
		return nil
	case <-s.drained:
		return fmt.Errorf("the server exited before it bound a port:\n%s", s.logTail())
	case <-time.After(readyBudget):
		return fmt.Errorf("no %q line within %s:\n%s", boundMarker, readyBudget, s.logTail())
	}
}

// awaitServing polls the health service, the one door that takes no
// credential, until it answers SERVING.
func (s *server) awaitServing() error {
	conn, err := grpc.NewClient(s.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("health dial %s: %w", s.addr, err)
	}
	defer func() { _ = conn.Close() }()

	health := grpc_health_v1.NewHealthClient(conn)
	deadline := time.Now().Add(readyBudget)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, checkErr := health.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
		cancel()
		if checkErr == nil && resp.GetStatus() == grpc_health_v1.HealthCheckResponse_SERVING {
			return nil
		}
		select {
		case <-s.drained:
			return fmt.Errorf("the server exited before it answered SERVING:\n%s", s.logTail())
		default:
		}
		if time.Now().After(deadline) {
			if checkErr != nil {
				return fmt.Errorf("%s did not answer a health check within %s:\n%s: %w",
					s.addr, readyBudget, s.logTail(), checkErr)
			}
			return fmt.Errorf("%s answered %v rather than SERVING within %s:\n%s",
				s.addr, resp.GetStatus(), readyBudget, s.logTail())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// stop drains the server and reaps it.
func (s *server) stop() {
	s.cancel()
	select {
	case <-s.drained:
	case <-time.After(drainBudget):
	}
	_ = s.cmd.Wait()
}

func (s *server) logTail() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	const lines = 40
	tail := s.log
	if len(tail) > lines {
		tail = tail[len(tail)-lines:]
	}
	return "--- flexiq-server ---\n" + strings.Join(tail, "\n")
}

// mint runs `flexiq-server token create` against the server's database, the
// way an operator provisions the first credential. It lives the longest the
// server allows, so every token the tests mint through it expires first.
func (s *server) mint(name string, scopes ...string) (string, error) {
	args := []string{"token", "create", "--name", name, "--expires-in-days", "365"}
	for _, scope := range scopes {
		args = append(args, "--scope", scope)
	}

	ctx, cancel := context.WithTimeout(context.Background(), stopGrace)
	defer cancel()
	cmd := exec.CommandContext(ctx, s.binary, args...)
	cmd.Env = serverEnv(map[string]string{
		"FLEXIQ_DSN":       s.dsn,
		"FLEXIQ_NAMESPACE": accNamespace,
	})
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("flexiq-server %s: %w\n%s", strings.Join(args, " "), err, stderr.String())
	}

	token := strings.TrimSpace(string(out))
	if !strings.HasPrefix(token, "fqt_") {
		return "", errors.New("token create printed something that is not a token")
	}
	return token, nil
}

// serverEnv is the caller's environment minus anything that would configure
// the server behind the harness's back. RUST_LOG is pinned to info because the
// bound address is read from an info line.
func serverEnv(extra map[string]string) []string {
	env := make([]string, 0, len(os.Environ())+len(extra))
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, "FLEXIQ_") || name == "RUST_LOG" {
			continue
		}
		env = append(env, entry)
	}
	env = append(env, "RUST_LOG=info")
	for name, value := range extra {
		env = append(env, name+"="+value)
	}
	return env
}

// serverBinary finds the build to drive, or says how to make one.
func serverBinary() (string, error) {
	if path := os.Getenv(binaryVar); path != "" {
		if _, err := os.Stat(path); err != nil {
			return "", fmt.Errorf("%s=%s: %w\nbuild one with:\n  %s", binaryVar, path, err, buildCommand)
		}
		return path, nil
	}

	root, err := repoRoot()
	if err != nil {
		return "", err
	}
	for _, profile := range []string{"debug", "release"} {
		candidate := filepath.Join(root, "target", profile, "flexiq-server")
		if _, statErr := os.Stat(candidate); statErr == nil {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("no flexiq-server under %s; build one with:\n  %s\nor point %s at one",
		filepath.Join(root, "target"), buildCommand, binaryVar)
}

// repoRoot walks up from the package directory to the workspace manifest.
func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("working directory: %w", err)
	}
	for {
		if _, statErr := os.Stat(filepath.Join(dir, "Cargo.toml")); statErr == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no Cargo.toml above the package; point %s at a flexiq-server", binaryVar)
		}
		dir = parent
	}
}
