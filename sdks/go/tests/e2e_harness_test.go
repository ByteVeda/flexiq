//go:build integration

package tests

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
)

// The end-to-end harness: a real flexiq-server process, and this client
// pointed at it over a real socket.
//
// It answers the one question the bufconn double cannot. A double proves this
// client sends what the contract says and branches on what the contract says
// comes back; both halves of that sentence are this repo's own reading of the
// contract. What is unproven until a server is on the other end is the pair —
// that a job this client enqueues is a job the server stores, that the bytes it
// writes are the bytes that come back, and that an error it names is an error
// the server actually raises.
//
// Behind `//go:build integration` because it needs a Rust build, which the rest
// of this suite deliberately does not. `go test ./...` stays double-only.

const (
	// binaryVar names the flexiq-server build to drive. Unset, the harness
	// looks in the workspace target directory.
	binaryVar = "FLEXIQ_SERVER_BIN"

	// buildCommand produces one. Quoted in every failure that cannot find a
	// binary, because "no such file" is not an actionable thing to read.
	buildCommand = "cargo build -p flexiq-server --features grpc"

	// e2eNamespace is the namespace the server serves and every token is
	// minted for. There is no third option: `token create` mints for the
	// process's own FLEXIQ_NAMESPACE, and a token minted for a namespace the
	// server does not schedule would enqueue jobs nothing ever dequeues.
	e2eNamespace = "go-e2e"

	// unpolledQueue is what the scheduler is pointed at, and no test enqueues
	// to it.
	//
	// Setting FLEXIQ_GRPC_LISTEN starts the scheduler as well as the door, so
	// the process this harness runs would otherwise be dispatching the jobs
	// these tests are asserting on. No executor ever attaches, so nothing would
	// actually run — but "nothing claimed it yet" is a race, not a guarantee,
	// and a status assertion resting on one is a flake waiting for a slow
	// runner.
	unpolledQueue = "unpolled"

	// boundMarker is what the listener logs once it knows its port. The
	// address is asked for as :0 and read back from here rather than chosen
	// here, because a port that is free when Go probes it can be taken by
	// something else before the server binds it.
	boundMarker = "gRPC listener on tcp://"

	// readyBudget is a failure deadline, not a delay: the server has this long
	// to answer SERVING, and the suite fails with its log when it does not.
	readyBudget = 60 * time.Second

	// stopGrace is how long a SIGTERM has to drain before the kill.
	stopGrace = 10 * time.Second

	// drainBudget outlives stopGrace deliberately. cmd.WaitDelay starts its own
	// stopGrace timer at the same instant the drain-wait below does, and if the
	// drain-wait won that race it would call cmd.Wait — which closes the stderr
	// pipe — while the log reader is still on it. That is the exact race the
	// ordering in stop() exists to avoid.
	drainBudget = stopGrace + 5*time.Second
)

// server is a flexiq-server process, the SQLite file behind it, and everything
// it said.
type server struct {
	binary string
	dsn    string
	// addr is the address it actually bound, host:port.
	addr string

	cmd    *exec.Cmd
	cancel context.CancelFunc
	// drained closes when the log reader reaches EOF, which is the process
	// having exited. Waiting on it before cmd.Wait is required, not tidiness:
	// Wait closes the stderr pipe out from under a reader still using it.
	drained chan struct{}

	mu  sync.Mutex
	log []string
}

// start launches a server against a fresh database and returns once it answers
// SERVING.
func start(binary, dsn string) (*server, error) {
	ctx, cancel := context.WithCancel(context.Background())

	cmd := exec.CommandContext(ctx, binary)
	cmd.Env = serverEnv(map[string]string{
		"FLEXIQ_DSN":         dsn,
		"FLEXIQ_NAMESPACE":   e2eNamespace,
		"FLEXIQ_GRPC_LISTEN": "127.0.0.1:0",
		"FLEXIQ_QUEUES":      unpolledQueue,
		// Retention would be free to delete a job between the enqueue that
		// created it and the read that asserts on it.
		"FLEXIQ_MAINTENANCE": "off",
	})
	// The default is SIGKILL, which skips the drain the server implements and
	// leaves the SQLite file mid-write for the next test binary that opens it.
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

// readLog keeps every line the server wrote and reports the first that names a
// bound address.
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

	// A read that ended on an error rather than on EOF closes `drained` all the
	// same, and every waiter reads that as the process having exited. Recording
	// it keeps the failure message from naming the wrong cause.
	if err := scanner.Err(); err != nil {
		s.mu.Lock()
		s.log = append(s.log, "[harness] stopped reading the server log: "+err.Error())
		s.mu.Unlock()
	}
}

// awaitAddress blocks until the listener reports its port, the process exits,
// or the budget runs out.
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

// awaitServing polls the health service until it answers SERVING.
//
// Health is the right probe rather than a producer call: it is the one door
// that takes no credential, so a failure here is the server not being up and
// never the token being wrong.
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
			// The two failures read differently and are worth telling apart: a
			// call that never landed is a listener problem, and one answering
			// NOT_SERVING is the database behind it.
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
	// The reader gets first refusal on the pipe; the budget is there so a
	// process that ignores both SIGTERM and cmd.WaitDelay's kill cannot hang
	// the suite instead of failing it.
	select {
	case <-s.drained:
	case <-time.After(drainBudget):
	}
	_ = s.cmd.Wait()
}

// logTail is what the server said, for a failure that needs the reason rather
// than the symptom.
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

// mint runs `flexiq-server token create` against the same database the server
// reads, which is how an operator provisions the first credential and so the
// path worth testing.
//
// It returns the token's id as well, because that is what revoking one takes.
func (s *server) mint(name string, scopes ...string) (id, token string, err error) {
	args := []string{"token", "create", "--name", name}
	for _, scope := range scopes {
		args = append(args, "--scope", scope)
	}

	out, err := s.runCLI(args...)
	if err != nil {
		return "", "", err
	}

	// `fqt_<id>.<secret>`, and the summary goes to stderr so stdout is the
	// token and nothing else.
	token = strings.TrimSpace(out)
	rest, ok := strings.CutPrefix(token, "fqt_")
	if !ok {
		return "", "", fmt.Errorf("token create printed something that is not a token")
	}
	id, _, ok = strings.Cut(rest, ".")
	if !ok {
		return "", "", fmt.Errorf("token create printed a token with no id separator")
	}
	return id, token, nil
}

// revoke retires a token by id. It takes effect on the next call, with no
// restart — which is the half of the claim only a live server can prove.
func (s *server) revoke(id string) error {
	_, err := s.runCLI("token", "revoke", id)
	return err
}

// runCLI runs an administrative subcommand and returns its stdout.
//
// stderr is folded into the error rather than the result: the summary line and
// the pool's first-open complaints both land there on a call that succeeded.
func (s *server) runCLI(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), stopGrace)
	defer cancel()

	cmd := exec.CommandContext(ctx, s.binary, args...)
	cmd.Env = serverEnv(map[string]string{
		"FLEXIQ_DSN":       s.dsn,
		"FLEXIQ_NAMESPACE": e2eNamespace,
	})

	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("flexiq-server %s: %w\n%s", strings.Join(args, " "), err, stderr.String())
	}
	return string(out), nil
}

// serverEnv builds the child's environment from the caller's, minus anything
// that would configure the server behind this harness's back.
//
// A developer's shell commonly has FLEXIQ_DSN or FLEXIQ_NAMESPACE exported for
// a queue they were working on, and inheriting one would point this suite at
// their database. RUST_LOG goes for a narrower reason: the bound address is
// read out of an info-level line, so a shell with RUST_LOG=warn would hang the
// startup on a message that is never printed.
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
	// Debug first: it is what a contributor has, and the only difference to a
	// release build here is how long it took to produce.
	for _, profile := range []string{"debug", "release"} {
		candidate := filepath.Join(root, "target", profile, "flexiq-server")
		if _, statErr := os.Stat(candidate); statErr == nil {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("no flexiq-server under %s; build one with:\n  %s\nor point %s at one",
		filepath.Join(root, "target"), buildCommand, binaryVar)
}

// repoRoot walks up from the suite's directory to the workspace manifest.
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
			return "", fmt.Errorf("no Cargo.toml above the suite; point %s at a flexiq-server", binaryVar)
		}
		dir = parent
	}
}

// dial builds a client for this server with the given token, over the
// plaintext loopback hop the option is documented for.
func (s *server) dial(token string) (*flexiq.Client, error) {
	return flexiq.New(s.addr, flexiq.WithToken(token), flexiq.WithInsecureTransport())
}
