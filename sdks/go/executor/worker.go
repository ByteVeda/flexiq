package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc"

	executorv1 "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/executor/v1"
)

// Handler runs one job.
//
// The value it returns becomes the job's result; a nil value means the task
// returned nothing, which is a different answer from returning an empty one. An
// error fails the job and the scheduler retries it, unless it is wrapped in
// [Fatal].
//
// The context carries the job's timeout and is cancelled when the scheduler
// asks for the job to stop. Honour it: cancellation is cooperative and nothing
// here can stop a goroutine that does not return.
type Handler func(ctx context.Context, job *Job) (any, error)

// Worker attaches to the executor door and runs the jobs it is given.
//
// A Worker holds one gRPC connection. Build one per server, register the
// handlers, then call [Worker.Run], which blocks.
type Worker struct {
	cfg    config
	log    *slog.Logger
	conn   *grpc.ClientConn
	client executorv1.ExecutorServiceClient

	mu       sync.Mutex
	handlers map[string]Handler
	started  bool
}

// New builds a worker for the server at target.
//
// The target is a gRPC name: "host:port", or "unix:///run/flexiq.sock" for a
// Unix socket. No connection is made here — [Worker.Run] opens one — so New
// failing means the arguments were wrong, never that the server is down.
//
// A token is required, and it must carry the "execute" scope.
func New(target string, opts ...Option) (*Worker, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	dialOptions, err := cfg.dialOptions()
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(target, dialOptions...)
	if err != nil {
		return nil, fmt.Errorf("flexiq: dial %q: %w", target, err)
	}

	return &Worker{
		cfg:      cfg,
		log:      cfg.logger,
		conn:     conn,
		client:   executorv1.NewExecutorServiceClient(conn),
		handlers: make(map[string]Handler),
	}, nil
}

// Handle registers the handler for a task name.
//
// Every registered name is advertised in the handshake, and nothing else is
// ever sent to this executor. Registration must happen before [Worker.Run]: the
// advertised list is fixed for the life of a stream, so a handler added later
// would be one the scheduler does not know exists.
func (w *Worker) Handle(task string, handler Handler) error {
	if task == "" {
		return errors.New("flexiq: task name must not be empty")
	}
	if handler == nil {
		return fmt.Errorf("flexiq: handler for %q must not be nil", task)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.started {
		return fmt.Errorf("flexiq: cannot register %q after Run has started", task)
	}
	if _, exists := w.handlers[task]; exists {
		return fmt.Errorf("flexiq: task %q is already registered", task)
	}
	w.handlers[task] = handler
	return nil
}

// Run attaches and dispatches until the scheduler says stop or ctx ends.
//
// It reconnects on its own. A clean stream end is a rotation — streams are
// bounded because a gRPC stream cannot be load balanced once started — and
// reconnecting from one is immediate and not an error. A transport failure
// backs off. A shutdown frame, a refusal that reconnecting would repeat, and a
// cancelled context each return.
//
// Cancelling ctx begins a graceful drain: no new work is accepted, running jobs
// keep their contexts until the drain budget expires, and their results are
// still reported. Run then returns ctx.Err().
func (w *Worker) Run(ctx context.Context) error {
	handlers, err := w.begin()
	if err != nil {
		return err
	}
	tasks := slices.Sorted(maps.Keys(handlers))
	capacity := newSlots(w.cfg.slots)
	schedule := newBackoff(w.cfg.backoffMin, w.cfg.backoffMax)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		end, attached, err := w.session(ctx, handlers, tasks, capacity)
		if attached {
			schedule.reset()
		}

		switch end {
		case endShutdown:
			w.log.Info("flexiq: the scheduler sent shutdown; stopping")
			return nil
		case endContext:
			return ctx.Err()
		case endRotated:
			// Not a failure, and not backed off. Saying so at info is the
			// difference between an operator seeing a rotation and seeing an
			// incident every half hour.
			w.log.Info("flexiq: the scheduler ended the stream; reconnecting")
			continue
		case endError:
			var refusal *AttachError
			if errors.As(err, &refusal) && refusal.Permanent {
				return err
			}
			delay := schedule.next()
			w.log.Warn("flexiq: attach failed; retrying", "error", err, "in", delay)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
	}
}

// Close releases the connection. It does not stop a running [Worker.Run];
// cancel its context for that.
func (w *Worker) Close() error {
	if w.conn == nil {
		return nil
	}
	if err := w.conn.Close(); err != nil {
		return fmt.Errorf("flexiq: close: %w", err)
	}
	return nil
}

// begin freezes the handler set and refuses a worker with nothing to run.
func (w *Worker) begin() (map[string]Handler, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.started {
		return nil, errors.New("flexiq: Run has already been called on this worker")
	}
	if len(w.handlers) == 0 {
		// An executor advertising no tasks is matched no jobs, so it would
		// attach, hold a stream open and do nothing until someone noticed.
		return nil, errors.New("flexiq: no handlers registered: an executor advertising no tasks is sent no work")
	}
	w.started = true
	return maps.Clone(w.handlers), nil
}

// session runs one stream from the handshake to its end, and reports whether
// the handshake completed — which is what resets the reconnect schedule.
func (w *Worker) session(ctx context.Context, handlers map[string]Handler, tasks []string, capacity *slots) (sessionEnd, bool, error) {
	// The stream outlives a cancelled caller context on purpose: a graceful
	// drain has to keep sending, and settling frames for jobs that already ran
	// are the last thing that should be dropped.
	streamCtx, cancelStream := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelStream()

	capacity.resume()
	raw, err := w.client.Attach(streamCtx)
	if err != nil {
		return endError, false, attachError(err)
	}

	s := newSession(&w.cfg, w.log, w.client, raw, handlers, capacity)
	defer s.cancelJobs()

	if err := s.handshake(streamCtx, cancelStream, tasks); err != nil {
		return endError, false, err
	}

	stop := make(chan struct{})
	var workers sync.WaitGroup

	workers.Add(3)
	go func() { defer workers.Done(); s.write(streamCtx, stop) }()
	go func() { defer workers.Done(); s.heartbeat(streamCtx) }()
	go func() { defer workers.Done(); w.watchDrain(ctx, streamCtx, cancelStream, s) }()

	end, readErr := s.read(streamCtx)

	// The stream is still open here, which is the point: the jobs it is running
	// have results to report, and those are the last thing that should be lost.
	s.beginDrain(streamCtx)
	s.waitInFlight(w.cfg.shutdownDrain)
	close(stop)
	s.awaitFlush()

	_ = raw.CloseSend()
	cancelStream()
	workers.Wait()

	return end, true, readErr
}

// watchDrain turns a cancelled caller context into a graceful stream end rather
// than a severed connection.
func (w *Worker) watchDrain(ctx, streamCtx context.Context, cancelStream context.CancelFunc, s *session) {
	select {
	case <-streamCtx.Done():
		return
	case <-ctx.Done():
	}

	s.beginDrain(streamCtx)
	s.waitInFlight(w.cfg.shutdownDrain)
	// Flush before cancelling, not after. Cancelling the stream is what unblocks
	// the reader, and doing it with results still queued would sever the
	// connection out from under the frames this drain exists to deliver.
	s.awaitFlush()
	cancelStream()
}
