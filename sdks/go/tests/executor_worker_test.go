package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/ByteVeda/flexiq/sdks/go/v2/executor"
)

func TestNewRefusesAWorkerWithNoCredential(t *testing.T) {
	_, err := executor.New("localhost:50051", executor.WithInsecureTransport())
	if !errors.Is(err, executor.ErrNoToken) {
		t.Fatalf("New without a token returned %v, want ErrNoToken", err)
	}
}

func TestNewRefusesAnImpossibleConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name string
		opt  executor.Option
	}{
		{"no slots", executor.WithSlots(0)},
		{"no identity", executor.WithID("")},
		{"a negative message cap", executor.WithMaxMessageBytes(-1)},
		{"backoff that shrinks", executor.WithReconnectBackoff(10, 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := executor.New("localhost:50051",
				executor.WithToken(testToken), executor.WithInsecureTransport(), tc.opt)
			if err == nil {
				t.Fatal("New accepted a configuration it cannot attach with")
			}
		})
	}
}

func TestHandleRefusesAnEmptyNameANilHandlerAndADuplicate(t *testing.T) {
	w, err := executor.New("localhost:50051",
		executor.WithToken(testToken), executor.WithInsecureTransport())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	if err := w.Handle("", noopHandler); err == nil {
		t.Error("Handle accepted an empty task name")
	}
	if err := w.Handle("t", nil); err == nil {
		t.Error("Handle accepted a nil handler")
	}
	if err := w.Handle("t", noopHandler); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if err := w.Handle("t", noopHandler); err == nil {
		t.Error("Handle accepted a second handler for one task; only one of them could ever run")
	}
}

func TestRunRefusesAWorkerWithNoHandlers(t *testing.T) {
	w, err := executor.New("localhost:50051",
		executor.WithToken(testToken), executor.WithInsecureTransport())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	// It would attach, hold a stream open, advertise nothing and be matched no
	// work — a worker that looks healthy and does nothing.
	if err := w.Run(context.Background()); err == nil {
		t.Fatal("Run accepted a worker advertising no tasks")
	}
}

func TestHandleRefusesRegistrationAfterRunHasStarted(t *testing.T) {
	fake := &fakeScheduler{attach: func(_ int, s *schedulerStream) error {
		if err := s.handshake(executor.CapLease); err != nil {
			return err
		}
		drain(s)
		return nil
	}}
	w := serveExecutor(t, fake)
	mustHandle(t, w, "t", noopHandler)
	runWorker(t, w)

	await(t, fake, "the handshake", func() bool { return firstHello(fake.frames()) != nil })

	// The advertised list is fixed for the life of a stream, so a handler added
	// now is one the scheduler does not know exists and will never dispatch to.
	if err := w.Handle("late", noopHandler); err == nil {
		t.Fatal("Handle accepted a registration the handshake had already gone out without")
	}
}

func TestFatalKeepsTheWrappedErrorReachable(t *testing.T) {
	sentinel := errors.New("no such customer")
	wrapped := executor.Fatal(sentinel)

	if !errors.Is(wrapped, executor.ErrFatal) {
		t.Error("a fatal error does not match ErrFatal")
	}
	if !errors.Is(wrapped, sentinel) {
		t.Error("a fatal error lost the error it wraps")
	}
	if wrapped.Error() != sentinel.Error() {
		t.Errorf("Error() = %q, want the wrapped error's own text %q", wrapped.Error(), sentinel.Error())
	}
}
