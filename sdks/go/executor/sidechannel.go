package executor

import (
	"sync"

	executorv1 "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/executor/v1"
)

// sideChannelLimit bounds the queue of progress and log frames waiting for the
// writer.
//
// Bounded because these frames are fire and forget: nothing answers them, they
// never settle a job, and a task that only wanted to report progress must never
// wait on the scheduler to do it. An unbounded queue would turn a slow socket
// into memory growth instead of into dropped telemetry, which is the worse of
// the two.
const sideChannelLimit = 256

// sideChannel is the bounded, lossy half of the outbound stream.
//
// Progress coalesces: a job's newer report replaces its older one in place,
// because the older number is not information once the newer exists. Logs drop
// oldest, because a log line is information and the newest lines are the ones
// describing what is happening now.
type sideChannel struct {
	mu      sync.Mutex
	entries []sideEntry
	dropped uint64
	signal  chan struct{}
}

type sideEntry struct {
	req      *executorv1.AttachRequest
	jobID    string
	progress bool
}

func newSideChannel() *sideChannel {
	return &sideChannel{signal: make(chan struct{}, 1)}
}

// ready is selected on by the writer. It carries no value: the writer wakes and
// calls pop, which is what actually holds the lock.
func (s *sideChannel) ready() <-chan struct{} { return s.signal }

func (s *sideChannel) push(req *executorv1.AttachRequest, jobID string, progress bool) {
	s.mu.Lock()
	if progress {
		for i := range s.entries {
			if s.entries[i].progress && s.entries[i].jobID == jobID {
				s.entries[i].req = req
				s.mu.Unlock()
				s.notify()
				return
			}
		}
	}

	s.entries = append(s.entries, sideEntry{req: req, jobID: jobID, progress: progress})
	if len(s.entries) > sideChannelLimit {
		s.entries = append(s.entries[:0], s.entries[1:]...)
		s.dropped++
	}
	s.mu.Unlock()
	s.notify()
}

// pop returns the next frame, or nil when the queue is empty. A queue that
// still has entries re-arms the signal, so one wake per pop is enough.
func (s *sideChannel) pop() *executorv1.AttachRequest {
	s.mu.Lock()
	if len(s.entries) == 0 {
		s.mu.Unlock()
		return nil
	}
	req := s.entries[0].req
	s.entries = append(s.entries[:0], s.entries[1:]...)
	remaining := len(s.entries)
	s.mu.Unlock()

	if remaining > 0 {
		s.notify()
	}
	return req
}

// take removes and returns everything queued for one job, in order.
//
// Called as that job settles, so its telemetry goes out ahead of the frame that
// ends it. Leaving it here instead would race the settling frame onto the wire,
// and the scheduler drops a progress or log frame naming a job the sending
// stream is no longer running.
func (s *sideChannel) take(jobID string) []*executorv1.AttachRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	var taken []*executorv1.AttachRequest
	kept := s.entries[:0]
	for _, entry := range s.entries {
		if entry.jobID == jobID {
			taken = append(taken, entry.req)
			continue
		}
		kept = append(kept, entry)
	}
	s.entries = kept
	return taken
}

func (s *sideChannel) droppedCount() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dropped
}

func (s *sideChannel) notify() {
	select {
	case s.signal <- struct{}{}:
	default:
	}
}
