package executor

import "sync/atomic"

// slots is this executor's local capacity accounting.
//
// It is not the scheduler's. The scheduler reserves a slot under its own lock
// before it writes a job frame, and a heartbeat may only shrink its view of
// free capacity, never grow it — so it is designed never to oversend. This
// counter exists because "designed never to" is not "cannot": a job already in
// flight when a zero-capacity heartbeat lands is normal, not a fault, and an
// executor that answers one by dropping it loses a job silently.
type slots struct {
	total    int64
	free     atomic.Int64
	draining atomic.Bool
}

func newSlots(total int) *slots {
	s := &slots{total: int64(total)}
	s.free.Store(int64(total))
	return s
}

// acquire takes a slot, or reports that there was none. It never blocks: the
// reader loop must keep reading, and a job it cannot run is answered rather
// than waited on.
func (s *slots) acquire() bool {
	if s.draining.Load() {
		return false
	}
	for {
		free := s.free.Load()
		if free <= 0 {
			return false
		}
		if s.free.CompareAndSwap(free, free-1) {
			return true
		}
	}
}

func (s *slots) release() {
	for {
		free := s.free.Load()
		if free >= s.total {
			return
		}
		if s.free.CompareAndSwap(free, free+1) {
			return
		}
	}
}

// available is what a heartbeat reports.
//
// A draining executor reports zero however many slots are idle, because the
// number is a request for work and it wants none.
func (s *slots) available() uint32 {
	if s.draining.Load() {
		return 0
	}
	free := s.free.Load()
	if free <= 0 {
		return 0
	}
	if free > s.total {
		free = s.total
	}
	return uint32(free)
}

// drain stops this executor taking new work. It does not touch the jobs already
// running; those are waited on separately.
func (s *slots) drain() { s.draining.Store(true) }

func (s *slots) isDraining() bool { return s.draining.Load() }

// resume undoes a drain, for the next stream. Draining belongs to one session:
// a rotation drains the stream that is ending, and the replacement wants its
// capacity back.
func (s *slots) resume() { s.draining.Store(false) }

func (s *slots) inFlight() int64 {
	free := s.free.Load()
	if free < 0 {
		free = 0
	}
	return s.total - free
}
