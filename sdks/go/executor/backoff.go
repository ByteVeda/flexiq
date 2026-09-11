package executor

import (
	"math/rand/v2"
	"time"
)

// backoff is the reconnect schedule after a transport failure.
//
// It does not apply to a stream the scheduler ended cleanly. That is a
// rotation — streams are bounded because a gRPC stream cannot be load balanced
// once it has started — and waiting before reconnecting from one would leave
// capacity idle for no reason, every half hour, forever.
type backoff struct {
	min     time.Duration
	max     time.Duration
	current time.Duration
}

func newBackoff(minDelay, maxDelay time.Duration) *backoff {
	return &backoff{min: minDelay, max: maxDelay, current: minDelay}
}

// next returns the delay to wait and doubles the schedule for the attempt after
// it.
//
// Jittered by plus or minus 20%, because a scheduler restart reconnects every
// executor it had at once, and an unjittered schedule reconnects them all at
// once again on every subsequent attempt.
func (b *backoff) next() time.Duration {
	delay := b.current
	b.current = min(b.current*2, b.max)

	// math/rand/v2 rather than crypto/rand: this picks a wake-up time, and
	// nothing downstream treats it as unguessable.
	jitter := 1 + (rand.Float64()*0.4 - 0.2)
	jittered := time.Duration(float64(delay) * jitter)
	if jittered < b.min {
		jittered = b.min
	}
	return jittered
}

// reset returns the schedule to its floor. Called on a completed handshake: a
// stream that attached and then failed is a different situation from one that
// never attached at all.
func (b *backoff) reset() { b.current = b.min }
