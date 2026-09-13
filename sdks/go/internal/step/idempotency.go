package step

import "encoding/json"

// Mirrors crates/flexiq-core/src/step/idempotency.rs.

// OriginJobIDKey carries the id the durable run started under.
//
// Written by retry_dead, which mints a **new** job id: without it an operator
// retrying a dead-lettered charge three days later sends a fresh key and
// charges the customer a second time — deliberately, through the admin UI.
//
// The double-underscore prefix marks it as the runtime's, not a user's.
const OriginJobIDKey = "__origin_job_id"

// RunKey is the durable run key of a job: the id its run began under.
//
// The job's own id for one that has only ever been retried in place — an
// ordinary retry, a requeue and a step.sleep wake all keep the row and its id.
// The stamped origin for one resurrected from the dead-letter queue, which is
// the single boundary where the id changes.
//
// metadata is the job's metadata blob as stored, opaque JSON text. Anything
// unreadable, absent or blank falls back to the job id — never to a *blank*
// run key, which would put every job in the deployment in one key space and
// dedupe each other's charges away.
func RunKey(jobID, metadata string) string {
	if origin := originJobID(metadata); origin != "" {
		return origin
	}
	return jobID
}

// IdempotencyKey is "{runKey}:{stepKey}" — the key to hand the *downstream*
// service.
//
// Memoization closes the replay window, not the crash window. Between "the
// charge succeeded" and "the step row committed" there is an instant where the
// process can die, and the next attempt has no record that the call happened —
// so it makes it again. Nothing on this side of the network can fix that. The
// only fix is a key the other service honours, and the only key that works is
// one this job mints the same way every time it runs.
//
// Derived from the run's identity and the step's position, and from nothing
// else: no clock, no payload, no serializer, no codec.
//
// Two limits worth knowing before relying on it. The key covers one run, not
// one order: two jobs enqueued for the same order have two run keys and will
// both charge. And downstream keys expire, typically after 24 hours, so a step
// that sleeps past that window and then replays is a new request whatever key
// it sends.
func IdempotencyKey(runKey, stepKey string) string {
	return runKey + ":" + stepKey
}

func originJobID(metadata string) string {
	if metadata == "" {
		return ""
	}
	// Read key by key rather than decoded into a shape. The blob is a caller's
	// and may be any JSON at all — an array, a bare string, a number — and a
	// whole-value decode would turn "this key is not here" into "this metadata
	// is broken". Neither answer changes what happens, but only one of them is
	// true.
	var parsed map[string]any
	if err := json.Unmarshal([]byte(metadata), &parsed); err != nil {
		return ""
	}
	origin, _ := parsed[OriginJobIDKey].(string)
	return origin
}
