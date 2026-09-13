package step

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Mirrors encode_step_snapshot / decode_step_snapshot in
// crates/flexiq-core/src/worker/protocol.rs.

// Kind says whether a committed step carries a memoized value or a deadline.
type Kind string

const (
	// KindRun is a step whose result is recorded so a later attempt replays it.
	KindRun Kind = "run"
	// KindSleep is a step.sleep: the attempt ends and the job is rescheduled.
	KindSleep Kind = "sleep"
)

// UnmarshalJSON refuses a kind this build does not know.
//
// There is no safe default. A sleep read as a run replays a deadline as a
// value; a run read as a sleep ends the attempt. Failing the whole snapshot is
// the only answer that cannot quietly be wrong.
func (k *Kind) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	switch Kind(raw) {
	case KindRun, KindSleep:
		*k = Kind(raw)
		return nil
	default:
		return fmt.Errorf("unknown step kind %q", Abbreviate(raw))
	}
}

// Record is one step a job has already committed.
type Record struct {
	// Seq is its position in the job's sequence: 0-based and gapless.
	Seq int32
	// StepKey is its identity, "name#occurrence" or "name:key".
	StepKey string
	// Kind says whether it holds a value or a deadline.
	Kind Kind
	// Result is the stored bytes. Nil is no result — every sleep, and a run
	// committed without one; a non-nil empty slice is an empty result. They
	// are different answers.
	Result []byte
	// WakeAt is the deadline a sleep row was given, in Unix milliseconds.
	WakeAt *int64
	// CreatedAt is when the row was written, in Unix milliseconds.
	CreatedAt int64
}

// snapshotEntry is one record's metadata inside an encoded snapshot.
//
// The blob is not here: a result serialized as a JSON array of numbers would
// inflate a snapshot several-fold. The bytes ride behind the metadata line
// instead, which is the same trade the outer frame makes. job_id is
// deliberately absent — the frame already names it, and a second copy could
// only disagree.
//
// Every field is a pointer, including the four the scheduler always writes.
// Decoded into values they would take a zero on an absent or null key and say
// nothing about it, where the scheduler's own decoder refuses both — serde has
// no default for a bare i32 or String. The difference is not cosmetic: a
// missing step_key reads as "" and a keyed replay then misses its row and runs
// the body again, which is the double charge this whole file exists to prevent.
type snapshotEntry struct {
	Seq       *int32  `json:"seq"`
	StepKey   *string `json:"step_key"`
	Kind      *Kind   `json:"kind"`
	ResultLen *int    `json:"result_len"`
	WakeAt    *int64  `json:"wake_at"`
	CreatedAt *int64  `json:"created_at"`
}

// required names the first field this entry did not supply, or "" when it
// supplied them all. result_len and wake_at are absent from the list on
// purpose: both are genuinely optional, and null is a value there.
func (e snapshotEntry) required() string {
	switch {
	case e.Seq == nil:
		return "seq"
	case e.StepKey == nil:
		return "step_key"
	case e.Kind == nil:
		return "kind"
	case e.CreatedAt == nil:
		return "created_at"
	default:
		return ""
	}
}

// Prefix of every message this file produces, so an operator grepping for one
// finds the rest.
const snapshotFault = "the step snapshot for job %s "

// EncodeSnapshot writes records the way the scheduler does: a JSON metadata
// line, one newline, then every blob concatenated in Seq order.
//
// The inverse of [DecodeSnapshot], and the only honest way to test it.
func EncodeSnapshot(records []Record) ([]byte, error) {
	entries := make([]snapshotEntry, 0, len(records))
	for _, record := range records {
		entry := snapshotEntry{
			Seq:       &record.Seq,
			StepKey:   &record.StepKey,
			Kind:      &record.Kind,
			WakeAt:    record.WakeAt,
			CreatedAt: &record.CreatedAt,
		}
		if record.Result != nil {
			length := len(record.Result)
			entry.ResultLen = &length
		}
		entries = append(entries, entry)
	}

	metadata, err := json.Marshal(entries)
	if err != nil {
		return nil, fmt.Errorf("encoding a step snapshot: %w", err)
	}
	var payload bytes.Buffer
	payload.Write(metadata)
	payload.WriteByte('\n')
	for _, record := range records {
		payload.Write(record.Result)
	}
	return payload.Bytes(), nil
}

// DecodeSnapshot reads a JobStepsFrame snapshot back into the job's steps.
//
// Fails rather than returning what it could parse. A snapshot that silently
// came back short is a memo that silently went missing, and that re-runs a
// charge — the one outcome the whole feature exists to prevent.
//
// An absent frame means an *empty* snapshot, never an unknown one, so the
// caller never reaches this with nothing to decode.
func DecodeSnapshot(jobID string, payload []byte) ([]Record, error) {
	split := bytes.IndexByte(payload, '\n')
	if split < 0 {
		return nil, fmt.Errorf(snapshotFault+"has no metadata line", jobID)
	}

	var entries []snapshotEntry
	if err := json.Unmarshal(payload[:split], &entries); err != nil {
		return nil, fmt.Errorf(snapshotFault+"has an unreadable metadata line: %w", jobID, err)
	}
	if entries == nil {
		// encoding/json accepts a bare null for a slice and leaves it nil, where
		// the scheduler's own decoder refuses one. Unrefused it is the worst
		// shape there is: a damaged snapshot that reads as "no steps recorded",
		// which runs every durable step body again. An empty "[]" stays valid.
		return nil, fmt.Errorf(snapshotFault+"has a null metadata line", jobID)
	}

	blobs := payload[split+1:]
	records := make([]Record, 0, len(entries))
	for position, entry := range entries {
		if missing := entry.required(); missing != "" {
			return nil, fmt.Errorf(snapshotFault+"is missing %s at position %d",
				jobID, missing, position)
		}
		result, rest, err := takeBlob(jobID, *entry.StepKey, entry.ResultLen, blobs)
		if err != nil {
			return nil, err
		}
		blobs = rest
		records = append(records, Record{
			Seq:       *entry.Seq,
			StepKey:   *entry.StepKey,
			Kind:      *entry.Kind,
			Result:    result,
			WakeAt:    entry.WakeAt,
			CreatedAt: *entry.CreatedAt,
		})
	}

	if len(blobs) > 0 {
		return nil, fmt.Errorf(snapshotFault+"carries %d byte(s) no step claims", jobID, len(blobs))
	}
	return records, nil
}

// takeBlob splits one entry's result off the front of the remaining bytes.
//
// Copied rather than sub-sliced: a Record outlives the frame it was decoded
// from, and a slice into that buffer would pin the whole snapshot in memory
// for the life of the job.
func takeBlob(jobID, stepKey string, resultLen *int, blobs []byte) (result, rest []byte, err error) {
	if resultLen == nil {
		return nil, blobs, nil
	}
	length := *resultLen
	if length < 0 {
		return nil, nil, fmt.Errorf(snapshotFault+"declares a negative length for step %s",
			jobID, Abbreviate(stepKey))
	}
	if len(blobs) < length {
		return nil, nil, fmt.Errorf(snapshotFault+"is truncated at step %s (%d of %d bytes)",
			jobID, Abbreviate(stepKey), len(blobs), length)
	}
	result = make([]byte, length)
	copy(result, blobs[:length])
	return result, blobs[length:], nil
}
