package flexiq

import "encoding/json"

// TaskError is the failure a job recorded.
//
// It is not a request failure. The RPC that returned it succeeded: a failed job
// is data. See [Error] for the other kind.
type TaskError struct {
	// Type is the exception class name, in the raising language's own
	// vocabulary — "ValueError" from Python, "IllegalStateException" from
	// Java. Empty when the error was not structured.
	Type string
	// Message is the failure text. For an unstructured error it is the whole
	// raw string.
	Message string
	// Traceback is the frames the raising runtime recorded, if any.
	Traceback []string
	// Structured is false when the recorded error was not canonical JSON.
	// That is not corruption: a timeout, a worker-death recovery, an expiry
	// and a cancellation are all plain text by design.
	Structured bool
	// Raw is what the job actually recorded, always.
	Raw string
}

// canonicalTaskError is the cross-SDK JSON shape. Every field is required —
// message may be empty and traceback may be [], but a document missing either
// key is not this shape and is surfaced as prose instead.
type canonicalTaskError struct {
	ErrType   *string   `json:"errtype"`
	Message   *string   `json:"message"`
	Traceback *[]string `json:"traceback"`
}

// ParseTaskError reads a job's recorded error.
//
// It never fails. A string that is not the canonical JSON object comes back
// with Structured false and the raw text as the message — losing it would lose
// the only account of why the job failed.
func ParseTaskError(raw string) TaskError {
	var parsed canonicalTaskError
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return TaskError{Message: raw, Raw: raw}
	}
	if parsed.ErrType == nil || parsed.Message == nil || parsed.Traceback == nil {
		return TaskError{Message: raw, Raw: raw}
	}

	traceback := *parsed.Traceback
	if traceback == nil {
		traceback = []string{}
	}
	return TaskError{
		Type:       *parsed.ErrType,
		Message:    *parsed.Message,
		Traceback:  traceback,
		Structured: true,
		Raw:        raw,
	}
}

// Error makes a TaskError usable as a Go error, so a caller can return a failed
// job's error up its own stack.
func (e TaskError) Error() string {
	if e.Type == "" {
		return e.Message
	}
	return e.Type + ": " + e.Message
}
