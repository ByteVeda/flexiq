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

// canonicalTaskError is the cross-SDK JSON shape this client writes: the three
// keys, in the order the contract pins them. Only the writer uses it — see
// ParseTaskError for why reading goes key by key instead.
type canonicalTaskError struct {
	ErrType   string   `json:"errtype"`
	Message   string   `json:"message"`
	Traceback []string `json:"traceback"`
}

// ParseTaskError reads a job's recorded error.
//
// It never fails. A string that is not the canonical JSON object comes back
// with Structured false and the raw text as the message — losing it would lose
// the only account of why the job failed.
//
// The contract's fallback rule turns on `message` alone, so the document is
// read key by key rather than decoded into canonicalTaskError: a single absent
// or wrong-typed sibling fails a whole-struct decode, which would surface a
// failure that does carry a message as prose and lose its errtype with it. A
// writer that omits a sibling, or writes it as null, is leaving a default to
// fill — the same defaults the other SDK readers apply.
func ParseTaskError(raw string) TaskError {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		return TaskError{Message: raw, Raw: raw}
	}

	message, ok := decodeString(fields["message"])
	if !ok {
		return TaskError{Message: raw, Raw: raw}
	}
	errType, ok := decodeString(fields["errtype"])
	if !ok {
		errType = "Error"
	}

	return TaskError{
		Type:       errType,
		Message:    message,
		Traceback:  decodeFrames(fields["traceback"]),
		Structured: true,
		Raw:        raw,
	}
}

// decodeString reads one key as a string. An absent key (a nil raw message), a
// null and a value of any other type all report false, so a caller can tell
// "the writer said nothing" from "the writer said an empty string".
func decodeString(value json.RawMessage) (string, bool) {
	// Through a pointer, because unmarshalling a JSON null into a string is a
	// no-op that reports no error and leaves the zero value behind.
	var decoded *string
	if err := json.Unmarshal(value, &decoded); err != nil || decoded == nil {
		return "", false
	}
	return *decoded, true
}

// decodeFrames reads the traceback. An absent key, a null and a non-array all
// mean no frames, and a frame that is not a string is dropped rather than
// taking the rest of the document with it.
func decodeFrames(value json.RawMessage) []string {
	var entries []json.RawMessage
	if err := json.Unmarshal(value, &entries); err != nil {
		return []string{}
	}

	frames := make([]string, 0, len(entries))
	for _, entry := range entries {
		if frame, ok := decodeString(entry); ok {
			frames = append(frames, frame)
		}
	}
	return frames
}

// Error makes a TaskError usable as a Go error, so a caller can return a failed
// job's error up its own stack.
func (e TaskError) Error() string {
	if e.Type == "" {
		return e.Message
	}
	return e.Type + ": " + e.Message
}

// EncodeTaskError writes the canonical cross-SDK JSON a failed job records:
// the three keys, in the order the contract pins them, with no extra
// whitespace.
//
// Traceback is written as [] and never as null, and Type as "Error" rather
// than being omitted: the contract makes all three keys required and pins the
// traceback's type to an array. A reader has to fill the gap either way, so
// leaving one is only a chance for two readers to fill it differently.
func EncodeTaskError(errType, message string, traceback []string) string {
	if errType == "" {
		errType = "Error"
	}
	if traceback == nil {
		traceback = []string{}
	}

	// Marshalled through the struct rather than a map, because a map would sort
	// the keys and the contract pins their order.
	encoded, err := json.Marshal(canonicalTaskError{
		ErrType:   errType,
		Message:   message,
		Traceback: traceback,
	})
	if err != nil {
		// Unreachable: every field is a string or a slice of them, and
		// encoding/json cannot fail on those. Losing the message would lose the
		// only account of why the job failed, so fall back to the prose form,
		// which every reader accepts.
		return message
	}
	return string(encoded)
}
