package tests

import (
	"encoding/hex"
	"testing"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
)

// The two encoders the executor door needs and the producer door never did:
// a result has to be written as well as read, and a failed job's error has to
// be formatted as well as parsed.

func TestEncodeTaskErrorMatchesTheCrossSDKVector(t *testing.T) {
	got := flexiq.EncodeTaskError("BoomError", "it broke", []string{"frame1", "frame2"})
	want := `{"errtype":"BoomError","message":"it broke","traceback":["frame1","frame2"]}`
	if got != want {
		t.Fatalf("EncodeTaskError() =\n  %s\nwant\n  %s", got, want)
	}
}

func TestEncodeTaskErrorWritesAnEmptyTracebackRatherThanNull(t *testing.T) {
	// The key is required and its type is an array. A null is a third shape
	// every reader has to special-case, and a reader that does not will take
	// the whole document for prose and lose the errtype with it.
	got := flexiq.EncodeTaskError("ValueError", "bad value", nil)
	want := `{"errtype":"ValueError","message":"bad value","traceback":[]}`
	if got != want {
		t.Fatalf("EncodeTaskError() =\n  %s\nwant\n  %s", got, want)
	}
}

func TestEncodeTaskErrorNamesSomethingWhenTheRuntimeCannot(t *testing.T) {
	got := flexiq.EncodeTaskError("", "just a message", nil)
	want := `{"errtype":"Error","message":"just a message","traceback":[]}`
	if got != want {
		t.Fatalf("EncodeTaskError() =\n  %s\nwant\n  %s", got, want)
	}
}

func TestAnEncodedTaskErrorParsesBackAsStructured(t *testing.T) {
	encoded := flexiq.EncodeTaskError("ValueError", "bad value 42", []string{"frame"})
	parsed := flexiq.ParseTaskError(encoded)

	if !parsed.Structured {
		t.Fatalf("ParseTaskError did not read this client's own output as canonical: %q", encoded)
	}
	if parsed.Type != "ValueError" || parsed.Message != "bad value 42" || len(parsed.Traceback) != 1 {
		t.Fatalf("round trip lost something: %+v", parsed)
	}
}

func TestEncodeResultIsABareValueBehindTheTagByte(t *testing.T) {
	// A call body is a two-element array because there are two things to pair.
	// A result is one value, so there is nothing to wrap it in. The contract's
	// vector for `true` is 02 f5.
	encoded, err := flexiq.EncodeResult(true)
	if err != nil {
		t.Fatalf("EncodeResult: %v", err)
	}
	if got := hex.EncodeToString(encoded); got != "02f5" {
		t.Fatalf("EncodeResult(true) = %s, want 02f5", got)
	}
}

func TestEncodeResultRoundTripsThroughDecodeResult(t *testing.T) {
	type receipt struct {
		OrderID string `cbor:"order_id"`
		Cents   int64  `cbor:"cents"`
	}

	encoded, err := flexiq.EncodeResult(receipt{OrderID: "ord-1", Cents: 1000})
	if err != nil {
		t.Fatalf("EncodeResult: %v", err)
	}

	var decoded receipt
	if err := flexiq.DecodeResult(encoded, &decoded); err != nil {
		t.Fatalf("DecodeResult: %v", err)
	}
	if decoded.OrderID != "ord-1" || decoded.Cents != 1000 {
		t.Fatalf("round trip gave %+v", decoded)
	}
}

func TestDecodeCallIntoBindsPositionalArgumentsToTheirOwnTypes(t *testing.T) {
	type order struct {
		ID string `cbor:"id"`
	}

	payload, err := flexiq.EncodeCall([]any{order{ID: "ord-1"}, int64(7)}, nil)
	if err != nil {
		t.Fatalf("EncodeCall: %v", err)
	}

	var first order
	var second int64
	if err := flexiq.DecodeCallInto(payload, &first, &second); err != nil {
		t.Fatalf("DecodeCallInto: %v", err)
	}
	if first.ID != "ord-1" || second != 7 {
		t.Fatalf("decoded %+v and %d", first, second)
	}
}

func TestDecodeCallIntoRefusesAKeywordArgument(t *testing.T) {
	payload, err := flexiq.EncodeCall([]any{1}, map[string]any{"flag": true})
	if err != nil {
		t.Fatalf("EncodeCall: %v", err)
	}

	var first int64
	if err := flexiq.DecodeCallInto(payload, &first); err == nil {
		t.Fatal("DecodeCallInto bound positionally past a keyword argument it has nowhere to put")
	}
}

func TestDecodeCallIntoRefusesFewerArgumentsThanTargets(t *testing.T) {
	payload, err := flexiq.EncodeCall([]any{1}, nil)
	if err != nil {
		t.Fatalf("EncodeCall: %v", err)
	}

	var first, second int64
	if err := flexiq.DecodeCallInto(payload, &first, &second); err == nil {
		t.Fatal("DecodeCallInto accepted a call with fewer arguments than targets")
	}
}
