package tests

import (
	"reflect"
	"testing"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
)

// TestTaskErrorParsesTheCanonicalShape covers the cross-SDK JSON a raising
// runtime writes.
func TestTaskErrorParsesTheCanonicalShape(t *testing.T) {
	raw := `{"errtype":"ValueError","message":"bad value 42","traceback":["frame one","frame two"]}`

	got := flexiq.ParseTaskError(raw)
	if !got.Structured {
		t.Fatal("the canonical shape was not recognised")
	}
	if got.Type != "ValueError" || got.Message != "bad value 42" {
		t.Errorf("type/message are %q/%q", got.Type, got.Message)
	}
	if !reflect.DeepEqual(got.Traceback, []string{"frame one", "frame two"}) {
		t.Errorf("traceback is %v", got.Traceback)
	}
	if got.Raw != raw {
		t.Error("the raw text was not kept")
	}
	if got.Error() != "ValueError: bad value 42" {
		t.Errorf("Error() is %q", got.Error())
	}
}

// TestTaskErrorSurfacesUnstructuredVerbatim is the rule that matters most here:
// a timeout, a worker-death recovery, an expiry and a cancellation are all
// plain text by design. A client that raised on one would lose the only account
// of why the job failed.
func TestTaskErrorSurfacesUnstructuredVerbatim(t *testing.T) {
	unstructured := []string{
		"job timed out after 30s",
		"worker died; recovered by the scheduler",
		"",
		"[]",
		"null",
		`"a bare JSON string"`,
		`{"errtype":"ValueError"}`, // no message: not the shape
		`{"errtype":"ValueError","message":5,"traceback":[]}`,
		"not json at all {",
	}

	for _, raw := range unstructured {
		got := flexiq.ParseTaskError(raw)
		if got.Structured {
			t.Errorf("%q was read as the canonical shape", raw)
		}
		if got.Message != raw || got.Raw != raw {
			t.Errorf("%q came back as message %q / raw %q", raw, got.Message, got.Raw)
		}
		if got.Type != "" {
			t.Errorf("%q produced a type %q out of nothing", raw, got.Type)
		}
	}
}

// TestTaskErrorFillsAbsentSiblings covers the contract's fallback rule, which
// turns on `message` alone: a document carrying one is the canonical shape, and
// an absent, null or wrong-typed sibling is a default to fill rather than
// grounds to reject the whole document as prose. The first case is verbatim
// what a Rust worker records for a failed task.
func TestTaskErrorFillsAbsentSiblings(t *testing.T) {
	cases := []struct {
		raw           string
		wantType      string
		wantMessage   string
		wantTraceback []string
	}{
		{
			`{"errtype":"TaskError","message":"card declined","traceback":null}`,
			"TaskError", "card declined", []string{},
		},
		{`{"message":"only a message"}`, "Error", "only a message", []string{}},
		{`{"errtype":null,"message":"null siblings","traceback":null}`, "Error", "null siblings", []string{}},
		{`{"errtype":5,"message":"wrong-typed siblings","traceback":"oops"}`, "Error", "wrong-typed siblings", []string{}},
		{
			`{"errtype":"ValueError","message":"mixed frames","traceback":["frame one",7]}`,
			"ValueError", "mixed frames", []string{"frame one"},
		},
	}

	for _, tc := range cases {
		got := flexiq.ParseTaskError(tc.raw)

		if !got.Structured {
			t.Errorf("%s was read as prose", tc.raw)
			continue
		}
		if got.Type != tc.wantType || got.Message != tc.wantMessage {
			t.Errorf("%s parsed as type/message %q/%q", tc.raw, got.Type, got.Message)
		}
		if !reflect.DeepEqual(got.Traceback, tc.wantTraceback) {
			t.Errorf("%s parsed as traceback %#v", tc.raw, got.Traceback)
		}
		if got.Raw != tc.raw {
			t.Errorf("%s did not keep its raw text", tc.raw)
		}
	}
}

// TestTaskErrorKeepsEmptyRequiredFields: message may be "" and traceback may be
// [], and both still make the canonical shape.
func TestTaskErrorKeepsEmptyRequiredFields(t *testing.T) {
	got := flexiq.ParseTaskError(`{"errtype":"Cancelled","message":"","traceback":[]}`)

	if !got.Structured {
		t.Fatal("an empty message and traceback are still the canonical shape")
	}
	if got.Type != "Cancelled" || got.Message != "" || len(got.Traceback) != 0 {
		t.Errorf("parsed as %+v", got)
	}
	if got.Error() != "Cancelled: " {
		t.Errorf("Error() is %q", got.Error())
	}
}
