package flexiq

import (
	"reflect"
	"testing"
)

// TestTaskErrorParsesTheCanonicalShape covers the cross-SDK JSON a raising
// runtime writes.
func TestTaskErrorParsesTheCanonicalShape(t *testing.T) {
	raw := `{"errtype":"ValueError","message":"bad value 42","traceback":["frame one","frame two"]}`

	got := ParseTaskError(raw)
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
		`{"errtype":"ValueError"}`, // missing keys: not the shape
		`{"message":"no errtype","traceback":[]}`,
		"not json at all {",
	}

	for _, raw := range unstructured {
		got := ParseTaskError(raw)
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

// TestTaskErrorKeepsEmptyRequiredFields: message may be "" and traceback may be
// [], and both still make the canonical shape.
func TestTaskErrorKeepsEmptyRequiredFields(t *testing.T) {
	got := ParseTaskError(`{"errtype":"Cancelled","message":"","traceback":[]}`)

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
