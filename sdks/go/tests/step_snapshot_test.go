package tests

import (
	"bytes"
	"strings"
	"testing"

	"github.com/ByteVeda/flexiq/sdks/go/v2/internal/step"
)

func wakeAt(ms int64) *int64 { return &ms }

func encodeSnapshot(t *testing.T, records []step.Record) []byte {
	t.Helper()
	payload, err := step.EncodeSnapshot(records)
	if err != nil {
		t.Fatalf("EncodeSnapshot: %v", err)
	}
	return payload
}

func TestASnapshotRoundTripsThroughItsOwnCodec(t *testing.T) {
	records := []step.Record{
		{Seq: 0, StepKey: "charge#0", Kind: step.KindRun, Result: []byte{0x02, 0xf5}, CreatedAt: 11},
		{Seq: 1, StepKey: "nap#0", Kind: step.KindSleep, WakeAt: wakeAt(1750), CreatedAt: 12},
		{Seq: 2, StepKey: "notify:42", Kind: step.KindRun, Result: []byte{}, CreatedAt: 13},
	}

	decoded, err := step.DecodeSnapshot("job-1", encodeSnapshot(t, records))
	if err != nil {
		t.Fatalf("DecodeSnapshot: %v", err)
	}
	if len(decoded) != len(records) {
		t.Fatalf("decoded %d records, want %d", len(decoded), len(records))
	}
	for i, want := range records {
		got := decoded[i]
		if got.Seq != want.Seq || got.StepKey != want.StepKey || got.Kind != want.Kind {
			t.Fatalf("record %d = %+v, want %+v", i, got, want)
		}
		if !bytes.Equal(got.Result, want.Result) {
			t.Fatalf("record %d result = %v, want %v", i, got.Result, want.Result)
		}
		if (got.WakeAt == nil) != (want.WakeAt == nil) {
			t.Fatalf("record %d wake_at = %v, want %v", i, got.WakeAt, want.WakeAt)
		}
	}
}

// Absent and present-and-empty are different answers, and the length field
// says which: null is "committed no result", 0 is "committed an empty one".
func TestAnAbsentResultIsNotAnEmptyOne(t *testing.T) {
	payload := encodeSnapshot(t, []step.Record{
		{Seq: 0, StepKey: "sleep#0", Kind: step.KindSleep, WakeAt: wakeAt(9), CreatedAt: 1},
		{Seq: 1, StepKey: "void#0", Kind: step.KindRun, Result: []byte{}, CreatedAt: 2},
	})
	if !bytes.Contains(payload, []byte(`"result_len":null`)) {
		t.Fatalf("a sleep should encode a null length, got %s", payload)
	}

	decoded, err := step.DecodeSnapshot("job-1", payload)
	if err != nil {
		t.Fatalf("DecodeSnapshot: %v", err)
	}
	if decoded[0].Result != nil {
		t.Fatalf("a sleep decoded a result: %v", decoded[0].Result)
	}
	if decoded[1].Result == nil {
		t.Fatal("an empty result decoded as absent")
	}
}

func TestADamagedSnapshotFailsRatherThanComingBackShort(t *testing.T) {
	whole := encodeSnapshot(t, []step.Record{
		{Seq: 0, StepKey: "charge#0", Kind: step.KindRun, Result: []byte("0123456789"), CreatedAt: 1},
	})

	for _, tc := range []struct {
		name    string
		payload []byte
		want    string
	}{
		{"no metadata line", []byte(`[]`), "has no metadata line"},
		{"unreadable metadata", []byte("not json\n"), "has an unreadable metadata line"},
		{"truncated blob", whole[:len(whole)-4], "is truncated at step charge#0 (6 of 10 bytes)"},
		{"trailing bytes", append(append([]byte{}, whole...), 'x'), "carries 1 byte(s) no step claims"},
		{
			"a kind this build does not know",
			[]byte(`[{"seq":0,"step_key":"x#0","kind":"teleport","result_len":null,` +
				`"wake_at":null,"created_at":1}]` + "\n"),
			`unknown step kind "teleport"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			records, err := step.DecodeSnapshot("job-1", tc.payload)
			if err == nil {
				t.Fatalf("DecodeSnapshot accepted it and returned %d records", len(records))
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to mention %q", err, tc.want)
			}
			if !strings.Contains(err.Error(), "job-1") {
				t.Fatalf("error = %q, want it to name the job", err)
			}
		})
	}
}

// An empty snapshot is a legal snapshot: the metadata line is "[]" and no
// blobs follow it. Only an absent *frame* means "this job has no steps".
func TestAnEmptySnapshotDecodesToNoRecords(t *testing.T) {
	records, err := step.DecodeSnapshot("job-1", []byte("[]\n"))
	if err != nil {
		t.Fatalf("DecodeSnapshot: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("decoded %d records, want none", len(records))
	}
}
