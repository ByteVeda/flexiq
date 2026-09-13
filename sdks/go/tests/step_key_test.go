package tests

import (
	"strings"
	"testing"

	"github.com/ByteVeda/flexiq/sdks/go/v2/internal/step"
)

func TestAnUnkeyedStepIsNamedByItsOccurrence(t *testing.T) {
	for _, tc := range []struct {
		occurrence uint32
		want       string
	}{{0, "charge#0"}, {12, "charge#12"}} {
		got, err := step.Derive("charge", tc.occurrence)
		if err != nil {
			t.Fatalf("Derive: %v", err)
		}
		if got != tc.want {
			t.Fatalf("Derive(charge, %d) = %q, want %q", tc.occurrence, got, tc.want)
		}
	}
}

func TestAKeyedStepIsNamedByItsData(t *testing.T) {
	got, err := step.Explicit("fetch", "1234")
	if err != nil {
		t.Fatalf("Explicit: %v", err)
	}
	if got != "fetch:1234" {
		t.Fatalf("Explicit = %q, want %q", got, "fetch:1234")
	}
}

// A name may hold neither separator, so no explicit key can be spelled the way
// an occurrence is, and vice versa.
func TestTheTwoKeyFormsCannotCollide(t *testing.T) {
	derived, err := step.Derive("fetch", 0)
	if err != nil {
		t.Fatalf("Derive: %v", err)
	}
	explicit, err := step.Explicit("fetch", "0")
	if err != nil {
		t.Fatalf("Explicit: %v", err)
	}
	if derived == explicit {
		t.Fatalf("both forms produced %q", derived)
	}
}

func TestAStepNameIsRefusedWhenItCouldNotBeWrittenAsItself(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input string
		want  string
	}{
		{"empty", "", "must not be empty"},
		{"holds the occurrence separator", "charge#1", "separates a name from its key"},
		{"holds the key separator", "charge:one", "separates a name from its key"},
		{"over the byte limit", strings.Repeat("n", 129), "over the 128 byte limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := step.Derive(tc.input, 0); err == nil {
				t.Fatal("Derive accepted the name")
			} else if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Derive error = %q, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestAnExplicitKeyIsRefusedWhenEmptyOrOversized(t *testing.T) {
	if _, err := step.Explicit("fetch", ""); err == nil {
		t.Fatal("Explicit accepted an empty key")
	} else if !strings.Contains(err.Error(), "omit the key to number it by occurrence") {
		t.Fatalf("Explicit error = %q", err)
	}
	if _, err := step.Explicit("fetch", strings.Repeat("k", 257)); err == nil {
		t.Fatal("Explicit accepted an oversized key")
	} else if !strings.Contains(err.Error(), "over the 256 byte limit") {
		t.Fatalf("Explicit error = %q", err)
	}
}

// A key is compared, never parsed back, so a separator inside one is fine.
func TestAnExplicitKeyMayHoldEitherSeparator(t *testing.T) {
	got, err := step.Explicit("fetch", "tenant:7#2")
	if err != nil {
		t.Fatalf("Explicit: %v", err)
	}
	if got != "fetch:tenant:7#2" {
		t.Fatalf("Explicit = %q", got)
	}
}

// Truncation counts characters, not bytes: a message that cut a multi-byte
// rune in half would be unreadable exactly where it matters.
func TestAbbreviateCutsAtACharacterBoundary(t *testing.T) {
	if got := step.Abbreviate("charge"); got != "charge" {
		t.Fatalf("Abbreviate(short) = %q, want it untouched", got)
	}
	long := strings.Repeat("é", 60)
	got := step.Abbreviate(long)
	if want := strings.Repeat("é", 48) + "…"; got != want {
		t.Fatalf("Abbreviate(long) = %q, want %q", got, want)
	}
}

func TestLimitsClampToTheirCeilingsAndBackOffZero(t *testing.T) {
	huge := step.Limits{
		MaxStepBytes:  1 << 30,
		MaxTotalBytes: 1 << 30,
		MaxSteps:      1 << 30,
	}.Clamped()
	if huge.MaxStepBytes != step.MaxStepBytesCeiling ||
		huge.MaxTotalBytes != step.MaxTotalBytesCeiling ||
		huge.MaxSteps != step.MaxStepsCeiling {
		t.Fatalf("Clamped(huge) = %+v", huge)
	}

	// A zero value is a struct nobody filled in, not a cap of nothing.
	empty := step.Limits{}.Clamped()
	if empty != step.DefaultLimits() {
		t.Fatalf("Clamped(zero) = %+v, want the defaults %+v", empty, step.DefaultLimits())
	}
}
