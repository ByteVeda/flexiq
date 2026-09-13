package step

import (
	"fmt"
	"strings"
)

// Mirrors crates/flexiq-core/src/step/key.rs.
const (
	// maxNameBytes bounds a step name. A name is written by hand at the call
	// site; a limit this generous only ever catches one built from data by
	// mistake.
	maxNameBytes = 128
	// maxKeyBytes bounds an explicit key. Keys are built from data — an order
	// id, a tenant — so they get more room than a name.
	maxKeyBytes = 256

	// occurrenceSeparator joins a name to its occurrence counter.
	occurrenceSeparator = "#"
	// keySeparator joins a name to an explicit key.
	keySeparator = ":"
)

// Derive names the occurrence-numbered form of a step: "name#occurrence",
// where occurrence is how many times this name has already been asked for in
// this attempt.
//
// Stable only while the surrounding code asks for the same names in the same
// order, which is exactly what the divergence check verifies. A loop over
// anything whose order is not guaranteed wants Explicit instead.
func Derive(name string, occurrence uint32) (string, error) {
	if err := validateName(name); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s%s%d", name, occurrenceSeparator, occurrence), nil
}

// Explicit names a step by its data rather than its position: "name:key".
//
// A key is only ever compared, never parsed back, so it may hold anything the
// caller likes, separators included.
func Explicit(name, key string) (string, error) {
	if err := validateName(name); err != nil {
		return "", err
	}
	if err := validateKey(name, key); err != nil {
		return "", err
	}
	return name + keySeparator + key, nil
}

// validateName refuses a name that could not be written as itself inside a
// key. "charge#1" as a name would collide with the second occurrence of
// "charge", so neither separator is allowed in one.
func validateName(name string) error {
	if name == "" {
		return fmt.Errorf("a step name must not be empty")
	}
	if len(name) > maxNameBytes {
		return fmt.Errorf("step name %q is %d bytes, over the %d byte limit",
			Abbreviate(name), len(name), maxNameBytes)
	}
	for _, separator := range []string{occurrenceSeparator, keySeparator} {
		if strings.Contains(name, separator) {
			return fmt.Errorf("step name %q contains %q, which separates a name from its key",
				Abbreviate(name), separator)
		}
	}
	return nil
}

func validateKey(name, key string) error {
	if key == "" {
		return fmt.Errorf("step %q was given an empty key; omit the key to number it by occurrence",
			Abbreviate(name))
	}
	if len(key) > maxKeyBytes {
		return fmt.Errorf("key %q of step %q is %d bytes, over the %d byte limit",
			Abbreviate(key), Abbreviate(name), len(key), maxKeyBytes)
	}
	return nil
}

// Abbreviate bounds what an error message quotes back.
//
// The value that failed is often the reason it failed — a name built from a
// payload — and pasting all of it into a log line helps nobody.
func Abbreviate(value string) string {
	const maxChars = 48

	chars := 0
	for offset := range value {
		if chars == maxChars {
			return value[:offset] + "…"
		}
		chars++
	}
	return value
}
