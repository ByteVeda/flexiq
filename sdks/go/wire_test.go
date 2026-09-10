package flexiq

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// The cross-SDK conformance bar. Every FlexiQ runtime asserts this file in its
// own suite, so an encoder that drifts fails its own build instead of quietly
// producing payloads its peers cannot read.
//
// A hex string here is never edited to make a test pass: a diff to one is a
// wire-format change, and it breaks every job already enqueued.
const vectorsPath = "../../contracts/wire-vectors.json"

type vectorFile struct {
	SchemaVersion int      `json:"$schema_version"`
	Encode        []vector `json:"encode"`
	DecodeOnly    []vector `json:"decode_only"`
}

type vector struct {
	Name          string                     `json:"name"`
	Hex           string                     `json:"hex"`
	Args          []json.RawMessage          `json:"args"`
	Kwargs        map[string]json.RawMessage `json:"kwargs"`
	RoundTripOnly bool                       `json:"round_trip_only"`
}

func loadVectors(t *testing.T) vectorFile {
	t.Helper()

	raw, err := os.ReadFile(filepath.FromSlash(vectorsPath))
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var file vectorFile
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatalf("parse vectors: %v", err)
	}
	if file.SchemaVersion != 1 {
		t.Fatalf("vector schema version %d is not the 1 this test was written against", file.SchemaVersion)
	}
	if len(file.Encode) == 0 || len(file.DecodeOnly) == 0 {
		t.Fatal("vector file carries no cases")
	}
	return file
}

// TestEncodeVectors is the producing half of the obligation: this client must
// emit the exact pinned bytes for every case its call API can express, and Go
// can express all of them.
func TestEncodeVectors(t *testing.T) {
	for _, v := range loadVectors(t).Encode {
		t.Run(v.Name, func(t *testing.T) {
			args, kwargs := callFor(t, v)

			got, err := EncodeCall(args, kwargs)
			if err != nil {
				t.Fatalf("EncodeCall: %v", err)
			}
			if hex.EncodeToString(got) != v.Hex {
				t.Errorf("encoded bytes drifted from the contract\n got: %s\nwant: %s",
					hex.EncodeToString(got), v.Hex)
			}
		})
	}
}

// TestDecodeVectors is the reading half, and it covers both arrays: a client
// must decode every case, including the three it is not asked to produce.
func TestDecodeVectors(t *testing.T) {
	file := loadVectors(t)
	for _, v := range append(append([]vector{}, file.Encode...), file.DecodeOnly...) {
		t.Run(v.Name, func(t *testing.T) {
			call, err := DecodeCall(mustHex(t, v.Hex))
			if err != nil {
				t.Fatalf("DecodeCall: %v", err)
			}
			if v.RoundTripOnly {
				// The value is not stated because JSON cannot hold it, so
				// there is nothing to compare a decode against. The bytes are
				// pinned by TestRoundTripOnlyVectors instead.
				return
			}

			assertEqual(t, "args", call.Args, expectedArgs(t, v))
			assertEqual(t, "kwargs", call.Kwargs, expectedKwargs(t, v))
		})
	}
}

// TestRoundTripOnlyVectors covers the two cases JSON cannot state: an integer
// past double precision, and a byte string. Definite length and shortest-form
// integers leave exactly one encoding of each, so re-encoding what was decoded
// has to land back on the same bytes.
func TestRoundTripOnlyVectors(t *testing.T) {
	found := 0
	for _, v := range loadVectors(t).DecodeOnly {
		if !v.RoundTripOnly {
			continue
		}
		found++
		t.Run(v.Name, func(t *testing.T) {
			call, err := DecodeCall(mustHex(t, v.Hex))
			if err != nil {
				t.Fatalf("DecodeCall: %v", err)
			}
			got, err := EncodeCall(call.Args, call.Kwargs)
			if err != nil {
				t.Fatalf("EncodeCall: %v", err)
			}
			if hex.EncodeToString(got) != v.Hex {
				t.Errorf("round trip changed the bytes\n got: %s\nwant: %s",
					hex.EncodeToString(got), v.Hex)
			}
		})
	}
	if found != 2 {
		t.Errorf("expected 2 round_trip_only vectors, found %d — the contract's exemptions moved", found)
	}
}

// TestEnvelopeRejectsForeignTags proves the tag byte is checked rather than
// sniffed past: a payload this client cannot read must fail naming its tag, so
// a caller can tell "another SDK's native format" from "corrupt bytes".
func TestEnvelopeRejectsForeignTags(t *testing.T) {
	for _, tag := range []byte{TagNative, TagMessagePack, 0x03, 0xff} {
		_, err := DecodeCall([]byte{tag, 0x80})
		if err == nil {
			t.Fatalf("tag 0x%02x decoded as if it were CBOR", tag)
		}
		if !errors.Is(err, ErrUnsupportedTag) {
			t.Errorf("tag 0x%02x: want ErrUnsupportedTag, got %v", tag, err)
		}
		if !bytes.Contains([]byte(err.Error()), []byte(fmt.Sprintf("0x%02x", tag))) {
			t.Errorf("tag 0x%02x: error text does not name the tag: %v", tag, err)
		}
	}

	if _, err := DecodeCall(nil); err == nil {
		t.Error("an empty payload decoded without error")
	}
}

// TestDecodeResultIsNotAnArray pins the shape that catches people: a payload is
// the tag then a two-element array, a result is the tag then a bare value.
func TestDecodeResultIsNotAnArray(t *testing.T) {
	var got bool
	if err := DecodeResult([]byte{TagCBOR, 0xf5}, &got); err != nil {
		t.Fatalf("DecodeResult: %v", err)
	}
	if !got {
		t.Error("decoded result is false, want true")
	}

	var wide uint64
	if err := DecodeResult(mustHex(t, "021b0020000000000000"), &wide); err != nil {
		t.Fatalf("DecodeResult: %v", err)
	}
	if wide != 1<<53 {
		t.Errorf("decoded result is %d, want %d", wide, uint64(1)<<53)
	}
}

// callFor turns a vector's JSON arguments into the Go values this client's call
// API takes.
//
// Object arguments become structs, not maps. That is not a convenience: a CBOR
// map is unordered but its bytes are not, and only a struct pins the field
// order the vector was written with. A new object vector with no struct behind
// it fails loudly here rather than encoding in Go's random map order.
func callFor(t *testing.T, v vector) ([]any, map[string]any) {
	t.Helper()

	type order struct {
		OrderID     string `cbor:"order_id"`
		AmountCents int    `cbor:"amount_cents"`
	}
	type innerObject struct {
		B string `cbor:"b"`
	}
	type nestedObject struct {
		A []any `cbor:"a"`
	}

	args := make([]any, 0, len(v.Args))
	for i, raw := range v.Args {
		switch {
		case v.Name == "single-object-arg" && i == 0:
			args = append(args, order{OrderID: "ord-0001", AmountCents: 1000})
		case v.Name == "nested-structures" && i == 0:
			args = append(args, nestedObject{A: []any{1, 2, innerObject{B: "c"}}})
		default:
			value := jsonToGo(t, raw)
			if _, isObject := value.(map[string]any); isObject {
				t.Fatalf("vector %q argument %d is an object with no Go struct behind it; "+
					"add one so its key order is pinned", v.Name, i)
			}
			args = append(args, value)
		}
	}

	kwargs := make(map[string]any, len(v.Kwargs))
	for k, raw := range v.Kwargs {
		kwargs[k] = jsonToGo(t, raw)
	}
	return args, kwargs
}

func expectedArgs(t *testing.T, v vector) []any {
	t.Helper()

	args := make([]any, 0, len(v.Args))
	for _, raw := range v.Args {
		args = append(args, jsonToGo(t, raw))
	}
	return args
}

func expectedKwargs(t *testing.T, v vector) map[string]any {
	t.Helper()

	kwargs := make(map[string]any, len(v.Kwargs))
	for k, raw := range v.Kwargs {
		kwargs[k] = jsonToGo(t, raw)
	}
	return kwargs
}

// jsonToGo reads one value out of the vector file and returns the Go value a
// caller would have passed for it. Numbers are read exactly — json.Number, not
// float64 — so an integer vector stays an integer instead of arriving as a
// float and encoding as one.
func jsonToGo(t *testing.T, raw json.RawMessage) any {
	t.Helper()

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		t.Fatalf("parse vector value %s: %v", raw, err)
	}
	return asGoValue(t, value)
}

// asGoValue turns a parsed JSON value into the Go value a caller would pass:
// an exact integer where the vector states one, a float only where the vector
// really is a float.
func asGoValue(t *testing.T, value any) any {
	t.Helper()

	switch v := value.(type) {
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i
		}
		f, err := v.Float64()
		if err != nil {
			t.Fatalf("vector number %s is neither an integer nor a float: %v", v, err)
		}
		return f
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = asGoValue(t, item)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, item := range v {
			out[k] = asGoValue(t, item)
		}
		return out
	default:
		return value
	}
}

// assertEqual compares a decoded CBOR value against the vector's statement of
// it. CBOR distinguishes integer widths and signedness where JSON has one
// number type, so both sides are reduced to a comparable shape first.
func assertEqual(t *testing.T, label string, got, want any) {
	t.Helper()

	gotFlat, wantFlat := flatten(t, got), flatten(t, want)
	if !reflect.DeepEqual(gotFlat, wantFlat) {
		t.Errorf("decoded %s does not match the vector\n got: %#v\nwant: %#v", label, gotFlat, wantFlat)
	}
}

// flatten reduces a value to a form where a number compares equal however it
// was written: CBOR's unsigned and negative major types both land on the same
// decimal string.
func flatten(t *testing.T, value any) any {
	t.Helper()

	switch v := value.(type) {
	case int:
		return "i" + big.NewInt(int64(v)).String()
	case int64:
		return "i" + big.NewInt(v).String()
	case uint64:
		return "i" + new(big.Int).SetUint64(v).String()
	case float64:
		return fmt.Sprintf("f%g", v)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = flatten(t, item)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, item := range v {
			out[k] = flatten(t, item)
		}
		return out
	default:
		return v
	}
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()

	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("decode vector hex %q: %v", s, err)
	}
	return b
}
