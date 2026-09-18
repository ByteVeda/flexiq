package tests

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/fxamacker/cbor/v2"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
)

// The cross-SDK conformance bar. Every FlexiQ runtime asserts this file in its
// own suite, so an encoder that drifts fails its own build instead of quietly
// producing payloads its peers cannot read.
//
// A hex string here is never edited to make a test pass: a diff to one is a
// wire-format change, and it breaks every job already enqueued.
const vectorsPath = "../../../contracts/wire-vectors.json"

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

			got, err := flexiq.EncodeCall(args, kwargs)
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
			call, err := flexiq.DecodeCall(mustHex(t, v.Hex))
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
			call, err := flexiq.DecodeCall(mustHex(t, v.Hex))
			if err != nil {
				t.Fatalf("DecodeCall: %v", err)
			}
			got, err := flexiq.EncodeCall(call.Args, call.Kwargs)
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

// TestFloatWidths pins both halves of the float rule: a finite float takes the
// 64-bit head even where a narrower width would round-trip it exactly — 1.5 is
// exact in binary16, and a narrower float interoperates while hashing
// differently, which is how it would silently move an `auto:` idempotency key —
// while a non-finite one keeps RFC 8949's two-byte form, the exemption the
// vectors state because CBOR libraries do not agree and mostly cannot be told
// to.
func TestFloatWidths(t *testing.T) {
	for _, tc := range []struct {
		name string
		arg  any
		want string
	}{
		{"finite", 1.5, "028281fb3ff8000000000000a0"},
		{"finite float32", float32(1.5), "028281fb3ff8000000000000a0"},
		{"infinity", math.Inf(1), "028281f97c00a0"},
		{"negative infinity", math.Inf(-1), "028281f9fc00a0"},
		{"nan", math.NaN(), "028281f97e00a0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := flexiq.EncodeCall([]any{tc.arg}, nil)
			if err != nil {
				t.Fatalf("EncodeCall: %v", err)
			}
			if hex.EncodeToString(got) != tc.want {
				t.Errorf("got %s, want %s", hex.EncodeToString(got), tc.want)
			}
		})
	}
}

// TestFloat32IsWidenedAtEveryDepth is the half of the rule fxamacker cannot be
// configured into: ShortestFloatNone keeps a float64 wide but a float32 takes the
// narrow path regardless, so the widening runs over the encoded bytes and has to
// reach a float32 wherever one can sit.
func TestFloat32IsWidenedAtEveryDepth(t *testing.T) {
	const wide = "fb3ff8000000000000"

	for _, tc := range []struct {
		name   string
		args   []any
		kwargs map[string]any
		want   string
	}{
		{"positional", []any{float32(1.5)}, nil, "028281" + wide + "a0"},
		{"keyword", nil, map[string]any{"k": float32(1.5)}, "028280a1616b" + wide},
		{
			"struct field",
			[]any{struct {
				A float32 `cbor:"a"`
			}{A: 1.5}},
			nil,
			"028281a16161" + wide + "a0",
		},
		{"slice element", []any{[]float32{1.5}}, nil, "02828181" + wide + "a0"},
		{"map value", []any{map[string]float32{"a": 1.5}}, nil, "028281a16161" + wide + "a0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := flexiq.EncodeCall(tc.args, tc.kwargs)
			if err != nil {
				t.Fatalf("EncodeCall: %v", err)
			}
			if hex.EncodeToString(got) != tc.want {
				t.Errorf("got %s, want %s", hex.EncodeToString(got), tc.want)
			}
		})
	}
}

// TestFloat32IsWidenedInsideNestedContainers goes deeper than a hand-computed
// envelope is worth: the assertion is that no narrow float survives anywhere and
// that both of them came out wide.
func TestFloat32IsWidenedInsideNestedContainers(t *testing.T) {
	type nested struct {
		Inner []float32      `cbor:"inner"`
		Deep  map[string]any `cbor:"deep"`
	}

	got, err := flexiq.EncodeCall(
		[]any{nested{Inner: []float32{1.5}, Deep: map[string]any{"d": float32(1.5)}}},
		nil,
	)
	if err != nil {
		t.Fatalf("EncodeCall: %v", err)
	}

	encoded := hex.EncodeToString(got)
	if strings.Contains(encoded, "fa3fc00000") || strings.Contains(encoded, "f93e00") {
		t.Errorf("a narrow float survived: %s", encoded)
	}
	if count := strings.Count(encoded, "fb3ff8000000000000"); count != 2 {
		t.Errorf("want two 64-bit floats, got %d in %s", count, encoded)
	}
}

// The three tests below are the risk the byte pass carries: it walks a payload
// nothing parsed before, so anything it mishandles is silent corruption rather
// than an error. Every encode vector already asserts its bytes through that pass,
// and these are the shapes the vectors do not reach.
func TestWideningLeavesAByteStringAlone(t *testing.T) {
	// Contents that are exactly a narrow float head and argument. The walk has to
	// honour the string's length instead of rewriting what it finds inside — a
	// text string cannot carry the trap, no UTF-8 byte being 0xf9 or 0xfa, but a
	// byte string can.
	literal := []byte{0xfa, 0x3f, 0xc0, 0x00, 0x00, 0xf9, 0x3e, 0x00}

	encoded, err := flexiq.EncodeResult(literal)
	if err != nil {
		t.Fatalf("EncodeResult: %v", err)
	}

	var survived []byte
	if decodeErr := flexiq.DecodeResult(encoded, &survived); decodeErr != nil {
		t.Fatalf("DecodeResult: %v (%s)", decodeErr, hex.EncodeToString(encoded))
	}
	if !bytes.Equal(survived, literal) {
		t.Errorf("a byte string was rewritten: got %x, want %x", survived, literal)
	}
}

// TestWideningKeepsEveryShapeDecodable covers a tag and a nesting the vectors do
// not reach. Each has to come out as something a decoder still reads.
func TestWideningKeepsEveryShapeDecodable(t *testing.T) {
	for _, value := range []any{
		new(big.Int).SetUint64(math.MaxUint64),
		map[string]any{"a": []any{1, "b", nil, true, []byte{0xfa}}},
		struct {
			A map[string][]any `cbor:"a"`
		}{A: map[string][]any{"b": {[]any{[]any{0.5}}}}},
	} {
		encoded, err := flexiq.EncodeResult(value)
		if err != nil {
			t.Fatalf("EncodeResult(%v): %v", value, err)
		}

		var back any
		if decodeErr := flexiq.DecodeResult(encoded, &back); decodeErr != nil {
			t.Errorf("DecodeResult(%v): %v — the widening pass produced bytes no decoder accepts: %s",
				value, decodeErr, hex.EncodeToString(encoded))
		}
	}
}

// TestWideningMovesOnlyTheNarrowFloat puts both widths in one call.
func TestWideningMovesOnlyTheNarrowFloat(t *testing.T) {
	mixed, err := flexiq.EncodeCall([]any{float32(1.5), 2.5}, nil)
	if err != nil {
		t.Fatalf("EncodeCall: %v", err)
	}
	if got, want := hex.EncodeToString(mixed), "028282fb3ff8000000000000fb4004000000000000a0"; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

// TestWideningReachesPreEncodedBytes covers the one way a narrow float arrives
// without a Go float32 behind it.
//
// A cbor.RawMessage is bytes the caller assembled, and the contract binds a
// payload's bytes however they were assembled — so the pass normalises those too,
// inside a container and behind a tag. An indefinite-length container is the
// exception: IndefLengthForbidden rejects one before the pass sees it, which is
// what keeps that shape off the wire.
func TestWideningReachesPreEncodedBytes(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  cbor.RawMessage
		want string
	}{
		{"bare", cbor.RawMessage{0xfa, 0x3f, 0xc0, 0x00, 0x00}, "02fb3ff8000000000000"},
		{"in an array", cbor.RawMessage{0x81, 0xfa, 0x3f, 0xc0, 0x00, 0x00}, "0281fb3ff8000000000000"},
		{"behind a tag", cbor.RawMessage{0xc1, 0xfa, 0x3f, 0xc0, 0x00, 0x00}, "02c1fb3ff8000000000000"},
		{"half precision", cbor.RawMessage{0xf9, 0x3e, 0x00}, "02fb3ff8000000000000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := flexiq.EncodeResult(tc.raw)
			if err != nil {
				t.Fatalf("EncodeResult: %v", err)
			}
			if hex.EncodeToString(got) != tc.want {
				t.Errorf("got %s, want %s", hex.EncodeToString(got), tc.want)
			}
		})
	}

	indefinite := cbor.RawMessage{0x9f, 0xfa, 0x3f, 0xc0, 0x00, 0x00, 0xff}
	if _, err := flexiq.EncodeResult(indefinite); err == nil {
		t.Error("an indefinite-length raw message must be refused, not widened")
	}
}

// TestEnvelopeRejectsForeignTags proves the tag byte is checked rather than
// sniffed past: a payload this client cannot read must fail naming its tag, so
// a caller can tell "another SDK's native format" from "corrupt bytes".
func TestEnvelopeRejectsForeignTags(t *testing.T) {
	for _, tag := range []byte{flexiq.TagNative, flexiq.TagMessagePack, 0x03, 0xff} {
		_, err := flexiq.DecodeCall([]byte{tag, 0x80})
		if err == nil {
			t.Fatalf("tag 0x%02x decoded as if it were CBOR", tag)
		}
		if !errors.Is(err, flexiq.ErrUnsupportedTag) {
			t.Errorf("tag 0x%02x: want ErrUnsupportedTag, got %v", tag, err)
		}
		if !bytes.Contains([]byte(err.Error()), []byte(fmt.Sprintf("0x%02x", tag))) {
			t.Errorf("tag 0x%02x: error text does not name the tag: %v", tag, err)
		}
	}

	if _, err := flexiq.DecodeCall(nil); err == nil {
		t.Error("an empty payload decoded without error")
	}
}

// TestDecodeResultIsNotAnArray pins the shape that catches people: a payload is
// the tag then a two-element array, a result is the tag then a bare value.
func TestDecodeResultIsNotAnArray(t *testing.T) {
	var got bool
	if err := flexiq.DecodeResult([]byte{flexiq.TagCBOR, 0xf5}, &got); err != nil {
		t.Fatalf("DecodeResult: %v", err)
	}
	if !got {
		t.Error("decoded result is false, want true")
	}

	var wide uint64
	if err := flexiq.DecodeResult(mustHex(t, "021b0020000000000000"), &wide); err != nil {
		t.Fatalf("DecodeResult: %v", err)
	}
	if wide != 1<<53 {
		t.Errorf("decoded result is %d, want %d", wide, uint64(1)<<53)
	}
}

// TestKeywordOrderIsStable covers the one container this client builds itself.
// Go map iteration order is unspecified, so without sorted keys the same call
// would encode to different bytes on different runs, and a caller deriving a
// unique key by hashing its own payload would get a different key each time.
func TestKeywordOrderIsStable(t *testing.T) {
	kwargs := map[string]any{
		"zulu": 1, "alpha": 2, "mike": 3, "bravo": 4,
		"yankee": 5, "delta": 6, "kilo": 7, "echo": 8,
	}

	first, err := flexiq.EncodeCall(nil, kwargs)
	if err != nil {
		t.Fatalf("EncodeCall: %v", err)
	}
	for i := range 50 {
		again, encodeErr := flexiq.EncodeCall(nil, kwargs)
		if encodeErr != nil {
			t.Fatalf("EncodeCall: %v", encodeErr)
		}
		if !bytes.Equal(first, again) {
			t.Fatalf("run %d encoded different bytes\n got: %s\nwant: %s",
				i, hex.EncodeToString(again), hex.EncodeToString(first))
		}
	}

	// Sorted, and still decodable as what went in.
	call, err := flexiq.DecodeCall(first)
	if err != nil {
		t.Fatalf("DecodeCall: %v", err)
	}
	if len(call.Kwargs) != len(kwargs) {
		t.Errorf("decoded %d keyword arguments, want %d", len(call.Kwargs), len(kwargs))
	}
	// Tag, then [args, kwargs]: 82 for the two-element array, 80 for the empty
	// args, a8 for an eight-entry definite-length map.
	if want := "02" + "8280" + "a8"; !strings.HasPrefix(hex.EncodeToString(first), want) {
		t.Errorf("body does not open %s: %s", want, hex.EncodeToString(first))
	}

	// The order is RFC 8949's: keys compare as their *encoded* bytes, so the
	// length header sorts first and "echo" (64 65 63 68 6f) leads a set whose
	// alphabetical first is "alpha". That is the deterministic order every
	// CBOR implementation agrees on, which is the point of choosing it.
	if !strings.HasPrefix(hex.EncodeToString(first), "028280a8"+"646563686f") {
		t.Errorf("keyword keys are not in the deterministic order: %s", hex.EncodeToString(first))
	}
}

// TestKeywordValuesKeepTheirOwnOrder: sorting applies to the keyword map's
// keys and stops there. A struct passed as a keyword argument still encodes in
// declaration order, which is what another runtime would have sent.
func TestKeywordValuesKeepTheirOwnOrder(t *testing.T) {
	type order struct {
		OrderID     string `cbor:"order_id"`
		AmountCents int    `cbor:"amount_cents"`
	}

	got, err := flexiq.EncodeCall(nil, map[string]any{
		"payload": order{OrderID: "ord-0001", AmountCents: 1000},
	})
	if err != nil {
		t.Fatalf("EncodeCall: %v", err)
	}

	// The same object bytes the single-object-arg vector pins, unsorted:
	// order_id first, amount_cents second.
	objectBytes := "a2686f726465725f6964686f72642d303030316c616d6f756e745f63656e74731903e8"
	if !strings.Contains(hex.EncodeToString(got), objectBytes) {
		t.Errorf("a struct keyword argument was reordered\n got: %s\nwant it to contain: %s",
			hex.EncodeToString(got), objectBytes)
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
