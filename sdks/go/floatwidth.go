package flexiq

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// Widening narrow floats in an already-encoded item.
//
// The wire contract pins a finite float to the 64-bit head (0xfb): a narrower
// width round-trips the same value and hashes differently, and the automatic
// `auto:` idempotency key is a hash over these bytes.
//
// fxamacker honours that for a Go float64 — ShortestFloatNone keeps it wide —
// but not for a float32, whose kind takes the narrow path whatever the option
// says, and there is no encoder option for it. Widening before marshalling
// would mean rebuilding a value through reflection to reach a float32 nested
// inside a struct, which reflect cannot do for a struct with unexported fields.
// So the widening happens after: one pass over the encoded item, rewriting each
// finite half- or single-precision head to its 64-bit form. A caller's own
// cbor.RawMessage is normalised by the same pass, because the contract binds
// the payload's bytes however they were assembled.
//
// A non-finite float is left exactly as it was written. Its width is the
// contract's stated exemption, and NaNConvert/InfConvert in wire.go choose RFC
// 8949's two-byte spelling deliberately.

const (
	majorUnsigned = 0
	majorNegative = 1
	majorByteStr  = 2
	majorTextStr  = 3
	majorArray    = 4
	majorMap      = 5
	majorTag      = 6
	majorPrimitve = 7

	// Additional-information codes that mean "the argument follows in N bytes",
	// and, under major type 7, "a float of that width follows".
	aiOneByte    = 24
	aiTwoBytes   = 25
	aiFourBytes  = 26
	aiEightBytes = 27
	aiIndefinite = 31

	headFloat64 = 0xfb
)

var errTruncated = errors.New("truncated CBOR item")

// widenNarrowFloats rewrites every finite narrow float in one CBOR item.
//
// The pre-check is one-directional, which is the only reason it is safe. A narrow
// float head *is* one of these two bytes, so if neither appears anywhere in the
// item there is no narrow float to rewrite and the input is returned untouched.
// The converse does not hold: either byte can appear as content rather than a
// head — inside a byte string, in a multi-byte argument, in a 64-bit float's own
// mantissa — so its presence decides nothing, and the structural walk below is
// what tells a head from a byte that merely looks like one.
func widenNarrowFloats(item []byte) ([]byte, error) {
	if bytes.IndexByte(item, byte(majorPrimitve)<<5|aiTwoBytes) < 0 &&
		bytes.IndexByte(item, byte(majorPrimitve)<<5|aiFourBytes) < 0 {
		return item, nil
	}

	w := floatWidener{in: item, out: make([]byte, 0, len(item)+8)}
	if err := w.item(); err != nil {
		return nil, err
	}
	if w.pos != len(w.in) {
		return nil, fmt.Errorf("%d byte(s) trailing a complete CBOR item", len(w.in)-w.pos)
	}
	return w.out, nil
}

// floatWidener copies one CBOR item from in to out, head by head.
//
// Recursion is bounded by the input: every level consumes at least its own head
// byte, so a nesting depth is never deeper than the item is long.
type floatWidener struct {
	in  []byte
	pos int
	out []byte
}

func (w *floatWidener) item() error {
	initial, err := w.next()
	if err != nil {
		return err
	}

	major := initial >> 5
	ai := initial & 0x1f

	switch major {
	case majorUnsigned, majorNegative:
		_, err := w.head(initial, ai)
		return err

	case majorByteStr, majorTextStr:
		length, err := w.head(initial, ai)
		if err != nil {
			return err
		}
		return w.copy(length)

	case majorArray, majorMap:
		count, err := w.head(initial, ai)
		if err != nil {
			return err
		}
		// A map's entries are a key and a value, and both are items. Counted in
		// two loops rather than one product, so that a malformed count cannot
		// overflow into a small one.
		entries := 1
		if major == majorMap {
			entries = 2
		}
		for i := uint64(0); i < count; i++ {
			for entry := 0; entry < entries; entry++ {
				if err := w.item(); err != nil {
					return err
				}
			}
		}
		return nil

	case majorTag:
		if _, err := w.head(initial, ai); err != nil {
			return err
		}
		return w.item()

	default:
		return w.primitive(initial, ai)
	}
}

// primitive handles major type 7: the simple values, the break, and the three
// float widths. Only a float is ever rewritten.
func (w *floatWidener) primitive(initial, ai byte) error {
	switch ai {
	case aiTwoBytes, aiFourBytes:
		return w.float(ai)
	case aiEightBytes:
		w.out = append(w.out, initial)
		return w.copy(8)
	case aiOneByte:
		w.out = append(w.out, initial)
		return w.copy(1)
	case aiIndefinite:
		return errors.New("a break outside an indefinite-length container")
	default:
		// A simple value riding in the head byte: false, true, null, undefined.
		w.out = append(w.out, initial)
		return nil
	}
}

// float rewrites a half- or single-precision float as a 64-bit one, unless it is
// non-finite, which the contract exempts from the pinned width.
//
// The value is read back with decMode rather than converted by hand: the same
// library that wrote these bytes knows what a subnormal in either width means.
func (w *floatWidener) float(ai byte) error {
	width := 2
	if ai == aiFourBytes {
		width = 4
	}

	head := w.pos - 1
	if err := w.skip(width); err != nil {
		return err
	}
	encoded := w.in[head:w.pos]

	var value float64
	if err := decMode.Unmarshal(encoded, &value); err != nil {
		return fmt.Errorf("read a %d-bit float back: %w", width*8, err)
	}
	if math.IsInf(value, 0) || math.IsNaN(value) {
		w.out = append(w.out, encoded...)
		return nil
	}

	var wide [1 + 8]byte
	wide[0] = headFloat64
	binary.BigEndian.PutUint64(wide[1:], math.Float64bits(value))
	w.out = append(w.out, wide[:]...)
	return nil
}

// head copies an initial byte and whatever argument follows it, and reports that
// argument.
//
// An indefinite-length head is refused rather than walked. IndefLengthForbidden
// means neither encoding mode can produce one, and it also rejects a caller's
// cbor.RawMessage that carries one, so nothing can reach this pass with an
// indefinite container — and the contract forbids a writer to emit one, so
// guessing at a break here would be a way to smuggle one through.
func (w *floatWidener) head(initial, ai byte) (uint64, error) {
	w.out = append(w.out, initial)

	switch {
	case ai < aiOneByte:
		return uint64(ai), nil
	case ai == aiOneByte:
		return w.argument(1)
	case ai == aiTwoBytes:
		return w.argument(2)
	case ai == aiFourBytes:
		return w.argument(4)
	case ai == aiEightBytes:
		return w.argument(8)
	case ai == aiIndefinite:
		return 0, errors.New("an indefinite-length head, which no writer may emit")
	default:
		return 0, fmt.Errorf("reserved additional information %d", ai)
	}
}

func (w *floatWidener) argument(width int) (uint64, error) {
	start := w.pos
	if err := w.copy(uint64(width)); err != nil {
		return 0, err
	}

	var argument uint64
	for _, b := range w.in[start:w.pos] {
		argument = argument<<8 | uint64(b)
	}
	return argument, nil
}

func (w *floatWidener) next() (byte, error) {
	if w.pos >= len(w.in) {
		return 0, errTruncated
	}
	b := w.in[w.pos]
	w.pos++
	return b, nil
}

// copy moves n bytes across unchanged.
func (w *floatWidener) copy(n uint64) error {
	if n > uint64(len(w.in)-w.pos) {
		return errTruncated
	}
	w.out = append(w.out, w.in[w.pos:w.pos+int(n)]...)
	w.pos += int(n)
	return nil
}

// skip advances past n bytes without copying them, for a head the caller will
// rewrite rather than copy.
func (w *floatWidener) skip(n int) error {
	if n > len(w.in)-w.pos {
		return errTruncated
	}
	w.pos += n
	return nil
}
