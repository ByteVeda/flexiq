package flexiq

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/fxamacker/cbor/v2"
)

var mapStringAny = reflect.TypeOf(map[string]any(nil))

// Payload envelope tags. A payload is one tag byte followed by the body that
// codec produced; the tag is what makes a payload readable by a runtime that
// did not write it.
//
// Only [TagCBOR] is written. The others are recognised so that a payload this
// client cannot read fails naming its tag rather than as a generic decode
// error — a caller can act on "this job was enqueued by a pickle-writing SDK"
// and cannot act on "invalid CBOR".
const (
	// TagNative marks a language-native body (Python's pickle, and its peers).
	// Never cross-SDK.
	TagNative byte = 0x00
	// TagMessagePack marks a MessagePack body. Legacy: readable by some
	// runtimes, written by none.
	TagMessagePack byte = 0x01
	// TagCBOR marks a CBOR body, the cross-SDK default. This client writes
	// this and nothing else.
	TagCBOR byte = 0x02
)

// ErrUnsupportedTag is returned when a payload carries a tag this client does
// not decode. Match it with [errors.Is]; the error text names the tag.
var ErrUnsupportedTag = errors.New("unsupported payload tag")

// Call is the decoded form of a job's payload: the positional and keyword
// arguments the task was enqueued with.
//
// The wire form is always a two-element array, so Kwargs is present and empty
// for a call made from a language that has no keyword arguments.
type Call struct {
	Args   []any
	Kwargs map[string]any
}

// encMode and decMode pin the two encoder rules the cross-SDK contract states,
// neither of which is a matter of style:
//
//   - Definite-length containers only. Both forms decode identically, so a
//     writer that emits the indefinite form still interoperates — which is the
//     danger. An SDK's automatic `auto:` idempotency key is a hash over these
//     bytes, and every call body ends in the kwargs map, so a divergent writer
//     would shift every payload's key at once and silently stop idempotent
//     enqueues deduping across runtimes.
//   - Shortest-form integers, for the same reason.
//
// Sorting stays off. A CBOR map is unordered, but the bytes are not: the
// contract pins an object argument's keys in the order the caller wrote them,
// which for Go means the order a struct declares its fields. Sorting would
// reorder those and produce bytes no other SDK produces for the same call.
//
// The decoder is the permissive half on purpose — readers must accept the
// indefinite form even though writers must not emit it.
var (
	encMode = mustEncMode(cbor.EncOptions{
		Sort:          cbor.SortNone,
		IndefLength:   cbor.IndefLengthForbidden,
		ShortestFloat: cbor.ShortestFloatNone,
		NaNConvert:    cbor.NaNConvert7e00,
		InfConvert:    cbor.InfConvertFloat16,
	})
	// kwargsMode differs from encMode in one setting, and it applies to one
	// value: the top-level keyword map.
	//
	// Go map iteration order is unspecified, so encoding that map with sorting
	// off would give the same call different bytes on different runs — and a
	// caller deriving a unique key by hashing its own payload would get a
	// different key each time. Sorting the keys makes it stable.
	//
	// The order is RFC 8949's core-deterministic one: keys compare as their
	// encoded bytes, so the length header sorts ahead of the text and shorter
	// keys come first. It looks unalphabetical and is the order every CBOR
	// implementation agrees on.
	//
	// It is safe to sort here and nowhere else because the keyword map is the
	// only container this client builds itself. Its *values* are marshalled
	// separately with encMode and spliced in pre-encoded, so a struct inside a
	// keyword argument still encodes in declaration order.
	kwargsMode = mustEncMode(cbor.EncOptions{
		Sort:          cbor.SortBytewiseLexical,
		IndefLength:   cbor.IndefLengthForbidden,
		ShortestFloat: cbor.ShortestFloatNone,
		NaNConvert:    cbor.NaNConvert7e00,
		InfConvert:    cbor.InfConvertFloat16,
	})
	decMode = mustDecMode(cbor.DecOptions{
		// A CBOR map keyed by anything but a string has no natural Go form
		// here; decoding to map[string]any is what makes a decoded payload
		// usable without a type assertion per level. A payload with non-string
		// keys fails loudly rather than arriving as map[any]any.
		DefaultMapType: mapStringAny,
		IndefLength:    cbor.IndefLengthAllowed,
	})
)

// EncodeCall builds the payload envelope for a call: the CBOR tag byte
// followed by a definite-length two-element array of args and kwargs.
//
// A nil slice or map is encoded as its empty form. The array is always two
// elements, so a call with neither argument kind is not an empty payload.
//
// The keyword map's own keys are sorted, so the same call encodes to the same
// bytes every time. A map *inside* an argument is not: Go map iteration order
// is unspecified, and sorting one would reorder keys the caller wrote in a
// particular order. Where those bytes matter — deriving a unique key by
// hashing the payload, or matching what another runtime would have sent — pass
// a struct: its fields encode in declaration order, every time.
func EncodeCall(args []any, kwargs map[string]any) ([]byte, error) {
	if args == nil {
		args = []any{}
	}

	encodedArgs, err := encMode.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("flexiq: encode call args: %w", err)
	}
	encodedKwargs, err := encodeKwargs(kwargs)
	if err != nil {
		return nil, err
	}

	// Both halves are already CBOR, so this writes the two-element array header
	// and copies them in.
	body, err := encMode.Marshal([2]cbor.RawMessage{encodedArgs, encodedKwargs})
	if err != nil {
		return nil, fmt.Errorf("flexiq: encode call: %w", err)
	}
	return append([]byte{TagCBOR}, body...), nil
}

// encodeKwargs encodes the keyword map with its keys sorted and its values
// left exactly as encMode wrote them.
func encodeKwargs(kwargs map[string]any) (cbor.RawMessage, error) {
	encoded := make(map[string]cbor.RawMessage, len(kwargs))
	for key, value := range kwargs {
		raw, err := encMode.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("flexiq: encode keyword argument %q: %w", key, err)
		}
		encoded[key] = raw
	}

	body, err := kwargsMode.Marshal(encoded)
	if err != nil {
		return nil, fmt.Errorf("flexiq: encode call kwargs: %w", err)
	}
	return body, nil
}

// DecodeCall reads a payload envelope back into the call it describes.
func DecodeCall(payload []byte) (Call, error) {
	body, err := unwrap(payload)
	if err != nil {
		return Call{}, err
	}

	// Decoded one level at a time so that a body which is not a two-element
	// array is reported as the malformed envelope it is, rather than as a type
	// error about one of its halves.
	var parts []cbor.RawMessage
	if err := decMode.Unmarshal(body, &parts); err != nil {
		return Call{}, fmt.Errorf("flexiq: decode call body: %w", err)
	}
	if len(parts) != 2 {
		return Call{}, fmt.Errorf("flexiq: decode call body: want a 2-element array, got %d", len(parts))
	}

	var call Call
	if err := decMode.Unmarshal(parts[0], &call.Args); err != nil {
		return Call{}, fmt.Errorf("flexiq: decode call args: %w", err)
	}
	if err := decMode.Unmarshal(parts[1], &call.Kwargs); err != nil {
		return Call{}, fmt.Errorf("flexiq: decode call kwargs: %w", err)
	}
	return call, nil
}

// DecodeCallInto decodes a payload's positional arguments into targets, in
// order. Each target is a pointer to the type that argument was sent as.
//
// It exists beside [DecodeCall] because that one decodes into `any`, which
// turns every number into whatever CBOR's widest form for it is and every
// object into a map. Decoding straight into the caller's own type is what makes
// a payload usable without a type switch per field.
//
// A call carrying keyword arguments is refused rather than bound positionally.
// Go has nothing to bind a keyword argument to, and binding them by position
// would pair a name with whatever happened to be next. Read them with
// [DecodeCall] instead. The cross-SDK convention is a single object argument —
// `args = [{…}]`, `kwargs = {}` — so a call written for more than one runtime
// rarely carries any.
func DecodeCallInto(payload []byte, targets ...any) error {
	args, kwargs, err := splitCall(payload)
	if err != nil {
		return err
	}
	if len(kwargs) > 0 {
		return fmt.Errorf(
			"flexiq: decode call: it carries %d keyword argument(s), which a Go handler takes no form of; read them with DecodeCall",
			len(kwargs),
		)
	}
	if len(args) < len(targets) {
		return fmt.Errorf("flexiq: decode call: want %d positional argument(s), got %d", len(targets), len(args))
	}

	for i, target := range targets {
		if err := decMode.Unmarshal(args[i], target); err != nil {
			return fmt.Errorf("flexiq: decode positional argument %d: %w", i, err)
		}
	}
	return nil
}

// splitCall unwraps the envelope and separates the call body's two halves,
// leaving each argument encoded so a caller can decode it into its own type.
func splitCall(payload []byte) ([]cbor.RawMessage, map[string]cbor.RawMessage, error) {
	body, err := unwrap(payload)
	if err != nil {
		return nil, nil, err
	}

	var parts []cbor.RawMessage
	if err := decMode.Unmarshal(body, &parts); err != nil {
		return nil, nil, fmt.Errorf("flexiq: decode call body: %w", err)
	}
	if len(parts) != 2 {
		return nil, nil, fmt.Errorf("flexiq: decode call body: want a 2-element array, got %d", len(parts))
	}

	var args []cbor.RawMessage
	if err := decMode.Unmarshal(parts[0], &args); err != nil {
		return nil, nil, fmt.Errorf("flexiq: decode call args: %w", err)
	}
	var kwargs map[string]cbor.RawMessage
	if err := decMode.Unmarshal(parts[1], &kwargs); err != nil {
		return nil, nil, fmt.Errorf("flexiq: decode call kwargs: %w", err)
	}
	return args, kwargs, nil
}

// EncodeResult builds the envelope for a task's return value: the CBOR tag
// byte followed by a bare CBOR value.
//
// The asymmetry with [EncodeCall] is the point. A call body is a two-element
// array because there are two things to pair — positional and keyword
// arguments. A result is one value, so there is nothing to wrap it in, and
// wrapping it anyway would make every reader unwrap a one-element array to
// find out.
//
// A map inside the value encodes in whatever order Go iterates it, for the
// reason [EncodeCall] gives. Nothing hashes a result, so it costs nothing here.
func EncodeResult(v any) ([]byte, error) {
	body, err := encMode.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("flexiq: encode result: %w", err)
	}
	return append([]byte{TagCBOR}, body...), nil
}

// DecodeResult reads a job's result envelope into v.
//
// A result is not shaped like a payload: the tag byte is followed by a bare
// CBOR value, with no array around it. Pass a pointer to the type the task
// returns, or to any for whatever it happens to be.
func DecodeResult(result []byte, v any) error {
	body, err := unwrap(result)
	if err != nil {
		return err
	}
	if err := decMode.Unmarshal(body, v); err != nil {
		return fmt.Errorf("flexiq: decode result: %w", err)
	}
	return nil
}

// unwrap strips and checks the envelope's tag byte.
//
// An untagged body is never guessed at. A raw CBOR or MessagePack body can
// begin with any byte value, so sniffing one would eventually misread a payload
// as a codec that did not write it.
func unwrap(envelope []byte) ([]byte, error) {
	if len(envelope) == 0 {
		return nil, fmt.Errorf("flexiq: empty payload, expected a tag byte")
	}
	tag := envelope[0]
	if tag != TagCBOR {
		return nil, fmt.Errorf("flexiq: %w 0x%02x (%s)", ErrUnsupportedTag, tag, tagName(tag))
	}
	return envelope[1:], nil
}

func tagName(tag byte) string {
	switch tag {
	case TagNative:
		return "language-native, never cross-SDK"
	case TagMessagePack:
		return "MessagePack, which this client does not decode"
	case TagCBOR:
		return "CBOR"
	default:
		return "unknown to this build"
	}
}

func mustEncMode(opts cbor.EncOptions) cbor.EncMode {
	mode, err := opts.EncMode()
	if err != nil {
		panic("flexiq: invalid CBOR encoder options: " + err.Error())
	}
	return mode
}

func mustDecMode(opts cbor.DecOptions) cbor.DecMode {
	mode, err := opts.DecMode()
	if err != nil {
		panic("flexiq: invalid CBOR decoder options: " + err.Error())
	}
	return mode
}
