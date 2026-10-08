package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
)

// maxSafeInteger is 2^53-1, the largest integer every runtime's JSON reads
// exactly. A bigger one travels as a float, as it would from JavaScript.
const maxSafeInteger = 1<<53 - 1

// jsonKind is the top-level shape a JSON attribute must have.
type jsonKind string

const (
	jsonArray  jsonKind = "array"
	jsonObject jsonKind = "object"
)

// parseJSON decodes one JSON value with its numbers normalised: an integral
// number within ±(2^53-1) becomes int64, anything else float64. Without it
// every number would decode as float64 and `[1]` would be sent as `[1.0]`.
func parseJSON(s string) (any, error) {
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("not valid JSON: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("not valid JSON: data after the first value")
	}
	return normalizeJSON(v)
}

// normalizeJSON rewrites every json.Number in a decoded value, in place.
func normalizeJSON(v any) (any, error) {
	switch x := v.(type) {
	case json.Number:
		return normalizeNumber(x)
	case []any:
		for i, elem := range x {
			n, err := normalizeJSON(elem)
			if err != nil {
				return nil, err
			}
			x[i] = n
		}
		return x, nil
	case map[string]any:
		for key, elem := range x {
			n, err := normalizeJSON(elem)
			if err != nil {
				return nil, err
			}
			x[key] = n
		}
		return x, nil
	default:
		return v, nil
	}
}

func normalizeNumber(n json.Number) (any, error) {
	if i, err := strconv.ParseInt(n.String(), 10, 64); err == nil && i >= -maxSafeInteger && i <= maxSafeInteger {
		return i, nil
	}
	f, err := strconv.ParseFloat(n.String(), 64)
	if err != nil {
		return nil, fmt.Errorf("number %s does not fit a 64-bit float", n)
	}
	if f == math.Trunc(f) && math.Abs(f) <= maxSafeInteger {
		return int64(f), nil
	}
	return f, nil
}

// parseJSONKind is parseJSON that also insists on the top-level shape.
func parseJSONKind(s string, kind jsonKind) (any, error) {
	v, err := parseJSON(s)
	if err != nil {
		return nil, err
	}
	switch v.(type) {
	case []any:
		if kind == jsonArray {
			return v, nil
		}
	case map[string]any:
		if kind == jsonObject {
			return v, nil
		}
	}
	return nil, fmt.Errorf("not a JSON %s", kind)
}

// canonicalJSON is v's compact form, object keys sorted, so two values
// compare as strings.
func canonicalJSON(v any) (string, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return "", err
	}
	return strings.TrimSuffix(buf.String(), "\n"), nil
}

// normalizedJSON parses s and answers its canonical form.
func normalizedJSON(s string) (string, error) {
	v, err := parseJSON(s)
	if err != nil {
		return "", err
	}
	return canonicalJSON(v)
}

// jsonEqual reports whether a and b hold the same JSON value once numbers are
// normalised, so whitespace and key order never show as a diff. Unparsable
// input is equal only to itself.
func jsonEqual(a, b string) bool {
	if a == b {
		return true
	}
	ca, errA := normalizedJSON(a)
	cb, errB := normalizedJSON(b)
	return errA == nil && errB == nil && ca == cb
}

// jsonFromDecoded turns a value decoded from a payload back into canonical
// JSON, numbers normalised the way a config's are.
func jsonFromDecoded(v any) (string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("not representable as JSON: %w", err)
	}
	return normalizedJSON(string(raw))
}

// jsonValidator refuses a string that is not JSON of the given kind.
type jsonValidator struct {
	kind jsonKind
}

var _ validator.String = jsonValidator{}

func (v jsonValidator) Description(context.Context) string {
	return "must be a JSON " + string(v.kind)
}

func (v jsonValidator) MarkdownDescription(ctx context.Context) string {
	return v.Description(ctx)
}

func (v jsonValidator) ValidateString(_ context.Context, req validator.StringRequest, resp *validator.StringResponse) {
	if req.ConfigValue.IsNull() || req.ConfigValue.IsUnknown() {
		return
	}
	if _, err := parseJSONKind(req.ConfigValue.ValueString(), v.kind); err != nil {
		resp.Diagnostics.AddAttributeError(req.Path, "Invalid JSON", err.Error())
	}
}
