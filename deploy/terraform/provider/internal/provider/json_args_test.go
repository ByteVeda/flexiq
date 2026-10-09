package provider

import (
	"context"
	"reflect"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
)

func TestParseJSONNormalisesNumbers(t *testing.T) {
	cases := []struct {
		in   string
		want any
	}{
		{`[1]`, []any{int64(1)}},
		{`[1.0]`, []any{int64(1)}},
		{`[-0.0]`, []any{int64(0)}},
		{`[1e3]`, []any{int64(1000)}},
		{`[1.5]`, []any{1.5}},
		{`[9007199254740991]`, []any{int64(9007199254740991)}},
		{`[-9007199254740991]`, []any{int64(-9007199254740991)}},
		// Past 2^53-1 an integer is a float, as every runtime's JSON reads it.
		{`[9007199254740992]`, []any{float64(9007199254740992)}},
		{`[-9007199254740993]`, []any{float64(-9007199254740992)}},
		{`[18446744073709551616]`, []any{float64(18446744073709551616)}},
		{`[9007199254740992.0]`, []any{float64(9007199254740992)}},
		{
			`{"a": [{"b": 2.0, "c": 0.25}], "d": null, "e": "1", "f": true}`,
			map[string]any{"a": []any{map[string]any{"b": int64(2), "c": 0.25}}, "d": nil, "e": "1", "f": true},
		},
	}
	for _, tc := range cases {
		got, err := parseJSON(tc.in)
		if err != nil {
			t.Errorf("parseJSON(%s): %v", tc.in, err)
			continue
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("parseJSON(%s) = %#v, want %#v", tc.in, got, tc.want)
		}
	}
}

func TestParseJSONRefuses(t *testing.T) {
	for _, in := range []string{``, `[1`, `[1] [2]`, `[1e400]`, `{"a":1}x`, `nope`} {
		if _, err := parseJSON(in); err == nil {
			t.Errorf("parseJSON(%q) = nil error, want one", in)
		}
	}
}

func TestParseJSONKind(t *testing.T) {
	cases := []struct {
		in   string
		kind jsonKind
		ok   bool
	}{
		{`[]`, jsonArray, true},
		{`{}`, jsonObject, true},
		{`{}`, jsonArray, false},
		{`[]`, jsonObject, false},
		{`1`, jsonArray, false},
		{`"x"`, jsonObject, false},
		{`null`, jsonObject, false},
	}
	for _, tc := range cases {
		_, err := parseJSONKind(tc.in, tc.kind)
		if (err == nil) != tc.ok {
			t.Errorf("parseJSONKind(%s, %s) err = %v, want ok %v", tc.in, tc.kind, err, tc.ok)
		}
	}
}

func TestJSONEqual(t *testing.T) {
	equal := [][2]string{
		{`[1, 2]`, `[1,2]`},
		{`{"b": 1, "a": 2}`, `{"a":2,"b":1}`},
		{`[1]`, `[1.0]`},
		{`[1e2]`, `[100]`},
		{"[\n  {\"x\": [true, null]}\n]", `[{"x":[true,null]}]`},
		// Both sides round to the same float, which is what the server keeps.
		{`[9007199254740993]`, `[9007199254740992]`},
		{`not json`, `not json`},
	}
	for _, pair := range equal {
		if !jsonEqual(pair[0], pair[1]) {
			t.Errorf("jsonEqual(%s, %s) = false, want true", pair[0], pair[1])
		}
	}
	differ := [][2]string{
		{`[1]`, `[2]`},
		{`[1]`, `["1"]`},
		{`[1, 2]`, `[2, 1]`},
		{`{"a":1}`, `{"a":1,"b":null}`},
		{`[1.5]`, `[1]`},
		{`not json`, `[]`},
	}
	for _, pair := range differ {
		if jsonEqual(pair[0], pair[1]) {
			t.Errorf("jsonEqual(%s, %s) = true, want false", pair[0], pair[1])
		}
	}
}

// TestJSONSurvivesThePayload sends JSON through the payload codec and reads it
// back: what Read sees must equal what Create sent.
func TestJSONSurvivesThePayload(t *testing.T) {
	const args = `[1, 1.5, -3, 9007199254740993, "s", null, {"z": [true], "a": 2.0}]`
	const kwargs = `{"to": "ops", "n": 18446744073709551616, "nested": {"k": [0.1]}}`

	argsValue, err := parseJSONKind(args, jsonArray)
	if err != nil {
		t.Fatal(err)
	}
	kwargsValue, err := parseJSONKind(kwargs, jsonObject)
	if err != nil {
		t.Fatal(err)
	}
	argList, _ := argsValue.([]any)
	kwargMap, _ := kwargsValue.(map[string]any)
	payload, err := flexiq.EncodeCall(argList, kwargMap)
	if err != nil {
		t.Fatalf("EncodeCall: %v", err)
	}
	call, err := flexiq.DecodeCall(payload)
	if err != nil {
		t.Fatalf("DecodeCall: %v", err)
	}

	// An integral number went out as a CBOR integer, not a float.
	if _, isFloat := call.Args[0].(float64); isFloat {
		t.Errorf("args[0] decoded as a float: %#v", call.Args[0])
	}
	gotArgs, err := jsonFromDecoded(call.Args)
	if err != nil {
		t.Fatal(err)
	}
	gotKwargs, err := jsonFromDecoded(call.Kwargs)
	if err != nil {
		t.Fatal(err)
	}
	if !jsonEqual(gotArgs, args) {
		t.Errorf("args read back as %s, want %s", gotArgs, args)
	}
	if !jsonEqual(gotKwargs, kwargs) {
		t.Errorf("kwargs read back as %s, want %s", gotKwargs, kwargs)
	}
}

func TestUnsafeIntegers(t *testing.T) {
	got := unsafeIntegers(`[9007199254740991, -9007199254740991, 9007199254740992, {"n": -18446744073709551616}, 1e20, 2.5]`)
	if want := []string{"9007199254740992", "-18446744073709551616"}; !reflect.DeepEqual(got, want) {
		t.Errorf("unsafeIntegers = %v, want %v", got, want)
	}
	if got := unsafeIntegers(`[1, "9007199254740993"]`); len(got) != 0 {
		t.Errorf("a string holding digits is not a number: %v", got)
	}
}

func TestJSONValidatorWarnsOnUnsafeIntegers(t *testing.T) {
	cases := []struct {
		value        string
		wantError    bool
		wantWarnings int
	}{
		{value: `[1, 2]`},
		{value: `[9007199254740993]`, wantWarnings: 1},
		{value: `{"a": 1}`, wantError: true},
	}
	for _, tc := range cases {
		var resp validator.StringResponse
		jsonValidator{kind: jsonArray}.ValidateString(context.Background(), validator.StringRequest{
			Path: path.Root("args"), ConfigValue: types.StringValue(tc.value),
		}, &resp)
		if resp.Diagnostics.HasError() != tc.wantError || resp.Diagnostics.WarningsCount() != tc.wantWarnings {
			t.Errorf("%s: diagnostics = %v, want error %v and %d warnings",
				tc.value, resp.Diagnostics, tc.wantError, tc.wantWarnings)
		}
	}
}

func TestKeepEquivalentState(t *testing.T) {
	m := keepEquivalentState{equal: jsonEqual}
	cases := []struct {
		name        string
		plan, state types.String
		want        types.String
	}{
		{"equivalent keeps state", types.StringValue(`[1.0, 2]`), types.StringValue(`[1,2]`), types.StringValue(`[1,2]`)},
		{"different keeps plan", types.StringValue(`[3]`), types.StringValue(`[1,2]`), types.StringValue(`[3]`)},
		{"create keeps plan", types.StringValue(`[1]`), types.StringNull(), types.StringValue(`[1]`)},
		{"unknown stays unknown", types.StringUnknown(), types.StringValue(`[1]`), types.StringUnknown()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := planmodifier.StringResponse{PlanValue: tc.plan}
			m.PlanModifyString(context.Background(), planmodifier.StringRequest{
				PlanValue: tc.plan, StateValue: tc.state, ConfigValue: tc.plan,
			}, &resp)
			if !resp.PlanValue.Equal(tc.want) {
				t.Errorf("plan = %v, want %v", resp.PlanValue, tc.want)
			}
		})
	}
}
