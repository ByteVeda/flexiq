package provider

import (
	"context"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

func TestCheckRate(t *testing.T) {
	for _, rate := range []string{"1/s", "100/m", "3600/h", "1.5/s", "1e3/m"} {
		if err := checkRate(rate); err != nil {
			t.Errorf("checkRate(%q) = %v, want nil", rate, err)
		}
	}
	// The server's own refusals, plus shapes its parser would never accept.
	for _, rate := range []string{
		"", "100", "0/s", "0.5/m", "-1/s", "10/d", "x/s", "/s", "10/", "10/s/s",
		"NaN/s", "Inf/s", "10/sec", " 10/s", "10/S",
	} {
		if err := checkRate(rate); err == nil {
			t.Errorf("checkRate(%q) = nil, want an error", rate)
		}
	}
}

func TestRateValidator(t *testing.T) {
	cases := []struct {
		name    string
		value   types.String
		wantErr bool
	}{
		{name: "valid", value: types.StringValue("10/s")},
		{name: "null is not overridden", value: types.StringNull()},
		{name: "unknown waits for apply", value: types.StringUnknown()},
		{name: "invalid", value: types.StringValue("10 per second"), wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var resp validator.StringResponse
			rateValidator{}.ValidateString(context.Background(), validator.StringRequest{
				Path:        path.Root("rate_limit"),
				ConfigValue: tc.value,
			}, &resp)
			if resp.Diagnostics.HasError() != tc.wantErr {
				t.Errorf("errors = %v, want error %v", resp.Diagnostics, tc.wantErr)
			}
		})
	}
}
