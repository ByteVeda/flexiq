package provider

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
)

// checkRate mirrors the server's rate parser so a bad rate fails at plan time
// rather than mid-apply: `<count>/<unit>`, unit s, m or h, count a finite
// number of at least one (a bucket under one never releases a job).
func checkRate(rate string) error {
	count, unit, found := strings.Cut(rate, "/")
	if !found {
		return fmt.Errorf("%q is not <count>/<unit>", rate)
	}
	switch unit {
	case "s", "m", "h":
	default:
		return fmt.Errorf("%q: unit %q is not one of s, m or h", rate, unit)
	}
	n, err := strconv.ParseFloat(count, 64)
	if err != nil || math.IsNaN(n) || math.IsInf(n, 0) {
		return fmt.Errorf("%q: count %q is not a number", rate, count)
	}
	if n < 1 {
		return fmt.Errorf("%q: count must be at least one", rate)
	}
	return nil
}

// rateValidator applies checkRate to a string attribute.
type rateValidator struct{}

var _ validator.String = rateValidator{}

func (rateValidator) Description(context.Context) string {
	return "must be <count>/<s|m|h> with a count of at least one, e.g. 100/m"
}

func (v rateValidator) MarkdownDescription(ctx context.Context) string {
	return v.Description(ctx)
}

func (rateValidator) ValidateString(_ context.Context, req validator.StringRequest, resp *validator.StringResponse) {
	if req.ConfigValue.IsNull() || req.ConfigValue.IsUnknown() {
		return
	}
	if err := checkRate(req.ConfigValue.ValueString()); err != nil {
		resp.Diagnostics.AddAttributeError(req.Path, "Invalid rate", err.Error())
	}
}
