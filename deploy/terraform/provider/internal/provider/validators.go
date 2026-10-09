package provider

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
)

// decimalCount is the finite subset of Rust's f64 grammar. Go's ParseFloat
// alone would also take hex floats and `_` separators, which the server refuses.
var decimalCount = regexp.MustCompile(`^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$`)

// rateUnits is every unit RateLimitConfig::parse takes, case-sensitive.
var rateUnits = []string{"s", "sec", "second", "m", "min", "minute", "h", "hr", "hour"}

// checkRate mirrors the server's RateLimitConfig::parse so a bad rate fails at
// plan time rather than mid-apply: exactly one `/`, both parts trimmed, a
// finite count of at least one (a bucket under one never releases a job), and
// a unit of s|sec|second, m|min|minute or h|hr|hour.
func checkRate(rate string) error {
	parts := strings.Split(rate, "/")
	if len(parts) != 2 {
		return fmt.Errorf("%q is not <count>/<unit>", rate)
	}
	count, unit := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	if !decimalCount.MatchString(count) {
		return fmt.Errorf("%q: count %q is not a number", rate, count)
	}
	n, err := strconv.ParseFloat(count, 64)
	if err != nil || math.IsInf(n, 0) {
		return fmt.Errorf("%q: count %q is not a finite number", rate, count)
	}
	if !slices.Contains(rateUnits, unit) {
		return fmt.Errorf("%q: unit %q is not one of %s", rate, unit, strings.Join(rateUnits, ", "))
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
	return "must be <count>/<unit>, e.g. 100/m, with a count of at least one and a unit of s, sec, second, m, min, minute, h, hr or hour"
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
