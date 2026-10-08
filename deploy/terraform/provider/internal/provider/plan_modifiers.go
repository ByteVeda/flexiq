package provider

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
)

// keepEquivalentState plans the prior state's value when the new one means
// the same thing, so a rewrite that changes nothing never shows as a diff.
// The attribute must be Computed: only then may the plan differ from config.
type keepEquivalentState struct {
	equal       func(a, b string) bool
	description string
}

var _ planmodifier.String = keepEquivalentState{}

func (m keepEquivalentState) Description(context.Context) string {
	return m.description
}

func (m keepEquivalentState) MarkdownDescription(ctx context.Context) string {
	return m.Description(ctx)
}

func (m keepEquivalentState) PlanModifyString(_ context.Context, req planmodifier.StringRequest, resp *planmodifier.StringResponse) {
	if req.StateValue.IsNull() || req.StateValue.IsUnknown() ||
		req.PlanValue.IsNull() || req.PlanValue.IsUnknown() {
		return
	}
	if m.equal(req.PlanValue.ValueString(), req.StateValue.ValueString()) {
		resp.PlanValue = req.StateValue
	}
}

// keepEquivalent picks the prior value when the remote one means the same
// thing, the Read-side half of keepEquivalentState.
func keepEquivalent(prior, remote string, priorKnown bool, equal func(a, b string) bool) string {
	if priorKnown && equal(prior, remote) {
		return prior
	}
	return remote
}
