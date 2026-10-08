package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework-validators/int64validator"
	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringdefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/ByteVeda/flexiq/sdks/go/v2/admin"
)

const (
	onExcessReject = "reject"
	onExcessDrop   = "drop"

	// defaultNamespaceName names the resource when the provider sets no
	// namespace label; the token decides the real namespace either way.
	defaultNamespaceName = "current"
)

var (
	_ resource.Resource                = (*namespaceResource)(nil)
	_ resource.ResourceWithConfigure   = (*namespaceResource)(nil)
	_ resource.ResourceWithImportState = (*namespaceResource)(nil)
)

type namespaceResource struct {
	client adminAPI
	// name is the provider's namespace label, or defaultNamespaceName.
	name string
}

func newNamespaceResource() resource.Resource {
	return &namespaceResource{name: defaultNamespaceName}
}

type namespaceModel struct {
	ID              types.String `tfsdk:"id"`
	Name            types.String `tfsdk:"name"`
	MaxPending      types.Int64  `tfsdk:"max_pending"`
	OnExcess        types.String `tfsdk:"on_excess"`
	EnqueueRate     types.String `tfsdk:"enqueue_rate"`
	MaxRunning      types.Int64  `tfsdk:"max_running"`
	MaxArchivedRows types.Int64  `tfsdk:"max_archived_rows"`
	MaxDeadRows     types.Int64  `tfsdk:"max_dead_rows"`
}

func (r *namespaceResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_namespace"
}

func (r *namespaceResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	limit := func(description string) schema.Int64Attribute {
		return schema.Int64Attribute{
			Description: description + " Unset is unlimited; 0 is a real limit.",
			Optional:    true,
			Validators:  []validator.Int64{int64validator.AtLeast(0)},
		}
	}
	resp.Schema = schema.Schema{
		Description: "The quota of the namespace the provider's token was minted for. " +
			"The server takes the namespace from the token, so manage several namespaces " +
			"with one provider alias each, each with a token for its namespace. " +
			"Destroying the resource lifts every limit.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Description:   "Same as name.",
				Computed:      true,
				PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()},
			},
			nameAttribute: schema.StringAttribute{
				Description:   `The provider's namespace label, or "current" when it sets none.`,
				Computed:      true,
				PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()},
			},
			"max_pending": limit("Cap on pending jobs, delayed ones included."),
			"on_excess": schema.StringAttribute{
				Description: `What an enqueue over max_pending or enqueue_rate does: "reject" refuses it, ` +
					`"drop" accepts it and dead-letters its jobs unrun.`,
				Optional:   true,
				Computed:   true,
				Default:    stringdefault.StaticString(onExcessReject),
				Validators: []validator.String{stringvalidator.OneOf(onExcessReject, onExcessDrop)},
			},
			"enqueue_rate": schema.StringAttribute{
				Description: "Enqueues per interval, <count>/<s|m|h>, e.g. 100/s. Unset is unlimited.",
				Optional:    true,
				Validators:  []validator.String{rateValidator{}},
			},
			"max_running":       limit("Cap on jobs running at once, gated at dispatch."),
			"max_archived_rows": limit("Row ceiling on the archive; retention trims the oldest rows over it."),
			"max_dead_rows":     limit("Row ceiling on the dead-letter queue, trimmed the same way."),
		},
	}
}

func (r *namespaceResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	data := providerDataFrom(req.ProviderData, &resp.Diagnostics)
	if data == nil {
		return
	}
	r.client = data.client
	if data.namespace != "" {
		r.name = data.namespace
	}
}

func (r *namespaceResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	r.write(ctx, req.Plan, &resp.State, &resp.Diagnostics, "create")
}

func (r *namespaceResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	r.write(ctx, req.Plan, &resp.State, &resp.Diagnostics, "update")
}

func (r *namespaceResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state namespaceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	quota, err := r.client.GetNamespaceQuota(ctx)
	if err != nil {
		resp.Diagnostics.AddError("Cannot read flexiq_namespace", err.Error())
		return
	}
	current, err := namespaceFromRemote(quota, state.OnExcess)
	if err != nil {
		resp.Diagnostics.AddError("Cannot read flexiq_namespace", err.Error())
		return
	}
	current.Name = state.Name
	if current.Name.IsNull() || current.Name.IsUnknown() {
		current.Name = types.StringValue(r.name) // an import
	}
	current.ID = current.Name
	resp.Diagnostics.Append(resp.State.Set(ctx, current)...)
}

func (r *namespaceResource) Delete(ctx context.Context, _ resource.DeleteRequest, resp *resource.DeleteResponse) {
	if err := r.client.ClearNamespaceQuota(ctx); err != nil {
		resp.Diagnostics.AddError("Cannot delete flexiq_namespace", err.Error())
	}
}

func (r *namespaceResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

// write is Create and Update: both replace the whole quota with the plan.
func (r *namespaceResource) write(ctx context.Context, planned tfsdk.Plan, state *tfsdk.State, diags *diag.Diagnostics, verb string) {
	var plan namespaceModel
	diags.Append(planned.Get(ctx, &plan)...)
	if diags.HasError() {
		return
	}
	if _, err := r.client.SetNamespaceQuota(ctx, quotaFromModel(plan)); err != nil {
		diags.AddError("Cannot "+verb+" flexiq_namespace", err.Error())
		return
	}
	if plan.Name.IsNull() || plan.Name.IsUnknown() {
		plan.Name = types.StringValue(r.name)
	}
	plan.ID = plan.Name
	diags.Append(state.Set(ctx, plan)...)
}

// quotaFromModel maps the resource to the admin client's quota. on_excess is
// always sent explicitly: the server reads an unspecified one back as reject.
func quotaFromModel(m namespaceModel) admin.NamespaceQuota {
	quota := admin.NamespaceQuota{
		MaxPending:      int64Pointer(m.MaxPending),
		OnExcess:        admin.OverflowReject,
		MaxRunning:      int64Pointer(m.MaxRunning),
		MaxArchivedRows: int64Pointer(m.MaxArchivedRows),
		MaxDeadRows:     int64Pointer(m.MaxDeadRows),
	}
	if m.OnExcess.ValueString() == onExcessDrop {
		quota.OnExcess = admin.OverflowDrop
	}
	if !m.EnqueueRate.IsNull() && !m.EnqueueRate.IsUnknown() {
		quota.EnqueueRate = m.EnqueueRate.ValueString()
	}
	return quota
}

// namespaceFromRemote is quotaFromModel's inverse. With no limit set the
// server stores nothing, on_excess included, so the prior on_excess is kept:
// it governs no limit, and reading it back as reject would be a false diff.
func namespaceFromRemote(quota admin.NamespaceQuota, priorOnExcess types.String) (namespaceModel, error) {
	m := namespaceModel{
		MaxPending:      types.Int64PointerValue(quota.MaxPending),
		EnqueueRate:     types.StringNull(),
		MaxRunning:      types.Int64PointerValue(quota.MaxRunning),
		MaxArchivedRows: types.Int64PointerValue(quota.MaxArchivedRows),
		MaxDeadRows:     types.Int64PointerValue(quota.MaxDeadRows),
	}
	if quota.EnqueueRate != "" {
		m.EnqueueRate = types.StringValue(quota.EnqueueRate)
	}

	if quota.IsZero() && !priorOnExcess.IsNull() && !priorOnExcess.IsUnknown() {
		m.OnExcess = priorOnExcess
		return m, nil
	}
	switch quota.OnExcess {
	case admin.OverflowUnspecified, admin.OverflowReject:
		m.OnExcess = types.StringValue(onExcessReject)
	case admin.OverflowDrop:
		m.OnExcess = types.StringValue(onExcessDrop)
	default:
		return namespaceModel{}, fmt.Errorf("the server answered on_excess %s, which this provider does not know", quota.OnExcess)
	}
	return m, nil
}

func int64Pointer(v types.Int64) *int64 {
	if v.IsNull() || v.IsUnknown() {
		return nil
	}
	return v.ValueInt64Pointer()
}
