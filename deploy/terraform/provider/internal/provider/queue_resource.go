package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework-validators/int32validator"
	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/ByteVeda/flexiq/sdks/go/v2/admin"
)

var (
	_ resource.Resource                = (*queueResource)(nil)
	_ resource.ResourceWithConfigure   = (*queueResource)(nil)
	_ resource.ResourceWithImportState = (*queueResource)(nil)
)

type queueResource struct {
	client adminAPI
}

func newQueueResource() resource.Resource {
	return &queueResource{}
}

type queueModel struct {
	ID            types.String `tfsdk:"id"`
	Name          types.String `tfsdk:"name"`
	MaxConcurrent types.Int32  `tfsdk:"max_concurrent"`
	RateLimit     types.String `tfsdk:"rate_limit"`
	Paused        types.Bool   `tfsdk:"paused"`
}

func (r *queueResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_queue"
}

func (r *queueResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "An operator override on one queue in the token's namespace: concurrency, rate and pause. " +
			"Overrides reach workers when they next start; a running worker keeps the values it started with. " +
			"Destroying the resource lifts the override and resumes the queue if it was paused.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Description:   "The queue name.",
				Computed:      true,
				PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()},
			},
			nameAttribute: schema.StringAttribute{
				Description:   "The queue to override. Changing it replaces the resource.",
				Required:      true,
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()},
				Validators:    []validator.String{stringvalidator.LengthAtLeast(1)},
			},
			"max_concurrent": schema.Int32Attribute{
				Description: "Cap on the queue's jobs running at once. Unset is not overridden; 0 is a real cap.",
				Optional:    true,
				Validators:  []validator.Int32{int32validator.AtLeast(0)},
			},
			"rate_limit": schema.StringAttribute{
				Description: "Dispatch rate, <count>/<unit>, e.g. 100/m, with a count of at least one and a unit of s, sec, second, m, min, minute, h, hr or hour. Unset is not overridden.",
				Optional:    true,
				Validators:  []validator.String{rateValidator{}},
			},
			"paused": schema.BoolAttribute{
				Description: "Whether the queue dispatches nothing. Jobs already running finish.",
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(false),
			},
		},
	}
}

func (r *queueResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if data := providerDataFrom(req.ProviderData, &resp.Diagnostics); data != nil {
		r.client = data.client
	}
}

func (r *queueResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan queueModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}
	if err := applyQueue(ctx, r.client, plan); err != nil {
		resp.Diagnostics.AddError("Cannot create flexiq_queue", err.Error())
		return
	}
	plan.ID = plan.Name
	resp.Diagnostics.Append(resp.State.Set(ctx, plan)...)
}

func (r *queueResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state queueModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	// A queue is never "gone": with no override and no pause it reads back as
	// nulls and false, and the next plan puts the override back.
	current, err := readQueue(ctx, r.client, state.Name.ValueString())
	if err != nil {
		resp.Diagnostics.AddError("Cannot read flexiq_queue", err.Error())
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, current)...)
}

func (r *queueResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan queueModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}
	if err := applyQueue(ctx, r.client, plan); err != nil {
		resp.Diagnostics.AddError("Cannot update flexiq_queue", err.Error())
		return
	}
	plan.ID = plan.Name
	resp.Diagnostics.Append(resp.State.Set(ctx, plan)...)
}

func (r *queueResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state queueModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	if err := deleteQueue(ctx, r.client, state); err != nil {
		resp.Diagnostics.AddError("Cannot delete flexiq_queue", err.Error())
	}
}

func (r *queueResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root(nameAttribute), req, resp)
}

// overrideFromModel maps the resource's limits to the admin client's: a null
// attribute is not overridden, and zero stays a real cap.
func overrideFromModel(m queueModel) admin.QueueOverride {
	var override admin.QueueOverride
	if !m.MaxConcurrent.IsNull() && !m.MaxConcurrent.IsUnknown() {
		override.MaxConcurrent = m.MaxConcurrent.ValueInt32Pointer()
	}
	if !m.RateLimit.IsNull() && !m.RateLimit.IsUnknown() {
		override.RateLimit = m.RateLimit.ValueString()
	}
	return override
}

// queueFromRemote is overrideFromModel's inverse, plus the pause state.
func queueFromRemote(name string, override admin.QueueOverride, paused bool) queueModel {
	m := queueModel{
		ID:            types.StringValue(name),
		Name:          types.StringValue(name),
		MaxConcurrent: types.Int32PointerValue(override.MaxConcurrent),
		RateLimit:     types.StringNull(),
		Paused:        types.BoolValue(paused),
	}
	if override.RateLimit != "" {
		m.RateLimit = types.StringValue(override.RateLimit)
	}
	return m
}

// applyQueue makes the server match m. The override goes first so the pause
// call has the last word on the pause state.
func applyQueue(ctx context.Context, api adminAPI, m queueModel) error {
	name := m.Name.ValueString()
	override := overrideFromModel(m)
	if override.IsZero() {
		// Clear rather than an empty Set: an empty Set keeps a pause flag the
		// dashboard stored in the same document.
		if err := api.ClearQueueOverride(ctx, name); err != nil {
			return fmt.Errorf("clear the override on queue %q: %w", name, err)
		}
	} else if _, err := api.SetQueueOverride(ctx, name, override); err != nil {
		return fmt.Errorf("set the override on queue %q: %w", name, err)
	}

	if m.Paused.ValueBool() {
		if _, err := api.PauseQueue(ctx, name); err != nil {
			return fmt.Errorf("pause queue %q: %w", name, err)
		}
		return nil
	}
	if _, err := api.ResumeQueue(ctx, name); err != nil {
		return fmt.Errorf("resume queue %q: %w", name, err)
	}
	return nil
}

// readQueue reads the queue's override and pause state. Neither read answers
// not-found: a queue with neither is simply not overridden and not paused.
func readQueue(ctx context.Context, api adminAPI, name string) (queueModel, error) {
	override, _, err := api.GetQueueOverride(ctx, name)
	if err != nil {
		return queueModel{}, fmt.Errorf("read the override on queue %q: %w", name, err)
	}
	queue, _, err := api.GetQueue(ctx, name)
	if err != nil {
		return queueModel{}, fmt.Errorf("read queue %q: %w", name, err)
	}
	return queueFromRemote(name, override, queue.Paused), nil
}

// deleteQueue lifts the override and, if this resource paused the queue,
// resumes it — the queue goes back to what its workers declare.
func deleteQueue(ctx context.Context, api adminAPI, m queueModel) error {
	name := m.Name.ValueString()
	if err := api.ClearQueueOverride(ctx, name); err != nil {
		return fmt.Errorf("clear the override on queue %q: %w", name, err)
	}
	if m.Paused.ValueBool() {
		if _, err := api.ResumeQueue(ctx, name); err != nil {
			return fmt.Errorf("resume queue %q: %w", name, err)
		}
	}
	return nil
}
