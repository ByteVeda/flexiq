package provider

import (
	"context"
	"errors"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringdefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	"github.com/ByteVeda/flexiq/sdks/go/v2/admin"
)

var (
	_ resource.Resource                = (*periodicResource)(nil)
	_ resource.ResourceWithConfigure   = (*periodicResource)(nil)
	_ resource.ResourceWithImportState = (*periodicResource)(nil)
)

// defaultQueue is the name the server stores for a task put with no queue.
const defaultQueue = "default"

type periodicResource struct {
	client adminAPI
}

func newPeriodicResource() resource.Resource {
	return &periodicResource{}
}

type periodicModel struct {
	ID       types.String `tfsdk:"id"`
	Name     types.String `tfsdk:"name"`
	TaskName types.String `tfsdk:"task_name"`
	Cron     types.String `tfsdk:"cron"`
	Queue    types.String `tfsdk:"queue"`
	Timezone types.String `tfsdk:"timezone"`
	Args     types.String `tfsdk:"args"`
	Kwargs   types.String `tfsdk:"kwargs"`
	Enabled  types.Bool   `tfsdk:"enabled"`
}

// queueEqual treats "" and "default" alike: the server stores one as the other.
func queueEqual(a, b string) bool {
	norm := func(q string) string {
		if q == "" {
			return defaultQueue
		}
		return q
	}
	return norm(a) == norm(b)
}

func (r *periodicResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_periodic_task"
}

func (r *periodicResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	sameJSON := keepEquivalentState{equal: jsonEqual, description: "Whitespace, key order and 1 vs 1.0 never show as a diff."}
	resp.Schema = schema.Schema{
		Description: "A periodic task in the token's namespace: a cron schedule that enqueues one task. " +
			"A task also declared in a worker's code is declared again when that worker starts, " +
			"so manage each schedule in one place.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Description:   "The periodic task's name.",
				Computed:      true,
				PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()},
			},
			nameAttribute: schema.StringAttribute{
				Description:   "Unique within the namespace. Changing it replaces the resource.",
				Required:      true,
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()},
				Validators:    []validator.String{stringvalidator.LengthAtLeast(1)},
			},
			"task_name": schema.StringAttribute{
				Description: "The task each firing enqueues. Not checked: the server holds no task registry.",
				Required:    true,
				Validators:  []validator.String{stringvalidator.LengthAtLeast(1)},
			},
			"cron": schema.StringAttribute{
				Description: "Six fields, seconds first: \"0 */5 * * * *\" is every five minutes. " +
					"A five-field crontab line is refused by the server.",
				Required:   true,
				Validators: []validator.String{stringvalidator.LengthAtLeast(1)},
			},
			"queue": schema.StringAttribute{
				Description: "The queue each firing enqueues onto. Defaults to \"default\"; \"\" means the same.",
				Optional:    true,
				Computed:    true,
				Default:     stringdefault.StaticString(defaultQueue),
				PlanModifiers: []planmodifier.String{keepEquivalentState{
					equal: queueEqual, description: "\"\" and \"default\" are the same queue.",
				}},
			},
			"timezone": schema.StringAttribute{
				Description: "IANA name to read cron in, e.g. Europe/Berlin. Unset is UTC.",
				Optional:    true,
				Validators:  []validator.String{stringvalidator.LengthAtLeast(1)},
			},
			"args": schema.StringAttribute{
				Description: "Positional arguments as a JSON array, e.g. jsonencode([1, \"a\"]). Defaults to []. " +
					"Integral numbers within ±(2^53-1) are sent as integers, others as 64-bit floats.",
				Optional:      true,
				Computed:      true,
				Default:       stringdefault.StaticString("[]"),
				Validators:    []validator.String{jsonValidator{kind: jsonArray}},
				PlanModifiers: []planmodifier.String{sameJSON},
			},
			"kwargs": schema.StringAttribute{
				Description: "Keyword arguments as a JSON object, e.g. jsonencode({ to = \"ops\" }). Defaults to {}. " +
					"Numbers are sent as in args.",
				Optional:      true,
				Computed:      true,
				Default:       stringdefault.StaticString("{}"),
				Validators:    []validator.String{jsonValidator{kind: jsonObject}},
				PlanModifiers: []planmodifier.String{sameJSON},
			},
			"enabled": schema.BoolAttribute{
				Description: "Whether the task fires. False pauses it, keeping its definition.",
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(true),
			},
		},
	}
}

func (r *periodicResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if data := providerDataFrom(req.ProviderData, &resp.Diagnostics); data != nil {
		r.client = data.client
	}
}

func (r *periodicResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan periodicModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}
	if err := applyPeriodic(ctx, r.client, plan); err != nil {
		resp.Diagnostics.AddError("Cannot create flexiq_periodic_task", err.Error())
		return
	}
	plan.ID = plan.Name
	resp.Diagnostics.Append(resp.State.Set(ctx, plan)...)
}

func (r *periodicResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state periodicModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	current, found, err := readPeriodic(ctx, r.client, state)
	if err != nil {
		resp.Diagnostics.AddError("Cannot read flexiq_periodic_task", err.Error())
		return
	}
	if !found {
		resp.State.RemoveResource(ctx)
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, current)...)
}

func (r *periodicResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan periodicModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}
	if err := applyPeriodic(ctx, r.client, plan); err != nil {
		resp.Diagnostics.AddError("Cannot update flexiq_periodic_task", err.Error())
		return
	}
	plan.ID = plan.Name
	resp.Diagnostics.Append(resp.State.Set(ctx, plan)...)
}

func (r *periodicResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state periodicModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	name := state.Name.ValueString()
	err := r.client.DeletePeriodicTask(ctx, name)
	if err != nil && !errors.Is(err, flexiq.ReasonPeriodicTaskNotFound) {
		resp.Diagnostics.AddError("Cannot delete flexiq_periodic_task", fmt.Sprintf("delete periodic task %q: %v", name, err))
	}
}

func (r *periodicResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root(nameAttribute), req, resp)
}

// periodicSpec maps the plan to the admin client's spec, numbers normalised.
func periodicSpec(m periodicModel) (admin.PeriodicTaskSpec, error) {
	args, err := parseJSONKind(m.Args.ValueString(), jsonArray)
	if err != nil {
		return admin.PeriodicTaskSpec{}, fmt.Errorf("args: %w", err)
	}
	kwargs, err := parseJSONKind(m.Kwargs.ValueString(), jsonObject)
	if err != nil {
		return admin.PeriodicTaskSpec{}, fmt.Errorf("kwargs: %w", err)
	}
	argList, _ := args.([]any)
	kwargMap, _ := kwargs.(map[string]any)
	return admin.PeriodicTaskSpec{
		Name:     m.Name.ValueString(),
		Task:     m.TaskName.ValueString(),
		Cron:     m.Cron.ValueString(),
		Queue:    m.Queue.ValueString(),
		Timezone: m.Timezone.ValueString(),
		Args:     argList,
		Kwargs:   kwargMap,
		// Only a create honours it; a replace keeps the pause state, which
		// the reconcile below then sets.
		StartPaused: !m.Enabled.ValueBool(),
	}, nil
}

// applyPeriodic puts the definition, then pauses or resumes it: a put that
// replaces an existing task never changes whether it is paused.
func applyPeriodic(ctx context.Context, api adminAPI, m periodicModel) error {
	spec, err := periodicSpec(m)
	if err != nil {
		return err
	}
	task, err := api.PutPeriodicTask(ctx, spec)
	if err != nil {
		return fmt.Errorf("put periodic task %q: %w", spec.Name, err)
	}

	want := m.Enabled.ValueBool()
	switch {
	case task.Enabled == want:
		return nil
	case want:
		if _, err := api.ResumePeriodicTask(ctx, spec.Name); err != nil {
			return fmt.Errorf("resume periodic task %q: %w", spec.Name, err)
		}
	default:
		if _, err := api.PausePeriodicTask(ctx, spec.Name); err != nil {
			return fmt.Errorf("pause periodic task %q: %w", spec.Name, err)
		}
	}
	return nil
}

// readPeriodic reads the task and its payload back into a model. Values that
// mean what prior already holds keep prior's spelling. found is false when the
// task is gone.
func readPeriodic(ctx context.Context, api adminAPI, prior periodicModel) (periodicModel, bool, error) {
	name := prior.Name.ValueString()
	task, err := api.GetPeriodicTask(ctx, name, admin.GetPeriodicTaskOptions{IncludePayload: true})
	if errors.Is(err, flexiq.ReasonPeriodicTaskNotFound) {
		return periodicModel{}, false, nil
	}
	if err != nil {
		return periodicModel{}, false, fmt.Errorf("read periodic task %q: %w", name, err)
	}

	call, err := task.DecodePayload()
	if err != nil {
		return periodicModel{}, false, fmt.Errorf("periodic task %q: %w", name, err)
	}
	args, err := jsonFromDecoded(nonNilArgs(call.Args))
	if err != nil {
		return periodicModel{}, false, fmt.Errorf("periodic task %q args: %w", name, err)
	}
	kwargs, err := jsonFromDecoded(nonNilKwargs(call.Kwargs))
	if err != nil {
		return periodicModel{}, false, fmt.Errorf("periodic task %q kwargs: %w", name, err)
	}

	m := periodicModel{
		ID:       types.StringValue(task.Name),
		Name:     types.StringValue(task.Name),
		TaskName: types.StringValue(task.TaskName),
		Cron:     types.StringValue(task.Cron),
		Queue:    types.StringValue(keepEquivalent(prior.Queue.ValueString(), task.Queue, known(prior.Queue), queueEqual)),
		Timezone: types.StringNull(),
		Args:     types.StringValue(keepEquivalent(prior.Args.ValueString(), args, known(prior.Args), jsonEqual)),
		Kwargs:   types.StringValue(keepEquivalent(prior.Kwargs.ValueString(), kwargs, known(prior.Kwargs), jsonEqual)),
		Enabled:  types.BoolValue(task.Enabled),
	}
	if task.Timezone != "" {
		m.Timezone = types.StringValue(task.Timezone)
	}
	return m, true, nil
}

func known(v types.String) bool {
	return !v.IsNull() && !v.IsUnknown()
}

// nonNilArgs and nonNilKwargs keep an empty call reading as [] and {}, not null.
func nonNilArgs(args []any) []any {
	if args == nil {
		return []any{}
	}
	return args
}

func nonNilKwargs(kwargs map[string]any) map[string]any {
	if kwargs == nil {
		return map[string]any{}
	}
	return kwargs
}
