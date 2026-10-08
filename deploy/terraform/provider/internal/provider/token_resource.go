package provider

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/terraform-plugin-framework-validators/int32validator"
	"github.com/hashicorp/terraform-plugin-framework-validators/listvalidator"
	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int32default"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int32planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/listplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	"github.com/ByteVeda/flexiq/sdks/go/v2/admin"
)

var (
	_ resource.Resource                = (*tokenResource)(nil)
	_ resource.ResourceWithConfigure   = (*tokenResource)(nil)
	_ resource.ResourceWithImportState = (*tokenResource)(nil)
	_ resource.ResourceWithModifyPlan  = (*tokenResource)(nil)
)

type tokenResource struct {
	client adminAPI
	// now is the clock rotation is decided against; a field so tests can
	// stand still.
	now func() time.Time
}

func newTokenResource() resource.Resource {
	return &tokenResource{now: time.Now}
}

type tokenModel struct {
	ID               types.String `tfsdk:"id"`
	Name             types.String `tfsdk:"name"`
	Scopes           types.List   `tfsdk:"scopes"`
	ExpireDays       types.Int32  `tfsdk:"expire_days"`
	RotateBeforeDays types.Int32  `tfsdk:"rotate_before_days"`
	Namespace        types.String `tfsdk:"namespace"`
	Secret           types.String `tfsdk:"secret"`
	ExpiresAt        types.String `tfsdk:"expires_at"`
	CreatedAt        types.String `tfsdk:"created_at"`
	Status           types.String `tfsdk:"status"`
}

func (r *tokenResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_token"
}

func (r *tokenResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	computed := func(description string) schema.StringAttribute {
		return schema.StringAttribute{
			Description:   description,
			Computed:      true,
			PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()},
		}
	}
	secret := computed("The credential to present. Known only from the apply that minted it: " +
		"the server keeps a digest, so an imported token's secret is null.")
	secret.Sensitive = true

	resp.Schema = schema.Schema{
		Description: "An API token minted in the provider token's namespace. Needs the tokens scope, " +
			"and grants that cover every scope minted. A minted token may not outlive the provider's own " +
			"token, so that token must outlive every token this resource mints — replacements included. " +
			"Destroying the resource revokes the token. Pair rotate_before_days with " +
			"lifecycle { create_before_destroy = true } so the new secret exists before the old is revoked.",
		Attributes: map[string]schema.Attribute{
			"id": computed("The token's public id, the import handle."),
			nameAttribute: schema.StringAttribute{
				Description:   "Label shown in listings, at most 64 characters. Changing it replaces the token.",
				Required:      true,
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()},
				Validators:    []validator.String{stringvalidator.LengthBetween(1, 64)},
			},
			"scopes": schema.ListAttribute{
				Description: "Grants, each `scope` or `scope:queue=<pattern>,task=<pattern>`. " +
					"Changing them replaces the token.",
				Required:      true,
				ElementType:   types.StringType,
				PlanModifiers: []planmodifier.List{listplanmodifier.RequiresReplace()},
				Validators: []validator.List{
					listvalidator.SizeAtLeast(1),
					listvalidator.ValueStringsAre(stringvalidator.LengthAtLeast(1)),
				},
			},
			"expire_days": schema.Int32Attribute{
				Description: "Days until the token expires, 1 to 365. Required: the server refuses a token " +
					"that outlives the provider's. Changing it replaces the token.",
				Required:      true,
				PlanModifiers: []planmodifier.Int32{int32planmodifier.RequiresReplace()},
				Validators:    []validator.Int32{int32validator.Between(1, 365)},
			},
			"rotate_before_days": schema.Int32Attribute{
				Description: "Plan a replacement this many days before expiry. 0 replaces only once the " +
					"token has expired or been revoked. The replacement's expiry must still fit inside the " +
					"provider token's.",
				Optional:   true,
				Computed:   true,
				Default:    int32default.StaticInt32(0),
				Validators: []validator.Int32{int32validator.AtLeast(0)},
			},
			"namespace":  computed("The namespace the token is scoped to."),
			"secret":     secret,
			"expires_at": computed("RFC 3339 expiry."),
			"created_at": computed("RFC 3339 mint time."),
			"status":     computed("ACTIVE, EXPIRED or REVOKED. Anything but ACTIVE plans a replacement."),
		},
	}
}

func (r *tokenResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if data := providerDataFrom(req.ProviderData, &resp.Diagnostics); data != nil {
		r.client = data.client
	}
}

// ModifyPlan replaces a token that is due: inside its rotation window, expired
// or revoked. Marking the secret unknown is what makes terraform act on the
// replacement — a RequiresReplace path whose value is unchanged is ignored.
func (r *tokenResource) ModifyPlan(ctx context.Context, req resource.ModifyPlanRequest, resp *resource.ModifyPlanResponse) {
	if req.State.Raw.IsNull() || req.Plan.Raw.IsNull() {
		return
	}
	var state, plan tokenModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() || plan.RotateBeforeDays.IsUnknown() {
		return
	}

	var expiresAt time.Time
	if raw := state.ExpiresAt.ValueString(); raw != "" {
		parsed, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			resp.Diagnostics.AddAttributeError(path.Root("expires_at"), "Unreadable expiry", err.Error())
			return
		}
		expiresAt = parsed
	}
	if !needsRotation(r.now(), expiresAt, int64(plan.RotateBeforeDays.ValueInt32()), state.Status.ValueString()) {
		return
	}

	plan.ID = types.StringUnknown()
	plan.Secret = types.StringUnknown()
	plan.Namespace = types.StringUnknown()
	plan.ExpiresAt = types.StringUnknown()
	plan.CreatedAt = types.StringUnknown()
	plan.Status = types.StringUnknown()
	resp.Diagnostics.Append(resp.Plan.Set(ctx, plan)...)
	resp.RequiresReplace = append(resp.RequiresReplace, path.Root("secret"))
}

func (r *tokenResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan tokenModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}
	var scopes []string
	resp.Diagnostics.Append(plan.Scopes.ElementsAs(ctx, &scopes, false)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Never retried: the server takes no idempotency key, so a retry after a
	// call that landed would mint a second token nobody holds the secret of.
	created, err := r.client.CreateToken(ctx, admin.CreateTokenRequest{
		Name:       plan.Name.ValueString(),
		Scopes:     scopes,
		ExpireDays: plan.ExpireDays.ValueInt32(),
	})
	if err != nil {
		resp.Diagnostics.AddError("Cannot create flexiq_token", fmt.Sprintf(
			"create token %q: %v\n\nNot retried. If the call reached the server before failing, a token named %q "+
				"may exist with no secret held; list the namespace's tokens and revoke it.",
			plan.Name.ValueString(), err, plan.Name.ValueString()))
		return
	}

	state := withRemoteToken(plan, created.Token)
	state.Secret = types.StringValue(created.Secret)
	resp.Diagnostics.Append(resp.State.Set(ctx, state)...)
}

func (r *tokenResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state tokenModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	id := state.ID.ValueString()
	token, err := r.client.GetToken(ctx, id)
	if errors.Is(err, flexiq.ReasonTokenNotFound) {
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("Cannot read flexiq_token", fmt.Sprintf("read token %q: %v", id, err))
		return
	}

	// A revoked or expired token stays in state with its status, so the next
	// plan replaces it rather than silently forgetting it.
	current, diags := tokenFromRead(ctx, state, token)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, current)...)
}

// Update only ever changes rotate_before_days, which lives in state alone.
func (r *tokenResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state tokenModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	state.RotateBeforeDays = plan.RotateBeforeDays
	resp.Diagnostics.Append(resp.State.Set(ctx, state)...)
}

func (r *tokenResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state tokenModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	id := state.ID.ValueString()
	if _, err := r.client.RevokeToken(ctx, id); err != nil && !errors.Is(err, flexiq.ReasonTokenNotFound) {
		resp.Diagnostics.AddError("Cannot revoke flexiq_token", fmt.Sprintf("revoke token %q: %v", id, err))
	}
}

// ImportState takes a token id. The secret is unrecoverable, so it imports null.
func (r *tokenResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

// withRemoteToken fills m's server-owned attributes from token.
func withRemoteToken(m tokenModel, token admin.Token) tokenModel {
	m.ID = types.StringValue(token.ID)
	m.Namespace = types.StringValue(token.Namespace)
	m.ExpiresAt = types.StringValue(token.ExpiresAt.UTC().Format(time.RFC3339))
	m.CreatedAt = types.StringValue(token.CreatedAt.UTC().Format(time.RFC3339))
	m.Status = types.StringValue(token.Status.String())
	return m
}

// tokenFromRead refreshes prior from the server. A token's name, grants and
// lifetime never change after mint, so prior's spelling of them stands; an
// import has none and takes the server's.
func tokenFromRead(ctx context.Context, prior tokenModel, token admin.Token) (tokenModel, diag.Diagnostics) {
	m := withRemoteToken(prior, token)
	var diags diag.Diagnostics
	if prior.Name.IsNull() {
		m.Name = types.StringValue(token.Name)
	}
	if prior.Scopes.IsNull() {
		scopes, d := types.ListValueFrom(ctx, types.StringType, token.Scopes)
		diags.Append(d...)
		m.Scopes = scopes
	}
	if prior.ExpireDays.IsNull() {
		m.ExpireDays = types.Int32Value(int32(lifetimeDays(token.CreatedAt, token.ExpiresAt))) //nolint:gosec // 1..365 by the server's own bound
	}
	if prior.RotateBeforeDays.IsNull() {
		m.RotateBeforeDays = types.Int32Value(0)
	}
	return m, diags
}
