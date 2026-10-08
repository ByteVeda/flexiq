package provider

import (
	"context"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
	"github.com/hashicorp/terraform-plugin-go/tftypes"
)

// Helpers that drive a resource's CRUD methods directly, the way the
// framework would, without a terraform binary.

func resourceSchema(t *testing.T, r resource.Resource) schema.Schema {
	t.Helper()
	ctx := context.Background()
	var resp resource.SchemaResponse
	r.Schema(ctx, resource.SchemaRequest{}, &resp)
	if resp.Diagnostics.HasError() {
		t.Fatalf("schema: %v", resp.Diagnostics)
	}
	if diags := resp.Schema.ValidateImplementation(ctx); diags.HasError() {
		t.Fatalf("ValidateImplementation: %v", diags)
	}
	return resp.Schema
}

func nullObject(s schema.Schema) tftypes.Value {
	return tftypes.NewValue(s.Type().TerraformType(context.Background()), nil)
}

func planOf(t *testing.T, s schema.Schema, model any) tfsdk.Plan {
	t.Helper()
	plan := tfsdk.Plan{Schema: s, Raw: nullObject(s)}
	if diags := plan.Set(context.Background(), model); diags.HasError() {
		t.Fatalf("plan: %v", diags)
	}
	return plan
}

func stateOf(t *testing.T, s schema.Schema, model any) tfsdk.State {
	t.Helper()
	state := emptyState(s)
	if diags := state.Set(context.Background(), model); diags.HasError() {
		t.Fatalf("state: %v", diags)
	}
	return state
}

func emptyState(s schema.Schema) tfsdk.State {
	return tfsdk.State{Schema: s, Raw: nullObject(s)}
}

// validateResourceConfig runs ValidateResourceConfig through the provider's
// protocol server — the call terraform makes at plan time — over a config
// whose unset attributes are null.
func validateResourceConfig(t *testing.T, typeName string, s schema.Schema, values map[string]tftypes.Value) []*tfprotov6.Diagnostic {
	t.Helper()
	ctx := context.Background()
	objectType, ok := s.Type().TerraformType(ctx).(tftypes.Object)
	if !ok {
		t.Fatal("resource schema is not an object")
	}
	attrs := make(map[string]tftypes.Value, len(objectType.AttributeTypes))
	for name, typ := range objectType.AttributeTypes {
		attrs[name] = tftypes.NewValue(typ, nil)
	}
	for name, value := range values {
		attrs[name] = value
	}
	config, err := tfprotov6.NewDynamicValue(objectType, tftypes.NewValue(objectType, attrs))
	if err != nil {
		t.Fatalf("config: %v", err)
	}

	server, err := providerserver.NewProtocol6WithError(New("test")())()
	if err != nil {
		t.Fatalf("provider server: %v", err)
	}
	resp, err := server.ValidateResourceConfig(ctx, &tfprotov6.ValidateResourceConfigRequest{
		TypeName: typeName,
		Config:   &config,
	})
	if err != nil {
		t.Fatalf("ValidateResourceConfig: %v", err)
	}
	return resp.Diagnostics
}

func hasProtoError(diags []*tfprotov6.Diagnostic) bool {
	for _, d := range diags {
		if d.Severity == tfprotov6.DiagnosticSeverityError {
			return true
		}
	}
	return false
}

func protoSummaries(diags []*tfprotov6.Diagnostic) []string {
	out := make([]string, 0, len(diags))
	for _, d := range diags {
		out = append(out, d.Summary+": "+d.Detail)
	}
	return out
}

// stateModel decodes a state into a model of type T.
func stateModel[T any](t *testing.T, state tfsdk.State) T {
	t.Helper()
	var model T
	if diags := state.Get(context.Background(), &model); diags.HasError() {
		t.Fatalf("decode state: %v", diags)
	}
	return model
}
