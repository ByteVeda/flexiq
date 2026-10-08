package provider

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-go/tftypes"

	"github.com/ByteVeda/flexiq/sdks/go/v2/admin"
)

func int64Ptr(v int64) *int64 { return &v }

// namespacePlan is a plan with every limit unset and on_excess at its default.
func namespacePlan() namespaceModel {
	return namespaceModel{
		ID:              types.StringUnknown(),
		Name:            types.StringUnknown(),
		MaxPending:      types.Int64Null(),
		OnExcess:        types.StringValue(onExcessReject),
		EnqueueRate:     types.StringNull(),
		MaxRunning:      types.Int64Null(),
		MaxArchivedRows: types.Int64Null(),
		MaxDeadRows:     types.Int64Null(),
	}
}

func TestQuotaFromModelKeepsNullAndZeroApart(t *testing.T) {
	unset := quotaFromModel(namespacePlan())
	if !unset.IsZero() {
		t.Errorf("null limits: want an unlimited quota, got %+v", unset)
	}
	if unset.OnExcess != admin.OverflowReject {
		t.Errorf("on_excess must be sent explicitly, got %v", unset.OnExcess)
	}

	plan := namespacePlan()
	plan.MaxPending = types.Int64Value(0)
	plan.MaxRunning = types.Int64Value(5)
	plan.MaxArchivedRows = types.Int64Value(1000)
	plan.MaxDeadRows = types.Int64Value(0)
	plan.EnqueueRate = types.StringValue("50/s")
	plan.OnExcess = types.StringValue(onExcessDrop)

	got := quotaFromModel(plan)
	if got.MaxPending == nil || *got.MaxPending != 0 || got.MaxDeadRows == nil || *got.MaxDeadRows != 0 {
		t.Errorf("zero limits must be real limits, got %+v", got)
	}
	if *got.MaxRunning != 5 || *got.MaxArchivedRows != 1000 || got.EnqueueRate != "50/s" || got.OnExcess != admin.OverflowDrop {
		t.Errorf("got %+v", got)
	}
}

func TestNamespaceFromRemoteKeepsNullAndZeroApart(t *testing.T) {
	unset, err := namespaceFromRemote(admin.NamespaceQuota{OnExcess: admin.OverflowReject}, types.StringNull())
	if err != nil {
		t.Fatalf("namespaceFromRemote: %v", err)
	}
	if !unset.MaxPending.IsNull() || !unset.EnqueueRate.IsNull() || !unset.MaxRunning.IsNull() ||
		!unset.MaxArchivedRows.IsNull() || !unset.MaxDeadRows.IsNull() {
		t.Errorf("no quota must read as nulls, got %+v", unset)
	}
	if unset.OnExcess.ValueString() != onExcessReject {
		t.Errorf("on_excess = %v", unset.OnExcess)
	}

	set, err := namespaceFromRemote(admin.NamespaceQuota{
		MaxPending: int64Ptr(0), OnExcess: admin.OverflowDrop, EnqueueRate: "1/m", MaxDeadRows: int64Ptr(7),
	}, types.StringValue(onExcessReject))
	if err != nil {
		t.Fatalf("namespaceFromRemote: %v", err)
	}
	if set.MaxPending.IsNull() || set.MaxPending.ValueInt64() != 0 || set.MaxDeadRows.ValueInt64() != 7 {
		t.Errorf("got %+v", set)
	}
	if set.OnExcess.ValueString() != onExcessDrop || set.EnqueueRate.ValueString() != "1/m" {
		t.Errorf("a remote drop must win over the prior state, got %+v", set)
	}
}

func TestNamespaceFromRemoteOnExcess(t *testing.T) {
	limited := admin.NamespaceQuota{MaxRunning: int64Ptr(1)}
	cases := []struct {
		name     string
		remote   admin.QuotaOverflow
		limited  bool
		prior    types.String
		want     string
		wantFail bool
	}{
		{name: "unspecified reads as reject", remote: admin.OverflowUnspecified, limited: true, prior: types.StringNull(), want: onExcessReject},
		{name: "drop", remote: admin.OverflowDrop, limited: true, prior: types.StringNull(), want: onExcessDrop},
		{name: "unlimited keeps the prior value", remote: admin.OverflowReject, prior: types.StringValue(onExcessDrop), want: onExcessDrop},
		{name: "unlimited import reads reject", remote: admin.OverflowReject, prior: types.StringNull(), want: onExcessReject},
		{name: "a value from a newer server", remote: admin.QuotaOverflow(9), limited: true, prior: types.StringNull(), wantFail: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			quota := admin.NamespaceQuota{OnExcess: tc.remote}
			if tc.limited {
				quota.MaxRunning = limited.MaxRunning
			}
			got, err := namespaceFromRemote(quota, tc.prior)
			if tc.wantFail {
				if err == nil {
					t.Fatal("want an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("namespaceFromRemote: %v", err)
			}
			if got.OnExcess.ValueString() != tc.want {
				t.Errorf("on_excess = %v, want %s", got.OnExcess, tc.want)
			}
		})
	}
}

// TestNamespaceResourceLifecycle drives the resource's own methods through
// create, a drifted read, update, import and delete.
func TestNamespaceResourceLifecycle(t *testing.T) {
	ctx := context.Background()
	fake := newFakeAdmin()
	r := &namespaceResource{client: fake, name: "billing"}
	s := resourceSchema(t, r)

	plan := namespacePlan()
	plan.MaxPending = types.Int64Value(100)
	plan.OnExcess = types.StringValue(onExcessDrop)
	create := resource.CreateResponse{State: emptyState(s)}
	r.Create(ctx, resource.CreateRequest{Plan: planOf(t, s, plan)}, &create)
	if create.Diagnostics.HasError() {
		t.Fatalf("Create: %v", create.Diagnostics)
	}
	created := stateModel[namespaceModel](t, create.State)
	if created.ID.ValueString() != "billing" || created.Name.ValueString() != "billing" {
		t.Errorf("id/name = %v/%v", created.ID, created.Name)
	}
	if fake.quota == nil || *fake.quota.MaxPending != 100 || fake.quota.OnExcess != admin.OverflowDrop {
		t.Fatalf("stored quota = %+v", fake.quota)
	}

	// Drift: the quota is cleared out of band.
	fake.quota = nil
	read := resource.ReadResponse{State: create.State}
	r.Read(ctx, resource.ReadRequest{State: create.State}, &read)
	if read.Diagnostics.HasError() {
		t.Fatalf("Read: %v", read.Diagnostics)
	}
	drifted := stateModel[namespaceModel](t, read.State)
	if !drifted.MaxPending.IsNull() || drifted.Name.ValueString() != "billing" {
		t.Errorf("drifted state = %+v", drifted)
	}

	update := resource.UpdateResponse{State: read.State}
	next := namespacePlan()
	next.ID, next.Name = drifted.ID, drifted.Name
	next.EnqueueRate = types.StringValue("10/s")
	r.Update(ctx, resource.UpdateRequest{Plan: planOf(t, s, next), State: read.State}, &update)
	if update.Diagnostics.HasError() {
		t.Fatalf("Update: %v", update.Diagnostics)
	}
	if fake.quota == nil || fake.quota.EnqueueRate != "10/s" || fake.quota.MaxPending != nil ||
		fake.quota.OnExcess != admin.OverflowReject {
		t.Errorf("stored quota after update = %+v", fake.quota)
	}

	imported := resource.ImportStateResponse{State: emptyState(s)}
	r.ImportState(ctx, resource.ImportStateRequest{ID: "billing"}, &imported)
	if imported.Diagnostics.HasError() {
		t.Fatalf("ImportState: %v", imported.Diagnostics)
	}
	afterImport := resource.ReadResponse{State: imported.State}
	r.Read(ctx, resource.ReadRequest{State: imported.State}, &afterImport)
	if afterImport.Diagnostics.HasError() {
		t.Fatalf("Read after import: %v", afterImport.Diagnostics)
	}
	importedModel := stateModel[namespaceModel](t, afterImport.State)
	if importedModel.Name.ValueString() != "billing" || importedModel.EnqueueRate.ValueString() != "10/s" ||
		importedModel.OnExcess.ValueString() != onExcessReject {
		t.Errorf("imported state = %+v", importedModel)
	}

	fake.calls = nil
	del := resource.DeleteResponse{State: update.State}
	r.Delete(ctx, resource.DeleteRequest{State: update.State}, &del)
	if del.Diagnostics.HasError() {
		t.Fatalf("Delete: %v", del.Diagnostics)
	}
	if want := []string{"ClearNamespaceQuota"}; !slices.Equal(fake.calls, want) || fake.quota != nil {
		t.Errorf("delete calls = %v, quota = %+v", fake.calls, fake.quota)
	}
}

func TestNamespaceUnlimitedWithDropHasNoDiff(t *testing.T) {
	ctx := context.Background()
	fake := newFakeAdmin()
	r := &namespaceResource{client: fake, name: defaultNamespaceName}
	s := resourceSchema(t, r)

	plan := namespacePlan()
	plan.OnExcess = types.StringValue(onExcessDrop)
	create := resource.CreateResponse{State: emptyState(s)}
	r.Create(ctx, resource.CreateRequest{Plan: planOf(t, s, plan)}, &create)
	if create.Diagnostics.HasError() {
		t.Fatalf("Create: %v", create.Diagnostics)
	}

	read := resource.ReadResponse{State: create.State}
	r.Read(ctx, resource.ReadRequest{State: create.State}, &read)
	if read.Diagnostics.HasError() {
		t.Fatalf("Read: %v", read.Diagnostics)
	}
	if !read.State.Raw.Equal(create.State.Raw) {
		t.Errorf("an unlimited quota with on_excess drop must read back unchanged:\nwant %v\ngot  %v",
			create.State.Raw, read.State.Raw)
	}
	if got := stateModel[namespaceModel](t, read.State); got.Name.ValueString() != defaultNamespaceName {
		t.Errorf("name = %v", got.Name)
	}
}

func TestNamespaceResourceSurfacesServerErrors(t *testing.T) {
	ctx := context.Background()
	fake := newFakeAdmin()
	r := &namespaceResource{client: fake, name: defaultNamespaceName}
	s := resourceSchema(t, r)

	for _, method := range []string{"SetNamespaceQuota", "GetNamespaceQuota", "ClearNamespaceQuota"} {
		fake.fail = map[string]error{method: errors.New("UNAVAILABLE")}
		var failed bool
		switch method {
		case "SetNamespaceQuota":
			resp := resource.CreateResponse{State: emptyState(s)}
			r.Create(ctx, resource.CreateRequest{Plan: planOf(t, s, namespacePlan())}, &resp)
			failed = resp.Diagnostics.HasError()
		case "GetNamespaceQuota":
			state := stateOf(t, s, namespaceModel{
				ID: types.StringValue("x"), Name: types.StringValue("x"), OnExcess: types.StringValue(onExcessReject),
			})
			resp := resource.ReadResponse{State: state}
			r.Read(ctx, resource.ReadRequest{State: state}, &resp)
			failed = resp.Diagnostics.HasError()
		default:
			resp := resource.DeleteResponse{}
			r.Delete(ctx, resource.DeleteRequest{}, &resp)
			failed = resp.Diagnostics.HasError()
		}
		if !failed {
			t.Errorf("%s failing did not fail the resource call", method)
		}
	}
}

func TestNamespaceConfigValidation(t *testing.T) {
	s := resourceSchema(t, &namespaceResource{})
	cases := []struct {
		name    string
		values  map[string]tftypes.Value
		wantErr bool
	}{
		{name: "empty", values: nil},
		{name: "every limit", values: map[string]tftypes.Value{
			"max_pending":       tftypes.NewValue(tftypes.Number, 0),
			"on_excess":         tftypes.NewValue(tftypes.String, "drop"),
			"enqueue_rate":      tftypes.NewValue(tftypes.String, "100/s"),
			"max_running":       tftypes.NewValue(tftypes.Number, 3),
			"max_archived_rows": tftypes.NewValue(tftypes.Number, 10),
			"max_dead_rows":     tftypes.NewValue(tftypes.Number, 10),
		}},
		{name: "unknown on_excess", wantErr: true, values: map[string]tftypes.Value{
			"on_excess": tftypes.NewValue(tftypes.String, "REJECT"),
		}},
		{name: "negative limit", wantErr: true, values: map[string]tftypes.Value{
			"max_dead_rows": tftypes.NewValue(tftypes.Number, -1),
		}},
		{name: "bad rate", wantErr: true, values: map[string]tftypes.Value{
			"enqueue_rate": tftypes.NewValue(tftypes.String, "100"),
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			diags := validateResourceConfig(t, "flexiq_namespace", s, tc.values)
			if hasProtoError(diags) != tc.wantErr {
				t.Errorf("diagnostics = %v, want error %v", protoSummaries(diags), tc.wantErr)
			}
		})
	}
}
