package provider

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-go/tftypes"

	"github.com/ByteVeda/flexiq/sdks/go/v2/admin"
)

func int32Ptr(v int32) *int32 { return &v }

func queuePlan(name string, maxConcurrent types.Int32, rate types.String, paused bool) queueModel {
	return queueModel{
		ID:            types.StringUnknown(),
		Name:          types.StringValue(name),
		MaxConcurrent: maxConcurrent,
		RateLimit:     rate,
		Paused:        types.BoolValue(paused),
	}
}

func TestOverrideFromModelKeepsNullAndZeroApart(t *testing.T) {
	unset := overrideFromModel(queuePlan("q", types.Int32Null(), types.StringNull(), false))
	if !unset.IsZero() {
		t.Errorf("null attributes: want no override, got %+v", unset)
	}

	zero := overrideFromModel(queuePlan("q", types.Int32Value(0), types.StringValue("5/s"), false))
	if zero.MaxConcurrent == nil || *zero.MaxConcurrent != 0 {
		t.Errorf("max_concurrent = 0 must be a real cap, got %v", zero.MaxConcurrent)
	}
	if zero.RateLimit != "5/s" {
		t.Errorf("rate_limit = %q", zero.RateLimit)
	}
}

func TestQueueFromRemoteKeepsNullAndZeroApart(t *testing.T) {
	unset := queueFromRemote("q", admin.QueueOverride{}, false)
	if !unset.MaxConcurrent.IsNull() || !unset.RateLimit.IsNull() || unset.Paused.ValueBool() {
		t.Errorf("no override: want nulls and not paused, got %+v", unset)
	}

	zero := queueFromRemote("q", admin.QueueOverride{MaxConcurrent: int32Ptr(0), RateLimit: "1/h"}, true)
	if zero.MaxConcurrent.IsNull() || zero.MaxConcurrent.ValueInt32() != 0 {
		t.Errorf("a stored 0 must read back as 0, got %v", zero.MaxConcurrent)
	}
	if zero.RateLimit.ValueString() != "1/h" || !zero.Paused.ValueBool() {
		t.Errorf("got %+v", zero)
	}
	if zero.ID.ValueString() != "q" || zero.Name.ValueString() != "q" {
		t.Errorf("id/name = %v/%v", zero.ID, zero.Name)
	}
}

func TestApplyQueueSetsThenPauses(t *testing.T) {
	fake := newFakeAdmin()
	plan := queuePlan("emails", types.Int32Value(4), types.StringValue("100/m"), true)
	if err := applyQueue(context.Background(), fake, plan); err != nil {
		t.Fatalf("applyQueue: %v", err)
	}
	if want := []string{"SetQueueOverride", "PauseQueue"}; !slices.Equal(fake.calls, want) {
		t.Errorf("calls = %v, want %v", fake.calls, want)
	}
	if got := fake.overrides["emails"]; *got.MaxConcurrent != 4 || got.RateLimit != "100/m" {
		t.Errorf("stored override = %+v", got)
	}
	if !fake.paused["emails"] {
		t.Error("queue not paused")
	}
}

func TestApplyQueueClearsAnEmptyOverride(t *testing.T) {
	fake := newFakeAdmin()
	fake.overrides["emails"] = admin.QueueOverride{RateLimit: "1/s"}
	fake.paused["emails"] = true

	plan := queuePlan("emails", types.Int32Null(), types.StringNull(), false)
	if err := applyQueue(context.Background(), fake, plan); err != nil {
		t.Fatalf("applyQueue: %v", err)
	}
	if want := []string{"ClearQueueOverride", "ResumeQueue"}; !slices.Equal(fake.calls, want) {
		t.Errorf("calls = %v, want %v", fake.calls, want)
	}
	if _, ok := fake.overrides["emails"]; ok || fake.paused["emails"] {
		t.Errorf("override %v / paused %v left behind", fake.overrides, fake.paused)
	}
}

func TestApplyQueueReportsTheFailingCall(t *testing.T) {
	for _, method := range []string{"SetQueueOverride", "PauseQueue"} {
		fake := newFakeAdmin()
		fake.fail[method] = errors.New("boom")
		err := applyQueue(context.Background(), fake, queuePlan("q", types.Int32Value(1), types.StringNull(), true))
		if err == nil || !errors.Is(err, fake.fail[method]) {
			t.Errorf("%s failing: err = %v", method, err)
		}
	}
}

func TestReadQueueSeesOutOfBandChanges(t *testing.T) {
	ctx := context.Background()
	fake := newFakeAdmin()
	if err := applyQueue(ctx, fake, queuePlan("q", types.Int32Value(2), types.StringValue("10/s"), false)); err != nil {
		t.Fatalf("applyQueue: %v", err)
	}

	// Someone clears the override and pauses the queue from the dashboard.
	delete(fake.overrides, "q")
	fake.paused["q"] = true

	got, err := readQueue(ctx, fake, "q")
	if err != nil {
		t.Fatalf("readQueue: %v", err)
	}
	if !got.MaxConcurrent.IsNull() || !got.RateLimit.IsNull() {
		t.Errorf("removed override must read as nulls, got %+v", got)
	}
	if !got.Paused.ValueBool() {
		t.Error("pause toggled out of band was not read")
	}
}

func TestReadQueueOfAnUnknownQueue(t *testing.T) {
	got, err := readQueue(context.Background(), newFakeAdmin(), "never-seen")
	if err != nil {
		t.Fatalf("readQueue: %v", err)
	}
	if got.Paused.ValueBool() || !got.MaxConcurrent.IsNull() || got.ID.ValueString() != "never-seen" {
		t.Errorf("got %+v", got)
	}
}

func TestDeleteQueueResumesOnlyWhatItPaused(t *testing.T) {
	cases := []struct {
		paused bool
		want   []string
	}{
		{paused: true, want: []string{"ClearQueueOverride", "ResumeQueue"}},
		{paused: false, want: []string{"ClearQueueOverride"}},
	}
	for _, tc := range cases {
		fake := newFakeAdmin()
		if err := deleteQueue(context.Background(), fake, queuePlan("q", types.Int32Null(), types.StringNull(), tc.paused)); err != nil {
			t.Fatalf("deleteQueue: %v", err)
		}
		if !slices.Equal(fake.calls, tc.want) {
			t.Errorf("paused=%v: calls = %v, want %v", tc.paused, fake.calls, tc.want)
		}
	}
}

// TestQueueResourceLifecycle drives the resource's own methods through
// create, a drifted read, update, import and delete.
func TestQueueResourceLifecycle(t *testing.T) {
	ctx := context.Background()
	fake := newFakeAdmin()
	r := &queueResource{client: fake}
	s := resourceSchema(t, r)

	create := resource.CreateResponse{State: emptyState(s)}
	r.Create(ctx, resource.CreateRequest{
		Plan: planOf(t, s, queuePlan("emails", types.Int32Value(0), types.StringValue("100/m"), false)),
	}, &create)
	if create.Diagnostics.HasError() {
		t.Fatalf("Create: %v", create.Diagnostics)
	}
	created := stateModel[queueModel](t, create.State)
	if created.ID.ValueString() != "emails" || created.MaxConcurrent.ValueInt32() != 0 {
		t.Fatalf("created state = %+v", created)
	}

	// Drift: the override is removed and the queue paused out of band.
	delete(fake.overrides, "emails")
	fake.paused["emails"] = true
	read := resource.ReadResponse{State: create.State}
	r.Read(ctx, resource.ReadRequest{State: create.State}, &read)
	if read.Diagnostics.HasError() {
		t.Fatalf("Read: %v", read.Diagnostics)
	}
	if read.State.Raw.IsNull() {
		t.Fatal("Read removed the resource; a queue is never gone")
	}
	drifted := stateModel[queueModel](t, read.State)
	if !drifted.MaxConcurrent.IsNull() || !drifted.RateLimit.IsNull() || !drifted.Paused.ValueBool() {
		t.Errorf("drifted state = %+v", drifted)
	}

	update := resource.UpdateResponse{State: read.State}
	r.Update(ctx, resource.UpdateRequest{
		Plan:  planOf(t, s, queuePlan("emails", types.Int32Value(8), types.StringNull(), false)),
		State: read.State,
	}, &update)
	if update.Diagnostics.HasError() {
		t.Fatalf("Update: %v", update.Diagnostics)
	}
	if got := fake.overrides["emails"]; got.MaxConcurrent == nil || *got.MaxConcurrent != 8 || got.RateLimit != "" {
		t.Errorf("override after update = %+v", got)
	}
	if fake.paused["emails"] {
		t.Error("update to paused=false did not resume")
	}

	imported := resource.ImportStateResponse{State: emptyState(s)}
	r.ImportState(ctx, resource.ImportStateRequest{ID: "emails"}, &imported)
	if imported.Diagnostics.HasError() {
		t.Fatalf("ImportState: %v", imported.Diagnostics)
	}
	var importedName types.String
	imported.State.GetAttribute(ctx, path.Root("name"), &importedName)
	if importedName.ValueString() != "emails" {
		t.Errorf("imported name = %v", importedName)
	}

	fake.calls = nil
	del := resource.DeleteResponse{State: update.State}
	r.Delete(ctx, resource.DeleteRequest{State: update.State}, &del)
	if del.Diagnostics.HasError() {
		t.Fatalf("Delete: %v", del.Diagnostics)
	}
	if want := []string{"ClearQueueOverride"}; !slices.Equal(fake.calls, want) {
		t.Errorf("delete calls = %v, want %v", fake.calls, want)
	}
}

func TestQueueResourceDeleteResumesAPausedQueue(t *testing.T) {
	ctx := context.Background()
	fake := newFakeAdmin()
	fake.overrides["q"] = admin.QueueOverride{MaxConcurrent: int32Ptr(1)}
	fake.paused["q"] = true
	r := &queueResource{client: fake}
	s := resourceSchema(t, r)

	state := stateOf(t, s, queueFromRemote("q", fake.overrides["q"], true))
	resp := resource.DeleteResponse{State: state}
	r.Delete(ctx, resource.DeleteRequest{State: state}, &resp)
	if resp.Diagnostics.HasError() {
		t.Fatalf("Delete: %v", resp.Diagnostics)
	}
	if _, ok := fake.overrides["q"]; ok || fake.paused["q"] {
		t.Errorf("after delete: overrides %v, paused %v", fake.overrides, fake.paused)
	}
}

func TestQueueResourceSurfacesServerErrors(t *testing.T) {
	ctx := context.Background()
	fake := newFakeAdmin()
	fake.fail["SetQueueOverride"] = errors.New("INVALID_REQUEST")
	r := &queueResource{client: fake}
	s := resourceSchema(t, r)

	resp := resource.CreateResponse{State: emptyState(s)}
	r.Create(ctx, resource.CreateRequest{
		Plan: planOf(t, s, queuePlan("q", types.Int32Value(1), types.StringNull(), false)),
	}, &resp)
	if !resp.Diagnostics.HasError() {
		t.Fatal("a refused Set must fail Create")
	}
	if !resp.State.Raw.Equal(nullObject(s)) {
		t.Error("a failed Create must not record state")
	}
}

func TestQueueConfigValidation(t *testing.T) {
	s := resourceSchema(t, &queueResource{})
	name := tftypes.NewValue(tftypes.String, "q")
	cases := []struct {
		name    string
		values  map[string]tftypes.Value
		wantErr bool
	}{
		{name: "name only", values: map[string]tftypes.Value{"name": name}},
		{name: "zero cap and a rate", values: map[string]tftypes.Value{
			"name":           name,
			"max_concurrent": tftypes.NewValue(tftypes.Number, 0),
			"rate_limit":     tftypes.NewValue(tftypes.String, "100/m"),
		}},
		{name: "negative cap", wantErr: true, values: map[string]tftypes.Value{
			"name": name, "max_concurrent": tftypes.NewValue(tftypes.Number, -1),
		}},
		{name: "rate under one", wantErr: true, values: map[string]tftypes.Value{
			"name": name, "rate_limit": tftypes.NewValue(tftypes.String, "0/s"),
		}},
		{name: "rate with an unknown unit", wantErr: true, values: map[string]tftypes.Value{
			"name": name, "rate_limit": tftypes.NewValue(tftypes.String, "10/d"),
		}},
		{name: "empty name", wantErr: true, values: map[string]tftypes.Value{
			"name": tftypes.NewValue(tftypes.String, ""),
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			diags := validateResourceConfig(t, "flexiq_queue", s, tc.values)
			if hasProtoError(diags) != tc.wantErr {
				t.Errorf("diagnostics = %v, want error %v", protoSummaries(diags), tc.wantErr)
			}
		})
	}
}
