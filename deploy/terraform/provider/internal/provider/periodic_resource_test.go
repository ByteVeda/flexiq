package provider

import (
	"context"
	"errors"
	"math"
	"slices"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-go/tftypes"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
)

// periodicPlan is a plan for the periodic task "p".
func periodicPlan(queue, args, kwargs string, enabled bool) periodicModel {
	return periodicModel{
		ID:       types.StringUnknown(),
		Name:     types.StringValue("p"),
		TaskName: types.StringValue("send_report"),
		Cron:     types.StringValue("0 */5 * * * *"),
		Queue:    types.StringValue(queue),
		Timezone: types.StringNull(),
		Args:     types.StringValue(args),
		Kwargs:   types.StringValue(kwargs),
		Enabled:  types.BoolValue(enabled),
	}
}

func TestQueueEqual(t *testing.T) {
	for _, pair := range [][2]string{{"", "default"}, {"default", ""}, {"", ""}, {"emails", "emails"}} {
		if !queueEqual(pair[0], pair[1]) {
			t.Errorf("queueEqual(%q, %q) = false", pair[0], pair[1])
		}
	}
	for _, pair := range [][2]string{{"", "emails"}, {"default", "Default"}} {
		if queueEqual(pair[0], pair[1]) {
			t.Errorf("queueEqual(%q, %q) = true", pair[0], pair[1])
		}
	}
}

func TestPeriodicSpecSendsIntegers(t *testing.T) {
	spec, err := periodicSpec(periodicPlan("", `[1, 2.5]`, `{"n": 3.0}`, true))
	if err != nil {
		t.Fatalf("periodicSpec: %v", err)
	}
	if spec.Args[0] != int64(1) || spec.Args[1] != 2.5 || spec.Kwargs["n"] != int64(3) {
		t.Errorf("args %#v kwargs %#v", spec.Args, spec.Kwargs)
	}
	if spec.StartPaused {
		t.Error("enabled = true must not start paused")
	}
}

func TestApplyPeriodicReconcilesEnabled(t *testing.T) {
	ctx := context.Background()
	fake := newFakeAdmin()

	// A create honours start_paused, so a disabled task needs no extra call.
	if err := applyPeriodic(ctx, fake, periodicPlan("", `[]`, `{}`, false)); err != nil {
		t.Fatalf("create: %v", err)
	}
	if want := []string{"PutPeriodicTask"}; !slices.Equal(fake.calls, want) {
		t.Errorf("create calls = %v, want %v", fake.calls, want)
	}
	if fake.periodic["p"].Enabled {
		t.Error("created enabled")
	}

	// A replace ignores start_paused, so enabling takes a resume.
	fake.calls = nil
	if err := applyPeriodic(ctx, fake, periodicPlan("", `[]`, `{}`, true)); err != nil {
		t.Fatalf("enable: %v", err)
	}
	if want := []string{"PutPeriodicTask", "ResumePeriodicTask"}; !slices.Equal(fake.calls, want) {
		t.Errorf("enable calls = %v, want %v", fake.calls, want)
	}

	fake.calls = nil
	if err := applyPeriodic(ctx, fake, periodicPlan("", `[]`, `{}`, false)); err != nil {
		t.Fatalf("disable: %v", err)
	}
	if want := []string{"PutPeriodicTask", "PausePeriodicTask"}; !slices.Equal(fake.calls, want) {
		t.Errorf("disable calls = %v, want %v", fake.calls, want)
	}
}

func TestApplyPeriodicReportsTheFailingCall(t *testing.T) {
	ctx := context.Background()
	for _, method := range []string{"PutPeriodicTask", "PausePeriodicTask"} {
		fake := newFakeAdmin()
		// An enabled task exists, so disabling it is a put then a pause.
		if err := applyPeriodic(ctx, fake, periodicPlan("", `[]`, `{}`, true)); err != nil {
			t.Fatal(err)
		}
		boom := errors.New("boom")
		fake.fail[method] = boom
		if err := applyPeriodic(ctx, fake, periodicPlan("", `[]`, `{}`, false)); !errors.Is(err, boom) {
			t.Errorf("%s failing: err = %v", method, err)
		}
	}
}

func TestReadPeriodicKeepsEquivalentSpelling(t *testing.T) {
	ctx := context.Background()
	fake := newFakeAdmin()
	plan := periodicPlan("", "[ 1.0,\n \"a\" ]", `{"b": 2, "a": 1}`, true)
	if err := applyPeriodic(ctx, fake, plan); err != nil {
		t.Fatal(err)
	}

	got, found, err := readPeriodic(ctx, fake, plan)
	if err != nil || !found {
		t.Fatalf("readPeriodic: found %v err %v", found, err)
	}
	if got.Args != plan.Args || got.Kwargs != plan.Kwargs || got.Queue.ValueString() != "" {
		t.Errorf("equivalent values must keep the config's spelling: %+v", got)
	}

	// Import has no prior: the server's canonical form is read.
	imported, _, err := readPeriodic(ctx, fake, periodicModel{Name: types.StringValue("p")})
	if err != nil {
		t.Fatal(err)
	}
	if imported.Args.ValueString() != `[1,"a"]` || imported.Kwargs.ValueString() != `{"a":1,"b":2}` ||
		imported.Queue.ValueString() != defaultQueue || !imported.Timezone.IsNull() {
		t.Errorf("imported = %+v", imported)
	}
}

func TestReadPeriodicSeesDrift(t *testing.T) {
	ctx := context.Background()
	fake := newFakeAdmin()
	plan := periodicPlan("emails", `[1]`, `{}`, true)
	if err := applyPeriodic(ctx, fake, plan); err != nil {
		t.Fatal(err)
	}
	if _, err := fake.PausePeriodicTask(ctx, "p"); err != nil {
		t.Fatal(err)
	}
	payload, err := flexiq.EncodeCall([]any{2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	task := fake.periodic["p"]
	task.Payload = payload
	fake.periodic["p"] = task

	got, _, err := readPeriodic(ctx, fake, plan)
	if err != nil {
		t.Fatal(err)
	}
	if got.Enabled.ValueBool() || got.Args.ValueString() != `[2]` {
		t.Errorf("drift not read: %+v", got)
	}

	delete(fake.periodic, "p")
	if _, found, err := readPeriodic(ctx, fake, plan); err != nil || found {
		t.Errorf("deleted task: found %v err %v, want gone", found, err)
	}
}

// TestPeriodicReadSurvivesAnUnreadablePayload: a payload written by something
// else warns, reads as null arguments, and the next apply overwrites it.
func TestPeriodicReadSurvivesAnUnreadablePayload(t *testing.T) {
	nan, err := flexiq.EncodeCall([]any{math.NaN()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	payloads := map[string][]byte{
		"a language-native payload": {flexiq.TagNative, 0x80, 0x04},
		"not CBOR at all":           {flexiq.TagCBOR, 0xff, 0xff},
		"no JSON form (NaN)":        nan,
	}
	for name, payload := range payloads {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			fake := newFakeAdmin()
			r := &periodicResource{client: fake}
			s := resourceSchema(t, r)

			plan := periodicPlan("", `[1]`, `{}`, false)
			create := resource.CreateResponse{State: emptyState(s)}
			r.Create(ctx, resource.CreateRequest{Plan: planOf(t, s, plan)}, &create)
			if create.Diagnostics.HasError() {
				t.Fatalf("Create: %v", create.Diagnostics)
			}
			task := fake.periodic["p"]
			task.Payload = payload
			fake.periodic["p"] = task

			read := resource.ReadResponse{State: create.State}
			r.Read(ctx, resource.ReadRequest{State: create.State}, &read)
			if read.Diagnostics.HasError() || read.Diagnostics.WarningsCount() != 1 {
				t.Fatalf("Read must warn, not fail: %v", read.Diagnostics)
			}
			got := stateModel[periodicModel](t, read.State)
			if !got.Args.IsNull() || !got.Kwargs.IsNull() || got.Enabled.ValueBool() || got.Cron.IsNull() {
				t.Errorf("state after an unreadable payload = %+v", got)
			}

			update := resource.UpdateResponse{State: read.State}
			r.Update(ctx, resource.UpdateRequest{Plan: planOf(t, s, plan), State: read.State}, &update)
			if update.Diagnostics.HasError() {
				t.Fatalf("Update: %v", update.Diagnostics)
			}
			if _, _, err := readPeriodic(ctx, fake, plan); err != nil {
				t.Errorf("the apply did not overwrite the payload: %v", err)
			}
		})
	}
}

func TestPeriodicResourceLifecycle(t *testing.T) {
	ctx := context.Background()
	fake := newFakeAdmin()
	r := &periodicResource{client: fake}
	s := resourceSchema(t, r)

	create := resource.CreateResponse{State: emptyState(s)}
	r.Create(ctx, resource.CreateRequest{Plan: planOf(t, s, periodicPlan("", `[1]`, `{}`, true))}, &create)
	if create.Diagnostics.HasError() {
		t.Fatalf("Create: %v", create.Diagnostics)
	}
	if got := stateModel[periodicModel](t, create.State); got.ID.ValueString() != "p" {
		t.Errorf("id = %v", got.ID)
	}

	// Deleted out of band: Read drops it so the next plan recreates it.
	delete(fake.periodic, "p")
	read := resource.ReadResponse{State: create.State}
	r.Read(ctx, resource.ReadRequest{State: create.State}, &read)
	if read.Diagnostics.HasError() {
		t.Fatalf("Read: %v", read.Diagnostics)
	}
	if !read.State.Raw.IsNull() {
		t.Error("a deleted task must leave state")
	}

	// Delete of a task that is already gone succeeds.
	del := resource.DeleteResponse{State: create.State}
	r.Delete(ctx, resource.DeleteRequest{State: create.State}, &del)
	if del.Diagnostics.HasError() {
		t.Errorf("Delete of a gone task: %v", del.Diagnostics)
	}

	fake.fail["DeletePeriodicTask"] = errors.New("boom")
	del = resource.DeleteResponse{State: create.State}
	r.Delete(ctx, resource.DeleteRequest{State: create.State}, &del)
	if !del.Diagnostics.HasError() {
		t.Error("a failing delete must surface")
	}
}

func TestPeriodicConfigValidation(t *testing.T) {
	s := resourceSchema(t, &periodicResource{})
	base := func(extra map[string]tftypes.Value) map[string]tftypes.Value {
		values := map[string]tftypes.Value{
			"name":      tftypes.NewValue(tftypes.String, "p"),
			"task_name": tftypes.NewValue(tftypes.String, "t"),
			"cron":      tftypes.NewValue(tftypes.String, "0 * * * * *"),
		}
		for k, v := range extra {
			values[k] = v
		}
		return values
	}
	str := func(v string) tftypes.Value { return tftypes.NewValue(tftypes.String, v) }
	cases := []struct {
		name    string
		values  map[string]tftypes.Value
		wantErr bool
	}{
		{name: "minimal", values: base(nil)},
		{name: "args and kwargs", values: base(map[string]tftypes.Value{"args": str(`[1]`), "kwargs": str(`{"a":1}`)})},
		{name: "args not an array", wantErr: true, values: base(map[string]tftypes.Value{"args": str(`{"a":1}`)})},
		{name: "kwargs not an object", wantErr: true, values: base(map[string]tftypes.Value{"kwargs": str(`[1]`)})},
		{name: "args not JSON", wantErr: true, values: base(map[string]tftypes.Value{"args": str(`[1`)})},
		{name: "empty timezone", wantErr: true, values: base(map[string]tftypes.Value{"timezone": str("")})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			diags := validateResourceConfig(t, "flexiq_periodic_task", s, tc.values)
			if hasProtoError(diags) != tc.wantErr {
				t.Errorf("diagnostics = %v, want error %v", protoSummaries(diags), tc.wantErr)
			}
		})
	}
}
