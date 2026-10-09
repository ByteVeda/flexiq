package provider

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-go/tftypes"

	"github.com/ByteVeda/flexiq/sdks/go/v2/admin"
)

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func fixedClock(at time.Time) func() time.Time { return func() time.Time { return at } }

func TestDecideRotation(t *testing.T) {
	expires := epoch.Add(30 * day)
	active := admin.TokenStatusActive.String()
	unspecified := admin.TokenStatusUnspecified.String()
	cases := []struct {
		name    string
		now     time.Time
		before  int64
		status  string
		replace bool
		unknown bool
	}{
		{"fresh, no window", epoch, 0, active, false, false},
		{"just before expiry, no window", expires.Add(-time.Second), 0, active, false, false},
		{"at expiry, no window", expires, 0, active, true, false},
		{"outside the window", epoch, 7, active, false, false},
		{"a second before the window", expires.Add(-7*day - time.Second), 7, active, false, false},
		{"window opens", expires.Add(-7 * day), 7, active, true, false},
		{"inside the window", expires.Add(-day), 7, active, true, false},
		{"revoked", epoch, 0, admin.TokenStatusRevoked.String(), true, false},
		{"expired", epoch, 0, admin.TokenStatusExpired.String(), true, false},
		// An unknown status alone warns and keeps the token; its window still counts.
		{"unspecified status", epoch, 0, unspecified, false, true},
		{"unnamed status", epoch, 0, admin.TokenStatus(9).String(), false, true},
		{"unknown status in the window", expires.Add(-day), 7, unspecified, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := decideRotation(tc.now, expires, tc.before, tc.status)
			if got.replace != tc.replace || got.unknownStatus != tc.unknown {
				t.Errorf("decideRotation = %+v, want replace %v unknown %v", got, tc.replace, tc.unknown)
			}
		})
	}
	if decideRotation(epoch, time.Time{}, 7, active).replace {
		t.Error("an active token with no expiry must not rotate")
	}
}

func TestLifetimeDays(t *testing.T) {
	if got, err := lifetimeDays(epoch, epoch.Add(30*day)); err != nil || got != 30 {
		t.Errorf("lifetimeDays = %d, %v; want 30", got, err)
	}
	// Millisecond rounding on the server never shifts the day count.
	if got, err := lifetimeDays(epoch, epoch.Add(90*day-time.Millisecond)); err != nil || got != 90 {
		t.Errorf("lifetimeDays = %d, %v; want 90", got, err)
	}
	for _, tc := range []struct {
		name               string
		created, expiresAt time.Time
	}{
		{"no expiry", epoch, time.Time{}},
		{"no creation time", time.Time{}, epoch},
		{"under a day", epoch, epoch.Add(time.Hour)},
		{"past a year", epoch, epoch.Add(366 * day)},
		{"expires before it was made", epoch, epoch.Add(-day)},
	} {
		if _, err := lifetimeDays(tc.created, tc.expiresAt); err == nil {
			t.Errorf("%s: lifetimeDays = nil error, want one", tc.name)
		}
	}
}

func tokenPlan(t *testing.T, rotateBefore int32) tokenModel {
	t.Helper()
	scopes, diags := types.ListValueFrom(context.Background(), types.StringType, []string{"inspect"})
	if diags.HasError() {
		t.Fatal(diags)
	}
	return tokenModel{
		ID:               types.StringUnknown(),
		Name:             types.StringValue("reader"),
		Scopes:           scopes,
		ExpireDays:       types.Int32Value(30),
		RotateBeforeDays: types.Int32Value(rotateBefore),
		Namespace:        types.StringUnknown(),
		Secret:           types.StringUnknown(),
		ExpiresAt:        types.StringUnknown(),
		CreatedAt:        types.StringUnknown(),
		Status:           types.StringUnknown(),
	}
}

// createToken mints through the resource and answers the state it recorded.
func createToken(t *testing.T, r *tokenResource) (tokenModel, resource.CreateResponse) {
	t.Helper()
	s := resourceSchema(t, r)
	resp := resource.CreateResponse{State: emptyState(s)}
	r.Create(context.Background(), resource.CreateRequest{Plan: planOf(t, s, tokenPlan(t, 0))}, &resp)
	if resp.Diagnostics.HasError() {
		t.Fatalf("Create: %v", resp.Diagnostics)
	}
	return stateModel[tokenModel](t, resp.State), resp
}

func TestTokenSecretIsSensitive(t *testing.T) {
	s := resourceSchema(t, &tokenResource{})
	attr, ok := s.Attributes["secret"]
	if !ok || !attr.IsSensitive() {
		t.Error("secret must be sensitive")
	}
}

func TestTokenCreateRecordsTheSecret(t *testing.T) {
	fake := newFakeAdmin()
	fake.clock = fixedClock(epoch)
	got, _ := createToken(t, &tokenResource{client: fake, now: fixedClock(epoch)})

	if got.ID.ValueString() != "tok-1" || got.Secret.ValueString() != "fqt_secret-1" ||
		got.Status.ValueString() != "ACTIVE" || got.Namespace.ValueString() != "ns" {
		t.Errorf("state = %+v", got)
	}
	if got.ExpiresAt.ValueString() != "2026-01-31T00:00:00Z" || got.CreatedAt.ValueString() != "2026-01-01T00:00:00Z" {
		t.Errorf("times = %v / %v", got.CreatedAt, got.ExpiresAt)
	}
	if fake.tokens["tok-1"].Scopes[0] != "inspect" {
		t.Errorf("minted scopes = %v", fake.tokens["tok-1"].Scopes)
	}
}

func TestTokenCreateIsNeverRetried(t *testing.T) {
	fake := newFakeAdmin()
	fake.fail["CreateToken"] = errors.New("UNAVAILABLE")
	r := &tokenResource{client: fake, now: time.Now}
	s := resourceSchema(t, r)

	resp := resource.CreateResponse{State: emptyState(s)}
	r.Create(context.Background(), resource.CreateRequest{Plan: planOf(t, s, tokenPlan(t, 0))}, &resp)
	if !resp.Diagnostics.HasError() {
		t.Fatal("a failed mint must fail Create")
	}
	if want := []string{"CreateToken"}; !slices.Equal(fake.calls, want) {
		t.Errorf("calls = %v, want exactly one CreateToken", fake.calls)
	}
	if !resp.State.Raw.Equal(nullObject(s)) {
		t.Error("a failed Create must not record state")
	}
}

func TestTokenModifyPlan(t *testing.T) {
	fake := newFakeAdmin()
	fake.clock = fixedClock(epoch)
	created, _ := createToken(t, &tokenResource{client: fake, now: fixedClock(epoch)})

	revoked := created
	revoked.Status = types.StringValue("REVOKED")
	unknown := created
	unknown.Status = types.StringValue("UNSPECIFIED")

	// The clock moves; the token stays the one minted at epoch, 30 days.
	cases := []struct {
		name         string
		now          time.Time
		state        tokenModel
		rotateBefore int32
		replace      bool
		warn         bool
	}{
		{"fresh token", epoch, created, 0, false, false},
		{"outside the window", epoch.Add(day), created, 7, false, false},
		{"a second before the window", epoch.Add(23*day - time.Second), created, 7, false, false},
		{"window opens", epoch.Add(23 * day), created, 7, true, false},
		{"inside the window", epoch.Add(25 * day), created, 7, true, false},
		{"expired", epoch.Add(31 * day), created, 0, true, false},
		{"revoked", epoch, revoked, 0, true, false},
		{"unknown status", epoch, unknown, 0, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &tokenResource{client: fake, now: fixedClock(tc.now)}
			s := resourceSchema(t, r)
			plan := tc.state
			plan.RotateBeforeDays = types.Int32Value(tc.rotateBefore)

			req := resource.ModifyPlanRequest{State: stateOf(t, s, tc.state), Plan: planOf(t, s, plan)}
			resp := resource.ModifyPlanResponse{Plan: req.Plan}
			r.ModifyPlan(context.Background(), req, &resp)
			if resp.Diagnostics.HasError() {
				t.Fatalf("ModifyPlan: %v", resp.Diagnostics)
			}

			var secret types.String
			resp.Plan.GetAttribute(context.Background(), path.Root("secret"), &secret)
			replaced := slices.ContainsFunc(resp.RequiresReplace, func(p path.Path) bool { return p.Equal(path.Root("secret")) })
			if replaced != tc.replace || secret.IsUnknown() != tc.replace {
				t.Errorf("requires replace = %v, secret unknown = %v; want %v", replaced, secret.IsUnknown(), tc.replace)
			}
			if warned := resp.Diagnostics.WarningsCount() > 0; warned != tc.warn {
				t.Errorf("warned = %v, want %v: %v", warned, tc.warn, resp.Diagnostics)
			}
		})
	}
}

func TestTokenModifyPlanLeavesCreateAndDestroyAlone(t *testing.T) {
	r := &tokenResource{now: fixedClock(epoch)}
	s := resourceSchema(t, r)
	create := resource.ModifyPlanRequest{State: emptyState(s), Plan: planOf(t, s, tokenPlan(t, 0))}
	resp := resource.ModifyPlanResponse{Plan: create.Plan}
	r.ModifyPlan(context.Background(), create, &resp)
	if len(resp.RequiresReplace) != 0 || resp.Diagnostics.HasError() {
		t.Errorf("create: %v %v", resp.RequiresReplace, resp.Diagnostics)
	}
}

func TestTokenReadKeepsARevokedToken(t *testing.T) {
	ctx := context.Background()
	fake := newFakeAdmin()
	r := &tokenResource{client: fake, now: time.Now}
	_, created := createToken(t, r)

	if _, err := fake.RevokeToken(ctx, "tok-1"); err != nil {
		t.Fatal(err)
	}
	read := resource.ReadResponse{State: created.State}
	r.Read(ctx, resource.ReadRequest{State: created.State}, &read)
	if read.Diagnostics.HasError() {
		t.Fatalf("Read: %v", read.Diagnostics)
	}
	got := stateModel[tokenModel](t, read.State)
	if got.Status.ValueString() != "REVOKED" || got.Secret.ValueString() != "fqt_secret-1" {
		t.Errorf("revoked token state = %+v", got)
	}

	delete(fake.tokens, "tok-1")
	read = resource.ReadResponse{State: created.State}
	r.Read(ctx, resource.ReadRequest{State: created.State}, &read)
	if !read.State.Raw.IsNull() {
		t.Error("a token the server no longer has must leave state")
	}
}

func TestTokenImportHasNoSecret(t *testing.T) {
	ctx := context.Background()
	fake := newFakeAdmin()
	r := &tokenResource{client: fake, now: time.Now}
	createToken(t, r)
	s := resourceSchema(t, r)

	imported := resource.ImportStateResponse{State: emptyState(s)}
	r.ImportState(ctx, resource.ImportStateRequest{ID: "tok-1"}, &imported)
	if imported.Diagnostics.HasError() {
		t.Fatalf("ImportState: %v", imported.Diagnostics)
	}
	read := resource.ReadResponse{State: imported.State}
	r.Read(ctx, resource.ReadRequest{State: imported.State}, &read)
	if read.Diagnostics.HasError() {
		t.Fatalf("Read: %v", read.Diagnostics)
	}
	got := stateModel[tokenModel](t, read.State)
	var scopes []string
	got.Scopes.ElementsAs(ctx, &scopes, false)
	if !got.Secret.IsNull() || got.Name.ValueString() != "reader" || got.ExpireDays.ValueInt32() != 30 ||
		got.RotateBeforeDays.ValueInt32() != 0 || !slices.Equal(scopes, []string{"inspect"}) {
		t.Errorf("imported = %+v", got)
	}
}

func TestTokenImportWithoutExpiry(t *testing.T) {
	ctx := context.Background()
	fake := newFakeAdmin()
	fake.tokens["tok-x"] = admin.Token{ID: "tok-x", Name: "x", Scopes: []string{"inspect"}, Status: admin.TokenStatusActive}
	r := &tokenResource{client: fake, now: time.Now}
	s := resourceSchema(t, r)

	imported := resource.ImportStateResponse{State: emptyState(s)}
	r.ImportState(ctx, resource.ImportStateRequest{ID: "tok-x"}, &imported)
	read := resource.ReadResponse{State: imported.State}
	r.Read(ctx, resource.ReadRequest{State: imported.State}, &read)
	if !read.Diagnostics.HasError() {
		t.Fatal("importing a token with no expiry must fail clearly, not invent expire_days")
	}

	// A read of a managed token keeps going: the unset times are null.
	got := withRemoteToken(tokenModel{}, fake.tokens["tok-x"])
	if !got.ExpiresAt.IsNull() || !got.CreatedAt.IsNull() {
		t.Errorf("unset times = %v / %v, want null", got.ExpiresAt, got.CreatedAt)
	}
}

func TestTokenDeleteRevokes(t *testing.T) {
	ctx := context.Background()
	fake := newFakeAdmin()
	r := &tokenResource{client: fake, now: time.Now}
	_, created := createToken(t, r)

	del := resource.DeleteResponse{State: created.State}
	r.Delete(ctx, resource.DeleteRequest{State: created.State}, &del)
	if del.Diagnostics.HasError() || fake.tokens["tok-1"].Status != admin.TokenStatusRevoked {
		t.Fatalf("Delete: %v, status %v", del.Diagnostics, fake.tokens["tok-1"].Status)
	}

	// Already gone counts as revoked.
	delete(fake.tokens, "tok-1")
	del = resource.DeleteResponse{State: created.State}
	r.Delete(ctx, resource.DeleteRequest{State: created.State}, &del)
	if del.Diagnostics.HasError() {
		t.Errorf("Delete of a gone token: %v", del.Diagnostics)
	}

	fake.fail["RevokeToken"] = errors.New("boom")
	del = resource.DeleteResponse{State: created.State}
	r.Delete(ctx, resource.DeleteRequest{State: created.State}, &del)
	if !del.Diagnostics.HasError() {
		t.Error("a failing revoke must surface")
	}
}

func TestTokenConfigValidation(t *testing.T) {
	s := resourceSchema(t, &tokenResource{})
	str := func(v string) tftypes.Value { return tftypes.NewValue(tftypes.String, v) }
	num := func(v int) tftypes.Value { return tftypes.NewValue(tftypes.Number, v) }
	list := func(vs ...string) tftypes.Value {
		elems := make([]tftypes.Value, 0, len(vs))
		for _, v := range vs {
			elems = append(elems, str(v))
		}
		return tftypes.NewValue(tftypes.List{ElementType: tftypes.String}, elems)
	}
	base := func(extra map[string]tftypes.Value) map[string]tftypes.Value {
		values := map[string]tftypes.Value{"name": str("t"), "scopes": list("inspect"), "expire_days": num(30)}
		for k, v := range extra {
			values[k] = v
		}
		return values
	}
	cases := []struct {
		name    string
		values  map[string]tftypes.Value
		wantErr bool
	}{
		{name: "minimal", values: base(nil)},
		{name: "a year", values: base(map[string]tftypes.Value{"expire_days": num(365)})},
		{name: "zero days", wantErr: true, values: base(map[string]tftypes.Value{"expire_days": num(0)})},
		{name: "past a year", wantErr: true, values: base(map[string]tftypes.Value{"expire_days": num(366)})},
		{name: "no scopes", wantErr: true, values: base(map[string]tftypes.Value{"scopes": list()})},
		{name: "empty scope", wantErr: true, values: base(map[string]tftypes.Value{"scopes": list("")})},
		{name: "negative window", wantErr: true, values: base(map[string]tftypes.Value{"rotate_before_days": num(-1)})},
		{name: "window inside the lifetime", values: base(map[string]tftypes.Value{"rotate_before_days": num(29)})},
		{name: "window as long as the lifetime", wantErr: true, values: base(map[string]tftypes.Value{"rotate_before_days": num(30)})},
		{name: "window past the lifetime", wantErr: true, values: base(map[string]tftypes.Value{"rotate_before_days": num(90)})},
		{name: "unknown lifetime waits", values: base(map[string]tftypes.Value{
			"expire_days": tftypes.NewValue(tftypes.Number, tftypes.UnknownValue), "rotate_before_days": num(90),
		})},
		{name: "long name", wantErr: true, values: base(map[string]tftypes.Value{"name": str(strings.Repeat("x", 65))})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			diags := validateResourceConfig(t, "flexiq_token", s, tc.values)
			if hasProtoError(diags) != tc.wantErr {
				t.Errorf("diagnostics = %v, want error %v", protoSummaries(diags), tc.wantErr)
			}
		})
	}
}
