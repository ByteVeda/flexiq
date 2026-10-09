package provider

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	tfprovider "github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-go/tftypes"
)

func providerSchema(t *testing.T) tfprovider.SchemaResponse {
	t.Helper()
	var resp tfprovider.SchemaResponse
	(&flexiqProvider{}).Schema(context.Background(), tfprovider.SchemaRequest{}, &resp)
	if resp.Diagnostics.HasError() {
		t.Fatalf("schema: %v", resp.Diagnostics)
	}
	return resp
}

func TestProviderSchemaIsValid(t *testing.T) {
	resp := providerSchema(t)
	if diags := resp.Schema.ValidateImplementation(context.Background()); diags.HasError() {
		t.Fatalf("ValidateImplementation: %v", diags)
	}
	if !resp.Schema.Attributes["token"].IsSensitive() {
		t.Error("token must be sensitive")
	}
}

// configure runs Configure over the given attribute values; unset ones are null.
func configure(t *testing.T, values map[string]tftypes.Value) tfprovider.ConfigureResponse {
	t.Helper()
	ctx := context.Background()
	s := providerSchema(t).Schema
	objectType, ok := s.Type().TerraformType(ctx).(tftypes.Object)
	if !ok {
		t.Fatal("provider schema is not an object")
	}

	attrs := make(map[string]tftypes.Value, len(objectType.AttributeTypes))
	for name, typ := range objectType.AttributeTypes {
		attrs[name] = tftypes.NewValue(typ, nil)
	}
	for name, value := range values {
		attrs[name] = value
	}

	var resp tfprovider.ConfigureResponse
	(&flexiqProvider{}).Configure(ctx, tfprovider.ConfigureRequest{
		Config: tfsdk.Config{Schema: s, Raw: tftypes.NewValue(objectType, attrs)},
	}, &resp)
	return resp
}

func hasErrorAbout(diags diag.Diagnostics, summary string) bool {
	for _, d := range diags.Errors() {
		if strings.Contains(d.Summary(), summary) {
			return true
		}
	}
	return false
}

func TestConfigureNeedsAddressAndToken(t *testing.T) {
	t.Setenv(addressEnv, "")
	t.Setenv(tokenEnv, "")

	resp := configure(t, nil)
	if !hasErrorAbout(resp.Diagnostics, "Missing flexiq address") || !hasErrorAbout(resp.Diagnostics, "Missing flexiq token") {
		t.Fatalf("want both missing-setting errors, got %v", resp.Diagnostics)
	}
}

func TestConfigureFallsBackToTheEnvironment(t *testing.T) {
	t.Setenv(addressEnv, "127.0.0.1:1")
	t.Setenv(tokenEnv, "fqt_id.secret")

	resp := configure(t, map[string]tftypes.Value{
		"namespace": tftypes.NewValue(tftypes.String, "billing"),
	})
	if resp.Diagnostics.HasError() {
		t.Fatalf("Configure: %v", resp.Diagnostics)
	}
	data, ok := resp.ResourceData.(*providerData)
	if !ok || data.client == nil || data.namespace != "billing" {
		t.Fatalf("ResourceData = %#v", resp.ResourceData)
	}
}

func TestConfigureRefusesAnUnknownAddress(t *testing.T) {
	t.Setenv(addressEnv, "127.0.0.1:1")
	t.Setenv(tokenEnv, "fqt_id.secret")

	resp := configure(t, map[string]tftypes.Value{
		"address": tftypes.NewValue(tftypes.String, tftypes.UnknownValue),
	})
	if !hasErrorAbout(resp.Diagnostics, "Missing flexiq address") {
		t.Fatalf("an address unknown at configure time must fail, got %v", resp.Diagnostics)
	}
}

func TestSettingPrefersTheAttribute(t *testing.T) {
	t.Setenv(addressEnv, "from-env:1")
	if got, known := setting(types.StringValue("from-config:1"), addressEnv); !known || got != "from-config:1" {
		t.Errorf("setting = %q, %v", got, known)
	}
	if got, known := setting(types.StringNull(), addressEnv); !known || got != "from-env:1" {
		t.Errorf("null attribute: setting = %q, %v", got, known)
	}
	if _, known := setting(types.StringUnknown(), addressEnv); known {
		t.Error("an unknown attribute must report unknown")
	}
}

func TestDialOptions(t *testing.T) {
	validPEM := selfSignedPEM(t)
	cases := []struct {
		name     string
		settings *tlsModel
		wantOpts int
		wantErr  string
	}{
		{name: "no block verifies against system roots", settings: nil, wantOpts: 1},
		{
			name:     "empty block is the default",
			settings: &tlsModel{CACert: types.StringNull(), Insecure: types.BoolNull()},
			wantOpts: 1,
		},
		{
			name:     "insecure",
			settings: &tlsModel{CACert: types.StringNull(), Insecure: types.BoolValue(true)},
			wantOpts: 2,
		},
		{
			name:     "ca_cert",
			settings: &tlsModel{CACert: types.StringValue(validPEM), Insecure: types.BoolValue(false)},
			wantOpts: 2,
		},
		{
			name:     "ca_cert without a certificate",
			settings: &tlsModel{CACert: types.StringValue("not pem"), Insecure: types.BoolNull()},
			wantErr:  "no PEM certificate",
		},
		{
			name:     "insecure with ca_cert",
			settings: &tlsModel{CACert: types.StringValue(validPEM), Insecure: types.BoolValue(true)},
			wantErr:  "contradict",
		},
		{
			name:     "unknown",
			settings: &tlsModel{CACert: types.StringUnknown(), Insecure: types.BoolNull()},
			wantErr:  "must be known",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts, err := dialOptions("fqt_id.secret", tc.settings)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want one containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("dialOptions: %v", err)
			}
			if len(opts) != tc.wantOpts {
				t.Errorf("got %d options, want %d", len(opts), tc.wantOpts)
			}
		})
	}
}

func selfSignedPEM(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "flexiq-test-ca"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("certificate: %v", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}
