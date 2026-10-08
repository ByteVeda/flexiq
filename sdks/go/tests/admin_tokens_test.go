package tests

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	"github.com/ByteVeda/flexiq/sdks/go/v2/admin"
	adminv1 "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/admin/v1"
)

// TestCreateTokenLeavesTheDefaultExpiryToTheServer: zero days is "the server's
// default", which only an unset field says.
func TestCreateTokenLeavesTheDefaultExpiryToTheServer(t *testing.T) {
	var got *adminv1.CreateTokenRequest
	created := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	client := serveAdmin(t, &fakeAdmin{
		createToken: func(_ context.Context, req *adminv1.CreateTokenRequest) (*adminv1.CreateTokenResponse, error) {
			got = req
			return &adminv1.CreateTokenResponse{
				Token: &adminv1.ApiToken{
					Id:        "tok1",
					Name:      req.GetName(),
					Scopes:    req.GetScopes(),
					CreatedAt: timestamppb.New(created),
					ExpiresAt: timestamppb.New(created.Add(90 * 24 * time.Hour)),
					Status:    adminv1.TokenStatus_TOKEN_STATUS_ACTIVE,
					CreatedBy: strPtr("token:parent"),
				},
				Secret: "fqt_tok1.secret",
			}, nil
		},
	})
	ctx := context.Background()

	minted, err := client.CreateToken(ctx, admin.CreateTokenRequest{Name: "ci", Scopes: []string{"produce"}})
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	if got.ExpireDays != nil {
		t.Errorf("zero days went out as %d", got.GetExpireDays())
	}
	if minted.Secret != "fqt_tok1.secret" || minted.Token.ID != "tok1" {
		t.Errorf("minted is %+v", minted)
	}
	token := minted.Token
	if token.Status != admin.TokenStatusActive || token.CreatedBy != "token:parent" ||
		!token.CreatedAt.Equal(created) || !token.LastUsedAt.IsZero() || !token.RevokedAt.IsZero() {
		t.Errorf("token is %+v", token)
	}
	if !slices.Equal(token.Scopes, []string{"produce"}) {
		t.Errorf("scopes are %v", token.Scopes)
	}

	if _, err := client.CreateToken(ctx, admin.CreateTokenRequest{Name: "ci", Scopes: []string{"produce"}, ExpireDays: 7}); err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	if got.ExpireDays == nil || got.GetExpireDays() != 7 {
		t.Errorf("expire_days went out as %v", got.ExpireDays)
	}
}

// TestCreateTokenRefusesNoScopes locally: a token with no grant is refused by
// the server, and refusing it here keeps the call from going out at all.
func TestCreateTokenRefusesNoScopes(t *testing.T) {
	fake := &fakeAdmin{}
	client := serveAdmin(t, fake)

	if _, err := client.CreateToken(context.Background(), admin.CreateTokenRequest{Name: "ci"}); err == nil {
		t.Error("a token with no scopes was accepted")
	}
	if fake.calls != 0 {
		t.Errorf("%d requests reached the server", fake.calls)
	}
}

// TestTokenNotFoundIsDistinguishable: a destroy of a token already gone, and a
// read of one, must be told apart from every other failure.
func TestTokenNotFoundIsDistinguishable(t *testing.T) {
	var asked []string
	client := serveAdmin(t, &fakeAdmin{
		getToken: func(_ context.Context, req *adminv1.GetTokenRequest) (*adminv1.GetTokenResponse, error) {
			asked = append(asked, req.GetTokenId())
			return nil, notFound(t, flexiq.ReasonTokenNotFound)
		},
		revokeToken: func(_ context.Context, req *adminv1.RevokeTokenRequest) (*adminv1.RevokeTokenResponse, error) {
			asked = append(asked, req.GetTokenId())
			if req.GetTokenId() == "live" {
				return &adminv1.RevokeTokenResponse{Token: &adminv1.ApiToken{
					Id:     "live",
					Status: adminv1.TokenStatus_TOKEN_STATUS_REVOKED,
				}}, nil
			}
			return nil, notFound(t, flexiq.ReasonTokenNotFound)
		},
	})
	ctx := context.Background()

	if _, err := client.GetToken(ctx, "gone"); !errors.Is(err, flexiq.ReasonTokenNotFound) {
		t.Errorf("GetToken: want ReasonTokenNotFound, got %v", err)
	}
	if _, err := client.RevokeToken(ctx, "gone"); !errors.Is(err, flexiq.ReasonTokenNotFound) {
		t.Errorf("RevokeToken: want ReasonTokenNotFound, got %v", err)
	}
	revoked, err := client.RevokeToken(ctx, "live")
	if err != nil || revoked.Status != admin.TokenStatusRevoked || revoked.Status.String() != "REVOKED" {
		t.Errorf("RevokeToken(live) = %+v, %v", revoked, err)
	}
	if !slices.Equal(asked, []string{"gone", "gone", "live"}) {
		t.Errorf("token ids on the wire were %v", asked)
	}
}
