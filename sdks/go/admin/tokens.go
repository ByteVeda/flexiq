package admin

import (
	"context"
	"errors"
	"strconv"
	"time"

	adminv1 "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/admin/v1"
)

// TokenStatus is what a token is, right now.
type TokenStatus int32

const (
	// TokenStatusUnspecified is the zero value, which a server never sends: it
	// is what a status this build has no name for reads as.
	TokenStatusUnspecified TokenStatus = 0
	// TokenStatusActive is usable.
	TokenStatusActive TokenStatus = 1
	// TokenStatusExpired is past its expiry.
	TokenStatusExpired TokenStatus = 2
	// TokenStatusRevoked is revoked. It wins over expired.
	TokenStatusRevoked TokenStatus = 3
)

func (s TokenStatus) String() string {
	switch s {
	case TokenStatusUnspecified:
		return "UNSPECIFIED"
	case TokenStatusActive:
		return "ACTIVE"
	case TokenStatusExpired:
		return "EXPIRED"
	case TokenStatusRevoked:
		return "REVOKED"
	default:
		return "TokenStatus(" + strconv.FormatInt(int64(s), 10) + ")"
	}
}

// Token is an API token as a listing shows it. It never carries the secret.
//
// A time the server did not set is the zero [time.Time]; test with IsZero.
type Token struct {
	// ID is the public identifier, the handle [Client.GetToken] and
	// [Client.RevokeToken] take.
	ID string
	// Name is the label chosen at mint. Two tokens may share it.
	Name string
	// Scopes are the grants, spelled `scope` or
	// `scope:queue=<pattern>,task=<pattern>`.
	Scopes []string
	// Namespace is the one namespace every call on the token is scoped to.
	Namespace string
	CreatedAt time.Time
	// LastUsedAt is zero until first use, and written at most once a minute.
	LastUsedAt time.Time
	ExpiresAt  time.Time
	// RevokedAt is zero unless revoked.
	RevokedAt time.Time
	Status    TokenStatus
	// CreatedBy is who minted it: a dashboard username, "cli", or
	// "token:<id>" for one minted through [Client.CreateToken]. Empty when
	// unrecorded.
	CreatedBy string
}

// CreateTokenRequest is a token to mint in the caller's namespace.
type CreateTokenRequest struct {
	// Name is at most 64 characters, no control characters.
	Name string
	// Scopes holds at least one grant. Each must be covered by one of the
	// caller's own — the same scope, reaching no queue or task the caller's
	// does not.
	Scopes []string
	// ExpireDays is days until it expires, 1 to 365. Zero takes the server's
	// default of 90. Refused, never shortened, when the token would outlive
	// the caller's.
	ExpireDays int32
}

// CreatedToken is a freshly minted token and its secret.
type CreatedToken struct {
	Token Token
	// Secret is the credential to present, shown this once. The server keeps
	// only a digest; it is unrecoverable afterwards.
	Secret string
}

// CreateToken mints an API token in the caller's namespace. Needs the "tokens"
// scope.
//
// Never retry it blindly. The server takes no idempotency key, so a call that
// landed and lost its answer — UNAVAILABLE, DEADLINE_EXCEEDED, CANCELLED —
// leaves a live token whose secret nobody holds, and a retry mints a second.
// After an ambiguous failure, find the orphan with [Client.ListTokens] by name
// and revoke it. This client configures no retry, and a retry policy passed
// through [flexiq.WithGRPCDialOptions] must not cover this method.
func (c *Client) CreateToken(ctx context.Context, req CreateTokenRequest) (CreatedToken, error) {
	if len(req.Scopes) == 0 {
		return CreatedToken{}, errors.New("flexiq: create token: at least one scope is required")
	}
	msg := &adminv1.CreateTokenRequest{Name: req.Name, Scopes: req.Scopes}
	if req.ExpireDays != 0 {
		msg.ExpireDays = &req.ExpireDays
	}

	resp, err := c.admin.CreateToken(ctx, msg)
	if err != nil {
		return CreatedToken{}, rpcError(err)
	}
	return CreatedToken{Token: tokenFromProto(resp.GetToken()), Secret: resp.GetSecret()}, nil
}

// GetToken reads one token of the caller's namespace, revoked or expired ones
// included. One that does not exist is [flexiq.ReasonTokenNotFound].
func (c *Client) GetToken(ctx context.Context, id string) (Token, error) {
	resp, err := c.admin.GetToken(ctx, &adminv1.GetTokenRequest{TokenId: id})
	if err != nil {
		return Token{}, rpcError(err)
	}
	return tokenFromProto(resp.GetToken()), nil
}

// ListTokens answers every token of the caller's namespace, newest first,
// revoked and expired ones included.
func (c *Client) ListTokens(ctx context.Context) ([]Token, error) {
	resp, err := c.admin.ListTokens(ctx, &adminv1.ListTokensRequest{})
	if err != nil {
		return nil, rpcError(err)
	}
	tokens := make([]Token, 0, len(resp.GetTokens()))
	for _, msg := range resp.GetTokens() {
		tokens = append(tokens, tokenFromProto(msg))
	}
	return tokens, nil
}

// RevokeToken revokes a token; it stops working on its next call. Refused
// unless every grant it carries is covered by one of the caller's. Revoking a
// revoked token answers it unchanged, so a retry is safe. One that does not
// exist is [flexiq.ReasonTokenNotFound].
func (c *Client) RevokeToken(ctx context.Context, id string) (Token, error) {
	resp, err := c.admin.RevokeToken(ctx, &adminv1.RevokeTokenRequest{TokenId: id})
	if err != nil {
		return Token{}, rpcError(err)
	}
	return tokenFromProto(resp.GetToken()), nil
}

func tokenFromProto(msg *adminv1.ApiToken) Token {
	return Token{
		ID:         msg.GetId(),
		Name:       msg.GetName(),
		Scopes:     msg.GetScopes(),
		Namespace:  msg.GetNamespace(),
		CreatedAt:  asTime(msg.GetCreatedAt()),
		LastUsedAt: asTime(msg.GetLastUsedAt()),
		ExpiresAt:  asTime(msg.GetExpiresAt()),
		RevokedAt:  asTime(msg.GetRevokedAt()),
		Status:     TokenStatus(msg.GetStatus()),
		CreatedBy:  msg.GetCreatedBy(),
	}
}
