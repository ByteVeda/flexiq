package provider

import (
	"errors"
	"fmt"
	"time"

	"github.com/ByteVeda/flexiq/sdks/go/v2/admin"
)

const (
	day = 24 * time.Hour

	// minLifetimeDays and maxLifetimeDays are the server's bounds on expire_days.
	minLifetimeDays = 1
	maxLifetimeDays = 365
)

// rotation is what a plan should do about a token.
type rotation struct {
	// replace plans a new token.
	replace bool
	// unknownStatus is a status this build has no name for; it alone never
	// replaces, since a newer server may mean something harmless by it.
	unknownStatus bool
}

// decideRotation replaces a token that is expired or revoked, or whose
// rotation window — rotateBeforeDays before expiry — has opened. Zero days
// rotates only once it has expired.
func decideRotation(now, expiresAt time.Time, rotateBeforeDays int64, status string) rotation {
	switch status {
	case admin.TokenStatusExpired.String(), admin.TokenStatusRevoked.String():
		return rotation{replace: true}
	}
	r := rotation{unknownStatus: status != admin.TokenStatusActive.String()}
	if !expiresAt.IsZero() {
		r.replace = !now.Before(expiresAt.Add(-time.Duration(rotateBeforeDays) * day))
	}
	return r
}

// lifetimeDays recovers expire_days from a token's timestamps, for import.
// The server sets expires_at to created_at plus whole days, 1 to 365.
func lifetimeDays(createdAt, expiresAt time.Time) (int32, error) {
	if createdAt.IsZero() || expiresAt.IsZero() {
		return 0, errors.New("the token has no creation or expiry time, so expire_days cannot be derived")
	}
	days := expiresAt.Sub(createdAt).Round(day) / day
	if days < minLifetimeDays || days > maxLifetimeDays {
		return 0, fmt.Errorf("the token lives %d days, outside expire_days' %d to %d", days, minLifetimeDays, maxLifetimeDays)
	}
	return int32(days), nil
}
