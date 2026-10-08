package provider

import (
	"time"

	"github.com/ByteVeda/flexiq/sdks/go/v2/admin"
)

const day = 24 * time.Hour

// needsRotation decides whether a token must be replaced: it is no longer
// active, or now is inside its rotation window — rotateBeforeDays before it
// expires. Zero days rotates only once it has expired.
func needsRotation(now, expiresAt time.Time, rotateBeforeDays int64, status string) bool {
	if status != admin.TokenStatusActive.String() {
		return true
	}
	if expiresAt.IsZero() {
		return false
	}
	return !now.Before(expiresAt.Add(-time.Duration(rotateBeforeDays) * day))
}

// lifetimeDays recovers expire_days from a token's timestamps, for import.
// The server sets expires_at to created_at plus whole days.
func lifetimeDays(createdAt, expiresAt time.Time) int64 {
	return int64(expiresAt.Sub(createdAt).Round(day) / day)
}
