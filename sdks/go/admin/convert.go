package admin

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ByteVeda/flexiq/sdks/go/v2/internal/door"
)

// rpcError reads a failed call as the root package's [*flexiq.Error], so an
// admin failure branches on the same reasons a producer failure does.
func rpcError(err error) error {
	if err == nil {
		return nil
	}
	return door.FromRPC(err)
}

// asTime keeps "the server did not set this" distinguishable from "the server
// set the epoch": an absent Timestamp becomes the zero time, not 1970.
func asTime(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

// optionalString maps the empty string to an unset proto3 optional. Every
// optional string this door takes refuses an empty one, so "" can only mean
// "not set".
func optionalString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
