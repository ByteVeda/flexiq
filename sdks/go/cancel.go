package flexiq

import (
	"context"

	pb "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/v1"
)

// CancelJob cancels a job and returns the state that leaves it in.
//
// The response describes state rather than what the call did, which is what
// makes calling it twice safe:
//
//   - A pending job comes back [StatusCancelled].
//   - A running job cannot be stopped from outside, so it comes back
//     [StatusRunning] with CancelRequested set; the task stops at its next
//     check.
//   - A job already in a terminal state comes back unchanged.
//
// The returned job carries neither payload nor result.
func (c *Client) CancelJob(ctx context.Context, jobID string) (Job, error) {
	resp, err := c.producer.CancelJob(ctx, &pb.CancelJobRequest{JobId: jobID})
	if err != nil {
		return Job{}, fromRPC(err)
	}
	return jobFromProto(resp.GetJob()), nil
}
