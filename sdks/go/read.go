package flexiq

import (
	"context"
	"iter"

	pb "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/v1"
)

// GetJobOptions asks for the two fields a read leaves out by default.
type GetJobOptions struct {
	// IncludePayload returns the job's payload. Off by default: a payload is
	// the largest thing a job carries and most readers do not want it.
	IncludePayload bool
	// IncludeResult returns the task's return value.
	IncludeResult bool
}

// ListJobsQuery filters and pages a listing. Every zero value means "no
// filter".
type ListJobsQuery struct {
	Status   JobStatus
	Queue    string
	TaskName string
	// PageSize is rows per page. Zero takes the server's default; the server
	// may return fewer, and may cap a large value.
	PageSize int32
	// PageToken is the previous page's [JobPage.NextPageToken]. It is opaque
	// and server-issued: pass it back, never construct one.
	PageToken string
}

// JobPage is one page of a listing.
type JobPage struct {
	// Jobs are newest first, and never carry a payload or a result — a page of
	// a hundred jobs would otherwise be a page of a hundred payloads. Read one
	// with [Client.GetJob].
	Jobs []Job
	// NextPageToken is empty on the last page.
	NextPageToken string
}

// QueueStats is the per-status job count for one queue or for the namespace.
type QueueStats struct {
	Pending   int64
	Running   int64
	Completed int64
	Failed    int64
	Dead      int64
	Cancelled int64
}

// GetJob reads one job by id.
//
// There is no completion notification anywhere on this door: no watch, and no
// server stream. Poll this, or subscribe a webhook on the server side.
//
// A job in another namespace answers [ReasonJobNotFound], indistinguishable
// from a job that never existed. So does a job retention has already deleted.
func (c *Client) GetJob(ctx context.Context, jobID string, opts GetJobOptions) (Job, error) {
	resp, err := c.producer.GetJob(ctx, &pb.GetJobRequest{
		JobId:          jobID,
		IncludePayload: opts.IncludePayload,
		IncludeResult:  opts.IncludeResult,
	})
	if err != nil {
		return Job{}, fromRPC(err)
	}
	return jobFromProto(resp.GetJob()), nil
}

// ListJobs reads one page of jobs, newest first. For every page, see
// [Client.AllJobs].
func (c *Client) ListJobs(ctx context.Context, query ListJobsQuery) (JobPage, error) {
	req := &pb.ListJobsRequest{
		PageSize:  query.PageSize,
		PageToken: query.PageToken,
	}
	// Each filter is left unset rather than sent as its zero value: an unset
	// status lists every status, where JOB_STATUS_UNSPECIFIED would be a
	// filter for a status no job has.
	if query.Status != StatusUnspecified {
		status := pb.JobStatus(query.Status)
		req.Status = &status
	}
	if query.Queue != "" {
		req.Queue = &query.Queue
	}
	if query.TaskName != "" {
		req.TaskName = &query.TaskName
	}

	resp, err := c.producer.ListJobs(ctx, req)
	if err != nil {
		return JobPage{}, fromRPC(err)
	}

	jobs := make([]Job, 0, len(resp.GetJobs()))
	for _, msg := range resp.GetJobs() {
		jobs = append(jobs, jobFromProto(msg))
	}
	return JobPage{Jobs: jobs, NextPageToken: resp.GetNextPageToken()}, nil
}

// AllJobs iterates every job the query matches, fetching pages as it goes.
//
// The iteration stops at the first error, which is yielded with a zero Job.
// A caller that breaks early simply stops fetching.
//
//	for job, err := range client.AllJobs(ctx, flexiq.ListJobsQuery{Queue: "payments"}) {
//		if err != nil {
//			return err
//		}
//		...
//	}
func (c *Client) AllJobs(ctx context.Context, query ListJobsQuery) iter.Seq2[Job, error] {
	return func(yield func(Job, error) bool) {
		for {
			page, err := c.ListJobs(ctx, query)
			if err != nil {
				yield(Job{}, err)
				return
			}
			for _, job := range page.Jobs {
				if !yield(job, nil) {
					return
				}
			}
			if page.NextPageToken == "" {
				return
			}
			query.PageToken = page.NextPageToken
		}
	}
}

// QueueStats counts jobs per status. An empty queue counts every queue in the
// namespace.
func (c *Client) QueueStats(ctx context.Context, queue string) (QueueStats, error) {
	req := &pb.QueueStatsRequest{}
	if queue != "" {
		req.Queue = &queue
	}

	resp, err := c.producer.QueueStats(ctx, req)
	if err != nil {
		return QueueStats{}, fromRPC(err)
	}
	return QueueStats{
		Pending:   resp.GetPending(),
		Running:   resp.GetRunning(),
		Completed: resp.GetCompleted(),
		Failed:    resp.GetFailed(),
		Dead:      resp.GetDead(),
		Cancelled: resp.GetCancelled(),
	}, nil
}
