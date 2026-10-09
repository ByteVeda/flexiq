package tests

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	"github.com/ByteVeda/flexiq/sdks/go/v2/admin"
	adminv1 "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/admin/v1"
)

// capturePut answers every PutPeriodicTask with the task it was asked to write
// and keeps the request.
func capturePut(t *testing.T, into **adminv1.PutPeriodicTaskRequest) *admin.Client {
	t.Helper()
	return serveAdmin(t, &fakeAdmin{
		putPeriodic: func(_ context.Context, req *adminv1.PutPeriodicTaskRequest) (*adminv1.PutPeriodicTaskResponse, error) {
			*into = req
			return &adminv1.PutPeriodicTaskResponse{PeriodicTask: &adminv1.PeriodicTask{
				Name: req.GetName(), TaskName: req.GetTaskName(), Cron: req.GetCron(), Enabled: !req.GetStartPaused(),
			}}, nil
		},
	})
}

// TestPutPeriodicTaskEncodesArgsAsEnqueueDoes: a firing must carry exactly
// what an enqueue of the same call would, so the body is EncodeCall's bytes on
// the raw arm.
func TestPutPeriodicTaskEncodesArgsAsEnqueueDoes(t *testing.T) {
	var got *adminv1.PutPeriodicTaskRequest
	client := capturePut(t, &got)

	args := []any{map[string]any{"report": "daily"}, int64(7)}
	kwargs := map[string]any{"dry_run": true}
	task, err := client.PutPeriodicTask(context.Background(), admin.PeriodicTaskSpec{
		Name:        "nightly",
		Task:        "reports.build",
		Cron:        "0 0 2 * * *",
		Queue:       "reports",
		Timezone:    "Europe/Berlin",
		Args:        args,
		Kwargs:      kwargs,
		StartPaused: true,
	})
	if err != nil {
		t.Fatalf("PutPeriodicTask: %v", err)
	}

	want, err := flexiq.EncodeCall(args, kwargs)
	if err != nil {
		t.Fatalf("EncodeCall: %v", err)
	}
	if !bytes.Equal(got.GetRaw(), want) {
		t.Errorf("raw body is %x, want EncodeCall's %x", got.GetRaw(), want)
	}
	if got.GetName() != "nightly" || got.GetTaskName() != "reports.build" || got.GetQueue() != "reports" {
		t.Errorf("request is %v", got)
	}
	if got.Timezone == nil || got.GetTimezone() != "Europe/Berlin" || !got.GetStartPaused() {
		t.Errorf("timezone or start_paused did not reach the wire: %v", got)
	}
	if task.Name != "nightly" || task.Enabled {
		t.Errorf("answered task is %+v", task)
	}
}

// TestPutPeriodicTaskWithoutArgsSendsAnEmptyCall: no arguments is a call with
// no arguments, encoded as one, and an empty timezone is left unset (UTC).
func TestPutPeriodicTaskWithoutArgsSendsAnEmptyCall(t *testing.T) {
	var got *adminv1.PutPeriodicTaskRequest
	client := capturePut(t, &got)

	if _, err := client.PutPeriodicTask(context.Background(), admin.PeriodicTaskSpec{
		Name: "tick", Task: "t", Cron: "* * * * * *",
	}); err != nil {
		t.Fatalf("PutPeriodicTask: %v", err)
	}

	want, err := flexiq.EncodeCall(nil, nil)
	if err != nil {
		t.Fatalf("EncodeCall: %v", err)
	}
	if !bytes.Equal(got.GetRaw(), want) {
		t.Errorf("raw body is %x, want the empty call %x", got.GetRaw(), want)
	}
	if got.Timezone != nil {
		t.Errorf("an empty timezone went out as %q", got.GetTimezone())
	}
}

// TestPutPeriodicTaskPassesRawThrough: a pre-encoded envelope reaches the
// server untouched, and wins over Args.
func TestPutPeriodicTaskPassesRawThrough(t *testing.T) {
	var got *adminv1.PutPeriodicTaskRequest
	client := capturePut(t, &got)

	raw := mustHex(t, "028281016161a0")
	if _, err := client.PutPeriodicTask(context.Background(), admin.PeriodicTaskSpec{
		Name: "p", Task: "t", Cron: "* * * * * *", Raw: raw, Args: []any{"ignored"},
	}); err != nil {
		t.Fatalf("PutPeriodicTask: %v", err)
	}
	if !bytes.Equal(got.GetRaw(), raw) {
		t.Errorf("raw body is %x, want %x", got.GetRaw(), raw)
	}
}

// TestPutPeriodicTaskRefusesAMissingName fails locally rather than spending a
// round trip on a request the server refuses.
func TestPutPeriodicTaskRefusesAMissingName(t *testing.T) {
	fake := &fakeAdmin{}
	client := serveAdmin(t, fake)

	if _, err := client.PutPeriodicTask(context.Background(), admin.PeriodicTaskSpec{Task: "t"}); err == nil {
		t.Error("a nameless periodic task was accepted")
	}
	if _, err := client.PutPeriodicTask(context.Background(), admin.PeriodicTaskSpec{Name: "p"}); err == nil {
		t.Error("a periodic task with no task name was accepted")
	}
	if fake.calls != 0 {
		t.Errorf("%d requests reached the server", fake.calls)
	}
}

// TestGetPeriodicTaskReadsThePayloadBack is how a caller detects drift: ask for
// the payload, decode it, compare it to the definition.
func TestGetPeriodicTaskReadsThePayloadBack(t *testing.T) {
	payload, err := flexiq.EncodeCall([]any{"a", int64(2)}, nil)
	if err != nil {
		t.Fatalf("EncodeCall: %v", err)
	}
	nextRun := time.Date(2026, 10, 9, 2, 0, 0, 0, time.UTC)

	var got *adminv1.GetPeriodicTaskRequest
	client := serveAdmin(t, &fakeAdmin{
		getPeriodic: func(_ context.Context, req *adminv1.GetPeriodicTaskRequest) (*adminv1.GetPeriodicTaskResponse, error) {
			got = req
			task := &adminv1.PeriodicTask{
				Name:     req.GetName(),
				TaskName: "reports.build",
				Cron:     "0 0 2 * * *",
				Enabled:  true,
				NextRun:  timestamppb.New(nextRun),
			}
			if req.GetIncludePayload() {
				task.Payload = payload
			}
			return &adminv1.GetPeriodicTaskResponse{PeriodicTask: task}, nil
		},
	})
	ctx := context.Background()

	plain, err := client.GetPeriodicTask(ctx, "nightly", admin.GetPeriodicTaskOptions{})
	if err != nil {
		t.Fatalf("GetPeriodicTask: %v", err)
	}
	if got.GetIncludePayload() || plain.Payload != nil {
		t.Error("a plain read asked for or carried the payload")
	}
	if !plain.NextRun.Equal(nextRun) || !plain.LastRun.IsZero() {
		t.Errorf("next run %v, last run %v; want %v and zero", plain.NextRun, plain.LastRun, nextRun)
	}

	full, err := client.GetPeriodicTask(ctx, "nightly", admin.GetPeriodicTaskOptions{IncludePayload: true})
	if err != nil {
		t.Fatalf("GetPeriodicTask: %v", err)
	}
	if !got.GetIncludePayload() {
		t.Error("asking for the payload did not reach the wire")
	}
	call, err := full.DecodePayload()
	if err != nil {
		t.Fatalf("DecodePayload: %v", err)
	}
	if len(call.Args) != 2 || call.Args[0] != "a" {
		t.Errorf("decoded args are %#v", call.Args)
	}
}

// TestPeriodicTaskNotFoundIsDistinguishable: the read a declarative caller
// makes to learn a resource is gone must be told apart from every other
// failure.
func TestPeriodicTaskNotFoundIsDistinguishable(t *testing.T) {
	client := serveAdmin(t, &fakeAdmin{
		getPeriodic: func(context.Context, *adminv1.GetPeriodicTaskRequest) (*adminv1.GetPeriodicTaskResponse, error) {
			return nil, notFound(t, flexiq.ReasonPeriodicTaskNotFound)
		},
		deletePeriodic: func(context.Context, *adminv1.DeletePeriodicTaskRequest) (*adminv1.DeletePeriodicTaskResponse, error) {
			return nil, notFound(t, flexiq.ReasonPeriodicTaskNotFound)
		},
	})
	ctx := context.Background()

	_, err := client.GetPeriodicTask(ctx, "gone", admin.GetPeriodicTaskOptions{})
	if !errors.Is(err, flexiq.ReasonPeriodicTaskNotFound) {
		t.Errorf("GetPeriodicTask: want ReasonPeriodicTaskNotFound, got %v", err)
	}
	if err := client.DeletePeriodicTask(ctx, "gone"); !errors.Is(err, flexiq.ReasonPeriodicTaskNotFound) {
		t.Errorf("DeletePeriodicTask: want ReasonPeriodicTaskNotFound, got %v", err)
	}
}

// TestListPeriodicTasksNarrowsOnlyWhenAsked: an empty filter field is left
// unset, because a set one is a narrowing the server checks grants against.
func TestListPeriodicTasksNarrowsOnlyWhenAsked(t *testing.T) {
	var got *adminv1.ListPeriodicTasksRequest
	client := serveAdmin(t, &fakeAdmin{
		listPeriodic: func(_ context.Context, req *adminv1.ListPeriodicTasksRequest) (*adminv1.ListPeriodicTasksResponse, error) {
			got = req
			return &adminv1.ListPeriodicTasksResponse{PeriodicTasks: []*adminv1.PeriodicTask{{Name: "a"}, {Name: "b"}}}, nil
		},
		pausePeriodic: func(_ context.Context, req *adminv1.PausePeriodicTaskRequest) (*adminv1.PausePeriodicTaskResponse, error) {
			return &adminv1.PausePeriodicTaskResponse{PeriodicTask: &adminv1.PeriodicTask{Name: req.GetName()}}, nil
		},
	})
	ctx := context.Background()

	tasks, err := client.ListPeriodicTasks(ctx, admin.ListPeriodicTasksQuery{})
	if err != nil {
		t.Fatalf("ListPeriodicTasks: %v", err)
	}
	if got.Queue != nil || got.TaskName != nil || len(tasks) != 2 {
		t.Errorf("unfiltered list sent %v and answered %d tasks", got, len(tasks))
	}

	if _, err = client.ListPeriodicTasks(ctx, admin.ListPeriodicTasksQuery{Queue: "reports"}); err != nil {
		t.Fatalf("ListPeriodicTasks: %v", err)
	}
	if got.GetQueue() != "reports" || got.TaskName != nil {
		t.Errorf("queue filter sent %v", got)
	}

	paused, err := client.PausePeriodicTask(ctx, "a")
	if err != nil || paused.Name != "a" || paused.Enabled {
		t.Errorf("PausePeriodicTask = %+v, %v", paused, err)
	}
}
