package provider

import (
	"context"
	"fmt"
	"time"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	"github.com/ByteVeda/flexiq/sdks/go/v2/admin"
)

// fakeAdmin is an in-memory admin door that answers the way flexiq-server
// does: reads of an unknown queue or an unset quota are empty, never
// not-found, and a quota with every limit unset is cleared rather than stored.
type fakeAdmin struct {
	overrides map[string]admin.QueueOverride
	paused    map[string]bool
	quota     *admin.NamespaceQuota
	periodic  map[string]admin.PeriodicTask
	tokens    map[string]admin.Token
	minted    int
	// clock stamps minted tokens.
	clock func() time.Time

	// calls records every method in order, for asserting which RPCs ran.
	calls []string
	// fail makes the named method answer this error.
	fail map[string]error
}

var _ adminAPI = (*fakeAdmin)(nil)

func newFakeAdmin() *fakeAdmin {
	return &fakeAdmin{
		overrides: map[string]admin.QueueOverride{},
		paused:    map[string]bool{},
		periodic:  map[string]admin.PeriodicTask{},
		tokens:    map[string]admin.Token{},
		clock:     time.Now,
		fail:      map[string]error{},
	}
}

func (f *fakeAdmin) record(method string) error {
	f.calls = append(f.calls, method)
	return f.fail[method]
}

func (f *fakeAdmin) GetQueue(_ context.Context, name string) (admin.Queue, bool, error) {
	if err := f.record("GetQueue"); err != nil {
		return admin.Queue{}, false, err
	}
	_, overridden := f.overrides[name]
	paused, seen := f.paused[name]
	if !overridden && !seen {
		return admin.Queue{}, false, nil
	}
	return admin.Queue{Name: name, Paused: paused}, true, nil
}

func (f *fakeAdmin) PauseQueue(_ context.Context, name string) (admin.Queue, error) {
	if err := f.record("PauseQueue"); err != nil {
		return admin.Queue{}, err
	}
	f.paused[name] = true
	return admin.Queue{Name: name, Paused: true}, nil
}

func (f *fakeAdmin) ResumeQueue(_ context.Context, name string) (admin.Queue, error) {
	if err := f.record("ResumeQueue"); err != nil {
		return admin.Queue{}, err
	}
	f.paused[name] = false
	return admin.Queue{Name: name}, nil
}

func (f *fakeAdmin) GetQueueOverride(_ context.Context, queue string) (admin.QueueOverride, bool, error) {
	if err := f.record("GetQueueOverride"); err != nil {
		return admin.QueueOverride{}, false, err
	}
	override, ok := f.overrides[queue]
	return override, ok, nil
}

func (f *fakeAdmin) SetQueueOverride(_ context.Context, queue string, override admin.QueueOverride) (admin.QueueOverride, error) {
	if err := f.record("SetQueueOverride"); err != nil {
		return admin.QueueOverride{}, err
	}
	f.overrides[queue] = override
	return override, nil
}

func (f *fakeAdmin) ClearQueueOverride(_ context.Context, queue string) error {
	if err := f.record("ClearQueueOverride"); err != nil {
		return err
	}
	delete(f.overrides, queue)
	return nil
}

func (f *fakeAdmin) GetNamespaceQuota(context.Context) (admin.NamespaceQuota, error) {
	if err := f.record("GetNamespaceQuota"); err != nil {
		return admin.NamespaceQuota{}, err
	}
	if f.quota == nil {
		return admin.NamespaceQuota{OnExcess: admin.OverflowReject}, nil
	}
	return *f.quota, nil
}

func (f *fakeAdmin) SetNamespaceQuota(_ context.Context, quota admin.NamespaceQuota) (admin.NamespaceQuota, error) {
	if err := f.record("SetNamespaceQuota"); err != nil {
		return admin.NamespaceQuota{}, err
	}
	// The server stores unspecified as reject, and an unlimited quota not at all.
	if quota.OnExcess == admin.OverflowUnspecified {
		quota.OnExcess = admin.OverflowReject
	}
	if quota.IsZero() {
		f.quota = nil
	} else {
		f.quota = &quota
	}
	return quota, nil
}

func (f *fakeAdmin) ClearNamespaceQuota(context.Context) error {
	if err := f.record("ClearNamespaceQuota"); err != nil {
		return err
	}
	f.quota = nil
	return nil
}

func periodicNotFound(name string) error {
	return fmt.Errorf("periodic task %q: %w", name, flexiq.ReasonPeriodicTaskNotFound)
}

func (f *fakeAdmin) GetPeriodicTask(_ context.Context, name string, opts admin.GetPeriodicTaskOptions) (admin.PeriodicTask, error) {
	if err := f.record("GetPeriodicTask"); err != nil {
		return admin.PeriodicTask{}, err
	}
	task, ok := f.periodic[name]
	if !ok {
		return admin.PeriodicTask{}, periodicNotFound(name)
	}
	if !opts.IncludePayload {
		task.Payload = nil
	}
	return task, nil
}

func (f *fakeAdmin) PutPeriodicTask(_ context.Context, spec admin.PeriodicTaskSpec) (admin.PeriodicTask, error) {
	if err := f.record("PutPeriodicTask"); err != nil {
		return admin.PeriodicTask{}, err
	}
	payload, err := flexiq.EncodeCall(spec.Args, spec.Kwargs)
	if err != nil {
		return admin.PeriodicTask{}, err
	}
	// The server stores an empty queue as "default", and a replace keeps the
	// pause state whatever StartPaused says.
	queue := spec.Queue
	if queue == "" {
		queue = defaultQueue
	}
	enabled := !spec.StartPaused
	if existing, ok := f.periodic[spec.Name]; ok {
		enabled = existing.Enabled
	}
	task := admin.PeriodicTask{
		Name: spec.Name, TaskName: spec.Task, Cron: spec.Cron, Queue: queue,
		Timezone: spec.Timezone, Enabled: enabled, Payload: payload,
	}
	f.periodic[spec.Name] = task
	task.Payload = nil
	return task, nil
}

func (f *fakeAdmin) DeletePeriodicTask(_ context.Context, name string) error {
	if err := f.record("DeletePeriodicTask"); err != nil {
		return err
	}
	if _, ok := f.periodic[name]; !ok {
		return periodicNotFound(name)
	}
	delete(f.periodic, name)
	return nil
}

func (f *fakeAdmin) setPeriodicEnabled(method, name string, enabled bool) (admin.PeriodicTask, error) {
	if err := f.record(method); err != nil {
		return admin.PeriodicTask{}, err
	}
	task, ok := f.periodic[name]
	if !ok {
		return admin.PeriodicTask{}, periodicNotFound(name)
	}
	task.Enabled = enabled
	f.periodic[name] = task
	task.Payload = nil
	return task, nil
}

func tokenNotFound(id string) error {
	return fmt.Errorf("token %q: %w", id, flexiq.ReasonTokenNotFound)
}

func (f *fakeAdmin) CreateToken(_ context.Context, req admin.CreateTokenRequest) (admin.CreatedToken, error) {
	if err := f.record("CreateToken"); err != nil {
		return admin.CreatedToken{}, err
	}
	f.minted++
	now := f.clock()
	token := admin.Token{
		ID:        fmt.Sprintf("tok-%d", f.minted),
		Name:      req.Name,
		Scopes:    req.Scopes,
		Namespace: "ns",
		CreatedAt: now,
		ExpiresAt: now.Add(time.Duration(req.ExpireDays) * day),
		Status:    admin.TokenStatusActive,
	}
	f.tokens[token.ID] = token
	return admin.CreatedToken{Token: token, Secret: fmt.Sprintf("fqt_secret-%d", f.minted)}, nil
}

func (f *fakeAdmin) GetToken(_ context.Context, id string) (admin.Token, error) {
	if err := f.record("GetToken"); err != nil {
		return admin.Token{}, err
	}
	token, ok := f.tokens[id]
	if !ok {
		return admin.Token{}, tokenNotFound(id)
	}
	return token, nil
}

func (f *fakeAdmin) RevokeToken(_ context.Context, id string) (admin.Token, error) {
	if err := f.record("RevokeToken"); err != nil {
		return admin.Token{}, err
	}
	token, ok := f.tokens[id]
	if !ok {
		return admin.Token{}, tokenNotFound(id)
	}
	token.Status = admin.TokenStatusRevoked
	f.tokens[id] = token
	return token, nil
}

func (f *fakeAdmin) PausePeriodicTask(_ context.Context, name string) (admin.PeriodicTask, error) {
	return f.setPeriodicEnabled("PausePeriodicTask", name, false)
}

func (f *fakeAdmin) ResumePeriodicTask(_ context.Context, name string) (admin.PeriodicTask, error) {
	return f.setPeriodicEnabled("ResumePeriodicTask", name, true)
}
