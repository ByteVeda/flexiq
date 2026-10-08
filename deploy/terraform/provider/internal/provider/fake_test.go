package provider

import (
	"context"

	"github.com/ByteVeda/flexiq/sdks/go/v2/admin"
)

// fakeAdmin is an in-memory admin door that answers the way flexiq-server
// does: reads of an unknown queue or an unset quota are empty, never
// not-found, and a quota with every limit unset is cleared rather than stored.
type fakeAdmin struct {
	overrides map[string]admin.QueueOverride
	paused    map[string]bool
	quota     *admin.NamespaceQuota

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
