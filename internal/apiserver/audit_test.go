package apiserver

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/volume"
)

// A copy of the database holds the panel's secrets: root's alone, as
// those secrets are.
func TestOnlyRootTakesACopyOfTheDatabase(t *testing.T) {
	f := newAPIFixture(t)
	secret, err := f.store.PanelSecret("ceremonies")
	if err != nil {
		t.Fatal(err)
	}
	someone := context.WithValue(context.Background(), uidKey{}, agentUID+1)
	backup := httptest.NewRecorder()
	f.handler.ServeHTTP(backup, httptest.NewRequest("GET", "/v1/backup", nil).WithContext(someone))
	if backup.Code != http.StatusForbidden || bytes.Contains(backup.Body.Bytes(), []byte(secret)) {
		t.Fatalf("a copy for someone not root: %d", backup.Code)
	}
}

func TestARequestIsOneJSONValueOfABoundedSize(t *testing.T) {
	var app spec.Tenant
	for _, body := range []string{
		`{"image":"busybox","user":"1000:1000"}` + strings.Repeat("x", 2<<20),
		`{"image":"busybox","user":"1000:1000"} {"image":"other"}`,
		`{"image":"busybox","user":"1000:1000","env":{"A":"` + strings.Repeat("x", 2<<20) + `"}}`,
	} {
		if readJSON(httptest.NewRecorder(), httptest.NewRequest("POST", "/", strings.NewReader(body)), &app) {
			t.Errorf("taken: %.60s…", body)
		}
	}
	if !readJSON(httptest.NewRecorder(), httptest.NewRequest("POST", "/", strings.NewReader(`{"image":"busybox"}`+"\n")), &app) {
		t.Fatal("one value refused")
	}
}

// A client that gave up while the restore waited for a reconcile pass
// restores nothing, and holds nothing waiting.
func TestARestoreGivenUpRestoresNothing(t *testing.T) {
	f := withApp(t)
	if _, err := f.client.Create(context.Background(), tenantOf("acme")); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Archive("acme", "test"); err != nil {
		t.Fatal(err)
	}
	held, release, unlocked := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(unlocked)
		_ = f.reconciler.Exclusive(func() error { close(held); <-release; return nil })
	}()
	<-held
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/tenants/acme/restore", nil).WithContext(ctx))
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the request kept waiting for the lock")
	}
	close(release)
	<-unlocked
	if record, err := f.store.Get("acme"); err != nil || !record.Archived() {
		t.Fatalf("restored by a client that gave up: %+v, %v", record, err)
	}
}

func TestACPULimitDockerCannotTakeIsRefused(t *testing.T) {
	for _, cpus := range []float64{1e-12, 0.001, 0.0099} {
		tenant := spec.Tenant{Name: "acme", Image: "busybox", User: "1000:1000", Limits: spec.Limits{CPUs: cpus}}.WithDefaults()
		if err := tenant.ValidateUnpinned(); err == nil {
			t.Errorf("cpus %g taken", cpus)
		}
	}
	tenant := spec.Tenant{Name: "acme", Image: "busybox", User: "1000:1000", Limits: spec.Limits{CPUs: 0.01}}.WithDefaults()
	if err := tenant.ValidateUnpinned(); err != nil {
		t.Fatal(err)
	}
}

// A client that gave up before its change was committed has nothing
// changed: whatever it waited for.
func TestAChangeGivenUpChangesNothing(t *testing.T) {
	f := withApp(t)
	if _, err := f.client.Create(context.Background(), tenantOf("acme")); err != nil {
		t.Fatal(err)
	}
	gone, cancel := context.WithCancel(context.Background())
	cancel()
	for _, request := range []struct{ method, path, body string }{
		{"POST", "/v1/tenants/acme/suspend", ""},
		{"POST", "/v1/tenants/acme/archive", ""},
		{"POST", "/v1/tenants", `{"name":"beta","app":"web"}`},
	} {
		w := httptest.NewRecorder()
		f.handler.ServeHTTP(w, httptest.NewRequest(request.method, request.path, strings.NewReader(request.body)).WithContext(gone))
		if w.Code != 499 {
			t.Errorf("%s %s by a client gone: %d %s", request.method, request.path, w.Code, w.Body)
		}
	}
	record, err := f.store.Get("acme")
	if err != nil || record.Spec.Suspended || record.Archived() {
		t.Fatalf("changed for a client gone: %+v %v", record, err)
	}
	if _, err := f.store.Get("beta"); err == nil {
		t.Fatal("created for a client gone")
	}
}

// A purge whose client gave up while it waited for a reconcile pass
// deletes nothing.
func TestAPurgeGivenUpWhileWaitingDeletesNothing(t *testing.T) {
	f := withApp(t)
	if _, err := f.client.Create(context.Background(), tenantOf("acme")); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Archive("acme", "test"); err != nil {
		t.Fatal(err)
	}
	held, release, unlocked := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(unlocked)
		_ = f.reconciler.Exclusive(func() error { close(held); <-release; return nil })
	}()
	<-held
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() {
		w := httptest.NewRecorder()
		f.handler.ServeHTTP(w, httptest.NewRequest("DELETE", "/v1/tenants/acme", nil).WithContext(ctx))
		done <- w.Code
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)
	close(release)
	<-unlocked
	select {
	case code := <-done:
		if code != 499 {
			t.Errorf("a purge given up: %d", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the purge kept waiting")
	}
	if _, err := f.store.Get("acme"); err != nil {
		t.Fatalf("purged for a client gone: %v", err)
	}
}

// slowLook is a data directory whose look for a tenant's data waits until
// let goes.
type slowLook struct {
	volume.Dir
	looking chan struct{}
	let     chan struct{}
}

func (d slowLook) Exists(ctx context.Context, tenant string) (bool, error) {
	select {
	case d.looking <- struct{}{}:
	default:
	}
	<-d.let
	return d.Dir.Exists(ctx, tenant)
}

// A client that gave up while its creation looked for leftover data has
// nothing created.
func TestACreationGivenUpWhileLookingForDataCreatesNothing(t *testing.T) {
	look := slowLook{looking: make(chan struct{}, 1), let: make(chan struct{})}
	f := newAPIFixtureWith(t, func(d volume.Dir) volume.Volumes { look.Dir = d; return look })
	if _, err := f.client.PutApp(context.Background(), "web", appSpec()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() {
		w := httptest.NewRecorder()
		f.handler.ServeHTTP(w, httptest.NewRequest("POST", "/v1/tenants", strings.NewReader(`{"name":"acme","app":"web"}`)).WithContext(ctx))
		done <- w.Code
	}()
	<-look.looking
	cancel()
	close(look.let)
	if code := <-done; code != 499 {
		t.Errorf("a creation given up: %d", code)
	}
	if _, err := f.store.Get("acme"); err == nil {
		t.Fatal("created for a client gone")
	}
}
