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
