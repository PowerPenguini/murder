package apiserver

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/access"
	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
)

// accessesOf are the register's entries of tenant (and of every tenant).
func accessesOf(t *testing.T, f *apiFixture, tenant string) []access.Entry {
	t.Helper()
	entries, err := f.client.Accesses(context.Background(), tenant, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

// A secret is read only once the read is recorded: who, which tenant and
// secret, why, at whose request.
func TestASecretIsReadOnlyRecorded(t *testing.T) {
	f := withApp(t)
	ctx := context.Background()
	if _, err := f.client.Create(ctx, tenantOf("acme")); err != nil {
		t.Fatal(err)
	}
	value, err := f.client.Secret(ctx, "acme", "SESSION_SECRET", access.Proof{})
	checkStatus(t, "a read that says not why", err, http.StatusForbidden)
	if value != "" || err == nil || !strings.Contains(err.Error(), "-reason") {
		t.Fatalf("read without a reason: %q %v", value, err)
	}
	if _, err := f.client.Secret(ctx, "acme", "SESSION_SECRET", access.Proof{Purpose: "login fails"}); err == nil || !strings.Contains(err.Error(), "-ticket") {
		t.Fatalf("read without a ticket: %v", err)
	}
	if got := accessesOf(t, f, "acme"); len(got) != 0 {
		t.Fatalf("refused reads recorded: %+v", got)
	}
	// A secret it has not: refused before anything is recorded.
	_, err = f.client.Secret(ctx, "acme", "NOPE", access.Proof{Purpose: "x", Ticket: "ZD-1"})
	checkStatus(t, "a secret it has not", err, http.StatusNotFound)
	value, err = f.client.WithActor("jan").Secret(ctx, "acme", "SESSION_SECRET", access.Proof{Purpose: "login fails", Ticket: "ZD-1234"})
	if err != nil || len(value) != 64 {
		t.Fatalf("read: %q %v", value, err)
	}
	got := accessesOf(t, f, "acme")
	if len(got) != 1 || got[0].Kind != access.Secret || got[0].What != "SESSION_SECRET" || got[0].Tenant != "acme" ||
		got[0].Purpose != "login fails" || got[0].Ticket != "ZD-1234" || got[0].Actor != "jan via unknown" {
		t.Fatalf("recorded: %+v", got)
	}
	events, _ := f.client.Events(ctx, store.EventQuery{Tenant: "acme", Limit: 1})
	if events[0].Action != "access.secret" || events[0].ID != got[0].ID {
		t.Fatalf("the audit log: %+v", events)
	}
}

// unrecordingStore is a store whose register records nothing, as one
// without a quorum.
type unrecordingStore struct{ store.Store }

func (unrecordingStore) Access(string, string, string, string, bool) (store.Event, error) {
	return store.Event{}, errors.New("no leader")
}

// What the register cannot record is not read: no secret, no logs, no
// copy of the state.
func TestAReadTheRegisterCannotRecordIsRefused(t *testing.T) {
	f := withApp(t)
	ctx := context.Background()
	if _, err := f.client.Create(ctx, tenantOf("acme")); err != nil {
		t.Fatal(err)
	}
	_ = f.client.Reconcile(ctx)
	broken := NewServer(unrecordingStore{f.store}, f.reconciler, f.runtime, f.server.key, f.server.log)
	server := httptest.NewServer(broken.Handler())
	t.Cleanup(server.Close)
	client := api.NewClientURL(server.URL)
	proof := access.Proof{Purpose: "x", Ticket: "incident"}
	if value, err := client.Secret(ctx, "acme", "SESSION_SECRET", proof); err == nil || value != "" || !strings.Contains(err.Error(), "could not record") {
		t.Fatalf("a secret read unrecorded: %q %v", value, err)
	}
	if logs, err := client.Logs(ctx, "acme", 10, proof); err == nil || len(logs) != 0 {
		t.Fatalf("logs read unrecorded: %q %v", logs, err)
	}
	var copy bytes.Buffer
	if err := client.Backup(ctx, &copy, proof); err == nil || copy.Len() != 0 {
		t.Fatalf("a copy of the state unrecorded: %d bytes, %v", copy.Len(), err)
	}
	if _, err := client.RecordAccess(ctx, access.Request{Tenant: "acme", Kind: access.Manual, What: "docker exec", Purpose: "x", Ticket: "incident"}); err == nil {
		t.Fatal("an access said recorded")
	}
}

// An access recorded before serves the same person's reads of the same
// thing for an hour, and nothing else.
func TestAGrantServesItsOwnReadsForAnHour(t *testing.T) {
	f := withApp(t)
	ctx := context.Background()
	if _, err := f.client.Create(ctx, tenantOf("acme")); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.Create(ctx, tenantOf("globex")); err != nil {
		t.Fatal(err)
	}
	_ = f.client.Reconcile(ctx)
	now := time.Now()
	f.server.clock = func() time.Time { return now }
	jan := f.client.WithActor("jan")
	entry, err := jan.RecordAccess(ctx, access.Request{Tenant: "acme", Kind: access.Logs, Purpose: "export fails", Ticket: "ZD-1"})
	if err != nil || entry.ID == 0 || entry.Actor != "jan via unknown" {
		t.Fatalf("recorded: %+v %v", entry, err)
	}
	grant := access.Proof{Grant: entry.ID}
	for range 3 {
		if logs, err := jan.Logs(ctx, "acme", 10, grant); err != nil || string(logs) != "logs of c-acme\n" {
			t.Fatalf("read under the grant: %q %v", logs, err)
		}
	}
	if got := accessesOf(t, f, "acme"); len(got) != 1 {
		t.Fatalf("reads under a grant recorded again: %d", len(got))
	}
	for name, read := range map[string]func() error{
		"another person":    func() error { _, err := f.client.WithActor("anna").Logs(ctx, "acme", 10, grant); return err },
		"another tenant":    func() error { _, err := jan.Logs(ctx, "globex", 10, grant); return err },
		"a secret":          func() error { _, err := jan.Secret(ctx, "acme", "SESSION_SECRET", grant); return err },
		"the state":         func() error { return jan.Backup(ctx, &bytes.Buffer{}, grant) },
		"an unknown access": func() error { _, err := jan.Logs(ctx, "acme", 10, access.Proof{Grant: entry.ID + 1000}); return err },
	} {
		checkStatus(t, name, read(), http.StatusForbidden)
	}
	// Another event is no access.
	events, _ := f.client.Events(ctx, store.EventQuery{Tenant: "globex", Limit: 1})
	_, err = jan.Logs(ctx, "globex", 10, access.Proof{Grant: events[0].ID})
	checkStatus(t, "an event that is no access", err, http.StatusForbidden)
	now = now.Add(access.GrantFor + time.Second)
	_, err = jan.Logs(ctx, "acme", 10, grant)
	checkStatus(t, "an ended grant", err, http.StatusForbidden)
	if err == nil || !strings.Contains(err.Error(), "say why again") {
		t.Fatalf("an ended grant: %v", err)
	}
}

// The previous container's lines are logs too.
func TestPreviousLinesAreRecorded(t *testing.T) {
	f := withApp(t)
	ctx := context.Background()
	if _, err := f.client.Create(ctx, tenantOf("acme")); err != nil {
		t.Fatal(err)
	}
	_ = f.client.Reconcile(ctx)
	if _, err := f.client.Roll(ctx, "acme"); err != nil {
		t.Fatal(err)
	}
	_ = f.client.Reconcile(ctx)
	if _, err := f.client.PreviousLogs(ctx, "acme", access.Proof{}); err == nil {
		t.Fatal("previous lines read unrecorded")
	}
	if _, err := f.client.PreviousLogs(ctx, "acme", access.Proof{Purpose: "why it exited", Ticket: "incident"}); err != nil {
		t.Fatal(err)
	}
	if got := accessesOf(t, f, "acme"); len(got) != 1 || got[0].Kind != access.Logs || got[0].Ticket != "incident" {
		t.Fatalf("recorded: %+v", got)
	}
}

// A copy of the whole state is every tenant's: recorded as such, and the
// timer's own copy, kept on the node, is not an access.
func TestACopyOfTheStateIsRecorded(t *testing.T) {
	f := withApp(t)
	ctx := context.Background()
	if _, err := f.client.Create(ctx, tenantOf("acme")); err != nil {
		t.Fatal(err)
	}
	var copy bytes.Buffer
	checkStatus(t, "a copy unrecorded", f.client.Backup(ctx, &copy, access.Proof{}), http.StatusForbidden)
	// What once let the timer's copy through hands nothing out now.
	recorder := httptest.NewRecorder()
	f.handler.ServeHTTP(recorder, httptest.NewRequest("GET", "/v1/backup?scheduled=1", nil))
	if recorder.Code != http.StatusForbidden || recorder.Body.Len() > 1000 {
		t.Fatalf("a copy said to be scheduled: %d, %d bytes", recorder.Code, recorder.Body.Len())
	}
	if copy.Len() != 0 {
		t.Fatal("a refused copy sent")
	}
	// The timer's copy the agent keeps itself, root's alone; it hands
	// nothing out, and is recorded nowhere.
	dir := t.TempDir()
	f.server.WithBackupDir(dir)
	kept, err := f.client.ScheduledBackup(ctx, 2)
	if err != nil || filepath.Dir(kept.Path) != dir || kept.Tenants != 1 || kept.Node != "test-node" {
		t.Fatalf("the scheduled copy: %+v %v", kept, err)
	}
	if info, err := os.Stat(kept.Path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("the copy: %v %v", info, err)
	}
	if got := accessesOf(t, f, ""); len(got) != 0 {
		t.Fatalf("the scheduled copy recorded: %+v", got)
	}
	someone := context.WithValue(context.Background(), uidKey{}, agentUID+1)
	recorder = httptest.NewRecorder()
	f.handler.ServeHTTP(recorder, httptest.NewRequest("POST", "/v1/backup/scheduled", nil).WithContext(someone))
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("a scheduled copy by someone not root: %d", recorder.Code)
	}
	copy.Reset()
	if err := f.client.Backup(ctx, &copy, access.Proof{Purpose: "move to new hardware", Ticket: "incident:INC-7"}); err != nil || copy.Len() == 0 {
		t.Fatalf("a copy by hand: %v", err)
	}
	// It shows in every tenant's extract.
	got := accessesOf(t, f, "acme")
	if len(got) != 1 || got[0].Kind != access.State || got[0].Tenant != "" || got[0].Ticket != "incident:INC-7" {
		t.Fatalf("recorded: %+v", got)
	}
}

// fakeBackups puts nothing back; it says what it was asked.
type fakeBackups struct{ replaced []string }

func (f *fakeBackups) RunNow(context.Context) ([]api.BackupResult, error) { return nil, nil }

func (f *fakeBackups) ReplaceTenant(_ context.Context, tenant, copy string) (api.ReplaceResult, error) {
	f.replaced = append(f.replaced, tenant)
	return api.ReplaceResult{}, nil
}

// A restore puts its backup back only under the entry it recorded before
// reading it, which serves a day: a big backup takes hours.
func TestARestoreNamesItsEntry(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	backups := &fakeBackups{}
	f.server.WithBackups(backups)
	now := time.Now()
	f.server.clock = func() time.Time { return now }
	_, err := f.client.ReplaceTenant(ctx, "acme", "murder-copy-acme-restore-1", 0)
	checkStatus(t, "a restore with no entry", err, http.StatusForbidden)
	logs, err := f.client.RecordAccess(ctx, access.Request{Tenant: "acme", Kind: access.Logs, Purpose: "x", Ticket: "ZD-1"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.client.ReplaceTenant(ctx, "acme", "murder-copy-acme-restore-1", logs.ID)
	checkStatus(t, "a restore under a read of logs", err, http.StatusForbidden)
	restore, err := f.client.RecordAccess(ctx, access.Request{Tenant: "acme", Kind: access.BackupRestore, Purpose: "data lost", Ticket: "ZD-2"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.client.ReplaceTenant(ctx, "globex", "murder-copy-globex-restore-1", restore.ID)
	checkStatus(t, "another tenant's restore", err, http.StatusForbidden)
	now = now.Add(20 * time.Hour)
	if _, err := f.client.ReplaceTenant(ctx, "acme", "murder-copy-acme-restore-1", restore.ID); err != nil || len(backups.replaced) != 1 {
		t.Fatalf("a restore under its entry: %v %v", err, backups.replaced)
	}
}

// Recording an access says why; only root records one made before, other
// than by hand, or what was first written offline.
func TestRecordingAnAccess(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	for name, request := range map[string]access.Request{
		"no purpose": {Tenant: "acme", Kind: access.Manual, What: "docker exec", Ticket: "ZD-1"},
		"no what":    {Tenant: "acme", Kind: access.Manual, Purpose: "x", Ticket: "ZD-1"},
		"no kind":    {Tenant: "acme", Purpose: "x", Ticket: "ZD-1"},
		"future":     {Tenant: "acme", Kind: access.Manual, What: "docker exec", Purpose: "x", Ticket: "ZD-1", AccessedAt: time.Now().Add(time.Hour)},
	} {
		_, err := f.client.RecordAccess(ctx, request)
		checkStatus(t, name, err, http.StatusBadRequest)
	}
	someone := context.WithValue(context.Background(), uidKey{}, agentUID+1)
	someoneElse := context.WithValue(someone, actorKey{}, "panel-user")
	post := func(body string) int {
		recorder := httptest.NewRecorder()
		f.handler.ServeHTTP(recorder, httptest.NewRequest("POST", "/v1/access", strings.NewReader(body)).WithContext(someoneElse))
		return recorder.Code
	}
	if code := post(`{"tenant":"acme","kind":"logs","purpose":"x","ticket":"y","offline":"ABC"}`); code != http.StatusForbidden {
		t.Fatalf("an offline record by someone not root: %d", code)
	}
	if code := post(`{"tenant":"acme","kind":"logs","purpose":"x","ticket":"y","accessedAt":"2026-11-01T00:00:00Z"}`); code != http.StatusForbidden {
		t.Fatalf("a read made before, by someone not root: %d", code)
	}
	if code := post(`{"tenant":"acme","kind":"manual","what":"read its files","purpose":"x","ticket":"y","accessedAt":"2026-11-01T00:00:00Z"}`); code != http.StatusCreated {
		t.Fatalf("a manual access made before: %d", code)
	}
	at := time.Date(2026, 11, 2, 3, 4, 5, 0, time.UTC)
	entry, err := f.client.RecordAccess(ctx, access.Request{Kind: access.State, Purpose: "cluster lost", Ticket: "incident", AccessedAt: at, Offline: "REC1"})
	if err != nil || entry.Offline != "REC1" || !entry.When().Equal(at) {
		t.Fatalf("synced: %+v %v", entry, err)
	}
	all := accessesOf(t, f, "")
	if len(all) != 2 || all[0].Actor != "panel-user" || all[0].Kind != access.Manual {
		t.Fatalf("the register: %+v", all)
	}
	since, err := f.client.Accesses(ctx, "", all[1].At)
	if err != nil || len(since) != 1 || since[0].ID != all[1].ID {
		t.Fatalf("since: %+v %v", since, err)
	}
}

// Routes carry tenants' domains' sealed private keys: for the edge, as
// root, not for the socket's group.
func TestOnlyRootReadsTheRoutes(t *testing.T) {
	f := newAPIFixture(t)
	someone := context.WithValue(context.Background(), uidKey{}, agentUID+1)
	recorder := httptest.NewRecorder()
	f.handler.ServeHTTP(recorder, httptest.NewRequest("GET", "/v1/routes", nil).WithContext(someone))
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("routes for someone not root: %d", recorder.Code)
	}
}

// While changes are frozen (an upgrade, which a rollback may undo with the
// database from before), no access is recorded, so nothing is read.
func TestNothingIsReadWhileFrozen(t *testing.T) {
	f := withApp(t)
	ctx := context.Background()
	if _, err := f.client.Create(ctx, tenantOf("acme")); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Freeze("upgrade to abc", "test"); err != nil {
		t.Fatal(err)
	}
	value, err := f.client.Secret(ctx, "acme", "SESSION_SECRET", recorded)
	checkStatus(t, "a secret while frozen", err, http.StatusConflict)
	if value != "" || err == nil || !strings.Contains(err.Error(), "changes are frozen") {
		t.Fatalf("read while frozen: %q %v", value, err)
	}
}
