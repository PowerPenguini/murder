package store

import (
	"database/sql"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/PowerPenguini/murder/internal/spec"
)

const image = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
const otherImage = "sha256:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"

func appSpec() spec.Tenant {
	return spec.Tenant{Image: image, HealthPath: "/health", User: "1000:1000", Secrets: spec.Secrets{"SESSION": {}}}
}

// fakeSealer seals by numbering every value it makes.
func fakeSealer() Sealer {
	var count atomic.Int64
	return func(tenant, name string, secret spec.Secret) (string, error) {
		return fmt.Sprintf("%s/%s/%s#%d", tenant, name, secret.Kind, count.Add(1)), nil
	}
}

func options() Options {
	return Options{NetworkPool: netip.MustParsePrefix("10.240.0.0/16"), Sealer: fakeSealer()}
}

func openNew(t *testing.T, opts Options) (*SQLite, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state", "murder.db")
	if err := Init(path, "node-1"); err != nil {
		t.Fatal(err)
	}
	st, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st, path
}

// withApp opens a store with app "web".
func withApp(t *testing.T, opts Options) (*SQLite, string) {
	t.Helper()
	st, path := openNew(t, opts)
	if _, err := st.PutApp(App{Name: "web", Spec: appSpec()}, "test"); err != nil {
		t.Fatal(err)
	}
	return st, path
}

func drain(st *SQLite) bool {
	select {
	case <-st.Changed():
		return true
	default:
		return false
	}
}

func TestTenantsLiveThroughARestart(t *testing.T) {
	st, path := withApp(t, options())
	drain(st)
	first, err := st.Create(NewTenant{Name: "acme", App: "web"}, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if first.Subnet != "10.240.0.0/28" || first.Generation != 1 || first.CreatedAt.IsZero() || first.Spec.Name != "acme" || first.Spec.Port != 8080 {
		t.Fatalf("created record = %+v", first)
	}
	if first.Sealed["SESSION"] == "" {
		t.Fatal("the app's secret got no value")
	}
	if !drain(st) {
		t.Fatal("Create did not signal a change")
	}
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "alice"); !errors.Is(err, ErrExists) {
		t.Fatalf("duplicate create = %v", err)
	}
	second, err := st.Create(NewTenant{Name: "beta", App: "web"}, "alice")
	if err != nil || second.Subnet != "10.240.0.16/28" {
		t.Fatalf("second tenant = %+v, %v", second, err)
	}
	drain(st)
	if err := st.Observe("acme", image); err != nil {
		t.Fatal(err)
	}
	if drain(st) {
		t.Fatal("Observe signalled a change of desired state")
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, options())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	again, err := reopened.Get("acme")
	if err != nil || again.Subnet != first.Subnet || again.DataImage != image || again.Sealed["SESSION"] != first.Sealed["SESSION"] || !again.Spec.Equal(first.Spec) {
		t.Fatalf("after reopen = %+v, %v", again, err)
	}
	if reopened.Node() != "node-1" {
		t.Fatalf("node = %q", reopened.Node())
	}
}

func TestOpenRefusesMissingLockedOrNewerDatabases(t *testing.T) {
	if _, err := Open(filepath.Join(t.TempDir(), "missing.db"), options()); err == nil || !strings.Contains(err.Error(), "murder init") {
		t.Fatalf("missing database = %v", err)
	}
	_, path := openNew(t, options())
	if _, err := Open(path, options()); err == nil || !strings.Contains(err.Error(), "in use") {
		t.Fatalf("second open = %v", err)
	}
	if err := Init(path, "node-1"); err == nil {
		t.Fatal("Init overwrote a database")
	}
	newer := filepath.Join(t.TempDir(), "newer.db")
	if err := Init(newer, "node-1"); err != nil {
		t.Fatal(err)
	}
	for version, want := range map[int]string{schemaVersion + 1: "upgrade Murder", 2: "development release", 0: "not a Murder database"} {
		db, err := sql.Open("sqlite", dsn(newer))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(fmt.Sprintf("PRAGMA user_version = %d", version)); err != nil {
			t.Fatal(err)
		}
		_ = db.Close()
		if _, err := Open(newer, options()); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("schema version %d = %v", version, err)
		}
	}
	empty := filepath.Join(t.TempDir(), "empty.db")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(empty, options()); err == nil || !strings.Contains(err.Error(), "not initialized") {
		t.Fatalf("an empty database = %v", err)
	}
	_, valid := openNew(t, options())
	for name, opts := range map[string]Options{
		"pool":    {NetworkPool: netip.MustParsePrefix("10.240.0.0/30"), Sealer: fakeSealer()},
		"no seal": {NetworkPool: netip.MustParsePrefix("10.240.0.0/16")},
	} {
		if _, err := Open(valid, opts); err == nil || strings.Contains(err.Error(), "in use") {
			t.Errorf("%s: invalid options = %v", name, err)
		}
	}
}

// An app change reaches every tenant of the app and bumps their generation,
// unless the tenant's spec stays the same.
func TestAppChangesReachTheirTenants(t *testing.T) {
	st, _ := withApp(t, options())
	if _, err := st.PutApp(App{Name: "other", Spec: appSpec()}, "test"); err != nil {
		t.Fatal(err)
	}
	acme, _ := st.Create(NewTenant{Name: "acme", App: "web"}, "alice")
	pinned, _ := st.Create(NewTenant{Name: "pinned", App: "web", Overrides: spec.Overrides{Image: otherImage}}, "alice")
	bystander, _ := st.Create(NewTenant{Name: "bystander", App: "other"}, "alice")

	same, err := st.PutApp(App{Name: "web", Spec: appSpec()}, "bob")
	if err != nil || same.Changed || same.Created {
		t.Fatalf("an unchanged app = %+v, %v", same, err)
	}
	next := appSpec()
	next.Image = otherImage
	next.Secrets["NEW"] = spec.Secret{Kind: "password"}
	change, err := st.PutApp(App{Name: "web", Spec: next}, "bob")
	if err != nil || !change.Changed || !slices.Equal(change.Tenants, []string{"acme", "pinned"}) {
		t.Fatalf("app change = %+v, %v", change, err)
	}
	after, _ := st.Get("acme")
	if after.Spec.Image != otherImage || after.Generation != acme.Generation+1 || after.Sealed["NEW"] == "" || after.Sealed["SESSION"] != acme.Sealed["SESSION"] {
		t.Fatalf("acme after the app change = %+v", after)
	}
	if again, _ := st.Get("pinned"); again.Generation != pinned.Generation+1 || again.Spec.Image != otherImage {
		t.Fatalf("pinned = %+v", again)
	}
	if again, _ := st.Get("bystander"); again.Generation != bystander.Generation {
		t.Fatal("a tenant of another app was touched")
	}
	// Only the image changes now; the pinned tenant runs its own image.
	final := next
	final.Image = image
	change, err = st.PutApp(App{Name: "web", Spec: final}, "bob")
	if err != nil || !slices.Equal(change.Tenants, []string{"acme"}) {
		t.Fatalf("image change = %+v, %v", change, err)
	}
}

// An app change that would make a live tenant invalid is refused as a
// whole. Archived tenants do not block it: they are checked on restore and
// can be reconfigured meanwhile. Nor does a tenant already invalid.
func TestAppChangeMustKeepLiveTenantsValid(t *testing.T) {
	st, _ := withApp(t, options())
	if _, err := st.Create(NewTenant{Name: "acme", App: "web", Overrides: spec.Overrides{Env: map[string]string{"TOKEN": "x"}}}, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(NewTenant{Name: "old", App: "web", Overrides: spec.Overrides{Env: map[string]string{"KEY": "x"}}}, "a"); err != nil {
		t.Fatal(err)
	}
	if err := st.Archive("old", "a"); err != nil {
		t.Fatal(err)
	}
	clash := appSpec()
	clash.Secrets["TOKEN"] = spec.Secret{}
	clash.Secrets["KEY"] = spec.Secret{}
	_, err := st.PutApp(App{Name: "web", Spec: clash}, "b")
	var validation *spec.ValidationError
	if !errors.As(err, &validation) || !strings.Contains(err.Error(), "tenant acme") || strings.Contains(err.Error(), "tenant old") {
		t.Fatalf("clashing app = %v", err)
	}
	if err := st.CheckApp(App{Name: "web", Spec: clash}); err == nil || !strings.Contains(err.Error(), "tenant acme") {
		t.Fatalf("CheckApp = %v", err)
	}
	if app, _ := st.App("web"); len(app.Spec.Secrets) != 1 {
		t.Fatal("a refused app change was stored")
	}
	// Without the live clash the change goes through; the archived tenant
	// cannot come back until its override is fixed.
	if _, _, err := st.Configure("acme", "web", spec.Overrides{}, 0, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PutApp(App{Name: "web", Spec: clash}, "b"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Restore("old", "a"); !errors.As(err, &validation) || !strings.Contains(err.Error(), "murder tenant set") {
		t.Fatalf("restore an incompatible tenant = %v", err)
	}
	if _, _, err := st.Configure("old", "web", spec.Overrides{}, 0, "a"); err != nil {
		t.Fatal(err)
	}
	if restored, err := st.Restore("old", "a"); err != nil || restored.Sealed["KEY"] == "" {
		t.Fatalf("restore after the fix = %+v, %v", restored, err)
	}
	for name, app := range map[string]spec.Tenant{"Bad": appSpec(), "fresh": func() spec.Tenant { a := appSpec(); a.User = "0:0"; return a }(), "named": func() spec.Tenant { a := appSpec(); a.Name = "x"; return a }(), "asleep": func() spec.Tenant { a := appSpec(); a.Suspended = true; return a }()} {
		if _, err := st.PutApp(App{Name: name, Spec: app}, "b"); err == nil {
			t.Errorf("app %s was accepted", name)
		}
	}
}

// A tenant already invalid under the current rules does not block changes
// to its app.
func TestAnInvalidTenantDoesNotBlockItsApp(t *testing.T) {
	st, path := withApp(t, options())
	if _, err := st.Create(NewTenant{Name: "acme", App: "web", Overrides: spec.Overrides{Env: map[string]string{"A": "1"}}}, "a"); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()
	db, err := sql.Open("sqlite", dsn(path))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE tenants SET overrides = '{"env":{"BAD-NAME":"x"}}'`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	st, err = Open(path, options())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	next := appSpec()
	next.Image = otherImage
	if _, err := st.PutApp(App{Name: "web", Spec: next}, "a"); err != nil {
		t.Fatalf("an invalid tenant blocked its app: %v", err)
	}
}

func TestDeleteAppOnlyWhenUnused(t *testing.T) {
	st, _ := withApp(t, options())
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "a"); err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteApp("web", "a"); !errors.Is(err, ErrInUse) {
		t.Fatalf("delete a used app = %v", err)
	}
	_ = st.Archive("acme", "a")
	if err := st.DeleteApp("web", "a"); !errors.Is(err, ErrInUse) {
		t.Fatalf("delete an app of an archived tenant = %v", err)
	}
	if err := st.Forget("acme", "a"); err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteApp("web", "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.App("web"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted app = %v", err)
	}
	if err := st.DeleteApp("web", "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("delete a missing app = %v", err)
	}
}

func TestConfigureChangesOverridesAndSkipsNoOps(t *testing.T) {
	st, _ := withApp(t, options())
	created, _ := st.Create(NewTenant{Name: "acme", App: "web"}, "a")
	same, changed, err := st.Configure("acme", "web", spec.Overrides{Env: map[string]string{}}, 0, "a")
	if err != nil || changed || same.Generation != created.Generation {
		t.Fatalf("no-op configure = %+v, %v, %v", same, changed, err)
	}
	updated, changed, err := st.Configure("acme", "web", spec.Overrides{Env: map[string]string{"TZ": "UTC"}, Secrets: spec.Secrets{"EXTRA": {}}, Limits: spec.Limits{MemoryMB: 768}}, 0, "a")
	if err != nil || !changed || updated.Generation != created.Generation+1 || updated.Spec.Env["TZ"] != "UTC" || updated.Spec.Limits.MemoryMB != 768 || updated.Sealed["EXTRA"] == "" {
		t.Fatalf("configure = %+v, %v, %v", updated, changed, err)
	}
	// Dropping the extra secret keeps its value for a later return.
	if _, _, err := st.Configure("acme", "web", spec.Overrides{}, 0, "a"); err != nil {
		t.Fatal(err)
	}
	back, _, _ := st.Configure("acme", "web", spec.Overrides{Secrets: spec.Secrets{"EXTRA": {}}}, 0, "a")
	if back.Sealed["EXTRA"] != updated.Sealed["EXTRA"] {
		t.Fatal("a secret that came back got a new value")
	}
	if _, _, err := st.Configure("acme", "missing", spec.Overrides{}, 0, "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("configure with a missing app = %v", err)
	}
	if _, _, err := st.Configure("acme", "web", spec.Overrides{Env: map[string]string{"SESSION": "x"}}, 0, "a"); err == nil {
		t.Fatal("an override clashing with a secret was accepted")
	}
	if _, _, err := st.Configure("nobody", "web", spec.Overrides{}, 0, "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("configure a missing tenant = %v", err)
	}
}

func TestRotateReplacesOneSecret(t *testing.T) {
	st, _ := withApp(t, options())
	created, _ := st.Create(NewTenant{Name: "acme", App: "web"}, "a")
	rotated, err := st.Rotate("acme", "SESSION", "a")
	if err != nil || rotated.Sealed["SESSION"] == created.Sealed["SESSION"] || rotated.Generation != created.Generation+1 {
		t.Fatalf("rotate = %+v, %v", rotated, err)
	}
	if _, err := st.Rotate("acme", "NOPE", "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rotate a secret outside the spec = %v", err)
	}
}

func TestArchiveKeepsEverythingARestoreNeeds(t *testing.T) {
	st, _ := withApp(t, options())
	created, _ := st.Create(NewTenant{Name: "acme", App: "web", Overrides: spec.Overrides{Env: map[string]string{"TZ": "UTC"}}}, "a")
	if err := st.Observe("acme", image); err != nil {
		t.Fatal(err)
	}
	if err := st.Archive("acme", "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SetSuspended("acme", true, "a"); !errors.Is(err, ErrArchived) {
		t.Fatalf("suspend an archived tenant = %v", err)
	}
	if _, err := st.Live("acme"); !errors.Is(err, ErrArchived) {
		t.Fatalf("Live on an archived tenant = %v", err)
	}
	if err := st.Archive("acme", "a"); !errors.Is(err, ErrArchived) {
		t.Fatalf("archive twice = %v", err)
	}
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "a"); !errors.Is(err, ErrArchived) {
		t.Fatalf("create over an archived name = %v", err)
	}
	// An archived tenant can be reconfigured; it runs that when restored.
	if _, _, err := st.Configure("acme", "web", spec.Overrides{Env: map[string]string{"TZ": "UTC"}}, 0, "a"); err != nil {
		t.Fatalf("configure an archived tenant = %v", err)
	}
	archived, err := st.Get("acme")
	if err != nil || !archived.Archived() || archived.Sealed["SESSION"] != created.Sealed["SESSION"] || archived.DataImage != image || archived.Spec.Suspended || archived.Subnet != created.Subnet {
		t.Fatalf("archived = %+v, %v", archived, err)
	}
	if list := st.List(); len(list) != 1 || list[0].Spec.Name != "acme" || !list[0].Archived() {
		t.Fatalf("List = %+v", list)
	}
	// It keeps its subnet until it is purged: a container may still run at
	// its address.
	other, err := st.Create(NewTenant{Name: "beta", App: "web"}, "a")
	if err != nil || other.Subnet == created.Subnet {
		t.Fatalf("a new tenant got an archived tenant's subnet: %+v, %v", other, err)
	}
	// A restore runs the app's current spec with the tenant's overrides.
	next := appSpec()
	next.Image = otherImage
	if _, err := st.PutApp(App{Name: "web", Spec: next}, "a"); err != nil {
		t.Fatal(err)
	}
	restored, err := st.Restore("acme", "a")
	if err != nil || restored.Archived() || restored.Subnet != created.Subnet || restored.Spec.Image != otherImage || restored.Spec.Env["TZ"] != "UTC" || restored.Sealed["SESSION"] != created.Sealed["SESSION"] || restored.Spec.Suspended || restored.DataImage != image {
		t.Fatalf("restored = %+v, %v", restored, err)
	}
	if !restored.CreatedAt.Equal(created.CreatedAt) || restored.Generation <= created.Generation {
		t.Fatalf("restore lost the history: %+v", restored)
	}
	if _, err := st.Restore("acme", "a"); !errors.Is(err, ErrNotArchived) {
		t.Fatalf("restore a live tenant = %v", err)
	}
	if _, err := st.Restore("nobody", "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("restore a missing tenant = %v", err)
	}
}

// An archive keeps the tenant's suspension for its restore.
func TestArchiveKeepsTheSuspension(t *testing.T) {
	st, _ := withApp(t, options())
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SetSuspended("acme", true, "a"); err != nil {
		t.Fatal(err)
	}
	if err := st.Archive("acme", "a"); err != nil {
		t.Fatal(err)
	}
	if archived, _ := st.Get("acme"); !archived.Spec.Suspended {
		t.Fatal("the archive lost the suspension")
	}
	if restored, err := st.Restore("acme", "a"); err != nil || !restored.Spec.Suspended {
		t.Fatalf("restore = %+v, %v", restored, err)
	}
	if err := st.Archive("nobody", "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("archive a missing tenant = %v", err)
	}
}

func TestForgetDeletesTheTenantAndItsSecrets(t *testing.T) {
	st, path := withApp(t, options())
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "a"); err != nil {
		t.Fatal(err)
	}
	if err := st.Forget("acme", "a"); !errors.Is(err, ErrNotArchived) {
		t.Fatalf("forget a live tenant = %v", err)
	}
	_ = st.Archive("acme", "a")
	if err := st.Forget("acme", "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Get("acme"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("the archived tenant survived: %v", err)
	}
	var secrets int
	if err := st.db.QueryRow(`SELECT count(*) FROM secrets`).Scan(&secrets); err != nil || secrets != 0 {
		t.Fatalf("secrets left in %s: %d, %v", path, secrets, err)
	}
	// A fresh tenant of the name gets fresh secrets.
	fresh, err := st.Create(NewTenant{Name: "acme", App: "web"}, "a")
	if err != nil || fresh.Generation != 1 {
		t.Fatalf("create after purge = %+v, %v", fresh, err)
	}
}

func TestEventsRecordWhoChangedWhat(t *testing.T) {
	st, _ := withApp(t, options())
	_, _ = st.Create(NewTenant{Name: "acme", App: "web"}, "alice")
	_, _ = st.SetSuspended("acme", true, "bob")
	_, _ = st.Rotate("acme", "SESSION", "carol")
	_, _ = st.Create(NewTenant{Name: "beta", App: "web"}, "")
	events, err := st.Events(EventQuery{Tenant: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range events {
		got = append(got, e.Actor+" "+e.Action+" "+e.Detail)
	}
	want := []string{"carol secret.rotated SESSION", "bob tenant.suspended ", "alice tenant.created "}
	if !slices.Equal(got, want) {
		t.Fatalf("events = %q, want %q", got, want)
	}
	all, _ := st.Events(EventQuery{})
	if len(all) != 5 || all[0].Actor != "unknown" || all[len(all)-1].Action != "app.created" || all[len(all)-1].App != "web" {
		t.Fatalf("all events = %+v", all)
	}
	page, _ := st.Events(EventQuery{Before: all[1].ID, Limit: 2})
	if len(page) != 2 || page[0].ID != all[2].ID {
		t.Fatalf("page = %+v", page)
	}
	// A failed change records nothing.
	_, _ = st.Create(NewTenant{Name: "acme", App: "web"}, "mallory")
	if again, _ := st.Events(EventQuery{}); len(again) != len(all) {
		t.Fatal("a refused change was recorded")
	}
}

func TestReturnedRecordsAreCopies(t *testing.T) {
	st, _ := withApp(t, options())
	_, _ = st.Create(NewTenant{Name: "acme", App: "web", Overrides: spec.Overrides{Env: map[string]string{"A": "1"}}}, "a")
	record, _ := st.Get("acme")
	record.Spec.Env["A"] = "changed"
	record.Overrides.Env["A"] = "changed"
	record.Sealed["SESSION"] = "changed"
	again, _ := st.Get("acme")
	if again.Spec.Env["A"] != "1" || again.Overrides.Env["A"] != "1" || again.Sealed["SESSION"] == "changed" {
		t.Fatal("a returned record shares maps with the store")
	}
	app, _ := st.App("web")
	app.Spec.Secrets["X"] = spec.Secret{}
	if again, _ := st.App("web"); len(again.Spec.Secrets) != 1 {
		t.Fatal("a returned app shares maps with the store")
	}
}

func TestSubnetPoolExhaustion(t *testing.T) {
	opts := options()
	opts.NetworkPool = netip.MustParsePrefix("10.240.0.0/24") // 16 subnets
	st, _ := withApp(t, opts)
	for i := range 16 {
		if _, err := st.Create(NewTenant{Name: fmt.Sprintf("t%d", i), App: "web"}, "a"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.Create(NewTenant{Name: "late", App: "web"}, "a"); err == nil || !strings.Contains(err.Error(), "no free") {
		t.Fatalf("create beyond the pool = %v", err)
	}
}

// A failed sealer leaves nothing behind.
func TestFailedWritesChangeNothing(t *testing.T) {
	opts := options()
	fail := false
	sealer := opts.Sealer
	opts.Sealer = func(tenant, name string, secret spec.Secret) (string, error) {
		if fail {
			return "", errors.New("sealer broke")
		}
		return sealer(tenant, name, secret)
	}
	st, _ := withApp(t, opts)
	fail = true
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "a"); err == nil {
		t.Fatal("create with a failing sealer succeeded")
	}
	if len(st.List()) != 0 {
		t.Fatal("a failed create left a tenant in memory")
	}
	var tenants int
	if err := st.db.QueryRow(`SELECT count(*) FROM tenants`).Scan(&tenants); err != nil || tenants != 0 {
		t.Fatalf("a failed create left %d tenants on disk, %v", tenants, err)
	}
}

// A tenant changed since a client read it is not overwritten.
func TestConfigureRefusesOutdatedGenerations(t *testing.T) {
	st, _ := withApp(t, options())
	created, _ := st.Create(NewTenant{Name: "acme", App: "web"}, "a")
	if _, _, err := st.Configure("acme", "web", spec.Overrides{Env: map[string]string{"TZ": "UTC"}}, created.Generation, "a"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.Configure("acme", "web", spec.Overrides{Image: otherImage}, created.Generation, "b"); !errors.Is(err, ErrConflict) {
		t.Fatalf("configure from an outdated read = %v", err)
	}
	if record, _ := st.Get("acme"); record.Overrides.Env["TZ"] != "UTC" || record.Overrides.Image != "" {
		t.Fatalf("the earlier change was overwritten: %+v", record.Overrides)
	}
}

// The audit log keeps names of env overrides, not their values, and shows a
// tenant the app changes that reached it.
func TestEventsKeepNoEnvValuesAndShowRollouts(t *testing.T) {
	st, _ := withApp(t, options())
	_, _ = st.Create(NewTenant{Name: "acme", App: "web", Overrides: spec.Overrides{Env: map[string]string{"SMTP_PASSWORD": "hunter2"}}}, "a")
	_, _, _ = st.Configure("acme", "web", spec.Overrides{Env: map[string]string{"SMTP_PASSWORD": "hunter3"}}, 0, "a")
	next := appSpec()
	next.Env = map[string]string{"MODE": "prod"}
	_, _ = st.PutApp(App{Name: "web", Spec: next}, "b")
	_ = st.Archive("acme", "c")
	events, err := st.Events(EventQuery{Tenant: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	for _, e := range events {
		lines = append(lines, e.Actor+" "+e.Action+" "+e.Detail)
		if strings.Contains(e.Detail, "hunter") {
			t.Fatalf("an env value reached the audit log: %+v", e)
		}
	}
	want := []string{"c tenant.archived ", "b app.rolled-out app changed env", "a tenant.updated overrides: env SMTP_PASSWORD", "a tenant.created overrides: env SMTP_PASSWORD"}
	if !slices.Equal(lines, want) {
		t.Fatalf("events = %q, want %q", lines, want)
	}
	// An app's trail includes its tenants' events.
	appEvents, _ := st.Events(EventQuery{App: "web"})
	var update *Event
	for i := range appEvents {
		if appEvents[i].Action == "app.updated" {
			update = &appEvents[i]
		}
	}
	if update == nil || !strings.HasPrefix(update.Detail, "changed env; image ") || len(appEvents) != 6 {
		t.Fatalf("app events = %+v", appEvents)
	}
}

// Paths with URI syntax open the file Init created, owner-only, with the
// pragmas applied.
func TestPathsWithURICharacters(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "a#b?c%41")
	path := filepath.Join(dir, "murder.db")
	if err := Init(path, "node-1"); err != nil {
		t.Fatal(err)
	}
	st, err := Open(path, options())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var foreignKeys int
	if err := st.db.QueryRow(`PRAGMA foreign_keys`).Scan(&foreignKeys); err != nil || foreignKeys != 1 {
		t.Fatalf("foreign_keys = %d, %v", foreignKeys, err)
	}
	entries, _ := os.ReadDir(dir)
	for _, entry := range entries {
		info, _ := entry.Info()
		if !strings.HasSuffix(entry.Name(), ".lock") && info.Mode().Perm() != 0o600 {
			t.Errorf("%s has mode %v", entry.Name(), info.Mode().Perm())
		}
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(dir), "a")); !os.IsNotExist(err) {
		t.Fatal("SQLite opened a file cut at the URI syntax")
	}
}

// Open makes a database copied into place owner-only.
func TestOpenTightensTheMode(t *testing.T) {
	_, path := openNew(t, options())
	copyPath := filepath.Join(t.TempDir(), "restored.db")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(copyPath, data, 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := Open(copyPath, options())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if info, _ := os.Stat(copyPath); info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %v", info.Mode().Perm())
	}
}

// A relative database path works, and an init interrupted before its
// transaction committed, which leaves an empty database, is completed by the
// next one. A database that is not empty is never taken over.
func TestRelativePathAndInterruptedInit(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	path := filepath.Join("state", "murder.db")
	if err := os.MkdirAll("state", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Init(path, "node-1"); err != nil {
		t.Fatal(err)
	}
	st, err := Open(path, options())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if st.Node() != "node-1" {
		t.Fatalf("node = %q", st.Node())
	}
	// Another program's database, in the default rollback-journal mode.
	other := filepath.Join(dir, "other.db")
	db, err := sql.Open("sqlite", "file:"+other)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE notes (text TEXT)`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	if err := Init(other, "node-1"); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("init over another database = %v", err)
	}
	// It is left as it was: still in its own journal mode, with no WAL.
	if _, err := os.Stat(other + "-wal"); !os.IsNotExist(err) {
		t.Fatalf("init made a write-ahead log for a database it refused: %v", err)
	}
	db, err = sql.Open("sqlite", "file:"+other)
	if err != nil {
		t.Fatal(err)
	}
	var mode string
	if err := db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil || mode != "delete" {
		t.Fatalf("journal mode = %q, %v", mode, err)
	}
	_ = db.Close()
	if _, err := Open(other, options()); err == nil || !strings.Contains(err.Error(), "not a Murder database") {
		t.Fatalf("open another database = %v", err)
	}
}

// A tenant keeps its subnet while archived; a purge frees it for the next
// tenant.
func TestPurgeFreesTheSubnet(t *testing.T) {
	st, _ := withApp(t, options())
	acme, _ := st.Create(NewTenant{Name: "acme", App: "web"}, "a")
	if err := st.Archive("acme", "a"); err != nil {
		t.Fatal(err)
	}
	beta, err := st.Create(NewTenant{Name: "beta", App: "web"}, "a")
	if err != nil || beta.Subnet == acme.Subnet {
		t.Fatalf("beta = %+v, %v; acme holds %s", beta, err, acme.Subnet)
	}
	if err := st.Forget("acme", "a"); err != nil {
		t.Fatal(err)
	}
	gamma, err := st.Create(NewTenant{Name: "gamma", App: "web"}, "a")
	if err != nil || gamma.Subnet != acme.Subnet {
		t.Fatalf("gamma = %+v, %v; want the purged %s", gamma, err, acme.Subnet)
	}
}

// Subnets reads every tenant's subnet, archived ones too, while the agent
// holds the database, and from a database opened with another pool.
func TestSubnetsReadsEveryTenant(t *testing.T) {
	st, path := withApp(t, options())
	acme, _ := st.Create(NewTenant{Name: "acme", App: "web"}, "a")
	beta, _ := st.Create(NewTenant{Name: "beta", App: "web"}, "a")
	if err := st.Archive("beta", "a"); err != nil {
		t.Fatal(err)
	}
	subnets, err := Subnets(path)
	if err != nil || len(subnets) != 2 || !slices.ContainsFunc(subnets, func(p netip.Prefix) bool { return p.String() == acme.Subnet }) || !slices.ContainsFunc(subnets, func(p netip.Prefix) bool { return p.String() == beta.Subnet }) {
		t.Fatalf("Subnets = %v, %v", subnets, err)
	}
	if none, err := Subnets(filepath.Join(t.TempDir(), "missing.db")); err != nil || len(none) != 0 {
		t.Fatalf("Subnets of a missing database = %v, %v", none, err)
	}
	_ = st.Close()
	moved := options()
	moved.NetworkPool = netip.MustParsePrefix("10.250.0.0/16")
	reopened, err := Open(path, moved)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if gamma, err := reopened.Create(NewTenant{Name: "gamma", App: "web"}, "a"); err != nil || !moved.NetworkPool.Contains(netip.MustParsePrefix(gamma.Subnet).Addr()) {
		t.Fatalf("a new tenant after a pool change = %+v, %v", gamma, err)
	}
}

// Observe keeps the image the data had before, and every image a tenant is
// given or runs is tracked until forgotten.
func TestPreviousImageAndTrackedImages(t *testing.T) {
	st, _ := withApp(t, options())
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "a"); err != nil {
		t.Fatal(err)
	}
	for _, step := range []struct{ data, previous string }{{image, ""}, {otherImage, image}, {otherImage, image}, {image, otherImage}} {
		if err := st.Observe("acme", step.data); err != nil {
			t.Fatal(err)
		}
		if got, _ := st.Get("acme"); got.DataImage != step.data || got.PreviousImage != step.previous {
			t.Fatalf("after observing %s: data %s, previous %s", step.data, got.DataImage, got.PreviousImage)
		}
	}
	if images := st.Images(); !slices.Equal(images, []string{image, otherImage}) {
		t.Fatalf("tracked = %v", images)
	}
	// An image a tenant still has stays tracked; one none has goes.
	if err := st.ForgetImage(otherImage); err != nil {
		t.Fatal(err)
	}
	if images := st.Images(); !slices.Equal(images, []string{image, otherImage}) {
		t.Fatalf("forgot an image acme still has: %v", images)
	}
	if err := st.Archive("acme", "a"); err != nil {
		t.Fatal(err)
	}
	if err := st.Forget("acme", "a"); err != nil {
		t.Fatal(err)
	}
	if err := st.ForgetImage(otherImage); err != nil {
		t.Fatal(err)
	}
	if images := st.Images(); !slices.Equal(images, []string{image}) {
		t.Fatalf("tracked after forgetting = %v", images)
	}
}

func TestRollReplacesOnlyLiveTenants(t *testing.T) {
	st, _ := withApp(t, options())
	created, err := st.Create(NewTenant{Name: "acme", App: "web"}, "alice")
	if err != nil {
		t.Fatal(err)
	}
	drain(st)
	rolled, err := st.Roll("acme", "bob")
	if err != nil {
		t.Fatal(err)
	}
	if rolled.Rollout != 1 || rolled.Generation != created.Generation+1 || !drain(st) {
		t.Fatalf("rolled = %+v", rolled)
	}
	events, _ := st.Events(EventQuery{Tenant: "acme", Limit: 1})
	if events[0].Action != "tenant.rolled" || events[0].Actor != "bob" {
		t.Fatalf("events = %+v", events)
	}
	if err := st.Archive("acme", "bob"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Roll("acme", "bob"); !errors.Is(err, ErrArchived) {
		t.Fatalf("roll an archived tenant: %v", err)
	}
}

func TestSecretsAreSealedAgainInOneChange(t *testing.T) {
	st, _ := openNew(t, options())
	if _, err := st.PutApp(App{Name: "web", Spec: appSpec()}, "test"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b"} {
		if _, err := st.Create(NewTenant{Name: name, App: "web"}, "test"); err != nil {
			t.Fatal(err)
		}
	}
	a, _ := st.Get("a")
	if len(a.Sealed) == 0 {
		t.Fatal("the app has no secrets")
	}
	reseal := func(tenant, name, sealed string) (string, bool, error) {
		if strings.HasPrefix(sealed, "again:") {
			return sealed, false, nil
		}
		return "again:" + sealed, true, nil
	}
	count, err := st.ResealSecrets(reseal, "root")
	if err != nil || count != 2*len(a.Sealed) {
		t.Fatalf("resealed %d, %v", count, err)
	}
	b, _ := st.Get("b")
	for name, sealed := range b.Sealed {
		if !strings.HasPrefix(sealed, "again:") {
			t.Fatalf("%s of b: %q", name, sealed)
		}
	}
	if b.Generation != 1 {
		t.Fatalf("sealing again changed b's generation to %d", b.Generation)
	}
	// Again, nothing to seal; still a change, and one while frozen.
	if err := st.Freeze("master key rotation from test", "root"); err != nil {
		t.Fatal(err)
	}
	if count, err := st.ResealSecrets(reseal, "root"); err != nil || count != 0 {
		t.Fatalf("again: %d, %v", count, err)
	}
	events, _ := st.Events(EventQuery{Limit: 2})
	if events[0].Action != "secrets.resealed" || events[0].Detail != "0 secrets sealed with the master key in use" {
		t.Fatalf("events %+v", events)
	}
	failing := func(tenant, name, sealed string) (string, bool, error) { return "", false, errors.New("does not open") }
	if _, err := st.ResealSecrets(failing, "root"); err == nil || !strings.Contains(err.Error(), "does not open") {
		t.Fatalf("a value that does not open: %v", err)
	}
}

func TestARotationThawsOnlyItsOwnFreeze(t *testing.T) {
	st, _ := openNew(t, options())
	if err := st.Freeze("upgrade to abc", "root"); err != nil {
		t.Fatal(err)
	}
	if err := st.ThawOwn(RotationFreeze+"n1", "root"); err != nil || st.Frozen() != "upgrade to abc" {
		t.Fatalf("thawed another's freeze: %q, %v", st.Frozen(), err)
	}
	if err := st.Thaw("root"); err != nil {
		t.Fatal(err)
	}
	if err := st.Freeze(RotationFreeze+"n1", "root"); err != nil {
		t.Fatal(err)
	}
	if err := st.ThawOwn(RotationFreeze+"n1", "root"); err != nil || st.Frozen() != "" {
		t.Fatalf("own freeze: %q, %v", st.Frozen(), err)
	}
}

// A thaw for the freeze someone saw ends that one alone: one taken since
// stays, and the thaw says so.
func TestAThawEndsTheFreezeItWasFor(t *testing.T) {
	st, _ := openNew(t, options())
	if err := st.Freeze(RotationFreeze+"n1", "root"); err != nil {
		t.Fatal(err)
	}
	if err := st.ThawIf("upgrade to abc", "root"); !errors.Is(err, ErrConflict) || st.Frozen() == "" {
		t.Fatalf("thawed another freeze: %v, %q", err, st.Frozen())
	}
	if err := st.ThawIf(RotationFreeze+"n1", "root"); err != nil || st.Frozen() != "" {
		t.Fatalf("its own freeze: %v, %q", err, st.Frozen())
	}
	if err := st.ThawIf("anything", "root"); err != nil {
		t.Fatalf("nothing frozen: %v", err)
	}
}
