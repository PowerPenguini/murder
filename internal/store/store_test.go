package store

import (
	"database/sql"
	"errors"
	"fmt"
	"net/netip"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/PowerPenguini/corral/internal/spec"
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

func options(from, to int) Options {
	return Options{PortFrom: from, PortTo: to, NetworkPool: netip.MustParsePrefix("10.240.0.0/16"), Sealer: fakeSealer()}
}

func openNew(t *testing.T, opts Options) (*SQLite, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state", "corral.db")
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
	st, path := withApp(t, options(20000, 20001))
	drain(st)
	first, err := st.Create(NewTenant{Name: "acme", App: "web"}, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if first.HostPort != 20000 || first.Subnet != "10.240.0.0/28" || first.Generation != 1 || first.CreatedAt.IsZero() || first.Spec.Name != "acme" || first.Spec.Port != 8080 {
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
	if err != nil || second.HostPort != 20001 || second.Subnet != "10.240.0.16/28" {
		t.Fatalf("second tenant = %+v, %v", second, err)
	}
	if _, err := st.Create(NewTenant{Name: "gamma", App: "web"}, "alice"); err == nil || !strings.Contains(err.Error(), "no free host port") {
		t.Fatalf("create beyond the port range = %v", err)
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
	reopened, err := Open(path, options(20000, 20001))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	again, err := reopened.Get("acme")
	if err != nil || again.HostPort != 20000 || again.DataImage != image || again.Sealed["SESSION"] != first.Sealed["SESSION"] || !again.Spec.Equal(first.Spec) {
		t.Fatalf("after reopen = %+v, %v", again, err)
	}
	if reopened.Node() != "node-1" {
		t.Fatalf("node = %q", reopened.Node())
	}
}

func TestOpenRefusesMissingLockedOrNewerDatabases(t *testing.T) {
	if _, err := Open(filepath.Join(t.TempDir(), "missing.db"), options(20000, 20001)); err == nil || !strings.Contains(err.Error(), "corral init") {
		t.Fatalf("missing database = %v", err)
	}
	_, path := openNew(t, options(20000, 20001))
	if _, err := Open(path, options(20000, 20001)); err == nil || !strings.Contains(err.Error(), "in use") {
		t.Fatalf("second open = %v", err)
	}
	if err := Init(path, "node-1"); err == nil {
		t.Fatal("Init overwrote a database")
	}
	newer := filepath.Join(t.TempDir(), "newer.db")
	if err := Init(newer, "node-1"); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", dsn(newer))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(fmt.Sprintf("PRAGMA user_version = %d", len(migrations)+1)); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	if _, err := Open(newer, options(20000, 20001)); err == nil || !strings.Contains(err.Error(), "upgrade Corral") {
		t.Fatalf("a newer schema = %v", err)
	}
	for name, opts := range map[string]Options{
		"ports":   {PortFrom: 80, PortTo: 90, NetworkPool: netip.MustParsePrefix("10.240.0.0/16"), Sealer: fakeSealer()},
		"pool":    {PortFrom: 20000, PortTo: 20001, NetworkPool: netip.MustParsePrefix("10.240.0.0/30"), Sealer: fakeSealer()},
		"no seal": {PortFrom: 20000, PortTo: 20001, NetworkPool: netip.MustParsePrefix("10.240.0.0/16")},
	} {
		if _, err := Open(newer, opts); err == nil {
			t.Errorf("%s: invalid options accepted", name)
		}
	}
}

// An app change reaches every tenant of the app and bumps their generation,
// unless the tenant's spec stays the same.
func TestAppChangesReachTheirTenants(t *testing.T) {
	st, _ := withApp(t, options(20000, 20010))
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

// An app change that would make any of its tenants invalid, archived ones
// included, is refused as a whole.
func TestAppChangeMustKeepEveryTenantValid(t *testing.T) {
	st, _ := withApp(t, options(20000, 20010))
	if _, err := st.Create(NewTenant{Name: "acme", App: "web", Overrides: spec.Overrides{Env: map[string]string{"TOKEN": "x"}}}, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(NewTenant{Name: "old", App: "web", Overrides: spec.Overrides{Env: map[string]string{"KEY": "x"}}}, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.StartArchive("old", "a"); err != nil {
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
	if !errors.As(err, &validation) || !strings.Contains(err.Error(), "tenant acme") || !strings.Contains(err.Error(), "tenant old") {
		t.Fatalf("clashing app = %v", err)
	}
	if app, _ := st.App("web"); len(app.Spec.Secrets) != 1 {
		t.Fatal("a refused app change was stored")
	}
	if _, err := st.PutApp(App{Name: "Bad", Spec: appSpec()}, "b"); err == nil {
		t.Fatal("an invalid app name was accepted")
	}
	broken := appSpec()
	broken.User = "0:0"
	if _, err := st.PutApp(App{Name: "fresh", Spec: broken}, "b"); err == nil {
		t.Fatal("an invalid app with no tenants was accepted")
	}
}

func TestDeleteAppOnlyWhenUnused(t *testing.T) {
	st, _ := withApp(t, options(20000, 20010))
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "a"); err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteApp("web", "a"); !errors.Is(err, ErrInUse) {
		t.Fatalf("delete a used app = %v", err)
	}
	_, _ = st.StartArchive("acme", "a")
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
	st, _ := withApp(t, options(20000, 20010))
	created, _ := st.Create(NewTenant{Name: "acme", App: "web"}, "a")
	same, changed, err := st.Configure("acme", "web", spec.Overrides{Env: map[string]string{}}, "a")
	if err != nil || changed || same.Generation != created.Generation {
		t.Fatalf("no-op configure = %+v, %v, %v", same, changed, err)
	}
	updated, changed, err := st.Configure("acme", "web", spec.Overrides{Env: map[string]string{"TZ": "UTC"}, Secrets: spec.Secrets{"EXTRA": {}}, Limits: spec.Limits{MemoryMB: 768}}, "a")
	if err != nil || !changed || updated.Generation != created.Generation+1 || updated.Spec.Env["TZ"] != "UTC" || updated.Spec.Limits.MemoryMB != 768 || updated.Sealed["EXTRA"] == "" {
		t.Fatalf("configure = %+v, %v, %v", updated, changed, err)
	}
	// Dropping the extra secret keeps its value for a later return.
	if _, _, err := st.Configure("acme", "web", spec.Overrides{}, "a"); err != nil {
		t.Fatal(err)
	}
	back, _, _ := st.Configure("acme", "web", spec.Overrides{Secrets: spec.Secrets{"EXTRA": {}}}, "a")
	if back.Sealed["EXTRA"] != updated.Sealed["EXTRA"] {
		t.Fatal("a secret that came back got a new value")
	}
	if _, _, err := st.Configure("acme", "missing", spec.Overrides{}, "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("configure with a missing app = %v", err)
	}
	if _, _, err := st.Configure("acme", "web", spec.Overrides{Env: map[string]string{"SESSION": "x"}}, "a"); err == nil {
		t.Fatal("an override clashing with a secret was accepted")
	}
	if _, _, err := st.Configure("nobody", "web", spec.Overrides{}, "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("configure a missing tenant = %v", err)
	}
}

func TestRotateReplacesOneSecret(t *testing.T) {
	st, _ := withApp(t, options(20000, 20010))
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
	st, _ := withApp(t, options(20000, 20001))
	created, _ := st.Create(NewTenant{Name: "acme", App: "web", Overrides: spec.Overrides{Env: map[string]string{"TZ": "UTC"}}}, "a")
	if err := st.Observe("acme", image); err != nil {
		t.Fatal(err)
	}
	// Archived as it was before the archive suspended it, however many
	// times the archive starts.
	for range 2 {
		if started, err := st.StartArchive("acme", "a"); err != nil || !started.Spec.Suspended {
			t.Fatalf("StartArchive = %+v, %v", started, err)
		}
	}
	if err := st.Archive("acme", "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Get("acme"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("archived tenant still live: %v", err)
	}
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "a"); !errors.Is(err, ErrArchived) {
		t.Fatalf("create over an archived name = %v", err)
	}
	if _, _, err := st.Configure("acme", "web", spec.Overrides{}, "a"); !errors.Is(err, ErrArchived) {
		t.Fatalf("configure an archived tenant = %v", err)
	}
	archived, ok := st.ArchivedTenant("acme")
	if !ok || archived.Record.Sealed["SESSION"] != created.Sealed["SESSION"] || archived.Record.DataImage != image || archived.Record.Spec.Suspended || archived.ArchivedAt.IsZero() || archived.Record.HostPort != 0 {
		t.Fatalf("archived = %+v, %v", archived, ok)
	}
	if list := st.ListArchived(); len(list) != 1 || list[0].Record.Spec.Name != "acme" {
		t.Fatalf("ListArchived = %+v", list)
	}
	// Its port and subnet are free for others meanwhile.
	other, err := st.Create(NewTenant{Name: "beta", App: "web"}, "a")
	if err != nil || other.HostPort != created.HostPort || other.Subnet != created.Subnet {
		t.Fatalf("port of an archived tenant = %+v, %v", other, err)
	}
	// A restore runs the app's current spec with the tenant's overrides.
	next := appSpec()
	next.Image = otherImage
	if _, err := st.PutApp(App{Name: "web", Spec: next}, "a"); err != nil {
		t.Fatal(err)
	}
	restored, err := st.Restore("acme", "a")
	if err != nil || restored.HostPort != 20001 || restored.Spec.Image != otherImage || restored.Spec.Env["TZ"] != "UTC" || restored.Sealed["SESSION"] != created.Sealed["SESSION"] || restored.Spec.Suspended || restored.DataImage != image {
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

// A suspend or resume after a failed archive decides what the next archive
// keeps.
func TestSuspensionAfterAFailedArchiveIsWhatIsArchived(t *testing.T) {
	st, _ := withApp(t, options(20000, 20001))
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.StartArchive("acme", "a"); err != nil { // running; the archive then fails
		t.Fatal(err)
	}
	if _, err := st.SetSuspended("acme", false, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SetSuspended("acme", true, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.StartArchive("acme", "a"); err != nil {
		t.Fatal(err)
	}
	if err := st.Archive("acme", "a"); err != nil {
		t.Fatal(err)
	}
	if archived, _ := st.ArchivedTenant("acme"); !archived.Record.Spec.Suspended {
		t.Fatal("a tenant suspended before its archive comes back running")
	}
}

func TestForgetDeletesTheTenantAndItsSecrets(t *testing.T) {
	st, path := withApp(t, options(20000, 20001))
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "a"); err != nil {
		t.Fatal(err)
	}
	if err := st.Forget("acme", "a"); !errors.Is(err, ErrNotArchived) {
		t.Fatalf("forget a live tenant = %v", err)
	}
	_, _ = st.StartArchive("acme", "a")
	_ = st.Archive("acme", "a")
	if err := st.Forget("acme", "a"); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.ArchivedTenant("acme"); ok {
		t.Fatal("the archive entry survived")
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
	st, _ := withApp(t, options(20000, 20010))
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
	st, _ := withApp(t, options(20000, 20010))
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
	opts := options(20000, 20100)
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
	opts := options(20000, 20010)
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
