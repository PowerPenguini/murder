package store

import (
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/PowerPenguini/corral/internal/spec"
)

func tenant(name string) spec.Tenant {
	return spec.Tenant{
		Name: name, Image: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Port: 8080, HealthPath: "/health", DataPath: "/data", DataUID: 1000, DataGID: 1000, User: "1000:1000",
	}
}

func options(from, to int) Options {
	return Options{PortFrom: from, PortTo: to, NetworkPool: netip.MustParsePrefix("10.240.0.0/16")}
}

func openNew(t *testing.T, opts Options) (*File, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state", "state.json")
	if err := InitFile(path, "node-1"); err != nil {
		t.Fatal(err)
	}
	st, err := OpenFile(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st, path
}

func TestFileStoreLifecycleAndPersistence(t *testing.T) {
	st, path := openNew(t, options(20000, 20001))
	first, err := st.Create(Record{Spec: tenant("acme"), Sealed: map[string]string{"K": "sealed"}, DataImage: "ignored"})
	if err != nil {
		t.Fatal(err)
	}
	if first.HostPort != 20000 || first.Subnet != "10.240.0.0/28" || first.Generation != 1 || first.CreatedAt.IsZero() || first.DataImage != "" {
		t.Fatalf("created record = %+v", first)
	}
	select {
	case <-st.Changed():
	default:
		t.Fatal("Create did not signal a change")
	}
	if _, err := st.Create(Record{Spec: tenant("acme")}); !errors.Is(err, ErrExists) {
		t.Fatalf("duplicate create = %v", err)
	}
	second, err := st.Create(Record{Spec: tenant("beta")})
	if err != nil || second.HostPort != 20001 || second.Subnet != "10.240.0.16/28" {
		t.Fatalf("second tenant = %+v, %v", second, err)
	}
	if _, err := st.Create(Record{Spec: tenant("gamma")}); err == nil {
		t.Fatal("created a tenant with the port range exhausted")
	}
	updated, err := st.Update("acme", func(record *Record) error {
		record.Spec.Image = "sha256:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
		return nil
	})
	if err != nil || updated.Generation != 2 {
		t.Fatalf("update = %+v, %v", updated, err)
	}
	if err := st.Observe("acme", updated.Spec.Image); err != nil {
		t.Fatal(err)
	}
	select {
	case <-st.Changed():
		// drain the update's signal
	default:
	}
	if err := st.Observe("acme", "sha256:other"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-st.Changed():
		t.Fatal("Observe signalled a change of desired state")
	default:
	}
	if err := st.Delete("beta"); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("state file mode = %v, %v", info.Mode().Perm(), err)
	}
	_ = st.Close()

	reopened, err := OpenFile(path, options(20000, 20001))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	records := reopened.List()
	if len(records) != 1 || records[0].Spec.Name != "acme" || records[0].Generation != 2 || records[0].Sealed["K"] != "sealed" || records[0].DataImage != "sha256:other" {
		t.Fatalf("reopened state = %+v", records)
	}
	// The freed port and subnet are reused.
	again, err := reopened.Create(Record{Spec: tenant("delta")})
	if err != nil || again.HostPort != 20001 || again.Subnet != "10.240.0.16/28" {
		t.Fatalf("reuse = %+v, %v", again, err)
	}
}

// A missing state must never look like an empty one: an empty state removes
// every tenant's containers.
func TestOpenRefusesMissingOrLockedState(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "state.json")
	if _, err := OpenFile(missing, options(20000, 20010)); err == nil || !strings.Contains(err.Error(), "corral init") {
		t.Fatalf("open of a missing state = %v", err)
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatal("OpenFile created a state file")
	}
	_, path := openNew(t, options(20000, 20010))
	if _, err := OpenFile(path, options(20000, 20010)); err == nil || !strings.Contains(err.Error(), "in use") {
		t.Fatalf("second open = %v", err)
	}
	if err := InitFile(path, "node-1"); err == nil {
		t.Fatal("InitFile overwrote a state")
	}
}

func TestUpdateGuardsIdentityAndValidity(t *testing.T) {
	st, _ := openNew(t, options(20000, 20010))
	if _, err := st.Create(Record{Spec: tenant("acme")}); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*Record) error{
		"rename":      func(r *Record) error { r.Spec.Name = "other"; return nil },
		"move port":   func(r *Record) error { r.HostPort++; return nil },
		"move subnet": func(r *Record) error { r.Subnet = "10.0.0.0/28"; return nil },
		"observe":     func(r *Record) error { r.DataImage = "sha256:x"; return nil },
		"invalidate":  func(r *Record) error { r.Spec.Port = 0; return nil },
		"refuse":      func(*Record) error { return errors.New("no") },
		"run as root": func(r *Record) error { r.Spec.User = "0"; return nil },
	} {
		if _, err := st.Update("acme", change); err == nil {
			t.Errorf("%s: update accepted", name)
		}
	}
	record, _ := st.Get("acme")
	if record.Generation != 1 || record.Spec.Port != 8080 {
		t.Fatalf("a rejected update changed the record: %+v", record)
	}
	if _, err := st.Update("missing", func(*Record) error { return nil }); !errors.Is(err, ErrNotFound) {
		t.Fatalf("update of a missing tenant = %v", err)
	}
	if err := st.Observe("missing", "x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("observe of a missing tenant = %v", err)
	}
	if _, err := st.Create(Record{Spec: spec.Tenant{Name: "bad"}}); err == nil {
		t.Fatal("created an invalid tenant")
	}
}

// A tenant stored under older rules can still be suspended (and so deleted).
func TestSetSuspendedIgnoresTodaysValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	legacy := `{"version":1,"node":"node-1","tenants":{"old":{"spec":{"name":"old","image":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","port":8080,"healthPath":"/h","dataPath":"/data","dataUID":0,"dataGID":0,"user":"","limits":{}},"hostPort":20000,"subnet":"10.240.0.0/28","generation":3}}}`
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	st, err := OpenFile(path, options(20000, 20010))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if st.Node() != "node-1" {
		t.Fatalf("node = %q", st.Node())
	}
	if _, err := st.Update("old", func(*Record) error { return nil }); err == nil {
		t.Fatal("the legacy spec unexpectedly validates; the test needs an invalid one")
	}
	suspended, err := st.SetSuspended("old", true)
	if err != nil || !suspended.Spec.Suspended || suspended.Generation != 4 {
		t.Fatalf("SetSuspended = %+v, %v", suspended, err)
	}
	if err := st.Delete("old"); err != nil {
		t.Fatal(err)
	}
}

func TestRetireKeepsWhatARestoreNeeds(t *testing.T) {
	st, path := openNew(t, options(20000, 20010))
	if _, err := st.Create(Record{Spec: tenant("acme"), Sealed: map[string]string{"K": "sealed-k"}}); err != nil {
		t.Fatal(err)
	}
	if err := st.Observe("acme", "sha256:data-image"); err != nil {
		t.Fatal(err)
	}
	if err := st.Retire("acme"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Get("acme"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("retired tenant still listed: %v", err)
	}
	retired, ok := st.RetiredTenant("acme")
	if !ok || retired.Sealed["K"] != "sealed-k" || retired.DataImage != "sha256:data-image" || retired.DeletedAt.IsZero() {
		t.Fatalf("retired = %+v, %v", retired, ok)
	}
	_ = st.Close()
	reopened, err := OpenFile(path, options(20000, 20010))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, ok := reopened.RetiredTenant("acme"); !ok {
		t.Fatal("the retired entry did not survive a restart")
	}
	restored, err := reopened.Create(Record{Spec: tenant("acme"), Sealed: retired.Sealed})
	if err != nil || restored.DataImage != "sha256:data-image" {
		t.Fatalf("restore = %+v, %v", restored, err)
	}
	if _, ok := reopened.RetiredTenant("acme"); ok {
		t.Fatal("Create did not consume the retired entry")
	}
	if err := reopened.Retire("acme"); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Forget("acme"); err != nil {
		t.Fatal(err)
	}
	if _, ok := reopened.RetiredTenant("acme"); ok {
		t.Fatal("Forget kept the retired entry")
	}
	if err := reopened.Forget("acme"); err != nil {
		t.Fatalf("forgetting twice = %v", err)
	}
}

func TestReturnedRecordsAreCopies(t *testing.T) {
	st, _ := openNew(t, options(20000, 20010))
	created := tenant("acme")
	created.Env = map[string]string{"A": "1"}
	if _, err := st.Create(Record{Spec: created}); err != nil {
		t.Fatal(err)
	}
	created.Env["A"] = "changed by caller"
	got, _ := st.Get("acme")
	got.Spec.Env["A"] = "changed again"
	again, _ := st.Get("acme")
	if again.Spec.Env["A"] != "1" {
		t.Fatalf("stored env was changed through a copy: %q", again.Spec.Env["A"])
	}
}

func TestOpenRejectsBrokenStateAndOptions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte(`{"version": 9, "tenants": {}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenFile(path, options(20000, 20010)); err == nil {
		t.Fatal("opened a state of an unknown version")
	}
	_, good := openNew(t, options(20000, 20010))
	if _, err := OpenFile(good, options(100, 200)); err == nil {
		t.Fatal("accepted a privileged port range")
	}
	if _, err := OpenFile(good, Options{PortFrom: 20000, PortTo: 20010, NetworkPool: netip.MustParsePrefix("10.0.0.0/30")}); err == nil {
		t.Fatal("accepted a pool too small for one subnet")
	}
}

func TestSubnetPoolExhaustion(t *testing.T) {
	st, _ := openNew(t, Options{PortFrom: 20000, PortTo: 20100, NetworkPool: netip.MustParsePrefix("10.9.0.0/24")})
	for i := range 16 {
		if _, err := st.Create(Record{Spec: tenant("t" + string(rune('a'+i)))}); err != nil {
			t.Fatalf("tenant %d: %v", i, err)
		}
	}
	if _, err := st.Create(Record{Spec: tenant("overflow")}); err == nil || !strings.Contains(err.Error(), "no free /28") {
		t.Fatalf("17th tenant in a /24 = %v", err)
	}
}
