package store

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/PowerPenguini/corral/internal/spec"
)

func tenant(name string) spec.Tenant {
	return spec.Tenant{
		Name: name, Image: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Port: 8080, HealthPath: "/health", DataPath: "/data",
	}
}

func TestFileStoreLifecycleAndPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "state.json")
	st, err := OpenFile(path, 20000, 20001)
	if err != nil {
		t.Fatal(err)
	}
	first, err := st.Create(Record{Spec: tenant("acme"), Sealed: map[string]string{"K": "sealed"}})
	if err != nil {
		t.Fatal(err)
	}
	if first.HostPort != 20000 || first.Generation != 1 || first.CreatedAt.IsZero() {
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
	if err != nil || second.HostPort != 20001 {
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
	if err := st.Delete("beta"); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("state file mode = %v, %v", info.Mode().Perm(), err)
	}

	reopened, err := OpenFile(path, 20000, 20001)
	if err != nil {
		t.Fatal(err)
	}
	records := reopened.List()
	if len(records) != 1 || records[0].Spec.Name != "acme" || records[0].Generation != 2 || records[0].Sealed["K"] != "sealed" {
		t.Fatalf("reopened state = %+v", records)
	}
	// The freed port is reused.
	again, err := reopened.Create(Record{Spec: tenant("delta")})
	if err != nil || again.HostPort != 20001 {
		t.Fatalf("port reuse = %+v, %v", again, err)
	}
}

func TestUpdateGuardsIdentityAndValidity(t *testing.T) {
	st, err := OpenFile(filepath.Join(t.TempDir(), "state.json"), 20000, 20010)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(Record{Spec: tenant("acme")}); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*Record) error{
		"rename":     func(r *Record) error { r.Spec.Name = "other"; return nil },
		"move port":  func(r *Record) error { r.HostPort++; return nil },
		"invalidate": func(r *Record) error { r.Spec.Port = 0; return nil },
		"refuse":     func(*Record) error { return errors.New("no") },
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
	if _, err := st.Create(Record{Spec: spec.Tenant{Name: "bad"}}); err == nil {
		t.Fatal("created an invalid tenant")
	}
}

func TestReturnedRecordsAreCopies(t *testing.T) {
	st, err := OpenFile(filepath.Join(t.TempDir(), "state.json"), 20000, 20010)
	if err != nil {
		t.Fatal(err)
	}
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

func TestOpenRejectsBrokenState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte(`{"version": 9, "tenants": {}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenFile(path, 20000, 20010); err == nil {
		t.Fatal("opened a state of an unknown version")
	}
	if _, err := OpenFile(filepath.Join(t.TempDir(), "s.json"), 100, 200); err == nil {
		t.Fatal("accepted a privileged port range")
	}
}
