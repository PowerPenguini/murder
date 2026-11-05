package edge

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTablesAreVersionedByTheirRoutes(t *testing.T) {
	a := NewTable(map[string]Route{"acme": {Backend: "10.240.0.2:8080"}, "beta": {Backend: "10.240.0.18:8080"}})
	b := NewTable(map[string]Route{"beta": {Backend: "10.240.0.18:8080"}, "acme": {Backend: "10.240.0.2:8080"}})
	c := NewTable(map[string]Route{"acme": {Backend: "10.240.0.2:8080", Suspended: true}, "beta": {Backend: "10.240.0.18:8080"}})
	if a.Version != b.Version || a.Version == c.Version || NewTable(nil).Version == a.Version {
		t.Fatalf("versions %s %s %s", a.Version, b.Version, c.Version)
	}
}

func TestSavedTablesLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "edge-routes.json")
	if _, ok, err := LoadTable(path); ok || err != nil {
		t.Fatalf("a missing file: %v %v", ok, err)
	}
	table := NewTable(map[string]Route{"acme": {Backend: "10.240.0.2:8080", Suspended: true}})
	if err := SaveTable(path, table); err != nil {
		t.Fatal(err)
	}
	loaded, ok, err := LoadTable(path)
	if err != nil || !ok || loaded.Version != table.Version || !loaded.Routes["acme"].Suspended {
		t.Fatalf("loaded %+v, %v %v", loaded, ok, err)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v, %v", info.Mode(), err)
	}
	live := &LiveTable{}
	if _, ok := live.Route("acme"); ok || !live.Updated().IsZero() {
		t.Fatal("an empty table routes")
	}
	if _, _, unconfirmed := live.Unconfirmed(); unconfirmed {
		t.Fatal("a table restored from nothing is unconfirmed")
	}
	live.Restore(loaded)
	if _, ok := live.Route("acme"); !ok || !live.Updated().IsZero() {
		t.Fatal("a saved table does not route, or counts as confirmed by the agent")
	}
	since, confirmed, unconfirmed := live.Unconfirmed()
	if !unconfirmed || since.IsZero() {
		t.Fatal("a saved table is not unconfirmed")
	}
	live.Set(loaded)
	if route, ok := live.Route("acme"); !ok || route.Backend != "10.240.0.2:8080" || live.Updated().IsZero() {
		t.Fatalf("route %+v %v", route, ok)
	}
	select {
	case <-confirmed:
	default:
		t.Fatal("the agent's table does not confirm the saved one")
	}
	if _, _, unconfirmed := live.Unconfirmed(); unconfirmed {
		t.Fatal("a table the agent gave is unconfirmed")
	}
	if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadTable(path); err == nil {
		t.Fatal("a broken file loaded")
	}
}
