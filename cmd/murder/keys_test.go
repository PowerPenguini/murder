package main

import (
	"context"
	"io"
	"log/slog"
	"net/netip"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/secrets"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

func TestARotationOnOneNodeFreezesAndSealsAgain(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "master.key")
	if err := secrets.CreateKey(keyPath); err != nil {
		t.Fatal(err)
	}
	key, err := secrets.LoadKey(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	database := filepath.Join(dir, "murder.db")
	if err := store.Init(database, "node-1"); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(database, store.Options{NetworkPool: netip.MustParsePrefix("10.240.0.0/16"), Sealer: key.NewSealed})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	app := spec.Tenant{Image: "sha256:" + strings.Repeat("a", 64), HealthPath: "/health", User: "1000:1000", Secrets: spec.Secrets{"SESSION": {}}}
	if _, err := st.PutApp(store.App{Name: "web", Spec: app}, "test"); err != nil {
		t.Fatal(err)
	}
	before, err := st.Create(store.NewTenant{Name: "acme", App: "web"}, "test")
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	none := func() *cluster.Cluster { return nil }
	ring := &keyRing{path: keyPath, key: key, member: none, log: log}
	rotator := &keyRotator{ring: ring, store: st, member: none, node: "node-1", log: log, settle: time.Millisecond}
	old := key.Current()

	// Another node's rotation holds the cluster: none here.
	if err := st.Freeze(rotationFreeze+"node-2", "root"); err != nil {
		t.Fatal(err)
	}
	if _, err := rotator.Rotate(context.Background(), "root"); err == nil || !strings.Contains(err.Error(), "node-2") {
		t.Fatalf("a rotation during another: %v", err)
	}
	if err := st.Thaw("root"); err != nil {
		t.Fatal(err)
	}

	result, err := rotator.Rotate(context.Background(), "root")
	if err != nil {
		t.Fatal(err)
	}
	if ring := key.Ring(); len(ring) != 1 || ring[0].ID != result.Key || result.Key == old || result.Resealed != 1 {
		t.Fatalf("ring %+v, result %+v", ring, result)
	}
	after, _ := st.Get("acme")
	if after.Sealed["SESSION"] == before.Sealed["SESSION"] {
		t.Fatal("the secret was not sealed again")
	}
	if plain, err := key.Open(secrets.Context("acme", "SESSION"), after.Sealed["SESSION"]); err != nil || plain == "" {
		t.Fatalf("open: %v", err)
	}
	if st.Frozen() != "" {
		t.Fatalf("still frozen: %s", st.Frozen())
	}
	events, _ := st.Events(store.EventQuery{Limit: 10})
	var actions []string
	for _, event := range events {
		actions = append(actions, event.Action)
	}
	if got := strings.Join(actions[:4], " "); got != "changes.thawed master-key.rotated secrets.resealed changes.frozen" {
		t.Fatalf("events %s", got)
	}
}
