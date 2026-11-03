package main

import (
	"context"
	"errors"
	"net/netip"
	"path/filepath"
	"strings"
	"testing"

	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/volume"
)

// memoryZFS answers the zfs commands a copy's rollback and a removal run,
// from an in-memory set of datasets.
type memoryZFS struct {
	datasets  map[string]map[string]string // dataset: property: value
	snapshots []string
	calls     []string
}

func (m *memoryZFS) run(_ context.Context, name string, args ...string) ([]byte, error) {
	m.calls = append(m.calls, name+" "+strings.Join(args, " "))
	dataset := args[len(args)-1]
	missing := func() ([]byte, error) {
		return []byte("cannot open '" + dataset + "': dataset does not exist\n"), errors.New("exit status 1")
	}
	switch args[0] {
	case "get":
		props, ok := m.datasets[dataset]
		if !ok {
			return missing()
		}
		var out strings.Builder
		for _, key := range strings.Split(args[len(args)-2], ",") {
			value := props[key]
			if value == "" {
				value = "-"
			}
			out.WriteString(value + "\n")
		}
		return []byte(out.String()), nil
	case "rename":
		if _, ok := m.datasets[args[1]]; !ok {
			return missing()
		}
		m.datasets[args[2]] = m.datasets[args[1]]
		delete(m.datasets, args[1])
	case "destroy":
		if _, ok := m.datasets[dataset]; !ok {
			return missing()
		}
		delete(m.datasets, dataset)
	case "inherit":
		delete(m.datasets[dataset], args[1])
	case "rollback":
		dataset, _, _ = strings.Cut(dataset, "@")
		if _, ok := m.datasets[dataset]; !ok {
			return missing()
		}
	case "list":
		if _, ok := m.datasets[dataset]; !ok {
			return missing()
		}
		return []byte(strings.Join(m.snapshots, "\n") + "\n"), nil
	default:
		return nil, errors.New("unexpected command " + strings.Join(args, " "))
	}
	return nil, nil
}

// A whole copy's receive that stopped after the old copy went aside leaves
// no copy under the tenant's name, only the two staging copies of one
// swap. A failed move's discard on the node that keeps the tenant's copy
// then finishes the swap and rolls the copy back: it never removes the
// copies, which would leave the tenant without a replica.
func TestDiscardKeepsAHalfSwappedReplica(t *testing.T) {
	database := filepath.Join(t.TempDir(), "murder.db")
	if err := store.Init(database, "node-1"); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(database, store.Options{NetworkPool: netip.MustParsePrefix("10.240.0.0/16"), Sealer: func(string, string, spec.Secret) (string, error) { return "x", nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.PutApp(store.App{Name: "web", Spec: spec.Tenant{Image: "sha256:" + strings.Repeat("a", 64), HealthPath: "/health", User: "1000:1000"}}, "test"); err != nil {
		t.Fatal(err)
	}
	for _, node := range []store.Node{{Name: "node-1", Address: "10.0.0.1:7400"}, {Name: "node-2", Address: "10.0.0.2:7400"}} {
		if err := st.PutNode(node, "test"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.Create(store.NewTenant{Name: "acme", App: "web", Node: "node-2"}, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PlaceReplicas("test"); err != nil {
		t.Fatal(err)
	}
	if record, err := st.Get("acme"); err != nil || record.Replica != "node-1" {
		t.Fatalf("acme's replica is %q, %v; want node-1", record.Replica, err)
	}

	copyOf := func() map[string]string {
		return map[string]string{"io.murder:tenant": "acme", "io.murder:role": "replica", "mounted": "no",
			"mountpoint": "/srv/murder/tenants/acme", "refquota": "0", "canmount": "noauto", "io.murder:swap": "t1"}
	}
	zfs := &memoryZFS{
		datasets: map[string]map[string]string{
			"tank/murder/murder-previous:acme": copyOf(),
			"tank/murder/murder-incoming:acme": copyOf(),
		},
		snapshots: []string{"tank/murder/acme@murder-replica-20261103T100000.000Z"},
	}
	// Marks the old copy, which is the one to put back.
	zfs.datasets["tank/murder/murder-previous:acme"]["origin"] = "previous"
	moves := &agentMoves{store: st, volumes: volume.ZFS{Parent: "tank/murder", MountRoot: "/srv/murder/tenants", Run: zfs.run}}
	if err := moves.Discard(context.Background(), "acme"); err != nil {
		t.Fatal(err)
	}
	if got := zfs.datasets["tank/murder/acme"]; got == nil || got["io.murder:tenant"] != "acme" || got["io.murder:swap"] != "" {
		t.Fatalf("the copy was not put back; datasets %v, calls %v", zfs.datasets, zfs.calls)
	}
	if len(zfs.datasets) != 1 {
		t.Fatalf("staging copies left: %v", zfs.datasets)
	}
	if zfs.datasets["tank/murder/acme"]["origin"] != "previous" {
		t.Fatalf("the received copy took the tenant's name, not the old one: %v", zfs.datasets)
	}
	if !strings.Contains(strings.Join(zfs.calls, "\n"), "zfs rollback -r tank/murder/acme@murder-replica-20261103T100000.000Z") {
		t.Fatalf("the copy was not rolled back; calls %v", zfs.calls)
	}
}
