package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/upgrade"
)

// upgradeAgent fakes a cluster of three nodes behind the agent's API, so
// "murder upgrade" is tested over the wire; internal/clusterops tests what
// an upgrade does. refuse names nodes that refuse the binary.
type upgradeAgent struct {
	mu        sync.Mutex
	running   map[string]string
	refuse    map[string]bool
	installed []string
	frozen    bool
	thawed    int
}

func (a *upgradeAgent) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	node := strings.TrimPrefix(r.URL.Path, "/v1/upgrade/nodes/")
	if node == "-" {
		node = "n1"
	}
	switch {
	case r.URL.Path == "/v1/cluster":
		_ = json.NewEncoder(w).Encode(cluster.Status{Node: "n1", Members: []cluster.Member{{Name: "n1"}, {Name: "n2"}, {Name: "n3"}}})
	case r.URL.Path == "/v1/upgrade/freeze":
		a.frozen = true
		w.WriteHeader(http.StatusNoContent)
	case r.URL.Path == "/v1/upgrade/thaw":
		a.frozen = false
		a.thawed++
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodGet:
		now := time.Now()
		_ = json.NewEncoder(w).Encode(upgrade.Info{Node: node, Version: upgrade.Version{Binary: a.running[node], Schema: 5}, Started: now.Add(-time.Second), LastPass: now})
	case r.Method == http.MethodPut:
		data, _ := io.ReadAll(r.Body)
		sum := sha256.Sum256(data)
		if a.refuse[node] || r.Header.Get(api.ShaHeader) != hex.EncodeToString(sum[:]) {
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = w.Write([]byte(`{"error":"the edge did not hand over"}`))
			return
		}
		a.installed = append(a.installed, node)
		a.running[node] = r.Header.Get(api.ShaHeader)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func startUpgradeAgent(t *testing.T, agent *upgradeAgent) (*api.Client, string) {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "agent.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: agent}
	go server.Serve(listener)
	t.Cleanup(func() { _ = server.Close() })
	binary := filepath.Join(t.TempDir(), "murder-new")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\necho '{\"binary\":\"says-anything\",\"schema\":5}'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return api.NewClient(socket), binary
}

func TestUpgradeGoesNodeByNodeThisOneLast(t *testing.T) {
	agent := &upgradeAgent{running: map[string]string{"n1": "old", "n2": "old", "n3": "old"}}
	client, binary := startUpgradeAgent(t, agent)
	capture(t)
	if err := upgradeCluster(context.Background(), client, binary, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	sum, _ := upgrade.Hash(binary)
	if !slices.Equal(agent.installed, []string{"n2", "n3", "n1"}) || agent.frozen || agent.thawed != 1 || agent.running["n1"] != sum {
		t.Fatalf("installed %v, frozen %v, thawed %d, n1 runs %s", agent.installed, agent.frozen, agent.thawed, agent.running["n1"])
	}
}

func TestARefusalBeforeAnyNodeChangedThaws(t *testing.T) {
	agent := &upgradeAgent{running: map[string]string{"n1": "old", "n2": "old", "n3": "old"}, refuse: map[string]bool{"n2": true}}
	client, binary := startUpgradeAgent(t, agent)
	capture(t)
	err := upgradeCluster(context.Background(), client, binary, 5*time.Second)
	if err == nil || !strings.Contains(err.Error(), "no node was upgraded; changes are taken again") || agent.frozen {
		t.Fatalf("upgrade: %v, frozen %v", err, agent.frozen)
	}
}

// A node takes a binary only under an upgrade's freeze: one that has not
// applied it yet refuses, and installs nothing.
func TestANodeInstallsOnlyUnderAnUpgradeFreeze(t *testing.T) {
	path := filepath.Join(t.TempDir(), "murder.db")
	if err := store.Init(path, "n1"); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(path, store.Options{NetworkPool: netip.MustParsePrefix("10.240.0.0/16"), Sealer: func(string, string, spec.Secret) (string, error) { return "sealed", nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	executable := filepath.Join(t.TempDir(), "murder")
	if err := os.WriteFile(executable, []byte("old"), 0o755); err != nil {
		t.Fatal(err)
	}
	caughtUp := 0
	upgrader := &agentUpgrader{store: st, Node: &upgrade.Node{Executable: executable}, catchUp: func() error { caughtUp++; return nil }}
	err = upgrader.Install(context.Background(), strings.NewReader("new"), "sha")
	if err == nil || !strings.Contains(err.Error(), "not frozen for an upgrade") || caughtUp != 1 {
		t.Fatalf("install without a freeze: %v (caught up %d times)", err, caughtUp)
	}
	if data, _ := os.ReadFile(executable); string(data) != "old" {
		t.Fatal("a binary was installed")
	}
}
