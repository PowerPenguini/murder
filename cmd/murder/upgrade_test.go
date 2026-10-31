package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/upgrade"
)

// upgradeAgent fakes a cluster of three nodes for "murder upgrade": refuse
// names nodes that refuse the binary, stuck nodes that never come back.
type upgradeAgent struct {
	mu        sync.Mutex
	running   map[string]string
	refuse    map[string]bool
	stuck     map[string]bool
	down      map[string]bool
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
	case a.down[node]:
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error":"node does not answer"}`))
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
		if !a.stuck[node] {
			a.running[node] = r.Header.Get(api.ShaHeader)
		}
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

func TestChangesStayFrozenOnceNodesDiffer(t *testing.T) {
	// n3 refuses after n2 took the binary: two releases run.
	agent := &upgradeAgent{running: map[string]string{"n1": "old", "n2": "old", "n3": "old"}, refuse: map[string]bool{"n3": true}}
	client, binary := startUpgradeAgent(t, agent)
	capture(t)
	err := upgradeCluster(context.Background(), client, binary, 5*time.Second)
	if err == nil || !strings.Contains(err.Error(), "changes stay frozen") || !agent.frozen {
		t.Fatalf("upgrade: %v, frozen %v", err, agent.frozen)
	}
	// A node that does not come back keeps them frozen too.
	agent = &upgradeAgent{running: map[string]string{"n1": "old", "n2": "old", "n3": "old"}, stuck: map[string]bool{"n2": true}}
	client, binary = startUpgradeAgent(t, agent)
	err = upgradeCluster(context.Background(), client, binary, 2*time.Second)
	if err == nil || !strings.Contains(err.Error(), "did not come back") || !strings.Contains(err.Error(), "murder.prev upgrade rollback") || !agent.frozen {
		t.Fatalf("upgrade: %v, frozen %v", err, agent.frozen)
	}
}

func TestAResumedUpgradeSkipsNodesThatRunTheBinary(t *testing.T) {
	agent := &upgradeAgent{running: map[string]string{"n1": "old", "n2": "old", "n3": "old"}}
	client, binary := startUpgradeAgent(t, agent)
	sum, _ := upgrade.Hash(binary)
	agent.running["n2"] = sum // upgraded before the upgrade stopped
	agent.frozen = true
	capture(t)
	if err := upgradeCluster(context.Background(), client, binary, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(agent.installed, []string{"n3", "n1"}) || agent.frozen {
		t.Fatalf("installed %v, frozen %v", agent.installed, agent.frozen)
	}
}

// A node that does not answer stops the upgrade before anything changed,
// changes not frozen.
func TestANodeThatDoesNotAnswerFreezesNothing(t *testing.T) {
	agent := &upgradeAgent{running: map[string]string{"n1": "old", "n2": "old", "n3": "old"}, down: map[string]bool{"n3": true}}
	client, binary := startUpgradeAgent(t, agent)
	capture(t)
	err := upgradeCluster(context.Background(), client, binary, 5*time.Second)
	if err == nil || !strings.Contains(err.Error(), "nothing was changed") || agent.frozen || len(agent.installed) != 0 {
		t.Fatalf("upgrade: %v, frozen %v, installed %v", err, agent.frozen, agent.installed)
	}
}
