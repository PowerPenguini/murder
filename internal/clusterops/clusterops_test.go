package clusterops

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
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
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/upgrade"
)

// fakeAgent is a cluster of nodes as the agent's API shows it to
// clusterops. "-" is the local node, n1 unless self says otherwise.
type fakeAgent struct {
	mu      sync.Mutex
	self    string
	members []string
	// alone answers the cluster's status as a node outside a cluster does.
	alone bool
	// running is the binary each node runs; refuse names nodes that refuse
	// a binary, stuck ones that take it but never run it, down ones that do
	// not answer.
	running map[string]string
	refuse  map[string]bool
	stuck   map[string]bool
	down    map[string]bool
	// behind is how many answers a node gives, once it took a binary,
	// before it has caught up with the cluster and reconciled; lagging
	// counts them down.
	behind  map[string]int
	lagging map[string]int

	applied   uint64
	installed []string
	frozen    string
	freezes   int
	thawed    int

	tenants  []api.Tenant
	failMove map[string]bool
	moved    []string
}

func newFakeAgent(running map[string]string) *fakeAgent {
	members := slices.Sorted(func(yield func(string) bool) {
		for node := range running {
			if !yield(node) {
				return
			}
		}
	})
	return &fakeAgent{self: "n1", members: members, running: running, applied: 7}
}

func (a *fakeAgent) node(name string) string {
	if name == "-" {
		return a.self
	}
	return name
}

func (a *fakeAgent) ClusterStatus(context.Context) (cluster.Status, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.alone {
		return cluster.Status{}, &api.Error{Status: http.StatusConflict, Message: "this node is not in a cluster"}
	}
	status := cluster.Status{Node: a.self, Applied: a.applied}
	for _, name := range a.members {
		status.Members = append(status.Members, cluster.Member{Name: name})
	}
	return status, nil
}

func (a *fakeAgent) NodeInfo(_ context.Context, name string) (upgrade.Info, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	node := a.node(name)
	if a.down[node] {
		return upgrade.Info{}, &api.Error{Status: http.StatusBadGateway, Message: "node does not answer"}
	}
	started := time.Now().Add(-time.Second)
	info := upgrade.Info{Node: node, Version: upgrade.Version{Binary: a.running[node], Schema: 5}, Started: started, Applied: a.applied, LastPass: time.Now()}
	if a.lagging[node] > 0 {
		a.lagging[node]--
		info.Applied = a.applied - 1
		info.LastPass = started.Add(-time.Second)
	}
	return info, nil
}

func (a *fakeAgent) Install(_ context.Context, name string, binary io.Reader, sha string) error {
	data, err := io.ReadAll(binary)
	if err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	node := a.node(name)
	sum := sha256.Sum256(data)
	if a.frozen == "" || a.refuse[node] || sha != hex.EncodeToString(sum[:]) {
		return &api.Error{Status: http.StatusUnprocessableEntity, Message: "the edge did not hand over"}
	}
	a.installed = append(a.installed, node)
	if a.lagging == nil {
		a.lagging = map[string]int{}
	}
	a.lagging[node] = a.behind[node]
	if !a.stuck[node] {
		a.running[node] = sha
	}
	return nil
}

func (a *fakeAgent) Freeze(_ context.Context, reason string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.frozen = reason
	a.freezes++
	return nil
}

func (a *fakeAgent) Thaw(context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.frozen = ""
	a.thawed++
	return nil
}

func (a *fakeAgent) List(context.Context) ([]api.Tenant, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.tenants), nil
}

func (a *fakeAgent) Move(_ context.Context, tenant, to string) (api.Tenant, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.failMove[tenant] {
		return api.Tenant{}, &api.Error{Status: http.StatusConflict, Message: "the copy did not arrive"}
	}
	for i := range a.tenants {
		if a.tenants[i].Spec.Name == tenant {
			a.tenants[i].Node = to
			a.moved = append(a.moved, tenant)
			return a.tenants[i], nil
		}
	}
	return api.Tenant{}, &api.Error{Status: http.StatusNotFound, Message: "no such tenant"}
}

// binary writes a file that answers "version -json" as a murder binary
// does, and returns its path and hash.
func binary(t *testing.T) (string, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "murder-new")
	if err := os.WriteFile(path, []byte("#!/bin/sh\necho '{\"binary\":\"says-anything\",\"schema\":5}'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	sum, err := upgrade.Hash(path)
	if err != nil {
		t.Fatal(err)
	}
	return path, sum
}

func old3() map[string]string { return map[string]string{"n1": "old", "n2": "old", "n3": "old"} }

func TestUpgradeGoesNodeByNodeThisOneLast(t *testing.T) {
	agent := newFakeAgent(old3())
	path, sum := binary(t)
	var out bytes.Buffer
	if err := Upgrade(context.Background(), agent, path, 5*time.Second, &out); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(agent.installed, []string{"n2", "n3", "n1"}) || agent.frozen != "" || agent.thawed != 1 {
		t.Fatalf("installed %v, frozen %q, thawed %d", agent.installed, agent.frozen, agent.thawed)
	}
	for node, running := range agent.running {
		if running != sum {
			t.Fatalf("%s runs %s, not the new binary", node, running)
		}
	}
	// The binary is named by its file's hash, not by what it says of itself.
	if !strings.Contains(out.String(), "upgrading [n2 n3 n1] to "+sum[:12]) || !strings.Contains(out.String(), "every node runs the new binary") {
		t.Fatalf("output:\n%s", out.String())
	}
}

func TestTheFreezeNamesTheUpgrade(t *testing.T) {
	agent := newFakeAgent(old3())
	agent.stuck = map[string]bool{"n2": true}
	path, sum := binary(t)
	_ = Upgrade(context.Background(), agent, path, time.Second, io.Discard)
	if agent.frozen != store.UpgradeFreeze+sum[:12] {
		t.Fatalf("frozen for %q", agent.frozen)
	}
}

func TestANodeAloneUpgradesItself(t *testing.T) {
	agent := newFakeAgent(map[string]string{"solo": "old"})
	agent.self, agent.alone = "solo", true
	path, sum := binary(t)
	if err := Upgrade(context.Background(), agent, path, 5*time.Second, io.Discard); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(agent.installed, []string{"solo"}) || agent.running["solo"] != sum || agent.frozen != "" {
		t.Fatalf("installed %v, runs %s, frozen %q", agent.installed, agent.running["solo"], agent.frozen)
	}
}

// A node counts as upgraded only once it runs the binary, has caught up
// with the cluster and has reconciled since it started.
func TestAnUpgradeWaitsForEachNodeToCatchUp(t *testing.T) {
	agent := newFakeAgent(old3())
	agent.behind = map[string]int{"n2": 2}
	path, _ := binary(t)
	if err := Upgrade(context.Background(), agent, path, 10*time.Second, io.Discard); err != nil {
		t.Fatal(err)
	}
	if agent.lagging["n2"] != 0 {
		t.Fatalf("the upgrade went on while n2 was %d answers behind", agent.lagging["n2"])
	}
}

func TestARefusalBeforeAnyNodeChangedThaws(t *testing.T) {
	agent := newFakeAgent(old3())
	agent.refuse = map[string]bool{"n2": true}
	path, _ := binary(t)
	err := Upgrade(context.Background(), agent, path, 5*time.Second, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "no node was upgraded; changes are taken again") || agent.frozen != "" {
		t.Fatalf("upgrade: %v, frozen %q", err, agent.frozen)
	}
}

func TestChangesStayFrozenOnceNodesDiffer(t *testing.T) {
	// n3 refuses after n2 took the binary: two releases run.
	agent := newFakeAgent(old3())
	agent.refuse = map[string]bool{"n3": true}
	path, _ := binary(t)
	err := Upgrade(context.Background(), agent, path, 5*time.Second, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "1 node(s) run the new one, so changes stay frozen") || agent.frozen == "" {
		t.Fatalf("upgrade: %v, frozen %q", err, agent.frozen)
	}
}

func TestANodeThatDoesNotComeBackKeepsChangesFrozen(t *testing.T) {
	agent := newFakeAgent(old3())
	agent.stuck = map[string]bool{"n2": true}
	path, _ := binary(t)
	err := Upgrade(context.Background(), agent, path, 2*time.Second, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "did not come back") || !strings.Contains(err.Error(), "murder.prev upgrade rollback") || agent.frozen == "" {
		t.Fatalf("upgrade: %v, frozen %q", err, agent.frozen)
	}
	if !slices.Equal(agent.installed, []string{"n2"}) {
		t.Fatalf("installed %v after n2 did not come back", agent.installed)
	}
}

func TestAResumedUpgradeSkipsNodesThatRunTheBinary(t *testing.T) {
	agent := newFakeAgent(old3())
	path, sum := binary(t)
	agent.running["n2"] = sum // upgraded before the upgrade stopped
	agent.frozen = store.UpgradeFreeze + sum[:12]
	var out bytes.Buffer
	if err := Upgrade(context.Background(), agent, path, 5*time.Second, &out); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(agent.installed, []string{"n3", "n1"}) || agent.frozen != "" {
		t.Fatalf("installed %v, frozen %q", agent.installed, agent.frozen)
	}
	if !strings.Contains(out.String(), "n2 already runs it") {
		t.Fatalf("output:\n%s", out.String())
	}
}

// A node that does not answer stops the upgrade before anything changed,
// changes not frozen.
func TestANodeThatDoesNotAnswerFreezesNothing(t *testing.T) {
	agent := newFakeAgent(old3())
	agent.down = map[string]bool{"n3": true}
	path, _ := binary(t)
	err := Upgrade(context.Background(), agent, path, 5*time.Second, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "n3 does not answer, so nothing was changed") || agent.freezes != 0 || len(agent.installed) != 0 {
		t.Fatalf("upgrade: %v, freezes %d, installed %v", err, agent.freezes, agent.installed)
	}
}

// An upgrade that took over a stopped one does not take changes again when
// its first node refuses: the nodes ran different binaries before it.
func TestARefusalLeavesNodesThatDifferFrozen(t *testing.T) {
	agent := newFakeAgent(map[string]string{"n1": "old", "n2": "x", "n3": "old"})
	agent.refuse = map[string]bool{"n2": true}
	agent.frozen = store.UpgradeFreeze + "x"
	path, _ := binary(t)
	err := Upgrade(context.Background(), agent, path, 5*time.Second, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "the nodes ran different binaries before this upgrade") || agent.frozen == "" || agent.thawed != 0 {
		t.Fatalf("upgrade: %v, frozen %q, thawed %d", err, agent.frozen, agent.thawed)
	}
}

func TestAFileThatIsNotMurderIsRefusedBeforeAnything(t *testing.T) {
	agent := newFakeAgent(old3())
	path := filepath.Join(t.TempDir(), "not-murder")
	if err := os.WriteFile(path, []byte("#!/bin/sh\necho hello\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	err := Upgrade(context.Background(), agent, path, 5*time.Second, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "not a murder binary") || agent.freezes != 0 {
		t.Fatalf("upgrade: %v, freezes %d", err, agent.freezes)
	}
}

func TestACancelledUpgradeStopsWaiting(t *testing.T) {
	agent := newFakeAgent(old3())
	agent.stuck = map[string]bool{"n2": true}
	path, _ := binary(t)
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := Upgrade(ctx, agent, path, time.Minute, io.Discard)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 10*time.Second {
		t.Fatalf("upgrade: %v after %v", err, time.Since(start))
	}
}

func tenant(name, node string, archived bool) api.Tenant {
	t := api.Tenant{Spec: spec.Tenant{Name: name}, Node: node}
	if archived {
		t.ArchivedAt = time.Now()
	}
	return t
}

// A drain moves every tenant of the node, archived ones too, and nothing
// else.
func TestDrainMovesEveryTenantOfTheNode(t *testing.T) {
	agent := newFakeAgent(old3())
	agent.tenants = []api.Tenant{tenant("a", "n2", false), tenant("b", "n1", false), tenant("c", "n2", true), tenant("d", "n3", false)}
	var out bytes.Buffer
	if err := Drain(context.Background(), agent, "n2", "n3", &out); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(agent.moved, []string{"a", "c"}) {
		t.Fatalf("moved %v", agent.moved)
	}
	for _, tenant := range agent.tenants {
		if tenant.Node == "n2" {
			t.Fatalf("%s still runs on n2", tenant.Spec.Name)
		}
	}
	if !strings.Contains(out.String(), "a moved to n3") || !strings.Contains(out.String(), "node n2 runs no tenant now (2 moved)") {
		t.Fatalf("output:\n%s", out.String())
	}
}

func TestADrainStopsAtTheFirstTenantThatDoesNotMove(t *testing.T) {
	agent := newFakeAgent(old3())
	agent.tenants = []api.Tenant{tenant("a", "n2", false), tenant("b", "n2", false), tenant("c", "n2", false)}
	agent.failMove = map[string]bool{"b": true}
	err := Drain(context.Background(), agent, "n2", "n3", io.Discard)
	if err == nil || !strings.Contains(err.Error(), "move b: the copy did not arrive (moved 1 before it)") {
		t.Fatalf("drain: %v", err)
	}
	if !slices.Equal(agent.moved, []string{"a"}) {
		t.Fatalf("moved %v", agent.moved)
	}
}
