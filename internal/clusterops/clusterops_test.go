package clusterops

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
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
	// statusFails makes the cluster's status fail as it does without a
	// leader. installFails makes a node take the binary but fail the
	// request, as a node that restarts before it answers does.
	statusFails  bool
	installFails map[string]bool
	// behind and unreconciled are how many answers a node gives, once it
	// took a binary, before it has caught up with the cluster and before
	// it has reconciled since it started; lagging and stale count them
	// down. restarting is a node that still reports its old binary once.
	behind       map[string]int
	unreconciled map[string]int
	lagging      map[string]int
	stale        map[string]int
	restarting   map[string]string

	applied   uint64
	installed []string
	frozen    string
	freezes   int
	thawed    int

	tenants  []api.Tenant
	failMove map[string]bool
	moved    []string

	// lease is the operation lease held now, "" for none; leases counts
	// those taken, released those let go. renewals counts renewals;
	// lostAfter, if set, has the lease broken at that renewal.
	lease     string
	leaseKind string
	leases    int
	released  int
	renewals  int
	lostAfter int
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
	if a.statusFails {
		return cluster.Status{}, &api.Error{Status: http.StatusServiceUnavailable, Message: "no leader"}
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
		return upgrade.Info{}, &api.Error{Status: http.StatusInternalServerError, Message: "node does not answer"}
	}
	started := time.Now().Add(-time.Second)
	info := upgrade.Info{Node: node, Version: upgrade.Version{Binary: a.running[node], Schema: 5}, Started: started, Applied: a.applied, LastPass: time.Now()}
	if old, ok := a.restarting[node]; ok {
		delete(a.restarting, node)
		info.Version.Binary = old
		return info, nil
	}
	if a.lagging[node] > 0 {
		a.lagging[node]--
		info.Applied = a.applied - 1
	}
	if a.stale[node] > 0 {
		a.stale[node]--
		info.LastPass = started.Add(-time.Second)
	}
	return info, nil
}

func (a *fakeAgent) Nodes(context.Context) (api.Nodes, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return api.Nodes{Self: a.self, Frozen: a.frozen}, nil
}

func (a *fakeAgent) Install(_ context.Context, name string, binary io.Reader, sha, freeze string) error {
	data, err := io.ReadAll(binary)
	if err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	node := a.node(name)
	sum := sha256.Sum256(data)
	if freeze != "" && a.frozen != freeze {
		return &api.Error{Status: http.StatusPreconditionFailed, Message: "the upgrade no longer holds the freeze"}
	}
	if !strings.HasPrefix(a.frozen, store.UpgradeFreeze) {
		return &api.Error{Status: http.StatusConflict, Message: "changes are not frozen for an upgrade"}
	}
	if a.refuse[node] || sha != hex.EncodeToString(sum[:]) {
		return &api.Error{Status: http.StatusUnprocessableEntity, Message: "the edge did not hand over"}
	}
	a.installed = append(a.installed, node)
	if a.lagging == nil {
		a.lagging, a.stale, a.restarting = map[string]int{}, map[string]int{}, map[string]string{}
	}
	a.lagging[node], a.stale[node] = a.behind[node], a.unreconciled[node]
	if !a.stuck[node] {
		// The node restarts: once more it answers as it ran before.
		a.restarting[node] = a.running[node]
		a.running[node] = sha
	}
	// The cluster went on meanwhile; the node must catch up to it.
	a.applied++
	if a.installFails[node] {
		return &api.Error{Status: http.StatusInternalServerError, Message: "connection reset"}
	}
	return nil
}

// Freeze, TakeOverFreeze and ThawOwn refuse as the store and the API do:
// a freeze for another reason stays.
func (a *fakeAgent) Freeze(_ context.Context, reason string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.frozen != "" && a.frozen != reason {
		return &api.Error{Status: http.StatusConflict, Message: "changes are frozen: " + a.frozen}
	}
	if a.lease != "" {
		return &api.Error{Status: http.StatusConflict, Message: "conflicting change: another operation changes the cluster: " + a.leaseKind}
	}
	a.frozen = reason
	a.freezes++
	return nil
}

func (a *fakeAgent) TakeOverFreeze(_ context.Context, from, reason string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.frozen != from {
		return &api.Error{Status: http.StatusConflict, Message: "changes are frozen: " + a.frozen}
	}
	a.frozen = reason
	a.freezes++
	return nil
}

func (a *fakeAgent) ThawOwn(_ context.Context, reason string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch a.frozen {
	case "":
		return nil
	case reason:
		a.frozen = ""
		a.thawed++
		return nil
	}
	return &api.Error{Status: http.StatusConflict, Message: "changes are frozen: " + a.frozen + ", not " + reason}
}

func (a *fakeAgent) List(context.Context) ([]api.Tenant, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.tenants), nil
}

func (a *fakeAgent) Move(ctx context.Context, tenant, to string) (api.Tenant, error) {
	return a.MoveFrom(ctx, tenant, "", to)
}

// MoveFrom refuses as the agent does: beside another operation's lease,
// and a tenant that no longer runs where the plan had it.
func (a *fakeAgent) MoveFrom(ctx context.Context, tenant, from, to string) (api.Tenant, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return api.Tenant{}, err
	}
	if a.lease != "" && api.LeaseOf(ctx) != a.lease {
		return api.Tenant{}, &api.Error{Status: http.StatusConflict, Message: "conflicting change: another operation changes the cluster: " + a.leaseKind}
	}
	if a.failMove[tenant] {
		return api.Tenant{}, &api.Error{Status: http.StatusConflict, Message: "the copy did not arrive"}
	}
	if from != "" {
		for _, t := range a.tenants {
			if t.Spec.Name == tenant && t.Node != from {
				return api.Tenant{}, &api.Error{Status: http.StatusConflict, Message: fmt.Sprintf("conflicting change: tenant %s runs on node %s now, not on node %s where the operation planned it", tenant, t.Node, from)}
			}
		}
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
	// It names the binary, the node that coordinates it and an operation
	// of its own.
	if !strings.HasPrefix(agent.frozen, store.UpgradeFreeze+sum[:12]+" from node n1, operation ") {
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
// with the cluster as it was once the node took the binary, and has
// reconciled since it started; each is waited for on its own.
func TestAnUpgradeWaitsForEachNodeToCatchUp(t *testing.T) {
	for _, c := range []struct {
		name         string
		behind       int
		unreconciled int
	}{{"applied", 2, 0}, {"reconciled", 0, 2}} {
		t.Run(c.name, func(t *testing.T) {
			agent := newFakeAgent(old3())
			agent.behind = map[string]int{"n2": c.behind}
			agent.unreconciled = map[string]int{"n2": c.unreconciled}
			path, _ := binary(t)
			if err := Upgrade(context.Background(), agent, path, 10*time.Second, io.Discard); err != nil {
				t.Fatal(err)
			}
			if agent.lagging["n2"] != 0 || agent.stale["n2"] != 0 {
				t.Fatalf("the upgrade went on while n2 was %d answers behind and %d unreconciled", agent.lagging["n2"], agent.stale["n2"])
			}
		})
	}
}

// A node whose install request failed in another way than a refusal may
// run the new binary: changes stay frozen, even on the first node.
func TestAFailedInstallIsNotARefusal(t *testing.T) {
	agent := newFakeAgent(old3())
	agent.installFails = map[string]bool{"n2": true}
	path, _ := binary(t)
	err := Upgrade(context.Background(), agent, path, 5*time.Second, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "n2: install") || !strings.Contains(err.Error(), "changes stay frozen") || agent.frozen == "" || agent.thawed != 0 {
		t.Fatalf("upgrade: %v, frozen %q, thawed %d", err, agent.frozen, agent.thawed)
	}
}

// Only a node outside a cluster upgrades alone: a cluster whose status
// fails is not upgraded at all.
func TestAClusterWithoutStatusIsNotUpgraded(t *testing.T) {
	agent := newFakeAgent(old3())
	agent.statusFails = true
	path, _ := binary(t)
	err := Upgrade(context.Background(), agent, path, 5*time.Second, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "no leader") || agent.freezes != 0 || len(agent.installed) != 0 {
		t.Fatalf("upgrade: %v, freezes %d, installed %v", err, agent.freezes, agent.installed)
	}
}

// An upgrade does not take over a freeze made for something else.
func TestAnUpgradeLeavesAnotherFreezeAlone(t *testing.T) {
	agent := newFakeAgent(old3())
	agent.frozen = "restoring a backup"
	path, _ := binary(t)
	err := Upgrade(context.Background(), agent, path, 5*time.Second, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "freeze changes") || agent.frozen != "restoring a backup" || len(agent.installed) != 0 {
		t.Fatalf("upgrade: %v, frozen %q, installed %v", err, agent.frozen, agent.installed)
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
	if err == nil || !strings.Contains(err.Error(), "did not come back") || !strings.Contains(err.Error(), `"murder upgrade rollback" (the new binary`) || agent.frozen == "" {
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
	err := TakeOver(context.Background(), agent, path, 5*time.Second, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "the nodes ran different binaries before this upgrade") || agent.frozen == "" || agent.thawed != 0 {
		t.Fatalf("upgrade: %v, frozen %q, thawed %d", err, agent.frozen, agent.thawed)
	}
}

// An upgrade never takes over the freeze of another, which may still run,
// unless told to: neither one of another binary, nor one of the same binary
// coordinated from another node.
func TestAnUpgradeTakesOverAnotherOnlyWhenTold(t *testing.T) {
	path, sum := binary(t)
	for _, held := range []string{store.UpgradeFreeze + "x", store.UpgradeFreeze + sum[:12] + " from node n2, operation 01"} {
		agent := newFakeAgent(old3())
		agent.frozen = held
		err := Upgrade(context.Background(), agent, path, 5*time.Second, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "changes are frozen for another upgrade") || !strings.Contains(err.Error(), "-take-over") || agent.frozen != held || len(agent.installed) != 0 {
			t.Fatalf("over %q: %v, frozen %q, installed %v", held, err, agent.frozen, agent.installed)
		}
		var out bytes.Buffer
		if err := TakeOver(context.Background(), agent, path, 5*time.Second, &out); err != nil {
			t.Fatal(err)
		}
		if agent.frozen != "" || !strings.Contains(out.String(), "taking over the freeze of another upgrade") || len(agent.installed) != 3 {
			t.Fatalf("a takeover of %q: frozen %q, installed %v:\n%s", held, agent.frozen, agent.installed, out.String())
		}
	}
}

// An upgrade this node coordinated, to the same binary, resumes under a
// freeze of its own: the one it was installs nothing more.
func TestAnUpgradeResumesItsOwnUnderANewFreeze(t *testing.T) {
	agent := newFakeAgent(old3())
	path, sum := binary(t)
	agent.running["n2"] = sum
	held := store.UpgradeFreeze + sum[:12] + " from node n1, operation 01"
	agent.frozen = held
	var out bytes.Buffer
	if err := Upgrade(context.Background(), agent, path, 5*time.Second, &out); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(agent.installed, []string{"n3", "n1"}) || agent.frozen != "" || !strings.Contains(out.String(), "resuming the upgrade that stopped") {
		t.Fatalf("installed %v, frozen %q:\n%s", agent.installed, agent.frozen, out.String())
	}
	file, _ := os.Open(path)
	defer file.Close()
	agent.frozen = store.UpgradeFreeze + "y"
	if err := agent.Install(context.Background(), "n2", file, sum, held); status(err) != http.StatusPreconditionFailed {
		t.Fatalf("an install under the freeze taken over: %v", err)
	}
}

// racingAgent is a cluster where another upgrade begins between this one's
// look at the nodes and its freeze, and upgrades n3 first.
type racingAgent struct {
	*fakeAgent
	path, sum string
}

func (a racingAgent) Freeze(ctx context.Context, reason string) error {
	other := store.UpgradeFreeze + a.sum[:12] + " from node n2, operation 01"
	if err := a.fakeAgent.Freeze(ctx, other); err != nil {
		return err
	}
	file, err := os.Open(a.path)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := a.fakeAgent.Install(ctx, "n3", file, a.sum, other); err != nil {
		return err
	}
	return a.fakeAgent.Freeze(ctx, reason)
}

// Two upgrades at once: the one that froze first goes on; the other
// changes nothing, and leaves the freeze alone, even though its first node
// would refuse and the nodes ran one release when it looked.
func TestAnUpgradeLeavesOneThatFrozeFirstAlone(t *testing.T) {
	path, sum := binary(t)
	agent := newFakeAgent(old3())
	agent.refuse = map[string]bool{"n2": true}
	err := Upgrade(context.Background(), racingAgent{agent, path, sum}, path, time.Second, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "freeze changes") || !strings.HasPrefix(agent.frozen, store.UpgradeFreeze) || agent.thawed != 0 {
		t.Fatalf("upgrade: %v, frozen %q, thawed %d", err, agent.frozen, agent.thawed)
	}
	if !slices.Equal(agent.installed, []string{"n3"}) {
		t.Fatalf("installed %v", agent.installed)
	}
}

// What the nodes run is read again under the upgrade's own freeze: one
// that took over another, which had upgraded a node, keeps changes frozen
// when its first node refuses.
func TestAnUpgradeReadsTheNodesOnceFrozen(t *testing.T) {
	path, sum := binary(t)
	agent := newFakeAgent(old3())
	agent.refuse = map[string]bool{"n2": true}
	agent.frozen = store.UpgradeFreeze + "x from node n2, operation 01"
	looked := 0
	client := lookingAgent{agent, func(node string) {
		// The other upgrade installed on n3 after this one looked at
		// every node, before it froze.
		if looked++; looked == 4 {
			agent.running["n3"] = sum
		}
	}}
	err := TakeOver(context.Background(), client, path, time.Second, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "the nodes ran different binaries before this upgrade") || agent.frozen == "" || agent.thawed != 0 {
		t.Fatalf("upgrade: %v, frozen %q, thawed %d", err, agent.frozen, agent.thawed)
	}
}

// lookingAgent calls look before each node's answer.
type lookingAgent struct {
	*fakeAgent
	look func(node string)
}

func (a lookingAgent) NodeInfo(ctx context.Context, node string) (upgrade.Info, error) {
	a.look(node)
	return a.fakeAgent.NodeInfo(ctx, node)
}

// An upgrade whose freeze another took over while it ran installs nothing
// more and takes no changes again.
func TestAnUpgradeStopsOnceItsFreezeIsTakenOver(t *testing.T) {
	path, _ := binary(t)
	agent := newFakeAgent(old3())
	took := store.UpgradeFreeze + "y from node n3, operation 02"
	client := lookingAgent{agent, func(string) {
		if len(agent.installed) == 1 && strings.Contains(agent.frozen, " from node n1,") {
			agent.frozen = took
		}
	}}
	err := Upgrade(context.Background(), client, path, 5*time.Second, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "took no binary") || !strings.Contains(err.Error(), "installs nothing more") || agent.frozen != took || agent.thawed != 0 {
		t.Fatalf("upgrade: %v, frozen %q, thawed %d", err, agent.frozen, agent.thawed)
	}
	if !slices.Equal(agent.installed, []string{"n2"}) {
		t.Fatalf("installed %v", agent.installed)
	}
}

// A node that ran the binary already is not sent it again, but waited for
// as one just upgraded: an upgrade resumed takes changes again only once
// every node has caught up and reconciled.
func TestAResumedUpgradeWaitsForNodesThatRanTheBinary(t *testing.T) {
	path, sum := binary(t)
	agent := newFakeAgent(map[string]string{"n1": sum, "n2": sum, "n3": sum})
	agent.frozen = store.UpgradeFreeze + sum[:12]
	agent.stale = map[string]int{"n1": 100, "n2": 100, "n3": 100}
	err := Upgrade(context.Background(), agent, path, time.Second, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "n2 runs the new binary but is not ready") || !strings.Contains(err.Error(), "has not reconciled") || agent.frozen == "" || agent.thawed != 0 || len(agent.installed) != 0 {
		t.Fatalf("upgrade: %v, frozen %q, thawed %d, installed %v", err, agent.frozen, agent.thawed, agent.installed)
	}
	agent.stale = map[string]int{"n1": 2, "n2": 2, "n3": 2}
	if err := Upgrade(context.Background(), agent, path, 10*time.Second, io.Discard); err != nil || agent.frozen != "" || len(agent.installed) != 0 {
		t.Fatalf("upgrade: %v, frozen %q, installed %v", err, agent.frozen, agent.installed)
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
// BalanceMoves plans as the agent does, from the tenants and members.
func (f *fakeAgent) BalanceMoves(context.Context) ([]store.Move, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var nodes []store.Node
	for _, name := range f.members {
		nodes = append(nodes, store.Node{Name: name})
	}
	var records []store.Record
	for _, t := range f.tenants {
		records = append(records, store.Record{Spec: t.Spec, Node: t.Node, ArchivedAt: t.ArchivedAt})
	}
	return store.BalanceMoves(nodes, records), nil
}

func TestBalanceMovesOneTenantAfterTheOther(t *testing.T) {
	agent := newFakeAgent(old3())
	agent.tenants = []api.Tenant{tenant("a", "n1", false), tenant("b", "n1", false), tenant("c", "n1", false), tenant("d", "n1", false), tenant("e", "n2", false)}
	var out bytes.Buffer
	if err := Balance(context.Background(), agent, true, &out); err != nil || len(agent.moved) != 0 || !strings.Contains(out.String(), "a would move from n1 to n3") {
		t.Fatalf("dry run moved %v, %v:\n%s", agent.moved, err, out.String())
	}
	out.Reset()
	if err := Balance(context.Background(), agent, false, &out); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(agent.moved, []string{"a", "b"}) || !strings.Contains(out.String(), "balanced: 2 tenants moved") {
		t.Fatalf("moved %v:\n%s", agent.moved, out.String())
	}
	out.Reset()
	if err := Balance(context.Background(), agent, false, &out); err != nil || !strings.Contains(out.String(), "the nodes are balanced") {
		t.Fatalf("again: %v\n%s", err, out.String())
	}
}

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

// An upgrade that ends takes changes again only if the freeze is still its
// own: one another upgrade took over at the last moment stays.
func TestAnUpgradeThawsOnlyItsOwnFreeze(t *testing.T) {
	path, _ := binary(t)
	agent := newFakeAgent(old3())
	took := store.UpgradeFreeze + "y from node n3, operation 02"
	client := lookingAgent{agent, func(string) {
		if len(agent.installed) == 3 {
			agent.frozen = took
		}
	}}
	err := Upgrade(context.Background(), client, path, 5*time.Second, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "another upgrade took the freeze over meanwhile") || agent.frozen != took || agent.thawed != 0 {
		t.Fatalf("upgrade: %v, frozen %q, thawed %d", err, agent.frozen, agent.thawed)
	}
}
