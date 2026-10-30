package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/netip"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/PowerPenguini/murder/internal/secrets"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

const testImage = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func masterKey(t *testing.T, fill byte) *secrets.Key {
	t.Helper()
	key, err := secrets.NewKey(bytes.Repeat([]byte{fill}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return key
}

// node is one test node: its database and log survive a restart.
type node struct {
	t       *testing.T
	name    string
	role    string
	dir     string
	key     *secrets.Key
	address string
	store   *store.SQLite
	cluster *Cluster
	fatal   chan error
}

func newNode(t *testing.T, name, role string) *node {
	t.Helper()
	n := &node{t: t, name: name, role: role, dir: t.TempDir(), key: masterKey(t, 7), address: "127.0.0.1:0", fatal: make(chan error, 1)}
	if err := store.Init(filepath.Join(n.dir, "murder.db"), name); err != nil {
		t.Fatal(err)
	}
	n.start()
	t.Cleanup(n.stop)
	return n
}

func (n *node) start() {
	n.t.Helper()
	st, err := store.Open(filepath.Join(n.dir, "murder.db"), store.Options{NetworkPool: netip.MustParsePrefix("10.240.0.0/16"), Sealer: n.key.NewSealed})
	if err != nil {
		n.t.Fatal(err)
	}
	c, err := Start(Config{
		Node: n.name, Address: n.address, Role: n.role, Dir: filepath.Join(n.dir, "raft"),
		Seed: n.key.Derive("cluster ca"), Store: st,
		Log:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Fatal: func(err error) { n.fatal <- err },
		raft: func(config *raft.Config) {
			config.HeartbeatTimeout = 500 * time.Millisecond
			config.ElectionTimeout = 500 * time.Millisecond
			config.LeaderLeaseTimeout = 250 * time.Millisecond
			config.CommitTimeout = 5 * time.Millisecond
		},
	})
	if err != nil {
		_ = st.Close()
		n.t.Fatal(err)
	}
	n.store, n.cluster = st, c
	// A restart listens where the node was: the others know it there.
	n.address = c.address
}

func (n *node) stop() {
	if n.cluster == nil {
		return
	}
	_ = n.cluster.Close()
	_ = n.store.Close()
	n.cluster, n.store = nil, nil
}

func ctx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func appSpec() spec.Tenant {
	return spec.Tenant{Image: testImage, HealthPath: "/health", User: "1000:1000", Secrets: spec.Secrets{"SESSION": {}}}
}

// eventually waits for check to pass.
func eventually(t *testing.T, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// threeNodes starts a cluster of a worker and two witnesses.
func threeNodes(t *testing.T) []*node {
	t.Helper()
	worker := newNode(t, "fsn-1", store.RoleWorker)
	if err := worker.cluster.Bootstrap(ctx(t), "alice"); err != nil {
		t.Fatal(err)
	}
	nodes := []*node{worker}
	for _, name := range []string{"nbg-1", "hel-1"} {
		witness := newNode(t, name, store.RoleWitness)
		if err := witness.cluster.Join(ctx(t), worker.address, "alice"); err != nil {
			t.Fatalf("join %s: %v", name, err)
		}
		// Join returns once the node follows the leader.
		if status, err := witness.cluster.Status(); err != nil || status.Leader != "fsn-1" {
			t.Fatalf("after joining, %s sees %+v, %v", name, status, err)
		}
		nodes = append(nodes, witness)
	}
	for _, n := range nodes {
		eventually(t, n.name+" knows every node", func() bool { return len(n.store.Nodes()) == 3 })
	}
	return nodes
}

func sameState(t *testing.T, nodes []*node) {
	t.Helper()
	first := nodes[0].store
	for _, n := range nodes[1:] {
		eventually(t, n.name+" caught up", func() bool { return n.store.Applied() == first.Applied() })
		if !reflect.DeepEqual(n.store.List(), first.List()) || !reflect.DeepEqual(n.store.Apps(), first.Apps()) || !reflect.DeepEqual(n.store.Nodes(), first.Nodes()) {
			t.Fatalf("%s differs from %s:\n%+v\n%+v", n.name, nodes[0].name, n.store.List(), first.List())
		}
		ours, _ := n.store.Events(store.EventQuery{Limit: 1000})
		theirs, _ := first.Events(store.EventQuery{Limit: 1000})
		if !reflect.DeepEqual(ours, theirs) {
			t.Fatalf("%s has other events", n.name)
		}
	}
}

func TestWritesOnAnyNodeReachEveryNode(t *testing.T) {
	nodes := threeNodes(t)
	worker, witness := nodes[0], nodes[2]
	if _, err := worker.store.PutApp(store.App{Name: "web", Spec: appSpec()}, "alice"); err != nil {
		t.Fatal(err)
	}
	// A write on a follower goes through the leader, and shows there at
	// once once it returns.
	created, err := witness.store.Create(store.NewTenant{Name: "acme", App: "web"}, "bob")
	if err != nil {
		t.Fatal(err)
	}
	if created.Subnet == "" || created.Sealed["SESSION"] == "" {
		t.Fatalf("created = %+v", created)
	}
	if _, err := witness.store.Live("acme"); err != nil {
		t.Fatalf("the writing node does not show its own write: %v", err)
	}
	sameState(t, nodes)

	// A refused write reports the state's reason, from any node.
	if _, err := nodes[1].store.Create(store.NewTenant{Name: "acme", App: "web"}, "bob"); !errors.Is(err, store.ErrExists) {
		t.Fatalf("create twice: %v", err)
	}

	status, err := witness.cluster.Status()
	if err != nil {
		t.Fatal(err)
	}
	if status.Leader != "fsn-1" || status.State != "Follower" || status.Role != store.RoleWitness || len(status.Members) != 3 ||
		status.Members[0] != (Member{Name: "fsn-1", Address: worker.address, Role: store.RoleWorker, Suffrage: "Voter"}) {
		t.Fatalf("status = %+v", status)
	}
}

func TestConcurrentWritesFromSeveralNodesAllLand(t *testing.T) {
	nodes := threeNodes(t)
	if _, err := nodes[0].store.PutApp(store.App{Name: "web", Spec: appSpec()}, "alice"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 9)
	for i, n := range nodes {
		for j := range 3 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := n.store.Create(store.NewTenant{Name: string(rune('a'+i)) + strings.Repeat("x", j+1), App: "web"}, "alice")
				errs <- err
			}()
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	sameState(t, nodes)
	subnets := map[string]bool{}
	for _, record := range nodes[0].store.List() {
		if subnets[record.Subnet] {
			t.Fatalf("two tenants got subnet %s", record.Subnet)
		}
		subnets[record.Subnet] = true
	}
	if len(subnets) != 9 {
		t.Fatalf("%d tenants", len(subnets))
	}
}

func TestTheClusterOutlivesItsLeader(t *testing.T) {
	nodes := threeNodes(t)
	if _, err := nodes[2].store.PutApp(store.App{Name: "web", Spec: appSpec()}, "alice"); err != nil {
		t.Fatal(err)
	}
	leader := nodes[0]
	leader.stop()
	rest := nodes[1:]
	eventually(t, "a new leader", func() bool { return rest[0].cluster.Leader() || rest[1].cluster.Leader() })
	if _, err := rest[0].store.Create(store.NewTenant{Name: "acme", App: "web"}, "bob"); err != nil {
		t.Fatal(err)
	}
	// The old leader comes back, follows, catches up, and applies nothing
	// twice.
	leader.start()
	sameState(t, nodes)
	if _, err := leader.store.SetSuspended("acme", true, "carol"); err != nil {
		t.Fatal(err)
	}
	sameState(t, nodes)
	if record, _ := nodes[1].store.Get("acme"); !record.Spec.Suspended || record.Generation != 2 {
		t.Fatalf("acme = %+v", record)
	}
}

func TestOneNodeLeftWritesNothing(t *testing.T) {
	nodes := threeNodes(t)
	nodes[1].stop()
	nodes[2].stop()
	start := time.Now()
	_, err := nodes[0].store.PutApp(store.App{Name: "web", Spec: appSpec()}, "alice")
	if err == nil {
		t.Fatal("a node without a majority made a change")
	}
	if time.Since(start) > commitTimeout+5*time.Second {
		t.Fatalf("the write hung for %v", time.Since(start))
	}
}

func TestJoinRules(t *testing.T) {
	worker := newNode(t, "fsn-1", store.RoleWorker)
	if err := worker.cluster.Bootstrap(ctx(t), "alice"); err != nil {
		t.Fatal(err)
	}
	if err := worker.cluster.Bootstrap(ctx(t), "alice"); err == nil || !strings.Contains(err.Error(), "already part of a cluster") {
		t.Fatalf("bootstrap twice: %v", err)
	}
	second := newNode(t, "nbg-1", store.RoleWorker)
	if err := second.cluster.Join(ctx(t), worker.address, "alice"); err == nil || !strings.Contains(err.Error(), "already has worker fsn-1") {
		t.Fatalf("a second worker joined: %v", err)
	}
	// The refused node took nothing, and joins in another role.
	if !second.store.Fresh() {
		t.Fatal("a refused node took the cluster's state")
	}
	second.stop()
	second.role = store.RoleWitness
	second.start()
	if err := second.cluster.Join(ctx(t), worker.address, "alice"); err != nil {
		t.Fatal(err)
	}

	// A node outside a cluster says so at once.
	lone := newNode(t, "hel-1", store.RoleWitness)
	start := time.Now()
	if _, err := lone.store.PutApp(store.App{Name: "web", Spec: appSpec()}, "alice"); err == nil || !strings.Contains(err.Error(), "not part of a cluster") || time.Since(start) > 5*time.Second {
		t.Fatalf("a write outside a cluster after %v: %v", time.Since(start), err)
	}
}

func TestANodeWithStateCannotJoin(t *testing.T) {
	worker := newNode(t, "fsn-1", store.RoleWorker)
	if err := worker.cluster.Bootstrap(ctx(t), "alice"); err != nil {
		t.Fatal(err)
	}
	other := newNode(t, "hel-1", store.RoleWitness)
	if err := other.cluster.Bootstrap(ctx(t), "bob"); err != nil {
		t.Fatal(err)
	}
	if err := other.cluster.Join(ctx(t), worker.address, "bob"); err == nil || !strings.Contains(err.Error(), "already part of a cluster") {
		t.Fatalf("a node of another cluster joined: %v", err)
	}
}

func TestAnotherMasterKeyIsNotTheCluster(t *testing.T) {
	worker := newNode(t, "fsn-1", store.RoleWorker)
	if err := worker.cluster.Bootstrap(ctx(t), "alice"); err != nil {
		t.Fatal(err)
	}
	stranger := &node{t: t, name: "evil", role: store.RoleWitness, dir: t.TempDir(), key: masterKey(t, 8), address: "127.0.0.1:0", fatal: make(chan error, 1)}
	if err := store.Init(filepath.Join(stranger.dir, "murder.db"), "evil"); err != nil {
		t.Fatal(err)
	}
	stranger.start()
	t.Cleanup(stranger.stop)
	if err := stranger.cluster.Join(ctx(t), worker.address, "mallory"); err == nil {
		t.Fatal("a node with another master key joined")
	}
	if nodes := worker.store.Nodes(); len(nodes) != 1 {
		t.Fatalf("nodes = %+v", nodes)
	}
}

func TestRemoveTakesANodeOut(t *testing.T) {
	nodes := threeNodes(t)
	if err := nodes[1].cluster.Remove(ctx(t), "hel-1", "alice"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "hel-1 is gone", func() bool {
		status, err := nodes[0].cluster.Status()
		return err == nil && len(status.Members) == 2 && len(nodes[1].store.Nodes()) == 2
	})
	if err := nodes[0].cluster.Remove(ctx(t), "nope", "alice"); err == nil || !strings.Contains(err.Error(), "not part of the cluster") {
		t.Fatalf("remove an unknown node: %v", err)
	}
	// Two voters still make a majority of two.
	if _, err := nodes[1].store.PutApp(store.App{Name: "web", Spec: appSpec()}, "alice"); err != nil {
		t.Fatal(err)
	}
}

func TestAFailureOfTheNodesOwnStopsIt(t *testing.T) {
	n := newNode(t, "fsn-1", store.RoleWorker)
	if err := n.cluster.Bootstrap(ctx(t), "alice"); err != nil {
		t.Fatal(err)
	}
	f := &fsm{store: n.store, fatal: func(err error) { n.fatal <- err }}
	if response := f.Apply(&raft.Log{Type: raft.LogCommand, Index: 1000, Data: []byte("not a batch")}); response == nil {
		t.Fatal("garbage was applied")
	}
	select {
	case <-n.fatal:
	default:
		t.Fatal("the node was not stopped")
	}
	// Nothing after it is applied.
	data, _ := store.EncodeBatch(store.Batch{})
	if response := f.Apply(&raft.Log{Type: raft.LogCommand, Index: 1001, Data: data}); response != errBroken {
		t.Fatalf("after a failure: %v", response)
	}
}

func TestRaftLogsAtItsLevels(t *testing.T) {
	var out bytes.Buffer
	logger := raftLogger(slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})))
	logger.Error("failed to accept connection", "error", "closed")
	logger.Info("entering follower state")
	logger.Debug("hidden")
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], `"level":"ERROR","msg":"failed to accept connection","component":"raft","error":"closed"`) || !strings.Contains(lines[1], `"level":"INFO"`) {
		t.Fatalf("logged:\n%s", out.String())
	}
}
