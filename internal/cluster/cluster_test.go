package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"os"
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
	dir     string
	key     *secrets.Key
	address string
	store   *store.SQLite
	cluster *Cluster
	fatal   chan error
}

func newNode(t *testing.T, name string) *node {
	t.Helper()
	n := &node{t: t, name: name, dir: t.TempDir(), key: masterKey(t, 7), address: "127.0.0.1:0", fatal: make(chan error, 1)}
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
		Node: n.name, Address: n.address, Dir: filepath.Join(n.dir, "raft"),
		Seed: n.key.Derive("cluster ca"), Store: st,
		Log:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Fatal: func(err error) { n.fatal <- err },
		raft: func(config *raft.Config) {
			// Raft's own defaults: a loaded test host with the race
			// detector misses shorter heartbeats and elects for nothing.
			config.HeartbeatTimeout = time.Second
			config.ElectionTimeout = time.Second
			config.LeaderLeaseTimeout = 500 * time.Millisecond
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
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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

// threeNodes starts a cluster of three nodes.
func threeNodes(t *testing.T) []*node {
	t.Helper()
	worker := newNode(t, "fsn-1")
	if err := worker.cluster.Bootstrap(ctx(t), "alice"); err != nil {
		t.Fatal(err)
	}
	nodes := []*node{worker}
	for _, name := range []string{"nbg-1", "hel-1"} {
		witness := newNode(t, name)
		if err := witness.cluster.Join(ctx(t), worker.address, "alice"); err != nil {
			t.Fatalf("join %s: %v", name, err)
		}
		// Join returns once the node follows a leader (under load,
		// leadership may have moved meanwhile).
		if status, err := witness.cluster.Status(); err != nil || status.Leader == "" {
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
	if status.Leader != "fsn-1" || status.State != "Follower" || len(status.Members) != 3 ||
		status.Members[0] != (Member{Name: "fsn-1", Address: worker.address, Suffrage: "Voter"}) {
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
	worker := newNode(t, "fsn-1")
	if err := worker.cluster.Bootstrap(ctx(t), "alice"); err != nil {
		t.Fatal(err)
	}
	if err := worker.cluster.Bootstrap(ctx(t), "alice"); err == nil || !strings.Contains(err.Error(), "already part of a cluster") {
		t.Fatalf("bootstrap twice: %v", err)
	}
	second := newNode(t, "nbg-1")
	if err := second.cluster.Join(ctx(t), worker.address, "alice"); err != nil {
		t.Fatal(err)
	}

	// A node outside a cluster says so at once.
	lone := newNode(t, "hel-1")
	start := time.Now()
	if _, err := lone.store.PutApp(store.App{Name: "web", Spec: appSpec()}, "alice"); err == nil || !strings.Contains(err.Error(), "not part of a cluster") || time.Since(start) > 5*time.Second {
		t.Fatalf("a write outside a cluster after %v: %v", time.Since(start), err)
	}
}

func TestANodeWithStateCannotJoin(t *testing.T) {
	worker := newNode(t, "fsn-1")
	if err := worker.cluster.Bootstrap(ctx(t), "alice"); err != nil {
		t.Fatal(err)
	}
	other := newNode(t, "hel-1")
	if err := other.cluster.Bootstrap(ctx(t), "bob"); err != nil {
		t.Fatal(err)
	}
	if err := other.cluster.Join(ctx(t), worker.address, "bob"); err == nil || !strings.Contains(err.Error(), "already part of a cluster") {
		t.Fatalf("a node of another cluster joined: %v", err)
	}
}

func TestAnotherMasterKeyIsNotTheCluster(t *testing.T) {
	worker := newNode(t, "fsn-1")
	if err := worker.cluster.Bootstrap(ctx(t), "alice"); err != nil {
		t.Fatal(err)
	}
	stranger := &node{t: t, name: "evil", dir: t.TempDir(), key: masterKey(t, 8), address: "127.0.0.1:0", fatal: make(chan error, 1)}
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
	n := newNode(t, "fsn-1")
	if err := n.cluster.Bootstrap(ctx(t), "alice"); err != nil {
		t.Fatal(err)
	}
	f := &fsm{store: n.store, fatal: func(err error) { n.fatal <- err }}
	// Every node refuses garbage alike; that stops nobody.
	if response := f.Apply(&raft.Log{Type: raft.LogCommand, Index: 999, Data: []byte("not a batch")}); response == nil {
		t.Fatal("garbage was applied")
	}
	select {
	case err := <-n.fatal:
		t.Fatalf("garbage stopped the node: %v", err)
	default:
	}
	// A batch of a newer release this node cannot apply stops it.
	newer, _ := store.EncodeBatch(store.Batch{Schema: 99})
	if response := f.Apply(&raft.Log{Type: raft.LogCommand, Index: 1000, Data: newer}); response == nil {
		t.Fatal("a newer release's batch was applied")
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

func TestANodeOfAnotherClusterCannotJoin(t *testing.T) {
	// A witness of a cluster since rebuilt keeps its old database: it must
	// not join the new cluster with it.
	old := newNode(t, "fsn-1")
	if err := old.cluster.Bootstrap(ctx(t), "alice"); err != nil {
		t.Fatal(err)
	}
	witness := newNode(t, "hel-1")
	if err := witness.cluster.Join(ctx(t), old.address, "alice"); err != nil {
		t.Fatal(err)
	}
	// The old cluster is gone; the witness's log is moved aside.
	old.stop()
	witness.stop()
	if err := os.RemoveAll(filepath.Join(witness.dir, "raft")); err != nil {
		t.Fatal(err)
	}
	witness.start()

	rebuilt := newNode(t, "nbg-1")
	if err := rebuilt.cluster.Bootstrap(ctx(t), "alice"); err != nil {
		t.Fatal(err)
	}
	err := witness.cluster.Join(ctx(t), rebuilt.address, "alice")
	if !errors.Is(err, ErrMember) || !strings.Contains(err.Error(), "belongs to cluster") {
		t.Fatalf("a node of another cluster joined: %v", err)
	}
	if !reflect.DeepEqual(rebuilt.store.Nodes()[0].Name, "nbg-1") || len(rebuilt.store.Nodes()) != 1 {
		t.Fatalf("the rebuilt cluster took the node: %+v", rebuilt.store.Nodes())
	}
}

func TestANameIsNotTakenOver(t *testing.T) {
	worker := newNode(t, "fsn-1")
	if err := worker.cluster.Bootstrap(ctx(t), "alice"); err != nil {
		t.Fatal(err)
	}
	witness := newNode(t, "hel-1")
	if err := witness.cluster.Join(ctx(t), worker.address, "alice"); err != nil {
		t.Fatal(err)
	}
	impostor := newNode(t, "hel-1")
	if err := impostor.cluster.Join(ctx(t), worker.address, "mallory"); err == nil || !strings.Contains(err.Error(), "already a member at "+witness.address) {
		t.Fatalf("a second node named hel-1: %v", err)
	}
	status, err := worker.cluster.Status()
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Members) != 2 || status.Members[1].Address != witness.address {
		t.Fatalf("members = %+v", status.Members)
	}
}

func TestBootstrapFinishesWhatItStarted(t *testing.T) {
	n := newNode(t, "fsn-1")
	if err := n.store.StartLog(); err != nil {
		t.Fatal(err)
	}
	// Stopped after Raft took the configuration, before the state did.
	configuration := raft.Configuration{Servers: []raft.Server{{Suffrage: raft.Voter, ID: "fsn-1", Address: raft.ServerAddress(n.address)}}}
	if err := n.cluster.raft.BootstrapCluster(configuration).Error(); err != nil {
		t.Fatal(err)
	}
	if err := n.cluster.Bootstrap(ctx(t), "alice"); err != nil {
		t.Fatal(err)
	}
	if n.store.Cluster() == "" || len(n.store.Nodes()) != 1 {
		t.Fatalf("cluster %q, nodes %+v", n.store.Cluster(), n.store.Nodes())
	}
	if err := n.cluster.Bootstrap(ctx(t), "alice"); !errors.Is(err, ErrMember) {
		t.Fatalf("bootstrap a finished cluster: %v", err)
	}
}

func TestTheLeaderAppendsOnlyWhatEveryNodeCanApply(t *testing.T) {
	nodes := threeNodes(t)
	before := nodes[0].cluster.raft.LastIndex()
	for _, body := range [][]byte{[]byte("garbage"), mustEncode(t, store.Batch{Schema: 99})} {
		err := nodes[1].cluster.toLeader(ctx(t), "POST", "/cluster/v1/commit", body, nil)
		var peer *PeerError
		if !errors.As(err, &peer) || peer.Status != 400 {
			t.Fatalf("the leader took %q: %v", body, err)
		}
	}
	if after := nodes[0].cluster.raft.LastIndex(); after != before {
		t.Fatalf("the log grew from %d to %d", before, after)
	}
	select {
	case err := <-nodes[2].fatal:
		t.Fatalf("a node stopped: %v", err)
	default:
	}
}

func mustEncode(t *testing.T, batch store.Batch) []byte {
	t.Helper()
	data, err := store.EncodeBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestABrokenNodeTakesNoSnapshot(t *testing.T) {
	n := newNode(t, "fsn-1")
	f := &fsm{store: n.store, fatal: func(error) {}}
	f.broken.Store(true)
	if _, err := f.Snapshot(); !errors.Is(err, errBroken) {
		t.Fatalf("snapshot of a broken node: %v", err)
	}
}

func TestHandshakesAreBounded(t *testing.T) {
	n := newNode(t, "fsn-1")
	// Connections that never handshake hold every slot; the next one is
	// closed at once.
	var idle []net.Conn
	for range handshakes {
		conn, err := net.Dial("tcp", n.address)
		if err != nil {
			t.Fatal(err)
		}
		idle = append(idle, conn)
	}
	defer func() {
		for _, conn := range idle {
			conn.Close()
		}
	}()
	eventually(t, "every slot is taken", func() bool { return len(n.cluster.listener.pending) == handshakes })
	extra, err := net.Dial("tcp", n.address)
	if err != nil {
		t.Fatal(err)
	}
	defer extra.Close()
	_ = extra.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := extra.Read(make([]byte, 1)); err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("a connection beyond the bound was kept: %v", err)
	}
	// Once they give up, the port takes nodes again.
	for _, conn := range idle {
		conn.Close()
	}
	eventually(t, "the slots are free", func() bool { return len(n.cluster.listener.pending) == 0 })
}
