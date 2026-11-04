package cluster

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

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
	// tune changes Raft's settings after the tests' own.
	tune func(*raft.Config)
	// replicas keeps copies of other nodes' tenants; none if nil.
	replicas Replicas
	// stable wraps where Raft keeps its term; nil leaves it.
	stable func(raft.StableStore) raft.StableStore
}

func newNode(t *testing.T, name string) *node {
	t.Helper()
	return newTunedNode(t, name, nil)
}

func newTunedNode(t *testing.T, name string, tune func(*raft.Config)) *node {
	t.Helper()
	return newNodeWith(t, name, func(n *node) { n.tune = tune })
}

// newNodeWith starts a node that set has changed first.
func newNodeWith(t *testing.T, name string, set func(*node)) *node {
	t.Helper()
	n := &node{t: t, name: name, dir: t.TempDir(), address: "127.0.0.1:0", fatal: make(chan error, 1)}
	set(n)
	n.key = keyFile(t, filepath.Join(n.dir, "master.key"), 7)
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
		Seeds: n.key.DeriveAll("cluster ca"), Store: st, Replicas: n.replicas, Keys: nodeKeys{n},
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Fatal:  func(err error) { n.fatal <- err },
		stable: n.stable,
		raft: func(config *raft.Config) {
			// Raft's own defaults: a loaded test host with the race
			// detector misses shorter heartbeats and elects for nothing.
			config.HeartbeatTimeout = time.Second
			config.ElectionTimeout = time.Second
			config.LeaderLeaseTimeout = 500 * time.Millisecond
			config.CommitTimeout = 5 * time.Millisecond
			if n.tune != nil {
				n.tune(config)
			}
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

// stopTimeout bounds stopping a node: Raft's Shutdown waits for every one
// of its goroutines, and one that never ends would hold the test until the
// whole run times out.
const stopTimeout = time.Minute

func (n *node) stop() {
	if n.cluster == nil {
		return
	}
	closed := make(chan struct{})
	go func(c *Cluster) {
		_ = c.Close()
		close(closed)
	}(n.cluster)
	select {
	case <-closed:
		_ = n.store.Close()
	case <-time.After(stopTimeout):
		// The store stays open: Raft may still apply to it.
		stacks := make([]byte, 4<<20)
		stacks = stacks[:runtime.Stack(stacks, true)]
		n.t.Errorf("%s did not stop within %s; goroutines:\n%s", n.name, stopTimeout, stacks)
	}
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

// eventuallyFor bounds a wait for the cluster to get somewhere: long, for
// a loaded host with the race detector.
const eventuallyFor = 60 * time.Second

// eventually waits for check to pass.
func eventually(t *testing.T, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(eventuallyFor)
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
	return threeTunedNodes(t, nil)
}

// threeTunedNodes starts a cluster of three nodes with Raft's settings
// changed by tune.
func threeTunedNodes(t *testing.T, tune func(*raft.Config)) []*node {
	t.Helper()
	worker := newTunedNode(t, "fsn-1", tune)
	if err := worker.cluster.Bootstrap(ctx(t), "alice"); err != nil {
		t.Fatal(err)
	}
	nodes := []*node{worker}
	for _, name := range []string{"nbg-1", "hel-1"} {
		witness := newTunedNode(t, name, tune)
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

// raftStats is what Raft says of n, for a failure to show.
func raftStats(n *node) map[string]string {
	stats := n.cluster.raft.Stats()
	keep := map[string]string{}
	for _, key := range []string{"state", "term", "last_log_index", "commit_index", "applied_index", "last_snapshot_index", "last_contact"} {
		keep[key] = stats[key]
	}
	return keep
}

func sameState(t *testing.T, nodes []*node) {
	t.Helper()
	first := nodes[0].store
	for _, n := range nodes[1:] {
		deadline := time.Now().Add(eventuallyFor)
		for n.store.Applied() != first.Applied() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting until %s caught up: it applied %d, %s %d\n%s: %v\n%s: %v", n.name, n.store.Applied(), nodes[0].name, first.Applied(),
					n.name, raftStats(n), nodes[0].name, raftStats(nodes[0]))
			}
			time.Sleep(20 * time.Millisecond)
		}
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

// A node down for longer than the others keep their log catches up from a
// snapshot, as Raft installs it from the leader, and goes on applying
// entries after it.
func TestANodeFarBehindCatchesUpFromASnapshot(t *testing.T) {
	nodes := threeTunedNodes(t, func(config *raft.Config) {
		config.SnapshotThreshold = 4
		config.TrailingLogs = 2
		config.SnapshotInterval = 100 * time.Millisecond
	})
	if _, err := nodes[0].store.PutApp(store.App{Name: "web", Spec: appSpec()}, "alice"); err != nil {
		t.Fatal(err)
	}
	sameState(t, nodes)
	behind := nodes[2]
	appliedAtStop := behind.store.Applied()
	behind.stop()
	for i := range 20 {
		if _, err := nodes[i%2].store.Create(store.NewTenant{Name: fmt.Sprintf("t%02d", i), App: "web"}, "bob"); err != nil {
			t.Fatal(err)
		}
	}
	// The log no longer has what the node missed.
	for _, n := range nodes[:2] {
		eventually(t, n.name+" compacted its log", func() bool {
			first, err := n.cluster.logs.FirstIndex()
			return err == nil && first > appliedAtStop+1
		})
	}
	behind.start()
	sameState(t, nodes)
	// Raft restores the snapshot into the store first and records it only
	// afterwards: the state can match before the record does.
	eventually(t, behind.name+" caught up from a snapshot newer than what it had", func() bool {
		installed, _ := strconv.ParseUint(behind.cluster.raft.Stats()["last_snapshot_index"], 10, 64)
		return installed > appliedAtStop
	})
	if len(behind.store.List()) != 20 {
		t.Fatalf("%s has %d tenants", behind.name, len(behind.store.List()))
	}
	// And it applies what comes next, as the others do.
	if _, err := behind.store.SetSuspended("t07", true, "carol"); err != nil {
		t.Fatal(err)
	}
	sameState(t, nodes)
	// A state installed from a snapshot hashes as one applied from the log.
	ours, err := behind.store.Digest(ctx(t))
	if err != nil {
		t.Fatal(err)
	}
	theirs, err := nodes[0].store.Digest(ctx(t))
	if err != nil || !ours.Comparable(theirs) || ours.Hash != theirs.Hash {
		t.Fatalf("digests after a snapshot: %+v and %+v, %v", ours, theirs, err)
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

// A node brought back with a new database at a member's name and address
// joins only once the member was removed: its empty log, as a voter, would
// let a node that lacks committed entries be elected.
func TestAMemberIsRemovedBeforeItJoinsAgain(t *testing.T) {
	nodes := threeNodes(t)
	old := nodes[2]
	old.stop()
	again := &node{t: t, name: old.name, dir: t.TempDir(), key: masterKey(t, 7), address: old.address, fatal: make(chan error, 1)}
	if err := store.Init(filepath.Join(again.dir, "murder.db"), again.name); err != nil {
		t.Fatal(err)
	}
	again.start()
	t.Cleanup(again.stop)
	if err := again.cluster.Join(ctx(t), nodes[0].address, "alice"); err == nil || !strings.Contains(err.Error(), "murder cluster remove "+old.name) {
		t.Fatalf("joining over a member: %v", err)
	}
	if err := nodes[0].cluster.Remove(ctx(t), old.name, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := again.cluster.Join(ctx(t), nodes[0].address, "alice"); err != nil {
		t.Fatalf("joining once removed: %v", err)
	}
	sameState(t, []*node{nodes[0], nodes[1], again})
}

// Neither a node of another cluster with the same master key, such as the
// one a backup was restored from, nor a node removed from this one reaches
// it: Raft refuses its connections, and the cluster's endpoints its
// requests.
func TestOnlyMembersOfTheClusterReachIt(t *testing.T) {
	nodes := threeNodes(t)
	other := newNode(t, "fsn-1")
	if err := other.cluster.Bootstrap(ctx(t), "mallory"); err != nil {
		t.Fatal(err)
	}
	if other.store.Cluster() == nodes[0].store.Cluster() {
		t.Fatal("two clusters of one name")
	}
	// Raft: a connection naming another cluster is closed unread.
	conn, err := other.cluster.listener.raftLayer().Dial(raft.ServerAddress(nodes[0].address), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("a Raft connection of another cluster: %v", err)
	}
	conn.Close()
	// Its requests are refused, and so are those of a node removed.
	asked := func(from *node, what string) {
		t.Helper()
		request, _ := http.NewRequestWithContext(ctx(t), http.MethodGet, "https://"+nodes[1].address+"/cluster/v1/node", nil)
		response, err := from.cluster.client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusForbidden {
			t.Fatalf("%s: %s", what, response.Status)
		}
	}
	asked(other, "a node of another cluster")
	removed := nodes[2]
	if err := nodes[0].cluster.Remove(ctx(t), removed.name, "alice"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the removal reaches nbg-1", func() bool { return len(nodes[1].store.Nodes()) == 2 })
	asked(removed, "a removed node")
}

// recordedReplicas answers for copies and records who asked.
type recordedReplicas struct {
	mu      sync.Mutex
	senders []string
	data    string
}

func (r *recordedReplicas) Snapshots(_ context.Context, sender, tenant string) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.senders = append(r.senders, sender+" asks for "+tenant)
	return []string{"murder-replica-1"}, nil
}

func (r *recordedReplicas) Receive(_ context.Context, sender, tenant string, whole bool, data io.Reader) error {
	body, err := io.ReadAll(data)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.senders = append(r.senders, fmt.Sprintf("%s sends %s whole=%v", sender, tenant, whole))
	r.data = string(body)
	return err
}

// A node keeping copies learns from the cluster certificate which node
// sends them; a node that keeps none says so.
func TestCopiesKnowTheirSender(t *testing.T) {
	nodes := threeNodes(t)
	replicas := &recordedReplicas{}
	nodes[1].stop()
	nodes[1].replicas = replicas
	nodes[1].start()
	snapshots, err := nodes[0].cluster.SnapshotsOn(ctx(t), "nbg-1", "acme")
	if err != nil || !slices.Equal(snapshots, []string{"murder-replica-1"}) {
		t.Fatalf("snapshots: %q, %v", snapshots, err)
	}
	if err := nodes[0].cluster.ReplicateOn(ctx(t), "nbg-1", "acme", true, strings.NewReader("stream")); err != nil {
		t.Fatal(err)
	}
	replicas.mu.Lock()
	senders, data := replicas.senders, replicas.data
	replicas.mu.Unlock()
	if !slices.Equal(senders, []string{"fsn-1 asks for acme", "fsn-1 sends acme whole=true"}) || data != "stream" {
		t.Fatalf("senders %v, data %q", senders, data)
	}
	if _, err := nodes[0].cluster.SnapshotsOn(ctx(t), "hel-1", "acme"); err == nil {
		t.Fatal("a node that keeps no copies answered for one")
	}
}

// The leader compares every node's state with its own: alike while they
// apply the same log, different once one's changed around it.
func TestTheLeaderFindsAStateThatDrifted(t *testing.T) {
	nodes := threeNodes(t)
	// Leadership may move under load: each comparison asks whoever leads.
	leader := func() *node {
		for _, n := range nodes {
			if n.cluster.Leader() {
				return n
			}
		}
		return nil
	}
	compare := func() (*node, map[string]bool) {
		var found map[string]bool
		var by *node
		eventually(t, "a leader compares the nodes", func() bool {
			by = leader()
			if by == nil {
				return false
			}
			found = by.cluster.CompareDigests(ctx(t))
			return len(found) == 2
		})
		return by, found
	}
	if _, err := nodes[0].store.PutApp(store.App{Name: "web", Spec: appSpec()}, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := nodes[0].store.Create(store.NewTenant{Name: "acme", App: "web"}, "alice"); err != nil {
		t.Fatal(err)
	}
	sameState(t, nodes)
	by, found := compare()
	for name, diverged := range found {
		if diverged {
			t.Fatalf("%s diverged from %s with one state: %v", name, by.name, found)
		}
	}
	for _, n := range nodes {
		if n != leader() && n.cluster.CompareDigests(ctx(t)) != nil && n != leader() {
			t.Fatalf("%s, not the leader, compared states", n.name)
		}
	}
	// Every node but one drifts alike, so whichever leads finds that one.
	drifted := nodes[2]
	db, err := sql.Open("sqlite", filepath.Join(drifted.dir, "murder.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE tenants SET node = 'elsewhere' WHERE name = 'acme'`); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the leader finds "+drifted.name+" drifted", func() bool {
		by := leader()
		if by == nil {
			return false
		}
		found := by.cluster.CompareDigests(ctx(t))
		if by == drifted {
			return len(found) == 2 && found[nodes[0].name] && found[nodes[1].name]
		}
		return found[drifted.name]
	})
	var watched Digests
	watch, stop := context.WithCancel(ctx(t))
	defer stop()
	for _, n := range nodes {
		go n.cluster.WatchDigests(watch, 10*time.Millisecond, &Digests{})
	}
	go func() {
		for watch.Err() == nil {
			if by := leader(); by != nil && by != drifted {
				by.cluster.WatchDigests(watch, 10*time.Millisecond, &watched)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	eventually(t, "the watch finds the drift", func() bool { return watched.Diverged()[drifted.name] })
}

// stalled is a Raft future that never ends.
type stalled struct{}

func (stalled) Error() error { select {} }

// A log that stalls does not hold a write for good.
func TestAFutureThatNeverEndsIsGivenUp(t *testing.T) {
	start := time.Now()
	if err := waitFuture(stalled{}, 50*time.Millisecond); !errors.Is(err, errStalled) {
		t.Fatalf("waited: %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("waited too long")
	}
}

type answered struct{}

func (answered) Error() error { return nil }

// A VerifyLeader that Raft does not take, as its queue is full, holds
// neither its caller nor more than one goroutine.
func TestAVerifyLeaderRaftDoesNotTakeIsGivenUp(t *testing.T) {
	slot := make(chan struct{}, 1)
	unblock := make(chan struct{})
	var calls atomic.Int32
	blocked := func() raft.Future {
		calls.Add(1)
		<-unblock
		return answered{}
	}
	for range 3 {
		start := time.Now()
		if err := verifyLeader(slot, blocked, 50*time.Millisecond); !errors.Is(err, errStalled) {
			t.Fatalf("verified: %v", err)
		}
		if time.Since(start) > time.Second {
			t.Fatal("waited too long")
		}
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("%d calls wait on Raft, want 1", n)
	}
	// Once Raft takes it, the next caller goes through.
	close(unblock)
	eventually(t, "the slot is free", func() bool {
		return verifyLeader(slot, func() raft.Future { return answered{} }, time.Second) == nil
	})
	if err := verifyLeader(slot, func() raft.Future { return stalled{} }, 50*time.Millisecond); !errors.Is(err, errStalled) {
		t.Fatalf("an answer that never comes: %v", err)
	}
}

// joinPause holds the first join that gets to a stage there, on whichever
// node takes it: leadership may move before the join gets anywhere.
type joinPause struct {
	stage   string
	nodes   []*node
	paused  chan *node
	release chan struct{}
	resumed sync.Once
	// reached counts the joins that got to the stage.
	reached atomic.Int32
}

func pauseJoins(t *testing.T, nodes []*node, stage string) *joinPause {
	t.Helper()
	p := &joinPause{stage: stage, nodes: nodes, paused: make(chan *node, 1), release: make(chan struct{})}
	// Only the first is held; later ones, on any node, go on.
	var held atomic.Bool
	for _, n := range nodes {
		hook := func(at, _ string) {
			if at != stage {
				return
			}
			p.reached.Add(1)
			if held.CompareAndSwap(false, true) {
				p.paused <- n
				<-p.release
			}
		}
		n.cluster.joinHook.Store(&hook)
	}
	// Before the nodes stop: a join held would hold them.
	t.Cleanup(p.resume)
	return p
}

// wait returns the node the join is held on, and another one.
func (p *joinPause) wait(t *testing.T, joined <-chan error) (held, other *node) {
	t.Helper()
	select {
	case held = <-p.paused:
	case err := <-joined:
		t.Fatalf("the join ended before it got to stage %s: %v", p.stage, err)
	case <-time.After(eventuallyFor):
		t.Fatalf("the join never got to stage %s", p.stage)
	}
	for _, n := range p.nodes {
		if n != held {
			return held, n
		}
	}
	panic("one node")
}

func (p *joinPause) resume() { p.resumed.Do(func() { close(p.release) }) }

// asLeader runs op with to leading, handing it the leadership first, and
// again after an attempt that timed out: under load the leadership can
// move back to the node holding a join, where op waits behind it until
// it gives up.
func asLeader(t *testing.T, nodes []*node, to *node, what string, op func(context.Context) error) {
	t.Helper()
	deadline := time.Now().Add(2 * eventuallyFor)
	for {
		handOver(t, nodes, to)
		attempt, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		err := op(attempt)
		cancel()
		if err == nil {
			return
		}
		if !errors.Is(err, context.DeadlineExceeded) || time.Now().After(deadline) {
			t.Fatalf("%s: %v", what, err)
		}
	}
}

// joinedAgain is a join's outcome, asked again: a node an attempt that
// timed out made a member has joined.
func joinedAgain(err error) error {
	if errors.Is(err, ErrMember) {
		return nil
	}
	return err
}

// handOver has to lead, asking whichever of nodes leads to hand over. A
// transfer that does not finish within Raft's election timeout, as on a
// loaded host, is asked for again.
func handOver(t *testing.T, nodes []*node, to *node) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !to.cluster.Leader() {
		if time.Now().After(deadline) {
			t.Fatalf("%s never took over the leadership", to.name)
		}
		for _, n := range nodes {
			if n != to && n.cluster.Leader() {
				_ = n.cluster.raft.LeadershipTransferToServer(raft.ServerID(to.name), raft.ServerAddress(to.address)).Error()
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// awaitJoin waits for a join a test resumed; Join gives up within its
// context's minute, so a join that outlasts that is a failure, not a wait.
func awaitJoin(t *testing.T, joined <-chan error) error {
	t.Helper()
	select {
	case err := <-joined:
		return err
	case <-time.After(90 * time.Second):
		t.Fatal("the resumed join never returned")
		return nil
	}
}

// A join keeps to the term it started in: a leader that lost its
// leadership and won it back, while the leader between removed the node,
// does not make a voter the state does not know. The host asks again and
// joins whole.
func TestAJoinAcrossTwoLeadershipChangesMakesNoUnknownVoter(t *testing.T) {
	for _, stage := range []string{joinVote, joinAdd} {
		t.Run(stage, func(t *testing.T) {
			nodes := threeNodes(t)
			pause := pauseJoins(t, nodes, stage)
			x := newNode(t, "x-1")
			joined := make(chan error, 1)
			go func() { joined <- x.cluster.Join(ctx(t), nodes[0].address, "alice") }()
			old, next := pause.wait(t, joined)
			asLeader(t, nodes, next, "remove x-1", func(ctx context.Context) error {
				return next.cluster.Remove(ctx, "x-1", "bob")
			})
			eventually(t, old.name+" applied the removal", func() bool {
				_, known := nodeNamed(old.store.Nodes(), "x-1")
				return !known
			})
			handOver(t, nodes, old)
			pause.resume()
			if err := awaitJoin(t, joined); err != nil {
				t.Fatalf("the join, resumed: %v", err)
			}
			if pause.reached.Load() < 2 {
				t.Fatal("the join was not asked again")
			}
			for _, n := range append(nodes, x) {
				eventually(t, n.name+" names x-1 in the state and the voters at one address", func() bool {
					node, member := nodeNamed(n.store.Nodes(), "x-1")
					voter, isVoter := n.cluster.voterAddress("x-1")
					return member && isVoter && node.Address == x.address && voter == x.address
				})
			}
			sameState(t, append(nodes, x))
		})
	}
}

// A join that waited while leadership moved does not move a node the next
// leader added meanwhile: the state and the voters keep naming one
// address.
func TestAJoinAcrossALeadershipChangeKeepsOneAddress(t *testing.T) {
	nodes := threeNodes(t)
	pause := pauseJoins(t, nodes, joinWrite)
	first, second := newNode(t, "x-1"), newNode(t, "x-1")
	joined := make(chan error, 1)
	go func() { joined <- first.cluster.Join(ctx(t), nodes[0].address, "mallory") }()
	_, next := pause.wait(t, joined)
	asLeader(t, nodes, next, "the second join", func(ctx context.Context) error {
		return joinedAgain(second.cluster.Join(ctx, next.address, "alice"))
	})
	pause.resume()
	if err := awaitJoin(t, joined); err == nil || !strings.Contains(err.Error(), "already a member at "+second.address) {
		t.Fatalf("the first join, resumed: %v", err)
	}
	for _, n := range nodes {
		eventually(t, n.name+" names x-1 at the second's address", func() bool {
			address, member := n.cluster.memberAddress("x-1")
			voter, isVoter := n.cluster.voterAddress("x-1")
			return member && isVoter && address == second.address && voter == second.address
		})
	}
	sameState(t, nodes)
}

// A join that waited across two leadership changes, while its node was
// removed and another of the same name joined at another address, leaves
// that other one where it is, in the state and the voters: whether it
// waited part way through reading the voters (reading) or after checking
// them (add), what it read is not what Raft has by then, and Raft refuses
// its change.
func TestAStaleJoinLeavesAReplacementInPlace(t *testing.T) {
	for _, stage := range []string{joinReading, joinAdd} {
		t.Run(stage, func(t *testing.T) { staleJoin(t, stage) })
	}
}

func staleJoin(t *testing.T, stage string) {
	nodes := threeNodes(t)
	pause := pauseJoins(t, nodes, stage)
	stale := newNode(t, "x-1")
	joined := make(chan error, 1)
	go func() { joined <- stale.cluster.Join(ctx(t), nodes[0].address, "alice") }()
	old, next := pause.wait(t, joined)
	asLeader(t, nodes, next, "remove x-1", func(ctx context.Context) error {
		return next.cluster.Remove(ctx, "x-1", "bob")
	})
	// Another host joins as x-1, through the new leader.
	replacement := newNode(t, "x-1")
	asLeader(t, nodes, next, "the replacement joins", func(ctx context.Context) error {
		return joinedAgain(replacement.cluster.Join(ctx, next.address, "bob"))
	})
	eventually(t, old.name+" knows the replacement", func() bool {
		node, known := nodeNamed(old.store.Nodes(), "x-1")
		return known && node.Address == replacement.address
	})
	handOver(t, nodes, old)
	pause.resume()
	_ = awaitJoin(t, joined)
	for _, n := range nodes {
		eventually(t, n.name+" names the replacement in the state and the voters", func() bool {
			node, member := nodeNamed(n.store.Nodes(), "x-1")
			voter, isVoter := n.cluster.voterAddress("x-1")
			return member && isVoter && node.Address == replacement.address && voter == replacement.address
		})
	}
	// Nor was the replacement's voter moved to the stale host meanwhile,
	// to be moved back: Raft refused the stale join's change.
	for _, configuration := range configurations(t, old) {
		if address, voter := serverAddress(configuration, "x-1"); voter && address == stale.address {
			t.Fatalf("the voters named x-1 at the stale host's address: %+v", configuration.Servers)
		}
	}
}

// configurations are the configurations of the voters in n's log.
func configurations(t *testing.T, n *node) []raft.Configuration {
	t.Helper()
	first, err := n.cluster.logs.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	last, err := n.cluster.logs.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	var found []raft.Configuration
	for index := first; index <= last && index > 0; index++ {
		var entry raft.Log
		if err := n.cluster.logs.GetLog(index, &entry); err != nil {
			t.Fatal(err)
		}
		if entry.Type == raft.LogConfiguration {
			found = append(found, raft.DecodeConfiguration(entry.Data))
		}
	}
	return found
}

// tcpStream is a plain TCP stream layer for Raft's transport.
type tcpStream struct{ net.Listener }

func (s tcpStream) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(address), timeout)
}

// Replication sends a follower one request at a time: Raft's pipeline can
// wedge the leader's replication to a follower, and its Shutdown, for good
// once the follower answers with a newer term, as after a leadership
// transfer. Under load the join tests, which transfer leadership, hung
// there.
func TestReplicationIsNotPipelined(t *testing.T) {
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	transport := raft.NewNetworkTransportWithConfig(transportConfig(tcpStream{inner}, hclog.NewNullLogger()))
	defer transport.Close()
	if pipeline, err := transport.AppendEntriesPipeline("nbg-1", raft.ServerAddress(inner.Addr().String())); !errors.Is(err, raft.ErrPipelineReplicationNotSupported) {
		if pipeline != nil {
			_ = pipeline.Close()
		}
		t.Fatalf("a pipeline to a follower: %v", err)
	}
}

// Closing a node waits for a heartbeat Raft is handling, and refuses later
// ones: Raft handles them outside what its Shutdown waits for, and one that
// saved the term to a closed log made Raft panic.
func TestNoHeartbeatOutlastsClose(t *testing.T) {
	transport := &gatedTransport{}
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	heartbeat := transport.gate(func(rpc raft.RPC) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		rpc.Respond(&raft.AppendEntriesResponse{}, nil)
	})
	rpc := func() (raft.RPC, chan raft.RPCResponse) {
		answer := make(chan raft.RPCResponse, 1)
		return raft.RPC{Command: &raft.AppendEntriesRequest{}, RespChan: answer}, answer
	}
	first, _ := rpc()
	go heartbeat(first)
	<-entered
	stopped := make(chan struct{})
	go func() {
		transport.stopHeartbeats()
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("heartbeats stopped while one was being handled")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("heartbeats did not stop once the one being handled was done")
	}
	later, answer := rpc()
	heartbeat(later)
	if response := <-answer; !errors.Is(response.Error, raft.ErrRaftShutdown) {
		t.Fatalf("a heartbeat after close: %+v", response)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("Raft handled %d heartbeats, want 1", n)
	}
}

// dialer is a stream layer that only dials, as the node it belongs to: a
// transport over it sends requests and takes none.
type dialer struct {
	*stream
	closed chan struct{}
	once   sync.Once
}

func (d *dialer) Accept() (net.Conn, error) {
	<-d.closed
	return nil, net.ErrClosed
}

func (d *dialer) Close() error {
	d.once.Do(func() { close(d.closed) })
	return nil
}

// slowTerms saves terms slowly, counting the saves under way.
type slowTerms struct {
	raft.StableStore
	saving *atomic.Int32
}

func (s slowTerms) SetUint64(key []byte, value uint64) error {
	s.saving.Add(1)
	defer s.saving.Add(-1)
	time.Sleep(100 * time.Millisecond)
	return s.StableStore.SetUint64(key, value)
}

// A node closed while heartbeats pour in does not crash: Raft saved the
// term of a heartbeat it was handling to a log Close had closed, and
// panicked.
func TestHeartbeatsDuringCloseDoNotCrash(t *testing.T) {
	for range 3 {
		// Saving a term takes a while, as on a loaded host: Close comes
		// while a heartbeat is saving one.
		var saving atomic.Int32
		n := newNodeWith(t, "fsn-1", func(n *node) {
			n.stable = func(inner raft.StableStore) raft.StableStore {
				return slowTerms{StableStore: inner, saving: &saving}
			}
		})
		if err := n.cluster.Bootstrap(ctx(t), "alice"); err != nil {
			t.Fatal(err)
		}
		// Heartbeats as from a leader of ever newer terms: each one has
		// the node save its term.
		client := raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{
			Stream:  &dialer{stream: &stream{l: n.cluster.listener, protocol: protocolRaft}, closed: make(chan struct{})},
			MaxPool: 8, Timeout: time.Second, Logger: hclog.NewNullLogger(),
		})
		var term atomic.Uint64
		term.Store(1000)
		stop := make(chan struct{})
		var senders sync.WaitGroup
		for range 8 {
			senders.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
					}
					request := &raft.AppendEntriesRequest{
						RPCHeader: raft.RPCHeader{ProtocolVersion: raft.ProtocolVersionMax, ID: []byte("nbg-1"), Addr: []byte("127.0.0.1:1")},
						Term:      term.Add(1),
					}
					_ = client.AppendEntries("fsn-1", raft.ServerAddress(n.address), request, &raft.AppendEntriesResponse{})
				}
			})
		}
		eventually(t, "a heartbeat is saving its term", func() bool { return saving.Load() > 0 })
		n.stop()
		close(stop)
		senders.Wait()
		_ = client.Close()
	}
}

// Once the log no longer holds the last configuration, the voters and
// their index come from the newest snapshot, as Raft has them: a node
// joins as before.
func TestAJoinAfterTheLogIsCompacted(t *testing.T) {
	nodes := threeTunedNodes(t, func(config *raft.Config) {
		config.SnapshotThreshold = 4
		config.TrailingLogs = 2
		config.SnapshotInterval = 100 * time.Millisecond
	})
	if _, err := nodes[0].store.PutApp(store.App{Name: "web", Spec: appSpec()}, "alice"); err != nil {
		t.Fatal(err)
	}
	for i := range 12 {
		if _, err := nodes[0].store.Create(store.NewTenant{Name: fmt.Sprintf("t%02d", i), App: "web"}, "bob"); err != nil {
			t.Fatal(err)
		}
	}
	var leader *node
	eventually(t, "the leader's log holds no configuration", func() bool {
		for _, n := range nodes {
			if n.cluster.Leader() {
				leader = n
			}
		}
		return leader != nil && len(configurations(t, leader)) == 0
	})
	configuration, index, err := leader.cluster.configuration()
	if err != nil {
		t.Fatal(err)
	}
	snapshots, err := leader.cluster.snapshots.List()
	if err != nil || len(snapshots) == 0 {
		t.Fatalf("snapshots: %v, %v", snapshots, err)
	}
	if want := leader.cluster.raft.GetConfiguration().Configuration(); !reflect.DeepEqual(configuration, want) || index != snapshots[0].ConfigurationIndex {
		t.Fatalf("configuration %+v at %d, want %+v at %d", configuration, index, want, snapshots[0].ConfigurationIndex)
	}
	x := newNode(t, "x-1")
	if err := x.cluster.Join(ctx(t), leader.address, "alice"); err != nil {
		t.Fatalf("join after compaction: %v", err)
	}
	eventually(t, "x-1 is a voter", func() bool {
		address, voter := leader.cluster.voterAddress("x-1")
		return voter && address == x.address
	})
}

// A change of the members queued behind another, whose sender gave up
// meanwhile, is not made once the other is done.
func TestAChangeWhoseSenderGaveUpIsNotMade(t *testing.T) {
	nodes := threeNodes(t)
	pause := pauseJoins(t, nodes, joinWrite)
	x := newNode(t, "x-1")
	joined := make(chan error, 1)
	go func() { joined <- x.cluster.Join(ctx(t), nodes[0].address, "alice") }()
	held, _ := pause.wait(t, joined)
	// y-1 asks the node holding x-1's join to add it, and gives up.
	y := newNode(t, "y-1")
	request := marshal(joinRequest{Address: y.address, Actor: "bob"})
	for {
		attempt, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := y.cluster.call(attempt, held.address, http.MethodPost, "/cluster/v1/nodes", request, nil)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			break
		}
		if !errors.Is(err, ErrNotLeader) {
			t.Fatalf("y-1's join: %v", err)
		}
		handOver(t, nodes, held)
	}
	pause.resume()
	if err := awaitJoin(t, joined); err != nil {
		t.Fatalf("x-1's join: %v", err)
	}
	time.Sleep(2 * time.Second)
	for _, n := range nodes {
		if _, known := nodeNamed(n.store.Nodes(), "y-1"); known {
			t.Fatalf("%s names y-1, whose join gave up", n.name)
		}
		if n.cluster.isVoter("y-1") {
			t.Fatalf("y-1, whose join gave up, is a voter on %s", n.name)
		}
	}
}

// A request whose sender gave up does not take the lock on the members,
// however the lock's select falls.
func TestAGivenUpChangeDoesNotTakeTheMembers(t *testing.T) {
	c := &Cluster{membership: make(chan struct{}, 1)}
	given, cancel := context.WithCancel(context.Background())
	cancel()
	for range 100 {
		r := httptest.NewRequest(http.MethodPost, "/cluster/v1/nodes", nil).WithContext(given)
		if c.lockMembership(httptest.NewRecorder(), r) {
			t.Fatal("a request whose sender gave up took the members")
		}
		if len(c.membership) != 0 {
			t.Fatal("a refused request left the members taken")
		}
	}
	r := httptest.NewRequest(http.MethodPost, "/cluster/v1/nodes", nil)
	if !c.lockMembership(httptest.NewRecorder(), r) {
		t.Fatal("a request whose sender waits did not take the members")
	}
	c.unlockMembership()
}

// discardFSM applies nothing, for a Raft whose configuration alone counts.
type discardFSM struct{}

func (discardFSM) Apply(*raft.Log) any                 { return nil }
func (discardFSM) Snapshot() (raft.FSMSnapshot, error) { return nil, errors.New("no snapshots") }
func (discardFSM) Restore(r io.ReadCloser) error       { return r.Close() }

// fastRaft is a Raft config for a lone voter that elects itself quickly.
func fastRaft(id string) *raft.Config {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(id)
	config.LogOutput = io.Discard
	config.HeartbeatTimeout = 50 * time.Millisecond
	config.ElectionTimeout = 50 * time.Millisecond
	config.LeaderLeaseTimeout = 50 * time.Millisecond
	return config
}

// leading waits for r to lead.
func leading(t *testing.T, r *raft.Raft) {
	t.Helper()
	eventually(t, "the lone voter leads", func() bool { return r.State() == raft.Leader })
}

// logOf is a log of entries 1 to last, those at the indexes of
// configurations making them, the others no-ops.
func logOf(last uint64, configurations map[uint64]raft.Configuration) []*raft.Log {
	var log []*raft.Log
	for index := uint64(1); index <= last; index++ {
		entry := &raft.Log{Index: index, Term: 1, Type: raft.LogNoop}
		if configuration, ok := configurations[index]; ok {
			entry = configurationEntry(index, configuration)
		}
		log = append(log, entry)
	}
	return log
}

// configurationEntry is a log entry that makes configuration.
func configurationEntry(index uint64, configuration raft.Configuration) *raft.Log {
	return &raft.Log{Index: index, Term: 1, Type: raft.LogConfiguration, Data: raft.EncodeConfiguration(configuration)}
}

// The voters and their index are what Raft has, however the log and the
// snapshot lie: Raft changes the voters given that index. In every case
// only a, alone, is the latest configuration, so a Raft that went by a
// stale one could not elect itself.
func TestTheVotersAreReadAsRaftHasThem(t *testing.T) {
	alone := raft.Configuration{Servers: []raft.Server{{ID: "a", Address: "a", Suffrage: raft.Voter}}}
	withB := raft.Configuration{Servers: []raft.Server{{ID: "a", Address: "a", Suffrage: raft.Voter}, {ID: "b", Address: "b", Suffrage: raft.Voter}}}
	type snapshot struct {
		index, configurationIndex uint64
		configuration             raft.Configuration
	}
	for _, test := range []struct {
		name     string
		log      []*raft.Log
		snapshot *snapshot
		index    uint64
	}{{
		name:  "the log alone",
		log:   logOf(4, map[uint64]raft.Configuration{1: withB, 3: alone}),
		index: 3,
	}, {
		name:     "a snapshot with no log after it, the log kept from before it",
		log:      logOf(10, map[uint64]raft.Configuration{1: withB}),
		snapshot: &snapshot{index: 10, configurationIndex: 5, configuration: alone},
		index:    5,
	}, {
		name:     "a snapshot with log after it, none of it configuration",
		log:      logOf(12, map[uint64]raft.Configuration{1: withB}),
		snapshot: &snapshot{index: 10, configurationIndex: 5, configuration: alone},
		index:    5,
	}, {
		name:     "a snapshot with a configuration in the log after it",
		log:      logOf(13, map[uint64]raft.Configuration{1: withB, 12: alone}),
		snapshot: &snapshot{index: 10, configurationIndex: 5, configuration: withB},
		index:    12,
	}} {
		t.Run(test.name, func(t *testing.T) {
			logs, err := raftboltdb.NewBoltStore(filepath.Join(t.TempDir(), "raft.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer logs.Close()
			for _, entry := range test.log {
				if err := logs.StoreLog(entry); err != nil {
					t.Fatal(err)
				}
			}
			snapshots := raft.NewInmemSnapshotStore()
			_, transport := raft.NewInmemTransport("a")
			if test.snapshot != nil {
				sink, err := snapshots.Create(raft.SnapshotVersionMax, test.snapshot.index, 1, test.snapshot.configuration, test.snapshot.configurationIndex, transport)
				if err != nil {
					t.Fatal(err)
				}
				if err := sink.Close(); err != nil {
					t.Fatal(err)
				}
			}
			r, err := raft.NewRaft(fastRaft("a"), discardFSM{}, logs, logs, snapshots, transport)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = r.Shutdown().Error() }()
			c := &Cluster{logs: logs, snapshots: snapshots}
			configuration, index, err := c.configuration()
			if err != nil {
				t.Fatal(err)
			}
			if want := r.GetConfiguration().Configuration(); !reflect.DeepEqual(configuration, want) || index != test.index {
				t.Fatalf("read %+v at %d, Raft has %+v at %d", configuration, index, want, test.index)
			}
			leading(t, r)
			if err := r.AddVoter("c", "c", index, time.Second).Error(); err != nil {
				t.Fatalf("Raft refused a change to the voters as read: %v", err)
			}
		})
	}
}

// After a snapshot from the leader replaced the history of the log it
// kept, the voters are read from the snapshot, not from that log: a node
// that leads then still changes them.
func TestAnInstalledSnapshotReplacesTheLogsVoters(t *testing.T) {
	logs, err := raftboltdb.NewBoltStore(filepath.Join(t.TempDir(), "raft.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	withB := raft.Configuration{Servers: []raft.Server{{ID: "a", Address: "a", Suffrage: raft.Voter}, {ID: "b", Address: "b", Suffrage: raft.Voter}}}
	alone := raft.Configuration{Servers: []raft.Server{{ID: "a", Address: "a", Suffrage: raft.Voter}}}
	entries := make([]*raft.Log, 0, 20000)
	for index := uint64(1); index <= 20000; index++ {
		entry := &raft.Log{Index: index, Term: 1, Type: raft.LogNoop}
		if index == 1 || index == 10000 {
			entry = configurationEntry(index, withB)
		}
		entries = append(entries, entry)
	}
	if err := logs.StoreLogs(entries); err != nil {
		t.Fatal(err)
	}
	snapshots := raft.NewInmemSnapshotStore()
	_, transport := raft.NewInmemTransport("a")
	_, sender := raft.NewInmemTransport("b")
	sender.Connect("a", transport)
	r, err := raft.NewRaft(fastRaft("a"), discardFSM{}, logs, logs, snapshots, transport)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Shutdown().Error() }()
	// b, leading in term 2, sends a snapshot whose voters are a alone.
	request := &raft.InstallSnapshotRequest{
		RPCHeader:       raft.RPCHeader{ProtocolVersion: raft.ProtocolVersionMax, ID: []byte("b"), Addr: []byte("b")},
		SnapshotVersion: raft.SnapshotVersionMax, Term: 2, LastLogIndex: 20000, LastLogTerm: 2,
		Configuration: raft.EncodeConfiguration(alone), ConfigurationIndex: 15000,
	}
	var response raft.InstallSnapshotResponse
	if err := sender.InstallSnapshot("a", "a", request, &response, bytes.NewReader(nil)); err != nil || !response.Success {
		t.Fatalf("install the snapshot: %+v, %v", response, err)
	}
	leading(t, r)
	if err := r.Barrier(time.Second).Error(); err != nil {
		t.Fatal(err)
	}
	c := &Cluster{logs: logs, snapshots: snapshots}
	configuration, index, err := c.configuration()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(configuration, alone) || index != 15000 {
		t.Fatalf("read %+v at %d, want a alone at 15000", configuration, index)
	}
	if err := r.AddVoter("c", "c", index, time.Second).Error(); err != nil {
		t.Fatalf("Raft refused a change to the voters as read: %v", err)
	}
}

// fileSnapshots is a snapshot store in dir holding, oldest first,
// snapshots at the indexes given, each made of contents.
func fileSnapshots(t *testing.T, dir string, transport raft.Transport, snapshots ...fileSnapshotOf) *raft.FileSnapshotStore {
	t.Helper()
	store, err := raft.NewFileSnapshotStore(dir, len(snapshots), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	for _, snapshot := range snapshots {
		sink, err := store.Create(raft.SnapshotVersionMax, snapshot.index, 1, snapshot.configuration, snapshot.configurationIndex, transport)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sink.Write([]byte(snapshot.contents)); err != nil {
			t.Fatal(err)
		}
		if err := sink.Close(); err != nil {
			t.Fatal(err)
		}
		if snapshot.spoil != nil {
			snapshot.spoil(t, filepath.Join(dir, "snapshots", sink.ID(), "state.bin"))
		}
	}
	return store
}

type fileSnapshotOf struct {
	index, configurationIndex uint64
	configuration             raft.Configuration
	contents                  string
	// spoil, if set, breaks the snapshot's state file.
	spoil func(t *testing.T, state string)
}

// A snapshot that does not open is passed over, as Raft passes over it:
// the voters are read from the newest that does, and the log after it.
func TestASnapshotThatDoesNotOpenIsPassedOver(t *testing.T) {
	alone := raft.Configuration{Servers: []raft.Server{{ID: "a", Address: "a", Suffrage: raft.Voter}}}
	withB := raft.Configuration{Servers: []raft.Server{{ID: "a", Address: "a", Suffrage: raft.Voter}, {ID: "b", Address: "b", Suffrage: raft.Voter}}}
	for _, test := range []struct {
		name  string
		spoil func(t *testing.T, state string)
	}{{
		name: "its state does not match its checksum",
		spoil: func(t *testing.T, state string) {
			if err := os.WriteFile(state, []byte("bad"), 0o600); err != nil {
				t.Fatal(err)
			}
		},
	}, {
		name: "its state is gone",
		spoil: func(t *testing.T, state string) {
			if err := os.Remove(state); err != nil {
				t.Fatal(err)
			}
		},
	}} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			logs, err := raftboltdb.NewBoltStore(filepath.Join(dir, "raft.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer logs.Close()
			_, transport := raft.NewInmemTransport("a")
			snapshots := fileSnapshots(t, dir, transport,
				fileSnapshotOf{index: 10, configurationIndex: 5, configuration: withB, contents: "snapshot"},
				fileSnapshotOf{index: 20, configurationIndex: 15, configuration: withB, contents: "snapshot", spoil: test.spoil})
			// After the snapshot Raft restores, at 10, the log makes a
			// alone at 12; the one it passes over would hide that.
			if err := logs.StoreLogs(logOf(12, map[uint64]raft.Configuration{1: withB, 12: alone})); err != nil {
				t.Fatal(err)
			}
			r, err := raft.NewRaft(fastRaft("a"), discardFSM{}, logs, logs, snapshots, transport)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = r.Shutdown().Error() }()
			leading(t, r)
			c := &Cluster{logs: logs, snapshots: snapshots}
			configuration, index, err := c.configuration()
			if err != nil {
				t.Fatal(err)
			}
			if want := r.GetConfiguration().Configuration(); !reflect.DeepEqual(configuration, want) || index != 12 {
				t.Fatalf("read %+v at %d, Raft has %+v at 12", configuration, index, want)
			}
			if err := r.AddVoter("c", "c", index, time.Second).Error(); err != nil {
				t.Fatalf("Raft refused a change to the voters as read: %v", err)
			}
		})
	}
}

// pickyFSM restores no snapshot made of "broken".
type pickyFSM struct{ discardFSM }

func (pickyFSM) Restore(r io.ReadCloser) error {
	defer r.Close()
	data, err := io.ReadAll(r)
	if err == nil && string(data) == "broken" {
		err = errors.New("a broken snapshot")
	}
	return err
}

// A snapshot that opens but does not restore, which Raft passes over
// unseen, makes Raft go by other voters than the ones read: a change is
// refused at once, with what to do, rather than asked again for good.
func TestRaftGoingByOtherVotersIsReported(t *testing.T) {
	alone := raft.Configuration{Servers: []raft.Server{{ID: "a", Address: "a", Suffrage: raft.Voter}}}
	dir := t.TempDir()
	logs, err := raftboltdb.NewBoltStore(filepath.Join(dir, "raft.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	_, transport := raft.NewInmemTransport("a")
	snapshots := fileSnapshots(t, dir, transport,
		fileSnapshotOf{index: 10, configurationIndex: 5, configuration: alone, contents: "snapshot"},
		fileSnapshotOf{index: 20, configurationIndex: 15, configuration: alone, contents: "broken"})
	r, err := raft.NewRaft(fastRaft("a"), pickyFSM{}, logs, logs, snapshots, transport)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Shutdown().Error() }()
	leading(t, r)
	c := &Cluster{config: Config{Node: "a"}, raft: r, logs: logs, snapshots: snapshots}
	_, index, err := c.configuration()
	if err != nil {
		t.Fatal(err)
	}
	if index != 15 {
		t.Fatalf("read the voters at %d, want the unrestored snapshot's 15", index)
	}
	start := time.Now()
	err = c.changeVoters(func(index uint64) raft.IndexFuture { return r.AddVoter("c", "c", index, time.Second) }, index)
	if err == nil || errors.Is(err, errVotersChanged) {
		t.Fatalf("the change: %v", err)
	}
	for _, want := range []string{"log entry 5", "as of 15", "restart node a"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("%q does not say %q", err, want)
		}
	}
	if time.Since(start) > 10*time.Second {
		t.Fatalf("took %s to give up", time.Since(start))
	}
	if _, voter := serverAddress(r.GetConfiguration().Configuration(), "c"); voter {
		t.Fatal("c was added")
	}
}
