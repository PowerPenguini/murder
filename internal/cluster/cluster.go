// Package cluster replicates the store between nodes with Raft. Every node
// keeps the whole state in its own database; every change is a batch the
// leader appends to the log, and every node applies the log's batches in
// order. A write on any node is computed there and handed to the leader.
//
// Nodes talk over one TLS port on the private network. They prove that
// they belong to the cluster with certificates of an authority derived from
// the master key, so a node needs nothing but the master key to join.
package cluster

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/upgrade"
)

// ErrNoLeader reports a cluster that has no leader to take a change: it is
// electing one, has lost its majority, or was never started.
var ErrNoLeader = errors.New("the cluster has no leader")

// ErrNotLeader reports a request made to a node that is not the leader.
var ErrNotLeader = errors.New("not the leader")

// ErrMaybe reports a change whose leader lost its leadership while it was
// in flight: the change may yet be made.
var ErrMaybe = errors.New("the cluster log did not take the change, though it may yet be made")

// ErrMember reports a node that is, or was, part of a cluster, where only a
// new one will do.
var ErrMember = errors.New("already part of a cluster")

// PeerError is another node's answer to a request it refused.
type PeerError struct {
	Status  int
	Message string
}

func (e *PeerError) Error() string { return e.Message }

const (
	// commitTimeout bounds one change, from proposal until this node has
	// applied it.
	commitTimeout = 15 * time.Second
	// maxBatch bounds a change sent to the leader.
	maxBatch = 64 << 20
	// snapshotsKept is how many of its own snapshots a node keeps.
	snapshotsKept = 2
)

// Config sets up one node of a cluster.
type Config struct {
	Node string
	// Address is where the node listens and where other nodes reach it:
	// an IP address and port on the private network.
	Address string
	// Dir holds the Raft log and snapshots.
	Dir string
	// Seed is derived from the master key; every node of a cluster has the
	// same.
	Seed  []byte
	Store *store.SQLite
	Log   *slog.Logger
	// Fatal is called when the node cannot apply an entry every other node
	// applied; the agent must stop, and its restart applies the entry again.
	Fatal func(error)
	// Upgrade reports on and installs this node's binary for an upgrade
	// run from another node.
	Upgrade Upgrader
	// Moves is this node's part in moving tenants between nodes.
	Moves Moves
	// Replicas keeps the copies of other nodes' tenants this node holds.
	Replicas Replicas

	// raft overrides Raft's timing in tests.
	raft func(*raft.Config)
}

// Cluster is this node's part of the cluster.
type Cluster struct {
	config    Config
	address   string
	raft      *raft.Raft
	logs      *raftboltdb.BoltStore
	snapshots raft.SnapshotStore
	listener  *listener
	server    *http.Server
	client    *http.Client
	// barrierTerm is the last term in which this node, as the leader, has
	// applied everything committed before it.
	barrierTerm atomic.Uint64
}

// Start opens the node's log, listens for other nodes and has the store
// commit every change through the log. A new node waits, without a
// leader, until it is bootstrapped or joins a cluster.
func Start(config Config) (*Cluster, error) {
	credentials, err := NewCredentials(config.Seed, config.Node, time.Now())
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(config.Dir, 0o700); err != nil {
		return nil, err
	}
	logger := raftLogger(config.Log)
	logs, err := raftboltdb.NewBoltStore(filepath.Join(config.Dir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("open the raft log: %w", err)
	}
	snapshots, err := raft.NewFileSnapshotStoreWithLogger(config.Dir, snapshotsKept, logger)
	if err != nil {
		_ = logs.Close()
		return nil, err
	}
	l, err := listen(config.Address, credentials, config.Store.Cluster, config.Log)
	if err != nil {
		_ = logs.Close()
		return nil, fmt.Errorf("cluster address: %w", err)
	}
	transport := raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{Stream: l.raftLayer(), MaxPool: 3, Timeout: 10 * time.Second, Logger: logger})
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.Node)
	raftConfig.Logger = logger
	if config.raft != nil {
		config.raft(raftConfig)
	}
	r, err := raft.NewRaft(raftConfig, &fsm{store: config.Store, fatal: config.Fatal}, logs, logs, snapshots, transport)
	if err != nil {
		_ = transport.Close()
		_ = logs.Close()
		return nil, err
	}
	c := &Cluster{config: config, address: l.inner.Addr().String(), raft: r, logs: logs, snapshots: snapshots, listener: l}
	c.client = &http.Client{
		Transport: namedTransport{cluster: config.Store.Cluster, next: &http.Transport{
			DialTLSContext: func(ctx context.Context, _, address string) (net.Conn, error) {
				return dial(ctx, credentials, protocolHTTP, address)
			},
			MaxIdleConnsPerHost: 2,
			IdleConnTimeout:     time.Minute,
		}},
	}
	c.server = &http.Server{Handler: c.handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := c.server.Serve(l.httpListener()); err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			config.Log.Error("the cluster port stopped serving", "error", err)
		}
	}()
	config.Store.Replicate(c.commit, c.catchUp)
	return c, nil
}

// Close stops the node's part of the cluster; the store's own state stays.
func (c *Cluster) Close() error {
	err := c.server.Close()
	err = errors.Join(err, c.raft.Shutdown().Error(), c.listener.close())
	return errors.Join(err, c.logs.Close())
}

// commit has the leader append batch to the log, and returns its index
// once this node has applied it: what a write here returns, a read here
// then shows.
func (c *Cluster) commit(batch store.Batch) (uint64, error) {
	data, err := store.EncodeBatch(batch)
	if err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), commitTimeout)
	defer cancel()
	index, err := c.propose(ctx, data)
	for attempt := 0; errors.Is(err, ErrMaybe) && attempt < 3; attempt++ {
		// Leadership moved while the batch was in flight: it is in the
		// log, or will never be. Once this node has applied what the new
		// leader has, it knows which, and sends it again if it is not.
		if catchUpErr := c.catchUp(); catchUpErr != nil {
			return 0, errors.Join(err, catchUpErr)
		}
		if outcome, ok := c.config.Store.Outcome(batch.ID); ok {
			index, err = outcome.Index, outcome.Err
			break
		}
		index, err = c.propose(ctx, data)
	}
	// A batch sent again is refused once its first copy reached the log;
	// that first copy is what happened.
	if err != nil && batch.ID != "" {
		if outcome, ok := c.config.Store.Outcome(batch.ID); ok && outcome.Err == nil {
			index, err = outcome.Index, nil
		}
	}
	if index == 0 {
		return 0, err
	}
	if waitErr := c.config.Store.WaitApplied(ctx, index); waitErr != nil {
		return 0, errors.Join(err, fmt.Errorf("the change was made (log entry %d) but this node has not applied it yet: %w", index, waitErr))
	}
	return index, err
}

// catchUp returns once this node has applied every change the cluster
// has acknowledged. Every acknowledged change has been applied by the
// leader: the leader names how far it has applied, once it has confirmed
// that it still leads, and the node waits until it has applied as far.
func (c *Cluster) catchUp() error {
	ctx, cancel := context.WithTimeout(context.Background(), commitTimeout)
	defer cancel()
	var target uint64
	for {
		var err error
		if c.raft.State() == raft.Leader {
			target, err = c.leaderApplied()
		} else {
			var answer indexAnswer
			err = c.toLeader(ctx, http.MethodGet, "/cluster/v1/index", nil, &answer)
			target = answer.Index
		}
		if err == nil {
			break
		}
		if !errors.Is(err, ErrNotLeader) && !errors.Is(err, ErrNoLeader) {
			return err
		}
		if err := c.unconfigured(); err == nil {
			return ErrNotJoined
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(100 * time.Millisecond):
		}
	}
	return c.config.Store.WaitApplied(ctx, target)
}

// CatchUp returns once this node has applied every change the cluster has
// acknowledged, such as one another node just made.
func (c *Cluster) CatchUp() error { return c.catchUp() }

// leaderApplied is the index of the last change the leader applied, which
// is at least that of every change acknowledged. A new leader first applies
// what the leaders before it committed: a barrier, once per term, waits for
// that.
func (c *Cluster) leaderApplied() (uint64, error) {
	term := c.raft.CurrentTerm()
	var err error
	if c.barrierTerm.Load() != term {
		if err = c.raft.Barrier(commitTimeout).Error(); err == nil {
			c.barrierTerm.Store(term)
		}
	} else {
		err = c.raft.VerifyLeader().Error()
	}
	switch {
	case errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost):
		return 0, ErrNotLeader
	case err != nil:
		return 0, err
	}
	return c.config.Store.Applied(), nil
}

// indexAnswer is how far the leader has applied the log.
type indexAnswer struct {
	Index uint64 `json:"index"`
}

// ErrNotJoined reports a node of no cluster yet.
var ErrNotJoined = errors.New("this node is not part of a cluster yet: bootstrap one, or join one (murder cluster)")

// propose appends data to the log through the leader and returns its index
// with the outcome of applying it. Without an index the change was not
// made, or, after a lost leadership, may have been.
func (c *Cluster) propose(ctx context.Context, data []byte) (uint64, error) {
	for {
		var index uint64
		var err error
		if c.raft.State() == raft.Leader {
			index, err = c.apply(data)
		} else {
			var result commitResult
			err = c.toLeader(ctx, http.MethodPost, "/cluster/v1/commit", data, &result)
			var peer *PeerError
			switch {
			case err == nil:
				index, err = result.Index, result.err()
			case errors.As(err, &peer) && peer.Status == http.StatusServiceUnavailable:
				err = fmt.Errorf("%w: %s", ErrMaybe, peer.Message)
			case !errors.Is(err, ErrNotLeader) && !errors.Is(err, ErrNoLeader) && !errors.As(err, &peer):
				// The request may have reached the leader before the
				// answer was lost.
				err = fmt.Errorf("%w: the leader did not answer: %w", ErrMaybe, err)
			}
		}
		if !errors.Is(err, ErrNotLeader) && !errors.Is(err, ErrNoLeader) {
			return index, err
		}
		if err := c.unconfigured(); err == nil {
			return 0, ErrNotJoined
		}
		// Leadership is moving: try again once it has moved.
		select {
		case <-ctx.Done():
			return 0, err
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// apply appends data to the log on the leader.
func (c *Cluster) apply(data []byte) (uint64, error) {
	// Nothing enters the log that a node of this release cannot apply.
	batch, err := store.DecodeBatch(data)
	if err == nil {
		err = batch.Check()
	}
	if err != nil {
		return 0, &PeerError{Status: http.StatusBadRequest, Message: err.Error()}
	}
	future := c.raft.Apply(data, commitTimeout)
	if err := future.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return 0, ErrNotLeader
		}
		return 0, fmt.Errorf("%w: %w", ErrMaybe, err)
	}
	if err, _ := future.Response().(error); err != nil {
		if store.Refused(err) {
			return future.Index(), err
		}
		// A failure of the leader's own, such as a full disk, is not the
		// change's: it is committed, and every other node applies it. The
		// leader stops; a writer here learns that it has not applied it.
		c.config.Log.Error("committed a change this node could not apply", "index", future.Index(), "error", err)
	}
	return future.Index(), nil
}

// commitResult is the leader's answer to a change from another node.
type commitResult struct {
	Index uint64 `json:"index"`
	Error string `json:"error,omitempty"`
	Stale bool   `json:"stale,omitempty"`
}

func (r commitResult) err() error {
	switch {
	case r.Error == "":
		return nil
	case r.Stale:
		return fmt.Errorf("%w: %s", store.ErrStale, strings.TrimPrefix(r.Error, store.ErrStale.Error()+": "))
	default:
		return errors.New(r.Error)
	}
}

// Bootstrap starts a new cluster of this node alone, with the state its
// database holds, and names it. A bootstrap stopped half way finishes.
func (c *Cluster) Bootstrap(ctx context.Context, actor string) error {
	if err := c.unconfigured(); err == nil {
		if c.config.Store.Cluster() != "" {
			return fmt.Errorf("%w: this node's database belongs to cluster %s; give it a new database, or rebuild the cluster from a backup", ErrMember, c.config.Store.Cluster())
		}
		if err := c.config.Store.StartLog(); err != nil {
			return err
		}
		configuration := raft.Configuration{Servers: []raft.Server{{Suffrage: raft.Voter, ID: raft.ServerID(c.config.Node), Address: raft.ServerAddress(c.address)}}}
		if err := c.raft.BootstrapCluster(configuration).Error(); err != nil {
			return fmt.Errorf("bootstrap: %w", err)
		}
	} else if !c.alone() || c.config.Store.Cluster() != "" && c.joined() {
		return err
	}
	for c.raft.State() != raft.Leader {
		select {
		case <-ctx.Done():
			return fmt.Errorf("bootstrap: no leadership: %w", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	if c.config.Store.Cluster() == "" {
		if err := c.config.Store.NameCluster(newClusterName(), actor); err != nil {
			return err
		}
	}
	return c.config.Store.PutNode(store.Node{Name: c.config.Node, Address: c.address}, actor)
}

// alone reports whether this node is the only voter of its cluster.
func (c *Cluster) alone() bool {
	future := c.raft.GetConfiguration()
	if future.Error() != nil {
		return false
	}
	servers := future.Configuration().Servers
	return len(servers) == 1 && servers[0].ID == raft.ServerID(c.config.Node)
}

// joined reports whether the state knows this node.
func (c *Cluster) joined() bool {
	_, known := nodeNamed(c.config.Store.Nodes(), c.config.Node)
	return known
}

// newClusterName names a new cluster, so a node of another one, or of this
// one before it was rebuilt, cannot join it with its old state.
func newClusterName() string {
	return rand.Text()
}

// unconfigured refuses a node that is, or was, part of a cluster.
func (c *Cluster) unconfigured() error {
	existing, err := raft.HasExistingState(c.logs, c.logs, c.snapshots)
	if err != nil {
		return err
	}
	if existing {
		return fmt.Errorf("%w: node %s has a cluster log in %s", ErrMember, c.config.Node, c.config.Dir)
	}
	return nil
}

// Join adds this node to the cluster of the node at peer. Only a node
// with a new database joins, since it takes the cluster's state; a node
// whose join stopped half way, holding this cluster's state, tries again.
func (c *Cluster) Join(ctx context.Context, peer, actor string) error {
	if err := c.unconfigured(); err != nil {
		return err
	}
	// The leader checks the node first, so a node it refuses keeps its
	// state as it was.
	request := joinRequest{Address: c.address, Actor: actor, Check: true}
	var answer joinAnswer
	if err := c.join(ctx, peer, request, &answer); err != nil {
		return err
	}
	if ours := c.config.Store.Cluster(); !c.config.Store.Fresh() && ours != answer.Cluster {
		if ours == "" {
			return fmt.Errorf("%w: this node already holds apps, tenants or nodes; only a node with a new database (murder init) can join a cluster", ErrMember)
		}
		return fmt.Errorf("%w: this node's database belongs to cluster %s, not %s; give it a new database", ErrMember, ours, answer.Cluster)
	}
	if err := c.fetchState(ctx, peer); err != nil {
		return err
	}
	if c.config.Store.Cluster() != answer.Cluster {
		return fmt.Errorf("the state from %s belongs to cluster %q, not %q", peer, c.config.Store.Cluster(), answer.Cluster)
	}
	request.Check = false
	if err := c.join(ctx, peer, request, nil); err != nil {
		return err
	}
	// The leader has added the node; it is part of the cluster once the
	// leader has reached it.
	for {
		if leader, _ := c.raft.LeaderWithID(); leader != "" && c.joined() {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("the leader added this node, which has not heard from it since: %w", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// fetchState installs the state of the node at peer.
func (c *Cluster) fetchState(ctx context.Context, peer string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+peer+"/cluster/v1/snapshot", nil)
	if err != nil {
		return err
	}
	response, err := c.client.Do(request)
	if err != nil {
		return fmt.Errorf("fetch the cluster's state from %s: %w", peer, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch the cluster's state from %s: %w", peer, responseError(response))
	}
	if _, err := c.config.Store.InstallSnapshot(response.Body); err != nil {
		return fmt.Errorf("install the cluster's state: %w", err)
	}
	return nil
}

// join sends request to peer, or to the leader peer names, while
// leadership moves.
func (c *Cluster) join(ctx context.Context, peer string, request joinRequest, out any) error {
	for {
		err := c.call(ctx, peer, http.MethodPost, "/cluster/v1/nodes", marshal(request), out)
		var moved *movedError
		if !errors.As(err, &moved) {
			return err
		}
		if moved.leader != "" {
			peer = moved.leader
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// Remove takes a node out of the cluster, once there is a leader to ask.
func (c *Cluster) Remove(ctx context.Context, node, actor string) error {
	for {
		err := c.toLeader(ctx, http.MethodDelete, "/cluster/v1/nodes/"+node+"?actor="+url.QueryEscape(actor), nil, nil)
		if !errors.Is(err, ErrNoLeader) && !errors.Is(err, ErrNotLeader) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// Moves is the node's part in moving tenants: it receives the data of a
// tenant moving here, reports on tenants it runs, and moves its own away.
type Moves interface {
	Receive(ctx context.Context, tenant string, incremental bool, data io.Reader) error
	Discard(ctx context.Context, tenant string) error
	Status(tenant string) (any, bool)
	Move(ctx context.Context, tenant, to, actor string) error
}

// ReceiveOn streams a moving tenant's data to node.
func (c *Cluster) ReceiveOn(ctx context.Context, node, tenant string, incremental bool, data io.Reader) error {
	path := "/cluster/v1/tenants/" + url.PathEscape(tenant) + "/data"
	if incremental {
		path += "?incremental=1"
	}
	return c.stream(ctx, node, http.MethodPut, path, data, nil)
}

// DiscardOn drops what node received for a move that failed.
func (c *Cluster) DiscardOn(ctx context.Context, node, tenant string) error {
	return c.stream(ctx, node, http.MethodDelete, "/cluster/v1/tenants/"+url.PathEscape(tenant)+"/data", nil, nil)
}

// StatusOn decodes a tenant's status on node into out.
func (c *Cluster) StatusOn(ctx context.Context, node, tenant string, out any) error {
	return c.stream(ctx, node, http.MethodGet, "/cluster/v1/tenants/"+url.PathEscape(tenant)+"/status", nil, out)
}

// MoveOn has node move one of its tenants to another node; it returns once
// the move is done.
func (c *Cluster) MoveOn(ctx context.Context, node, tenant, to, actor string) error {
	body := marshal(moveRequest{To: to, Actor: actor})
	return c.stream(ctx, node, http.MethodPost, "/cluster/v1/tenants/"+url.PathEscape(tenant)+"/move", bytes.NewReader(body), nil)
}

// Replicas is the node's part in keeping copies of tenants other nodes
// run. sender is the node a request comes from, as its cluster certificate
// proves.
type Replicas interface {
	// Snapshots lists the snapshots of the copy of tenant this node keeps
	// for sender, oldest first; none if it keeps none yet.
	Snapshots(ctx context.Context, sender, tenant string) ([]string, error)
	// Receive takes a stream of tenant's data from sender into the copy:
	// whole, or the changes since its newest snapshot.
	Receive(ctx context.Context, sender, tenant string, whole bool, data io.Reader) error
}

// SnapshotsOn lists the snapshots of the copy of tenant node keeps.
func (c *Cluster) SnapshotsOn(ctx context.Context, node, tenant string) ([]string, error) {
	var answer struct {
		Snapshots []string `json:"snapshots"`
	}
	err := c.stream(ctx, node, http.MethodGet, "/cluster/v1/tenants/"+url.PathEscape(tenant)+"/replica", nil, &answer)
	return answer.Snapshots, err
}

// ReplicateOn streams tenant's data to the copy node keeps.
func (c *Cluster) ReplicateOn(ctx context.Context, node, tenant string, whole bool, data io.Reader) error {
	path := "/cluster/v1/tenants/" + url.PathEscape(tenant) + "/replica"
	if whole {
		path += "?whole=1"
	}
	return c.stream(ctx, node, http.MethodPut, path, data, nil)
}

// moveRequest asks a node to move one of its tenants.
type moveRequest struct {
	To    string `json:"to"`
	Actor string `json:"actor"`
}

// stream sends a request with any body to node and decodes its answer.
func (c *Cluster) stream(ctx context.Context, node, method, path string, body io.Reader, out any) error {
	address, err := c.addressOf(node)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, method, "https://"+address+path, body)
	if err != nil {
		return err
	}
	response, err := c.client.Do(request)
	if err != nil {
		return fmt.Errorf("node %s: %w", node, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return responseError(response)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(response.Body).Decode(out)
}

// Upgrader is the node's part in an upgrade.
type Upgrader interface {
	Info() upgrade.Info
	Install(ctx context.Context, binary io.Reader, sha string) error
}

// NodeInfo asks a node of the cluster how it is, for an upgrade.
func (c *Cluster) NodeInfo(ctx context.Context, node string) (upgrade.Info, error) {
	address, err := c.addressOf(node)
	if err != nil {
		return upgrade.Info{}, err
	}
	var info upgrade.Info
	err = c.call(ctx, address, http.MethodGet, "/cluster/v1/node", nil, &info)
	return info, err
}

// Install sends a node of the cluster a binary to install.
func (c *Cluster) Install(ctx context.Context, node string, binary io.Reader, sha string) error {
	address, err := c.addressOf(node)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, "https://"+address+"/cluster/v1/node/binary", binary)
	if err != nil {
		return err
	}
	request.Header.Set(shaHeader, sha)
	response, err := c.client.Do(request)
	if err != nil {
		return fmt.Errorf("node %s: %w", node, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return responseError(response)
	}
	return nil
}

// shaHeader carries the SHA-256 a binary must have.
const shaHeader = "X-Murder-Sha256"

// addressOf is where the cluster reaches node.
func (c *Cluster) addressOf(node string) (string, error) {
	if address, member := c.memberAddress(node); member {
		return address, nil
	}
	return "", fmt.Errorf("node %s: %w", node, store.ErrNotFound)
}

// Status is this node's view of the cluster.
type Status struct {
	Node    string `json:"node"`
	Address string `json:"address"`
	// State is the node's Raft state: Leader, Follower, Candidate or
	// Shutdown.
	State  string `json:"state"`
	Leader string `json:"leader,omitempty"`
	// Applied is the index of the last log entry applied here.
	Applied uint64 `json:"applied"`
	// LastContact is when a follower last heard from the leader.
	LastContact time.Time `json:"lastContact,omitzero"`
	Members     []Member  `json:"members"`
}

// Member is one voter of the cluster.
type Member struct {
	Name     string `json:"name"`
	Address  string `json:"address"`
	Suffrage string `json:"suffrage"`
}

func (c *Cluster) Status() (Status, error) {
	status := Status{Node: c.config.Node, Address: c.address, State: c.raft.State().String(), Applied: c.config.Store.Applied()}
	if _, leader := c.raft.LeaderWithID(); leader != "" {
		status.Leader = string(leader)
	}
	if status.State != raft.Leader.String() {
		status.LastContact = c.raft.LastContact()
	}
	future := c.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return Status{}, err
	}
	for _, server := range future.Configuration().Servers {
		status.Members = append(status.Members, Member{Name: string(server.ID), Address: string(server.Address), Suffrage: server.Suffrage.String()})
	}
	slices.SortFunc(status.Members, func(a, b Member) int { return strings.Compare(a.Name, b.Name) })
	return status, nil
}

// Leader reports whether this node leads the cluster.
func (c *Cluster) Leader() bool { return c.raft.State() == raft.Leader }

func marshal(value any) []byte {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err) // only plain structs are marshalled
	}
	return data
}

// toLeader sends a request to the leader.
func (c *Cluster) toLeader(ctx context.Context, method, path string, body []byte, out any) error {
	leader, _ := c.raft.LeaderWithID()
	if leader == "" {
		return ErrNoLeader
	}
	return c.call(ctx, string(leader), method, path, body, out)
}

// call sends a request to the node at address and decodes its answer.
func (c *Cluster) call(ctx context.Context, address, method, path string, body []byte, out any) error {
	request, err := http.NewRequestWithContext(ctx, method, "https://"+address+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	response, err := c.client.Do(request)
	if err != nil {
		return fmt.Errorf("node %s: %w", address, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return responseError(response)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(response.Body).Decode(out)
}

// failure is an error answer between nodes.
type failure struct {
	Error  string `json:"error"`
	Leader string `json:"leader,omitempty"`
}

// movedError answers a request only the leader takes.
type movedError struct{ leader string }

func (e *movedError) Error() string {
	if e.leader == "" {
		return ErrNoLeader.Error()
	}
	return "not the leader; the leader is at " + e.leader
}

func (e *movedError) Is(target error) bool { return target == ErrNotLeader }

func responseError(response *http.Response) error {
	var answer failure
	data, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if json.Unmarshal(data, &answer) != nil || answer.Error == "" {
		answer.Error = fmt.Sprintf("%s: %s", response.Status, bytes.TrimSpace(data))
	}
	if response.StatusCode == http.StatusMisdirectedRequest {
		return &movedError{leader: answer.Leader}
	}
	return &PeerError{Status: response.StatusCode, Message: answer.Error}
}
