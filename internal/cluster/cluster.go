// Package cluster replicates the store between nodes with Raft. Every node
// keeps the whole state in its own database; every change is a batch the
// leader appends to the log, and every node applies the log's batches in
// order. A write on any node is computed there and handed to the leader.
//
// Nodes talk over one TLS port on the private network. Their certificates
// come from an authority derived from the master key, and each is issued
// over the node's own identity key, which the state pins to its name: a
// peer is a member only if the state names it and holds the key its
// certificate carries. A node joins with an invitation, which admits its
// name and key.
package cluster

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/throttle"
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
	// Seeds are derived from the master key ring, the key in use first;
	// every node of a cluster has the same. Rekey changes them.
	Seeds [][]byte
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
	// Usage is what this node measures of its tenants; nil if nothing.
	Usage Usage
	// Invites lets hosts join with an invitation; nil takes none.
	Invites Invites
	// Keys is this node's master key ring, for rotations; nil has none.
	Keys Keys

	// raft overrides Raft's timing in tests.
	raft func(*raft.Config)
	// stable, if set, wraps where Raft keeps its term and vote, in tests.
	stable func(raft.StableStore) raft.StableStore
	// unregistered leaves the node's identity key unregistered, for tests
	// of a cluster from before keys.
	unregistered bool
}

// Cluster is this node's part of the cluster.
type Cluster struct {
	config      Config
	address     string
	raft        *raft.Raft
	logs        *raftboltdb.BoltStore
	snapshots   raft.SnapshotStore
	transport   *gatedTransport
	listener    *listener
	server      *http.Server
	credentials *Credentials
	// joins answers hosts that bring an invitation.
	joins  *http.Server
	client *http.Client
	// membership keeps joins and removals one at a time on the leader: a
	// channel of one, held while one runs.
	membership chan struct{}
	// joinHook, if set, runs at each stage of a join (joinWrite, joinVote,
	// joinReading, joinAdd): tests pause a join there.
	joinHook atomic.Pointer[func(stage, name string)]
	// departedHook, if set, runs as the upkeep is about to drop a node's
	// departure: tests pause it there.
	departedHook atomic.Pointer[func(name string)]
	// catchUpVerify and joinVerify each let one VerifyLeader call at a
	// time hand its request to Raft; see verifyLeader.
	catchUpVerify, joinVerify chan struct{}
	// barrierTerm is the last term in which this node, as the leader, has
	// applied everything committed before it.
	barrierTerm atomic.Uint64
	// identity is the node's identity key.
	identity ed25519.PrivateKey
	// joinKeys are the members' identity keys an invitation named, by
	// name, which this node trusts until it holds the cluster's state.
	joinKeys atomic.Pointer[map[string]string]
	// done stops the node's upkeep of its members as it closes, and ends
	// what waits on the cluster.
	done chan struct{}
	// refusals says which requests of other nodes were refused, without
	// a line for each time one tries again.
	refusals throttle.Log
}

// Start opens the node's log, listens for other nodes and has the store
// commit every change through the log. A new node waits, without a
// leader, until it is bootstrapped or joins a cluster.
func Start(config Config) (*Cluster, error) {
	if err := os.MkdirAll(config.Dir, 0o700); err != nil {
		return nil, err
	}
	identity, err := LoadIdentity(config.Dir)
	if err != nil {
		return nil, err
	}
	credentials, err := NewCredentials(config.Seeds, config.Node, identity, time.Now())
	if err != nil {
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
	transport := &gatedTransport{NetworkTransport: raft.NewNetworkTransportWithConfig(transportConfig(l.raftLayer(), logger))}
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.Node)
	raftConfig.Logger = logger
	if config.raft != nil {
		config.raft(raftConfig)
	}
	var stable raft.StableStore = logs
	if config.stable != nil {
		stable = config.stable(logs)
	}
	r, err := raft.NewRaft(raftConfig, &fsm{store: config.Store, fatal: config.Fatal}, logs, stable, snapshots, transport)
	if err != nil {
		_ = transport.Close()
		_ = logs.Close()
		return nil, err
	}
	c := &Cluster{config: config, address: l.inner.Addr().String(), raft: r, logs: logs, snapshots: snapshots, transport: transport, listener: l, credentials: credentials,
		catchUpVerify: make(chan struct{}, 1), joinVerify: make(chan struct{}, 1), membership: make(chan struct{}, 1), identity: identity, done: make(chan struct{})}
	raftPeer := c.raftPeer
	l.raftPeer.Store(&raftPeer)
	credentials.checkServers(c.knownServer)
	c.client = &http.Client{
		Transport: namedTransport{cluster: config.Store.Cluster, check: c.knownServer, next: &http.Transport{
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
	c.joins = &http.Server{Handler: joinHandler(config.Invites), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}
	c.joins.SetKeepAlivesEnabled(false)
	go func() {
		if err := c.joins.Serve(l.joinListener()); err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			config.Log.Error("the cluster port stopped taking invitations", "error", err)
		}
	}()
	config.Store.Replicate(c.commit, c.catchUp)
	go c.upkeep()
	return c, nil
}

// transportConfig sets up Raft's transport over stream.
//
// Replication sends each follower one request at a time (MaxRPCsInFlight
// 1). Raft's pipelined replication, its default, can wedge for good: once
// the follower answers with a newer term (the leader was replaced) or a
// refusal, the pipeline's decoder stops, and a send already under way
// waits, with nothing left to take it, for the pipeline to close, which
// only that send's caller does. The leader then never replicates to that
// follower again, and its Shutdown waits for that goroutine forever.
// Raft's own documentation finds that pipelining gains little.
func transportConfig(stream raft.StreamLayer, logger hclog.Logger) *raft.NetworkTransportConfig {
	return &raft.NetworkTransportConfig{Stream: stream, MaxPool: 3, MaxRPCsInFlight: 1, Timeout: 10 * time.Second, Logger: logger}
}

// IdentityKey is the public key of this node's identity.
func (c *Cluster) IdentityKey() ed25519.PublicKey {
	return c.identity.Public().(ed25519.PublicKey)
}

// upkeepEvery is how often a node closes the Raft connections of nodes no
// longer members and, until it has, registers its identity key.
const upkeepEvery = 5 * time.Second

// upkeep closes Raft connections of nodes that left, and registers this
// node's identity key if the state holds none for it yet, until the node
// closes.
func (c *Cluster) upkeep() {
	registered := false
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-timer.C:
		}
		c.listener.sweep(c.raftPeer)
		c.forgetDeparted()
		if !registered && !c.config.unregistered {
			ctx, cancel := context.WithTimeout(context.Background(), commitTimeout)
			var err error
			registered, err = c.registerKey(ctx)
			cancel()
			if err != nil {
				c.config.Log.Warn("could not register this node's identity key", "error", err)
			}
		}
		timer.Reset(upkeepEvery)
	}
}

// forgetDeparted, on the leader, drops the identity keys kept for nodes
// taken out of the state that are out of the voters too, as a removal that
// stopped half way and was finished another way leaves them. It holds the
// lock on changes of the members, so no join or removal of the name runs
// between its look at the voters and its change, and it drops only the
// very departure it looked at.
func (c *Cluster) forgetDeparted() {
	if c.raft.State() != raft.Leader || len(c.config.Store.Departed()) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), commitTimeout)
	defer cancel()
	if c.holdMembership(ctx) != nil {
		return // a change of the members runs: the next upkeep tries again
	}
	defer c.unlockMembership()
	for _, departure := range c.config.Store.Departed() {
		if c.isVoter(departure.Name) {
			continue
		}
		if hook := c.departedHook.Load(); hook != nil {
			(*hook)(departure.Name)
		}
		if err := c.config.Store.ForgetDeparted(departure, "murder"); err != nil {
			c.config.Log.Debug("could not drop the identity key of a node removed", "node", departure.Name, "error", err)
			return
		}
	}
}

// registerKey has the state hold this node's identity key, if it holds
// none for it: a node of a cluster from before identity keys registers
// its own once it runs a release that has them. It reports whether there
// is nothing left to do. It waits until every node runs such a release, as
// one before would not apply the change, and while changes are frozen, by
// an upgrade above all: the write is refused under any freeze, and leaves
// the freeze, its owner and operation, to whoever holds it. An upgrade
// thaws once every node runs the new release, and the key goes in then.
func (c *Cluster) registerKey(ctx context.Context) (bool, error) {
	own := EncodeKey(c.IdentityKey())
	pinned, member := c.config.Store.NodeKey(c.config.Node)
	switch {
	case !member:
		return false, nil // not joined yet, or removed
	case pinned == own:
		return true, nil
	case pinned != "":
		return true, fmt.Errorf("the cluster holds another identity key for node %s than the one in %s, so the other nodes refuse it: take it out of the cluster (murder cluster remove %s, on another node) and join it again with an invitation", c.config.Node, IdentityPath(c.config.Dir), c.config.Node)
	}
	// The leader registers it, for the node that presents it.
	var err error
	if c.raft.State() == raft.Leader {
		err = c.registerHere(ctx, c.config.Node, c.IdentityKey())
	} else {
		err = c.toLeader(ctx, http.MethodPost, "/cluster/v1/node/key", marshal(identityRequest{Key: own}), nil)
	}
	var peer *PeerError
	switch {
	case err == nil:
		c.config.Log.Info("registered this node's identity key with the cluster")
		return true, nil
	case errors.Is(err, store.ErrFrozen), errors.Is(err, errNotEveryNode), errors.Is(err, ErrNoLeader), errors.Is(err, ErrNotLeader), errors.Is(err, ErrNotJoined),
		errors.As(err, &peer) && (peer.Status == http.StatusServiceUnavailable || peer.Status == http.StatusNotFound):
		// Not yet: the leader says when it may, or runs a release before
		// keys (it knows no such request) during a rolling upgrade.
		return false, nil
	case errors.As(err, &peer):
		return false, err
	default:
		// The leader did not answer: asked again later.
		return false, nil
	}
}

// errNotEveryNode reports a cluster with a node of a release before
// identity keys, which could not apply their registration.
var errNotEveryNode = errors.New("not every node runs a release with identity keys yet")

// identityRequest asks the leader to register the identity key of the node that
// presents it.
type identityRequest struct {
	Key string `json:"key"`
}

// registerHere, on the leader, registers key as node name's identity key,
// for name itself: the caller made sure that name presented key. It waits
// while changes are frozen (an upgrade thaws once every node runs its
// release) and until every node runs a release that applies the change.
func (c *Cluster) registerHere(ctx context.Context, name string, key ed25519.PublicKey) error {
	if frozen := c.config.Store.Frozen(); frozen != "" {
		return fmt.Errorf("%w: %s", store.ErrFrozen, frozen)
	}
	for _, node := range c.config.Store.Nodes() {
		if node.Name == c.config.Node {
			continue
		}
		var answer releaseAnswer
		if err := c.request(ctx, node.Name, http.MethodGet, "/cluster/v1/release", nil, &answer); err != nil || answer.Schema < store.IdentitySchema {
			return errNotEveryNode
		}
	}
	return c.config.Store.SetNodeKey(name, EncodeKey(key), "node "+name)
}

// Admit lets a host that brought an invitation join as node name, with the
// identity key key, for store.AdmissionValid. The leader admits it, under
// its lock on changes of the members: a name of one of the voters, or a
// node at work, is refused.
func (c *Cluster) Admit(ctx context.Context, name string, key ed25519.PublicKey) error {
	for {
		var err error
		if c.raft.State() == raft.Leader {
			err = c.admitHere(ctx, name, key)
		} else {
			err = c.toLeader(ctx, http.MethodPost, "/cluster/v1/admissions", marshal(admitRequest{Name: name, Key: EncodeKey(key)}), nil)
		}
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

// admitRequest asks the leader to admit a host.
type admitRequest struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

// admitHere admits a host, on the leader.
func (c *Cluster) admitHere(ctx context.Context, name string, key ed25519.PublicKey) error {
	if err := c.holdMembership(ctx); err != nil {
		return err
	}
	defer c.unlockMembership()
	return c.config.Store.Admit(name, EncodeKey(key), time.Now().Add(store.AdmissionValid), c.isVoter, "murder")
}

// releaseAnswer is what release of Murder a node runs.
type releaseAnswer struct {
	Schema int `json:"schema"`
}

// member checks that the node name, presenting identity key key, is a
// member of the cluster: the state names it, and holds that key for it,
// or none, for a node of a release before identity keys that has not
// registered its own yet.
func (c *Cluster) member(name string, key ed25519.PublicKey) error {
	pinned, member := c.config.Store.NodeKey(name)
	switch {
	case !member:
		return fmt.Errorf("node %s is not a member of cluster %s", name, c.config.Store.Cluster())
	case pinned != "" && pinned != EncodeKey(key):
		return fmt.Errorf("the certificate naming node %s does not carry the identity key cluster %s holds for it", name, c.config.Store.Cluster())
	}
	return nil
}

// raftPeer checks a node Raft talks with: a member, or a voter the state
// no longer names, as a removal leaves it between taking the node out of
// the state and out of the voters, or for good if it stopped there. That
// one is taken by the identity key it had, which the state keeps as
// departed: never by its name alone.
func (c *Cluster) raftPeer(name string, key ed25519.PublicKey) error {
	err := c.member(name, key)
	if err == nil {
		return nil
	}
	if _, named := c.config.Store.NodeKey(name); named || !c.isVoter(name) {
		return err
	}
	return c.departing(name, key)
}

// departing checks a node taken out of the state on its way out of the
// voters: it presents the identity key it had, or, if it had registered
// none (a cluster from before keys), it is taken by its name, as it was
// while it was a member.
func (c *Cluster) departing(name string, key ed25519.PublicKey) error {
	departed, ok := c.config.Store.DepartedKey(name)
	if ok && (departed == "" || departed == EncodeKey(key)) {
		return nil
	}
	return fmt.Errorf("node %s, a voter on its way out, did not present the identity key it had", name)
}

// knownServer checks the node a connection this node made reached: a
// member, or, while this node joins a cluster, one of the members the
// invitation named, with the key it named.
func (c *Cluster) knownServer(name string, key ed25519.PublicKey) error {
	keys := c.joinKeys.Load()
	if keys == nil {
		if c.config.Store.Cluster() == "" {
			return errors.New("this node is part of no cluster yet, and trusts no node: join one with an invitation")
		}
		return c.raftPeer(name, key)
	}
	want, named := (*keys)[name]
	switch {
	case !named:
		return fmt.Errorf("node %s is not one of the members the invitation named", name)
	case want != "" && want != EncodeKey(key):
		return fmt.Errorf("node %s does not present the identity key the invitation named for it", name)
	}
	return nil
}

// Invitation is an invitation to this node, for its secret.
func (c *Cluster) Invitation(secret []byte) Invite {
	return Invite{Address: c.config.Address, Node: c.config.Node, Key: c.IdentityKey(), Secret: secret}
}

// Rekey has the node present and trust what seeds, derived from the master
// key ring as it is now, make. Connections made before stay; idle ones
// close, so the next request makes a new one.
func (c *Cluster) Rekey(seeds [][]byte) error {
	if err := c.credentials.Update(seeds, time.Now()); err != nil {
		return err
	}
	c.client.CloseIdleConnections()
	return nil
}

// InviteSecret makes a new invitation secret.
func InviteSecret() ([]byte, error) {
	secret := make([]byte, inviteSecretSize)
	_, err := rand.Read(secret)
	return secret, err
}

// Close stops the node's part of the cluster; the store's own state stays.
func (c *Cluster) Close() error {
	close(c.done)
	err := errors.Join(c.server.Close(), c.joins.Close())
	err = errors.Join(err, c.raft.Shutdown().Error(), c.listener.close())
	// Shutdown does not wait for the heartbeats the transport is handling:
	// the log closes once none is.
	c.transport.stopHeartbeats()
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
	ctx, cancel := c.bounded(context.Background(), commitTimeout)
	defer cancel()
	if batch.LeaderOnly {
		return c.commitHere(ctx, batch, data)
	}
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

// commitHere appends a change of who the nodes are, which only the leader
// makes, to the log: never through another node. A node that does not lead
// returns ErrNotLeader, and whoever asked asks the leader.
func (c *Cluster) commitHere(ctx context.Context, batch store.Batch, data []byte) (uint64, error) {
	if c.raft.State() != raft.Leader {
		return 0, ErrNotLeader
	}
	index, err := c.apply(data)
	if errors.Is(err, ErrMaybe) {
		// Leadership moved while it was in flight: once this node has
		// applied what the new leader has, it knows whether it was made.
		if catchUpErr := c.catchUp(); catchUpErr != nil {
			return 0, errors.Join(err, catchUpErr)
		}
		outcome, ok := c.config.Store.Outcome(batch.ID)
		if !ok {
			return 0, ErrNotLeader
		}
		index, err = outcome.Index, outcome.Err
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
func (c *Cluster) catchUp() error { return c.catchUpWithin(context.Background()) }

// catchUpWithin is catchUp, given up once ctx ends.
func (c *Cluster) catchUpWithin(parent context.Context) error {
	ctx, cancel := c.bounded(parent, commitTimeout)
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

// CatchUpContext is CatchUp, given up once ctx ends: a node that cannot
// reach the cluster's leader, such as one removed while it was down,
// would otherwise hold whoever waits for as long as a change may take.
func (c *Cluster) CatchUpContext(ctx context.Context) error { return c.catchUpWithin(ctx) }

// bounded is ctx, ended after timeout, or as soon as the node closes:
// nothing waits on a cluster that is gone, such as a write left over as
// the agent stops.
func (c *Cluster) bounded(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	if c.done != nil {
		go func() {
			select {
			case <-c.done:
				cancel()
			case <-ctx.Done():
			}
		}()
	}
	return ctx, cancel
}

// leaderApplied is the index of the last change the leader applied, which
// is at least that of every change acknowledged. A new leader first applies
// what the leaders before it committed: a barrier, once per term, waits for
// that.
func (c *Cluster) leaderApplied() (uint64, error) {
	term := c.raft.CurrentTerm()
	var err error
	if c.barrierTerm.Load() != term {
		if err = waitFuture(c.raft.Barrier(commitTimeout), commitTimeout+futureGrace); err == nil {
			c.barrierTerm.Store(term)
		}
	} else {
		// A Raft that stalls answers neither: a writer waiting to learn
		// what happened to its change returns all the same.
		err = verifyLeader(c.catchUpVerify, c.raft.VerifyLeader, commitTimeout+futureGrace)
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

// futureGrace is how long past its own timeout a Raft future is waited
// for.
const futureGrace = 5 * time.Second

// errStalled is a Raft future that did not end in time: what it did, if
// anything, is not known.
var errStalled = errors.New("the log did not answer in time")

// verifyLeader runs verify, Raft's VerifyLeader, and waits for its answer,
// all within d. Unlike Raft's other calls, VerifyLeader has no timeout for
// handing its request over: once Raft's queue of them is full, the call
// itself blocks until Raft takes one. So the call runs in a goroutine,
// which slot, a channel of one, lets only one caller at a time start: a
// stalled Raft holds at most one goroutine per slot, and each caller
// returns errStalled by d all the same. Requests handed over are answered
// once Raft runs again, or as it shuts down.
func verifyLeader(slot chan struct{}, verify func() raft.Future, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case slot <- struct{}{}:
	case <-timer.C:
		return errStalled
	}
	queued := make(chan raft.Future, 1)
	go func() {
		future := verify()
		<-slot
		queued <- future
	}()
	var future raft.Future
	select {
	case future = <-queued:
	case <-timer.C:
		return errStalled
	}
	done := make(chan error, 1)
	go func() { done <- future.Error() }()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		return errStalled
	}
}

// waitFuture waits for future at most d. One that does not end by then is
// errStalled: it may still take effect.
func waitFuture(future raft.Future, d time.Duration) error {
	done := make(chan error, 1)
	go func() { done <- future.Error() }()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		return errStalled
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
	// Apply's timeout bounds getting into the log only; a log that stalls
	// (a disk that hangs) must not hold the writer for good.
	if err := waitFuture(future, commitTimeout+futureGrace); err != nil {
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
// restored starts one from the state of a cluster, as a backup restored
// it: the cluster is rebuilt under a new name, so no node of the one the
// backup came from is taken for one of it, and this node's identity key
// goes in place of the one the state held for it. Its other nodes join
// again with invitations.
func (c *Cluster) Bootstrap(ctx context.Context, actor string, restored bool) error {
	own := EncodeKey(c.IdentityKey())
	if err := c.unconfigured(); err == nil {
		if c.config.Store.Cluster() != "" && !restored {
			return fmt.Errorf("%w: this node's database belongs to cluster %s; give it a new database, or, for one restored from a backup, rebuild the cluster from it with `murder cluster bootstrap -restored`", ErrMember, c.config.Store.Cluster())
		}
		// A rebuild is noted before anything changes, with the name it
		// leaves: asked again, the bootstrap goes on with it.
		if restored && c.config.Store.Cluster() != "" {
			if err := beginRebuild(c.config.Dir, c.config.Store.Cluster()); err != nil {
				return fmt.Errorf("note the rebuild: %w", err)
			}
		}
		if err := c.config.Store.StartLog(); err != nil {
			return err
		}
		configuration := raft.Configuration{Servers: []raft.Server{{Suffrage: raft.Voter, ID: raft.ServerID(c.config.Node), Address: raft.ServerAddress(c.address)}}}
		if err := c.raft.BootstrapCluster(configuration).Error(); err != nil {
			return fmt.Errorf("bootstrap: %w", err)
		}
	} else if key, _ := c.config.Store.NodeKey(c.config.Node); !c.alone() || c.rebuilding() == "" && c.config.Store.Cluster() != "" && key == own {
		// Done already, or a node of a cluster of more.
		return err
	}
	for c.raft.State() != raft.Leader {
		select {
		case <-ctx.Done():
			return fmt.Errorf("bootstrap: no leadership: %w", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	// A new cluster, or one rebuilt that still has the name of the one it
	// was restored from, gets a name of its own.
	rebuilt, err := rebuilding(c.config.Dir)
	if err != nil {
		return fmt.Errorf("read the note of the rebuild: %w", err)
	}
	if c.config.Store.Cluster() == "" || rebuilt != "" && c.config.Store.Cluster() == rebuilt {
		if err := c.config.Store.NameCluster(newClusterName(), actor); err != nil {
			return err
		}
	}
	if err := c.config.Store.PutNode(store.Node{Name: c.config.Node, Address: c.address, Key: own}, actor); err != nil {
		return err
	}
	return endRebuild(c.config.Dir)
}

// rebuilding is the name a rebuild under way leaves, or empty.
func (c *Cluster) rebuilding() string {
	name, err := rebuilding(c.config.Dir)
	if err != nil {
		return "?"
	}
	return name
}

// ReplaceKey gives node name the identity key key in place of the one the
// state holds, as an operator asks for a node that lost its key file (it
// shows its new key with `murder node key`): it keeps its place, its
// tenants and its data. The leader does it, under its lock on changes of
// the members, never while changes are frozen, and the connections of the
// old key are closed.
func (c *Cluster) ReplaceKey(ctx context.Context, name string, key ed25519.PublicKey, actor string) error {
	for {
		var err error
		if c.raft.State() == raft.Leader {
			err = c.replaceKeyHere(ctx, name, key, actor)
		} else {
			err = c.toLeader(ctx, http.MethodPut, "/cluster/v1/nodes/"+url.PathEscape(name)+"/key?actor="+url.QueryEscape(actor), marshal(identityRequest{Key: EncodeKey(key)}), nil)
		}
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

// replaceKeyHere replaces a node's key, on the leader.
func (c *Cluster) replaceKeyHere(ctx context.Context, name string, key ed25519.PublicKey, actor string) error {
	if err := c.holdMembership(ctx); err != nil {
		return err
	}
	defer c.unlockMembership()
	if frozen := c.config.Store.Frozen(); frozen != "" {
		return fmt.Errorf("%w: %s; give node %s its new key once changes are taken again", store.ErrFrozen, frozen, name)
	}
	if err := c.config.Store.ReplaceNodeKey(name, EncodeKey(key), actor); err != nil {
		return err
	}
	// Nothing goes on over a connection of the old key.
	c.listener.sweep(c.raftPeer)
	c.client.CloseIdleConnections()
	return nil
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

// ErrOlderCluster refuses to join a cluster whose nodes run a release
// before identity keys: they neither admit a node's key nor name theirs.
var ErrOlderCluster = errors.New("the cluster runs a release before node identity keys, which admits no node of this one: upgrade it first (murder upgrade), then make a new invitation")

// JoinInvited joins this node to the cluster an invitation names: the node
// the invitation names admits this node's name and identity key, and
// tells which members to trust, then this node joins.
func (c *Cluster) JoinInvited(ctx context.Context, invite Invite, actor string) error {
	if err := c.unconfigured(); err != nil {
		return err
	}
	// The inviting node's certificate must be of this node's authority: a
	// node of another master key would not take it in.
	invite.CA = c.credentials.CAKey()
	bundle, err := Redeem(ctx, invite, c.config.Node, c.IdentityKey())
	if errors.Is(err, errOtherAuthority) {
		return errOtherAuthority
	}
	if err != nil {
		return err
	}
	if len(bundle.Members) == 0 {
		return ErrOlderCluster
	}
	return c.Join(ctx, invite.Address, bundle.Members, actor)
}

// Join adds this node to the cluster of the node at peer, once an
// invitation admitted its name and identity key. members are the
// members' identity keys the invitation named, by name: until this node
// holds the cluster's state, it talks to them only. Only a node with a new
// database joins, since it takes the cluster's state; a node whose join
// stopped half way, holding this cluster's state, tries again.
func (c *Cluster) Join(ctx context.Context, peer string, members map[string]string, actor string) error {
	if len(members) == 0 {
		return errors.New("a node joins with an invitation: run `murder cluster invite` on a node of the cluster, then `murder cluster join <invitation>` here")
	}
	if err := c.unconfigured(); err != nil {
		return err
	}
	// While it joins, the node talks to the members the invitation named;
	// once it has, to those its state names.
	// Connections made before, under other trust, are not reused.
	keys := maps.Clone(members)
	c.joinKeys.Store(&keys)
	c.client.CloseIdleConnections()
	defer func() {
		c.joinKeys.Store(nil)
		c.client.CloseIdleConnections()
	}()
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
	// Create creates tenant to run on this node, once no data an earlier
	// tenant of its name left is here.
	Create(ctx context.Context, tenant store.NewTenant, actor string) (store.Record, error)
	Move(ctx context.Context, tenant, to, actor string) error
}

// Usage is what a node measured of itself and its tenants, for the
// others to show.
type Usage interface {
	// Report is the node's use now; Series a tenant's day in steps.
	Report() any
	Series(tenant string, step time.Duration) (any, bool)
}

// UsageOn decodes node's use now into out.
func (c *Cluster) UsageOn(ctx context.Context, node string, out any) error {
	return c.request(ctx, node, http.MethodGet, "/cluster/v1/usage", nil, out)
}

// TenantUsageOn decodes the day of a tenant node runs into out.
func (c *Cluster) TenantUsageOn(ctx context.Context, node, tenant string, step time.Duration, out any) error {
	path := "/cluster/v1/tenants/" + url.PathEscape(tenant) + "/usage?step=" + strconv.Itoa(int(step/time.Second))
	return c.stream(ctx, node, http.MethodGet, path, nil, out)
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

// createRequest asks a node to create a tenant to run there.
type createRequest struct {
	Tenant store.NewTenant `json:"tenant"`
	Actor  string          `json:"actor"`
}

// CreateOn has node create tenant to run there.
func (c *Cluster) CreateOn(ctx context.Context, node string, tenant store.NewTenant, actor string) (store.Record, error) {
	var record store.Record
	body := marshal(createRequest{Tenant: tenant, Actor: actor})
	err := c.stream(ctx, node, http.MethodPost, "/cluster/v1/tenants/"+url.PathEscape(tenant.Name), bytes.NewReader(body), &record)
	return record, err
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

// Ping reports whether node answers on the cluster port, within ctx.
func (c *Cluster) Ping(ctx context.Context, node string) error {
	return c.request(ctx, node, http.MethodGet, "/cluster/v1/ping", nil, nil)
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
// It is cancelled once node has answered no ping for long: one that died
// with the request unacknowledged would otherwise hold it for as long as
// the kernel retransmits, many minutes, keepalive or not.
func (c *Cluster) stream(ctx context.Context, node, method, path string, body io.Reader, out any) error {
	ctx, stop := watchPeer(ctx, func(ctx context.Context) error { return c.Ping(ctx, node) }, peerPingEvery, peerSilentFor)
	defer stop()
	return c.request(ctx, node, method, path, body, out)
}

// request is stream without the watch: for pings, which their caller
// bounds.
func (c *Cluster) request(ctx context.Context, node, method, path string, body io.Reader, out any) error {
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
		if cause := context.Cause(ctx); errors.Is(cause, errPeerSilent) {
			err = cause
		}
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
	Install(ctx context.Context, binary io.Reader, sha, freeze string) error
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

// Install sends a node of the cluster a binary to install, if changes are
// still frozen for freeze there.
func (c *Cluster) Install(ctx context.Context, node string, binary io.Reader, sha, freeze string) error {
	address, err := c.addressOf(node)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, "https://"+address+"/cluster/v1/node/binary", binary)
	if err != nil {
		return err
	}
	request.Header.Set(shaHeader, sha)
	if freeze != "" {
		request.Header.Set(freezeHeader, freeze)
	}
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

// shaHeader carries the SHA-256 a binary must have, freezeHeader the
// freeze of the upgrade that sends it.
const (
	shaHeader    = "X-Murder-Sha256"
	freezeHeader = "X-Murder-Freeze"
)

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
	// Key is the identity key the state holds for the node: empty for a
	// node of a cluster from before identity keys that has not registered
	// its own yet. For a node on its way out, it is the key it had.
	Key string `json:"key,omitempty"`
	// Leaving is set for a node taken out of the state that is still a
	// voter: Raft takes it by Key, or by its name if that is empty, until
	// its removal finishes.
	Leaving bool `json:"leaving,omitempty"`
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
		member := Member{Name: string(server.ID), Address: string(server.Address), Suffrage: server.Suffrage.String()}
		key, named := c.config.Store.NodeKey(member.Name)
		member.Key = key
		if !named {
			member.Key, member.Leaving = c.config.Store.DepartedKey(member.Name)
		}
		status.Members = append(status.Members, member)
	}
	slices.SortFunc(status.Members, func(a, b Member) int { return strings.Compare(a.Name, b.Name) })
	return status, nil
}

// Leader reports whether this node leads the cluster.
func (c *Cluster) Leader() bool { return c.raft.State() == raft.Leader }

// Term is the node's current Raft term.
func (c *Cluster) Term() uint64 { return c.raft.CurrentTerm() }

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
