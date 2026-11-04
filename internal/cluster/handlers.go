package cluster

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"os"
	"strconv"
	"time"

	"github.com/hashicorp/raft"

	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/upgrade"
)

// handler serves other nodes. Only members of this cluster are served:
// nodes the state names, presenting the identity key it holds for them.
// So neither a node removed from it, nor one of another cluster with the
// same master key, nor whoever holds the master key and names a member in
// a certificate, changes anything here. A node joins, and fetches the
// state it joins with, before it is a member: an invitation admitted its
// name and key.
func (c *Cluster) handler() http.Handler {
	mux := http.NewServeMux()
	members := func(pattern string, handle http.HandlerFunc) { mux.HandleFunc(pattern, c.membersOnly(handle)) }
	members("POST /cluster/v1/commit", c.handleCommit)
	members("GET /cluster/v1/index", c.handleIndex)
	mux.HandleFunc("GET /cluster/v1/snapshot", c.handleSnapshot)
	mux.HandleFunc("POST /cluster/v1/nodes", c.handleJoin)
	mux.HandleFunc("DELETE /cluster/v1/nodes/{name}", c.membersOrLeaving(c.handleRemove))
	members("GET /cluster/v1/node", c.handleNodeInfo)
	members("PUT /cluster/v1/tenants/{name}/data", c.handleReceive)
	members("DELETE /cluster/v1/tenants/{name}/data", c.handleDiscard)
	members("POST /cluster/v1/tenants/{name}", c.handleCreate)
	members("GET /cluster/v1/tenants/{name}/status", c.handleTenantStatus)
	members("POST /cluster/v1/tenants/{name}/move", c.handleMove)
	members("PUT /cluster/v1/node/binary", c.handleInstall)
	members("GET /cluster/v1/ping", c.handlePing)
	members("GET /cluster/v1/release", c.handleRelease)
	members("POST /cluster/v1/node/key", c.handleRegisterKey)
	members("POST /cluster/v1/admissions", c.handleAdmit)
	members("PUT /cluster/v1/nodes/{name}/key", c.handleReplaceKey)
	members("GET /cluster/v1/tenants/{name}/replica", c.handleSnapshots)
	members("PUT /cluster/v1/tenants/{name}/replica", c.handleReplicate)
	members("GET /cluster/v1/usage", c.handleUsage)
	members("GET /cluster/v1/digest", c.handleDigest)
	members("GET /cluster/v1/tenants/{name}/usage", c.handleTenantUsage)
	members("GET /cluster/v1/keys", c.handleKeyRing)
	members("POST /cluster/v1/keys", c.handleKeyAdd)
	members("POST /cluster/v1/keys/{id}/use", c.handleKeyUse)
	members("DELETE /cluster/v1/keys/{id}", c.handleKeyDrop)
	return mux
}

// clusterHeader names the cluster of the node that sends a request.
const clusterHeader = "X-Murder-Cluster"

// namedTransport says, on every request to another node, which cluster
// this node belongs to.
type namedTransport struct {
	cluster func() string
	next    http.RoundTripper
	// check, if set, checks the node a connection reached, by the name and
	// identity key its certificate carries, each time a request is to go
	// out on a connection made before: the nodes may have changed since it
	// was checked as it was made.
	check func(name string, key ed25519.PublicKey) error
}

func (t namedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	ctx := request.Context()
	if t.check != nil {
		ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GotConn: t.checkConn})
	}
	request = request.Clone(ctx)
	request.Header.Set(clusterHeader, t.cluster())
	return t.next.RoundTrip(request)
}

// checkConn checks the node a reused connection reached before a request
// goes out on it, and closes the connection if the node is not one to
// talk to any more: nothing of the request has been written, and the
// client sends it again on a new connection, checked as it is made.
func (t namedTransport) checkConn(info httptrace.GotConnInfo) {
	if !info.Reused {
		return
	}
	conn, ok := info.Conn.(*tls.Conn)
	if !ok {
		_ = info.Conn.Close()
		return
	}
	state := conn.ConnectionState()
	name, key, err := peerIdentity(&state)
	if err == nil {
		err = t.check(name, key)
	}
	if err != nil {
		_ = conn.Close()
	}
}

// CloseIdleConnections closes the idle connections of the transport below:
// without it, the client's would close none.
func (t namedTransport) CloseIdleConnections() {
	if closer, ok := t.next.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

// membersOnly serves nodes of this cluster that are its members.
func (c *Cluster) membersOnly(handle http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ours := c.config.Store.Cluster()
		if theirs := r.Header.Get(clusterHeader); ours == "" || theirs != ours {
			writeError(w, http.StatusForbidden, fmt.Errorf("node %s belongs to cluster %q, not %q", c.config.Node, ours, theirs))
			return
		}
		name, key, err := peerIdentity(r.TLS)
		if err != nil {
			writeError(w, http.StatusForbidden, err)
			return
		}
		if err := c.member(name, key); err != nil {
			writeError(w, http.StatusForbidden, err)
			return
		}
		handle(w, r)
	}
}

// membersOrLeaving serves members, and a node on its way out, taken out
// of the state but not yet out of the voters, for its own removal and
// nothing else: so it finishes the removal it began, asked again.
func (c *Cluster) membersOrLeaving(handle http.HandlerFunc) http.HandlerFunc {
	members := c.membersOnly(handle)
	return func(w http.ResponseWriter, r *http.Request) {
		name, key, err := peerIdentity(r.TLS)
		ours := c.config.Store.Cluster()
		if err != nil || ours == "" || r.Header.Get(clusterHeader) != ours || name != r.PathValue("name") {
			members(w, r)
			return
		}
		if _, named := c.config.Store.NodeKey(name); named || c.departing(name, key) != nil {
			members(w, r)
			return
		}
		handle(w, r)
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, failure{Error: err.Error()})
}

// notLeader answers what only the leader takes, if this node is not it.
func (c *Cluster) notLeader(w http.ResponseWriter) bool {
	if c.raft.State() == raft.Leader {
		return false
	}
	c.moved(w)
	return true
}

// moved names the leader, as far as this node knows it.
func (c *Cluster) moved(w http.ResponseWriter) {
	leader, _ := c.raft.LeaderWithID()
	if leader == raft.ServerAddress(c.address) {
		leader = ""
	}
	writeJSON(w, http.StatusMisdirectedRequest, failure{Error: "not the leader", Leader: string(leader)})
}

// handleCommit appends another node's batch to the log.
func (c *Cluster) handleCommit(w http.ResponseWriter, r *http.Request) {
	if c.notLeader(w) {
		return
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBatch))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	index, err := c.apply(data)
	var refused *PeerError
	switch {
	case errors.Is(err, ErrNotLeader):
		c.moved(w)
	case errors.As(err, &refused):
		writeError(w, refused.Status, err)
	case index == 0:
		writeError(w, http.StatusServiceUnavailable, err)
	case err != nil:
		writeJSON(w, http.StatusOK, commitResult{Index: index, Error: err.Error(), Stale: errors.Is(err, store.ErrStale)})
	default:
		writeJSON(w, http.StatusOK, commitResult{Index: index})
	}
}

// handleRegisterKey registers the identity key of the node that asks, as
// its certificate carries it, once the state holds none for it.
func (c *Cluster) handleRegisterKey(w http.ResponseWriter, r *http.Request) {
	if c.notLeader(w) {
		return
	}
	name, key, err := peerIdentity(r.TLS)
	if err != nil {
		writeError(w, http.StatusForbidden, err)
		return
	}
	var request identityRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	// A node registers the key it presents, for its own name: none other.
	if encoded := EncodeKey(key); encoded == "" || request.Key != encoded {
		writeError(w, http.StatusForbidden, fmt.Errorf("node %s registers only the identity key its certificate carries", name))
		return
	}
	err = c.registerHere(r.Context(), name, key)
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, struct{}{})
	case errors.Is(err, ErrNotLeader) || errors.Is(err, ErrNoLeader):
		c.moved(w)
	case errors.Is(err, store.ErrFrozen), errors.Is(err, errNotEveryNode):
		writeError(w, http.StatusServiceUnavailable, err)
	case errors.Is(err, store.ErrConflict):
		writeError(w, http.StatusConflict, err)
	default:
		writeError(w, http.StatusUnprocessableEntity, err)
	}
}

// handleReplaceKey gives a node the identity key an operator asked for on
// the asking node.
func (c *Cluster) handleReplaceKey(w http.ResponseWriter, r *http.Request) {
	if c.notLeader(w) {
		return
	}
	var request identityRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	key, err := base64.StdEncoding.DecodeString(request.Key)
	if err != nil || len(key) != ed25519.PublicKeySize {
		writeError(w, http.StatusBadRequest, errors.New("an identity key is an ed25519 public key in base64"))
		return
	}
	err = c.replaceKeyHere(r.Context(), r.PathValue("name"), key, r.URL.Query().Get("actor"))
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, struct{}{})
	case errors.Is(err, ErrNotLeader) || errors.Is(err, ErrNoLeader):
		c.moved(w)
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, err)
	case errors.Is(err, store.ErrFrozen):
		writeError(w, http.StatusConflict, err)
	default:
		writeError(w, http.StatusUnprocessableEntity, err)
	}
}

// handleAdmit admits a host an invitation made on the asking node let in.
func (c *Cluster) handleAdmit(w http.ResponseWriter, r *http.Request) {
	if c.notLeader(w) {
		return
	}
	var request admitRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	key, err := base64.StdEncoding.DecodeString(request.Key)
	if err != nil || len(key) != ed25519.PublicKeySize {
		writeError(w, http.StatusBadRequest, errors.New("an identity key is an ed25519 public key in base64"))
		return
	}
	err = c.admitHere(r.Context(), request.Name, key)
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, struct{}{})
	case errors.Is(err, ErrNotLeader) || errors.Is(err, ErrNoLeader):
		c.moved(w)
	case errors.Is(err, store.ErrConflict), errors.Is(err, store.ErrFrozen):
		writeError(w, http.StatusConflict, err)
	default:
		writeError(w, http.StatusUnprocessableEntity, err)
	}
}

// handleIndex names how far the leader has applied the log, for a node
// about to write.
func (c *Cluster) handleIndex(w http.ResponseWriter, _ *http.Request) {
	if c.notLeader(w) {
		return
	}
	index, err := c.leaderApplied()
	switch {
	case errors.Is(err, ErrNotLeader):
		c.moved(w)
	case err != nil:
		writeError(w, http.StatusServiceUnavailable, err)
	default:
		writeJSON(w, http.StatusOK, indexAnswer{Index: index})
	}
}

// handleSnapshot sends this node's state to a node about to join, which an
// invitation admitted, or to a member.
func (c *Cluster) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if err := c.admitted(r); err != nil {
		writeError(w, http.StatusForbidden, err)
		return
	}
	name, err := c.config.Store.SnapshotFile()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer os.Remove(name)
	file, err := os.Open(name)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer file.Close()
	w.Header().Set("Content-Type", "application/vnd.sqlite3")
	if _, err := io.Copy(w, file); err != nil {
		panic(http.ErrAbortHandler)
	}
}

// admitted checks that a request comes from a host an invitation admitted,
// with the identity key it was admitted with, or from a member with the
// key the state holds for it, such as a node whose join stopped half way.
// A node the state holds no key for is not taken by its name here: only
// joining nodes ask, and they all have keys.
func (c *Cluster) admitted(r *http.Request) error {
	name, key, err := peerIdentity(r.TLS)
	if err != nil {
		return err
	}
	encoded := EncodeKey(key)
	if c.config.Store.Admitted(name, encoded, time.Now()) {
		return nil
	}
	if pinned, member := c.config.Store.NodeKey(name); member && pinned != "" && pinned == encoded {
		return nil
	}
	return c.notAdmitted(name, encoded)
}

// notAdmitted refuses node name, presenting identity key key, which no
// invitation admitted.
func (c *Cluster) notAdmitted(name, key string) error {
	if key == "" {
		return fmt.Errorf("node %s presents no identity key: it runs a release before them, which cannot join this cluster; set it up with this release", name)
	}
	return fmt.Errorf("node %s with this identity key was not admitted to cluster %s: join with an invitation (murder cluster invite, on a node of the cluster)", name, c.config.Store.Cluster())
}

// handleRelease names the release this node runs, by its schema: a node
// changes the state in ways only later releases apply once every node
// runs one.
func (c *Cluster) handleRelease(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, releaseAnswer{Schema: store.SchemaVersion})
}

// joinRequest asks the leader to add the node presenting the request's
// certificate.
type joinRequest struct {
	Address string `json:"address"`
	Actor   string `json:"actor"`
	// Check only checks that the node may join.
	Check bool `json:"check,omitempty"`
}

// joinAnswer names the cluster a node checked it may join.
type joinAnswer struct {
	Cluster string `json:"cluster"`
}

// memberAddress is where the state or the voters have node name.
func (c *Cluster) memberAddress(name string) (string, bool) {
	if node, known := nodeNamed(c.config.Store.Nodes(), name); known {
		return node.Address, true
	}
	return c.voterAddress(name)
}

// Voter reports whether node name is one of the cluster's voters.
func (c *Cluster) Voter(name string) bool { return c.isVoter(name) }

func (c *Cluster) isVoter(name string) bool {
	_, voter := c.voterAddress(name)
	return voter
}

// voterAddress is where the voters have node name.
func (c *Cluster) voterAddress(name string) (string, bool) {
	future := c.raft.GetConfiguration()
	if future.Error() != nil {
		return "", false
	}
	return serverAddress(future.Configuration(), name)
}

// configuration is the latest configuration of the voters, and the index
// of the log entry that made it, as one pair: Raft passed an index changes
// the voters only if the configuration is still the one the index names.
// Raft's GetConfiguration does not say its index (as of v1.8.0). Raft
// builds its own as it does here: the configuration of the newest snapshot
// that opens, replaced by the last configuration entry in the log after
// that snapshot, if there is one. Entries the log keeps from before the
// snapshot do not count: an installed snapshot may have replaced the
// history they belong to. (Raft also passes over a snapshot that opens but
// does not restore, which cannot be seen from here: changeVoters tells
// when Raft goes by another configuration.)
func (c *Cluster) configuration() (raft.Configuration, uint64, error) {
	snapshot, err := c.usableSnapshot()
	if err != nil {
		return raft.Configuration{}, 0, err
	}
	var configuration raft.Configuration
	var index, after uint64
	if snapshot != nil {
		configuration, index, after = snapshot.Configuration, snapshot.ConfigurationIndex, snapshot.Index
	}
	last, err := c.logs.LastIndex()
	if err != nil {
		return raft.Configuration{}, 0, err
	}
	first, err := c.logs.FirstIndex()
	if err != nil {
		return raft.Configuration{}, 0, err
	}
	c.joinStage(joinReading, "")
	for at := last; at > after && at >= first && at > 0; at-- {
		var entry raft.Log
		if err := c.logs.GetLog(at, &entry); errors.Is(err, raft.ErrLogNotFound) {
			// Compacted meanwhile, into a newer snapshot: the pair read
			// is older than Raft's, and Raft refuses a change to it.
			break
		} else if err != nil {
			return raft.Configuration{}, 0, err
		}
		if entry.Type == raft.LogConfiguration {
			return raft.DecodeConfiguration(entry.Data), at, nil
		}
	}
	if snapshot == nil {
		return raft.Configuration{}, 0, errors.New("neither the log nor a snapshot holds a configuration")
	}
	return configuration, index, nil
}

// usableSnapshot is the newest snapshot that opens, as Raft restores the
// newest that does; nil if there is none at all. Opening checks the
// snapshot whole; it is closed again unread.
func (c *Cluster) usableSnapshot() (*raft.SnapshotMeta, error) {
	snapshots, err := c.snapshots.List()
	if err != nil {
		return nil, err
	}
	var failures []error
	for _, snapshot := range snapshots {
		_, contents, err := c.snapshots.Open(snapshot.ID)
		if err != nil {
			failures = append(failures, fmt.Errorf("snapshot %s: %w", snapshot.ID, err))
			continue
		}
		_ = contents.Close()
		return snapshot, nil
	}
	if len(snapshots) > 0 {
		// Raft would not have started.
		return nil, fmt.Errorf("no snapshot opens: %w", errors.Join(failures...))
	}
	return nil, nil
}

// errVotersChanged is a change to the voters Raft refused, as they changed
// since they were read: asking again reads them afresh.
var errVotersChanged = errors.New("the voters changed meanwhile")

// changeVoters has Raft make change to the voters as they are at index,
// and tells apart why Raft refuses it: the voters changed since (errVotersChanged),
// or Raft goes by a configuration other than the one this node's log and
// snapshots name. Raft may just be storing a change it already counts, so
// that is only concluded when it refuses the same index twice, a moment
// apart.
func (c *Cluster) changeVoters(change func(index uint64) raft.IndexFuture, index uint64) error {
	for attempt := 1; ; attempt++ {
		err := waitFuture(change(index), commitTimeout+futureGrace)
		var since, latest uint64
		if _, scanErr := fmt.Sscanf(fmt.Sprint(err), "configuration changed since %d (latest is %d)", &since, &latest); err == nil || scanErr != nil {
			return err
		}
		if _, read, readErr := c.configuration(); readErr != nil || read != index {
			return errVotersChanged
		}
		if attempt == 2 {
			return fmt.Errorf("Raft goes by the voters as of log entry %d, while this node's log and snapshots name those as of %d, maybe as Raft passed over a snapshot it could not restore; restart node %s to have Raft read them afresh", latest, index, c.config.Node)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// serverAddress is where configuration has node name.
func serverAddress(configuration raft.Configuration, name string) (string, bool) {
	for _, server := range configuration.Servers {
		if server.ID == raft.ServerID(name) {
			return string(server.Address), true
		}
	}
	return "", false
}

// The stages of a join a test can pause it at.
const (
	joinWrite   = "write"   // before the node is written to the state
	joinVote    = "vote"    // after that, before the checks for the voters
	joinReading = "reading" // part way through reading the voters, for a join or a removal
	joinAdd     = "add"     // after the checks, before the node is a voter
)

// joinStage runs the test hook, if any, at stage of a join.
func (c *Cluster) joinStage(stage, name string) {
	if hook := c.joinHook.Load(); hook != nil {
		(*hook)(stage, name)
	}
}

// handleJoin adds a node: first to the state, so its role is known before
// it votes, then to the voters.
func (c *Cluster) handleJoin(w http.ResponseWriter, r *http.Request) {
	if c.notLeader(w) {
		return
	}
	// The join keeps to the term it started in. A leader that lost its
	// leadership and won it back may find the node removed meanwhile, by
	// the leader between: adding it to the voters then would make a voter
	// the state does not know.
	term := c.raft.CurrentTerm()
	name, key, err := peerIdentity(r.TLS)
	if err != nil {
		writeError(w, http.StatusForbidden, err)
		return
	}
	encoded := EncodeKey(key)
	var request joinRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	// One change of the members at a time, its checks and both its writes:
	// two hosts joining with one name cannot both pass the checks and leave
	// the state and the voters naming different addresses.
	if !c.lockMembership(w, r) {
		return
	}
	defer c.unlockMembership()
	// A node of the same name elsewhere would take the member's place.
	if address, member := c.memberAddress(name); member && address != request.Address {
		writeError(w, http.StatusConflict, fmt.Errorf("node %s is already a member at %s; remove it first, or give the new node another name", name, address))
		return
	}
	// Nor does one take its place at the same address: a node joins with an
	// empty log, which, as a voter, would let a node that lacks committed
	// entries be elected. Every join checks first, so the check refuses it;
	// the join itself, asked again after leadership moved, finds the node
	// a voter already and adds it again, which changes nothing.
	if request.Check && c.isVoter(name) {
		writeError(w, http.StatusConflict, fmt.Errorf("node %s is a member already; to bring it back with a new database, run \"murder cluster remove %s\" on another node first", name, name))
		return
	}
	// Only a host an invitation admitted joins, with the key it was
	// admitted with; asked again, the node the state holds with that key.
	// The write checks the same, as part of the replicated change, and
	// never puts another key in the place of a voter's.
	if err := c.admitted(r); err != nil {
		writeError(w, http.StatusForbidden, err)
		return
	}
	if request.Check {
		writeJSON(w, http.StatusOK, joinAnswer{Cluster: c.config.Store.Cluster()})
		return
	}
	actor := request.Actor
	if actor == "" {
		actor = "node " + name
	}
	c.joinStage(joinWrite, name)
	// The mutex keeps joins apart on this node only: a leader that lost
	// its leadership meanwhile still gets here, and its write, which only
	// a leader makes, is refused; the host asks the next leader. The write
	// refuses a node that one added at another address, and another key
	// for a voter, as part of the replicated change.
	if err := c.config.Store.AddNode(store.Node{Name: name, Address: request.Address, Key: encoded}, c.isVoter, actor); err != nil {
		switch {
		case errors.Is(err, ErrNotLeader) || errors.Is(err, ErrNoLeader):
			c.moved(w)
		case errors.Is(err, store.ErrNotAdmitted):
			writeError(w, http.StatusForbidden, err)
		case errors.Is(err, store.ErrConflict):
			writeError(w, http.StatusConflict, err)
		default:
			writeError(w, http.StatusUnprocessableEntity, err)
		}
		return
	}
	c.joinStage(joinVote, name)
	// The state names the node at its address now. Only a leader that
	// still leads, in the same term, adds it to the voters, and only if the
	// state still names it there and the voters do not name it elsewhere:
	// in one term no other join or removal changes them, as no other node
	// leads, and this one's are held off by the mutex.
	if err := verifyLeader(c.joinVerify, c.raft.VerifyLeader, commitTimeout+futureGrace); err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			c.moved(w)
			return
		}
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	if c.raft.CurrentTerm() != term {
		// Asking again writes the node afresh.
		c.moved(w)
		return
	}
	if node, known := nodeNamed(c.config.Store.Nodes(), name); !known || node.Address != request.Address {
		c.moved(w)
		return
	}
	// Every decision on the voters is made from this one configuration,
	// and the change made to it alone.
	configuration, index, err := c.configuration()
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	if address, voter := serverAddress(configuration, name); voter && address != request.Address {
		writeError(w, http.StatusConflict, fmt.Errorf("node %s is already a voter at %s; remove it first, or give the new node another name", name, address))
		return
	}
	c.joinStage(joinAdd, name)
	// Raft adds the voter only to the configuration the checks saw: one
	// that changed meanwhile, as under a leader between, is not taken
	// over, nor a node of the same name moved to this host's address.
	addVoter := func(index uint64) raft.IndexFuture {
		return c.raft.AddVoter(raft.ServerID(name), raft.ServerAddress(request.Address), index, commitTimeout)
	}
	if err := c.changeVoters(addVoter, index); err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			// The node may be a voter now, or not: asking again is safe.
			c.moved(w)
			return
		}
		if errors.Is(err, errVotersChanged) {
			// The voters changed since the checks: asking again checks
			// them afresh.
			c.moved(w)
			return
		}
		writeError(w, http.StatusServiceUnavailable, fmt.Errorf("add %s to the voters: %w", name, err))
		return
	}
	// Terms only grow: the same term now means the voter was added in it.
	// Otherwise leadership was lost and won back between the checks and
	// the change, and the node may have been removed meanwhile. A voter
	// the state does not name is taken out again; either way the host is
	// told to ask again, which writes the node and adds it afresh.
	if c.raft.CurrentTerm() != term {
		if err := c.catchUp(); err != nil {
			// What the state says is not known: the voter stays as it is,
			// and the host asks again.
			c.moved(w)
			return
		}
		node, known := nodeNamed(c.config.Store.Nodes(), name)
		if known && node.Address == request.Address {
			writeJSON(w, http.StatusOK, struct{}{})
			return
		}
		// Each undo too changes only the configuration it was decided
		// on; one that changed meanwhile is left to the host's next ask.
		if configuration, index, err := c.configuration(); err == nil {
			address, voter := serverAddress(configuration, name)
			switch {
			case known && voter && address != node.Address:
				// Another node of that name joined meanwhile: the voter
				// goes back to its address, never out.
				_ = waitFuture(c.raft.AddVoter(raft.ServerID(name), raft.ServerAddress(node.Address), index, commitTimeout), commitTimeout+futureGrace)
			case !known && voter:
				_ = waitFuture(c.raft.RemoveServer(raft.ServerID(name), index, commitTimeout), commitTimeout+futureGrace)
			}
		}
		c.moved(w)
		return
	}
	writeJSON(w, http.StatusOK, struct{}{})
}

// lockMembership waits for the members to be this request's to change,
// for as long as its sender waits: a change whose sender gave up, such as
// one queued behind a join that stalled, is not made later, from checks
// its sender no longer stands behind.
func (c *Cluster) lockMembership(w http.ResponseWriter, r *http.Request) bool {
	refuse := func() bool {
		writeError(w, http.StatusServiceUnavailable, fmt.Errorf("the change of the members was given up: %w", r.Context().Err()))
		return false
	}
	if r.Context().Err() != nil {
		return refuse()
	}
	select {
	case c.membership <- struct{}{}:
	case <-r.Context().Done():
		return refuse()
	}
	// A select between a free lock and a sender that gave up takes either.
	if r.Context().Err() != nil {
		c.unlockMembership()
		return refuse()
	}
	return true
}

func (c *Cluster) unlockMembership() { <-c.membership }

// holdMembership takes the members for a change this node makes on its own
// account (an admission, a new key, the upkeep's cleanup), for as long as
// ctx lasts; unlockMembership gives them back.
func (c *Cluster) holdMembership(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case c.membership <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	// A select between a free lock and a context that ended takes either.
	if err := ctx.Err(); err != nil {
		c.unlockMembership()
		return err
	}
	return nil
}

// handleRemove takes a node out of the voters and the state. A leader
// removing itself hands over leadership.
func (c *Cluster) handleRemove(w http.ResponseWriter, r *http.Request) {
	if c.notLeader(w) {
		return
	}
	name := r.PathValue("name")
	if !c.lockMembership(w, r) {
		return
	}
	defer c.unlockMembership()
	// A new leader may not have applied a node's addition yet, committed
	// under the leader before it: it applies what was committed first.
	if err := c.catchUp(); err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	// The voter is removed only from the configuration read here: one
	// that changed meanwhile, as under a leader between, is read again.
	configuration, index, err := c.configuration()
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_, voter := serverAddress(configuration, name)
	_, known := nodeNamed(c.config.Store.Nodes(), name)
	if !voter && !known {
		writeError(w, http.StatusNotFound, fmt.Errorf("node %s is not part of the cluster", name))
		return
	}
	// Nothing changes while changes are frozen, as by an upgrade: not the
	// state, not the voters, whose nodes the upgrade counts on.
	if frozen := c.config.Store.Frozen(); frozen != "" {
		writeError(w, http.StatusConflict, fmt.Errorf("%w: %s; remove the node once changes are taken again", store.ErrFrozen, frozen))
		return
	}
	// A leader does not take itself out: it hands leadership over, and the
	// next leader takes it out as any other node, asked again.
	if name == c.config.Node && voter {
		if err := waitFuture(c.raft.LeadershipTransfer(), commitTimeout+futureGrace); err != nil {
			writeError(w, http.StatusServiceUnavailable, fmt.Errorf("node %s leads the cluster and could not hand leadership over (%w): run the removal again", name, err))
			return
		}
		c.moved(w)
		return
	}
	// The node leaves the state first: the write checks, with the change,
	// everything that may refuse the removal (the freeze, tenants on the
	// node or moving to it), so nothing refuses once the voters changed. Its
	// identity key stays as departed, which Raft checks it by until it is
	// out of the voters too.
	if known {
		if err := c.config.Store.RemoveNode(name, r.URL.Query().Get("actor")); err != nil {
			switch {
			case errors.Is(err, ErrNotLeader) || errors.Is(err, ErrNoLeader):
				c.moved(w)
			case errors.Is(err, store.ErrInUse), errors.Is(err, store.ErrFrozen):
				writeError(w, http.StatusConflict, err)
			default:
				writeError(w, http.StatusServiceUnavailable, err)
			}
			return
		}
	}
	if voter {
		// A freeze taken meanwhile stops the removal here: the node is out
		// of the state, still a voter by its key; asked again once changes
		// are taken, the removal finishes.
		if frozen := c.config.Store.Frozen(); frozen != "" {
			writeError(w, http.StatusConflict, fmt.Errorf("%w: %s; node %s is out of the state but still a voter: once changes are taken again, run `murder cluster remove %s` again, on any node of the cluster or on %s itself", store.ErrFrozen, frozen, name, name, name))
			return
		}
		// The voter is removed only from the configuration read above.
		removeServer := func(index uint64) raft.IndexFuture {
			return c.raft.RemoveServer(raft.ServerID(name), index, commitTimeout)
		}
		if err := c.changeVoters(removeServer, index); err != nil {
			if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
				// Asking the next leader again is safe.
				c.moved(w)
				return
			}
			if errors.Is(err, errVotersChanged) {
				// The voters changed since they were read: asking again
				// reads them afresh.
				c.moved(w)
				return
			}
			writeError(w, http.StatusServiceUnavailable, fmt.Errorf("remove %s from the voters: %w; node %s is out of the state but still a voter: run `murder cluster remove %s` again, on any node of the cluster or on %s itself", name, err, name, name, name))
			return
		}
	}
	// Raft connections with it, made or taken, close now, not at the next
	// upkeep.
	c.listener.sweep(c.raftPeer)
	// Out of the voters, the node is taken by its key no more: its
	// departure goes, under the lock on changes of the members, this very
	// one. If this fails, the leader's upkeep drops it later.
	if departure, ok := c.config.Store.Departure(name); ok && !c.isVoter(name) {
		if err := c.config.Store.ForgetDeparted(departure, r.URL.Query().Get("actor")); err != nil {
			c.config.Log.Debug("could not drop the identity key of a node removed", "node", name, "error", err)
		}
	}
	writeJSON(w, http.StatusOK, struct{}{})
}

// maxBinary bounds a binary sent for an upgrade.
const maxBinary = 512 << 20

func (c *Cluster) handleNodeInfo(w http.ResponseWriter, _ *http.Request) {
	if c.config.Upgrade == nil {
		writeError(w, http.StatusNotImplemented, errors.New("this node does not take upgrades"))
		return
	}
	writeJSON(w, http.StatusOK, c.config.Upgrade.Info())
}

// handleInstall installs the binary another node sends; the agent then
// restarts into it.
func (c *Cluster) handleInstall(w http.ResponseWriter, r *http.Request) {
	if c.config.Upgrade == nil {
		writeError(w, http.StatusNotImplemented, errors.New("this node does not take upgrades"))
		return
	}
	if err := c.config.Upgrade.Install(r.Context(), http.MaxBytesReader(w, r.Body, maxBinary), r.Header.Get(shaHeader), r.Header.Get(freezeHeader)); err != nil {
		// A node that took no binary, as the upgrade no longer holds the
		// freeze (412) or another install restarts it (409), is no
		// refusal.
		status := http.StatusUnprocessableEntity
		switch {
		case errors.Is(err, upgrade.ErrNotOwner):
			status = http.StatusPreconditionFailed
		case errors.Is(err, upgrade.ErrRestarting):
			status = http.StatusConflict
		}
		writeError(w, status, err)
		return
	}
	writeJSON(w, http.StatusOK, struct{}{})
}

func (c *Cluster) moves(w http.ResponseWriter) bool {
	if c.config.Moves == nil {
		writeError(w, http.StatusNotImplemented, errors.New("this node takes part in no moves"))
		return false
	}
	return true
}

// handleUsage is this node's use now.
func (c *Cluster) handleUsage(w http.ResponseWriter, _ *http.Request) {
	if c.config.Usage == nil {
		writeError(w, http.StatusNotImplemented, errors.New("this node measures no use"))
		return
	}
	writeJSON(w, http.StatusOK, c.config.Usage.Report())
}

// handleTenantUsage is the day of one of this node's tenants.
func (c *Cluster) handleTenantUsage(w http.ResponseWriter, r *http.Request) {
	if c.config.Usage == nil {
		writeError(w, http.StatusNotImplemented, errors.New("this node measures no use"))
		return
	}
	seconds, err := strconv.Atoi(r.URL.Query().Get("step"))
	if err != nil || seconds < 60 || seconds > 3600 {
		writeError(w, http.StatusBadRequest, errors.New("step must be 60-3600 seconds"))
		return
	}
	series, ok := c.config.Usage.Series(r.PathValue("name"), time.Duration(seconds)*time.Second)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("node %s has not measured tenant %s", c.config.Node, r.PathValue("name")))
		return
	}
	writeJSON(w, http.StatusOK, series)
}

// handlePing answers the leader's probe: this node is up.
func (c *Cluster) handlePing(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, struct{}{})
}

// replicaSender is the node a replica request comes from, once this node
// keeps copies at all.
func (c *Cluster) replicaSender(w http.ResponseWriter, r *http.Request) (string, bool) {
	if c.config.Replicas == nil {
		writeError(w, http.StatusNotImplemented, errors.New("this node keeps no copies of tenants"))
		return "", false
	}
	sender, err := peerName(r.TLS)
	if err != nil {
		writeError(w, http.StatusForbidden, err)
		return "", false
	}
	return sender, true
}

// handleSnapshots lists the snapshots of a copy this node keeps.
func (c *Cluster) handleSnapshots(w http.ResponseWriter, r *http.Request) {
	sender, ok := c.replicaSender(w, r)
	if !ok {
		return
	}
	snapshots, err := c.config.Replicas.Snapshots(r.Context(), sender, r.PathValue("name"))
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	if snapshots == nil {
		snapshots = []string{}
	}
	writeJSON(w, http.StatusOK, struct {
		Snapshots []string `json:"snapshots"`
	}{snapshots})
}

// handleReplicate takes a tenant's data into the copy this node keeps.
func (c *Cluster) handleReplicate(w http.ResponseWriter, r *http.Request) {
	sender, ok := c.replicaSender(w, r)
	if !ok {
		return
	}
	if err := c.config.Replicas.Receive(r.Context(), sender, r.PathValue("name"), r.URL.Query().Get("whole") == "1", r.Body); err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, struct{}{})
}

// handleReceive takes the data of a tenant moving here.
func (c *Cluster) handleReceive(w http.ResponseWriter, r *http.Request) {
	if !c.moves(w) {
		return
	}
	if err := c.config.Moves.Receive(r.Context(), r.PathValue("name"), r.URL.Query().Get("incremental") == "1", r.Body); err != nil {
		writeError(w, http.StatusUnprocessableEntity, err)
		return
	}
	writeJSON(w, http.StatusOK, struct{}{})
}

func (c *Cluster) handleDiscard(w http.ResponseWriter, r *http.Request) {
	if !c.moves(w) {
		return
	}
	if err := c.config.Moves.Discard(r.Context(), r.PathValue("name")); err != nil {
		writeError(w, http.StatusUnprocessableEntity, err)
		return
	}
	writeJSON(w, http.StatusOK, struct{}{})
}

func (c *Cluster) handleCreate(w http.ResponseWriter, r *http.Request) {
	if !c.moves(w) {
		return
	}
	var request createRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&request); err != nil || request.Tenant.Name != r.PathValue("name") {
		writeError(w, http.StatusBadRequest, fmt.Errorf("a tenant to create named %s", r.PathValue("name")))
		return
	}
	record, err := c.config.Moves.Create(r.Context(), request.Tenant, request.Actor)
	if err != nil {
		status := http.StatusInternalServerError
		var invalid *spec.ValidationError
		switch {
		case errors.Is(err, store.ErrNotFound):
			status = http.StatusNotFound
		case errors.Is(err, store.ErrExists), errors.Is(err, store.ErrArchived), errors.Is(err, store.ErrConflict), errors.Is(err, store.ErrFrozen), errors.Is(err, store.ErrStale):
			status = http.StatusConflict
		case errors.Is(err, reconcile.ErrStopping), errors.Is(err, ErrNoLeader), errors.Is(err, ErrNotLeader):
			status = http.StatusServiceUnavailable
		case errors.Is(err, context.Canceled):
			status = 499
		case errors.As(err, &invalid), errors.Is(err, store.ErrInvalid):
			status = http.StatusBadRequest
		}
		writeError(w, status, err)
		return
	}
	writeJSON(w, http.StatusOK, record)
}

func (c *Cluster) handleTenantStatus(w http.ResponseWriter, r *http.Request) {
	if !c.moves(w) {
		return
	}
	status, ok := c.config.Moves.Status(r.PathValue("name"))
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("node %s has no status of tenant %s", c.config.Node, r.PathValue("name")))
		return
	}
	writeJSON(w, http.StatusOK, status)
}

// handleMove moves one of this node's tenants, for a node replacement run
// from another node.
func (c *Cluster) handleMove(w http.ResponseWriter, r *http.Request) {
	if !c.moves(w) {
		return
	}
	var request moveRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := c.config.Moves.Move(r.Context(), r.PathValue("name"), request.To, request.Actor); err != nil {
		writeError(w, http.StatusUnprocessableEntity, err)
		return
	}
	writeJSON(w, http.StatusOK, struct{}{})
}

func nodeNamed(nodes []store.Node, name string) (store.Node, bool) {
	for _, node := range nodes {
		if node.Name == name {
			return node, true
		}
	}
	return store.Node{}, false
}
