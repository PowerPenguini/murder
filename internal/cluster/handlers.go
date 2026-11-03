package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/hashicorp/raft"

	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/upgrade"
)

// handler serves other nodes. They are all trusted alike: any of them holds
// the master key. Only members of this cluster are served, so neither a
// node removed from it nor one of another cluster with the same master key
// changes anything here; a node joins, and fetches the state it joins
// with, before it is a member.
func (c *Cluster) handler() http.Handler {
	mux := http.NewServeMux()
	members := func(pattern string, handle http.HandlerFunc) { mux.HandleFunc(pattern, c.membersOnly(handle)) }
	members("POST /cluster/v1/commit", c.handleCommit)
	members("GET /cluster/v1/index", c.handleIndex)
	mux.HandleFunc("GET /cluster/v1/snapshot", c.handleSnapshot)
	mux.HandleFunc("POST /cluster/v1/nodes", c.handleJoin)
	members("DELETE /cluster/v1/nodes/{name}", c.handleRemove)
	members("GET /cluster/v1/node", c.handleNodeInfo)
	members("PUT /cluster/v1/tenants/{name}/data", c.handleReceive)
	members("DELETE /cluster/v1/tenants/{name}/data", c.handleDiscard)
	members("POST /cluster/v1/tenants/{name}", c.handleCreate)
	members("GET /cluster/v1/tenants/{name}/status", c.handleTenantStatus)
	members("POST /cluster/v1/tenants/{name}/move", c.handleMove)
	members("PUT /cluster/v1/node/binary", c.handleInstall)
	members("GET /cluster/v1/ping", c.handlePing)
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
}

func (t namedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	request = request.Clone(request.Context())
	request.Header.Set(clusterHeader, t.cluster())
	return t.next.RoundTrip(request)
}

// membersOnly serves nodes of this cluster that are its members.
func (c *Cluster) membersOnly(handle http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ours := c.config.Store.Cluster()
		if theirs := r.Header.Get(clusterHeader); ours == "" || theirs != ours {
			writeError(w, http.StatusForbidden, fmt.Errorf("node %s belongs to cluster %q, not %q", c.config.Node, ours, theirs))
			return
		}
		name, err := peerName(r.TLS)
		if err != nil {
			writeError(w, http.StatusForbidden, err)
			return
		}
		// A node being removed is out of the state before it is out of
		// the voters: it may still finish its own removal.
		if _, member := nodeNamed(c.config.Store.Nodes(), name); !member && !c.isVoter(name) {
			writeError(w, http.StatusForbidden, fmt.Errorf("node %s is not a member of cluster %s", name, ours))
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

// handleSnapshot sends this node's state to a node about to join.
func (c *Cluster) handleSnapshot(w http.ResponseWriter, _ *http.Request) {
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
	future := c.raft.GetConfiguration()
	if future.Error() == nil {
		for _, server := range future.Configuration().Servers {
			if server.ID == raft.ServerID(name) {
				return string(server.Address), true
			}
		}
	}
	return "", false
}

func (c *Cluster) isVoter(name string) bool {
	future := c.raft.GetConfiguration()
	if future.Error() != nil {
		return false
	}
	for _, server := range future.Configuration().Servers {
		if server.ID == raft.ServerID(name) {
			return true
		}
	}
	return false
}

// handleJoin adds a node: first to the state, so its role is known before
// it votes, then to the voters.
func (c *Cluster) handleJoin(w http.ResponseWriter, r *http.Request) {
	if c.notLeader(w) {
		return
	}
	name, err := peerName(r.TLS)
	if err != nil {
		writeError(w, http.StatusForbidden, err)
		return
	}
	var request joinRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
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
	if request.Check {
		writeJSON(w, http.StatusOK, joinAnswer{Cluster: c.config.Store.Cluster()})
		return
	}
	actor := request.Actor
	if actor == "" {
		actor = "node " + name
	}
	if err := c.config.Store.PutNode(store.Node{Name: name, Address: request.Address}, actor); err != nil {
		switch {
		case errors.Is(err, ErrNotLeader) || errors.Is(err, ErrNoLeader):
			c.moved(w)
		case errors.Is(err, store.ErrConflict):
			writeError(w, http.StatusConflict, err)
		default:
			writeError(w, http.StatusUnprocessableEntity, err)
		}
		return
	}
	if err := c.raft.AddVoter(raft.ServerID(name), raft.ServerAddress(request.Address), 0, commitTimeout).Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			// The node may be a voter now, or not: asking again is safe.
			c.moved(w)
			return
		}
		writeError(w, http.StatusServiceUnavailable, fmt.Errorf("add %s to the voters: %w", name, err))
		return
	}
	writeJSON(w, http.StatusOK, struct{}{})
}

// handleRemove takes a node out of the voters and the state. A leader
// removing itself hands over leadership.
func (c *Cluster) handleRemove(w http.ResponseWriter, r *http.Request) {
	if c.notLeader(w) {
		return
	}
	name := r.PathValue("name")
	future := c.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	voter := false
	for _, server := range future.Configuration().Servers {
		voter = voter || server.ID == raft.ServerID(name)
	}
	_, known := nodeNamed(c.config.Store.Nodes(), name)
	if !voter && !known {
		writeError(w, http.StatusNotFound, fmt.Errorf("node %s is not part of the cluster", name))
		return
	}
	if known {
		if err := c.config.Store.RemoveNode(name, r.URL.Query().Get("actor")); err != nil {
			switch {
			case errors.Is(err, ErrNotLeader) || errors.Is(err, ErrNoLeader):
				c.moved(w)
			case errors.Is(err, store.ErrInUse):
				writeError(w, http.StatusConflict, err)
			default:
				writeError(w, http.StatusServiceUnavailable, err)
			}
			return
		}
	}
	if voter {
		if err := c.raft.RemoveServer(raft.ServerID(name), 0, commitTimeout).Error(); err != nil {
			if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
				// Asking the next leader again is safe.
				c.moved(w)
				return
			}
			writeError(w, http.StatusServiceUnavailable, fmt.Errorf("remove %s from the voters: %w", name, err))
			return
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
