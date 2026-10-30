package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/hashicorp/raft"

	"github.com/PowerPenguini/murder/internal/store"
)

// handler serves other nodes. They are all trusted alike: any of them holds
// the master key.
func (c *Cluster) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /cluster/v1/commit", c.handleCommit)
	mux.HandleFunc("GET /cluster/v1/index", c.handleIndex)
	mux.HandleFunc("GET /cluster/v1/snapshot", c.handleSnapshot)
	mux.HandleFunc("POST /cluster/v1/nodes", c.handleJoin)
	mux.HandleFunc("DELETE /cluster/v1/nodes/{name}", c.handleRemove)
	mux.HandleFunc("GET /cluster/v1/node", c.handleNodeInfo)
	mux.HandleFunc("PUT /cluster/v1/node/binary", c.handleInstall)
	return mux
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
			status := http.StatusServiceUnavailable
			if errors.Is(err, store.ErrInUse) {
				status = http.StatusConflict
			}
			writeError(w, status, err)
			return
		}
	}
	if voter {
		if err := c.raft.RemoveServer(raft.ServerID(name), 0, commitTimeout).Error(); err != nil {
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
	if err := c.config.Upgrade.Install(r.Context(), http.MaxBytesReader(w, r.Body, maxBinary), r.Header.Get(shaHeader)); err != nil {
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
