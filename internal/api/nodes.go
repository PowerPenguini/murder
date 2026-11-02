package api

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/store"
)

// Nodes is the cluster as this node sees it, for the panel.
type Nodes struct {
	// Cluster names the cluster; empty on a node in none.
	Cluster string `json:"cluster,omitempty"`
	Self    string `json:"self"`
	Leader  string `json:"leader,omitempty"`
	// Frozen says why changes are refused, while an upgrade runs.
	Frozen string      `json:"frozen,omitempty"`
	Nodes  []NodeState `json:"nodes"`
}

// NodeState is one node and whether it answers this node now.
type NodeState struct {
	store.Node
	Answers bool `json:"answers"`
}

// nodePingTimeout bounds how long the panel's view waits for a node.
const nodePingTimeout = 2 * time.Second

func (s *Server) nodes(w http.ResponseWriter, r *http.Request) {
	view := Nodes{Cluster: s.store.Cluster(), Self: s.store.Node(), Frozen: s.store.Frozen()}
	members := s.store.Nodes()
	if len(members) == 0 {
		// A node in no cluster yet: itself alone.
		members = []store.Node{{Name: view.Self}}
	}
	if s.cluster != nil {
		if status, err := s.cluster.Status(); err == nil {
			view.Leader = status.Leader
		}
	}
	view.Nodes = make([]NodeState, len(members))
	var wg sync.WaitGroup
	for i, node := range members {
		view.Nodes[i] = NodeState{Node: node, Answers: node.Name == view.Self}
		if node.Name == view.Self || s.cluster == nil {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(r.Context(), nodePingTimeout)
			defer cancel()
			view.Nodes[i].Answers = s.cluster.Ping(ctx, node.Name) == nil
		}()
	}
	wg.Wait()
	writeJSON(w, http.StatusOK, view)
}

// Nodes returns the cluster's nodes and whether each answers.
func (c *Client) Nodes(ctx context.Context) (Nodes, error) {
	var view Nodes
	err := c.do(ctx, http.MethodGet, "/v1/nodes", nil, &view)
	return view, err
}
