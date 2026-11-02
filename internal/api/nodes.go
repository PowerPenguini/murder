package api

import (
	"context"
	"net/http"

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

// Nodes returns the cluster's nodes and whether each answers.
func (c *Client) Nodes(ctx context.Context) (Nodes, error) {
	var view Nodes
	err := c.do(ctx, http.MethodGet, "/v1/nodes", nil, &view)
	return view, err
}
