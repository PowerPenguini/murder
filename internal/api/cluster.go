package api

import (
	"context"
	"net/http"

	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/store"
)

// JoinRequest asks the node to join the cluster of the node at Peer.
type JoinRequest struct {
	Peer string `json:"peer"`
}

// FailOver runs a powered-off node's tenants from their replicas.
func (c *Client) FailOver(ctx context.Context, node string) (store.FailedOver, error) {
	var outcome store.FailedOver
	err := c.do(ctx, http.MethodPost, "/v1/cluster/nodes/"+node+"/failover", nil, &outcome)
	return outcome, err
}

// ClusterStatus returns the node's view of its cluster.
func (c *Client) ClusterStatus(ctx context.Context) (cluster.Status, error) {
	var status cluster.Status
	err := c.do(ctx, http.MethodGet, "/v1/cluster", nil, &status)
	return status, err
}

// Bootstrap starts a cluster of this node alone.
func (c *Client) Bootstrap(ctx context.Context) (cluster.Status, error) {
	var status cluster.Status
	err := c.do(ctx, http.MethodPost, "/v1/cluster/bootstrap", nil, &status)
	return status, err
}

// Join adds the node to the cluster of the node at peer.
func (c *Client) Join(ctx context.Context, peer string) (cluster.Status, error) {
	var status cluster.Status
	err := c.do(ctx, http.MethodPost, "/v1/cluster/join", JoinRequest{Peer: peer}, &status)
	return status, err
}

// RemoveNode takes a node out of the cluster.
func (c *Client) RemoveNode(ctx context.Context, node string) error {
	return c.do(ctx, http.MethodDelete, "/v1/cluster/nodes/"+node, nil, nil)
}
