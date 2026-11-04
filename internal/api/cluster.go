package api

import (
	"context"
	"net/http"
	"time"

	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/store"
)

// JoinRequest asks the node to join a cluster: with Invitation, which
// admits the node; or, once an invitation admitted it (as setup -join
// does), at the node at Peer, trusting the members' identity keys the
// invitation named, Members.
type JoinRequest struct {
	Invitation string            `json:"invitation,omitempty"`
	Peer       string            `json:"peer,omitempty"`
	Members    map[string]string `json:"members,omitempty"`
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

// BootstrapRequest asks for a bootstrap; Restored rebuilds a cluster from
// the state a backup restored.
type BootstrapRequest struct {
	Restored bool `json:"restored,omitempty"`
}

// BootstrapRestored rebuilds a cluster of this node alone from the state a
// backup restored, under a new name.
func (c *Client) BootstrapRestored(ctx context.Context) (cluster.Status, error) {
	var status cluster.Status
	err := c.do(ctx, http.MethodPost, "/v1/cluster/bootstrap", BootstrapRequest{Restored: true}, &status)
	return status, err
}

// NodeKeyRequest gives a node a new identity key.
type NodeKeyRequest struct {
	Key string `json:"key"`
}

// ReplaceNodeKey gives node the identity key key (base64), in place of the
// one it lost.
func (c *Client) ReplaceNodeKey(ctx context.Context, node, key string) error {
	return c.do(ctx, http.MethodPut, "/v1/cluster/nodes/"+node+"/key", NodeKeyRequest{Key: key}, nil)
}

// NodeInviteRequest asks for an invitation valid for ValidSeconds (an hour if
// zero).
type NodeInviteRequest struct {
	ValidSeconds int64 `json:"validSeconds,omitempty"`
}

// NodeInvitation is a one-time invitation for a host to join as a node.
type NodeInvitation struct {
	Invitation string    `json:"invitation"`
	Expires    time.Time `json:"expires"`
}

// Invite makes an invitation for a host to join the cluster as a node.
func (c *Client) Invite(ctx context.Context, valid time.Duration) (NodeInvitation, error) {
	var invitation NodeInvitation
	err := c.do(ctx, http.MethodPost, "/v1/cluster/invites", NodeInviteRequest{ValidSeconds: int64(valid / time.Second)}, &invitation)
	return invitation, err
}

// Join adds the node to the cluster of the node at peer, once an
// invitation admitted it; members are the identity keys the invitation
// named.
func (c *Client) Join(ctx context.Context, peer string, members map[string]string) (cluster.Status, error) {
	var status cluster.Status
	err := c.do(ctx, http.MethodPost, "/v1/cluster/join", JoinRequest{Peer: peer, Members: members}, &status)
	return status, err
}

// JoinInvited adds the node to the cluster an invitation names.
func (c *Client) JoinInvited(ctx context.Context, invitation string) (cluster.Status, error) {
	var status cluster.Status
	err := c.do(ctx, http.MethodPost, "/v1/cluster/join", JoinRequest{Invitation: invitation}, &status)
	return status, err
}

// RemoveNode takes a node out of the cluster.
func (c *Client) RemoveNode(ctx context.Context, node string) error {
	return c.do(ctx, http.MethodDelete, "/v1/cluster/nodes/"+node, nil, nil)
}
