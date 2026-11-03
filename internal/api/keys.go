package api

import (
	"context"
	"net/http"

	"github.com/PowerPenguini/murder/internal/rotation"
	"github.com/PowerPenguini/murder/internal/secrets"
)

// NodeKeys is a node's master key ring, without the keys.
type NodeKeys struct {
	Node  string          `json:"node"`
	Keys  []secrets.Entry `json:"keys,omitempty"`
	Error string          `json:"error,omitempty"`
}

// KeyRings are the master key rings of every node.
func (c *Client) KeyRings(ctx context.Context) ([]NodeKeys, error) {
	var rings []NodeKeys
	err := c.do(ctx, http.MethodGet, "/v1/keys", nil, &rings)
	return rings, err
}

// RotateKey rotates the master key of every node, or finishes a rotation
// stopped half way.
func (c *Client) RotateKey(ctx context.Context) (rotation.Result, error) {
	var result rotation.Result
	err := c.do(ctx, http.MethodPost, "/v1/keys/rotate", nil, &result)
	return result, err
}
