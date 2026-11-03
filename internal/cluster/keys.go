package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"

	"github.com/PowerPenguini/murder/internal/secrets"
)

// Keys is a node's master key ring, as a rotation run from any node sees
// and changes it. A key is sent to a node, never fetched from one: a node
// that holds a leaked key gets nothing new by asking.
type Keys interface {
	Ring() []secrets.Entry
	// Add adds a key as the next one: trusted, not used yet.
	Add(raw []byte) error
	// Use makes the key id, which the ring has, the one in use.
	Use(id string) error
	// Drop takes the key id, not the one in use, out of the ring.
	Drop(id string) error
}

type keyRequest struct {
	Key []byte `json:"key"`
}

type ringAnswer struct {
	Keys []secrets.Entry `json:"keys"`
}

// ErrNoKeys is a node that does not take part in rotations: of an older
// release, or without a ring.
var ErrNoKeys = errors.New("the node does not rotate master keys")

func (c *Cluster) keys(w http.ResponseWriter) (Keys, bool) {
	if c.config.Keys == nil {
		writeError(w, http.StatusNotImplemented, ErrNoKeys)
		return nil, false
	}
	return c.config.Keys, true
}

func (c *Cluster) handleKeyRing(w http.ResponseWriter, _ *http.Request) {
	if keys, ok := c.keys(w); ok {
		writeJSON(w, http.StatusOK, ringAnswer{Keys: keys.Ring()})
	}
}

func (c *Cluster) handleKeyAdd(w http.ResponseWriter, r *http.Request) {
	keys, ok := c.keys(w)
	if !ok {
		return
	}
	var request keyRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := keys.Add(request.Key); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, ringAnswer{Keys: keys.Ring()})
}

func (c *Cluster) handleKeyUse(w http.ResponseWriter, r *http.Request) {
	if keys, ok := c.keys(w); ok {
		if err := keys.Use(r.PathValue("id")); err != nil {
			status := http.StatusInternalServerError
			if secrets.IsNoKey(err) {
				status = http.StatusConflict
			}
			writeError(w, status, err)
			return
		}
		writeJSON(w, http.StatusOK, ringAnswer{Keys: keys.Ring()})
	}
}

func (c *Cluster) handleKeyDrop(w http.ResponseWriter, r *http.Request) {
	if keys, ok := c.keys(w); ok {
		if err := keys.Drop(r.PathValue("id")); err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusOK, ringAnswer{Keys: keys.Ring()})
	}
}

// KeyRingOn is node's master key ring.
func (c *Cluster) KeyRingOn(ctx context.Context, node string) ([]secrets.Entry, error) {
	var answer ringAnswer
	err := c.request(ctx, node, http.MethodGet, "/cluster/v1/keys", nil, &answer)
	return answer.Keys, keysError(err)
}

// AddKeyOn adds raw to node's ring as the next key.
func (c *Cluster) AddKeyOn(ctx context.Context, node string, raw []byte) ([]secrets.Entry, error) {
	var answer ringAnswer
	err := c.request(ctx, node, http.MethodPost, "/cluster/v1/keys", bytes.NewReader(marshal(keyRequest{Key: raw})), &answer)
	return answer.Keys, keysError(err)
}

// UseKeyOn makes id the key node uses.
func (c *Cluster) UseKeyOn(ctx context.Context, node, id string) ([]secrets.Entry, error) {
	var answer ringAnswer
	err := c.request(ctx, node, http.MethodPost, "/cluster/v1/keys/"+url.PathEscape(id)+"/use", nil, &answer)
	return answer.Keys, keysError(err)
}

// DropKeyOn takes id out of node's ring.
func (c *Cluster) DropKeyOn(ctx context.Context, node, id string) ([]secrets.Entry, error) {
	var answer ringAnswer
	err := c.request(ctx, node, http.MethodDelete, "/cluster/v1/keys/"+url.PathEscape(id), nil, &answer)
	return answer.Keys, keysError(err)
}

// keysError says what a node of an older release, which does not know
// the request, means.
func keysError(err error) error {
	var peer *PeerError
	if errors.As(err, &peer) && (peer.Status == http.StatusNotFound || peer.Status == http.StatusNotImplemented) {
		return errors.Join(ErrNoKeys, err)
	}
	return err
}
