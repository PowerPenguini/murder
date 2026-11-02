package cluster

import (
	"context"
	"errors"
	"maps"
	"net/http"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/store"
)

// digestTimeout bounds asking one node for its digest.
const digestTimeout = 10 * time.Second

// handleDigest answers this node's digest of the replicated state.
func (c *Cluster) handleDigest(w http.ResponseWriter, _ *http.Request) {
	digest, err := c.config.Store.Digest()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, digest)
}

// Digests is what the leader last found comparing the nodes' states.
type Digests struct {
	mu sync.Mutex
	// diverged names each node compared, and whether its state differs from
	// the leader's.
	diverged map[string]bool
	checked  time.Time
}

// Diverged returns, while this node leads the cluster, each node last
// compared with it and whether that node's state differs; a node not
// compared, as one of another release or one that did not answer, is not
// named.
func (d *Digests) Diverged() map[string]bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return maps.Clone(d.diverged)
}

// WatchDigests has the leader compare every other node's state with its
// own every interval, until ctx ends, and records what it found in d. A
// state that differs is an error worth an operator's attention: the nodes
// apply the same batches, so their states drifted apart.
func (c *Cluster) WatchDigests(ctx context.Context, interval time.Duration, d *Digests) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
		found := c.CompareDigests(ctx)
		d.mu.Lock()
		d.diverged, d.checked = found, time.Now()
		d.mu.Unlock()
	}
}

// CompareDigests compares every other node's digest with this node's, if
// this node leads the cluster: nil otherwise. A node is named only if its
// digest was of the same version and schema as the leader's.
func (c *Cluster) CompareDigests(ctx context.Context) map[string]bool {
	if !c.Leader() {
		return nil
	}
	own, err := c.config.Store.Digest()
	if err != nil {
		c.config.Log.Warn("hash the replicated state", "error", err)
		return nil
	}
	found := map[string]bool{}
	for _, node := range c.config.Store.Nodes() {
		if node.Name == c.config.Node {
			continue
		}
		var theirs store.Digest
		ask, cancel := context.WithTimeout(ctx, digestTimeout)
		err := c.request(ask, node.Name, http.MethodGet, "/cluster/v1/digest", nil, &theirs)
		cancel()
		var peer *PeerError
		switch {
		case errors.As(err, &peer) && peer.Status == http.StatusNotFound:
			continue // a release that does not hash its state
		case err != nil:
			continue // asked again next time
		case !own.Comparable(theirs):
			continue // a write in between, or another schema
		}
		found[node.Name] = theirs.Hash != own.Hash
		if found[node.Name] {
			c.config.Log.Error("a node's replicated state differs from the leader's: compare the two databases, and replace that node's state (remove it from the cluster and join it again)",
				"node", node.Name, "version", own.Version, "leader", own.Hash, "theirs", theirs.Hash)
		}
	}
	return found
}
