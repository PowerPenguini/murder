package apiserver

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
)

// nodePingTimeout bounds how long the panel's view waits for a node.
const nodePingTimeout = 2 * time.Second

func (s *Server) nodes(w http.ResponseWriter, r *http.Request) {
	view := api.Nodes{Cluster: s.store.Cluster(), Self: s.store.Node(), Frozen: s.store.Frozen()}
	if lease := s.store.Lease(); lease.Held(time.Now()) {
		view.Lease = &lease
	}
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
	view.Nodes = make([]api.NodeState, len(members))
	var wg sync.WaitGroup
	for i, node := range members {
		view.Nodes[i] = api.NodeState{Node: node, Answers: node.Name == view.Self}
		// A failed node shows as failed whether it answers or not; it is
		// marked back once it answers the leader.
		if node.Name == view.Self || s.cluster == nil || node.Failed() {
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
