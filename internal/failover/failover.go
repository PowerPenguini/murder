// Package failover runs a dead node's tenants from their replicas. The
// cluster's leader probes every other node; one that has not answered for
// a while is powered off through the provider's API, and only once the
// provider confirms it is off, and long enough after it last answered,
// does the leader give its tenants to their replicas' nodes. Without that
// confirmation nothing is promoted: a node that may still run its tenants
// never has them run twice.
package failover

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/metrics"
	"github.com/PowerPenguini/murder/internal/store"
)

// Fencer powers nodes off through the provider's API.
type Fencer interface {
	// PowerOff powers node off and returns once the provider confirms it
	// is off; an error means it is not known to be off.
	PowerOff(ctx context.Context, node string) error
}

// Monitor watches the other nodes from the cluster's leader.
type Monitor struct {
	Store store.Store
	// Probe reports whether a node answers.
	Probe func(ctx context.Context, node string) error
	// Leader reports whether this node leads the cluster.
	Leader func() bool
	// Fence powers dead nodes off; without it a dead node's tenants fail
	// over only when an operator says it is off.
	Fence Fencer
	Log   *slog.Logger
	// After is how long a node goes unanswered before it counts as dead;
	// 30 s if zero.
	After time.Duration
	// Settle is how long after a node last answered its tenants may run
	// elsewhere at the earliest; 60 s if zero.
	Settle time.Duration
	// Interval is how often nodes are probed; 2 s if zero.
	Interval time.Duration
	Now      func() time.Time
	// Wait waits d, or until ctx ends; a real wait if nil.
	Wait func(ctx context.Context, d time.Duration) error

	mu sync.Mutex
	// seen is when each node last answered, as this leader saw it.
	seen map[string]time.Time
	// fencing is the node being powered off and failed over; one at a
	// time. blocked says why a node's failover waits.
	fencing string
	blocked map[string]string
	// retry is when a node whose failover failed is tried again.
	retry   map[string]time.Time
	leading bool
}

// retryAfter is how long a failover that failed waits before it is tried
// again.
const retryAfter = time.Minute

func (m *Monitor) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now()
}

func (m *Monitor) after() time.Duration  { return or(m.After, 30*time.Second) }
func (m *Monitor) settle() time.Duration { return or(m.Settle, 60*time.Second) }

func or(d, otherwise time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return otherwise
}

// Run watches until ctx ends.
func (m *Monitor) Run(ctx context.Context) {
	interval := or(m.Interval, 2*time.Second)
	for {
		m.Check(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

// Check probes every other node once and starts the failover of one that
// has been dead long enough. Only the leader acts; a new leader counts
// from its first probe, as it knows nothing older.
func (m *Monitor) Check(ctx context.Context) {
	if !m.Leader() {
		m.mu.Lock()
		m.seen, m.leading = nil, false
		m.mu.Unlock()
		return
	}
	now := m.now()
	self := m.Store.Node()
	var nodes []store.Node
	for _, node := range m.Store.Nodes() {
		if node.Name != self {
			nodes = append(nodes, node)
		}
	}
	answered := make([]bool, len(nodes))
	var wg sync.WaitGroup
	for i, node := range nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			probe, cancel := context.WithTimeout(ctx, or(m.Interval, 2*time.Second))
			defer cancel()
			answered[i] = m.Probe(probe, node.Name) == nil
		}()
	}
	wg.Wait()
	if ctx.Err() != nil {
		return
	}
	m.mu.Lock()
	if m.seen == nil {
		m.seen = map[string]time.Time{}
	}
	if !m.leading {
		m.leading = true
		m.Log.Info("leading the cluster: watching the other nodes")
	}
	var dead []store.Node
	for i, node := range nodes {
		if answered[i] {
			m.seen[node.Name] = now
			delete(m.blocked, node.Name)
			delete(m.retry, node.Name)
			if node.Failed() {
				// After the unlock: a write waits for the log.
				defer m.back(node.Name)
			}
			continue
		}
		if _, known := m.seen[node.Name]; !known {
			m.seen[node.Name] = now // unanswered since this leader began
		}
		if !node.Failed() && now.Sub(m.seen[node.Name]) >= m.after() && !now.Before(m.retry[node.Name]) {
			dead = append(dead, node)
		}
	}
	start := ""
	if m.fencing == "" && len(dead) > 0 {
		if frozen := m.Store.Frozen(); frozen != "" {
			m.note(dead[0].Name, "changes are frozen ("+frozen+"): nodes restart on purpose during an upgrade")
		} else {
			start = dead[0].Name
			m.fencing = start
		}
	}
	lastSeen := m.seen[start]
	m.mu.Unlock()
	if start != "" {
		go m.failOver(context.WithoutCancel(ctx), start, lastSeen)
	}
}

func (m *Monitor) back(node string) {
	if err := m.Store.NodeBack(node, "murder (failover)"); err != nil {
		m.Log.Warn("mark a failed node back", "node", node, "error", err)
		return
	}
	m.Log.Info("a failed node answers again; it keeps copies again", "node", node)
}

// block records why a node's failover failed, and tries it again a minute
// later.
func (m *Monitor) block(node, why string) {
	if m.retry == nil {
		m.retry = map[string]time.Time{}
	}
	m.retry[node] = m.now().Add(retryAfter)
	m.note(node, why)
}

// note records why a node's failover waits, logged once per reason.
func (m *Monitor) note(node, why string) {
	if m.blocked == nil {
		m.blocked = map[string]string{}
	}
	if m.blocked[node] != why {
		m.Log.Error("a node does not answer, and its tenants cannot fail over yet", "node", node, "why", why)
	}
	m.blocked[node] = why
}

// failOver powers node off and, once it is confirmed off and settle has
// passed since it last answered, gives its tenants to their replicas.
func (m *Monitor) failOver(ctx context.Context, node string, lastSeen time.Time) {
	defer func() {
		m.mu.Lock()
		m.fencing = ""
		m.mu.Unlock()
	}()
	log := m.Log.With("node", node)
	if m.Fence == nil {
		m.mu.Lock()
		m.block(node, "no provider to power it off is set up (cluster.failover); once it is off, run: murder cluster failover "+node+" -powered-off")
		m.mu.Unlock()
		return
	}
	log.Warn("a node does not answer: powering it off", "since", lastSeen)
	fenceCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	err := m.Fence.PowerOff(fenceCtx, node)
	cancel()
	if err != nil {
		m.mu.Lock()
		m.block(node, "its power-off is not confirmed: "+err.Error()+"; once it is off, run: murder cluster failover "+node+" -powered-off")
		m.mu.Unlock()
		return
	}
	log.Warn("the node is powered off")
	if wait := lastSeen.Add(m.settle()).Sub(m.now()); wait > 0 {
		if err := m.wait(ctx, wait); err != nil {
			return
		}
	}
	if !m.Leader() {
		log.Warn("no longer leading: the new leader fails the node over")
		return
	}
	outcome, err := m.Store.FailOver(node, "murder (failover)")
	if err != nil {
		m.mu.Lock()
		m.block(node, "the failover was not written: "+err.Error())
		m.mu.Unlock()
		return
	}
	m.mu.Lock()
	delete(m.blocked, node)
	m.mu.Unlock()
	log.Warn("the node's tenants run from their replicas", "promoted", outcome.Promoted, "stranded", outcome.Stranded)
}

func (m *Monitor) wait(ctx context.Context, d time.Duration) error {
	if m.Wait != nil {
		return m.Wait(ctx, d)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// ManualFailOver fails a node over that an operator states is powered off;
// it is refused while the node answers.
func ManualFailOver(ctx context.Context, st store.Store, probe func(context.Context, string) error, node, actor string) (store.FailedOver, error) {
	check, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := probe(check, node); err == nil {
		return store.FailedOver{}, fmt.Errorf("%w: node %s answers: it is not off, and its tenants stay", store.ErrConflict, node)
	}
	return st.FailOver(node, actor)
}

// WriteMetrics writes, from the leader, how long each node has gone
// unanswered and whether its failover is blocked.
func (m *Monitor) WriteMetrics(out *metrics.Writer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.leading {
		return
	}
	now := m.now()
	out.Family("murder_node_unanswered_seconds", "gauge", "How long the node has not answered the leader's probes; 0 if it answers.")
	for _, name := range slices.Sorted(maps.Keys(m.seen)) {
		out.Sample("murder_node_unanswered_seconds", now.Sub(m.seen[name]).Seconds(), "node", name)
	}
	out.Family("murder_failover_blocked", "gauge", "1 if a node does not answer and its tenants cannot fail over (see the agent's log).")
	for _, name := range slices.Sorted(maps.Keys(m.seen)) {
		_, blocked := m.blocked[name]
		out.Sample("murder_failover_blocked", metrics.Bool(blocked), "node", name)
	}
}
