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
	"github.com/PowerPenguini/murder/internal/records"
	"github.com/PowerPenguini/murder/internal/store"
)

// Fencer powers nodes off through the provider's API.
type Fencer interface {
	// PowerOff powers node off and returns once the provider confirms it
	// is off; an error means it is not known to be off.
	PowerOff(ctx context.Context, node store.Node) error
	// Off reports whether the provider has node off now.
	Off(ctx context.Context, node store.Node) (bool, error)
}

// Monitor watches the other nodes from the cluster's leader.
type Monitor struct {
	Store State
	// Probe reports whether a node answers.
	Probe func(ctx context.Context, node string) error
	// Leader reports whether this node leads the cluster.
	Leader func() bool
	// Term is the leader's Raft term; nil if not known. A failover acts
	// only in the term it began in: a leader that lost and won leadership
	// again meanwhile decides again.
	Term func() uint64
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
	// Records, if set, keeps the base domain's DNS records naming the
	// public addresses of the nodes that serve; PanelRecords, the panel's
	// name leading to their addresses in the panel's tunnel.
	Records      records.Publisher
	PanelRecords records.Publisher
	Now          func() time.Time
	// Wait waits d, or until ctx ends; a real wait if nil.
	Wait func(ctx context.Context, d time.Duration) error

	mu sync.Mutex
	// seen is when each node last answered, as this leader saw it.
	seen map[string]time.Time
	// fencing is the node being powered off and failed over; one at a
	// time. blocked says why a node's failover waits.
	fencing string
	blocked map[string]string
	// stuck names the tenants each blocked failover would have promoted.
	stuck map[string][]string
	// waits says why a silent node is left alone on purpose.
	waits map[string]string
	// retry is when a node whose failover failed is tried again.
	retry   map[string]time.Time
	leading bool
	// stopWorker ends the failover under way, once leadership is lost.
	stopWorker context.CancelFunc
	records    recordsView
	// writing is set while the records are being written, across
	// leaderships: one write at a time.
	writing bool
}

// retryAfter is how long a failover that failed waits before it is tried
// again.
const retryAfter = time.Minute

// copiesAfter is how long a node goes unanswered before the copies it keeps
// are given to other nodes, when it is not failed over sooner: long enough
// that a pause of a node does not cost every tenant whose copy it keeps a
// whole new one.
const copiesAfter = 5 * time.Minute

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
//
// The leader acts only while it hears a majority of the nodes, itself
// included: one that hears too few may be the node cut off, and would
// otherwise power the others off one after the other.
func (m *Monitor) Check(ctx context.Context) {
	if !m.Leader() {
		m.mu.Lock()
		m.seen, m.leading = nil, false
		if m.stopWorker != nil {
			m.stopWorker()
			m.stopWorker = nil
		}
		m.mu.Unlock()
		return
	}
	now := m.now()
	self := m.Store.Node()
	all := m.Store.Nodes()
	var nodes []store.Node
	for _, node := range all {
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
		m.startRecords(now, all)
	}
	// Nodes no longer in the cluster leave the metrics.
	for name := range m.seen {
		if !slices.ContainsFunc(nodes, func(node store.Node) bool { return node.Name == name }) {
			delete(m.seen, name)
			delete(m.blocked, name)
			delete(m.stuck, name)
			delete(m.retry, name)
		}
	}
	heard := 1 // this node
	frozen := m.Store.Frozen()
	var dead, silent []store.Node
	var settled []string
	for i, node := range nodes {
		if answered[i] {
			heard++
			m.seen[node.Name] = now
			delete(m.blocked, node.Name)
			delete(m.stuck, node.Name)
			delete(m.retry, node.Name)
			delete(m.waits, node.Name)
			if node.Failed() && frozen == "" {
				// After the unlock: a write waits for the log.
				defer m.back(node.Name)
			}
			continue
		}
		if _, known := m.seen[node.Name]; !known {
			m.seen[node.Name] = now // unanswered since this leader began
		}
		unanswered := now.Sub(m.seen[node.Name])
		switch {
		case unanswered < m.after(), now.Before(m.retry[node.Name]):
			continue
		case now.Before(node.AwayUntil):
			m.leaveAlone(node.Name, "it restarts on purpose (its agent stopped cleanly) until "+node.AwayUntil.Format(time.RFC3339))
			continue
		case frozen != "":
			m.leaveAlone(node.Name, "changes are frozen ("+frozen+"): nodes restart on purpose during an upgrade")
			continue
		}
		if !node.Failed() && unanswered >= copiesAfter {
			silent = append(silent, node)
		}
		if len(m.promotableLocked(node.Name)) > 0 {
			dead = append(dead, node)
		} else {
			settled = append(settled, node.Name)
		}
	}
	majority := heard >= len(all)/2+1
	heardNow := map[string]bool{}
	for i, node := range nodes {
		heardNow[node.Name] = answered[i]
	}
	if job, write := m.serveLocked(now, self, all, heardNow, frozen, majority); write {
		go m.publish(ctx, job)
	}
	// A blocked failover ends once none of the tenants it would have
	// promoted still runs on the node, as after an operator's failover.
	// Tenants merely no longer promotable, their replica gone silent too,
	// still wait.
	if majority {
		for _, name := range settled {
			if !m.stillOn(name) {
				delete(m.blocked, name)
				delete(m.stuck, name)
			}
		}
	}
	if !majority && (len(dead) > 0 || len(silent) > 0) {
		for _, node := range append(dead, silent...) {
			m.note(node.Name, fmt.Sprintf("this leader hears only %d of %d nodes: it may be the one cut off", heard, len(all)))
		}
		dead, silent = nil, nil
	}
	// The copies a node long silent keeps go to other nodes, failed over
	// or not.
	for _, node := range silent {
		defer m.markFailed(node.Name)
	}
	start := store.Node{}
	if m.fencing == "" && len(dead) > 0 {
		start = dead[0]
		m.fencing = start.Name
	}
	lastSeen := m.seen[start.Name]
	var worker context.Context
	if start.Name != "" {
		// Its own life, not the probe's, but ended once this node no
		// longer leads.
		worker, m.stopWorker = context.WithCancel(context.WithoutCancel(ctx))
	}
	m.mu.Unlock()
	if start.Name != "" {
		go m.failOver(worker, start, lastSeen, m.term())
	}
}

// promotable names the tenants of node a copy that holds their whole data,
// on a node this leader hears, could run elsewhere.
func (m *Monitor) promotable(node string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.promotableLocked(node)
}

func (m *Monitor) promotableLocked(node string) []string {
	failed := map[string]bool{}
	for _, n := range m.Store.Nodes() {
		failed[n.Name] = n.Failed()
	}
	self, now := m.Store.Node(), m.now()
	hears := func(name string) bool {
		seen, ok := m.seen[name]
		return name == self || ok && now.Sub(seen) < m.after()
	}
	var names []string
	for _, record := range m.Store.List() {
		if record.Node == node && record.ReplicaReady && record.Replica != "" && record.Replica != node && !failed[record.Replica] && hears(record.Replica) {
			names = append(names, record.Spec.Name)
		}
	}
	return names
}

// stillOn reports whether a tenant a blocked failover of node would have
// promoted still runs on node.
func (m *Monitor) stillOn(node string) bool {
	for _, name := range m.stuck[node] {
		if record, err := m.Store.Get(name); err == nil && record.Node == node {
			return true
		}
	}
	return false
}

func (m *Monitor) markFailed(node string) {
	if marked, err := m.Store.MarkFailed(node, "murder (failover)"); err != nil {
		m.Log.Warn("mark a silent node failed", "node", node, "error", err)
	} else if marked {
		m.Log.Warn("a node has not answered for long: the copies it keeps go to other nodes", "node", node)
	}
}

// leaveAlone records, once per reason, why a silent node is left alone on
// purpose: not a failure to alert on.
func (m *Monitor) leaveAlone(node, why string) {
	if m.waits == nil {
		m.waits = map[string]string{}
	}
	if m.waits[node] != why {
		m.Log.Info("a node does not answer, and is left alone", "node", node, "why", why)
	}
	m.waits[node] = why
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

// failOver marks node failed, so the copies it kept go elsewhere, and, if
// any of its tenants could run from a copy, powers it off; once the power-
// off is confirmed and settle has passed since it last answered, it gives
// those tenants to their replicas, unless the node answered meanwhile or
// is no longer off.
func (m *Monitor) failOver(ctx context.Context, node store.Node, lastSeen time.Time, term uint64) {
	defer func() {
		m.mu.Lock()
		m.fencing = ""
		m.mu.Unlock()
	}()
	log := m.Log.With("node", node.Name)
	tenants := m.promotable(node.Name)
	if len(tenants) == 0 {
		return
	}
	blocked := func(why string) {
		m.mu.Lock()
		m.block(node.Name, why)
		if m.stuck == nil {
			m.stuck = map[string][]string{}
		}
		m.stuck[node.Name] = tenants
		m.mu.Unlock()
	}
	manual := "; once it is off, run: murder cluster failover " + node.Name + " -powered-off"
	if m.Fence == nil {
		blocked("no provider to power it off is set up (cluster.failover)" + manual)
		return
	}
	if why := m.unauthorized(ctx, node.Name, lastSeen, term); why != "" {
		log.Warn("the node is not powered off: "+why, "since", lastSeen)
		return
	}
	log.Warn("a node does not answer: powering it off", "since", lastSeen, "tenants", tenants)
	fenceCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	err := m.Fence.PowerOff(fenceCtx, node)
	cancel()
	if err != nil {
		blocked("its power-off is not confirmed: " + err.Error() + manual)
		return
	}
	log.Warn("the node is powered off")
	if wait := lastSeen.Add(m.settle()).Sub(m.now()); wait > 0 {
		if err := m.wait(ctx, wait); err != nil {
			return
		}
	}
	if why := m.unauthorized(ctx, node.Name, lastSeen, term); why != "" {
		log.Warn("its tenants stay: "+why, "since", lastSeen)
		return
	}
	check, cancel := context.WithTimeout(ctx, 30*time.Second)
	off, err := m.Fence.Off(check, node)
	cancel()
	if err != nil || !off {
		blocked(fmt.Sprintf("it is no longer known to be off (off %v, %v)", off, err) + manual)
		return
	}
	// The check of power took a while: the decision is checked again.
	if why := m.unauthorized(ctx, node.Name, lastSeen, term); why != "" {
		log.Warn("its tenants stay: "+why, "since", lastSeen)
		return
	}
	outcome, err := m.Store.FailOver(node.Name, "murder (failover)")
	if err != nil {
		blocked("the failover was not written: " + err.Error())
		return
	}
	m.mu.Lock()
	delete(m.blocked, node.Name)
	delete(m.stuck, node.Name)
	m.mu.Unlock()
	log.Warn("the node's tenants run from their replicas", "promoted", outcome.Promoted, "stranded", outcome.Stranded)
}

// term is the leader's Raft term, 0 if not known.
func (m *Monitor) term() uint64 {
	if m.Term == nil {
		return 0
	}
	return m.Term()
}

// unauthorized says why a failover begun in term may no longer act on
// node, last seen at lastSeen: empty while this node still leads in that
// term, hears a majority, and node has not answered since.
func (m *Monitor) unauthorized(ctx context.Context, node string, lastSeen time.Time, term uint64) string {
	switch {
	case ctx.Err() != nil:
		return "this node no longer leads"
	case !m.Leader():
		return "this node no longer leads: the new leader decides"
	case m.Term != nil && m.Term() != term:
		return "leadership changed meanwhile: the leader decides again"
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.seen[node].After(lastSeen) {
		return "the node answered again"
	}
	now, heard, all := m.now(), 1, m.Store.Nodes()
	self := m.Store.Node()
	for _, other := range all {
		if other.Name != self && now.Sub(m.seen[other.Name]) < m.after() {
			heard++
		}
	}
	if heard < len(all)/2+1 {
		return fmt.Sprintf("this leader hears only %d of %d nodes: it may be the one cut off", heard, len(all))
	}
	return ""
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
func ManualFailOver(ctx context.Context, st State, probe func(context.Context, string) error, node, actor string) (store.FailedOver, error) {
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
	m.writeRecordMetrics(out)
}
