package failover

import (
	"context"
	"net/netip"
	"slices"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/metrics"
	"github.com/PowerPenguini/murder/internal/store"
)

// The leader keeps the base domain's records naming the public addresses
// of the nodes that serve. A node leaves them once it has not answered for
// After, unless it restarts on purpose or changes are frozen for an
// upgrade, when its edge keeps serving; it comes back once it has answered
// without a break for After. A new leader starts from every node not
// failed, and writes nothing until it has led for After. Nothing is
// written while this leader hears too few nodes: it may be the one cut
// off. The records are written when they change, and again every
// recordsRefresh to undo changes made by hand.
const (
	recordsRefresh = 5 * time.Minute
	recordsRetry   = 30 * time.Second
	// recordsTimeout bounds one write of the records.
	recordsTimeout = 2 * time.Minute
)

// recordsView is the leader's view of which nodes the records name.
type recordsView struct {
	serving map[string]bool
	// upSince is since when each node has answered without a break.
	upSince map[string]time.Time
	since   time.Time // leading since
	// published are the addresses last written, at publishedAt; failing
	// says the last write failed, retry when to try again.
	published   string
	publishedAt time.Time
	count       int
	failing     bool
	retry       time.Time
	// missing names the serving nodes without public addresses, while
	// nothing is written for want of them.
	missing string
	// term counts the leaderships: a write begun in an earlier one does
	// not count for this one.
	term int
	// unkept is set once this leader, without cluster.dns, said so.
	unkept bool
}

// startRecords begins a leader's view: every node not failed serves.
func (m *Monitor) startRecords(now time.Time, all []store.Node) {
	m.records = recordsView{serving: map[string]bool{}, upSince: map[string]time.Time{}, since: now, term: m.records.term + 1}
	for _, node := range all {
		if !node.Failed() {
			m.records.serving[node.Name] = true
		}
	}
}

// serveLocked updates which nodes serve from this round's probes, and
// returns the addresses to write, if they are to be written now.
func (m *Monitor) serveLocked(now time.Time, self string, all []store.Node, answered map[string]bool, frozen string, majority bool) ([]netip.Addr, int, bool) {
	r := &m.records
	if m.Records == nil {
		// Only the leader writes the records: one without cluster.dns
		// leaves them as they are, dead nodes included.
		if !r.unkept && slices.ContainsFunc(all, func(node store.Node) bool { return len(node.Public) > 0 }) {
			m.Log.Warn("this leader keeps no DNS records, though nodes have public addresses: give every node the same cluster.dns")
			r.unkept = true
		}
		return nil, 0, false
	}
	for _, node := range all {
		name := node.Name
		switch {
		case name == self:
			r.serving[name] = true
		case answered[name]:
			if _, ok := r.upSince[name]; !ok {
				r.upSince[name] = now
			}
			if !r.serving[name] && now.Sub(r.upSince[name]) >= m.after() {
				r.serving[name] = true
				m.Log.Info("a node serves: the records name it", "node", name)
			}
		default:
			delete(r.upSince, name)
			leftAlone := now.Before(node.AwayUntil) || frozen != ""
			if r.serving[name] && majority && !leftAlone && now.Sub(m.seen[name]) >= m.after() {
				r.serving[name] = false
				m.Log.Warn("a node does not answer: the records no longer name it", "node", name)
			}
		}
	}
	for name := range r.serving {
		if !slices.ContainsFunc(all, func(node store.Node) bool { return node.Name == name }) {
			delete(r.serving, name)
			delete(r.upSince, name)
		}
	}
	if !majority || now.Sub(r.since) < m.after() || m.writing || now.Before(r.retry) {
		return nil, 0, false
	}
	// While a node that serves has not recorded its public addresses, as
	// before its agent runs a release that does, nothing is written: the
	// records would leave out a node that serves.
	var addresses []netip.Addr
	var missing []string
	for _, node := range all {
		if r.serving[node.Name] {
			if len(node.Public) == 0 {
				missing = append(missing, node.Name)
			}
			addresses = append(addresses, node.Public...)
		}
	}
	if key := strings.Join(missing, " "); key != r.missing {
		if key != "" {
			m.Log.Warn("the DNS records are not written while nodes that serve have no public addresses (cluster.publicAddresses)", "nodes", key)
		}
		r.missing = key
	}
	if len(missing) > 0 || len(addresses) == 0 {
		return nil, 0, false
	}
	slices.SortFunc(addresses, func(a, b netip.Addr) int { return a.Compare(b) })
	addresses = slices.Compact(addresses)
	if key := addressKey(addresses); key == r.published && now.Sub(r.publishedAt) < recordsRefresh {
		return nil, 0, false
	}
	m.writing = true
	return addresses, r.term, true
}

func addressKey(addresses []netip.Addr) string {
	fields := make([]string, len(addresses))
	for i, address := range addresses {
		fields[i] = address.String()
	}
	return strings.Join(fields, " ")
}

// publish writes the records, stopping as soon as this node no longer
// leads: a write of an old leader landing after the new leader's would
// name the nodes as it last saw them.
func (m *Monitor) publish(ctx context.Context, addresses []netip.Addr, term int) {
	write, cancel := context.WithTimeout(context.WithoutCancel(ctx), recordsTimeout)
	defer cancel()
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-write.Done():
				return
			case <-ticker.C:
				if !m.Leader() {
					cancel()
					return
				}
			}
		}
	}()
	err := context.Canceled
	if m.Leader() {
		err = m.Records.Publish(write, addresses)
	}
	leads := m.Leader()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writing = false
	r := &m.records
	// A write stopped because this node no longer leads is no failure.
	if !leads || !m.leading || r.term != term {
		return
	}
	key := addressKey(addresses)
	if err != nil {
		r.retry = m.now().Add(recordsRetry)
		if !r.failing {
			m.Log.Error("the DNS records cannot be written: clients may still reach nodes that do not answer", "addresses", key, "error", err)
		}
		r.failing = true
		return
	}
	if r.failing || key != r.published {
		m.Log.Info("the DNS records name the nodes that serve", "addresses", key)
	}
	r.published, r.publishedAt, r.count, r.failing = key, m.now(), len(addresses), false
}

func (m *Monitor) writeRecordMetrics(out *metrics.Writer) {
	if m.Records == nil {
		return
	}
	out.Family("murder_dns_records_failing", "gauge", "1 if the leader's last write of the DNS records failed.")
	out.Sample("murder_dns_records_failing", metrics.Bool(m.records.failing))
	out.Family("murder_dns_records_addresses", "gauge", "How many addresses the DNS records last written name.")
	out.Sample("murder_dns_records_addresses", float64(m.records.count))
}
