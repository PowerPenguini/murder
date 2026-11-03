package store

import (
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"slices"
	"strings"
	"time"
)

func (s *SQLite) PlaceReplicas(actor string) (int, error) {
	placed := 0
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		placed = 0
		// A copy stops moving to a node that can no longer keep it.
		for _, name := range slices.Sorted(maps.Keys(current.tenants)) {
			record := current.tenants[name]
			if record.ReplicaNext == "" || canKeep(current, record, record.ReplicaNext) {
				continue
			}
			if err := tx.Exec(`UPDATE tenants SET replica_next = '' WHERE name = ? AND replica_next = ?`, name, record.ReplicaNext); err != nil {
				return err
			}
			if err := event(tx, now, actor, "tenant.replica", name, record.App, "the copy no longer moves to "+record.ReplicaNext); err != nil {
				return err
			}
			placed++
		}
		changes := placeReplicas(current, now)
		if len(changes) == 0 && placed == 0 {
			// Copies move only once nothing else changes: one at a time,
			// on a state this batch has not changed.
			name, target := rebalance(current, now)
			if name == "" {
				return errUnchanged
			}
			record := current.tenants[name]
			if err := tx.Exec(`UPDATE tenants SET replica_next = ? WHERE name = ? AND replica = ? AND replica_next = ''`, target, name, record.Replica); err != nil {
				return err
			}
			placed++
			return event(tx, now, actor, "tenant.replica-moving", name, record.App, "copy moves from "+record.Replica+" to "+target+", which keeps fewer")
		}
		for _, name := range slices.Sorted(maps.Keys(changes)) {
			record := current.tenants[name]
			// Only as the tenant is now: a batch computed against another
			// state is refused as stale anyway.
			if err := tx.Exec(`UPDATE tenants SET replica = ?, replica_ready = 0 WHERE name = ? AND node = ? AND replica = ?`, changes[name], name, record.Node, record.Replica); err != nil {
				return err
			}
			detail := "copy kept on " + changes[name]
			if changes[name] == "" {
				detail = "no other node to keep a copy"
			}
			if err := event(tx, now, actor, "tenant.replica", name, record.App, detail); err != nil {
				return err
			}
			placed++
		}
		return nil
	})
	if errors.Is(err, errUnchanged) {
		return 0, nil
	}
	return placed, err
}

// canKeep reports whether node can take the copy of record as its next
// replica.
func canKeep(current *snapshot, record Record, node string) bool {
	member, ok := current.nodes[node]
	return ok && !member.Failed() && node != record.Node && node != record.Replica && record.MoveTo == ""
}

// rebalance picks the copy to move, and where: once every tenant whose
// node is not failed has a whole copy, and none moves already, the copy
// of the first tenant (by name) kept on the node keeping the most copies,
// to the node keeping the fewest, at least two fewer. A tenant's copy is
// never moved to its own node; tenants of failed nodes keep theirs where
// it is, their node sending nothing.
func rebalance(current *snapshot, now time.Time) (string, string) {
	counts := map[string]int{}
	for name, node := range current.nodes {
		if !node.Failed() {
			counts[name] = 0
		}
	}
	refused := func(node string) bool { return now.Before(current.nodes[node].CopiesRefusedUntil) }
	for _, record := range current.tenants {
		if current.nodes[record.Node].Failed() {
			if _, ok := counts[record.Replica]; ok && record.Replica != "" {
				counts[record.Replica]++
			}
			continue
		}
		if record.Replica == "" || !record.ReplicaReady || record.ReplicaNext != "" || record.MoveTo != "" {
			return "", ""
		}
		if _, ok := counts[record.Replica]; !ok {
			return "", "" // placement gives it another first
		}
		counts[record.Replica]++
	}
	nodes := slices.Sorted(maps.Keys(counts))
	most := slices.Clone(nodes)
	slices.SortStableFunc(most, func(a, b string) int { return counts[b] - counts[a] })
	fewest := slices.Clone(nodes)
	slices.SortStableFunc(fewest, func(a, b string) int { return counts[a] - counts[b] })
	names := slices.Sorted(maps.Keys(current.tenants))
	for _, from := range most {
		for _, to := range fewest {
			if counts[from]-counts[to] < 2 {
				break
			}
			if refused(to) {
				continue
			}
			for _, name := range names {
				record := current.tenants[name]
				if record.Replica == from && record.Node != to && !current.nodes[record.Node].Failed() {
					return name, to
				}
			}
		}
	}
	return "", ""
}

// SwitchReplica makes next, the tenant's ReplicaNext, its replica, ready,
// as owner sent it the whole data; like ReplicaReady, it changes nothing
// unless owner still runs the tenant and it is not moving.
func (s *SQLite) SwitchReplica(name, next, owner string) (bool, error) {
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		record, ok := current.tenants[name]
		if !ok || next == "" || record.ReplicaNext != next || !reportedByOwner(record, owner) {
			return errUnchanged
		}
		if err := tx.Exec(`UPDATE tenants SET replica = replica_next, replica_ready = 1, replica_next = '' WHERE name = ? AND replica_next = ?`, name, next); err != nil {
			return err
		}
		return event(tx, now, "murder", "tenant.replica", name, record.App, "copy kept on "+next+", which holds the whole data, instead of "+record.Replica)
	})
	if errors.Is(err, errUnchanged) {
		return false, nil
	}
	return err == nil, err
}

// copiesRefusedFor is how long no copy goes to a node one failed to get
// to.
const copiesRefusedFor = 30 * time.Minute

func (s *SQLite) GiveUpCopy(name, node, why string) (bool, error) {
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		record, ok := current.tenants[name]
		switch {
		case !ok || node == "":
			return errUnchanged
		case record.ReplicaNext == node:
			if err := tx.Exec(`UPDATE tenants SET replica_next = '' WHERE name = ? AND replica_next = ?`, name, node); err != nil {
				return err
			}
		case record.Replica == node && !record.ReplicaReady:
			// A copy that never became whole: placement gives another.
			if err := tx.Exec(`UPDATE tenants SET replica = '', replica_ready = 0 WHERE name = ? AND replica = ? AND replica_ready = 0`, name, node); err != nil {
				return err
			}
		default:
			return errUnchanged
		}
		next := node
		// Other copies would fail to get there too: none moves there for
		// a while.
		if err := tx.Exec(`UPDATE nodes SET copies_refused_until = ? WHERE name = ?`, formatTime(now.Add(copiesRefusedFor)), next); err != nil {
			return err
		}
		return event(tx, now, "murder", "tenant.replica", name, record.App, "no copy is kept on "+next+" any more: "+why)
	})
	if errors.Is(err, errUnchanged) {
		return false, nil
	}
	return err == nil, err
}

// reportedByOwner reports whether a send's report, by owner, may change
// the readiness of record's copies: a report from a node that no longer
// runs the tenant, or of a tenant moving, proves nothing of a copy now. A
// send begun before its data was replaced from a backup cannot report
// either: the replacement holds the tenant's sends off on its node, the
// only one that sends it, and ends any under way first (see
// replicate.Replicator.Hold). Other changes of the tenant, to its
// settings, image or containers, leave its data, and its copies, as they
// are.
func reportedByOwner(record Record, owner string) bool {
	return owner != "" && record.Node == owner && record.MoveTo == ""
}

// ReplicaReady records that the copy of name on replica holds the whole
// data, as owner sent it. A report reportedByOwner refuses changes
// nothing: the next send reports again.
func (s *SQLite) ReplicaReady(name, replica, owner string) error {
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		record, ok := current.tenants[name]
		if !ok || record.Replica != replica || replica == "" || record.ReplicaReady || !reportedByOwner(record, owner) {
			return errUnchanged
		}
		if err := tx.Exec(`UPDATE tenants SET replica_ready = 1 WHERE name = ? AND replica = ?`, name, replica); err != nil {
			return err
		}
		return event(tx, now, "murder", "tenant.replica-ready", name, record.App, "the copy on "+replica+" holds the whole data")
	})
	if errors.Is(err, errUnchanged) {
		return nil
	}
	return err
}

// ReplicaStale records that the copy of name no longer matches its data,
// replaced from a backup say: no failover takes it until a send makes it
// whole again. The caller holds the tenant's sends off meanwhile, so none
// begun before reports it whole. It writes even when the copy is not
// ready: a report still on its way, computed against the state before,
// is then refused as stale.
func (s *SQLite) ReplicaStale(name, why, actor string) error {
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		record, ok := current.tenants[name]
		if !ok {
			return fmt.Errorf("tenant %s: %w", name, ErrNotFound)
		}
		if err := tx.Exec(`UPDATE tenants SET replica_ready = 0 WHERE name = ?`, name); err != nil {
			return err
		}
		if record.Replica == "" {
			return nil
		}
		return event(tx, now, actor, "tenant.replica", name, record.App, "the copy on "+record.Replica+" no longer matches: "+why)
	})
	return err
}

// placeReplicas returns the tenants whose replica must change, and the node
// each gets: for each whose replica is missing, its own node or a node no
// longer in the cluster, the node of the cluster, other than its own, that
// keeps the fewest copies (the first by name among equals); "" if there is
// none. Moving tenants are left as they are: a move settles their nodes.
func placeReplicas(current *snapshot, now time.Time) map[string]string {
	// Failed nodes keep no copies until they answer again.
	counts := map[string]int{}
	for name, node := range current.nodes {
		if !node.Failed() {
			counts[name] = 0
		}
	}
	// A tenant of a failed node stays there: its node sends nothing to a
	// copy, which could never be whole.
	ownerFailed := func(record Record) bool { return current.nodes[record.Node].Failed() }
	valid := func(record Record) bool {
		_, usable := counts[record.Replica]
		return record.Replica != "" && usable && record.Replica != record.Node
	}
	for _, record := range current.tenants {
		if valid(record) {
			counts[record.Replica]++
		}
		if _, usable := counts[record.ReplicaNext]; usable && record.ReplicaNext != "" {
			counts[record.ReplicaNext]++
		}
	}
	nodes := slices.Sorted(maps.Keys(counts))
	changes := map[string]string{}
	for _, name := range slices.Sorted(maps.Keys(current.tenants)) {
		record := current.tenants[name]
		if record.MoveTo != "" || valid(record) || ownerFailed(record) {
			continue
		}
		// A node that refused a copy lately gets one only if no other can.
		best := ""
		for _, refusing := range []bool{false, true} {
			for _, node := range nodes {
				if node == record.Node || now.Before(current.nodes[node].CopiesRefusedUntil) != refusing {
					continue
				}
				if best == "" || counts[node] < counts[best] {
					best = node
				}
			}
			if best != "" {
				break
			}
		}
		if best == record.Replica {
			continue // still none to be had
		}
		changes[name] = best
		if best != "" {
			counts[best]++
		}
	}
	return changes
}

func (s *SQLite) FailOver(node, actor string) (FailedOver, error) {
	var outcome FailedOver
	_, err := s.write(true, func(tx *recorder, current *snapshot, now time.Time) error {
		outcome = FailedOver{Promoted: []string{}, Stranded: []string{}}
		failed, member := current.nodes[node]
		if !member {
			return fmt.Errorf("node %s: %w", node, ErrNotFound)
		}
		if node == current.node {
			return fmt.Errorf("%w: node %s cannot fail itself over", ErrConflict, node)
		}
		changed := !failed.Failed()
		for _, name := range slices.Sorted(maps.Keys(current.tenants)) {
			record := current.tenants[name]
			switch {
			case record.MoveTo == node:
				// Its target is gone: it stays where it is.
				if err := tx.Exec(`UPDATE tenants SET move_to = '', move_held = 0, updated_at = ? WHERE name = ? AND move_to = ?`, formatTime(now), name, node); err != nil {
					return err
				}
				if err := event(tx, now, actor, "tenant.move-cancelled", name, record.App, "its target "+node+" failed"); err != nil {
					return err
				}
				changed = true
			case record.Node != node:
				continue
			case !record.ReplicaReady || record.Replica == "" || record.Replica == node || current.nodes[record.Replica].Failed():
				// No copy that holds the whole data to run it from.
				outcome.Stranded = append(outcome.Stranded, name)
				continue
			default:
				if _, ok := current.nodes[record.Replica]; !ok {
					outcome.Stranded = append(outcome.Stranded, name)
					continue
				}
				if err := tx.Exec(`UPDATE tenants SET node = replica, replica = '', replica_ready = 0, replica_next = '', move_to = '', move_held = 0, generation = generation + 1, updated_at = ? WHERE name = ? AND node = ? AND replica_ready = 1`, formatTime(now), name, node); err != nil {
					return err
				}
				if err := event(tx, now, actor, "tenant.failed-over", name, record.App, node+" → "+record.Replica); err != nil {
					return err
				}
				outcome.Promoted = append(outcome.Promoted, name)
				changed = true
			}
		}
		// The copies the failed node kept are gone with it.
		if dropped, err := dropCopiesOn(tx, current, node); err != nil {
			return err
		} else if dropped {
			changed = true
		}
		if !changed {
			return errUnchanged
		}
		if !failed.Failed() {
			if err := tx.Exec(`UPDATE nodes SET failed_at = ? WHERE name = ?`, formatTime(now), node); err != nil {
				return err
			}
		}
		detail := fmt.Sprintf("%d tenants run from their replicas", len(outcome.Promoted))
		if len(outcome.Stranded) > 0 {
			detail += fmt.Sprintf("; %d had no replica and stay: %v", len(outcome.Stranded), outcome.Stranded)
		}
		return event(tx, now, actor, "node.failed-over", "", "", node+": "+detail)
	})
	if errors.Is(err, errUnchanged) {
		return outcome, nil
	}
	return outcome, err
}

func (s *SQLite) NodeBack(node, actor string) error {
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		failed, member := current.nodes[node]
		if !member || !failed.Failed() {
			return errUnchanged
		}
		if err := tx.Exec(`UPDATE nodes SET failed_at = NULL, copies_refused_until = NULL WHERE name = ?`, node); err != nil {
			return err
		}
		return event(tx, now, actor, "node.back", "", "", node+" answers again; it keeps copies again")
	})
	if errors.Is(err, errUnchanged) {
		return nil
	}
	return err
}

// dropCopiesOn clears the replica of every tenant whose copy node keeps,
// and stops copies moving to it.
func dropCopiesOn(tx *recorder, current *snapshot, node string) (bool, error) {
	dropped := false
	for _, name := range slices.Sorted(maps.Keys(current.tenants)) {
		record := current.tenants[name]
		if record.Replica == node && record.Node != node {
			if err := tx.Exec(`UPDATE tenants SET replica = '', replica_ready = 0 WHERE name = ? AND replica = ?`, name, node); err != nil {
				return false, err
			}
			dropped = true
		}
		if record.ReplicaNext == node {
			if err := tx.Exec(`UPDATE tenants SET replica_next = '' WHERE name = ? AND replica_next = ?`, name, node); err != nil {
				return false, err
			}
			dropped = true
		}
	}
	return dropped, nil
}

func (s *SQLite) MarkFailed(node, actor string) (bool, error) {
	_, err := s.write(true, func(tx *recorder, current *snapshot, now time.Time) error {
		failed, member := current.nodes[node]
		if !member {
			return fmt.Errorf("node %s: %w", node, ErrNotFound)
		}
		if node == current.node {
			return fmt.Errorf("%w: node %s cannot mark itself failed", ErrConflict, node)
		}
		dropped, err := dropCopiesOn(tx, current, node)
		if err != nil {
			return err
		}
		if failed.Failed() && !dropped {
			return errUnchanged
		}
		if !failed.Failed() {
			if err := tx.Exec(`UPDATE nodes SET failed_at = ? WHERE name = ?`, formatTime(now), node); err != nil {
				return err
			}
		}
		return event(tx, now, actor, "node.failed", "", "", node+" does not answer: it keeps no copies until it does")
	})
	if errors.Is(err, errUnchanged) {
		return false, nil
	}
	return err == nil, err
}

func (s *SQLite) SetPublic(node string, addresses []netip.Addr) error {
	sorted := slices.Clone(addresses)
	slices.SortFunc(sorted, func(a, b netip.Addr) int { return a.Compare(b) })
	sorted = slices.Compact(sorted)
	fields := make([]string, 0, len(sorted))
	for _, address := range sorted {
		if !address.IsValid() {
			return fmt.Errorf("an invalid public address of node %s", node)
		}
		fields = append(fields, address.Unmap().String())
	}
	public := strings.Join(fields, " ")
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		existing, member := current.nodes[node]
		if !member {
			return fmt.Errorf("node %s: %w", node, ErrNotFound)
		}
		if publicField(existing.Public) == public {
			return errUnchanged
		}
		if err := tx.Exec(`UPDATE nodes SET public = ? WHERE name = ?`, public, node); err != nil {
			return err
		}
		return event(tx, now, "murder", "node.public", "", "", node+" serves at "+public)
	})
	if errors.Is(err, errUnchanged) {
		return nil
	}
	return err
}

func publicField(addresses []netip.Addr) string {
	fields := make([]string, len(addresses))
	for i, address := range addresses {
		fields[i] = address.String()
	}
	return strings.Join(fields, " ")
}

func (s *SQLite) Away(node string, until time.Time) error {
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		existing, member := current.nodes[node]
		if !member || existing.AwayUntil.Equal(until) {
			return errUnchanged
		}
		if until.IsZero() {
			return tx.Exec(`UPDATE nodes SET away_until = NULL WHERE name = ?`, node)
		}
		if err := tx.Exec(`UPDATE nodes SET away_until = ? WHERE name = ?`, formatTime(until), node); err != nil {
			return err
		}
		return event(tx, now, "murder", "node.away", "", "", node+" restarts on purpose until "+formatTime(until))
	})
	if errors.Is(err, errUnchanged) {
		return nil
	}
	return err
}
