package store

import (
	"errors"
	"maps"
	"slices"
	"time"
)

func (s *SQLite) PlaceReplicas(actor string) (int, error) {
	placed := 0
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		placed = 0
		changes := placeReplicas(current)
		if len(changes) == 0 {
			return errUnchanged
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

func (s *SQLite) ReplicaReady(name, replica string) error {
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		record, ok := current.tenants[name]
		if !ok || record.Replica != replica || replica == "" || record.ReplicaReady {
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

// placeReplicas returns the tenants whose replica must change, and the node
// each gets: for each whose replica is missing, its own node or a node no
// longer in the cluster, the node of the cluster, other than its own, that
// keeps the fewest copies (the first by name among equals); "" if there is
// none. Moving tenants are left as they are: a move settles their nodes.
func placeReplicas(current *snapshot) map[string]string {
	counts := map[string]int{}
	for name := range current.nodes {
		counts[name] = 0
	}
	valid := func(record Record) bool {
		_, member := current.nodes[record.Replica]
		return record.Replica != "" && member && record.Replica != record.Node
	}
	for _, record := range current.tenants {
		if valid(record) {
			counts[record.Replica]++
		}
	}
	nodes := slices.Sorted(maps.Keys(current.nodes))
	changes := map[string]string{}
	for _, name := range slices.Sorted(maps.Keys(current.tenants)) {
		record := current.tenants[name]
		if record.MoveTo != "" || valid(record) {
			continue
		}
		best := ""
		for _, node := range nodes {
			if node != record.Node && (best == "" || counts[node] < counts[best]) {
				best = node
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
