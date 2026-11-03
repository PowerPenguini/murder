package store

import (
	"maps"
	"slices"
)

// Auto, as a new tenant's node, places it on the least loaded node.
const Auto = "auto"

// Move is one tenant to move, to balance the nodes.
type Move struct {
	Tenant string `json:"tenant"`
	From   string `json:"from"`
	To     string `json:"to"`
}

// loads is how many tenants each node that is not failed runs: live and
// not suspended, a moving tenant counted where it goes. A cluster without
// nodes is none.
func loads(nodes []Node, records []Record) map[string]int {
	load := map[string]int{}
	for _, node := range nodes {
		if !node.Failed() {
			load[node.Name] = 0
		}
	}
	for _, record := range records {
		if record.Archived() || record.Spec.Suspended {
			continue
		}
		node := record.Node
		if record.MoveTo != "" {
			node = record.MoveTo
		}
		if _, ok := load[node]; ok {
			load[node]++
		}
	}
	return load
}

// LeastLoaded is the node a new tenant goes to: the one running the fewest
// tenants, self among equals, then the first by name; self outside a
// cluster.
func LeastLoaded(nodes []Node, records []Record, self string) string {
	load := loads(nodes, records)
	if len(load) == 0 {
		return self
	}
	best := ""
	for _, name := range slices.Sorted(maps.Keys(load)) {
		switch {
		case best == "", load[name] < load[best]:
			best = name
		case load[name] == load[best] && name == self:
			best = name
		}
	}
	return best
}

// BalanceMoves are the moves, one after the other, that leave no node
// running two tenants more than another: from the node running the most
// to the one running the fewest, a tenant whose whole copy that node keeps
// first (its move sends only what changed), then the first by name.
// Suspended and archived tenants stay; none move while one moves already.
func BalanceMoves(nodes []Node, records []Record) []Move {
	load := loads(nodes, records)
	if len(load) < 2 {
		return nil
	}
	where := map[string]Record{}
	for _, record := range records {
		if record.MoveTo != "" {
			return nil
		}
		if record.Archived() || record.Spec.Suspended {
			continue
		}
		if _, ok := load[record.Node]; ok {
			where[record.Spec.Name] = record
		}
	}
	names := slices.Sorted(maps.Keys(load))
	var moves []Move
	for range len(where) {
		most, fewest := names[0], names[0]
		for _, name := range names {
			if load[name] > load[most] {
				most = name
			}
			if load[name] < load[fewest] {
				fewest = name
			}
		}
		if load[most]-load[fewest] < 2 {
			break
		}
		var chosen string
		for _, name := range slices.Sorted(maps.Keys(where)) {
			record := where[name]
			if record.Node != most {
				continue
			}
			if record.Replica == fewest && record.ReplicaReady {
				chosen = name
				break
			}
			if chosen == "" {
				chosen = name
			}
		}
		record := where[chosen]
		moves = append(moves, Move{Tenant: chosen, From: most, To: fewest})
		load[most]--
		load[fewest]++
		// Moved onto its copy, it keeps none until placement gives it one.
		record.Node, record.Replica, record.ReplicaReady = fewest, "", false
		where[chosen] = record
	}
	return moves
}
