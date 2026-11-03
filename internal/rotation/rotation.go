// Package rotation rotates the master key of every node, without stopping
// one: each node gets the new key as its next one (trusted, not used),
// then uses it (presents its authority, seals with it), then every secret
// is sealed again with it, and the old keys go. Nodes part way through
// still know each other, as each trusts every key of its ring. Every step
// can be taken again: a rotation stopped half way goes on where it was.
package rotation

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/secrets"
)

// Ring is one node's master key ring.
type Ring interface {
	Ring(ctx context.Context) ([]secrets.Entry, error)
	Add(ctx context.Context, raw []byte) error
	Use(ctx context.Context, id string) error
	Drop(ctx context.Context, id string) error
}

// DefaultSettle is how long a step waits for every node's edge, which
// reads the key file every few seconds, to follow it.
const DefaultSettle = 10 * time.Second

// Rotation rotates the master key of the nodes, from one of them.
type Rotation struct {
	// Nodes are every node of the cluster; this one alone outside one.
	Nodes []string
	// On is a node's ring.
	On func(node string) Ring
	// Raw is a key of this node's ring, to send to the other nodes.
	Raw func(id string) ([]byte, bool)
	// Reseal seals every secret again with key, which this node must use.
	Reseal func(key string) (int, error)
	Settle time.Duration
	// Report says what the rotation does as it does it.
	Report func(string)
}

// Result is what a rotation did.
type Result struct {
	// Key is the key in use now; Dropped the keys taken out.
	Key      string   `json:"key"`
	Dropped  []string `json:"dropped,omitempty"`
	Resealed int      `json:"resealed"`
	// Steps say what was done, in order.
	Steps []string `json:"steps"`
}

// Run rotates the master key, or finishes a rotation stopped half way.
func (r *Rotation) Run(ctx context.Context) (Result, error) {
	var result Result
	report := func(format string, args ...any) {
		step := fmt.Sprintf(format, args...)
		result.Steps = append(result.Steps, step)
		if r.Report != nil {
			r.Report(step)
		}
	}
	rings := map[string][]secrets.Entry{}
	for _, node := range r.Nodes {
		ring, err := r.On(node).Ring(ctx)
		if err != nil {
			return result, fmt.Errorf("node %s: %w: every node takes part in a rotation; bring it back, or take it out of the cluster", node, err)
		}
		rings[node] = ring
	}
	target, raw, err := r.target(rings)
	if err != nil {
		return result, err
	}
	result.Key = target
	if raw != nil {
		report("made key %s", target)
	} else {
		report("goes on with key %s", target)
	}

	// 1. Every node trusts the key.
	added := false
	for _, node := range r.Nodes {
		if has(rings[node], target) {
			continue
		}
		if raw == nil {
			found, ok := r.Raw(target)
			if !ok {
				return result, fmt.Errorf("key %s is on %s, not on this node: run the rotation there", target, strings.Join(holders(rings, target), ", "))
			}
			raw = found
		}
		if err := r.On(node).Add(ctx, raw); err != nil {
			return result, fmt.Errorf("node %s: add key %s: %w", node, target, err)
		}
		report("node %s trusts key %s", node, target)
		added = true
	}
	if added {
		if err := r.settle(ctx); err != nil {
			return result, err
		}
	}

	// 2. Every node uses it.
	used := false
	for _, node := range r.Nodes {
		if rings[node][0].ID == target {
			continue
		}
		if err := r.On(node).Use(ctx, target); err != nil {
			return result, fmt.Errorf("node %s: use key %s: %w", node, target, err)
		}
		report("node %s uses key %s", node, target)
		used = true
	}
	if used {
		if err := r.settle(ctx); err != nil {
			return result, err
		}
	}

	// 3. Every secret is sealed with it.
	count, err := r.Reseal(target)
	if err != nil {
		return result, fmt.Errorf("seal the secrets again: %w; the old keys stay", err)
	}
	result.Resealed = count
	report("sealed %d secrets with key %s", count, target)

	// 4. The other keys go.
	for _, node := range r.Nodes {
		ring, err := r.On(node).Ring(ctx)
		if err != nil {
			return result, fmt.Errorf("node %s: %w", node, err)
		}
		if ring[0].ID != target {
			return result, fmt.Errorf("node %s uses key %s, not %s: has the rotation been run elsewhere at once?", node, ring[0].ID, target)
		}
		for _, entry := range ring[1:] {
			if err := r.On(node).Drop(ctx, entry.ID); err != nil {
				return result, fmt.Errorf("node %s: drop key %s: %w", node, entry.ID, err)
			}
			report("node %s dropped key %s", node, entry.ID)
			if !slices.Contains(result.Dropped, entry.ID) {
				result.Dropped = append(result.Dropped, entry.ID)
			}
		}
	}
	return result, nil
}

// target is the key the nodes rotate to: a key on its way in, or the one
// they use with older ones left; a new one, with its bytes, if every node
// has one key, the same.
func (r *Rotation) target(rings map[string][]secrets.Entry) (string, []byte, error) {
	var next, current []string
	whole := true
	for _, node := range r.Nodes {
		ring := rings[node]
		if len(ring) == 0 {
			return "", nil, fmt.Errorf("node %s has no master key", node)
		}
		if !slices.Contains(current, ring[0].ID) {
			current = append(current, ring[0].ID)
		}
		if len(ring) > 1 {
			whole = false
		}
		for _, entry := range ring {
			if entry.State == secrets.StateNext && !slices.Contains(next, entry.ID) {
				next = append(next, entry.ID)
			}
		}
	}
	switch {
	case len(next) > 1:
		return "", nil, fmt.Errorf("the nodes have several next keys (%s): two rotations ran at once; take all but one out by hand", strings.Join(next, ", "))
	case len(next) == 1:
		// A node using it already, and another using a third key, is no
		// rotation this one made.
		for _, id := range current {
			if id != next[0] && !r.everyNodeHas(rings, id) {
				return "", nil, fmt.Errorf("the nodes use keys %s: not one rotation", strings.Join(current, ", "))
			}
		}
		return next[0], nil, nil
	case len(current) > 1:
		return "", nil, fmt.Errorf("the nodes use different keys (%s) with none next: put the same key ring on each by hand", strings.Join(current, ", "))
	case !whole:
		// Every node uses the same key; older keys are left to drop.
		return current[0], nil, nil
	}
	raw, err := secrets.NewRaw()
	if err != nil {
		return "", nil, err
	}
	return secrets.KeyID(raw), raw, nil
}

func (r *Rotation) everyNodeHas(rings map[string][]secrets.Entry, id string) bool {
	for _, node := range r.Nodes {
		if !has(rings[node], id) {
			return false
		}
	}
	return true
}

func (r *Rotation) settle(ctx context.Context) error {
	settle := r.Settle
	if settle == 0 {
		settle = DefaultSettle
	}
	select {
	case <-ctx.Done():
		return errors.Join(errors.New("stopped part way: run it again to go on"), context.Cause(ctx))
	case <-time.After(settle):
		return nil
	}
}

func has(ring []secrets.Entry, id string) bool {
	return slices.ContainsFunc(ring, func(e secrets.Entry) bool { return e.ID == id })
}

func holders(rings map[string][]secrets.Entry, id string) []string {
	var out []string
	for node, ring := range rings {
		if has(ring, id) {
			out = append(out, node)
		}
	}
	slices.Sort(out)
	return out
}
