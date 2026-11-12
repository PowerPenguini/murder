package cluster

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/failover"
	"github.com/PowerPenguini/murder/internal/store"
)

// fakeFence powers nodes off at once, and says when.
type fakeFence struct {
	mu     sync.Mutex
	fenced map[string]time.Time
}

func (f *fakeFence) PowerOff(_ context.Context, node store.Node) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.fenced[node.Name]; !ok {
		f.fenced[node.Name] = time.Now()
	}
	return nil
}

func (f *fakeFence) Off(_ context.Context, node store.Node) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.fenced[node.Name]
	return ok, nil
}

func (f *fakeFence) at(node string) (time.Time, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	at, ok := f.fenced[node]
	return at, ok
}

// A node of a real three-node cluster stops answering: the leader's
// monitor powers it off, waits for the settle since its last answer, and
// only then gives its tenants to their replicas' nodes, held at the edges
// until they answer there. The surviving nodes agree on all of it.
func TestADeadNodesTenantsFailOverInARealCluster(t *testing.T) {
	nodes := threeNodes(t)
	var leader, victim *node
	eventually(t, "a leader", func() bool {
		for _, n := range nodes {
			if n.cluster.Leader() {
				leader = n
				return true
			}
		}
		return false
	})
	for _, n := range nodes {
		if n != leader {
			victim = n
			break
		}
	}
	if _, err := leader.store.PutApp(store.App{Name: "web", Spec: appSpec()}, "alice"); err != nil {
		t.Fatal(err)
	}
	tenants := []string{"a", "b"}
	for _, name := range tenants {
		if _, err := leader.store.Create(store.NewTenant{Name: name, App: "web", Node: victim.name}, "alice"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := leader.store.PlaceReplicas("alice"); err != nil {
		t.Fatal(err)
	}
	replicas := map[string]string{}
	for _, name := range tenants {
		record, err := leader.store.Get(name)
		if err != nil || record.Replica == "" || record.Replica == victim.name {
			t.Fatalf("%s before: %+v, %v", name, record, err)
		}
		// Its owner reports the copy whole, as after its first send.
		if err := leader.store.ReplicaReady(name, record.Replica, victim.name); err != nil {
			t.Fatal(err)
		}
		replicas[name] = record.Replica
	}

	const after, settle = 2 * time.Second, 4 * time.Second
	fence := &fakeFence{fenced: map[string]time.Time{}}
	monitor := &failover.Monitor{
		Store: leader.store, Probe: leader.cluster.Ping, Leader: leader.cluster.Leader, Term: leader.cluster.Term,
		Fence: fence, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		After: after, Settle: settle, Interval: 250 * time.Millisecond,
	}
	run, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { monitor.Run(run); close(done) }()
	t.Cleanup(func() { stop(); <-done })

	// The monitor hears every node first, then the victim goes silent.
	eventually(t, "the monitor hears "+victim.name, func() bool { return leader.cluster.Ping(ctx(t), victim.name) == nil })
	time.Sleep(time.Second)
	lastAnswer := time.Now()
	victim.stop()

	eventually(t, victim.name+"'s tenants run elsewhere", func() bool {
		for _, name := range tenants {
			if record, err := leader.store.Get(name); err != nil || record.Node == victim.name {
				return false
			}
		}
		return true
	})
	promoted := time.Now()
	fenced, ok := fence.at(victim.name)
	if !ok {
		t.Fatal("the tenants ran elsewhere without a power-off")
	}
	// Fenced no sooner than after since its last answer, and promoted no
	// sooner than settle since then: the monitor's last answer from it is
	// at most this test's.
	if fenced.Sub(lastAnswer) < after-2*monitor.Interval {
		t.Fatalf("powered off %s after its last answer, before %s", fenced.Sub(lastAnswer), after)
	}
	if promoted.Before(fenced) || promoted.Sub(lastAnswer) < settle-2*monitor.Interval {
		t.Fatalf("promoted %s after its last answer (fenced at %s), before the settle of %s", promoted.Sub(lastAnswer), fenced.Sub(lastAnswer), settle)
	}
	survivors := slices.DeleteFunc(slices.Clone(nodes), func(n *node) bool { return n == victim })
	for _, n := range survivors {
		eventually(t, n.name+" sees the failover", func() bool {
			for _, name := range tenants {
				if record, err := n.store.Get(name); err != nil || record.Node != replicas[name] || !record.MoveHeld {
					return false
				}
			}
			return true
		})
		if failed := slices.ContainsFunc(n.store.Nodes(), func(node store.Node) bool { return node.Name == victim.name && node.Failed() }); !failed {
			t.Fatalf("%s does not see %s failed", n.name, victim.name)
		}
	}
	// Only the dead node was powered off.
	for _, n := range survivors {
		if _, ok := fence.at(n.name); ok {
			t.Fatalf("%s, which answers, was powered off", n.name)
		}
	}
}
