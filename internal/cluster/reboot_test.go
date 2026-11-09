package cluster

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/reboot"
	"github.com/PowerPenguini/murder/internal/store"
)

// stateMoves moves tenants as the state has them, without data: the
// handover a move makes once the data is across.
type stateMoves struct{ n *node }

func (m stateMoves) Receive(context.Context, string, bool, io.Reader) error { return nil }
func (m stateMoves) Discard(context.Context, string) error                  { return nil }
func (m stateMoves) Status(string) (any, bool)                              { return nil, false }
func (m stateMoves) Create(context.Context, store.NewTenant, string) (store.Record, error) {
	return store.Record{}, errors.New("not here")
}

func (m stateMoves) Move(_ context.Context, tenant, to, actor string) error {
	if _, err := m.n.store.StartMove(tenant, to, actor); err != nil {
		return err
	}
	_, err := m.n.store.FinishMove(tenant, actor)
	return err
}

// fakeReboot is a node's part in a reboot, which the test carries out:
// asked, it says so on asked; notReady refuses as a node whose units do
// not start at boot does.
type fakeReboot struct {
	mu       sync.Mutex
	notReady bool
	asked    chan reboot.Request
}

func (f *fakeReboot) Reboot(_ context.Context, request reboot.Request) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.notReady {
		return &reboot.NotReadyError{Problems: []string{"murder-agent.service is disabled"}}
	}
	if !request.Check {
		f.asked <- request
	}
	return nil
}

// A node of three is drained, asked to reboot through the cluster by
// another node, goes down and comes back as a reboot brings it (its agent
// started anew from its database and log), catches up with what the
// cluster did meanwhile, and takes its tenants back.
func TestADrainedNodeRebootsAndTakesItsTenantsBack(t *testing.T) {
	rebooter := &fakeReboot{asked: make(chan reboot.Request, 1)}
	withMoves := func(extra func(*node)) func(*node) {
		return func(n *node) {
			n.moves = stateMoves{n}
			if extra != nil {
				extra(n)
			}
		}
	}
	coordinator := newNodeWith(t, "fsn-1", withMoves(nil))
	if err := coordinator.cluster.Bootstrap(ctx(t), "alice", false); err != nil {
		t.Fatal(err)
	}
	rebooting := newNodeWith(t, "nbg-1", withMoves(func(n *node) { n.reboot = rebooter }))
	target := newNodeWith(t, "hel-1", withMoves(nil))
	nodes := []*node{coordinator, rebooting, target}
	for _, joiner := range nodes[1:] {
		if err := joiner.cluster.Join(ctx(t), coordinator.address, invite(t, coordinator, joiner), "alice"); err != nil {
			t.Fatalf("join %s: %v", joiner.name, err)
		}
	}
	for _, n := range nodes {
		eventually(t, n.name+" knows every node", func() bool { return len(n.store.Nodes()) == 3 })
	}
	if _, err := coordinator.store.PutApp(store.App{Name: "web", Spec: appSpec()}, "alice"); err != nil {
		t.Fatal(err)
	}
	tenants := []string{"acme", "shop"}
	for _, name := range tenants {
		if _, err := coordinator.store.Create(store.NewTenant{Name: name, App: "web", Node: "nbg-1"}, "alice"); err != nil {
			t.Fatal(err)
		}
	}
	// The node is asked whether it would come back before anything moves:
	// one whose units do not start at boot refuses, as a refusal.
	rebooter.mu.Lock()
	rebooter.notReady = true
	rebooter.mu.Unlock()
	var peer *PeerError
	if err := coordinator.cluster.RebootOn(ctx(t), "nbg-1", reboot.Request{Check: true}); !errors.As(err, &peer) || peer.Status != http.StatusUnprocessableEntity {
		t.Fatalf("a node that would not come back: %v", err)
	}
	rebooter.mu.Lock()
	rebooter.notReady = false
	rebooter.mu.Unlock()
	if err := coordinator.cluster.RebootOn(ctx(t), "nbg-1", reboot.Request{Check: true}); err != nil {
		t.Fatal(err)
	}
	// A node without a part in reboots says so.
	if err := rebooting.cluster.RebootOn(ctx(t), "hel-1", reboot.Request{Check: true}); !errors.As(err, &peer) || peer.Status != http.StatusNotImplemented {
		t.Fatalf("a node that takes no reboots: %v", err)
	}

	// Drain: the node that runs each tenant moves it, as asked from fsn-1.
	for _, name := range tenants {
		if err := coordinator.cluster.MoveOn(ctx(t), "nbg-1", name, "hel-1", "alice"); err != nil {
			t.Fatalf("move %s away: %v", name, err)
		}
	}
	eventually(t, "fsn-1 sees the tenants on hel-1", func() bool {
		for _, name := range tenants {
			if record, err := coordinator.store.Get(name); err != nil || record.Node != "hel-1" {
				return false
			}
		}
		return true
	})

	// Reboot: asked through the cluster, the node goes down.
	if err := coordinator.cluster.RebootOn(ctx(t), "nbg-1", reboot.Request{Away: 15 * time.Minute}); err != nil {
		t.Fatal(err)
	}
	select {
	case request := <-rebooter.asked:
		if request.Away != 15*time.Minute {
			t.Fatalf("asked %+v", request)
		}
	case <-time.After(eventuallyFor):
		t.Fatal("the node was not asked to reboot")
	}
	rebooting.stop()
	// Two of three answer: the cluster takes changes meanwhile.
	if _, err := coordinator.store.Create(store.NewTenant{Name: "late", App: "web", Node: "hel-1"}, "alice"); err != nil {
		t.Fatalf("a change while the node reboots: %v", err)
	}
	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := coordinator.cluster.Ping(pingCtx, "nbg-1"); err == nil {
		t.Fatal("the node answers while it reboots")
	}
	cancel()
	// Back: its agent starts anew from what it kept, and catches up.
	rebooting.start()
	eventually(t, "nbg-1 answers again", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return coordinator.cluster.Ping(ctx, "nbg-1") == nil
	})
	eventually(t, "nbg-1 caught up with the change made while it rebooted", func() bool {
		_, err := rebooting.store.Get("late")
		return err == nil
	})

	// Return: the tenants move back to it.
	for _, name := range tenants {
		if err := coordinator.cluster.MoveOn(ctx(t), "hel-1", name, "nbg-1", "alice"); err != nil {
			t.Fatalf("move %s back: %v", name, err)
		}
	}
	for _, n := range nodes {
		eventually(t, n.name+" sees the tenants back on nbg-1", func() bool {
			for _, name := range tenants {
				if record, err := n.store.Get(name); err != nil || record.Node != "nbg-1" {
					return false
				}
			}
			return true
		})
	}
	sameState(t, nodes)
}
