package clusterops

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
)

// TakeLease, RenewLease and ReleaseLease refuse as the store and the API
// do: one lease at a time, none while changes are frozen.
func (a *fakeAgent) TakeLease(_ context.Context, kind, detail string) (store.Lease, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch {
	case a.frozen != "":
		return store.Lease{}, &api.Error{Status: http.StatusConflict, Message: "changes are frozen: " + a.frozen}
	case a.lease != "":
		return store.Lease{}, &api.Error{Status: http.StatusConflict, Message: "conflicting change: another operation changes the cluster: " + a.leaseKind + ", begun by alice on node n2"}
	}
	a.leases++
	a.lease, a.leaseKind = fmt.Sprintf("lease-%d", a.leases), kind+" "+detail
	return store.Lease{ID: a.lease, Kind: kind, Detail: detail}, nil
}

func (a *fakeAgent) RenewLease(_ context.Context, id string) (store.Lease, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.renewals++
	if a.lostAfter > 0 && a.renewals >= a.lostAfter && a.lease == id {
		a.lease = "" // a person broke it
	}
	if a.lease != id {
		return store.Lease{}, &api.Error{Status: http.StatusGone, Message: "conflicting change: the operation no longer holds the cluster"}
	}
	return store.Lease{ID: id}, nil
}

func (a *fakeAgent) ReleaseLease(_ context.Context, id string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.lease == id {
		a.lease = ""
		a.released++
	}
	return nil
}

// A drain holds the lease while it moves, and lets go of it once done,
// failed or not.
func TestADrainHoldsTheLeaseWhileItMoves(t *testing.T) {
	agent := newFakeAgent(old3())
	agent.tenants = []api.Tenant{tenant("a", "n2", false), tenant("b", "n2", false)}
	if err := Drain(context.Background(), agent, "n2", "n3", io.Discard); err != nil {
		t.Fatal(err)
	}
	if agent.leases != 1 || agent.released != 1 || agent.lease != "" || !slices.Equal(agent.moved, []string{"a", "b"}) {
		t.Fatalf("leases %d, released %d, held %q, moved %v", agent.leases, agent.released, agent.lease, agent.moved)
	}
	agent.failMove = map[string]bool{"a": true}
	agent.tenants = []api.Tenant{tenant("a", "n3", false)}
	if err := Drain(context.Background(), agent, "n3", "n1", io.Discard); err == nil {
		t.Fatal("a drain whose move failed succeeded")
	}
	if agent.lease != "" || agent.released != 2 {
		t.Fatalf("a failed drain kept the lease: held %q, released %d", agent.lease, agent.released)
	}
}

// While one operation holds the cluster, another one is refused before it
// changes anything, and is told whose it is; so is an upgrade's freeze.
func TestAnOperationIsRefusedWhileAnotherHoldsTheCluster(t *testing.T) {
	agent := newFakeAgent(old3())
	agent.tenants = []api.Tenant{tenant("a", "n1", false), tenant("b", "n1", false), tenant("c", "n1", false), tenant("d", "n2", false)}
	held, err := agent.TakeLease(context.Background(), "reboot", "node n2")
	if err != nil {
		t.Fatal(err)
	}
	for name, run := range map[string]func() error{
		"drain":   func() error { return Drain(context.Background(), agent, "n1", "n3", io.Discard) },
		"balance": func() error { return Balance(context.Background(), agent, false, io.Discard) },
		"replace": func() error {
			return Replace(context.Background(), agent, func(context.Context, string) error { return errors.New("removed") }, "n1", "n3", io.Discard)
		},
	} {
		err := run()
		if err == nil || !strings.Contains(err.Error(), "another operation changes the cluster: reboot node n2, begun by alice") || !strings.Contains(err.Error(), "nothing was changed") {
			t.Fatalf("%s: %v", name, err)
		}
	}
	path, _ := binary(t)
	if err := Upgrade(context.Background(), agent, path, time.Second, io.Discard); err == nil || !strings.Contains(err.Error(), "another operation changes the cluster") {
		t.Fatalf("upgrade: %v", err)
	}
	// A move made beside it, not under its lease, is refused too.
	if _, err := agent.Move(context.Background(), "a", "n3"); err == nil {
		t.Fatal("a move beside the lease was taken")
	}
	if len(agent.moved) != 0 || agent.lease != held.ID || agent.frozen != "" {
		t.Fatalf("moved %v, lease %q, frozen %q", agent.moved, agent.lease, agent.frozen)
	}
	// A dry run changes nothing, and takes no lease.
	var out bytes.Buffer
	if err := Balance(context.Background(), agent, true, &out); err != nil || !strings.Contains(out.String(), "would move") {
		t.Fatalf("dry run: %v\n%s", err, out.String())
	}
}

// Changes frozen for an upgrade refuse the lease, so no drain runs beside
// it.
func TestAFreezeRefusesTheLease(t *testing.T) {
	agent := newFakeAgent(old3())
	agent.frozen = store.RotationFreeze + "n1"
	agent.tenants = []api.Tenant{tenant("a", "n2", false)}
	err := Drain(context.Background(), agent, "n2", "n3", io.Discard)
	if err == nil || !strings.Contains(err.Error(), "changes are frozen: "+store.RotationFreeze) || len(agent.moved) != 0 {
		t.Fatalf("drain: %v, moved %v", err, agent.moved)
	}
}

// A replace drains and removes under one lease: the removal acts under
// the drain's lease, which no other operation takes in between.
func TestAReplaceHoldsOneLeaseThroughout(t *testing.T) {
	agent := newFakeAgent(old3())
	agent.tenants = []api.Tenant{tenant("a", "n2", false)}
	var removedUnder string
	remove := func(ctx context.Context, node string) error {
		agent.mu.Lock()
		defer agent.mu.Unlock()
		if api.LeaseOf(ctx) != agent.lease {
			return errors.New("removed outside the lease")
		}
		removedUnder = agent.lease
		return nil
	}
	if err := Replace(context.Background(), agent, remove, "n2", "n3", io.Discard); err != nil {
		t.Fatal(err)
	}
	if agent.leases != 1 || removedUnder != "lease-1" || agent.lease != "" {
		t.Fatalf("leases %d, removed under %q, held %q", agent.leases, removedUnder, agent.lease)
	}
}

// An operation whose lease is broken stops at its next request, and says
// why; its holder renews it while it runs.
func TestAnOperationStopsOnceItsLeaseIsBroken(t *testing.T) {
	defer func(every time.Duration) { renewEvery = every }(renewEvery)
	renewEvery = 10 * time.Millisecond
	agent := newFakeAgent(old3())
	agent.lostAfter = 3
	agent.tenants = []api.Tenant{tenant("a", "n2", false), tenant("b", "n2", false)}
	client := slowAgent{agent, 100 * time.Millisecond}
	err := Drain(context.Background(), client, "n2", "n3", io.Discard)
	if err == nil || !strings.Contains(err.Error(), "lost the cluster's operation lease") {
		t.Fatalf("drain: %v", err)
	}
	if agent.renewals < 3 || len(agent.moved) != 1 {
		t.Fatalf("renewals %d, moved %v", agent.renewals, agent.moved)
	}
}

// A drain that runs long renews its lease and keeps it.
func TestALongOperationRenewsItsLease(t *testing.T) {
	defer func(every time.Duration) { renewEvery = every }(renewEvery)
	renewEvery = 10 * time.Millisecond
	agent := newFakeAgent(old3())
	agent.tenants = []api.Tenant{tenant("a", "n2", false), tenant("b", "n2", false)}
	if err := Drain(context.Background(), slowAgent{agent, 60 * time.Millisecond}, "n2", "n3", io.Discard); err != nil {
		t.Fatal(err)
	}
	if agent.renewals < 4 || agent.released != 1 {
		t.Fatalf("renewals %d, released %d", agent.renewals, agent.released)
	}
}

// slowAgent takes its time over each move, after it moved the tenant.
type slowAgent struct {
	*fakeAgent
	delay time.Duration
}

func (a slowAgent) MoveFrom(ctx context.Context, tenant, from, to string) (api.Tenant, error) {
	moved, err := a.fakeAgent.MoveFrom(ctx, tenant, from, to)
	if err != nil {
		return moved, err
	}
	select {
	case <-time.After(a.delay):
	case <-ctx.Done():
	}
	return moved, err
}
