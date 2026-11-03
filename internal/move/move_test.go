package move

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/volume"
)

// target is the other node: it receives into a directory of its own.
type target struct {
	// snapshots are those of the copy the target keeps as replica.
	snapshots []string
	// snapshotsWait, if set, holds the snapshots' lookup until its
	// context ends.
	snapshotsWait bool
	volumes       volume.Dir
	received      []bool
	discarded     bool
	failReceive   error
	status        reconcile.Status
	// statusErr until moved, then status.
	st *store.SQLite
}

func (p *target) Receive(ctx context.Context, node, tenant string, incremental bool, data io.Reader) error {
	if p.failReceive != nil && len(p.received) > 0 == incremental {
		_, _ = io.Copy(io.Discard, data)
		return p.failReceive
	}
	p.received = append(p.received, incremental)
	return p.volumes.Receive(ctx, tenant, incremental, data)
}

func (p *target) Discard(ctx context.Context, node, tenant string) error {
	p.discarded = true
	return p.volumes.Remove(ctx, tenant)
}

func (p *target) Status(_ context.Context, node, tenant string) (reconcile.Status, error) {
	return p.status, nil
}

func (p *target) Snapshots(ctx context.Context, _, _ string) ([]string, error) {
	if p.snapshotsWait {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return p.snapshots, nil
}

type releaser struct {
	released []string
	fail     error
	// heldWhileReleased says whether requests were held when the tenant
	// stopped.
	st                *store.SQLite
	heldWhileReleased bool
}

func (r *releaser) Release(_ context.Context, tenant string) error {
	record, _ := r.st.Get(tenant)
	r.heldWhileReleased = record.MoveHeld
	r.released = append(r.released, tenant)
	return r.fail
}

type fixture struct {
	st       *store.SQLite
	source   volume.Dir
	target   *target
	releaser *releaser
	mover    *Mover
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "murder.db")
	if err := store.Init(path, "n1"); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(path, store.Options{NetworkPool: netip.MustParsePrefix("10.240.0.0/16"), Sealer: func(string, string, spec.Secret) (string, error) { return "sealed", nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.PutNode(store.Node{Name: "n2", Address: "10.0.0.2:7400"}, "test"); err != nil {
		t.Fatal(err)
	}
	app := spec.Tenant{Image: "sha256:" + strings.Repeat("a", 64), HealthPath: "/health", User: "1000:1000"}
	if _, err := st.PutApp(store.App{Name: "web", Spec: app}, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(store.NewTenant{Name: "acme", App: "web"}, "test"); err != nil {
		t.Fatal(err)
	}
	source := volume.Dir{Root: filepath.Join(dir, "n1")}
	data, err := source.Ensure(context.Background(), "acme", os.Getuid(), os.Getgid(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(data, "app.db"), []byte("the tenant's data"), 0o600); err != nil {
		t.Fatal(err)
	}
	f := &fixture{st: st, source: source, target: &target{volumes: volume.Dir{Root: filepath.Join(dir, "n2")}, status: reconcile.Status{Phase: reconcile.PhaseRunning, Healthy: true}}, releaser: &releaser{st: st}}
	f.mover = &Mover{Store: st, Reconciler: f.releaser, Volumes: source, Peer: f.target, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Healthy: time.Second}
	return f
}

func TestAMoveTakesTheDataAndTheTenant(t *testing.T) {
	f := newFixture(t)
	if err := f.mover.Move(context.Background(), "acme", "n2", "alice"); err != nil {
		t.Fatal(err)
	}
	// Requests wait until the new node serves it; its reconciler says so.
	record, _ := f.st.Get("acme")
	if record.Node != "n2" || record.MoveTo != "" || !record.MoveHeld {
		t.Fatalf("after the move: %+v", record)
	}
	data, err := os.ReadFile(filepath.Join(f.target.volumes.Root, "acme", "app.db"))
	if err != nil || string(data) != "the tenant's data" {
		t.Fatalf("data on the target: %q, %v", data, err)
	}
	if exists, _ := f.source.Exists(context.Background(), "acme"); exists {
		t.Fatal("the data stayed on the old node")
	}
	// Everything while it ran, then, held and stopped, the rest.
	if len(f.target.received) != 2 || f.target.received[0] || len(f.releaser.released) != 1 || !f.releaser.heldWhileReleased {
		t.Fatalf("received %v, released %v, held %v", f.target.received, f.releaser.released, f.releaser.heldWhileReleased)
	}
}

func TestAFailedMoveLeavesTheTenantWhereItWas(t *testing.T) {
	for name, fail := range map[string]func(f *fixture){
		"the first copy": func(f *fixture) { f.target.failReceive = errors.New("disk full") },
		"stopping":       func(f *fixture) { f.releaser.fail = errors.New("engine down") },
		"the last copy": func(f *fixture) {
			f.target.failReceive = errors.New("connection reset")
			f.target.received = nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			fail(f)
			if name == "the last copy" {
				// The first copy goes through; the second fails.
				f.target.failReceive = nil
				orig := f.target
				f.mover.Peer = &failSecond{target: orig}
			}
			if err := f.mover.Move(context.Background(), "acme", "n2", "alice"); err == nil {
				t.Fatal("the move succeeded")
			}
			record, _ := f.st.Get("acme")
			if record.Node != "n1" || record.MoveTo != "" || record.MoveHeld {
				t.Fatalf("after a failed move: %+v", record)
			}
			if exists, _ := f.source.Exists(context.Background(), "acme"); !exists {
				t.Fatal("a failed move lost the data")
			}
			if !f.target.discarded {
				t.Fatal("the target kept its partial copy")
			}
		})
	}
}

// failSecond lets the first copy through and fails the second.
type failSecond struct{ *target }

func (p *failSecond) Receive(ctx context.Context, node, tenant string, incremental bool, data io.Reader) error {
	if incremental || len(p.received) > 0 {
		_, _ = io.Copy(io.Discard, data)
		return errors.New("connection reset")
	}
	return p.target.Receive(ctx, node, tenant, incremental, data)
}

func TestAnUnhealthyTenantKeepsItsOldData(t *testing.T) {
	f := newFixture(t)
	f.target.status = reconcile.Status{Phase: reconcile.PhaseFailed, Message: "image missing"}
	err := f.mover.Move(context.Background(), "acme", "n2", "alice")
	if !errors.Is(err, ErrUnhealthy) || !strings.Contains(err.Error(), "image missing") {
		t.Fatalf("move: %v", err)
	}
	if record, _ := f.st.Get("acme"); record.Node != "n2" || record.MoveHeld {
		t.Fatalf("the tenant did not move, or requests still wait for it: %+v", record)
	}
	if exists, _ := f.source.Exists(context.Background(), "acme"); !exists {
		t.Fatal("the old node's copy went though the tenant is not healthy")
	}
}

func TestAnArchivedTenantMovesWithoutStopping(t *testing.T) {
	f := newFixture(t)
	if err := f.st.Archive("acme", "test"); err != nil {
		t.Fatal(err)
	}
	if err := f.mover.Move(context.Background(), "acme", "n2", "alice"); err != nil {
		t.Fatal(err)
	}
	if record, _ := f.st.Get("acme"); record.Node != "n2" || len(f.releaser.released) != 0 {
		t.Fatalf("after the move: %+v, released %v", record, f.releaser.released)
	}
}

// finishFails makes FinishMove report a failure after it was made, as a
// leader that lost its leadership in flight may.
type finishFails struct{ *store.SQLite }

func (s finishFails) FinishMove(name, actor string) (store.Record, error) {
	record, _ := s.SQLite.FinishMove(name, actor)
	return record, errors.New("leadership lost while committing log")
}

func TestAHandoverThatReportsAFailureButWasMadeIsKept(t *testing.T) {
	f := newFixture(t)
	f.mover.Store = finishFails{f.st}
	if err := f.mover.Move(context.Background(), "acme", "n2", "alice"); err != nil {
		t.Fatal(err)
	}
	if f.target.discarded {
		t.Fatal("the copy the new node runs was dropped")
	}
	if record, _ := f.st.Get("acme"); record.Node != "n2" {
		t.Fatalf("after the move: %+v", record)
	}
}

func TestAMoveCutShortIsUndone(t *testing.T) {
	f := newFixture(t)
	if _, err := f.st.StartMove("acme", "n2", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := f.st.HoldMove("acme"); err != nil {
		t.Fatal(err)
	}
	// As after an agent restart: no Move here is making it.
	if err := f.mover.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if record, _ := f.st.Get("acme"); record.Node != "n1" || record.MoveTo != "" || record.MoveHeld {
		t.Fatalf("after recovery: %+v", record)
	}
	if !f.target.discarded {
		t.Fatal("the target kept its partial copy")
	}
	// One under way is left alone.
	f.target.discarded = false
	if _, err := f.st.StartMove("acme", "n2", "alice"); err != nil {
		t.Fatal(err)
	}
	f.mover.begin("acme")
	if err := f.mover.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if record, _ := f.st.Get("acme"); record.MoveTo != "n2" || f.target.discarded {
		t.Fatal("recovery undid a move under way")
	}
}

// hangSecond lets the first copy through and hangs on the second, as a
// target switched off mid-copy would, until the move gives up.
type hangSecond struct{ *target }

func (p *hangSecond) Receive(ctx context.Context, node, tenant string, incremental bool, data io.Reader) error {
	if len(p.received) > 0 {
		<-ctx.Done()
		return ctx.Err()
	}
	return p.target.Receive(ctx, node, tenant, incremental, data)
}

// A target that hangs while the tenant runs nowhere does not keep it down:
// the move is undone once Away has passed, and the tenant runs here again.
func TestATargetThatHangsDoesNotKeepTheTenantDown(t *testing.T) {
	f := newFixture(t)
	f.mover.Peer = &hangSecond{target: f.target}
	f.mover.Away = 300 * time.Millisecond
	start := time.Now()
	if err := f.mover.Move(context.Background(), "acme", "n2", "alice"); err == nil {
		t.Fatal("the move succeeded")
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("the move gave up after %s", took)
	}
	record, _ := f.st.Get("acme")
	if record.Node != "n1" || record.MoveTo != "" || record.MoveHeld {
		t.Fatalf("after a move whose target hung: %+v", record)
	}
	if exists, _ := f.source.Exists(context.Background(), "acme"); !exists {
		t.Fatal("the data is gone")
	}
}

// listedDir is a directory driver that lists snapshots, as ZFS does.
type listedDir struct {
	volume.Dir
	snapshots []string
}

func (d listedDir) Snapshots(context.Context, string, string) ([]string, error) {
	return d.snapshots, nil
}

// A move onto the node that keeps the tenant's copy starts from the newest
// snapshot both have; onto another node, it sends everything.
func TestAMoveOntoTheReplicaStartsFromItsCopy(t *testing.T) {
	f := newFixture(t)
	if err := f.st.PutNode(store.Node{Name: "n1", Address: "10.0.0.1:7400"}, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.PlaceReplicas("test"); err != nil {
		t.Fatal(err)
	}
	if record, _ := f.st.Get("acme"); record.Replica != "n2" {
		t.Fatalf("replica %q", record.Replica)
	}
	f.target.snapshots = []string{"murder-replica-1", "murder-replica-2"}
	f.mover.Volumes = listedDir{Dir: f.source, snapshots: []string{"murder-replica-1", "murder-replica-2", "murder-replica-3"}}
	if base := f.mover.replicaBase(context.Background(), "acme", "n2"); base != "@murder-replica-2" {
		t.Fatalf("base %q", base)
	}
	if base := f.mover.replicaBase(context.Background(), "acme", "n3"); base != "" {
		t.Fatalf("base onto a node without the copy: %q", base)
	}
	f.target.snapshots = []string{"murder-replica-0"}
	if base := f.mover.replicaBase(context.Background(), "acme", "n2"); base != "" {
		t.Fatalf("base without one in common: %q", base)
	}
}

// A move given up while it asked the target for its copy's snapshots does
// not begin.
func TestAMoveGivenUpWhileAskingForTheCopyDoesNotBegin(t *testing.T) {
	f := newFixture(t)
	if err := f.st.PutNode(store.Node{Name: "n1", Address: "10.0.0.1:7400"}, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.PlaceReplicas("test"); err != nil {
		t.Fatal(err)
	}
	f.target.snapshotsWait = true
	f.mover.Volumes = listedDir{Dir: f.source}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := f.mover.Move(ctx, "acme", "n2", "test"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a move given up: %v", err)
	}
	events, err := f.st.Events(store.EventQuery{Tenant: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range events {
		if e.Action == "tenant.moving" {
			t.Fatal("the move began after it was given up")
		}
	}
	if len(f.target.received) > 0 {
		t.Fatal("data was sent")
	}
}
