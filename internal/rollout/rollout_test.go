package rollout

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/netip"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

const (
	imageA = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	imageB = "sha256:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
)

// fixture is a store with app web and its tenants, a clock, and the
// status each tenant's node reports: healthy unless told otherwise.
type fixture struct {
	store    *store.SQLite
	driver   *Driver
	clock    time.Time
	sick     map[string]bool
	unknown  map[string]bool
	restarts map[string]int
}

func newFixture(t *testing.T, tenants int, canaries ...string) *fixture {
	t.Helper()
	path := filepath.Join(t.TempDir(), "murder.db")
	if err := store.Init(path, "n1"); err != nil {
		t.Fatal(err)
	}
	var sealed atomic.Int64
	st, err := store.Open(path, store.Options{NetworkPool: netip.MustParsePrefix("10.240.0.0/16"), Sealer: func(tenant, name string, _ spec.Secret) (string, error) {
		return fmt.Sprintf("%s/%s#%d", tenant, name, sealed.Add(1)), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	app := spec.Tenant{Image: imageA, User: "1000:1000"}
	if _, err := st.PutApp(store.App{Name: "web", Spec: app}, "test"); err != nil {
		t.Fatal(err)
	}
	for i := range tenants {
		if _, err := st.Create(store.NewTenant{Name: fmt.Sprintf("t%02d", i), App: "web"}, "test"); err != nil {
			t.Fatal(err)
		}
	}
	yes := true
	for _, name := range canaries {
		if _, err := st.SetFlags(name, &yes, nil, "test"); err != nil {
			t.Fatal(err)
		}
	}
	f := &fixture{store: st, clock: time.Now(), sick: map[string]bool{}, unknown: map[string]bool{}, restarts: map[string]int{}}
	f.driver = &Driver{Store: st, Now: func() time.Time { return f.clock }, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Status: func(_ context.Context, record store.Record) (reconcile.Status, bool) {
			name := record.Spec.Name
			if f.unknown[name] {
				return reconcile.Status{}, false
			}
			return reconcile.Status{Phase: reconcile.PhaseRunning, Healthy: !f.sick[name], Generation: record.Generation, ContainerID: "c-" + name + "-" + record.Spec.Image[7:12], Restarts: f.restarts[name]}, true
		}}
	app.Image = imageB
	if change, err := st.PutApp(store.App{Name: "web", Spec: app}, "alice"); err != nil || !change.Rollout {
		t.Fatalf("change %+v, %v", change, err)
	}
	return f
}

// step moves the clock on, the driver looking every five seconds as it
// does.
func (f *fixture) step(d time.Duration) {
	for d > 0 {
		next := min(d, 5*time.Second)
		f.clock, d = f.clock.Add(next), d-next
		f.driver.Step(context.Background())
	}
}

// jump moves the clock on with the driver not looking meanwhile.
func (f *fixture) jump(d time.Duration) {
	f.clock = f.clock.Add(d)
	f.driver.Step(context.Background())
}

func (f *fixture) onNewest() []string {
	var names []string
	for _, record := range f.store.List() {
		if record.AppRevision == 0 {
			names = append(names, record.Spec.Name)
		}
	}
	return names
}

func (f *fixture) rollout(t *testing.T) store.Rollout {
	t.Helper()
	rollout, err := f.store.Rollout("web")
	if err != nil {
		t.Fatal(err)
	}
	return rollout
}

func TestWavesMoveCanariesThenShares(t *testing.T) {
	f := newFixture(t, 10, "t07")
	f.step(time.Second)
	if got := f.onNewest(); !slices.Equal(got, []string{"t07"}) {
		t.Fatalf("first wave %v", got)
	}
	// The canary soaks two minutes: not through after one.
	f.step(time.Second)
	f.step(time.Minute)
	if got := f.onNewest(); len(got) != 1 {
		t.Fatalf("moved before the canary soaked: %v", got)
	}
	f.step(time.Minute + time.Second)
	if got := f.onNewest(); !slices.Equal(got, []string{"t00", "t07"}) {
		t.Fatalf("second wave %v", got)
	}
	// Then up to half, then all, a minute each.
	f.step(time.Second)
	f.step(time.Minute + time.Second)
	if got := f.onNewest(); len(got) != 5 {
		t.Fatalf("third wave %v", got)
	}
	f.step(time.Second)
	f.step(time.Minute + time.Second)
	if got := f.onNewest(); len(got) != 10 {
		t.Fatalf("fourth wave %v", got)
	}
	f.step(time.Second)
	f.step(time.Minute + time.Second)
	if rollout := f.rollout(t); rollout.State != store.RolloutDone || rollout.Wave != 4 {
		t.Fatalf("rollout %+v", rollout)
	}
}

func TestATenantThatNeverComesUpGoesBackWithItsData(t *testing.T) {
	f := newFixture(t, 3)
	f.sick["t00"] = true
	f.step(time.Second)
	f.step(5 * time.Second) // watched from its move on
	f.step(4 * time.Minute)
	if rollout := f.rollout(t); rollout.State != store.RolloutRunning {
		t.Fatalf("gave up early: %+v", rollout)
	}
	f.step(2 * time.Minute)
	rollout := f.rollout(t)
	if rollout.State != store.RolloutPaused || !strings.Contains(rollout.Reason, "t00 did not come up") {
		t.Fatalf("rollout %+v", rollout)
	}
	t00, _ := f.store.Get("t00")
	if t00.AppRevision != 1 || t00.Restore != imageA || t00.Spec.Image != imageA {
		t.Fatalf("t00 %+v", t00)
	}
	if got := f.onNewest(); len(got) != 0 {
		t.Fatalf("on the newest: %v", got)
	}
}

func TestATenantThatBreaksAfterComingUpPausesTheRollout(t *testing.T) {
	f := newFixture(t, 3)
	f.step(time.Second)
	f.step(time.Second) // up and healthy
	// It restarts: paused at once, nothing goes back.
	f.restarts["t00"] = 1
	f.step(time.Second)
	rollout := f.rollout(t)
	if rollout.State != store.RolloutPaused || !strings.Contains(rollout.Reason, "t00 restarted") {
		t.Fatalf("rollout %+v", rollout)
	}
	if t00, _ := f.store.Get("t00"); t00.AppRevision != 0 || t00.Restore != "" {
		t.Fatalf("t00 went back: %+v", t00)
	}
}

func TestAShortFailureIsNotABreak(t *testing.T) {
	f := newFixture(t, 3)
	f.step(time.Second)
	f.step(time.Second)
	f.sick["t00"] = true
	f.step(10 * time.Second)
	f.sick["t00"] = false
	f.step(10 * time.Second)
	if rollout := f.rollout(t); rollout.State != store.RolloutRunning {
		t.Fatalf("a failed check paused it: %+v", rollout)
	}
	// Failing longer pauses it.
	f.sick["t00"] = true
	f.step(time.Second)
	f.step(31 * time.Second)
	if rollout := f.rollout(t); rollout.State != store.RolloutPaused || !strings.Contains(rollout.Reason, "fails its health check") {
		t.Fatalf("rollout %+v", rollout)
	}
}

func TestANodeThatDoesNotAnswerPausesWithoutGoingBack(t *testing.T) {
	f := newFixture(t, 3)
	f.unknown["t00"] = true
	f.step(time.Second)
	f.step(6 * time.Minute)
	rollout := f.rollout(t)
	if rollout.State != store.RolloutPaused || !strings.Contains(rollout.Reason, "does not say how it runs") {
		t.Fatalf("rollout %+v", rollout)
	}
	if t00, _ := f.store.Get("t00"); t00.Restore != "" || t00.AppRevision != 0 {
		t.Fatalf("t00 %+v", t00)
	}
}

func TestOnlyTheLeaderDrives(t *testing.T) {
	f := newFixture(t, 3)
	f.driver.Leader = func() bool { return false }
	f.step(time.Second)
	if got := f.onNewest(); len(got) != 0 {
		t.Fatalf("a follower moved %v", got)
	}
}

// A canary that served, then fails once the next wave moved, pauses the
// rollout: its data, written while it served, stays.
func TestACanaryThatServedNeverLosesItsData(t *testing.T) {
	f := newFixture(t, 10, "t07")
	f.step(time.Second)
	f.step(5 * time.Second)
	f.step(2*time.Minute + time.Second)
	if got := f.onNewest(); len(got) != 2 {
		t.Fatalf("second wave %v", got)
	}
	if t07, _ := f.store.Get("t07"); !t07.Served {
		t.Fatal("the canary is not known to have served")
	}
	// A new leader knows nothing but what the store says.
	f.driver.watches = nil
	f.driver.Leader = func() bool { return true }
	f.sick["t07"] = true
	f.step(time.Second)
	f.step(6 * time.Minute)
	rollout := f.rollout(t)
	t07, _ := f.store.Get("t07")
	if rollout.State != store.RolloutPaused || t07.Restore != "" || t07.AppRevision != 0 {
		t.Fatalf("rollout %+v, t07 %+v", rollout, t07)
	}
}

// A tenant this driver did not watch from its move on may have served:
// it pauses the rollout, its data as it is.
func TestATenantNotWatchedAllAlongKeepsItsData(t *testing.T) {
	f := newFixture(t, 3)
	f.sick["t00"] = true
	f.step(time.Second)
	f.jump(6 * time.Minute) // looked at again long after it moved
	rollout := f.rollout(t)
	t00, _ := f.store.Get("t00")
	if rollout.State != store.RolloutPaused || !strings.Contains(rollout.Reason, "did not watch it all along") || t00.Restore != "" || t00.AppRevision != 0 {
		t.Fatalf("rollout %+v, t00 %+v", rollout, t00)
	}
}

func TestSuspendedTenantsDoNotHoldARolloutUp(t *testing.T) {
	f := newFixture(t, 2)
	if _, err := f.store.SetSuspended("t00", true, "test"); err != nil {
		t.Fatal(err)
	}
	f.unknown["t00"] = true // nothing runs, nothing answers
	for range 8 {
		f.step(2*time.Minute + time.Second)
	}
	if rollout := f.rollout(t); rollout.State != store.RolloutDone {
		t.Fatalf("rollout %+v", rollout)
	}
}

// A container first seen after it restarted is no healthy soak.
func TestAContainerSeenFirstAfterARestartIsNotThrough(t *testing.T) {
	f := newFixture(t, 2)
	f.restarts["t00"] = 1
	f.step(time.Second)
	// Its soak starts at the look after the one that saw the restart.
	f.step(2 * time.Minute)
	if got := f.onNewest(); len(got) != 1 {
		t.Fatalf("moved on after a restart: %v", got)
	}
	f.step(30 * time.Second)
	if got := f.onNewest(); len(got) != 2 {
		t.Fatalf("not moved on after a whole soak: %v", got)
	}
}

// Paused and resumed, a tenant was not watched meanwhile: it may have
// served, so its data stays.
func TestAPauseEndsWhatTheDriverKnows(t *testing.T) {
	f := newFixture(t, 3)
	f.sick["t00"] = true
	f.step(time.Second)
	f.step(10 * time.Second)
	if err := f.store.PauseRollout("web", "a look", "alice"); err != nil {
		t.Fatal(err)
	}
	f.step(10 * time.Second)
	if err := f.store.ResumeRollout("web", "alice"); err != nil {
		t.Fatal(err)
	}
	f.step(6 * time.Minute)
	rollout := f.rollout(t)
	t00, _ := f.store.Get("t00")
	if rollout.State != store.RolloutPaused || !strings.Contains(rollout.Reason, "did not watch it all along") || t00.Restore != "" || t00.AppRevision != 0 {
		t.Fatalf("rollout %+v, t00 restore %q revision %d", rollout, t00.Restore, t00.AppRevision)
	}
}

// One that moved to another node since its wave keeps its revision and
// data: that node has no snapshot of before.
func TestATenantMovedSinceItsWaveKeepsItsRevision(t *testing.T) {
	f := newFixture(t, 3)
	if err := f.store.PutNode(store.Node{Name: "n1", Address: "10.0.0.1:7400"}, "test"); err != nil {
		t.Fatal(err)
	}
	if err := f.store.PutNode(store.Node{Name: "n2", Address: "10.0.0.2:7400"}, "test"); err != nil {
		t.Fatal(err)
	}
	f.sick["t00"] = true
	f.step(time.Second)
	if _, err := f.store.StartMove("t00", "n2", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.FinishMove("t00", "test"); err != nil {
		t.Fatal(err)
	}
	f.step(6 * time.Minute)
	rollout := f.rollout(t)
	t00, _ := f.store.Get("t00")
	if rollout.State != store.RolloutPaused || !strings.Contains(rollout.Reason, "moved since its wave") || t00.Restore != "" || t00.AppRevision != 0 {
		t.Fatalf("rollout %+v, t00 restore %q revision %d", rollout, t00.Restore, t00.AppRevision)
	}
}
