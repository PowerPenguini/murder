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
	// stuck tenants' nodes answer with the result of a check made at
	// stuckAt, again and again, their checks held up.
	stuck   map[string]bool
	stuckAt map[string]time.Time
	// skew is how far the tenants' node's clock is from the driver's.
	skew time.Duration
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
	f := &fixture{store: st, clock: time.Now(), sick: map[string]bool{}, unknown: map[string]bool{}, restarts: map[string]int{}, stuck: map[string]bool{}, stuckAt: map[string]time.Time{}}
	f.driver = &Driver{Store: st, Now: func() time.Time { return f.clock }, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Status: func(_ context.Context, record store.Record) (reconcile.Status, bool) {
			name := record.Spec.Name
			if f.unknown[name] {
				return reconcile.Status{}, false
			}
			checked := f.clock.Add(f.skew)
			if f.stuck[name] {
				checked = f.stuckAt[name]
			}
			return reconcile.Status{Phase: reconcile.PhaseRunning, Healthy: !f.sick[name], Generation: record.Generation, ContainerID: "c-" + name + "-" + record.Spec.Image[7:12], Restarts: f.restarts[name], CheckedAt: checked}, true
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

// stays reports that the tenant is still on the newest revision, its data
// left as it is: the driver takes nothing back itself.
func (f *fixture) stays(t *testing.T, name string) {
	t.Helper()
	record, err := f.store.Get(name)
	if err != nil {
		t.Fatal(err)
	}
	if record.AppRevision != 0 || record.BeforeRevision == 0 || record.Restore != "" {
		t.Fatalf("%s went back: revision %d, before %d, restore %q", name, record.AppRevision, record.BeforeRevision, record.Restore)
	}
}

// pausedFor reports that the rollout is paused, naming what and the two
// ways on.
func (f *fixture) pausedFor(t *testing.T, what string) {
	t.Helper()
	rollout := f.rollout(t)
	if rollout.State != store.RolloutPaused || !strings.Contains(rollout.Reason, what) {
		t.Fatalf("rollout %+v", rollout)
	}
	for _, way := range []string{"`murder app rollout abort web`", "`murder app rollout restore web`", "losing what they wrote since"} {
		if !strings.Contains(rollout.Reason, way) {
			t.Fatalf("the reason does not say %s: %s", way, rollout.Reason)
		}
	}
}

// A tenant that does not come up pauses the rollout, for someone to take
// it back as it is, or with its data.
func TestATenantThatNeverComesUpPausesTheRollout(t *testing.T) {
	f := newFixture(t, 3)
	f.sick["t00"] = true
	f.step(time.Second)
	f.step(5 * time.Second)
	f.step(4 * time.Minute)
	if rollout := f.rollout(t); rollout.State != store.RolloutRunning {
		t.Fatalf("gave up early: %+v", rollout)
	}
	f.step(2 * time.Minute)
	f.pausedFor(t, "t00 did not come up healthy within 5m0s")
	f.stays(t, "t00")
	if got := f.onNewest(); !slices.Equal(got, []string{"t00"}) {
		t.Fatalf("on the newest: %v", got)
	}
	// Taken back with its data, as someone asks: the rollout stays paused.
	back, err := f.store.RestoreTenants("web", nil, "alice")
	if err != nil || !slices.Equal(back, []string{"t00"}) {
		t.Fatalf("restored %v, %v", back, err)
	}
	t00, _ := f.store.Get("t00")
	if t00.AppRevision != 1 || t00.Restore != imageA || t00.Spec.Image != imageA {
		t.Fatalf("t00 %+v", t00)
	}
	f.step(time.Minute)
	if rollout := f.rollout(t); rollout.State != store.RolloutPaused {
		t.Fatalf("rollout %+v", rollout)
	}
}

// A node that has not started the new revision for long pauses it too.
func TestATenantItsNodeDoesNotStartPausesTheRollout(t *testing.T) {
	f := newFixture(t, 3)
	f.driver.Status = func(_ context.Context, record store.Record) (reconcile.Status, bool) {
		return reconcile.Status{Phase: reconcile.PhaseRunning, Healthy: true, Generation: record.Generation - 1, CheckedAt: f.clock}, true
	}
	f.step(time.Second)
	f.step(6 * time.Minute)
	f.pausedFor(t, "its node has not started the new revision")
	f.stays(t, "t00")
}

func TestATenantThatBreaksAfterComingUpPausesTheRollout(t *testing.T) {
	f := newFixture(t, 3)
	f.step(time.Second)
	f.step(time.Second) // up and healthy
	// It restarts: paused at once, nothing goes back.
	f.restarts["t00"] = 1
	f.step(time.Second)
	f.pausedFor(t, "t00 restarted after it came up")
	f.stays(t, "t00")
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
	f.pausedFor(t, "t00 fails its health check after it came up")
	f.stays(t, "t00")
}

func TestANodeThatDoesNotAnswerPausesWithoutGoingBack(t *testing.T) {
	f := newFixture(t, 3)
	f.unknown["t00"] = true
	f.step(time.Second)
	f.step(6 * time.Minute)
	f.pausedFor(t, "t00's node has not said how it runs for 5m0s")
	f.stays(t, "t00")
}

// A node that stops answering late in a wave has as long as at its start
// before the rollout pauses.
func TestANodeThatStopsAnsweringHasItsTime(t *testing.T) {
	f := newFixture(t, 3)
	f.driver.CanarySoak = 10 * time.Minute
	f.step(time.Second)
	f.step(6 * time.Minute)
	f.unknown["t00"] = true
	f.step(4 * time.Minute)
	if rollout := f.rollout(t); rollout.State != store.RolloutRunning {
		t.Fatalf("paused early: %+v", rollout)
	}
	f.step(2 * time.Minute)
	f.pausedFor(t, "t00's node has not said how it runs")
}

func TestOnlyTheLeaderDrives(t *testing.T) {
	f := newFixture(t, 3)
	f.driver.Leader = func() bool { return false }
	f.step(time.Second)
	if got := f.onNewest(); len(got) != 0 {
		t.Fatalf("a follower moved %v", got)
	}
}

// A new leader knows nothing of what the old one saw: a canary that fails
// under it pauses the rollout all the same, nothing taken back.
func TestANewLeaderPausesTooWithoutTakingBack(t *testing.T) {
	f := newFixture(t, 10, "t07")
	f.step(time.Second)
	f.step(5 * time.Second)
	f.step(2*time.Minute + time.Second)
	if got := f.onNewest(); len(got) != 2 {
		t.Fatalf("second wave %v", got)
	}
	f.driver.watches = nil
	f.sick["t07"] = true
	f.step(time.Second)
	f.step(6 * time.Minute)
	f.pausedFor(t, "t07 did not come up healthy")
	f.stays(t, "t07")
}

// A tenant not looked at for long is paused for all the same, nothing
// taken back.
func TestATenantNotWatchedAllAlongPausesToo(t *testing.T) {
	f := newFixture(t, 3)
	f.sick["t00"] = true
	f.step(time.Second)
	f.jump(6 * time.Minute)
	f.pausedFor(t, "t00 did not come up healthy")
	f.stays(t, "t00")
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

// Paused and resumed, a tenant's soak starts over: the time paused was not
// watched.
func TestAPauseStartsTheSoakOver(t *testing.T) {
	f := newFixture(t, 3)
	f.step(time.Second)
	f.step(time.Minute)
	if err := f.store.PauseRollout("web", "a look", "alice"); err != nil {
		t.Fatal(err)
	}
	f.step(10 * time.Second)
	if err := f.store.ResumeRollout("web", "alice"); err != nil {
		t.Fatal(err)
	}
	f.step(90 * time.Second)
	if got := f.onNewest(); len(got) != 1 {
		t.Fatalf("moved on before a whole soak since the resume: %v", got)
	}
	f.step(time.Minute)
	if got := f.onNewest(); len(got) != 2 {
		t.Fatalf("not moved on after a whole soak: %v", got)
	}
}

// A healthy result read again and again, its node's checks held up, is
// no soak: the next wave waits.
func TestTheSameResultReadAgainIsNoSoak(t *testing.T) {
	f := newFixture(t, 3)
	f.step(time.Second)
	f.step(5 * time.Second)
	f.stuck["t00"], f.stuckAt["t00"] = true, f.clock
	f.step(3 * time.Minute)
	if got := f.onNewest(); len(got) != 1 {
		t.Fatalf("moved on with no fresh check: %v", got)
	}
}

// The tenants' node's clock need not agree with the leader's: a rollout
// goes through with it a minute behind.
func TestARolloutGoesThroughWithClocksApart(t *testing.T) {
	for _, skew := range []time.Duration{-time.Minute, 2 * time.Minute} {
		f := newFixture(t, 3)
		f.skew = skew
		for range 12 {
			f.step(time.Minute)
		}
		if rollout := f.rollout(t); rollout.State != store.RolloutDone {
			t.Fatalf("skew %v: rollout %+v", skew, rollout)
		}
	}
}
