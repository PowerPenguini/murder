package store

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/PowerPenguini/murder/internal/spec"
)

// rolling is a store with app web at revision 1 (image), tenants a, b, c
// and a held tenant h; web then changes to otherImage, in waves.
func rolling(t *testing.T) (*SQLite, AppChange) {
	t.Helper()
	st, _ := openNew(t, options())
	if _, err := st.PutApp(App{Name: "web", Spec: appSpec()}, "test"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b", "c", "h"} {
		if _, err := st.Create(NewTenant{Name: name, App: "web"}, "test"); err != nil {
			t.Fatal(err)
		}
	}
	hold := true
	if _, err := st.SetFlags("h", nil, &hold, "test"); err != nil {
		t.Fatal(err)
	}
	next := appSpec()
	next.Image = otherImage
	change, err := st.PutApp(App{Name: "web", Spec: next}, "bob")
	if err != nil {
		t.Fatal(err)
	}
	return st, change
}

// at names web's rollout as it is now.
func at(t *testing.T, st *SQLite) RolloutAt {
	t.Helper()
	rollout, err := st.Rollout("web")
	if err != nil {
		t.Fatal(err)
	}
	return rollout.At()
}

func TestAnAppChangeRollsOutInWaves(t *testing.T) {
	st, change := rolling(t)
	if !change.Rollout || !slices.Equal(change.Tenants, []string{"a", "b", "c"}) || change.App.Revision != 2 {
		t.Fatalf("change %+v", change)
	}
	// Nothing moved yet: every tenant runs revision 1, as it was.
	for _, name := range []string{"a", "b", "c", "h"} {
		record, _ := st.Get(name)
		if record.Spec.Image != image || record.AppRevision != 1 || record.Generation != 1 {
			t.Fatalf("%s before the first wave: %+v", name, record)
		}
	}
	rollout, err := st.Rollout("web")
	if err != nil || rollout.State != RolloutRunning || rollout.ToRevision != 2 || rollout.Wave != 0 {
		t.Fatalf("rollout %+v, %v", rollout, err)
	}
	// The first wave.
	first := at(t, st)
	if err := st.AdvanceRollout("web", first, []string{"a", "h"}, "murder"); err != nil {
		t.Fatal(err)
	}
	a, _ := st.Get("a")
	if a.Spec.Image != otherImage || a.AppRevision != 0 || a.BeforeRevision != 1 || a.Generation != 2 || a.MovedOn != st.Node() {
		t.Fatalf("a after its wave: %+v", a)
	}
	if h, _ := st.Get("h"); h.AppRevision != 1 || h.Spec.Image != image {
		t.Fatalf("a held tenant moved: %+v", h)
	}
	if b, _ := st.Get("b"); b.Spec.Image != image {
		t.Fatalf("b moved with a's wave: %+v", b)
	}
	// A leader that read wave 0 does not move it again.
	if err := st.AdvanceRollout("web", first, []string{"b"}, "murder"); !errors.Is(err, ErrConflict) {
		t.Fatalf("a stale wave: %v", err)
	}
	if err := st.AdvanceRollout("web", at(t, st), []string{"b"}, "murder"); err != nil {
		t.Fatal(err)
	}
	// Not done while c waits.
	if err := st.FinishRollout("web", at(t, st), "murder"); !errors.Is(err, ErrConflict) {
		t.Fatalf("done with c waiting: %v", err)
	}
	if err := st.AdvanceRollout("web", at(t, st), []string{"c"}, "murder"); err != nil {
		t.Fatal(err)
	}
	if err := st.FinishRollout("web", at(t, st), "murder"); err != nil {
		t.Fatal(err)
	}
	rollout, _ = st.Rollout("web")
	if rollout.State != RolloutDone || rollout.Wave != 3 {
		t.Fatalf("rollout %+v", rollout)
	}
	for _, name := range []string{"a", "b", "c"} {
		if record, _ := st.Get(name); record.Spec.Image != otherImage || record.BeforeRevision != 0 {
			t.Fatalf("%s after the rollout: %+v", name, record)
		}
	}
	events, _ := st.Events(EventQuery{App: "web", Limit: 1})
	if events[0].Action != "rollout.done" || events[0].Detail != "revision 2, in 3 waves" {
		t.Fatalf("events %+v", events)
	}
	// Released, the held tenant runs the newest revision.
	hold := false
	if h, err := st.SetFlags("h", nil, &hold, "alice"); err != nil || h.AppRevision != 0 || h.Spec.Image != otherImage {
		t.Fatalf("h released: %+v, %v", h, err)
	}
}

// Tenants a rollout moved go back with their data only as someone asks:
// those named, or every one it moved. The rollout pauses.
func TestRestoreTakesTenantsBackWithTheirData(t *testing.T) {
	st, _ := rolling(t)
	if err := st.AdvanceRollout("web", at(t, st), []string{"a", "b"}, "murder"); err != nil {
		t.Fatal(err)
	}
	before := at(t, st)
	// One it did not move, or of another app, is refused, and nothing
	// changes.
	for _, names := range [][]string{{"c"}, {"a", "c"}, {"h"}, {"nobody"}} {
		if _, err := st.RestoreTenants("web", names, "alice"); !errors.Is(err, ErrConflict) {
			t.Fatalf("restore %v: %v", names, err)
		}
	}
	if a, _ := st.Get("a"); a.AppRevision != 0 || a.Restore != "" || at(t, st) != before {
		t.Fatalf("a refused restore changed a %+v, or the rollout", a)
	}
	other := at(t, st)
	other.ToRevision++
	if err := st.PauseRolloutAt("web", other, "another rollout", "murder"); !errors.Is(err, ErrConflict) {
		t.Fatalf("a pause of another rollout: %v", err)
	}
	back, err := st.RestoreTenants("web", []string{"a"}, "alice")
	if err != nil || !slices.Equal(back, []string{"a"}) {
		t.Fatalf("restore: %v, %v", back, err)
	}
	a, _ := st.Get("a")
	if a.AppRevision != 1 || a.BeforeRevision != 0 || a.Spec.Image != image || a.Restore != image || a.Generation != 3 || !a.RestorePending() {
		t.Fatalf("a after going back: %+v", a)
	}
	rollout, _ := st.Rollout("web")
	if rollout.State != RolloutPaused || !strings.Contains(rollout.Reason, "a went back to the revision before, data too, as alice asked") || rollout.Epoch != before.Epoch+1 {
		t.Fatalf("rollout %+v", rollout)
	}
	if err := st.AdvanceRollout("web", at(t, st), []string{"c"}, "murder"); err == nil {
		t.Fatal("a paused rollout moved a tenant")
	}
	// Taken back, a is no longer one the rollout moved.
	if _, err := st.RestoreTenants("web", []string{"a"}, "alice"); !errors.Is(err, ErrConflict) {
		t.Fatalf("restored twice: %v", err)
	}
	// Its node restores the data, then says so.
	if err := st.Restored("a", "sha256:another"); err != nil {
		t.Fatal(err)
	}
	if a, _ := st.Get("a"); a.Restore != image {
		t.Fatal("a restore of another image was recorded")
	}
	if err := st.Restored("a", image); err != nil {
		t.Fatal(err)
	}
	if a, _ := st.Get("a"); a.Restore != "" || a.RestorePending() {
		t.Fatalf("restore %q", a.Restore)
	}
	// None named: every tenant it moved.
	if back, err := st.RestoreTenants("web", nil, "alice"); err != nil || !slices.Equal(back, []string{"b"}) {
		t.Fatalf("restore all: %v, %v", back, err)
	}
	if _, err := st.RestoreTenants("web", nil, "alice"); !errors.Is(err, ErrConflict) {
		t.Fatalf("nothing left to restore: %v", err)
	}
	// Abort leaves the data of those still waiting for it as it is.
	back, err = st.AbortRollout("web", "the image is broken", "alice")
	if err != nil || len(back) != 0 {
		t.Fatalf("abort: %v, %v", back, err)
	}
	if b, _ := st.Get("b"); b.Spec.Image != image || b.AppRevision != 1 || b.Restore != "" {
		t.Fatalf("b after the abort: %+v", b)
	}
	if rollout, _ := st.Rollout("web"); rollout.State != RolloutAborted {
		t.Fatalf("rollout %+v", rollout)
	}
	if _, err := st.RestoreTenants("web", nil, "alice"); !errors.Is(err, ErrConflict) {
		t.Fatalf("restore of an aborted rollout: %v", err)
	}
}

// Abort takes every tenant it moved back, their data as it is.
func TestAbortKeepsTheData(t *testing.T) {
	st, _ := rolling(t)
	if err := st.AdvanceRollout("web", at(t, st), []string{"a", "b"}, "murder"); err != nil {
		t.Fatal(err)
	}
	back, err := st.AbortRollout("web", "the image is broken", "alice")
	if err != nil || !slices.Equal(back, []string{"a", "b"}) {
		t.Fatalf("abort: %v, %v", back, err)
	}
	for _, name := range back {
		if record, _ := st.Get(name); record.Spec.Image != image || record.AppRevision != 1 || record.Restore != "" {
			t.Fatalf("%s after the abort: %+v", name, record)
		}
	}
}

func TestPauseResumeAndChangesAtOnce(t *testing.T) {
	st, _ := rolling(t)
	if err := st.PauseRollout("web", "looking at the logs", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.AdvanceRollout("web", at(t, st), []string{"a"}, "murder"); err == nil {
		t.Fatal("a paused rollout moved a tenant")
	}
	paused := at(t, st)
	if err := st.ResumeRollout("web", "alice"); err != nil {
		t.Fatal(err)
	}
	// What was decided before the resume is out of date.
	if err := st.AdvanceRollout("web", paused, []string{"a"}, "murder"); !errors.Is(err, ErrConflict) {
		t.Fatalf("a decision from before the resume: %v", err)
	}
	if err := st.AdvanceRollout("web", at(t, st), []string{"a"}, "murder"); err != nil {
		t.Fatal(err)
	}
	// A change at once moves every tenant now and ends the rollout; the
	// held one stays.
	third := appSpec()
	third.Env = map[string]string{"MODE": "prod"}
	change, err := st.PutApp(App{Name: "web", Spec: third, AllAtOnce: true}, "alice")
	if err != nil || change.Rollout || !slices.Equal(change.Tenants, []string{"a", "b", "c"}) {
		t.Fatalf("at once: %+v, %v", change, err)
	}
	for _, name := range []string{"a", "b", "c"} {
		if record, _ := st.Get(name); record.AppRevision != 0 || record.Spec.Env["MODE"] != "prod" {
			t.Fatalf("%s: %+v", name, record)
		}
	}
	if h, _ := st.Get("h"); h.AppRevision != 1 {
		t.Fatalf("the held tenant moved: %+v", h)
	}
	if rollout, _ := st.Rollout("web"); rollout.State != RolloutDone || !strings.Contains(rollout.Reason, "at once") {
		t.Fatalf("rollout %+v", rollout)
	}
}

func TestRevisionsTenantsRunAreKept(t *testing.T) {
	st, _ := rolling(t)
	// h runs revision 1; twenty-five changes later it is still there.
	for i := range revisionsKept + 5 {
		next := appSpec()
		next.Env = map[string]string{"N": strings.Repeat("x", i+1)}
		if _, err := st.PutApp(App{Name: "web", Spec: next, AllAtOnce: true}, "bob"); err != nil {
			t.Fatal(err)
		}
	}
	revisions, _ := st.Revisions("web")
	if revisions[len(revisions)-1].Revision != 1 {
		t.Fatalf("oldest kept %d", revisions[len(revisions)-1].Revision)
	}
	if h, _ := st.Get("h"); h.Spec.Image != image || h.AppRevision != 1 {
		t.Fatalf("h %+v", h)
	}
}

func TestAPinnedTenantKeepsItsRevisionWhenConfigured(t *testing.T) {
	st, _ := rolling(t)
	record, changed, err := st.Configure("a", "web", spec.Overrides{Env: map[string]string{"TZ": "UTC"}}, 0, "alice")
	if err != nil || !changed || record.AppRevision != 1 || record.Spec.Image != image || record.Spec.Env["TZ"] != "UTC" {
		t.Fatalf("configured %+v, %v", record, err)
	}
	if err := st.Archive("a", "alice"); err != nil {
		t.Fatal(err)
	}
	restored, err := st.Restore("a", "alice")
	if err != nil || restored.AppRevision != 0 || restored.Spec.Image != otherImage {
		t.Fatalf("restored %+v, %v", restored, err)
	}
}

func TestDataGoesBackOnlyWhereItsSnapshotIs(t *testing.T) {
	st, _ := rolling(t)
	if err := st.PutNode(Node{Name: st.Node(), Address: "10.0.0.1:7400"}, "test"); err != nil {
		t.Fatal(err)
	}
	if err := st.PutNode(Node{Name: "n2", Address: "10.0.0.2:7400"}, "test"); err != nil {
		t.Fatal(err)
	}
	if err := st.AdvanceRollout("web", at(t, st), []string{"a"}, "murder"); err != nil {
		t.Fatal(err)
	}
	// a moved to n2 after its wave: n2 has no snapshot from before.
	if _, err := st.StartMove("a", "n2", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.FinishMove("a", "test"); err != nil {
		t.Fatal(err)
	}
	// Its data cannot go back: its revision stays too.
	if _, err := st.RestoreTenants("web", []string{"a"}, "alice"); !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "no snapshot of its data from before") {
		t.Fatalf("a moved since its wave: %v", err)
	}
	if _, err := st.RestoreTenants("web", nil, "alice"); !errors.Is(err, ErrConflict) {
		t.Fatalf("every tenant, a among them: %v", err)
	}
	if a, _ := st.Get("a"); a.Restore != "" || a.AppRevision != 0 {
		t.Fatalf("a moved since its wave: %+v", a)
	}
	// Nor can one moving now.
	if err := st.AdvanceRollout("web", at(t, st), []string{"b"}, "murder"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.StartMove("b", "n2", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.RestoreTenants("web", []string{"b"}, "alice"); !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "is moving") {
		t.Fatalf("b moving: %v", err)
	}
}

// A rollout that did not change a tenant's image took no snapshot of its
// data as it moved: there is nothing to go back to.
func TestDataGoesBackOnlyIfTheImageChanged(t *testing.T) {
	st, _ := openNew(t, options())
	if _, err := st.PutApp(App{Name: "web", Spec: appSpec()}, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(NewTenant{Name: "a", App: "web"}, "test"); err != nil {
		t.Fatal(err)
	}
	next := appSpec()
	next.Env = map[string]string{"MODE": "new"}
	if change, err := st.PutApp(App{Name: "web", Spec: next}, "bob"); err != nil || !change.Rollout {
		t.Fatalf("change %+v, %v", change, err)
	}
	if err := st.AdvanceRollout("web", at(t, st), []string{"a"}, "murder"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.RestoreTenants("web", nil, "alice"); !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "did not change tenant a's image") {
		t.Fatalf("restore: %v", err)
	}
}

func TestTheSameSpecAtOnceMovesTheRest(t *testing.T) {
	st, _ := rolling(t)
	if err := st.AdvanceRollout("web", at(t, st), []string{"a"}, "murder"); err != nil {
		t.Fatal(err)
	}
	app, _ := st.App("web")
	change, err := st.PutApp(App{Name: "web", Spec: app.Spec, Tag: app.Tag, AllAtOnce: true}, "alice")
	if err != nil || change.Changed || !slices.Equal(change.Tenants, []string{"b", "c"}) {
		t.Fatalf("change %+v, %v", change, err)
	}
	if rollout, _ := st.Rollout("web"); rollout.State != RolloutDone {
		t.Fatalf("rollout %+v", rollout)
	}
	if h, _ := st.Get("h"); h.AppRevision != 1 {
		t.Fatal("the held tenant moved")
	}
	// Again: nothing.
	if change, err := st.PutApp(App{Name: "web", Spec: app.Spec, Tag: app.Tag, AllAtOnce: true}, "alice"); err != nil || change.Changed || len(change.Tenants) != 0 {
		t.Fatalf("again %+v, %v", change, err)
	}
}

func TestAnAppWithARolloutIsDeleted(t *testing.T) {
	st, _ := rolling(t)
	for _, name := range []string{"a", "b", "c", "h"} {
		if err := st.Archive(name, "test"); err != nil {
			t.Fatal(err)
		}
		if err := st.Forget(name, "test"); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.DeleteApp("web", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Rollout("web"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("its rollout stayed: %v", err)
	}
}
