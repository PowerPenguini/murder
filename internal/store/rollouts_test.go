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
	if err := st.AdvanceRollout("web", 0, []string{"a", "h"}, "murder"); err != nil {
		t.Fatal(err)
	}
	a, _ := st.Get("a")
	if a.Spec.Image != otherImage || a.AppRevision != 0 || a.BeforeRevision != 1 || a.Generation != 2 {
		t.Fatalf("a after its wave: %+v", a)
	}
	if h, _ := st.Get("h"); h.AppRevision != 1 || h.Spec.Image != image {
		t.Fatalf("a held tenant moved: %+v", h)
	}
	if b, _ := st.Get("b"); b.Spec.Image != image {
		t.Fatalf("b moved with a's wave: %+v", b)
	}
	// A leader that read wave 0 does not move it again.
	if err := st.AdvanceRollout("web", 0, []string{"b"}, "murder"); !errors.Is(err, ErrConflict) {
		t.Fatalf("a stale wave: %v", err)
	}
	if err := st.AdvanceRollout("web", 1, []string{"b", "c"}, "murder"); err != nil {
		t.Fatal(err)
	}
	if err := st.FinishRollout("web", 2, "murder"); err != nil {
		t.Fatal(err)
	}
	rollout, _ = st.Rollout("web")
	if rollout.State != RolloutDone || rollout.Wave != 2 {
		t.Fatalf("rollout %+v", rollout)
	}
	for _, name := range []string{"a", "b", "c"} {
		if record, _ := st.Get(name); record.Spec.Image != otherImage || record.BeforeRevision != 0 {
			t.Fatalf("%s after the rollout: %+v", name, record)
		}
	}
	events, _ := st.Events(EventQuery{App: "web", Limit: 1})
	if events[0].Action != "rollout.done" || events[0].Detail != "revision 2, in 2 waves" {
		t.Fatalf("events %+v", events)
	}
	// Released, the held tenant runs the newest revision.
	hold := false
	if h, err := st.SetFlags("h", nil, &hold, "alice"); err != nil || h.AppRevision != 0 || h.Spec.Image != otherImage {
		t.Fatalf("h released: %+v, %v", h, err)
	}
}

func TestAFailedTenantGoesBackWithItsData(t *testing.T) {
	st, _ := rolling(t)
	if err := st.AdvanceRollout("web", 0, []string{"a", "b"}, "murder"); err != nil {
		t.Fatal(err)
	}
	if err := st.RollBackTenants("web", []string{"a"}, true, "a never became healthy", "murder"); err != nil {
		t.Fatal(err)
	}
	a, _ := st.Get("a")
	if a.AppRevision != 1 || a.Spec.Image != image || a.Restore != image || a.Generation != 3 {
		t.Fatalf("a after going back: %+v", a)
	}
	rollout, _ := st.Rollout("web")
	if rollout.State != RolloutPaused || rollout.Reason != "a never became healthy" {
		t.Fatalf("rollout %+v", rollout)
	}
	if err := st.AdvanceRollout("web", 1, []string{"c"}, "murder"); err == nil {
		t.Fatal("a paused rollout moved a tenant")
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
	if a, _ := st.Get("a"); a.Restore != "" {
		t.Fatalf("restore %q", a.Restore)
	}
	// Abort takes b back too; its data stays.
	back, err := st.AbortRollout("web", "the image is broken", "alice")
	if err != nil || !slices.Equal(back, []string{"b"}) {
		t.Fatalf("abort: %v, %v", back, err)
	}
	if b, _ := st.Get("b"); b.Spec.Image != image || b.AppRevision != 1 || b.Restore != "" {
		t.Fatalf("b after the abort: %+v", b)
	}
	if rollout, _ := st.Rollout("web"); rollout.State != RolloutAborted {
		t.Fatalf("rollout %+v", rollout)
	}
}

func TestPauseResumeAndChangesAtOnce(t *testing.T) {
	st, _ := rolling(t)
	if err := st.PauseRollout("web", "looking at the logs", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.AdvanceRollout("web", 0, []string{"a"}, "murder"); err == nil {
		t.Fatal("a paused rollout moved a tenant")
	}
	if err := st.ResumeRollout("web", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.AdvanceRollout("web", 0, []string{"a"}, "murder"); err != nil {
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
