package reconcile

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/runtime"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

// What is kept is the last lines, at most PreviousLines of them within
// PreviousBytes, whole lines only.
func TestPreviousKeepsTheLastLinesWithinBounds(t *testing.T) {
	var many bytes.Buffer
	for i := 1; i <= 1500; i++ {
		fmt.Fprintf(&many, "line %d\n", i)
	}
	kept, cut := bound(many.Bytes())
	lines := strings.Split(strings.TrimSuffix(string(kept), "\n"), "\n")
	if !cut || len(lines) != PreviousLines || lines[0] != "line 501" || lines[len(lines)-1] != "line 1500" {
		t.Fatalf("by lines: cut %v, %d lines, %q to %q", cut, len(lines), lines[0], lines[len(lines)-1])
	}
	long := bytes.Repeat([]byte(strings.Repeat("x", 199)+"\n"), 1000) // 200 KB in 1000 lines
	kept, cut = bound(long)
	if !cut || len(kept) > PreviousBytes || len(kept)%200 != 0 || !bytes.HasSuffix(kept, []byte("x\n")) {
		t.Fatalf("by bytes: cut %v, %d bytes", cut, len(kept))
	}
	if kept, cut := bound([]byte("one\ntwo\n")); cut || string(kept) != "one\ntwo\n" {
		t.Fatalf("within bounds: %q, cut %v", kept, cut)
	}
}

func previousFixture(t *testing.T) *fixture {
	t.Helper()
	f := newFixture(t)
	f.reconciler.config.PreviousDir = filepath.Join(t.TempDir(), "previous")
	f.runtime.logs = func(id string, _ int) ([]byte, error) {
		return []byte("start " + id + "\nERROR cannot allocate memory\n"), nil
	}
	return f
}

// When the app exits, its lines up to the exit are kept with the exit,
// privately, before its container is started again; the new run's lines
// begin after that exit.
func TestAnExitKeepsTheContainersLastLines(t *testing.T) {
	f := previousFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	id := f.runtime.only(t).ID
	if _, err := f.reconciler.Previous("acme"); !errors.Is(err, ErrNoPrevious) {
		t.Fatalf("before any exit: %v", err)
	}
	at := time.Now().UTC()
	f.runtime.mu.Lock()
	f.runtime.containers[id].Running, f.runtime.containers[id].State = false, "exited"
	f.runtime.state = runtime.ContainerState{ExitKnown: true, ExitCode: 137, OOMKilled: true, ExitedAt: at}
	f.runtime.mu.Unlock()
	f.runtime.takeEvents()
	f.reconcile(t)
	kept, err := f.reconciler.Previous("acme")
	if err != nil {
		t.Fatal(err)
	}
	if kept.Why != "exited" || kept.Container != id || kept.ExitCode != 137 || !kept.OOMKilled || !kept.ExitedAt.Equal(at) ||
		kept.Image != imageA || kept.MemoryMB != 256 || !strings.Contains(string(kept.Lines), "cannot allocate memory") {
		t.Fatalf("kept = %+v", kept)
	}
	if events := f.runtime.takeEvents(); !slices.Contains(events, "start "+id) {
		t.Fatalf("events = %v, want it started again", events)
	}
	if since := f.reconciler.LinesSince("acme", id); !since.Equal(at) {
		t.Fatalf("the current run's lines begin %v, want %v", since, at)
	}
	if since := f.reconciler.LinesSince("acme", "another"); !since.IsZero() {
		t.Fatalf("another container's lines begin %v", since)
	}
	info, err := os.Stat(filepath.Join(f.reconciler.config.PreviousDir, "acme.json"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("the kept file: %v, %v", info, err)
	}
	// The same exit, seen again on the next pass, keeps nothing new.
	f.runtime.logs = func(string, int) ([]byte, error) { return []byte("later\n"), nil }
	f.reconcile(t)
	if again, _ := f.reconciler.Previous("acme"); !again.At.Equal(kept.At) {
		t.Fatalf("kept again for one exit: %+v", again)
	}
	// The exit is in the audit log once, with the limit it ran under.
	events, err := f.store.Events(store.EventQuery{Tenant: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	var exits []string
	for _, e := range events {
		if e.Action == "tenant.restarted" {
			exits = append(exits, e.Actor+": "+e.Detail)
		}
	}
	if !slices.Equal(exits, []string{"murder: exit 137: killed for using more than its 256 MB of memory"}) {
		t.Fatalf("the audit log's exits: %q", exits)
	}
}

// A replaced container's lines are kept as it goes, with its own image; a
// suspended tenant's as its container is removed.
func TestAReplacementOrSuspensionKeepsTheLastLines(t *testing.T) {
	f := previousFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	old := f.runtime.only(t).ID
	f.runtime.mu.Lock()
	f.runtime.images[imageB] = true
	f.runtime.mu.Unlock()
	f.updateApp(t, "acme", func(app *spec.Tenant) { app.Image = imageB })
	f.reconcile(t)
	kept, err := f.reconciler.Previous("acme")
	if err != nil || kept.Why != "replaced" || kept.Container != old || kept.Image != imageA || kept.ExitKnown || !strings.Contains(string(kept.Lines), "start "+old) {
		t.Fatalf("after the image change: %+v, %v", kept, err)
	}
	current := f.runtime.only(t).ID
	if since := f.reconciler.LinesSince("acme", current); !since.IsZero() {
		t.Fatalf("a new container's lines are all its own, yet begin %v", since)
	}
	f.suspend(t, "acme", true)
	f.reconcile(t)
	if kept, err := f.reconciler.Previous("acme"); err != nil || kept.Why != "stopped" || kept.Container != current {
		t.Fatalf("after suspension: %+v, %v", kept, err)
	}
}

// A tenant this node no longer has takes what was kept with it.
func TestAPurgedTenantsLinesAreForgotten(t *testing.T) {
	f := previousFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	f.suspend(t, "acme", true)
	f.reconcile(t)
	if _, err := f.reconciler.Previous("acme"); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Archive("acme", "test"); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	if _, err := f.reconciler.Previous("acme"); err != nil {
		t.Fatalf("an archived tenant keeps its lines: %v", err)
	}
	if err := f.reconciler.Purge(t.Context(), "acme", "test"); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	if _, err := f.reconciler.Previous("acme"); !errors.Is(err, ErrNoPrevious) {
		t.Fatalf("after the purge: %v", err)
	}
}

// Keeping the lines never holds up the container: a failed read is
// logged, and the app is started again all the same. With no place to
// keep them, none are.
func TestKeepingTheLinesNeverHoldsUpTheContainer(t *testing.T) {
	f := previousFixture(t)
	f.runtime.logs = func(string, int) ([]byte, error) { return nil, errors.New("the engine is busy") }
	f.create(t, "acme")
	f.reconcile(t)
	id := f.runtime.only(t).ID
	f.runtime.mu.Lock()
	f.runtime.containers[id].Running, f.runtime.containers[id].State = false, "exited"
	f.runtime.state = runtime.ContainerState{ExitKnown: true, ExitCode: 1, ExitedAt: time.Now()}
	f.runtime.mu.Unlock()
	f.runtime.takeEvents()
	f.reconcile(t)
	if events := f.runtime.takeEvents(); !slices.Contains(events, "start "+id) {
		t.Fatalf("events = %v, want it started again", events)
	}
	if _, err := f.reconciler.Previous("acme"); !errors.Is(err, ErrNoPrevious) {
		t.Fatalf("kept after a failed read: %v", err)
	}
	f.reconciler.config.PreviousDir = ""
	if _, err := f.reconciler.Previous("acme"); !errors.Is(err, ErrNoPrevious) {
		t.Fatalf("kept with nowhere to keep it: %v", err)
	}
}

// An exit is said by its code, and a kill with the limit it ran under.
func TestExitDetail(t *testing.T) {
	for _, c := range []struct {
		state runtime.ContainerState
		want  string
	}{
		{runtime.ContainerState{ExitKnown: true, ExitCode: 137, OOMKilled: true}, "exit 137: killed for using more than its 256 MB of memory"},
		{runtime.ContainerState{ExitKnown: true, ExitCode: 137}, "exit 137: killed (SIGKILL), most likely for using more than its 256 MB of memory"},
		{runtime.ContainerState{ExitKnown: true, ExitCode: 2}, "exit 2"},
		{runtime.ContainerState{}, "exited; its code is not known"},
	} {
		if got := ExitDetail(c.state, 256); got != c.want {
			t.Errorf("ExitDetail(%+v) = %q, want %q", c.state, got, c.want)
		}
	}
}
