package panel

import (
	"testing"

	"github.com/PowerPenguini/murder/internal/store"
)

// A tenant's settings event says its own settings as people read them.
func TestOverridesSaid(t *testing.T) {
	for _, c := range []struct{ detail, want string }{
		{"overrides: env FEATURE_PLANNER,TZ; secrets ADMIN_PASSWORD; limits memoryMB=768 cpus=0 pids=0 diskGB=2",
			"its own: env FEATURE_PLANNER, TZ; secret ADMIN_PASSWORD; memory 768 MB, disk 2 GB"},
		{"app web → testapp; overrides: secrets A,B; limits memoryMB=0 cpus=0.5 pids=0 diskGB=0",
			"app web → testapp; its own: secrets A, B; CPUs 0.5"},
		{"overrides: image ghcr.io/x:1", "its own: image ghcr.io/x:1"},
		{"no overrides", "no overrides"},
	} {
		if got := overridesSaid(c.detail, "tenant.updated"); got != c.want {
			t.Errorf("%q said %q, want %q", c.detail, got, c.want)
		}
	}
	if got := overridesSaid("limits memoryMB=1 cpus=0 pids=0 diskGB=0", "app.updated"); got != "limits memoryMB=1 cpus=0 pids=0 diskGB=0" {
		t.Errorf("another action's detail changed: %q", got)
	}
}

// An events line says what a settings change changed; its opened record
// says the whole set.
func TestBriefSaysTheChange(t *testing.T) {
	for _, c := range []struct{ detail, want string }{
		{"changes: disk limit 2 GB → 10 GB; overrides: env FEATURE_PLANNER,TZ; secrets ADMIN_PASSWORD; limits memoryMB=768 cpus=0 pids=0 diskGB=10", "disk limit 2 GB → 10 GB"},
		{"app web → testapp; overrides: env TZ", "app web → testapp"},
		{"overrides: env TZ", "its own: env TZ"},
	} {
		e := store.Event{Action: "tenant.updated", Detail: c.detail}
		if got := groupEvents([]store.Event{e})[0].Brief(); got != c.want {
			t.Errorf("%q: brief %q, want %q", c.detail, got, c.want)
		}
	}
}
