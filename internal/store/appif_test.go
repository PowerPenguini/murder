package store

import (
	"errors"
	"testing"
	"time"
)

// A conditional store refuses, changing nothing, when the app is not as
// read: two editors do not overwrite each other.
func TestStoringAnAppOnlyAsRead(t *testing.T) {
	st, _ := withApp(t, options())
	if _, err := st.PutAppIf(App{Name: "web", Spec: appSpec()}, AppIf{Absent: true}, "anna"); !errors.Is(err, ErrConflict) {
		t.Fatalf("created over an app of the name: %v", err)
	}
	read, err := st.App("web")
	if err != nil {
		t.Fatal(err)
	}
	changed := appSpec()
	changed.HealthPath = "/ready"
	if _, err := st.PutAppIf(App{Name: "web", Spec: changed}, AppIf{UpdatedAt: read.UpdatedAt.Add(-time.Second)}, "anna"); !errors.Is(err, ErrConflict) {
		t.Fatalf("stored over a change made since: %v", err)
	}
	if now, _ := st.App("web"); now.Spec.HealthPath != "/health" {
		t.Fatalf("a refused store changed the app: %+v", now.Spec)
	}
	change, err := st.PutAppIf(App{Name: "web", Spec: changed}, AppIf{UpdatedAt: read.UpdatedAt}, "jan")
	if err != nil || !change.Changed {
		t.Fatalf("storing over the app as read: %+v, %v", change, err)
	}
	// Then the version read is stale.
	changed.HealthPath = "/live"
	if _, err := st.PutAppIf(App{Name: "web", Spec: changed}, AppIf{UpdatedAt: read.UpdatedAt}, "anna"); !errors.Is(err, ErrConflict) {
		t.Fatalf("stored over jan's change: %v", err)
	}
	if _, err := st.PutAppIf(App{Name: "new", Spec: appSpec()}, AppIf{UpdatedAt: read.UpdatedAt}, "anna"); !errors.Is(err, ErrConflict) {
		t.Fatalf("stored an app that was not read: %v", err)
	}
	if change, err := st.PutAppIf(App{Name: "new", Spec: appSpec()}, AppIf{Absent: true}, "anna"); err != nil || !change.Created {
		t.Fatalf("creating a new app: %+v, %v", change, err)
	}
}
