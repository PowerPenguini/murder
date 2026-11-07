package store

import (
	"errors"
	"testing"
	"time"
)

// An access is recorded as of the first schema version of batches, so
// every node applies it during an upgrade, but not while changes are
// frozen: a rollback of the upgrade would lose it. It is found again by its
// ID, and listed with the tenant's and the copies of the whole state, not
// with other tenants'.
func TestAccessesAreRecordedUnlessFrozen(t *testing.T) {
	st, _ := withApp(t, options())
	entries := logged(t, st)
	if err := st.Freeze("upgrade to abc", "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Access("acme", "access.logs", `{"purpose":"export fails","ticket":"ZD-1"}`, "jan via root", false); !errors.Is(err, ErrFrozen) {
		t.Fatalf("an access while frozen: %v", err)
	}
	if err := st.Thaw("alice"); err != nil {
		t.Fatal(err)
	}
	first, err := st.Access("acme", "access.logs", `{"purpose":"export fails","ticket":"ZD-1"}`, "jan via root", false)
	if err != nil {
		t.Fatal(err)
	}
	if last := (*entries)[len(*entries)-1]; last.Schema != firstBatchSchema {
		t.Fatalf("an access is written as of schema version %d", last.Schema)
	}
	if first.ID == 0 || first.Action != "access.logs" || first.Tenant != "acme" || first.Actor != "jan via root" || first.At.IsZero() {
		t.Fatalf("recorded: %+v", first)
	}
	state, err := st.Access("", "access.state", `{"purpose":"restore drill","ticket":"incident"}`, "root", false)
	if err != nil {
		t.Fatal(err)
	}
	other, err := st.Access("globex", "access.secret", `{"what":"DB","purpose":"x","ticket":"legal"}`, "root", false)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := st.Event(first.ID); err != nil || got != first {
		t.Fatalf("found again: %+v %v (recorded %+v)", got, err, first)
	}
	if _, err := st.Event(other.ID + 100); !errors.Is(err, ErrNotFound) {
		t.Fatalf("an event that is not: %v", err)
	}
	ids := func(events []Event) (out []int64) {
		for _, e := range events {
			out = append(out, e.ID)
		}
		return out
	}
	if got, err := st.Accesses("acme", time.Time{}); err != nil || len(got) != 2 || got[0].ID != first.ID || got[1].ID != state.ID {
		t.Fatalf("acme's accesses: %v %v", ids(got), err)
	}
	if got, _ := st.Accesses("", time.Time{}); len(got) != 3 {
		t.Fatalf("every access: %v", ids(got))
	}
	if got, _ := st.Accesses("acme", state.At); len(got) != 1 || got[0].ID != state.ID {
		t.Fatalf("acme's since the state's copy: %v", ids(got))
	}
	// Other events are not accesses.
	if got, _ := st.Accesses("", time.Time{}); got[0].Action == "changes.frozen" {
		t.Fatal("a freeze listed as an access")
	}
	if _, err := st.Access("acme", "tenant.updated", "", "root", false); !errors.Is(err, ErrInvalid) {
		t.Fatalf("another action through Access: %v", err)
	}
	if _, err := st.Access("acme", "access.", "", "root", false); !errors.Is(err, ErrInvalid) {
		t.Fatalf("an access of no kind: %v", err)
	}
}

// An access recorded once is recorded once, however often it is sent:
// a racing writer's batch is stale, and computed again, finding it.
func TestAnAccessRecordedOnceIsOne(t *testing.T) {
	st, _ := withApp(t, options())
	detail := `{"purpose":"x","ticket":"incident","offline":"REC1"}`
	first, err := st.Access("acme", "access.logs", detail, "jan via root", true)
	if err != nil {
		t.Fatal(err)
	}
	again, err := st.Access("acme", "access.logs", detail, "anna via root", true)
	if err != nil || again.ID != first.ID || again.Actor != "jan via root" {
		t.Fatalf("again: %+v %v", again, err)
	}
	// Another node's write of it lands between this one's computation and
	// its commit: the commit is stale, and the write finds it.
	original := st.commit
	raced := false
	st.commit = func(b Batch) (*snapshot, error) {
		if !raced {
			raced = true
			if _, err := original(Batch{ID: "other", Schema: firstBatchSchema, Version: b.Version, Statements: b.Statements}); err != nil {
				t.Fatal(err)
			}
		}
		return original(b)
	}
	other := `{"purpose":"x","ticket":"incident","offline":"REC2"}`
	got, err := st.Access("acme", "access.logs", other, "root", true)
	if err != nil {
		t.Fatal(err)
	}
	if all, _ := st.Accesses("acme", time.Time{}); len(all) != 2 || got.ID != all[1].ID {
		t.Fatalf("a raced access recorded twice: %d (%+v)", len(all), got)
	}
}
