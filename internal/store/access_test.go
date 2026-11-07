package store

import (
	"errors"
	"testing"
	"time"
)

// An access is recorded while changes are frozen, as of the first schema
// version of batches, so every node applies it during an upgrade; it is
// found again by its ID, and listed with the tenant's and the copies of the
// whole state, not with other tenants'.
func TestAccessesAreRecordedFrozenOrNot(t *testing.T) {
	st, _ := withApp(t, options())
	entries := logged(t, st)
	if err := st.Freeze("upgrade to abc", "alice"); err != nil {
		t.Fatal(err)
	}
	first, err := st.Access("acme", "access.logs", `{"purpose":"export fails","ticket":"ZD-1"}`, "jan via root")
	if err != nil {
		t.Fatalf("an access while frozen: %v", err)
	}
	if last := (*entries)[len(*entries)-1]; last.Schema != firstBatchSchema {
		t.Fatalf("an access is written as of schema version %d", last.Schema)
	}
	if first.ID == 0 || first.Action != "access.logs" || first.Tenant != "acme" || first.Actor != "jan via root" || first.At.IsZero() {
		t.Fatalf("recorded: %+v", first)
	}
	if err := st.Thaw("alice"); err != nil {
		t.Fatal(err)
	}
	state, err := st.Access("", "access.state", `{"purpose":"restore drill","ticket":"incident"}`, "root")
	if err != nil {
		t.Fatal(err)
	}
	other, err := st.Access("globex", "access.secret", `{"what":"DB","purpose":"x","ticket":"legal"}`, "root")
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
	if _, err := st.Access("acme", "tenant.updated", "", "root"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("another action through Access: %v", err)
	}
	if _, err := st.Access("acme", "access.", "", "root"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("an access of no kind: %v", err)
	}
}
