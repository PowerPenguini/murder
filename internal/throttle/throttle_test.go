package throttle

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestAWarningThatRepeatsIsLoggedOnceAMinute(t *testing.T) {
	var out bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&out, nil))
	at := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	limit := &Log{now: func() time.Time { return at }}
	lines := func() []map[string]any {
		var entries []map[string]any
		for line := range strings.SplitSeq(strings.TrimSpace(out.String()), "\n") {
			if line == "" {
				continue
			}
			var entry map[string]any
			if err := json.Unmarshal([]byte(line), &entry); err != nil {
				t.Fatal(err)
			}
			entries = append(entries, entry)
		}
		out.Reset()
		return entries
	}

	// A peer that tries again every second for two minutes.
	for range 60 {
		limit.Warn(log, "n3", "refused a node that is not a member", "node", "n3")
		at = at.Add(time.Second)
	}
	got := lines()
	if len(got) != 1 || got[0]["level"] != "WARN" || got[0]["node"] != "n3" || got[0]["suppressed"] != nil {
		t.Fatalf("the first minute logged %v", got)
	}
	limit.Warn(log, "n3", "refused a node that is not a member", "node", "n3")
	if got := lines(); len(got) != 1 || got[0]["suppressed"] != float64(59) {
		t.Fatalf("a minute later logged %v", got)
	}
	// Another source, or another reason, is logged at once.
	limit.Warn(log, "n4", "refused a node that is not a member")
	limit.Warn(log, "n3", "refused a node of another cluster")
	if got := lines(); len(got) != 2 {
		t.Fatalf("other sources and reasons logged %v", got)
	}
}

func TestTheWarningsRememberedAreBounded(t *testing.T) {
	at := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	limit := &Log{now: func() time.Time { return at }}
	for i := range 3 * kept {
		limit.allow(fmt.Sprintf("10.0.0.%d", i), "refused")
		at = at.Add(time.Second)
	}
	if len(limit.seen) > kept {
		t.Fatalf("%d remembered", len(limit.seen))
	}
	// As many sources at once as it remembers: the next is left out
	// until the interval passes.
	for i := range kept {
		limit.allow(fmt.Sprintf("10.1.0.%d", i), "refused")
	}
	if _, ok := limit.allow("10.2.0.1", "refused"); ok || len(limit.seen) > kept {
		t.Fatalf("a flood of sources: %d remembered", len(limit.seen))
	}
	at = at.Add(Every)
	if _, ok := limit.allow("10.2.0.1", "refused"); !ok {
		t.Fatal("a new source is not logged once the flood has passed")
	}
}
