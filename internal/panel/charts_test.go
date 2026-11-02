package panel

import (
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/store"
)

// Events fall in clock hours, the last the one now runs in; columns are
// scaled to the busiest.
func TestActivityBucketsByTheClock(t *testing.T) {
	now := time.Date(2026, 11, 2, 17, 25, 0, 0, time.UTC)
	at := func(h, m int) store.Event { return store.Event{At: time.Date(2026, 11, 2, h, m, 0, 0, time.UTC)} }
	events := []store.Event{at(17, 20), at(17, 1), at(16, 59), at(15, 0), at(15, 30), at(15, 45), {At: now.Add(-30 * time.Hour)}}
	a := activityOf(events, now, 24*time.Hour, time.Hour, true, hourLabel)
	if len(a.Columns) != 24 || a.Total != 6 || a.Max != 3 || a.Partial {
		t.Fatalf("activity: %d columns, %d events, max %d, partial %v", len(a.Columns), a.Total, a.Max, a.Partial)
	}
	last := a.Columns[23]
	if last.N != 2 || !last.Now || last.Title != "17:00: 2 events" || a.Columns[22].N != 1 || a.Columns[21].N != 3 || a.Columns[21].H != activityHeight {
		t.Fatalf("the last hours: %+v %+v %+v", a.Columns[21], a.Columns[22], last)
	}
	if a.From != "18:00" {
		t.Fatalf("from %s", a.From)
	}
	// Read only from 15:30 on, the hours before are not said to be quiet.
	partial := activityOf(events[:5], now, 24*time.Hour, time.Hour, false, hourLabel)
	if !partial.Partial || !partial.Columns[20].Unknown || partial.Columns[21].Unknown || partial.Columns[20].Title != "14:00: not read" {
		t.Fatalf("partial: %+v", partial.Columns[20:22])
	}
}

// A part at zero draws nothing; the others share the width, apart.
func TestSegmentedBar(t *testing.T) {
	bar := newSegmentedBar([]segment{{Label: "a", N: 1}, {Label: "b", N: 0}, {Label: "c", N: 3}})
	if bar.Total != 4 || bar.Parts[1].W != 0 || bar.Parts[0].W+bar.Parts[2].W+segmentGap != chartWidth || bar.Parts[2].X != bar.Parts[0].W+segmentGap {
		t.Fatalf("bar: %+v", bar)
	}
	if empty := newSegmentedBar([]segment{{Label: "a"}}); empty.Total != 0 || empty.Parts[0].W != 0 {
		t.Fatalf("empty: %+v", empty)
	}
}

func TestMemory(t *testing.T) {
	for mb, want := range map[int]string{256: "256 MB", 1024: "1 GB", 1536: "1.5 GB", 1300: "1.3 GB"} {
		if got := memory(mb); got != want {
			t.Errorf("memory(%d) = %q, want %q", mb, got, want)
		}
	}
}
