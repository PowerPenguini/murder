package panel

import (
	"slices"
	"strconv"
	"strings"
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

// A line breaks where nothing was measured; the limit is drawn as a level
// and names the top of the scale; every step has a hover.
func TestLineChart(t *testing.T) {
	now := time.Date(2026, 11, 2, 17, 23, 0, 0, time.UTC)
	step := 5 * time.Minute
	end := now.Truncate(step)
	var samples []sample
	for i := 1; i <= 10; i++ {
		if i == 5 || i == 6 {
			continue // the agent did not measure
		}
		samples = append(samples, sample{At: end.Add(-time.Duration(i) * step), Values: []float64{float64(i), 1}})
	}
	slices.SortFunc(samples, func(a, b sample) int { return a.At.Compare(b.At) })
	chart := newLineChart("Memory", []string{"used", "errors"}, samples, 20, now, step, func(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) })
	if len(chart.Series) != 2 || strings.Count(chart.Series[0].Line, "M") != 2 || strings.Count(chart.Series[0].Area, "Z") != 2 {
		t.Fatalf("a gap did not break the line: %+v", chart.Series[0])
	}
	if chart.Limit == nil || chart.Limit.Label != "limit 20" || chart.Top != "" || len(chart.Hover) != 8 {
		t.Fatalf("chart: limit %+v, top %q, %d hovers", chart.Limit, chart.Top, len(chart.Hover))
	}
	if chart.Now != "1 used · 1 errors" || chart.Hover[7].Title != "17:15: 1 used, 1 errors" {
		t.Fatalf("now %q, last hover %q", chart.Now, chart.Hover[7].Title)
	}
	// The limit at 20 is the highest value: the scale leaves 8% above it.
	if want := lineTop + (lineHeight-lineTop)*(1-1/1.08); chart.Limit.Y < want-0.01 || chart.Limit.Y > want+0.01 {
		t.Fatalf("the limit's level: %v", chart.Limit.Y)
	}
	if empty := newLineChart("CPU", []string{"cores"}, nil, 0, now, step, cores); !empty.Empty {
		t.Fatal("a chart of nothing is not empty")
	}
}

func TestUnits(t *testing.T) {
	for v, want := range map[float64]string{0: "0 B", 812: "812 B", 1536: "1.5 KB", 300 << 20: "300 MB", 1.2 * (1 << 30): "1.2 GB"} {
		if got := size(v); got != want {
			t.Errorf("size(%v) = %q, want %q", v, got, want)
		}
	}
	for v, want := range map[float64]string{0: "0", 0.004: "<0.01", 0.123: "0.12", 12.34: "12.3"} {
		if got := cores(v); got != want {
			t.Errorf("cores(%v) = %q, want %q", v, got, want)
		}
	}
	for v, want := range map[float64]string{0: "0", 0.25: "0.3", 97.4: "97", 1530: "1.5k"} {
		if got := rate(v); got != want {
			t.Errorf("rate(%v) = %q, want %q", v, got, want)
		}
	}
	for v, want := range map[float64]string{0: "—", 0.0423: "42 ms", 1.234: "1.23 s"} {
		if got := wait(v); got != want {
			t.Errorf("wait(%v) = %q, want %q", v, got, want)
		}
	}
}

// A step alone between gaps is drawn across its width.
func TestALoneStepIsDrawn(t *testing.T) {
	now := time.Date(2026, 11, 2, 17, 23, 0, 0, time.UTC)
	step := 5 * time.Minute
	chart := newLineChart("CPU", []string{"cores"}, []sample{{At: now.Truncate(step).Add(-step), Values: []float64{1}}}, 0, now, step, cores)
	if line := chart.Series[0].Line; strings.Count(line, "M") != 1 || strings.Count(line, "L") != 1 {
		t.Fatalf("a lone step: %q", line)
	}
}
