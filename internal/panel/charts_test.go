package panel

import (
	"encoding/json"
	"math"
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
	if last.N != 2 || !last.Now || last.Title != "17:00: 2 events" || a.Columns[22].N != 1 || a.Columns[21].N != 3 || a.Columns[21].H != plotHeight {
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

func TestMemory(t *testing.T) {
	for mb, want := range map[int]string{256: "256 MB", 1024: "1 GB", 1536: "1.5 GB", 1300: "1.3 GB"} {
		if got := memory(mb); got != want {
			t.Errorf("memory(%d) = %q, want %q", mb, got, want)
		}
	}
}

// A line breaks where nothing was measured; the limit is drawn as a level;
// the scale ends at a round number; every step is in the readout.
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
	chart := newLineChart("Memory", []string{"used", "errors"}, samples, 20, now, step, 24*time.Hour, func(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }, false)
	if len(chart.Series) != 2 || strings.Count(chart.Series[0].Line, "M") != 2 || strings.Count(chart.Series[0].Area, "Z") != 2 {
		t.Fatalf("a gap did not break the line: %+v", chart.Series[0])
	}
	if chart.Series[0].Dashed || !chart.Series[1].Dashed {
		t.Fatalf("the second series is not dashed: %+v", chart.Series)
	}
	var points [][]any
	if err := json.Unmarshal([]byte(chart.Points), &points); err != nil || len(points) != 8 {
		t.Fatalf("points %q: %v", chart.Points, err)
	}
	// The scale keeps room above the limit: the line at its limit is not
	// drawn on it.
	if chart.Limit == nil || chart.Limit.Label != "limit 20" || chart.Top != "25" {
		t.Fatalf("chart: limit %+v, top %q", chart.Limit, chart.Top)
	}
	if last := points[7]; chart.Now != "1 used · 1 error" || last[1] != "Nov 2 17:15" || last[2] != "1 used" || last[3] != "1 error" {
		t.Fatalf("now %q, last point %v", chart.Now, points[7])
	}
	// The limit at 20 of 25, all along.
	if chart.Limit.Path != "M0,20.6H1000" {
		t.Fatalf("the limit's level: %v", chart.Limit.Path)
	}
	if !strings.Contains(chart.Summary, "highest 10 used at 16:30 UTC") {
		t.Fatalf("summary %q", chart.Summary)
	}
	if empty := newLineChart("CPU", []string{"cores"}, nil, 0, now, step, 24*time.Hour, cores, false); !empty.Empty {
		t.Fatal("a chart of nothing is not empty")
	}
}

// A scale ends at a round number, of the values' unit when in bytes.
func TestNiceCeil(t *testing.T) {
	for _, c := range []struct {
		v     float64
		bytes bool
		want  float64
	}{{0.42, false, 0.5}, {1, false, 1}, {1.01, false, 1.2}, {172, false, 200}, {256 << 20, true, 300 << 20}, {9000, false, 10000}, {0, false, 1}} {
		if got := niceCeil(c.v, c.bytes); math.Abs(got-c.want) > 1e-6*c.want {
			t.Errorf("niceCeil(%v, %v) = %v, want %v", c.v, c.bytes, got, c.want)
		}
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
	for v, want := range map[float64]string{0: "0", 0.25: "under 1", 97.4: "97", 1530: "1.5k"} {
		if got := rate(v); got != want {
			t.Errorf("rate(%v) = %q, want %q", v, got, want)
		}
	}
	for v, want := range map[float64]string{0: "none", 0.0423: "42 ms", 1.234: "1.23 s"} {
		if got := wait(v); got != want {
			t.Errorf("wait(%v) = %q, want %q", v, got, want)
		}
	}
}

// A step alone between gaps is drawn across its width.
func TestALoneStepIsDrawn(t *testing.T) {
	now := time.Date(2026, 11, 2, 17, 23, 0, 0, time.UTC)
	step := 5 * time.Minute
	chart := newLineChart("CPU", []string{"cores"}, []sample{{At: now.Truncate(step).Add(-step), Values: []float64{1}}}, 0, now, step, 24*time.Hour, cores, false)
	if line := chart.Series[0].Line; strings.Count(line, "M") != 1 || strings.Count(line, "L") != 1 {
		t.Fatalf("a lone step: %q", line)
	}
}

// A chart of the last hour marks the restarts and changes in it.
func TestChartSpanAndMarks(t *testing.T) {
	now := time.Date(2026, 11, 2, 17, 23, 0, 0, time.UTC)
	var samples []sample
	for i := 1; i <= 120; i++ {
		samples = append(samples, sample{At: now.Truncate(time.Minute).Add(-time.Duration(121-i) * time.Minute), Values: []float64{1}})
	}
	chart := newLineChart("CPU", []string{"cores"}, samples, 0, now, time.Minute, time.Hour, cores, false)
	var points [][]any
	_ = json.Unmarshal([]byte(chart.Points), &points)
	if len(points) < 59 || len(points) > 61 || chart.From != "Nov 2 16:23" || !strings.Contains(chart.Summary, "the last hour") {
		t.Fatalf("%d points from %s: %s", len(points), chart.From, chart.Summary)
	}
	marks := chart.marksOf([]store.Event{{At: now.Add(-10 * time.Minute), Action: "tenant.restarted"}, {At: now.Add(-2 * time.Hour), Action: "tenant.rolled"}, {At: now.Add(-5 * time.Minute), Action: "tenant.rolled"}})
	if len(marks) != 2 || marks[0].Kind != "restart" || marks[1].Kind != "change" {
		t.Fatalf("marks %+v", marks)
	}
}

// The limit is drawn as it was: a step where it changed, said in its
// label; a step whose least is well below it is a drop, and climbs back;
// the line reaches now, and nothing is drawn below 0.
func TestLimitHistoryDropsAndNow(t *testing.T) {
	now := time.Date(2026, 11, 2, 17, 23, 0, 0, time.UTC)
	step := 15 * time.Minute
	var samples []sample
	for i := 8; i >= 1; i-- {
		s := sample{At: now.Truncate(step).Add(-time.Duration(i-1) * step), Values: []float64{250}, Limit: 256}
		if i <= 3 {
			s.Values, s.Limit = []float64{260}, 512
		}
		if i == 6 {
			s.Low = []float64{120}
		}
		samples = append(samples, s)
	}
	chart := newLineChart("Memory", []string{"used"}, samples, 512, now, step, 2*time.Hour, func(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) + " MB" }, false)
	if chart.Limit == nil || !strings.Contains(chart.Limit.Path, "V") || chart.Limit.Label != "limit 256 MB until 16:45, then 512 MB" {
		t.Fatalf("the limit: %+v", chart.Limit)
	}
	if chart.Limit.Hot {
		t.Fatal("260 of 512 MB is not near the limit")
	}
	line := chart.Series[0].Line
	if !strings.HasSuffix(strings.Split(line, "L")[len(strings.Split(line, "L"))-1], "1000,"+strings.Split(strings.Split(line, "L")[len(strings.Split(line, "L"))-2], ",")[1]) {
		t.Fatalf("the line does not reach now: %q", line)
	}
	// 120 of a top of 800: a point far below its neighbours.
	if !strings.Contains(line, ","+f(1+98*(1-120.0/800))) {
		t.Fatalf("no drop in %q (top %s)", line, chart.Top)
	}
	zero := newLineChart("Errors", []string{"errors"}, []sample{{At: now.Truncate(step), Values: []float64{0}}}, 0, now, step, time.Hour, cores, false)
	if strings.Contains(zero.Series[0].Line, ",100") {
		t.Fatalf("0 drawn on or below the axis: %q", zero.Series[0].Line)
	}
}
