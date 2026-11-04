package panel

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/store"
)

// The charts are drawn by the server as SVG, with geometry computed here
// so the templates only place it. Their labels are HTML, at a size that
// does not change with the chart's width; the plot stretches to its box at
// a fixed height. One ramp of grey, validated as an ordinal ramp against
// the page's #000 (each step at least 2:1 on it, steps apart): the loudest
// step is kept for what needs attention.
const (
	ink1 = "#ececec" // what the reader must see
	ink2 = "#b0b0b0"
	ink3 = "#7a7a7a" // the calm majority
	ink4 = "#4a4a4a" // what is at rest
	// inkBad is the one colour, kept for what is wrong: a failure, an
	// error, a limit nearly reached. 6.5:1 on #000.
	inkBad = "#ff5147"
)

// chartWidth and plotHeight are the plot's coordinates; it scales to its
// box, its lines kept at one pixel.
const (
	chartWidth = 1000.0
	plotHeight = 100.0
)

// meter is one value drawn against the largest of its kind, as a percent
// of its track.
type meter struct {
	Value int
	Width float64
}

func meterOf(value, max int) meter {
	if max <= 0 || value <= 0 {
		return meter{Value: value}
	}
	return meter{Value: value, Width: 100 * float64(value) / float64(max)}
}

// column is one bucket of an activity chart.
type column struct {
	X, Y, W, H float64
	N          int
	// Title says the bucket and its count, for the readout.
	Title string
	Now   bool
	// Unknown buckets precede the oldest event read: nothing is said of
	// them.
	Unknown bool
}

// activity counts events into equal buckets ending now.
type activity struct {
	Columns []column
	Max     int
	Total   int
	// From and To label the axis's ends; Mid its middle.
	From, Mid, To string
	// Partial says the oldest buckets were not all read.
	Partial bool
	// Points are the buckets for the readout, as JSON: [x, when, what].
	Points string
	// Summary says the chart in words, for those who do not see it.
	Summary string
}

const columnMaxWidth = 24.0

// activityOf buckets events (newest first, as the store gives them) over
// span, in whole buckets (hours on the clock, days from midnight UTC), the
// last the one now runs in. complete says no older event was left unread.
func activityOf(events []store.Event, now time.Time, span, bucket time.Duration, complete bool, label func(time.Time) string) activity {
	n := int(span / bucket)
	counts := make([]int, n)
	end := now.Truncate(bucket).Add(bucket)
	start := end.Add(-span)
	known := start
	if !complete && len(events) > 0 {
		known = events[len(events)-1].At
	}
	a := activity{}
	for _, e := range events {
		if e.At.Before(start) || !e.At.Before(end) {
			continue
		}
		i := int(e.At.Sub(start) / bucket)
		if i >= n {
			i = n - 1
		}
		counts[i]++
		a.Total++
	}
	busiest := 0
	for i, c := range counts {
		if c > a.Max {
			a.Max, busiest = c, i
		}
	}
	slot := chartWidth / float64(n)
	w := min(columnMaxWidth, slot*0.62)
	var points [][]any
	for i, c := range counts {
		from := start.Add(time.Duration(i) * bucket)
		col := column{X: float64(i)*slot + (slot-w)/2, W: w, N: c, Now: i == n-1}
		col.Unknown = from.Add(bucket).Before(known)
		if col.Unknown {
			a.Partial = true
		}
		if c > 0 && a.Max > 0 {
			col.H = max(2, plotHeight*float64(c)/float64(a.Max))
		}
		col.Y = plotHeight - col.H
		what := plural(c, "event", "events")
		if col.Unknown {
			what = "not read"
		}
		col.Title = label(from) + ": " + what
		points = append(points, []any{round1(float64(i)*slot + slot/2), label(from), what})
		a.Columns = append(a.Columns, col)
	}
	a.From, a.Mid, a.To = label(start), label(start.Add(span/2)), "now"
	a.Points = jsonOf(points)
	a.Summary = fmt.Sprintf("%s since %s UTC", plural(a.Total, "event", "events"), a.From)
	if a.Max > 0 {
		a.Summary += fmt.Sprintf(", the most, %d, at %s", a.Max, label(start.Add(time.Duration(busiest)*bucket)))
	}
	return a
}

func hourLabel(t time.Time) string { return t.UTC().Format("15:04") }
func dayLabel(t time.Time) string  { return t.UTC().Format("Jan 2") }

// lineChart is one measure over the kept day: a line per series (two at
// most, the second dashed), and its limit if it has one.
type lineChart struct {
	Title string
	// Now is the measure's last value, as text.
	Now    string
	Series []lineSeries
	// Limit draws the limit's level, when it is within the chart.
	Limit *level
	// Top names the top of the scale; the bottom is zero.
	Top             string
	From, Mid       string
	Empty           bool
	Points, Summary string
}

type lineSeries struct {
	Label, Stroke string
	// Dashed tells the second series from the first by more than its grey.
	Dashed bool
	// Line and Area are SVG paths: the line, broken where nothing was
	// measured, and the wash under it.
	Line, Area string
}

type level struct {
	Y     float64
	Label string
	// Hot says the measure's last value is near it.
	Hot bool
}

// sample is one step's value of each series, at a time.
type sample struct {
	At     time.Time
	Values []float64
}

// newLineChart draws samples over the day ending with the step now runs
// in. limit is drawn as a level when positive; format says a value, for
// the axis, the readout and the last value; bytes says values are in
// bytes, so the scale's top is a round number of their unit.
func newLineChart(title string, labels []string, samples []sample, limit float64, now time.Time, step time.Duration, format func(float64) string, bytes bool) lineChart {
	chart := lineChart{Title: title, Empty: len(samples) == 0}
	end := now.Truncate(step).Add(step)
	start := end.Add(-24 * time.Hour)
	chart.From, chart.Mid = hourLabel(start), hourLabel(start.Add(12*time.Hour))
	highest, peak, peakAt := 0.0, math.Inf(-1), time.Time{}
	for _, s := range samples {
		if s.At.Before(start) {
			continue
		}
		for _, v := range s.Values {
			highest = max(highest, v)
		}
		if s.Values[0] > peak {
			peak, peakAt = s.Values[0], s.At
		}
	}
	top := niceCeil(max(highest, limit), bytes)
	chart.Top = format(top)
	y := func(v float64) float64 { return plotHeight * (1 - v/top) }
	x := func(t time.Time) float64 { return chartWidth * t.Sub(start).Seconds() / end.Sub(start).Seconds() }
	strokes := []string{ink2, ink1}
	for i, label := range labels {
		series := lineSeries{Label: label, Stroke: strokes[min(i, len(strokes)-1)], Dashed: i > 0}
		var line, area strings.Builder
		type pt struct{ x, y float64 }
		var run []pt
		half := x(start.Add(step/2)) - x(start)
		flush := func() {
			if len(run) == 0 {
				return
			}
			// A step alone between gaps is drawn across its width: a lone
			// point would draw nothing.
			if len(run) == 1 {
				run = []pt{{run[0].x - half, run[0].y}, {run[0].x + half, run[0].y}}
			}
			coords := make([]string, len(run))
			for i, p := range run {
				coords[i] = f(p.x) + "," + f(p.y)
			}
			joined := strings.Join(coords, "L")
			line.WriteString("M" + joined)
			area.WriteString("M" + f(run[0].x) + "," + f(plotHeight) + "L" + joined + "L" + f(run[len(run)-1].x) + "," + f(plotHeight) + "Z")
			run = run[:0]
		}
		var previous time.Time
		for _, s := range samples {
			if s.At.Before(start) {
				continue
			}
			// A step not measured breaks the line.
			if !previous.IsZero() && s.At.Sub(previous) > step {
				flush()
			}
			// A point sits at the middle of its step.
			run = append(run, pt{x(s.At.Add(step / 2)), y(s.Values[i])})
			previous = s.At
		}
		flush()
		series.Line, series.Area = line.String(), area.String()
		chart.Series = append(chart.Series, series)
	}
	if limit > 0 {
		chart.Limit = &level{Y: y(limit), Label: "limit " + format(limit)}
		if len(samples) > 0 {
			for _, v := range samples[len(samples)-1].Values {
				chart.Limit.Hot = chart.Limit.Hot || 100*v/limit >= memoryWarn
			}
		}
	}
	say := func(s sample) []string {
		var parts []string
		for i, label := range labels {
			parts = append(parts, format(s.Values[i])+" "+label)
		}
		return parts
	}
	var points [][]any
	for _, s := range samples {
		if s.At.Before(start) {
			continue
		}
		points = append(points, append([]any{round1(x(s.At.Add(step / 2))), hourLabel(s.At)}, anyOf(say(s))...))
	}
	chart.Points = jsonOf(points)
	if len(samples) > 0 {
		chart.Now = strings.Join(say(samples[len(samples)-1]), " · ")
		chart.Summary = fmt.Sprintf("%s, the last 24 hours: now %s", title, chart.Now)
		if !peakAt.IsZero() {
			chart.Summary += fmt.Sprintf("; highest %s %s at %s UTC", format(peak), labels[0], hourLabel(peakAt))
		}
		if chart.Limit != nil {
			chart.Summary += "; " + chart.Limit.Label
		}
	}
	return chart
}

// niceCeil is the round number at or above v that a scale ends at: 1, 1.2,
// 1.5, 2, 2.5, 3, 4, 5, 6 or 8 times a power of ten, of v's unit when
// bytes (KB, MB, GB).
func niceCeil(v float64, bytes bool) float64 {
	if v <= 0 || math.IsNaN(v) || math.IsInf(v, 0) {
		return 1
	}
	unit := 1.0
	if bytes {
		for v/unit >= 1024 {
			unit *= 1024
		}
	}
	scaled := v / unit
	power := math.Pow(10, math.Floor(math.Log10(scaled)))
	for _, step := range []float64{1, 1.2, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10} {
		if step*power >= scaled*(1-1e-9) {
			return step * power * unit
		}
	}
	return 10 * power * unit
}

func plural(n int, one, many string) string {
	if n == 1 {
		return "1 " + one
	}
	return strconv.Itoa(n) + " " + many
}

func anyOf(values []string) []any {
	out := make([]any, len(values))
	for i, v := range values {
		out[i] = v
	}
	return out
}

func jsonOf(v any) string {
	data, _ := json.Marshal(v)
	return string(data)
}

func round1(v float64) float64 { return math.Round(v*10) / 10 }

// f writes a coordinate, to a tenth.
func f(v float64) string { return strconv.FormatFloat(math.Round(v*10)/10, 'f', -1, 64) }
