package panel

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/store"
)

// The charts are drawn by the server as SVG, with geometry computed here
// so the templates only place it. One ramp of grey, validated as an
// ordinal ramp against the page's #000 (each step at least 2:1 on it,
// steps apart): the loudest step is kept for what needs attention.
const (
	ink1 = "#ececec" // what the reader must see
	ink2 = "#b0b0b0"
	ink3 = "#7a7a7a" // the calm majority
	ink4 = "#4a4a4a" // what is at rest
)

// chartWidth is the width the charts are drawn at; they scale to their box.
const chartWidth = 1000.0

// segment is one part of a segmented bar.
type segment struct {
	Label string
	N     int
	// X and W place the block; TX its count.
	X, W, TX float64
	Fill     string
	// Ink is the label's colour inside the fill: near black on the light
	// steps, near white on the dark one.
	Ink string
	// Fits says the count fits inside the block.
	Fits bool
}

// segmentedBar splits a whole into its parts, in the order given; a part
// at zero draws nothing, though it stays in the legend.
type segmentedBar struct {
	Parts []segment
	Total int
}

const segmentGap = 3.0

func newSegmentedBar(parts []segment) segmentedBar {
	bar := segmentedBar{Parts: parts}
	for _, p := range parts {
		bar.Total += p.N
	}
	if bar.Total == 0 {
		return bar
	}
	drawn := 0
	for _, p := range parts {
		if p.N > 0 {
			drawn++
		}
	}
	room := chartWidth - segmentGap*float64(drawn-1)
	x := 0.0
	for i := range bar.Parts {
		p := &bar.Parts[i]
		if p.N == 0 {
			continue
		}
		p.X, p.W = x, room*float64(p.N)/float64(bar.Total)
		p.TX = x + 10
		p.Fits = p.W >= 14+9*float64(len(fmt.Sprint(p.N)))
		x += p.W + segmentGap
	}
	return bar
}

// stateBar is the live tenants by what they need: attention first.
func stateBar(rows []tenantRow) segmentedBar {
	var failing, changing, running, stopped int
	for _, row := range rows {
		switch {
		case row.Archived():
			continue
		case row.State.Kind == "bad" || row.State.Label == "unhealthy":
			failing++
		case row.State.Kind == "warn":
			changing++
		case row.State.Kind == "ok":
			running++
		default:
			stopped++
		}
	}
	return newSegmentedBar([]segment{
		{Label: "needs attention", N: failing, Fill: ink1, Ink: "#000"},
		{Label: "changing", N: changing, Fill: ink2, Ink: "#000"},
		{Label: "running", N: running, Fill: ink3, Ink: "#000"},
		{Label: "suspended", N: stopped, Fill: ink4, Ink: "#ececec"},
	})
}

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
	// HitX and HitW are the whole slot, for hover.
	HitX, HitW float64
	N          int
	// Title says the bucket and its count, for hover.
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
}

const (
	activityHeight = 64.0
	columnMaxWidth = 24.0
)

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
	for _, c := range counts {
		a.Max = max(a.Max, c)
	}
	slot := chartWidth / float64(n)
	w := min(columnMaxWidth, slot*0.62)
	for i, c := range counts {
		from := start.Add(time.Duration(i) * bucket)
		col := column{X: float64(i)*slot + (slot-w)/2, W: w, HitX: float64(i) * slot, HitW: slot, N: c, Now: i == n-1}
		col.Unknown = from.Add(bucket).Before(known)
		if col.Unknown {
			a.Partial = true
		}
		if c > 0 && a.Max > 0 {
			col.H = max(2, activityHeight*float64(c)/float64(a.Max))
		}
		col.Y = activityHeight - col.H
		switch {
		case col.Unknown:
			col.Title = label(from) + ": not read"
		case c == 1:
			col.Title = label(from) + ": 1 event"
		default:
			col.Title = fmt.Sprintf("%s: %d events", label(from), c)
		}
		a.Columns = append(a.Columns, col)
	}
	a.From, a.Mid, a.To = label(start), label(start.Add(span/2)), "now"
	return a
}

func hourLabel(t time.Time) string { return t.UTC().Format("15:04") }
func dayLabel(t time.Time) string  { return t.UTC().Format("Jan 2") }

// placement is where a tenant is on one node: it runs there, keeps its
// copy there (ready or not), or its copy is moving there.
type placement struct {
	Node, Kind, Title string
}

func placementsOf(t tenantRow, nodes []nodeView) []placement {
	out := make([]placement, len(nodes))
	for i, n := range nodes {
		p := placement{Node: n.Name}
		switch n.Name {
		case t.Node:
			p.Kind, p.Title = "runs", t.Spec.Name+" runs on "+n.Name
		case t.Tenant.Replica:
			if t.ReplicaReady {
				p.Kind, p.Title = "copy", n.Name+" keeps a copy that can take over"
			} else {
				p.Kind, p.Title = "syncing", n.Name+" keeps a copy, still catching up"
			}
		case t.ReplicaNext:
			p.Kind, p.Title = "next", "the copy moves to "+n.Name
		}
		if t.Archived() && p.Kind == "runs" {
			p.Kind, p.Title = "archived", t.Spec.Name+" is archived; its data is on "+n.Name
		}
		out[i] = p
	}
	return out
}

// lineChart is one measure over the kept day: a line per series (two at
// most, the second the one to watch), a limit if it has one, and a hover
// target per step.
type lineChart struct {
	Title string
	// Now is the measure's last value, as text.
	Now    string
	Series []lineSeries
	// Limit draws the limit's level, when it is within the chart.
	Limit     *level
	Top       string
	Hover     []hoverStep
	From, Mid string
	Empty     bool
}

type lineSeries struct {
	Label, Stroke string
	// Line and Area are SVG paths: the line, broken where nothing was
	// measured, and the wash under it.
	Line, Area string
}

type level struct {
	Y     float64
	Label string
}

type hoverStep struct {
	X, W  float64
	Title string
}

const (
	lineHeight = 150.0
	lineTop    = 8.0 // room above the highest value
)

// sample is one step's value of each series, at a time.
type sample struct {
	At     time.Time
	Values []float64
}

// newLineChart draws samples over the day ending with the step now runs
// in. limit is drawn as a level when positive; format says a value, for
// the axis, the hover and the last value.
func newLineChart(title string, labels []string, samples []sample, limit float64, now time.Time, step time.Duration, format func(float64) string) lineChart {
	chart := lineChart{Title: title, Empty: len(samples) == 0}
	end := now.Truncate(step).Add(step)
	start := end.Add(-24 * time.Hour)
	chart.From, chart.Mid = hourLabel(start), hourLabel(start.Add(12*time.Hour))
	top := limit
	for _, s := range samples {
		for _, v := range s.Values {
			top = max(top, v)
		}
	}
	if top <= 0 {
		top = 1
	}
	top *= 1.08
	y := func(v float64) float64 { return lineTop + (lineHeight-lineTop)*(1-v/top) }
	x := func(t time.Time) float64 { return chartWidth * t.Sub(start).Seconds() / end.Sub(start).Seconds() }
	strokes := []string{ink2, ink1}
	for i, label := range labels {
		series := lineSeries{Label: label, Stroke: strokes[min(i, len(strokes)-1)]}
		var line, area strings.Builder
		var run []string
		flush := func() {
			if len(run) == 0 {
				return
			}
			line.WriteString("M" + strings.Join(run, "L"))
			first, last := run[0], run[len(run)-1]
			firstX, _, _ := strings.Cut(first, ",")
			lastX, _, _ := strings.Cut(last, ",")
			area.WriteString("M" + firstX + "," + f(lineHeight) + "L" + strings.Join(run, "L") + "L" + lastX + "," + f(lineHeight) + "Z")
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
			run = append(run, f(x(s.At.Add(step/2)))+","+f(y(s.Values[i])))
			previous = s.At
		}
		flush()
		series.Line, series.Area = line.String(), area.String()
		chart.Series = append(chart.Series, series)
	}
	if limit > 0 {
		chart.Limit = &level{Y: y(limit), Label: "limit " + format(limit)}
	}
	// The highest value names the top of the scale, unless the limit is it.
	if limit <= 0 || top/1.08 > limit*1.05 {
		chart.Top = format(top / 1.08)
	}
	slot := chartWidth / float64(int(24*time.Hour/step))
	for _, s := range samples {
		if s.At.Before(start) {
			continue
		}
		var parts []string
		for i, label := range labels {
			parts = append(parts, format(s.Values[i])+" "+label)
		}
		chart.Hover = append(chart.Hover, hoverStep{X: x(s.At), W: slot, Title: hourLabel(s.At) + ": " + strings.Join(parts, ", ")})
	}
	if len(samples) > 0 {
		last := samples[len(samples)-1]
		var parts []string
		for i, label := range labels {
			if len(labels) == 1 {
				parts = append(parts, format(last.Values[i]))
			} else {
				parts = append(parts, format(last.Values[i])+" "+label)
			}
		}
		chart.Now = strings.Join(parts, " · ")
	}
	return chart
}

// f writes a coordinate, to a tenth.
func f(v float64) string { return strconv.FormatFloat(math.Round(v*10)/10, 'f', -1, 64) }
