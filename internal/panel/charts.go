package panel

import (
	"fmt"
	"time"

	"github.com/PowerPenguini/murder/internal/store"
)

// The charts are drawn by the server as SVG, with geometry computed here
// so the templates only place it. One ramp of grey, validated as an
// ordinal ramp against the page (each step at least 2:1 on it, steps
// apart): the loudest step is kept for what needs attention.
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
		{Label: "needs attention", N: failing, Fill: ink1, Ink: "#0a0a0a"},
		{Label: "changing", N: changing, Fill: ink2, Ink: "#0a0a0a"},
		{Label: "running", N: running, Fill: ink3, Ink: "#0a0a0a"},
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
