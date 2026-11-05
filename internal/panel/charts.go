package panel

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
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

// dateLabel says a moment with its day: "Nov 4 14:05".
func dateLabel(t time.Time) string { return t.UTC().Format("Jan 2 15:04") }

// spanText says a chart's span: "hour", "6 hours", "24 hours".
func spanText(span time.Duration) string {
	if span == time.Hour {
		return "hour"
	}
	return strconv.Itoa(int(span/time.Hour)) + " hours"
}

// lineChart is one measure over the kept day: a line per series (two at
// most, the second dashed), and its limit if it has one.
type lineChart struct {
	Title string
	// Now is the measure's last value, as text.
	Now string
	// NowSaid says whose the last value is: "now", or while a new container
	// starts, "previous container, at 18:31"; LastAt is its step.
	NowSaid string
	LastAt  time.Time
	Series  []lineSeries
	// Limit draws the limit's level, when it is within the chart.
	Limit *level
	// Top names the top of the scale; the bottom is zero.
	Top             string
	From, Mid       string
	Empty           bool
	Points, Summary string
	// Start and End bound its time; Marks are moments marked on it.
	Start, End time.Time
	Marks      []mark
}

// mark is a moment on a chart: a restart, or a change, with its event.
type mark struct {
	X          float64
	Kind, Text string
	// Href is its event's record.
	Href string
	// When is its time, as the readout says it; HitX where its touch area,
	// HitWidth wide, begins, kept within the plot. Label and Desc say it
	// short, for the line under the plot: "Restart 17:07 UTC", "out of
	// memory (exit 137)".
	When        string
	HitX        float64
	Label, Desc string
}

// markHit is how wide a mark's touch area is without the script, in the
// plot's thousandths; with it, the script makes it 24 px wide.
const markHit = 24.0

// marksOf places events on a chart: restarts, and changes of what runs.
func (c lineChart) marksOf(events []store.Event) []mark {
	var out []mark
	for _, e := range events {
		if e.At.Before(c.Start) || !e.At.Before(c.End) {
			continue
		}
		kind := ""
		switch e.Action {
		case "tenant.restarted", "tenant.failed-over":
			kind = "restart"
		case "tenant.rolled", "tenant.updated", "app.rolled-out", "tenant.moved", "tenant.resumed", "rollout.rolled-back", "rollout.restored":
			kind = "change"
		default:
			continue
		}
		x := chartWidth * e.At.Sub(c.Start).Seconds() / c.End.Sub(c.Start).Seconds()
		said := eventRow{Event: e}.Predicate()
		if said == "" {
			said = actionText(e.Action)
		}
		if ex, ok := parseExit(e.Detail); ok && e.Action == "tenant.restarted" {
			said += ": " + ex.Reason(time.Time{})
			if ex.Memory {
				said += ", out of memory"
			}
		}
		m := mark{X: round1(x), Kind: kind, Text: dateLabel(e.At) + " UTC: " + capital(said), When: dateLabel(e.At),
			Href: "/events?tenant=" + url.QueryEscape(e.Tenant) + "&show=all#ev-" + strconv.FormatInt(e.ID, 10)}
		m.Label, m.Desc = "Change "+e.At.UTC().Format("15:04")+" UTC", said
		if kind == "restart" {
			m.Label = "Restart " + e.At.UTC().Format("15:04") + " UTC"
			m.Desc = actionText(e.Action)
			if ex, ok := parseExit(e.Detail); ok && e.Action == "tenant.restarted" {
				m.Desc = ex.Short(time.Time{}) + " (" + ex.Reason(time.Time{}) + ")"
			}
		}
		out = append(out, m)
	}
	// A change at the moment of a restart is drawn beside it, not over it:
	// the two never make one glyph.
	for i := range out {
		if out[i].Kind != "change" {
			continue
		}
		for _, r := range out {
			if r.Kind == "restart" && math.Abs(r.X-out[i].X) < markApart {
				if r.X+markApart <= chartWidth-3 {
					out[i].X = round1(r.X + markApart)
				} else {
					out[i].X = round1(r.X - markApart)
				}
			}
		}
	}
	for i := range out {
		// A mark at "now" or at the start is drawn whole, inside the lane.
		out[i].X = round1(math.Max(3, math.Min(chartWidth-3, out[i].X)))
		out[i].HitX = round1(math.Max(0, math.Min(chartWidth-markHit, out[i].X-markHit/2)))
	}
	return out
}

// markApart is how far apart a change and a restart are drawn, at least,
// in the plot's thousandths.
const markApart = 12.0

// Restarts and Changes count its marks of each kind.
func (c lineChart) Restarts() int { return c.count("restart") }
func (c lineChart) Changes() int  { return c.count("change") }

func (c lineChart) count(kind string) int {
	n := 0
	for _, m := range c.Marks {
		if m.Kind == kind {
			n++
		}
	}
	return n
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
	// Path draws it across the plot: a step where it changed.
	Path  string
	Label string
	// Hot says the measure's last value is near it.
	Hot bool
}

// sample is one step's value of each series, at a time; Low, if set, the
// least of each in the step (a container killed and started again low),
// and Limit the limit in effect then, 0 if not known.
type sample struct {
	At     time.Time
	Values []float64
	Low    []float64
	Limit  float64
}

// dropShare is how far below its step's value a series' least must be
// for the step to be drawn as a drop: a restart, not a wobble.
const dropShare = 0.85

// headroom is how much room the scale keeps above the highest value or
// limit, so a line at its limit is not drawn on it.
const headroom = 1.25

// newLineChart draws samples over the day ending with the step now runs
// in. limit is drawn as a level when positive; format says a value, for
// the axis, the readout and the last value; bytes says values are in
// bytes, so the scale's top is a round number of their unit.
func newLineChart(title string, labels []string, samples []sample, limit float64, now time.Time, step, span time.Duration, format func(float64) string, bytes bool) lineChart {
	chart := lineChart{Title: title, Empty: len(samples) == 0}
	// The plot ends now, its lines reaching it.
	end := now
	start := end.Add(-span)
	chart.Start, chart.End = start, end
	// The axis's start says its date; the readout says each moment's.
	chart.From, chart.Mid = dateLabel(start), hourLabel(start.Add(span/2))
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
	// The limit as it was at each step: the one in effect now unless the
	// samples say.
	limits := make([]float64, len(samples))
	highestLimit := limit
	for i, s := range samples {
		limits[i] = limit
		if s.Limit > 0 {
			limits[i] = s.Limit
		}
		if !s.At.Before(start) {
			highestLimit = max(highestLimit, limits[i])
		}
	}
	top := niceCeil(max(highest, highestLimit)*headroom, bytes)
	chart.Top = format(top)
	if chart.Top == "under 1" {
		// A scale's top is a number, even where a reading says "under 1".
		chart.Top = strconv.FormatFloat(round1(top), 'f', -1, 64)
	}
	// Inset a unit at each end: a line at 0 is not drawn below the axis.
	y := func(v float64) float64 { return 1 + (plotHeight-2)*(1-max(0, v)/top) }
	x := func(t time.Time) float64 {
		return min(chartWidth, chartWidth*t.Sub(start).Seconds()/end.Sub(start).Seconds())
	}
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
			// A point sits at the middle of its step. A step whose least is
			// well below it is a drop: the line falls to that least within
			// the step, and climbs from there.
			mid := x(s.At.Add(step / 2))
			if i < len(s.Low) && s.Low[i] > 0 && s.Low[i] < dropShare*s.Values[i] {
				q := (x(s.At.Add(step)) - x(s.At)) / 4
				run = append(run, pt{mid - q, y(s.Values[i])}, pt{mid + q, y(s.Low[i])})
			} else {
				run = append(run, pt{mid, y(s.Values[i])})
			}
			previous = s.At
		}
		// The newest step's value holds to now, at the plot's edge.
		if n := len(run); n > 0 && len(samples) > 0 && now.Sub(samples[len(samples)-1].At) <= 2*step {
			run = append(run, pt{chartWidth, run[n-1].y})
		}
		flush()
		series.Line, series.Area = line.String(), area.String()
		chart.Series = append(chart.Series, series)
	}
	if highestLimit > 0 {
		// A step where the limit changed; the label says each, the summary
		// when.
		current, path := limit, ""
		var said []string
		var changedAt time.Time
		for i, s := range samples {
			if s.At.Before(start) || limits[i] <= 0 {
				continue
			}
			switch {
			case path == "":
				path = "M0," + f(y(limits[i]))
				said = append(said, format(limits[i]))
			case limits[i] != current:
				path += "H" + f(x(s.At)) + "V" + f(y(limits[i]))
				said = append(said, format(limits[i]))
				changedAt = s.At
			}
			current = limits[i]
		}
		if path == "" {
			path, current = "M0,"+f(y(limit)), limit
			said = []string{format(limit)}
		} else if limit > 0 && limit != current {
			path += "H" + f(x(now.Add(-step))) + "V" + f(y(limit))
			said, current = append(said, format(limit)), limit
		}
		path += "H" + f(chartWidth)
		chart.Limit = &level{Path: path, Label: "limit " + strings.Join(said, ", then ")}
		if len(said) > 1 && !changedAt.IsZero() {
			chart.Limit.Label = "limit " + said[len(said)-2] + " until " + hourLabel(changedAt) + ", then " + said[len(said)-1]
		}
		if len(samples) > 0 && current > 0 {
			for _, v := range samples[len(samples)-1].Values {
				chart.Limit.Hot = chart.Limit.Hot || 100*v/current >= memoryWarn
			}
		}
	}
	say := func(s sample) []string {
		var parts []string
		for i, label := range labels {
			parts = append(parts, unit(format(s.Values[i]), label))
		}
		return parts
	}
	var points [][]any
	for _, s := range samples {
		if s.At.Before(start) {
			continue
		}
		points = append(points, append([]any{round1(x(s.At.Add(step / 2))), dateLabel(s.At)}, anyOf(say(s))...))
	}
	chart.Points = jsonOf(points)
	chart.NowSaid = "now"
	if len(samples) > 0 {
		chart.LastAt = samples[len(samples)-1].At
		chart.Now = strings.Join(say(samples[len(samples)-1]), " · ")
		chart.Summary = fmt.Sprintf("%s, the last %s: now %s", title, spanText(span), chart.Now)
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

// unit says a value and its label, the label singular for one: "1
// error", "2 errors".
func unit(value, label string) string {
	if (value == "1" || value == "under 1") && strings.HasSuffix(label, "s") {
		return value + " " + strings.TrimSuffix(label, "s")
	}
	return value + " " + label
}
