package panel

import (
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/spec"
)

// A review says, for each tenant, what the change does to it: the store
// composes each tenant's spec from the app's and its overrides (spec.
// Compose), and leaves one whose spec does not change as it is.

// specDiff is one setting that differs between two specs.
type specDiff struct{ Key, From, To string }

// Text says it changing: "memory 256 MB → 512 MB", "TZ set to UTC".
func (d specDiff) Text() string {
	switch {
	case d.From == "":
		return d.Key + " set to " + d.To
	case d.To == "":
		return d.Key + " removed"
	}
	return d.Key + " " + d.From + " → " + d.To
}

// diffSpecs lists the settings that differ from a to b, in a fixed order.
func diffSpecs(a, b spec.Tenant) []specDiff {
	var out []specDiff
	add := func(key, from, to string) {
		if from != to {
			out = append(out, specDiff{key, from, to})
		}
	}
	_, shortA := ImageParts(a.Image)
	_, shortB := ImageParts(b.Image)
	if shortA == shortB {
		shortA, shortB = a.Image, b.Image
	}
	add("image", shortA, shortB)
	add("port", portText(a.Port), portText(b.Port))
	add("health check", a.HealthPath, b.HealthPath)
	add("data path", a.DataPath, b.DataPath)
	add("user", a.User, b.User)
	la, lb := a.Limits, b.Limits
	add("memory", memText(la.MemoryMB), memText(lb.MemoryMB))
	add("CPUs", floatText(la.CPUs), floatText(lb.CPUs))
	add("processes", intText(la.PIDs), intText(lb.PIDs))
	add("disk", gbText(la.DiskGB), gbText(lb.DiskGB))
	keys := map[string]bool{}
	for k := range a.Env {
		keys[k] = true
	}
	for k := range b.Env {
		keys[k] = true
	}
	for _, k := range slices.Sorted(maps.Keys(keys)) {
		add(k, a.Env[k], b.Env[k])
	}
	secrets := map[string]bool{}
	for k := range a.Secrets {
		secrets[k] = true
	}
	for k := range b.Secrets {
		secrets[k] = true
	}
	for _, k := range slices.Sorted(maps.Keys(secrets)) {
		_, inA := a.Secrets[k]
		_, inB := b.Secrets[k]
		switch {
		case inA && !inB:
			add(k+" (secret)", "kept", "")
		case !inA && inB:
			add(k+" (secret)", "", "made")
		case a.Secrets[k] != b.Secrets[k]:
			add(k+" (secret)", secretText(a.Secrets[k]), secretText(b.Secrets[k]))
		}
	}
	if len(out) == 0 && !a.Equal(b) {
		out = append(out, specDiff{"other settings", "as they were", "changed"})
	}
	return out
}

func portText(p int) string {
	if p == 0 {
		return ""
	}
	return strconv.Itoa(p)
}

func memText(mb int) string {
	if mb == 0 {
		return ""
	}
	return memory(mb)
}

func floatText(f float64) string {
	if f == 0 {
		return ""
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func intText(n int) string {
	if n == 0 {
		return ""
	}
	return strconv.Itoa(n)
}

func gbText(n int) string {
	if n == 0 {
		return ""
	}
	return strconv.Itoa(n) + " GB"
}

// scope is what a change of app does to one tenant.
type scope struct {
	// Restarts says its spec changes: it gets a new container; Joins that
	// it runs the new spec already but its rollout moves it on paper;
	// neither, that the change leaves it as it is.
	Restarts, Joins bool
	// Changes are the settings that change for it; Stays those the change
	// makes in the app's spec that its overrides keep from it.
	Changes, Stays []specDiff
}

// scopeOf is what changing t's app from old to next does to t.
func scopeOf(t api.Tenant, old, next spec.Tenant) scope {
	composed := spec.Compose(next, t.Spec.Name, t.Overrides)
	composed.Suspended = t.Spec.Suspended
	var s scope
	s.Changes = diffSpecs(t.Spec, composed)
	s.Restarts = len(s.Changes) > 0
	s.Joins = !s.Restarts && t.AppRevision != 0
	mine := map[string]bool{}
	for _, d := range s.Changes {
		mine[d.Key] = true
	}
	for _, d := range diffSpecs(old.WithDefaults(), next.WithDefaults()) {
		if !mine[d.Key] {
			// What it keeps: its own value, as composed.
			for _, own := range diffSpecs(spec.Tenant{}, composed) {
				if own.Key == d.Key {
					s.Stays = append(s.Stays, specDiff{Key: d.Key, To: own.To})
				}
			}
		}
	}
	return s
}

// Result says it in a line: "memory 256 MB → 512 MB", "unchanged: memory
// stays 768 MB, TZ stays Europe/Berlin".
func (s scope) Result() string {
	var stays []string
	for _, d := range s.Stays {
		stays = append(stays, d.Key+" stays "+d.To)
	}
	switch {
	case s.Restarts:
		var changes []string
		for _, d := range s.Changes {
			changes = append(changes, d.Text())
		}
		text := capital(strings.Join(changes, ", "))
		if len(stays) > 0 {
			text += "; " + strings.Join(stays, ", ")
		}
		return text
	case s.Joins:
		return "Runs it already: keeps its container, and moves to the new revision in its wave"
	case len(stays) > 0:
		return "Unchanged: " + strings.Join(stays, ", ")
	}
	return "Unchanged"
}

// secretText says a secret's kind and length: "hex, 32 bytes".
func secretText(s spec.Secret) string {
	text := s.Kind
	if s.Bytes > 0 {
		text += ", " + strconv.Itoa(s.Bytes) + " bytes"
	}
	return text
}

// Lines says it a line each: a changed setting a line, then what stays.
func (s scope) Lines() []string {
	if !s.Restarts {
		return []string{s.Result()}
	}
	var lines []string
	for _, d := range s.Changes {
		lines = append(lines, capital(d.Text()))
	}
	for _, d := range s.Stays {
		lines = append(lines, capital(d.Key)+" stays "+d.To)
	}
	return lines
}
