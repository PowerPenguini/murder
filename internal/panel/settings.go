package panel

import (
	"crypto/sha256"
	"encoding/hex"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/spec"
)

// A tenant's settings show each value in effect and where it comes from:
// its own, its app's, or the node's default. A value of its own is turned
// on with a switch; turned off, the app's value is used again. Saving is
// reviewed first: the changed fields, then "Save and restart".

// settingField is one of a tenant's settings that it may keep its own.
type settingField struct {
	// Name is the form's field; Label says it, Unit its unit.
	Name, Label, Unit string
	// Effective is the value in effect, said; Source where it comes from.
	Effective, Source string
	// Own says the form overrides the app's value, Value is it; Stored
	// says an override is saved now; Proposed that the form's differs from
	// what is saved, not yet reviewed.
	Own, Stored, Proposed bool
	Value                 string
	// Mono says the value is code-like (an image).
	Mono bool
	// Kind, Step and Min shape the input.
	Kind, Step, Min string
}

// envRow is one variable of the environment, as the tenant gets it, and
// as the form being edited has it.
type envRow struct {
	Key, Value string
	// Own says the tenant sets it; App the value its app gives, if any.
	Own bool
	App string
	// FromApp says the app sets it too.
	FromApp bool
	// DraftOwn says the form gives it a value of its own, Draft that
	// value (as the editor shows it); New that only the form has it.
	DraftOwn, New bool
	Draft         string
}

// secretField is one of the tenant's secrets, as it has them and as the
// form being edited has them.
type secretField struct {
	Name, Kind string
	Bytes      int
	// Own says the tenant keeps its own, FromApp that the app has it.
	Own, FromApp bool
	// DraftOwn says the form keeps one of its own, of DraftKind and
	// DraftBytes; New that only the form has it.
	DraftOwn, New bool
	DraftKind     string
	DraftBytes    int
}

// secretKinds are the kinds a secret can be made of.
var secretKinds = []string{"hex", "base64", "password"}

// secretFields are a tenant's secrets: its app's and its own, as saved,
// and as the form has them.
func secretFields(app spec.Tenant, stored, draft spec.Secrets) []secretField {
	keys := map[string]bool{}
	for k := range app.Secrets {
		keys[k] = true
	}
	for k := range stored {
		keys[k] = true
	}
	for k := range draft {
		keys[k] = true
	}
	var out []secretField
	for _, k := range slices.Sorted(maps.Keys(keys)) {
		f := secretField{Name: k}
		appSecret, inApp := app.Secrets[k]
		own, isOwn := stored[k]
		f.FromApp, f.Own = inApp, isOwn
		switch {
		case isOwn:
			f.Kind, f.Bytes = own.Kind, own.Bytes
		case inApp:
			f.Kind, f.Bytes = appSecret.Kind, appSecret.Bytes
		}
		if d, ok := draft[k]; ok {
			f.DraftOwn, f.DraftKind, f.DraftBytes = true, d.Kind, d.Bytes
		} else {
			f.DraftKind, f.DraftBytes = f.Kind, f.Bytes
		}
		if f.DraftKind == "" {
			f.DraftKind = "hex"
		}
		f.New = !inApp && !isOwn
		out = append(out, f)
	}
	return out
}

// settingChange is one changed field of a review.
type settingChange struct{ Field, From, To string }

// settingFields are a tenant's fields, from its app's spec and its form.
func settingFields(t api.Tenant, app spec.Tenant, form settingsForm) []settingField {
	stored := formOf(t)
	source := func(own bool, appValue bool) string {
		switch {
		case own:
			return "Tenant override"
		case appValue:
			return "Inherited from " + t.App
		}
		return "the node's default"
	}
	l, a := t.Spec.Limits, app.Limits
	// What is in effect and where it comes from is what is saved; the form
	// is only proposed.
	field := func(name, label, unit, kind, step, value, saved string, effective string, appSet bool) settingField {
		return settingField{Name: name, Label: label, Unit: unit, Kind: kind, Step: step, Min: "0", Own: value != "", Value: value,
			Stored: saved != "", Proposed: value != saved, Effective: effective, Source: source(saved != "", appSet)}
	}
	number := func(n int, unit string) string {
		if n == 0 {
			return "none set"
		}
		return strconv.Itoa(n) + unit
	}
	cpus := "none set"
	if l.CPUs != 0 {
		cpus = strconv.FormatFloat(l.CPUs, 'f', -1, 64)
	}
	memoryText := "none set"
	if l.MemoryMB != 0 {
		memoryText = memory(l.MemoryMB)
	}
	return []settingField{
		{Name: "image", Label: "Image", Kind: "text", Own: form.Image != "", Value: form.Image, Mono: true, Unit: "",
			Stored: stored.Image != "", Proposed: form.Image != stored.Image, Effective: shortImage(t.Spec.Image), Source: source(stored.Image != "", true)},
		field("memoryMB", "Memory", "MB", "number", "1", form.MemoryMB, stored.MemoryMB, memoryText, a.MemoryMB != 0),
		field("cpus", "CPUs", "cores", "number", "0.05", form.CPUs, stored.CPUs, cpus, a.CPUs != 0),
		field("pids", "Processes", "at most", "number", "1", form.PIDs, stored.PIDs, number(l.PIDs, ""), a.PIDs != 0),
		field("diskGB", "Disk", "GB", "number", "1", form.DiskGB, stored.DiskGB, number(l.DiskGB, " GB"), a.DiskGB != 0),
	}
}

// envRows is the environment the tenant gets: the app's, with its own
// over it.
func envRows(app spec.Tenant, own, draft map[string]string) []envRow {
	rows := map[string]*envRow{}
	for key, value := range app.Env {
		rows[key] = &envRow{Key: key, Value: value, App: value, FromApp: true}
	}
	for key, value := range own {
		row := rows[key]
		if row == nil {
			row = &envRow{Key: key}
			rows[key] = row
		}
		row.Own, row.Value = true, value
	}
	for key, value := range draft {
		row := rows[key]
		if row == nil {
			row = &envRow{Key: key, New: true}
			rows[key] = row
		}
		row.DraftOwn, row.Draft = true, envValue(value)
	}
	for _, row := range rows {
		if !row.DraftOwn {
			row.Draft = envValue(row.Value)
		}
	}
	var out []envRow
	for _, key := range slices.Sorted(maps.Keys(rows)) {
		out = append(out, *rows[key])
	}
	return out
}

// settingChanges says what a form changes of a tenant's settings.
func settingChanges(t api.Tenant, app spec.Tenant, form settingsForm) []settingChange {
	was := formOf(t)
	var out []settingChange
	if form.App != was.App {
		out = append(out, settingChange{"App", was.App, form.App + ": its whole spec changes"})
	}
	say := func(own, unit string, appValue string) string {
		if own == "" {
			if appValue == "" || appValue == "0" {
				return "the node's default"
			}
			return appValue + unit + ", inherited from " + t.App
		}
		return own + unit + ", tenant override"
	}
	appImage := shortImage(app.Image)
	pairs := []struct {
		field, unit, before, after, app string
	}{
		{"Image", "", was.Image, form.Image, appImage},
		{"Memory", " MB", was.MemoryMB, form.MemoryMB, strconv.Itoa(app.Limits.MemoryMB)},
		{"CPUs", "", was.CPUs, form.CPUs, strconv.FormatFloat(app.Limits.CPUs, 'f', -1, 64)},
		{"Processes", "", was.PIDs, form.PIDs, strconv.Itoa(app.Limits.PIDs)},
		{"Disk", " GB", was.DiskGB, form.DiskGB, strconv.Itoa(app.Limits.DiskGB)},
	}
	for _, p := range pairs {
		if p.before != p.after {
			out = append(out, settingChange{p.field, say(p.before, p.unit, p.app), say(p.after, p.unit, p.app)})
		}
	}
	before, _ := was.overrides()
	after, _ := form.overrides()
	keys := map[string]bool{}
	for key := range before.Env {
		keys[key] = true
	}
	for key := range after.Env {
		keys[key] = true
	}
	for _, key := range slices.Sorted(maps.Keys(keys)) {
		old, had := before.Env[key]
		next, has := after.Env[key]
		appValue, appHas := app.Env[key]
		switch {
		case had && !has:
			to := "unset"
			if appHas {
				to = appValue + ", inherited from " + t.App
			}
			out = append(out, settingChange{"Environment " + key, old + ", tenant override", to})
		case !had && has:
			from := "unset"
			if appHas {
				from = appValue + ", inherited from " + t.App
			}
			out = append(out, settingChange{"Environment " + key, from, next + ", tenant override"})
		case had && has && old != next:
			out = append(out, settingChange{"Environment " + key, old, next})
		}
	}
	for _, key := range slices.Sorted(maps.Keys(after.Secrets)) {
		if _, had := before.Secrets[key]; !had {
			out = append(out, settingChange{"Secret " + key, "none of its own", "made once, " + after.Secrets[key].Kind})
		}
	}
	for _, key := range slices.Sorted(maps.Keys(before.Secrets)) {
		if _, has := after.Secrets[key]; !has {
			out = append(out, settingChange{"Secret " + key, "tenant override", "dropped: the app's, if it has one"})
		}
	}
	flag := func(on string, yes, no string) string {
		if on == "on" {
			return yes
		}
		return no
	}
	if form.Flags && form.Canary != was.Canary {
		out = append(out, settingChange{"Canary", flag(was.Canary, "yes", "no"), flag(form.Canary, "yes: takes "+t.App+"'s changes first", "no: with the others")})
	}
	if form.Flags && form.Hold != was.Hold {
		out = append(out, settingChange{"Held", flag(was.Hold, "yes", "no"), flag(form.Hold, "yes: stays on the revision it runs", "no: follows "+t.App+" again")})
	}
	return out
}

// reviewToken binds a save to the values reviewed: the save carries it,
// and is refused when its values are not those it was made of. Line breaks
// are taken as browsers send them, either way.
func reviewToken(tenant string, form settingsForm) string {
	hash := sha256.New()
	for _, part := range []string{tenant, strconv.FormatInt(form.Generation, 10), form.App, form.Image, form.Env, form.Secrets, form.MemoryMB, form.CPUs, form.PIDs, form.DiskGB, form.Canary, form.Hold} {
		hash.Write([]byte(strings.ReplaceAll(part, "\r\n", "\n")))
		hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))[:32]
}
