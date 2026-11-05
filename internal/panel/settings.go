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
	// Mono says the value is code-like (an image); Empty that its
	// Override was sent on with no value.
	Mono, Empty bool
	// Inherit is the value in effect, raw: an Override turned on starts at
	// it. Risk says it runs without this limit while others have one; Open
	// that the row was asked open for a value of its own (?set=).
	Inherit string
	Risk    bool
	Open    bool
	// Note says what helps choose its value ("its data: 1.4 GB").
	Note string
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
type settingChange struct {
	Field, From, To string
	// Risk says the change leaves it without a limit the others have;
	// FromRisk that it runs without one now.
	Risk, FromRisk bool
}

// settingFields are a tenant's fields, from its app's spec and its form.
func settingFields(t api.Tenant, app spec.Tenant, form settingsForm, limited map[string]bool, disk diskRoom) []settingField {
	stored := formOf(t)
	appSource := "Inherited from " + t.App
	if stored.Image != "" {
		appSource = "Tenant override"
	}
	out := []settingField{
		{Name: "image", Label: "Image", Kind: "text", Inherit: t.Spec.Image, Own: form.Image != "" || slices.Contains(form.empty(), "image"), Empty: slices.Contains(form.empty(), "image"), Value: form.Image, Mono: true, Unit: "",
			Stored: stored.Image != "", Proposed: form.Image != stored.Image, Effective: imageDigest(t.Spec.Image), Source: appSource},
	}
	// The limits as every page says them: in effect, and from where.
	values := map[string][2]string{"memoryMB": {form.MemoryMB, stored.MemoryMB}, "cpus": {form.CPUs, stored.CPUs}, "pids": {form.PIDs, stored.PIDs}, "diskGB": {form.DiskGB, stored.DiskGB}}
	shape := map[string][3]string{"memoryMB": {"MB", "1"}, "cpus": {"cores", "0.05"}, "pids": {"at most", "1"}, "diskGB": {"GB", "1"}}
	for _, l := range limitsOf(t, limited) {
		v := values[l.Field]
		empty := slices.Contains(form.empty(), l.Field)
		open := form.Open == l.Field && v[0] == ""
		inherit := ""
		if l.Value > 0 {
			inherit = strconv.FormatFloat(l.Value, 'f', -1, 64)
		}
		f := settingField{Name: l.Field, Label: l.Label, Unit: shape[l.Field][0], Kind: "number", Step: shape[l.Field][1], Min: "0",
			Own: v[0] != "" || empty || open, Open: open, Value: v[0], Inherit: inherit, Stored: v[1] != "", Proposed: v[0] != v[1] && !empty && !open,
			Effective: l.Short(), Source: l.Source(), Risk: l.Risk, Empty: empty}
		if l.Field == "diskGB" {
			// What the limit must leave it, said beside its value; opened
			// by a link, it starts at a value that leaves room.
			if disk.Used > 0 {
				f.Note = "its data: " + nbsp(size(float64(disk.Used)))
			}
			if open && disk.Suggest() > 0 {
				f.Value, f.Inherit = strconv.Itoa(disk.Suggest()), strconv.Itoa(disk.Suggest())
				f.Note = "suggested: " + strconv.Itoa(disk.Suggest()) + "\u00a0GB"
				if disk.Others > 0 && disk.Suggest() == disk.Others {
					f.Note += ", as the other tenants have"
				} else if disk.Least > 0 {
					f.Note += ", twice its data"
				}
				if disk.Used > 0 {
					f.Note += "; its data: " + nbsp(size(float64(disk.Used)))
				}
			}
		}
		out = append(out, f)
	}
	return out
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
func settingChanges(t api.Tenant, app spec.Tenant, form settingsForm, limited map[string]bool) []settingChange {
	was := formOf(t)
	var out []settingChange
	if form.App != was.App {
		out = append(out, settingChange{"App", was.App, form.App + ": its whole spec changes", false, false})
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
	if was.Image != form.Image {
		out = append(out, settingChange{"Image", say(was.Image, "", imageDigest(app.Image)), say(form.Image, "", imageDigest(app.Image)), false, false})
	}
	// Its limits as every page says them, now and with the form's values.
	afterOwn, _ := form.overrides()
	now := limitsOf(t, limited)
	then := limitsInEffect(afterOwn.Limits.Or(app.Limits), afterOwn.Limits, t.App, t.Status, t.Node, limited)
	for i, n := range now {
		a := then[i]
		if n.Value != a.Value || n.From != a.From {
			out = append(out, settingChange{n.Label, n.WithSource(), a.WithSource(), a.Risk, n.Risk})
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
			out = append(out, settingChange{"Environment " + key, old + ", tenant override", to, false, false})
		case !had && has:
			from := "unset"
			if appHas {
				from = appValue + ", inherited from " + t.App
			}
			out = append(out, settingChange{"Environment " + key, from, next + ", tenant override", false, false})
		case had && has && old != next:
			out = append(out, settingChange{"Environment " + key, old, next, false, false})
		}
	}
	for _, key := range slices.Sorted(maps.Keys(after.Secrets)) {
		if _, had := before.Secrets[key]; !had {
			out = append(out, settingChange{"Secret " + key, "none of its own", "made once, " + after.Secrets[key].Kind, false, false})
		}
	}
	for _, key := range slices.Sorted(maps.Keys(before.Secrets)) {
		if _, has := after.Secrets[key]; !has {
			out = append(out, settingChange{"Secret " + key, "tenant override", "dropped: the app's, if it has one", false, false})
		}
	}
	flag := func(on string, yes, no string) string {
		if on == "on" {
			return yes
		}
		return no
	}
	if form.Flags && form.Canary != was.Canary {
		out = append(out, settingChange{"Canary", flag(was.Canary, "yes", "no"), flag(form.Canary, "yes: takes "+t.App+"'s changes first", "no: with the others"), false, false})
	}
	if form.Flags && form.Hold != was.Hold {
		out = append(out, settingChange{"Held", flag(was.Hold, "yes", "no"), flag(form.Hold, "yes: stays on the revision it runs", "no: follows "+t.App+" again"), false, false})
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

// exactMemory says an amount of memory without rounding it: in GB when
// whole ones, else in MB.
func exactMemory(mb int) string {
	if mb >= 1024 && mb%1024 == 0 {
		return strconv.Itoa(mb/1024) + " GB"
	}
	return strconv.Itoa(mb) + " MB"
}

// settingLabel names a limit's form field as the page does.
func settingLabel(name string) string {
	return map[string]string{"image": "Image", "memoryMB": "Memory", "cpus": "CPUs", "pids": "Processes", "diskGB": "Disk"}[name]
}
