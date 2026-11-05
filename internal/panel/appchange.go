package panel

import (
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/spec"
)

// appChange is what moving a tenant to another app does to it: the spec
// it runs now against the one it would run (the new app's, with its own
// settings over it), its own settings that stay, what it loses with the
// old app, and whether its data was written by another user than the new
// app runs as.
type appChange struct {
	From, To string
	// Fields are the settings that differ, now and after.
	Fields []appField
	// Kept are its own settings, which stay over the new app's; Dropped
	// what it had from the old app and the new one lacks; Made the
	// secrets the new app has and it gets made.
	Kept, Dropped, Made []string
	// OwnerNow and OwnerAfter are set when its data, as its node read it,
	// was written by another user than the new app runs as; OwnerMixed says
	// some of it has yet another owner. OwnerNote says it, in grey, when
	// there is nothing to warn of, or the owner was not read.
	OwnerNow, OwnerAfter, OwnerNote string
	OwnerMixed                      bool
	// Back are the secrets the new app has that murder kept a value of:
	// they get that value again. SecretsKept says secrets go, their values
	// kept.
	Back        []string
	SecretsKept bool
	DataPath    string
	// Summary says it in one line; Unlimited names the limits it would
	// lose ("CPUs, processes, disk").
	Summary, Unlimited string
	// OneLost says one limit goes.
	OneLost bool
}

// appField is one setting, as it is now and as it would be; Loses says a
// limit goes, a protection the tenant had; Gone that the setting goes.
type appField struct {
	Field, Now, After string
	Loses, Gone       bool
	// Same says its value stays; only where it comes from changes.
	// NowRisk says it runs without this limit now, which others have.
	Same, NowRisk bool
}

// appChangeOf composes what tenant t would run on app to, with the
// overrides the form holds.
func appChangeOf(t api.Tenant, to api.App, form settingsForm, limited map[string]bool) appChange {
	overrides, _ := form.overrides()
	now := t.Spec
	after := spec.Compose(to.Spec, t.Spec.Name, overrides)
	c := appChange{From: t.App, To: to.Name, DataPath: now.DataPath}
	var lost []string
	var head, tail []appField
	for _, d := range diffSpecs(now, after) {
		field, from, to := d.Key, d.From, d.To
		// Limits are said apart, as in effect: below.
		switch d.Key {
		case "memory", "CPUs", "processes", "disk":
			continue
		}
		if name, ok := strings.CutSuffix(d.Key, " (secret)"); ok {
			from, to = "", ""
			if v, ok := now.Secrets[name]; ok {
				from = secretText(v)
			}
			if v, ok := after.Secrets[name]; ok {
				to = secretText(v)
			}
		}
		gone := to == "" && from != ""
		if from == "" {
			from = "not set"
		}
		if to == "" {
			to = "not set"
		}
		row := appField{Field: field, Now: from, After: to, Gone: gone}
		if field[0] >= 'a' && field[0] <= 'z' {
			row.Field = capital(field)
		}
		switch d.Key {
		case "image", "port", "health check", "data path", "user":
			head = append(head, row)
		default:
			tail = append(tail, row)
		}
	}
	// Each limit as it is in effect now and would be: the one account the
	// settings, its page and its risks give. A limit whose value stays but
	// whose source changes is shown, in grey, and not counted.
	nowLimits := limitsOf(t, limited)
	afterLimits := limitsInEffect(after.Limits, overrides.Limits, to.Name, t.Status, t.Node, limited)
	var limits []appField
	var fixes []string
	for i, n := range nowLimits {
		a := afterLimits[i]
		row := appField{Field: n.Label, Now: n.InEffect(), After: a.InEffect(), NowRisk: n.Risk}
		switch {
		case n.Value == a.Value && n.From == a.From:
			continue
		case n.Value == a.Value:
			row.Same, row.After = true, "same: "+a.InEffect()
		case a.From == fromNone:
			row.Loses = true
			lost = append(lost, n.Key)
		}
		if n.Risk && a.Value > 0 {
			fixes = append(fixes, n.Key)
		}
		limits = append(limits, row)
	}
	c.Fields = append(append(head, limits...), tail...)
	// Its own settings stay, over the new app's.
	if overrides.Image != "" {
		c.Kept = append(c.Kept, "image "+imageDigest(overrides.Image))
	}
	l := overrides.Limits
	if l.MemoryMB != 0 {
		c.Kept = append(c.Kept, "memory "+memory(l.MemoryMB))
	}
	if l.CPUs != 0 {
		c.Kept = append(c.Kept, strconv.FormatFloat(l.CPUs, 'f', -1, 64)+" CPUs")
	}
	if l.PIDs != 0 {
		c.Kept = append(c.Kept, strconv.Itoa(l.PIDs)+" processes")
	}
	if l.DiskGB != 0 {
		c.Kept = append(c.Kept, "disk "+strconv.Itoa(l.DiskGB)+" GB")
	}
	for _, key := range slices.Sorted(maps.Keys(overrides.Env)) {
		c.Kept = append(c.Kept, key)
	}
	for _, key := range slices.Sorted(maps.Keys(overrides.Secrets)) {
		c.Kept = append(c.Kept, key+" (secret)")
	}
	// What it had from the old app only goes with it.
	for _, key := range slices.Sorted(maps.Keys(now.Env)) {
		if _, own := overrides.Env[key]; own {
			continue
		}
		if _, ok := after.Env[key]; !ok {
			c.Dropped = append(c.Dropped, key)
		}
	}
	for _, key := range slices.Sorted(maps.Keys(now.Secrets)) {
		if _, own := overrides.Secrets[key]; own {
			continue
		}
		if _, ok := after.Secrets[key]; !ok {
			c.Dropped = append(c.Dropped, key+" (secret)")
			c.SecretsKept = true
		}
	}
	// A secret the new app has: the value murder kept of it (an earlier app
	// gave it one), or a new one.
	for _, key := range slices.Sorted(maps.Keys(after.Secrets)) {
		if _, ok := now.Secrets[key]; ok {
			continue
		}
		if slices.Contains(t.KeptSecrets, key) {
			c.Back = append(c.Back, key)
		} else {
			c.Made = append(c.Made, key)
		}
	}
	// Who wrote its data, as its node read it, against the user the new
	// app runs as: a warning only for an owner read and different.
	if now.DataPath != "" {
		read := ""
		if t.Status != nil {
			read, c.OwnerMixed = t.Status.DataOwner, t.Status.DataMixed
		}
		switch {
		case read != "" && after.User != "" && read != after.User:
			c.OwnerNow, c.OwnerAfter = read, after.User
		case read != "" && after.User == "":
			c.OwnerNote = to.Name + " runs as its image's own user; " + t.Spec.Name + "'s data at " + now.DataPath + " was written as " + read + ": check that its image can use it."
		case read != "":
			c.OwnerNote = t.Spec.Name + "'s data at " + now.DataPath + " was written as " + read + ", the user " + to.Name + " runs as."
		case now.User != after.User && t.Status == nil:
			c.OwnerNote = "Who wrote " + t.Spec.Name + "'s data is not known: its node has not said. " + to.Name + " runs as " + orRoot(after.User) + "."
		case now.User != after.User:
			c.OwnerNote = "Who wrote " + t.Spec.Name + "'s data was not read (it may be empty). " + to.Name + " runs as " + orRoot(after.User) + "."
		}
	}
	// The summary counts as the table does: changed, gone, limits lost; a
	// row whose value stays is not a change.
	changed, gone := 0, 0
	for _, f := range c.Fields {
		switch {
		case f.Same:
		case f.Gone || f.Loses:
			gone++
		default:
			changed++
		}
	}
	c.Unlimited, c.OneLost = strings.Join(lost, ", "), len(lost) == 1
	// Plainly: how many change, how many of those only get new values and
	// which go; and a risk it ends.
	c.Summary = "App " + t.App + " → " + to.Name + ": " + plural(changed+gone, "setting changes", "settings change")
	if gone > 0 {
		var goes []string
		for _, f := range c.Fields {
			if f.Gone || f.Loses {
				name := f.Field
				if f.Loses {
					name = "the " + strings.ToLower(f.Field) + " limit"
				}
				goes = append(goes, name)
			}
		}
		c.Summary += ": " + strconv.Itoa(changed) + " get new values, " + strconv.Itoa(gone) + " go (" + strings.Join(goes, ", ") + ")"
	}
	if len(fixes) > 0 {
		c.Summary += "; it ends the missing " + strings.Join(fixes, ", ") + " limit"
	}
	c.Summary += "; " + t.Spec.Name + " restarts, its data stays"
	return c
}

func orRoot(user string) string {
	if user == "" {
		return "the image's own user"
	}
	return user
}
