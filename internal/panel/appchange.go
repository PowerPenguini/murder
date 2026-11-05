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
	// Owner is set when its data was written as another user than the new
	// app runs as: the user now, and the one after.
	OwnerNow, OwnerAfter string
	DataPath             string
	// Summary says it in one line; Unlimited names the limits it would
	// lose ("CPUs, processes, disk").
	Summary, Unlimited string
}

// appField is one setting, as it is now and as it would be; Loses says a
// limit goes, a protection the tenant had; Gone that the setting goes.
type appField struct {
	Field, Now, After string
	Loses, Gone       bool
}

// appChangeOf composes what tenant t would run on app to, with the
// overrides the form holds.
func appChangeOf(t api.Tenant, to api.App, form settingsForm) appChange {
	overrides, _ := form.overrides()
	now := t.Spec
	after := spec.Compose(to.Spec, t.Spec.Name, overrides)
	c := appChange{From: t.App, To: to.Name, DataPath: now.DataPath}
	var lost []string
	for _, d := range diffSpecs(now, after) {
		field, from, to := d.Key, d.From, d.To
		// What an empty value means depends on the setting.
		unset := "not set"
		switch d.Key {
		case "memory", "CPUs", "processes", "disk":
			unset = "no limit"
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
		loses := unset == "no limit" && to == "" && from != ""
		gone := to == "" && from != ""
		if from == "" {
			from = unset
		}
		if to == "" {
			to = unset
			if loses {
				to = "no limit (" + c.To + " sets none)"
			}
		}
		if loses {
			lost = append(lost, d.Key)
		}
		if field[0] >= 'a' && field[0] <= 'z' {
			field = capital(field)
		}
		c.Fields = append(c.Fields, appField{Field: field, Now: from, After: to, Loses: loses, Gone: gone && !loses})
	}
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
		}
	}
	for _, key := range slices.Sorted(maps.Keys(after.Secrets)) {
		if _, ok := now.Secrets[key]; !ok {
			c.Made = append(c.Made, key)
		}
	}
	// Its data stays as written: by the user it runs as now.
	if now.User != after.User && now.DataPath != "" {
		c.OwnerNow, c.OwnerAfter = orRoot(now.User), orRoot(after.User)
	}
	// The summary counts as the table does: changed, gone, limits lost.
	changed, gone := 0, 0
	for _, f := range c.Fields {
		if f.Gone || f.Loses {
			gone++
		} else {
			changed++
		}
	}
	c.Unlimited = strings.Join(lost, ", ")
	c.Summary = "App " + t.App + " → " + to.Name + ": " + plural(changed, "setting changes", "settings change")
	if gone > 0 {
		c.Summary += ", " + strconv.Itoa(gone) + " go"
		if len(lost) > 0 {
			c.Summary += " (" + plural(len(lost), "limit", "limits") + ": " + c.Unlimited + ")"
		}
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
