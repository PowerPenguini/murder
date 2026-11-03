package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/strictyaml"
)

const appUsage = `Usage: murder app <command> [options]

  apply FILE... [-wait 10m] [-update] [-now]
                             store app specs and roll them out to their tenants;
                             an app is named after its file (apps/web.yaml is "web").
                             A tag the app has already keeps the image it is pinned
                             to; -update pins it to the newest one in the registry
  list                       list apps, how many tenants run each, and newer images
  show NAME                  print an app as JSON
  check [NAME...]            ask the registry now for newer images of the apps' tags
  update NAME [-wait 10m] [-now]
                             take the newest image of the app's tag and roll it out
  history NAME               the app's recent revisions
  rollback NAME [-to N] [-wait 10m] [-now]
                             store an earlier revision again (by default the one
                             before the current) and roll it out
  rollout NAME               how the app's rollout goes: its waves and tenants
  rollout pause|resume|abort|promote NAME
                             stop it moving tenants; let it go on; take every
                             tenant it moved back to the revision before, their
                             data as it is; move every waiting tenant now
  rollout restore NAME [-tenant T ...] -yes
                             take the tenants it moved (or those named) back to
                             the revision before with their data from before
                             they moved: what they wrote since is lost
  delete NAME                delete an app no tenant runs

A change rolls out in waves: canary tenants first (murder tenant set NAME
-canary true), then a tenth of the tenants, half, and the rest, each wave
healthy for a while before the next; held tenants (-hold true) stay. A tenant
that does not come up healthy, or breaks after it came up, pauses the rollout:
abort or restore it from there. -now moves every tenant at once; -wait waits
for the waves too.

Every command accepts -socket (default $MURDER_SOCKET or /run/murder/agent.sock).
`

func runApp(args []string) error {
	if len(args) == 0 || args[0] == "-h" || args[0] == "help" {
		fmt.Fprint(os.Stderr, appUsage)
		return flag.ErrHelp
	}
	command, args := args[0], args[1:]
	flags := flag.NewFlagSet("app "+command, flag.ContinueOnError)
	socket := socketPath(flags)
	wait := flags.Duration("wait", 0, "with apply: wait up to this long in total for every tenant of the apps to be healthy (e.g. 3m)")
	asJSON := flags.Bool("json", false, "with list, check and history: print JSON")
	update := flags.Bool("update", false, "with apply: pin tags to their newest images, also tags the apps have already")
	to := flags.Int64("to", 0, "with rollback: the revision to go back to")
	now := flags.Bool("now", false, "with apply, update and rollback: move every tenant at once, not in waves")
	var restoreTenants listFlag
	flags.Var(&restoreTenants, "tenant", "with rollout restore: a tenant to take back with its data (repeatable; default every tenant the rollout moved)")
	yes := flags.Bool("yes", false, "with rollout restore: confirm that what the tenants wrote since they moved is lost")
	positional, err := parseInterspersed(flags, args)
	if err != nil {
		return err
	}
	ctx := context.Background()
	client := api.NewClient(*socket)
	if *now {
		client = client.AtOnce()
	}
	switch command {
	case "rollout":
		if len(positional) == 2 && positional[0] == "restore" {
			return restoreRollout(ctx, client, positional[1], restoreTenants, *yes)
		}
		if len(restoreTenants) > 0 || *yes {
			return errors.New("-tenant and -yes only make sense with rollout restore")
		}
		return runRollout(ctx, client, positional)
	case "apply":
		if len(positional) == 0 {
			return errors.New("usage: murder app apply FILE... [-wait 3m]")
		}
		return applyApps(ctx, client, positional, *wait, *update)
	case "list":
		apps, err := client.Apps(ctx)
		if err != nil {
			return err
		}
		if *asJSON {
			return printJSON(apps)
		}
		table := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(table, "NAME\tTENANTS\tREVISION\tTAG\tIMAGE\tNEWER\tUPDATED")
		for _, app := range apps {
			fmt.Fprintf(table, "%s\t%d\t%s\t%s\t%s\t%s\t%s\n", app.Name, len(app.Tenants), revisionOf(app.Revision), orDash(app.Tag), shortImage(app.Spec.Image), newerOf(app), app.UpdatedAt.Local().Format(time.DateTime))
		}
		return table.Flush()
	case "check":
		return checkApps(ctx, client, positional, *asJSON)
	case "update":
		if len(positional) != 1 {
			return errors.New("usage: murder app update NAME [-wait 3m]")
		}
		applied, err := client.UpdateApp(ctx, positional[0], time.Time{})
		if err != nil {
			return err
		}
		return reportApplied(ctx, client, positional[0], applied, *wait)
	case "history":
		if len(positional) != 1 {
			return errors.New("usage: murder app history NAME")
		}
		return appHistory(ctx, client, positional[0], *asJSON)
	case "rollback":
		if len(positional) != 1 {
			return errors.New("usage: murder app rollback NAME [-to N] [-wait 3m]")
		}
		return rollbackApp(ctx, client, positional[0], *to, *wait)
	case "show", "delete":
		if len(positional) != 1 {
			return fmt.Errorf("usage: murder app %s NAME", command)
		}
		if command == "delete" {
			if err := client.DeleteApp(ctx, positional[0]); err != nil {
				return err
			}
			fmt.Fprintf(stdout, "app %s deleted\n", positional[0])
			return nil
		}
		app, err := client.App(ctx, positional[0])
		if err != nil {
			return err
		}
		return printJSON(app)
	default:
		fmt.Fprint(os.Stderr, appUsage)
		return fmt.Errorf("unknown app command %q", command)
	}
}

// loadApp reads an app spec file; the app is named after the file.
func loadApp(path string) (string, spec.Tenant, error) {
	name := strings.TrimSuffix(strings.TrimSuffix(filepath.Base(path), ".yaml"), ".yml")
	if err := spec.ValidLabel(name); err != nil {
		return "", spec.Tenant{}, fmt.Errorf("%s: an app is named after its file: %w", path, err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", spec.Tenant{}, err
	}
	var app spec.Tenant
	if err := strictyaml.Decode(data, &app); err != nil {
		return "", spec.Tenant{}, fmt.Errorf("parse %s: %w", path, err)
	}
	if err := spec.CheckApp(name, app, spec.Tenant.ValidateUnpinned); err != nil {
		return "", spec.Tenant{}, fmt.Errorf("%s: %w", path, err)
	}
	return name, app, nil
}

// applyApps stores every app, all files read and checked first so a mistake
// in one stops the apply before anything changes. With wait, it then waits for every
// live tenant of the apps, changed or not: a retry after a failed rollout
// must not pass while a tenant is unhealthy.
func applyApps(ctx context.Context, client *api.Client, paths []string, wait time.Duration, update bool) error {
	type loaded struct {
		name string
		spec spec.Tenant
	}
	var apps []loaded
	seen := map[string]string{}
	for _, path := range paths {
		name, app, err := loadApp(path)
		if err != nil {
			return err
		}
		if earlier, ok := seen[name]; ok {
			return fmt.Errorf("%s and %s both define app %s", earlier, path, name)
		}
		seen[name] = path
		apps = append(apps, loaded{name, app})
	}
	var failures []string
	var rolling []string
	applied := map[string]bool{}
	for _, app := range apps {
		put := client.PutApp
		if update {
			put = client.PutAppNewest
		}
		result, err := put(ctx, app.name, app.spec)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", app.name, err))
			fmt.Fprintf(stdout, "%-20s failed: %v\n", app.name, err)
			continue
		}
		applied[app.name] = true
		state := "unchanged"
		switch {
		case result.Created:
			state = "created"
		case result.Changed:
			state = "updated"
		}
		fmt.Fprintf(stdout, "%-20s %-9s %s\n", app.name, state, shortImage(result.App.Spec.Image))
		if result.Note != "" {
			fmt.Fprintf(stdout, "%-20s note: %s\n", "", result.Note)
		}
		reportTenants(app.name, result)
		// Changed or not: a retry waits for what an earlier apply began.
		rolling = append(rolling, app.name)
	}
	deadline := time.Now().Add(wait)
	if wait > 0 {
		bounded, cancel := context.WithDeadline(ctx, deadline.Add(rolloutPoll))
		defer cancel()
		failures = append(failures, waitRollouts(bounded, client, rolling, deadline)...)
		current, err := client.List(bounded)
		if err != nil {
			return err
		}
		var tenants []api.Tenant
		for _, tenant := range current {
			if applied[tenant.App] && !tenant.Archived() {
				tenants = append(tenants, tenant)
			}
		}
		failures = append(failures, waitAll(ctx, client, tenants, max(time.Until(deadline).Round(time.Second), time.Second))...)
	}
	if len(failures) > 0 {
		return fmt.Errorf("%d problem(s):\n  %s", len(failures), strings.Join(failures, "\n  "))
	}
	return nil
}

func orDash(text string) string {
	if text == "" {
		return "-"
	}
	return text
}

func revisionOf(revision int64) string {
	if revision == 0 {
		return "-"
	}
	return fmt.Sprint(revision)
}

// newerOf says whether a newer image of the app's tag was found.
func newerOf(app api.App) string {
	if app.Latest == "" {
		return "-"
	}
	return shortImage(app.Latest)
}

// checkApps asks the registry about the tags of the named apps, or of every
// app given a tag.
func checkApps(ctx context.Context, client *api.Client, names []string, asJSON bool) error {
	if len(names) == 0 {
		apps, err := client.Apps(ctx)
		if err != nil {
			return err
		}
		for _, app := range apps {
			if app.Tag != "" {
				names = append(names, app.Name)
			}
		}
		if len(names) == 0 && !asJSON {
			fmt.Fprintln(stdout, "no app was given a tag: there is nothing to ask a registry about")
			return nil
		}
	}
	var checks []api.AppCheck
	var failures []string
	for _, name := range names {
		check, err := client.CheckApp(ctx, name)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", name, err))
			if !asJSON {
				fmt.Fprintf(stdout, "%-20s failed: %v\n", name, err)
			}
			continue
		}
		checks = append(checks, check)
		if asJSON {
			continue
		}
		if check.Newer {
			fmt.Fprintf(stdout, "%-20s newer image %s of %s (runs %s): murder app update %s\n", name, shortImage(check.Image), check.App.Tag, shortImage(check.App.Spec.Image), name)
		} else {
			fmt.Fprintf(stdout, "%-20s runs the newest image of %s\n", name, check.App.Tag)
		}
	}
	if asJSON {
		if checks == nil {
			checks = []api.AppCheck{}
		}
		if err := printJSON(checks); err != nil {
			return err
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("%d problem(s):\n  %s", len(failures), strings.Join(failures, "\n  "))
	}
	return nil
}

func appHistory(ctx context.Context, client *api.Client, name string, asJSON bool) error {
	revisions, err := client.AppRevisions(ctx, name)
	if err != nil {
		return err
	}
	if asJSON {
		return printJSON(revisions)
	}
	if len(revisions) == 0 {
		fmt.Fprintf(stdout, "app %s keeps no revision yet: it was stored before revisions and has not changed since\n", name)
		return nil
	}
	table := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "REVISION\tSTORED\tBY\tTAG\tIMAGE")
	for i, revision := range revisions {
		number := fmt.Sprint(revision.Revision)
		if i == 0 {
			number += " (current)"
		}
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\n", number, revision.CreatedAt.Local().Format(time.DateTime), revision.Actor, orDash(revision.Tag), shortImage(revision.Spec.Image))
	}
	return table.Flush()
}

// rollbackApp stores revision to again, or the one before the current.
func rollbackApp(ctx context.Context, client *api.Client, name string, to int64, wait time.Duration) error {
	app, err := client.App(ctx, name)
	if err != nil {
		return err
	}
	if to == 0 {
		revisions, err := client.AppRevisions(ctx, name)
		if err != nil {
			return err
		}
		if len(revisions) < 2 {
			return fmt.Errorf("app %s keeps no earlier revision to go back to", name)
		}
		to = revisions[1].Revision
	}
	applied, err := client.RestoreRevision(ctx, name, to, app.UpdatedAt)
	if err != nil {
		return err
	}
	return reportApplied(ctx, client, name, applied, wait)
}

// reportApplied says what storing an app changed and, with wait, waits for
// its live tenants to be healthy.
func reportApplied(ctx context.Context, client *api.Client, name string, applied api.AppApplied, wait time.Duration) error {
	if applied.Changed {
		fmt.Fprintf(stdout, "%-20s revision %d: %s\n", name, applied.App.Revision, shortImage(applied.App.Spec.Image))
	} else {
		fmt.Fprintf(stdout, "%-20s unchanged %s\n", name, shortImage(applied.App.Spec.Image))
	}
	if applied.Note != "" {
		fmt.Fprintf(stdout, "%-20s note: %s\n", "", applied.Note)
	}
	reportTenants(name, applied)
	if wait <= 0 {
		return nil
	}
	// Changed or not: a retry waits for what an earlier one began.
	deadline := time.Now().Add(wait)
	bounded, cancel := context.WithDeadline(ctx, deadline.Add(rolloutPoll))
	defer cancel()
	if failures := waitRollouts(bounded, client, []string{name}, deadline); len(failures) > 0 {
		return fmt.Errorf("%d problem(s):\n  %s", len(failures), strings.Join(failures, "\n  "))
	}
	current, err := client.List(bounded)
	if err != nil {
		return err
	}
	applied.Tenants = applied.Tenants[:0]
	for _, tenant := range current {
		if tenant.App == name && !tenant.Archived() {
			applied.Tenants = append(applied.Tenants, tenant)
		}
	}
	if failures := waitAll(ctx, client, applied.Tenants, max(time.Until(deadline).Round(time.Second), time.Second)); len(failures) > 0 {
		return fmt.Errorf("%d problem(s):\n  %s", len(failures), strings.Join(failures, "\n  "))
	}
	return nil
}

// reportTenants says which tenants a change moves, and how.
func reportTenants(app string, applied api.AppApplied) {
	if len(applied.Tenants) == 0 {
		return
	}
	names := make([]string, 0, len(applied.Tenants))
	for _, tenant := range applied.Tenants {
		names = append(names, tenant.Spec.Name)
	}
	if applied.Rollout {
		fmt.Fprintf(stdout, "%-20s rolls out in waves to %s (murder app rollout %s)\n", "", strings.Join(names, ", "), app)
		return
	}
	fmt.Fprintf(stdout, "%-20s rolls out to %s\n", "", strings.Join(names, ", "))
}

// rolloutPoll is how often waiting for a rollout asks how it goes.
var rolloutPoll = 5 * time.Second

// waitRollouts waits until the apps' rollouts are done, and reports those
// that paused, were aborted, or are not done by deadline.
func waitRollouts(ctx context.Context, client *api.Client, apps []string, deadline time.Time) []string {
	var failures []string
	for _, name := range apps {
		for {
			app, err := client.App(ctx, name)
			if err != nil {
				failures = append(failures, fmt.Sprintf("%s: %v", name, err))
				break
			}
			rollout := app.Rollout
			// A rollout of an older revision is not this one's: one that
			// went at once has none.
			if rollout == nil || rollout.ToRevision != app.Revision {
				break
			}
			if rollout.State == store.RolloutDone {
				fmt.Fprintf(stdout, "%-20s rolled out\n", name)
				break
			}
			if rollout.State != store.RolloutRunning {
				failures = append(failures, fmt.Sprintf("%s: the rollout is %s: %s", name, rollout.State, rollout.Reason))
				break
			}
			if time.Now().After(deadline) {
				failures = append(failures, fmt.Sprintf("%s: the rollout is at wave %d, not done yet (murder app rollout %s)", name, rollout.Wave, name))
				break
			}
			select {
			case <-ctx.Done():
				return append(failures, ctx.Err().Error())
			case <-time.After(rolloutPoll):
			}
		}
	}
	return failures
}

// runRollout shows an app's rollout, or acts on it.
func runRollout(ctx context.Context, client *api.Client, args []string) error {
	switch {
	case len(args) == 1:
		return showRollout(ctx, client, args[0])
	case len(args) == 2 && slices.Contains([]string{"pause", "resume", "abort", "promote"}, args[0]):
		if _, err := client.RolloutAction(ctx, args[1], args[0]); err != nil {
			return err
		}
		return showRollout(ctx, client, args[1])
	default:
		return errors.New("usage: murder app rollout NAME, murder app rollout pause|resume|abort|promote NAME, or murder app rollout restore NAME [-tenant T ...] -yes")
	}
}

// restoreRollout takes tenants of an app's rollout back to the revision
// before with their data from before they moved; without yes it only says
// what that loses.
func restoreRollout(ctx context.Context, client *api.Client, name string, tenants []string, yes bool) error {
	if !yes {
		named := tenants
		if len(named) == 0 {
			list, err := client.List(ctx)
			if err != nil {
				return err
			}
			for _, tenant := range list {
				if tenant.App == name && !tenant.Archived() && tenant.AppRevision == 0 && tenant.BeforeRevision != 0 {
					named = append(named, fmt.Sprintf("%s (to revision %d)", tenant.Spec.Name, tenant.BeforeRevision))
				}
			}
			if len(named) == 0 {
				return fmt.Errorf("the rollout of app %s moved no tenant to take back", name)
			}
		}
		return fmt.Errorf("restore takes %s back to the revision before, their data to the snapshot taken as they moved: writes to these tenants since they moved are lost. Add -yes to confirm (`murder app rollout abort %s` takes them back with their data as it is)", strings.Join(named, ", "), name)
	}
	restored, err := client.RestoreRollout(ctx, name, tenants)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "%s go back to the revision before, their data too; the rollout is paused\n", strings.Join(restored.Tenants, ", "))
	return showRollout(ctx, client, name)
}

func showRollout(ctx context.Context, client *api.Client, name string) error {
	app, err := client.App(ctx, name)
	if err != nil {
		return err
	}
	rollout := app.Rollout
	if rollout == nil {
		fmt.Fprintf(stdout, "app %s has rolled nothing out in waves\n", name)
		return nil
	}
	fmt.Fprintf(stdout, "app %s, revision %d: %s, wave %d, since %s\n", name, rollout.ToRevision, rollout.State, rollout.Wave, rollout.WaveStarted.Local().Format(time.DateTime))
	if rollout.Reason != "" {
		fmt.Fprintf(stdout, "  %s\n", rollout.Reason)
	}
	tenants, err := client.List(ctx)
	if err != nil {
		return err
	}
	var moved, waiting, held []string
	for _, tenant := range tenants {
		if tenant.App != name || tenant.Archived() {
			continue
		}
		label := tenant.Spec.Name
		if tenant.Canary {
			label += " (canary)"
		}
		switch {
		case tenant.Hold && tenant.AppRevision != 0:
			held = append(held, fmt.Sprintf("%s (revision %d)", label, tenant.AppRevision))
		case tenant.AppRevision != 0:
			waiting = append(waiting, fmt.Sprintf("%s (revision %d)", label, tenant.AppRevision))
		default:
			moved = append(moved, label)
		}
	}
	for _, row := range []struct {
		title string
		names []string
	}{{"on revision " + strconv.FormatInt(rollout.ToRevision, 10), moved}, {"waiting", waiting}, {"held", held}} {
		if len(row.names) > 0 {
			fmt.Fprintf(stdout, "  %-16s %s\n", row.title+":", strings.Join(row.names, ", "))
		}
	}
	return nil
}
