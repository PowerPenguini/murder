package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/PowerPenguini/corral/internal/api"
	"github.com/PowerPenguini/corral/internal/spec"
	"github.com/PowerPenguini/corral/internal/strictyaml"
)

const appUsage = `Usage: corral app <command> [options]

  apply FILE... [-wait 3m]   store app specs and roll them out to their tenants;
                             an app is named after its file (apps/web.yaml is "web")
  list                       list apps and how many tenants run each
  show NAME                  print an app as JSON
  delete NAME                delete an app no tenant runs

Every command accepts -socket (default $CORRAL_SOCKET or /run/corral/agent.sock).
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
	asJSON := flags.Bool("json", false, "with list: print JSON")
	positional, err := parseInterspersed(flags, args)
	if err != nil {
		return err
	}
	ctx := context.Background()
	client := api.NewClient(*socket)
	switch command {
	case "apply":
		if len(positional) == 0 {
			return errors.New("usage: corral app apply FILE... [-wait 3m]")
		}
		return applyApps(ctx, client, positional, *wait)
	case "list":
		apps, err := client.Apps(ctx)
		if err != nil {
			return err
		}
		if *asJSON {
			return printJSON(apps)
		}
		table := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(table, "NAME\tTENANTS\tIMAGE\tUPDATED")
		for _, app := range apps {
			fmt.Fprintf(table, "%s\t%d\t%s\t%s\n", app.Name, len(app.Tenants), shortImage(app.Spec.Image), app.UpdatedAt.Local().Format(time.DateTime))
		}
		return table.Flush()
	case "show", "delete":
		if len(positional) != 1 {
			return fmt.Errorf("usage: corral app %s NAME", command)
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
	return name, app, nil
}

// applyApps stores every app, all files read first so a typo in one stops
// the apply before anything changes. With wait, it then waits for every
// live tenant of the apps, changed or not: a retry after a failed rollout
// must not pass while a tenant is unhealthy.
func applyApps(ctx context.Context, client *api.Client, paths []string, wait time.Duration) error {
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
	applied := map[string]bool{}
	for _, app := range apps {
		result, err := client.PutApp(ctx, app.name, app.spec)
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
		if len(result.Tenants) > 0 {
			names := make([]string, 0, len(result.Tenants))
			for _, tenant := range result.Tenants {
				names = append(names, tenant.Spec.Name)
			}
			fmt.Fprintf(stdout, "%-20s rolls out to %s\n", "", strings.Join(names, ", "))
		}
	}
	if wait > 0 {
		current, err := client.List(ctx)
		if err != nil {
			return err
		}
		var tenants []api.Tenant
		for _, tenant := range current {
			if applied[tenant.App] {
				tenants = append(tenants, tenant)
			}
		}
		failures = append(failures, waitAll(ctx, client, tenants, wait)...)
	}
	if len(failures) > 0 {
		return fmt.Errorf("%d problem(s):\n  %s", len(failures), strings.Join(failures, "\n  "))
	}
	return nil
}
