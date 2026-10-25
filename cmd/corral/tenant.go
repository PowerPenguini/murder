package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/PowerPenguini/corral/internal/api"
	"github.com/PowerPenguini/corral/internal/fleet"
	"github.com/PowerPenguini/corral/internal/reconcile"
)

const tenantUsage = `Usage: corral tenant <command> [options]

  list [-archived]       list tenants (or archived tenants) and their state
  show NAME              print a tenant as JSON
  suspend NAME           stop a tenant, keeping its address (visitors get 503)
  resume NAME            start a suspended tenant again
  archive NAME           take a tenant out of service, keeping data, secrets and spec
  restore NAME           bring an archived tenant back as it was
  purge NAME -yes        delete an archived tenant and its data for good
  secret NAME KEY        print one generated secret
  logs NAME [-tail N]    print the container's recent output

Tenants are created and changed with "corral apply FLEET.yaml".
Every command accepts -socket (default $CORRAL_SOCKET or /run/corral/agent.sock).
resume and restore accept -wait to block until the tenant is healthy.
`

var stdout io.Writer = os.Stdout

func socketPath(flags *flag.FlagSet) *string {
	fallback := os.Getenv("CORRAL_SOCKET")
	if fallback == "" {
		fallback = "/run/corral/agent.sock"
	}
	return flags.String("socket", fallback, "agent API socket")
}

func runReconcile(args []string) error {
	flags := flag.NewFlagSet("reconcile", flag.ContinueOnError)
	socket := socketPath(flags)
	if err := flags.Parse(args); err != nil {
		return err
	}
	return api.NewClient(*socket).Reconcile(context.Background())
}

// runApply makes the node's tenants match a fleet file: missing tenants are
// created and changed ones updated. It never removes a tenant; one missing
// from the file is only reported, and an archived one must be restored.
func runApply(args []string) error {
	flags := flag.NewFlagSet("apply", flag.ContinueOnError)
	socket := socketPath(flags)
	wait := flags.Duration("wait", 0, "wait up to this long for created and changed tenants to be healthy (e.g. 3m)")
	positional, err := parseInterspersed(flags, args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return errors.New("usage: corral apply FLEET.yaml [-wait 3m]")
	}
	desired, err := fleet.Load(positional[0])
	if err != nil {
		return err
	}
	ctx := context.Background()
	client := api.NewClient(*socket)
	current, err := client.List(ctx)
	if err != nil {
		return err
	}
	archived, err := client.ListArchived(ctx)
	if err != nil {
		return err
	}
	existing := map[string]bool{}
	for _, tenant := range current {
		existing[tenant.Spec.Name] = true
	}
	isArchived := map[string]bool{}
	for _, tenant := range archived {
		isArchived[tenant.Name] = true
	}
	var touched []api.Tenant
	var failures []string
	listed := map[string]bool{}
	for _, tenant := range desired {
		listed[tenant.Name] = true
		var applied api.Applied
		switch {
		case isArchived[tenant.Name]:
			fmt.Fprintf(stdout, "%-20s archived — restore it with \"corral tenant restore %s\" first\n", tenant.Name, tenant.Name)
			continue
		case existing[tenant.Name]:
			applied, err = client.Replace(ctx, tenant)
		default:
			applied, err = client.Create(ctx, tenant)
		}
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", tenant.Name, err))
			fmt.Fprintf(stdout, "%-20s failed: %v\n", tenant.Name, err)
			continue
		}
		state := "unchanged"
		switch {
		case !existing[tenant.Name]:
			state = "created"
		case applied.Changed:
			state = "updated"
		}
		fmt.Fprintf(stdout, "%-20s %-9s %s\n", tenant.Name, state, shortImage(applied.Tenant.Spec.Image))
		if applied.Note != "" {
			fmt.Fprintf(stdout, "%-20s note: %s\n", "", applied.Note)
		}
		if applied.Changed {
			touched = append(touched, applied.Tenant)
		}
	}
	for _, tenant := range current {
		if !listed[tenant.Spec.Name] {
			fmt.Fprintf(stdout, "%-20s not in the fleet file; left as it is (archive it to remove it)\n", tenant.Spec.Name)
		}
	}
	for _, tenant := range touched {
		if err := waitHealthy(ctx, client, tenant, *wait); err != nil {
			failures = append(failures, err.Error())
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("%d tenant(s) failed:\n  %s", len(failures), strings.Join(failures, "\n  "))
	}
	return nil
}

func runTenant(args []string) error {
	if len(args) == 0 || args[0] == "-h" || args[0] == "help" {
		fmt.Fprint(os.Stderr, tenantUsage)
		return flag.ErrHelp
	}
	command, args := args[0], args[1:]
	flags := flag.NewFlagSet("tenant "+command, flag.ContinueOnError)
	socket := socketPath(flags)
	wait := flags.Duration("wait", 0, "wait up to this long for the tenant to be healthy (e.g. 2m)")
	yes := flags.Bool("yes", false, "confirm purge")
	tail := flags.Int("tail", 200, "with logs: number of lines")
	asJSON := flags.Bool("json", false, "with list: print JSON")
	showArchived := flags.Bool("archived", false, "with list: list archived tenants")
	positional, err := parseInterspersed(flags, args)
	if err != nil {
		return err
	}
	ctx := context.Background()
	client := api.NewClient(*socket)
	need := func(count int) error {
		if len(positional) != count {
			return fmt.Errorf("tenant %s needs %d argument(s), got %d\n\n%s", command, count, len(positional), tenantUsage)
		}
		return nil
	}
	switch command {
	case "list":
		if err := need(0); err != nil {
			return err
		}
		if *showArchived {
			archived, err := client.ListArchived(ctx)
			if err != nil {
				return err
			}
			if *asJSON {
				return printJSON(archived)
			}
			return printArchived(archived)
		}
		tenants, err := client.List(ctx)
		if err != nil {
			return err
		}
		if *asJSON {
			return printJSON(tenants)
		}
		return printTable(tenants)
	case "show":
		if err := need(1); err != nil {
			return err
		}
		tenant, err := client.Get(ctx, positional[0])
		if err != nil {
			return err
		}
		return printJSON(tenant)
	case "suspend", "resume":
		if err := need(1); err != nil {
			return err
		}
		result, err := client.Suspend(ctx, positional[0], command == "suspend")
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "%s %s\n", result.Spec.Name, map[string]string{"suspend": "suspended", "resume": "resumed"}[command])
		if command == "resume" {
			return waitHealthy(ctx, client, result, *wait)
		}
		return nil
	case "archive":
		if err := need(1); err != nil {
			return err
		}
		if err := client.Archive(ctx, positional[0]); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "%s archived; \"corral tenant restore %s\" brings it back, \"corral tenant purge %s -yes\" deletes it\n", positional[0], positional[0], positional[0])
		return nil
	case "restore":
		if err := need(1); err != nil {
			return err
		}
		result, err := client.Restore(ctx, positional[0])
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "%s restored at %s\n", result.Spec.Name, result.URL)
		return waitHealthy(ctx, client, result, *wait)
	case "purge":
		if err := need(1); err != nil {
			return err
		}
		if !*yes {
			return errors.New("purge deletes an archived tenant and its data for good; add -yes to confirm")
		}
		if err := client.Purge(ctx, positional[0]); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "%s purged\n", positional[0])
		return nil
	case "secret":
		if err := need(2); err != nil {
			return err
		}
		value, err := client.Secret(ctx, positional[0], positional[1])
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, value)
		return nil
	case "logs":
		if err := need(1); err != nil {
			return err
		}
		output, err := client.Logs(ctx, positional[0], *tail)
		if err != nil {
			return err
		}
		_, err = stdout.Write(output)
		return err
	default:
		fmt.Fprint(os.Stderr, tenantUsage)
		return fmt.Errorf("unknown tenant command %q", command)
	}
}

// parseInterspersed parses flags that may appear before, between or after
// positional arguments.
func parseInterspersed(flags *flag.FlagSet, args []string) ([]string, error) {
	var positional []string
	for {
		if err := flags.Parse(args); err != nil {
			return nil, err
		}
		args = flags.Args()
		if len(args) == 0 {
			return positional, nil
		}
		positional = append(positional, args[0])
		args = args[1:]
	}
}

// waitHealthy polls until the agent reports the tenant's current generation
// running and healthy, or failed.
func waitHealthy(ctx context.Context, client *api.Client, tenant api.Tenant, timeout time.Duration) error {
	if timeout <= 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	last := ""
	for {
		current, err := client.Get(ctx, tenant.Spec.Name)
		if err != nil {
			return err
		}
		// Only a pass that started after the change counts; a tenant of the
		// same name archived earlier may have left an old status behind.
		if status := current.Status; status != nil && status.Generation >= tenant.Generation && status.CheckedAt.After(tenant.UpdatedAt) {
			switch {
			case status.Phase == reconcile.PhaseRunning && status.Healthy:
				fmt.Fprintf(stdout, "%s is healthy\n", tenant.Spec.Name)
				return nil
			case status.Phase == reconcile.PhaseFailed:
				return fmt.Errorf("%s failed: %s", tenant.Spec.Name, status.Message)
			}
			last = status.Message
		}
		select {
		case <-ctx.Done():
			if last != "" {
				return fmt.Errorf("%s did not become healthy within %s: %s", tenant.Spec.Name, timeout, last)
			}
			return fmt.Errorf("%s did not become healthy within %s", tenant.Spec.Name, timeout)
		case <-time.After(time.Second):
		}
	}
}

func printJSON(value any) error {
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func printTable(tenants []api.Tenant) error {
	table := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "NAME\tSTATE\tHEALTHY\tIMAGE\tURL")
	for _, tenant := range tenants {
		state, healthy := "pending", "-"
		if tenant.Status != nil {
			state = tenant.Status.Phase
			if tenant.Status.Phase == reconcile.PhaseRunning {
				healthy = fmt.Sprint(tenant.Status.Healthy)
			}
		}
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\n", tenant.Spec.Name, state, healthy, shortImage(tenant.Spec.Image), tenant.URL)
	}
	return table.Flush()
}

func printArchived(archived []api.ArchivedTenant) error {
	table := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "NAME\tARCHIVED\tIMAGE")
	slices.SortFunc(archived, func(a, b api.ArchivedTenant) int { return strings.Compare(a.Name, b.Name) })
	for _, tenant := range archived {
		fmt.Fprintf(table, "%s\t%s\t%s\n", tenant.Name, tenant.ArchivedAt.Format(time.DateTime), shortImage(tenant.Image))
	}
	return table.Flush()
}

func shortImage(image string) string {
	if repository, digest, ok := strings.Cut(image, "@sha256:"); ok && len(digest) > 12 {
		return repository + "@" + digest[:12]
	}
	if id, ok := strings.CutPrefix(image, "sha256:"); ok && len(id) > 12 {
		return id[:12]
	}
	return image
}
