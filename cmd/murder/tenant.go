package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/reconcile"
)

const tenantUsage = `Usage: murder tenant <command> [options]

  create NAME -app APP [-node NODE|auto] [overrides]
                         add a tenant running an app, on the node running the
                         fewest tenants (auto), or the one named
  set NAME [-app APP] [overrides]    change a tenant's app or overrides;
                                     -canary true|false, -hold true|false set how
                                     its app's rollouts treat it
  list                   list tenants, archived ones too, and their state
  show NAME              print a tenant as JSON
  suspend NAME           stop a tenant, keeping its address (visitors get 503)
  resume NAME            start a suspended tenant again
  archive NAME           take a tenant out of service, keeping data, secrets and overrides
  restore NAME           bring an archived tenant back with its data and secrets
  purge NAME -yes        delete an archived tenant and its data for good
  secret NAME KEY        print one generated secret
  rotate NAME KEY        give a secret a new value (the container restarts)
  move NAME -to NODE     move a tenant with its data to another node of the
                         cluster; it is away for seconds, and requests wait
  roll NAME | -all       replace the container as it is, applying what a new
                         release of Murder changed in containers; -all rolls
                         this node's tenants one at a time, each healthy
                         before the next (-wait, 5m by default)
  logs NAME [-tail N]    print the container's recent output

Overrides (repeatable where it makes sense; set changes only what is given):
  -image REF             run this image instead of the app's (a canary)
  -app-image             (set) drop the image override
  -env KEY=VALUE         add or replace an env value
  -unset-env KEY         (set) drop an env override
  -secret KEY[=KIND]     add a secret (KIND hex, base64 or password; default hex)
  -unset-secret KEY      (set) drop a secret override; its value is kept
  -memory MB, -cpus N, -pids N, -disk GB   limits; 0 goes back to the app's
                         (-disk needs zfs volumes)

Every command accepts -socket (default $MURDER_SOCKET or /run/murder/agent.sock).
create, set, resume, restore and rotate accept -wait to block until the
tenant is healthy.
`

var stdout io.Writer = os.Stdout

// lockedWriter lets tenants that are waited for side by side report.
type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

func socketPath(flags *flag.FlagSet) *string {
	fallback := os.Getenv("MURDER_SOCKET")
	if fallback == "" {
		fallback = "/run/murder/agent.sock"
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

func runTenant(args []string) error {
	if len(args) == 0 || args[0] == "-h" || args[0] == "help" {
		fmt.Fprint(os.Stderr, tenantUsage)
		return flag.ErrHelp
	}
	command, args := args[0], args[1:]
	if command == "create" || command == "set" {
		return configureTenant(command, args)
	}
	flags := flag.NewFlagSet("tenant "+command, flag.ContinueOnError)
	socket := socketPath(flags)
	wait := flags.Duration("wait", 0, "wait up to this long for the tenant to be healthy (e.g. 2m)")
	yes := flags.Bool("yes", false, "confirm purge")
	tail := flags.Int("tail", 200, "with logs: number of lines")
	asJSON := flags.Bool("json", false, "with list: print JSON")
	all := flags.Bool("all", false, "with roll: every tenant this node runs")
	to := flags.String("to", "", "with move: the node to move to")
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
		fmt.Fprintf(stdout, "%s archived; \"murder tenant restore %s\" brings it back, \"murder tenant purge %s -yes\" deletes it\n", positional[0], positional[0], positional[0])
		return nil
	case "restore":
		if err := need(1); err != nil {
			return err
		}
		result, err := client.Restore(ctx, positional[0])
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "%s restored at %s, running app %s\n", result.Spec.Name, result.URL, result.App)
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
	case "rotate":
		if err := need(2); err != nil {
			return err
		}
		result, err := client.Rotate(ctx, positional[0], positional[1])
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "%s %s rotated; the container restarts with the new value\n", positional[0], positional[1])
		return waitHealthy(ctx, client, result, *wait)
	case "move":
		if err := need(1); err != nil {
			return err
		}
		if *to == "" {
			return errors.New("tenant move needs -to NODE")
		}
		moved, err := client.Move(ctx, positional[0], *to)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "%s runs on node %s now\n", moved.Spec.Name, moved.Node)
		return nil
	case "roll":
		if !*all {
			if err := need(1); err != nil {
				return err
			}
			result, err := client.Roll(ctx, positional[0])
			if err != nil {
				return err
			}
			fmt.Fprintf(stdout, "%s rolled; its container is replaced\n", positional[0])
			return waitHealthy(ctx, client, result, *wait)
		}
		if err := need(0); err != nil {
			return err
		}
		if *wait <= 0 {
			*wait = 5 * time.Minute
		}
		return rollAll(ctx, client, *wait)
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

// rollAll rolls the running tenants this node runs one after the other,
// each healthy before the next, and stops at the first that is not: one
// tenant down at a time, and none after a release that breaks them.
func rollAll(ctx context.Context, client *api.Client, wait time.Duration) error {
	tenants, err := client.List(ctx)
	if err != nil {
		return err
	}
	rolled := 0
	for _, tenant := range tenants {
		if tenant.Archived() || tenant.Elsewhere || tenant.Spec.Suspended {
			continue
		}
		result, err := client.Roll(ctx, tenant.Spec.Name)
		if err != nil {
			return fmt.Errorf("roll %s: %w", tenant.Spec.Name, err)
		}
		fmt.Fprintf(stdout, "%s rolled\n", tenant.Spec.Name)
		if err := waitHealthy(ctx, client, result, wait); err != nil {
			return fmt.Errorf("stopped after %d tenant(s): %w", rolled, err)
		}
		rolled++
	}
	fmt.Fprintf(stdout, "rolled %d tenant(s)\n", rolled)
	return nil
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

// waitHealthy waits up to timeout for a tenant; see waitFor.
func waitHealthy(ctx context.Context, client *api.Client, tenant api.Tenant, timeout time.Duration) error {
	if timeout <= 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return waitFor(ctx, client, tenant, timeout, stdout)
}

// waitAll waits for tenants side by side within one budget, so a slow one
// cannot use up the time of the others, and returns one problem per tenant
// that did not become healthy.
func waitAll(ctx context.Context, client *api.Client, tenants []api.Tenant, timeout time.Duration) []string {
	if timeout <= 0 || len(tenants) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	results := make([]error, len(tenants))
	out := &lockedWriter{w: stdout}
	var group sync.WaitGroup
	for i, tenant := range tenants {
		group.Go(func() { results[i] = waitFor(ctx, client, tenant, timeout, out) })
	}
	group.Wait()
	var problems []string
	for _, err := range results {
		if err != nil {
			problems = append(problems, err.Error())
		}
	}
	return problems
}

// waitFor polls until the agent reports the tenant's current generation
// running and healthy, or suspended (there is nothing more to wait for), or
// failed, or ctx ends. timeout only labels the error.
func waitFor(ctx context.Context, client *api.Client, tenant api.Tenant, timeout time.Duration, out io.Writer) error {
	last := ""
	for {
		current, err := client.Get(ctx, tenant.Spec.Name)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			return err
		}
		if current.Archived() {
			return fmt.Errorf("%s was archived while waiting for it", tenant.Spec.Name)
		}
		// Only a pass that started after the change counts; a tenant of the
		// same name archived earlier may have left an old status behind.
		if status := current.Status; status != nil && status.Generation >= tenant.Generation && status.CheckedAt.After(tenant.UpdatedAt) {
			switch {
			case status.Phase == reconcile.PhaseRunning && status.Healthy:
				fmt.Fprintf(out, "%s is healthy\n", tenant.Spec.Name)
				return nil
			case status.Phase == reconcile.PhaseSuspended:
				fmt.Fprintf(out, "%s is suspended\n", tenant.Spec.Name)
				return nil
			case status.Phase == reconcile.PhaseFailed:
				return fmt.Errorf("%s failed: %s", tenant.Spec.Name, status.Message)
			}
			last = status.Message
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
			continue
		}
		break
	}
	if last != "" {
		return fmt.Errorf("%s did not become healthy within %s: %s", tenant.Spec.Name, timeout, last)
	}
	return fmt.Errorf("%s did not become healthy within %s", tenant.Spec.Name, timeout)
}

func printJSON(value any) error {
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func printTable(tenants []api.Tenant) error {
	table := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "NAME\tAPP\tNODE\tSTATE\tHEALTHY\tIMAGE\tURL")
	for _, tenant := range tenants {
		state, healthy, url := "pending", "-", tenant.URL
		if tenant.Elsewhere {
			state = "-" // the node that runs it knows
		}
		if tenant.Archived() {
			// Not routed; when it was archived helps decide what to purge.
			state, url = "archived "+tenant.ArchivedAt.Local().Format(time.DateOnly), "-"
		} else if tenant.Status != nil {
			// tenant show tells what the warning is and why it restarted.
			state = tenant.Status.Phase
			var notes []string
			if n := tenant.Status.Restarts; n > 0 {
				notes = append(notes, fmt.Sprintf("%d restarts", n))
			}
			if tenant.Status.Warning != "" {
				notes = append(notes, "warning")
			}
			if len(notes) > 0 {
				state += " (" + strings.Join(notes, ", ") + ")"
			}
			if tenant.Status.Phase == reconcile.PhaseRunning {
				healthy = fmt.Sprint(tenant.Status.Healthy)
			}
		}
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", tenant.Spec.Name, tenant.App, tenant.Node, state, healthy, shortImage(tenant.Spec.Image), url)
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
