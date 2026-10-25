package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/PowerPenguini/corral/internal/api"
	"github.com/PowerPenguini/corral/internal/reconcile"
	"github.com/PowerPenguini/corral/internal/spec"
)

const tenantUsage = `Usage: corral tenant <command> [options]

  list                          list tenants and their state
  show NAME                     print a tenant as JSON
  create NAME -file APP.yaml    create a tenant from an app spec [-adopt-data]
  apply NAME -file APP.yaml     replace a tenant's spec (secrets are kept)
  set-image NAME IMAGE          run a different pinned image
  suspend NAME | resume NAME    stop or restart a tenant, keeping its data
  delete NAME -yes [-purge]     remove a tenant; -purge also deletes its data
  purge-data NAME -yes          delete data a tenant deleted without -purge left behind
  secret NAME KEY               print one generated secret
  logs NAME [-tail N]           print the container's recent output

Every command accepts -socket (default $CORRAL_SOCKET or /run/corral/agent.sock).
create, apply, set-image and resume accept -wait to block until the tenant is healthy.
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

func runTenant(args []string) error {
	if len(args) == 0 || args[0] == "-h" || args[0] == "help" {
		fmt.Fprint(os.Stderr, tenantUsage)
		return flag.ErrHelp
	}
	command, args := args[0], args[1:]
	flags := flag.NewFlagSet("tenant "+command, flag.ContinueOnError)
	socket := socketPath(flags)
	file := flags.String("file", "", "app spec (YAML) for create and apply")
	image := flags.String("image", "", "override the spec's image")
	wait := flags.Duration("wait", 0, "wait up to this long for the tenant to be healthy (e.g. 2m)")
	yes := flags.Bool("yes", false, "confirm delete")
	purge := flags.Bool("purge", false, "with delete: also delete the tenant's data")
	tail := flags.Int("tail", 200, "with logs: number of lines")
	asJSON := flags.Bool("json", false, "with list: print JSON")
	adoptData := flags.Bool("adopt-data", false, "with create: take over data a deleted tenant of the same name left behind")
	// Allow options after positional arguments: "create acme -file app.yaml".
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
	case "create", "apply":
		if err := need(1); err != nil {
			return err
		}
		tenant, err := loadSpec(*file, positional[0], *image)
		if err != nil {
			return err
		}
		var result api.Tenant
		if command == "create" {
			result, err = client.Create(ctx, tenant, *adoptData)
		} else {
			result, err = client.Replace(ctx, tenant)
		}
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "%s %s at %s (generation %d)\n", result.Spec.Name, pastTense(command), result.URL, result.Generation)
		return waitHealthy(ctx, client, result, *wait)
	case "set-image":
		if err := need(2); err != nil {
			return err
		}
		result, err := client.SetImage(ctx, positional[0], positional[1])
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "%s now runs %s (generation %d)\n", result.Spec.Name, result.Spec.Image, result.Generation)
		return waitHealthy(ctx, client, result, *wait)
	case "suspend", "resume":
		if err := need(1); err != nil {
			return err
		}
		result, err := client.Suspend(ctx, positional[0], command == "suspend")
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "%s %s\n", result.Spec.Name, pastTense(command))
		if command == "resume" {
			return waitHealthy(ctx, client, result, *wait)
		}
		return nil
	case "delete":
		if err := need(1); err != nil {
			return err
		}
		if !*yes {
			return errors.New("delete removes the tenant; add -yes to confirm (and -purge to delete its data too)")
		}
		if err := client.Delete(ctx, positional[0], *purge); err != nil {
			return err
		}
		if *purge {
			fmt.Fprintf(stdout, "%s deleted with its data\n", positional[0])
		} else {
			fmt.Fprintf(stdout, "%s deleted; its data is kept\n", positional[0])
		}
		return nil
	case "purge-data":
		if err := need(1); err != nil {
			return err
		}
		if !*yes {
			return errors.New("purge-data deletes a deleted tenant's leftover data for good; add -yes to confirm")
		}
		if err := client.PurgeData(ctx, positional[0]); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "leftover data of %s deleted\n", positional[0])
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

func pastTense(command string) string {
	switch command {
	case "create":
		return "created"
	case "apply":
		return "updated"
	case "suspend":
		return "suspended"
	case "resume":
		return "resumed"
	}
	return command
}

// loadSpec reads an app spec and names it; the same file serves every
// tenant that runs the app. A name inside the file must match.
func loadSpec(path, name, image string) (spec.Tenant, error) {
	if path == "" {
		return spec.Tenant{}, errors.New("-file is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return spec.Tenant{}, err
	}
	var tenant spec.Tenant
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&tenant); err != nil {
		return spec.Tenant{}, fmt.Errorf("parse %s: %w", path, err)
	}
	if tenant.Name != "" && tenant.Name != name {
		return spec.Tenant{}, fmt.Errorf("%s names tenant %q, not %q", path, tenant.Name, name)
	}
	tenant.Name = name
	if image != "" {
		tenant.Image = image
	}
	return tenant, tenant.Validate()
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
		// same name deleted earlier may have left an old status behind.
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

func shortImage(image string) string {
	if repository, digest, ok := strings.Cut(image, "@sha256:"); ok && len(digest) > 12 {
		return repository + "@" + digest[:12]
	}
	if id, ok := strings.CutPrefix(image, "sha256:"); ok && len(id) > 12 {
		return id[:12]
	}
	return image
}
