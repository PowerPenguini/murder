package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"maps"
	"strconv"
	"strings"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

// listFlag collects a repeatable flag.
type listFlag []string

func (l *listFlag) String() string         { return strings.Join(*l, ",") }
func (l *listFlag) Set(value string) error { *l = append(*l, value); return nil }

// configureTenant creates a tenant or changes one: "tenant create" starts
// from no overrides, "tenant set" from the tenant's current ones, and both
// change only what their flags name.
func configureTenant(command string, args []string) error {
	flags := flag.NewFlagSet("tenant "+command, flag.ContinueOnError)
	socket := socketPath(flags)
	wait := flags.Duration("wait", 0, "wait up to this long for the tenant to be healthy (e.g. 2m)")
	app := flags.String("app", "", "the app the tenant runs")
	node := flags.String("node", store.Auto, "with create: the node that runs the tenant, or auto for the one running the fewest")
	image := flags.String("image", "", "run this image instead of the app's")
	appImage := flags.Bool("app-image", false, "drop the image override")
	memory := flags.Int("memory", 0, "memory limit in MB; 0 is the app's")
	cpus := flags.Float64("cpus", 0, "CPU limit; 0 is the app's")
	pids := flags.Int("pids", 0, "process limit; 0 is the app's")
	disk := flags.Int("disk", 0, "data limit in GB (zfs volumes only); 0 is the app's")
	canary := flags.String("canary", "", "with set: true makes the tenant take its app's rollouts first, false not")
	hold := flags.String("hold", "", "with set: true leaves the tenant out of its app's rollouts, on the revision it runs; false lets it follow again")
	var env, unsetEnv, secrets, unsetSecrets listFlag
	flags.Var(&env, "env", "add or replace an env value: KEY=VALUE (repeatable)")
	flags.Var(&unsetEnv, "unset-env", "drop an env override: KEY (repeatable)")
	flags.Var(&secrets, "secret", "add a secret: KEY or KEY=KIND (repeatable)")
	flags.Var(&unsetSecrets, "unset-secret", "drop a secret override: KEY (repeatable)")
	positional, err := parseInterspersed(flags, args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return fmt.Errorf("usage: murder tenant %s NAME [options]\n\n%s", command, tenantUsage)
	}
	given := map[string]bool{}
	flags.Visit(func(f *flag.Flag) { given[f.Name] = true })
	name := positional[0]
	ctx := context.Background()
	client := api.NewClient(*socket)

	request := api.TenantRequest{App: *app}
	if command == "set" && given["node"] {
		return errors.New("-node only places a new tenant: move one with murder tenant move NAME -to NODE")
	}
	var flagsOnly api.TenantFlags
	for flagName, into := range map[string]**bool{"canary": &flagsOnly.Canary, "hold": &flagsOnly.Hold} {
		if !given[flagName] {
			continue
		}
		if command != "set" {
			return fmt.Errorf("-%s only makes sense with tenant set", flagName)
		}
		value, err := strconv.ParseBool(map[string]string{"canary": *canary, "hold": *hold}[flagName])
		if err != nil {
			return fmt.Errorf("-%s takes true or false", flagName)
		}
		*into = &value
	}
	if flagsOnly.Canary != nil || flagsOnly.Hold != nil {
		tenant, err := client.SetFlags(ctx, name, flagsOnly)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "%s: canary %v, held %v\n", name, tenant.Canary, tenant.Hold)
		// Only the flags given: nothing else to change.
		if len(given) == len(flagsOnlyNames(given)) {
			return nil
		}
	}
	if command == "set" {
		current, err := client.Get(ctx, name)
		if err != nil {
			return err
		}
		if !given["app"] {
			request.App = current.App
		}
		// The agent refuses the change if the tenant changed meanwhile,
		// instead of overwriting someone else's edit.
		request.Overrides, request.Generation = current.Overrides, current.Generation
	} else {
		if *app == "" {
			return errors.New("tenant create needs -app")
		}
		request.Node = *node
		if *appImage || len(unsetEnv) > 0 || len(unsetSecrets) > 0 {
			return errors.New("-app-image, -unset-env and -unset-secret only make sense with tenant set")
		}
		request.Name = name
	}
	overrides := &request.Overrides
	overrides.Env, overrides.Secrets = maps.Clone(overrides.Env), maps.Clone(overrides.Secrets)
	switch {
	case given["image"] && *appImage:
		return errors.New("-image and -app-image contradict each other")
	case given["image"]:
		overrides.Image = *image
	case *appImage:
		overrides.Image = ""
	}
	for _, key := range unsetEnv {
		delete(overrides.Env, key)
	}
	for _, pair := range env {
		key, value, ok := strings.Cut(pair, "=")
		if !ok || key == "" {
			return fmt.Errorf("-env %q: use KEY=VALUE", pair)
		}
		if overrides.Env == nil {
			overrides.Env = map[string]string{}
		}
		overrides.Env[key] = value
	}
	for _, key := range unsetSecrets {
		delete(overrides.Secrets, key)
	}
	for _, entry := range secrets {
		key, kind, _ := strings.Cut(entry, "=")
		if overrides.Secrets == nil {
			overrides.Secrets = spec.Secrets{}
		}
		overrides.Secrets[key] = spec.Secret{Kind: kind}
	}
	if given["memory"] {
		overrides.Limits.MemoryMB = *memory
	}
	if given["cpus"] {
		overrides.Limits.CPUs = *cpus
	}
	if given["pids"] {
		overrides.Limits.PIDs = *pids
	}
	if given["disk"] {
		overrides.Limits.DiskGB = *disk
	}

	var result api.Applied
	if command == "create" {
		result, err = client.Create(ctx, request)
	} else {
		result, err = client.Configure(ctx, name, request)
	}
	if err != nil {
		return err
	}
	archived := result.Tenant.Archived()
	switch {
	case command == "create":
		where := ""
		if result.Tenant.Elsewhere {
			where = " on node " + result.Tenant.Node
		}
		fmt.Fprintf(stdout, "%s created%s at %s, running app %s (%s)\n", name, where, result.Tenant.URL, result.Tenant.App, shortImage(result.Tenant.Spec.Image))
	case archived && result.Changed:
		fmt.Fprintf(stdout, "%s updated; it is archived and runs this when restored\n", name)
	case archived:
		fmt.Fprintf(stdout, "%s unchanged; it is archived\n", name)
	case result.Changed:
		fmt.Fprintf(stdout, "%s updated (%s)\n", name, shortImage(result.Tenant.Spec.Image))
	default:
		fmt.Fprintf(stdout, "%s unchanged\n", name)
	}
	if result.Note != "" {
		fmt.Fprintf(stdout, "note: %s\n", result.Note)
	}
	if archived {
		return nil // nothing runs until it is restored
	}
	return waitHealthy(ctx, client, result.Tenant, *wait)
}

// flagsOnlyNames are the given flags that set a tenant's rollout flags,
// or name the socket.
func flagsOnlyNames(given map[string]bool) []string {
	var names []string
	for _, name := range []string{"canary", "hold", "socket"} {
		if given[name] {
			names = append(names, name)
		}
	}
	return names
}
