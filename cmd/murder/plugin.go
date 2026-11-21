package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/plugins"
)

const pluginUsage = `Usage: murder plugin <command> [arguments]

  apply FILE...                install plugins, or change them, from files such
                               as plugins/openai.yaml: each is named after its file,
                               has its image pinned, and its manifest read from
                               the image's label (Murder's own are
                               ghcr.io/powerpenguini/murder-openai and -here)
  list                         the plugins: kind, key, limits and the apps that list them
  show NAME                    a plugin's spec, and its image's manifest
  delete NAME -yes             remove a plugin no app lists, with its keys and
                               tenants' limits; what tenants used is kept
  key NAME -file PATH          give a plugin the operator's key, read from a file
                               (- reads standard input); -tenant T gives tenant T
                               a key of its own instead; -remove takes it away
  limit NAME -tenant T LIMIT...  give a tenant limits of its own in place of the
                               plugin's, such as tokens=5M/month or
                               requests=1000/day:soft; -none for no limits at
                               all, -reset for the plugin's again
  tenant TENANT                a tenant's plugins: the limits that hold, and what
                               it used today and this month
  usage                        what tenants used: -tenant, -plugin, -from and -to
                               (days, 2006-01-02; this month by default), -by
                               total, month or day, -format csv

A tenant's app lists the plugins its tenants may use (plugins: [openai]),
and reaches each at {{plugin.NAME.url}}, with any key: the edge passes the
request on with the plugin's. -json prints JSON.
`

// runPlugin serves "murder plugin".
func runPlugin(args []string) error {
	if len(args) == 0 || args[0] == "-h" || args[0] == "help" {
		fmt.Fprint(os.Stderr, pluginUsage)
		return flag.ErrHelp
	}
	command, args := args[0], args[1:]
	flags := flag.NewFlagSet("plugin "+command, flag.ContinueOnError)
	socket := socketPath(flags)
	asJSON := flags.Bool("json", false, "print JSON")
	yes := flags.Bool("yes", false, "with delete: confirm")
	tenant := flags.String("tenant", "", "with key, limit and usage: the tenant")
	plugin := flags.String("plugin", "", "with usage: the plugin")
	file := flags.String("file", "", "with key: the file that holds the key (- for standard input)")
	remove := flags.Bool("remove", false, "with key: take the key away")
	none := flags.Bool("none", false, "with limit: no limits at all for the tenant")
	reset := flags.Bool("reset", false, "with limit: the plugin's limits again")
	from := flags.String("from", "", "with usage: the first day, 2006-01-02")
	to := flags.String("to", "", "with usage: the last day, 2006-01-02")
	by := flags.String("by", "total", "with usage: total, month or day")
	format := flags.String("format", "table", "with usage: table or csv")
	positional, err := parseInterspersed(flags, args)
	if err != nil {
		return err
	}
	need := func(count int) error {
		if len(positional) != count {
			return fmt.Errorf("plugin %s needs %d argument(s), got %d\n\n%s", command, count, len(positional), pluginUsage)
		}
		return nil
	}
	ctx := context.Background()
	client := api.NewClient(*socket)
	switch command {
	case "apply":
		if len(positional) == 0 {
			return fmt.Errorf("plugin apply needs a file\n\n%s", pluginUsage)
		}
		return applyPlugins(ctx, client, positional, *asJSON)
	case "list":
		if err := need(0); err != nil {
			return err
		}
		list, err := client.Plugins(ctx)
		if err != nil {
			return err
		}
		if *asJSON {
			return printJSON(list)
		}
		return printPlugins(list)
	case "show":
		if err := need(1); err != nil {
			return err
		}
		shown, err := client.Plugin(ctx, positional[0])
		if err != nil {
			return err
		}
		return printJSON(shown)
	case "delete":
		if err := need(1); err != nil {
			return err
		}
		if !*yes {
			return errors.New("delete removes the plugin's keys and tenants' limits of it; add -yes to confirm")
		}
		if err := client.DeletePlugin(ctx, positional[0]); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "plugin %s deleted\n", positional[0])
		return nil
	case "key":
		if err := need(1); err != nil {
			return err
		}
		key := ""
		switch {
		case *remove && *file != "":
			return errors.New("plugin key takes -file or -remove, not both")
		case !*remove:
			if *file == "" {
				return errors.New("plugin key reads the key from a file, never from the command line: -file PATH, or -file - for standard input")
			}
			if key, err = readKey(*file); err != nil {
				return err
			}
		}
		if err := client.SetPluginKey(ctx, positional[0], *tenant, key); err != nil {
			return err
		}
		whose := "the operator's key"
		if *tenant != "" {
			whose = "tenant " + *tenant + "'s own key"
		}
		if *remove {
			fmt.Fprintf(stdout, "plugin %s: %s removed\n", positional[0], whose)
		} else {
			fmt.Fprintf(stdout, "plugin %s: %s set; every edge uses it within seconds\n", positional[0], whose)
		}
		return nil
	case "limit":
		if len(positional) < 1 || *tenant == "" {
			return fmt.Errorf("plugin limit needs a plugin and -tenant\n\n%s", pluginUsage)
		}
		var limits []plugins.Limit
		for _, text := range positional[1:] {
			limit, err := plugins.ParseLimit(text)
			if err != nil {
				return err
			}
			limits = append(limits, limit)
		}
		switch {
		case *reset && (*none || len(limits) > 0), *none && len(limits) > 0:
			return errors.New("plugin limit takes limits, -none or -reset: one of them")
		case !*reset && !*none && len(limits) == 0:
			return errors.New("plugin limit needs limits, such as tokens=5M/month, or -none or -reset")
		}
		if err := client.SetPluginLimits(ctx, positional[0], *tenant, limits, !*reset); err != nil {
			return err
		}
		switch {
		case *reset:
			fmt.Fprintf(stdout, "tenant %s: plugin %s's limits hold again\n", *tenant, positional[0])
		case *none:
			fmt.Fprintf(stdout, "tenant %s: no limits of plugin %s\n", *tenant, positional[0])
		default:
			fmt.Fprintf(stdout, "tenant %s: its own limits of plugin %s hold\n", *tenant, positional[0])
		}
		return nil
	case "tenant":
		if err := need(1); err != nil {
			return err
		}
		uses, err := client.TenantPlugins(ctx, positional[0])
		if err != nil {
			return err
		}
		if *asJSON {
			return printJSON(uses)
		}
		return printPluginUses(uses)
	case "usage":
		if err := need(0); err != nil {
			return err
		}
		if !slices.Contains([]string{"total", "month", "day"}, *by) {
			return fmt.Errorf("-by is total, month or day, not %q", *by)
		}
		usage, err := client.PluginUsage(ctx, *tenant, *plugin, *from, *to)
		if err != nil {
			return err
		}
		if *asJSON {
			return printJSON(usage)
		}
		return printPluginUsage(stdout, usage, *by, *format)
	default:
		fmt.Fprint(os.Stderr, pluginUsage)
		return fmt.Errorf("unknown plugin command %q", command)
	}
}

// applyPlugins reads and checks every file before it installs any.
func applyPlugins(ctx context.Context, client *api.Client, paths []string, asJSON bool) error {
	type file struct {
		name string
		spec plugins.Spec
	}
	var files []file
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		name := strings.TrimSuffix(strings.TrimSuffix(filepath.Base(path), ".yaml"), ".yml")
		if err := plugins.ValidName(name); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		spec, err := plugins.Decode(data)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		if err := spec.ValidateUnpinned(); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		files = append(files, file{name, spec})
	}
	var changes []api.PluginChange
	for _, f := range files {
		change, err := client.PutPlugin(ctx, f.name, f.spec)
		if err != nil {
			return fmt.Errorf("plugin %s: %w", f.name, err)
		}
		changes = append(changes, change)
		if asJSON {
			continue
		}
		switch {
		case !change.Changed:
			fmt.Fprintf(stdout, "plugin %s unchanged\n", f.name)
		case change.Plugin.CreatedAt.Equal(change.Plugin.UpdatedAt):
			fmt.Fprintf(stdout, "plugin %s installed (%s)\n", f.name, f.spec.Kind)
		default:
			fmt.Fprintf(stdout, "plugin %s changed\n", f.name)
		}
		if change.Note != "" {
			fmt.Fprintf(stdout, "  %s\n", change.Note)
		}
		if !change.Plugin.HasKey {
			fmt.Fprintf(stdout, "  it has no key yet: murder plugin key %s -file PATH\n", f.name)
		}
	}
	if asJSON {
		return printJSON(changes)
	}
	return nil
}

// readKey reads a key from a file, or standard input for -, without its
// trailing newline.
func readKey(path string) (string, error) {
	var data []byte
	var err error
	if path == "-" {
		data, err = io.ReadAll(io.LimitReader(os.Stdin, 8192))
	} else {
		data, err = os.ReadFile(path)
	}
	if err != nil {
		return "", err
	}
	key := strings.TrimSpace(string(data))
	if key == "" || strings.ContainsAny(key, "\r\n") {
		return "", fmt.Errorf("%s holds no key, or more than one line", path)
	}
	return key, nil
}

func printPlugins(list []api.Plugin) error {
	if len(list) == 0 {
		fmt.Fprintln(stdout, "no plugins: murder plugin apply plugins/NAME.yaml installs one")
		return nil
	}
	table := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "PLUGIN\tKIND\tKEY\tLIMITS\tAPPS")
	for _, p := range list {
		key := "none"
		if p.HasKey {
			key = "set"
		}
		limits := make([]string, 0, len(p.Spec.Limits))
		for _, limit := range p.Spec.Limits {
			limits = append(limits, limit.String())
		}
		kind := p.Spec.Kind
		if p.Manifest != nil {
			kind += " (" + strings.TrimSpace(p.Manifest.Name+" "+p.Manifest.Version) + ")"
		}
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\n", p.Name, kind, key, orDash(strings.Join(limits, " ")), orDash(strings.Join(p.Apps, ", ")))
	}
	return table.Flush()
}

func printPluginUses(uses []api.PluginUse) error {
	if len(uses) == 0 {
		fmt.Fprintln(stdout, "the tenant's app lists no plugins")
		return nil
	}
	for i, use := range uses {
		if i > 0 {
			fmt.Fprintln(stdout)
		}
		key, limits := "the plugin's key", "the plugin's limits"
		if use.OwnKey {
			key = "a key of its own"
		}
		if use.OwnLimits {
			limits = "limits of its own"
		} else if use.OwnKey {
			limits = "only limits of its own (it has its own key)"
		}
		granted := ""
		if !use.Granted {
			granted = "; its app does not list it, so it may not use it"
		}
		fmt.Fprintf(stdout, "%s: %s, %s%s\n", use.Plugin, key, limits, granted)
		table := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
		for _, limit := range use.Limits {
			state := ""
			if limit.Reached {
				state = "reached: further requests are refused"
				if !limit.Hard() {
					state = "reached (soft: requests still pass)"
				}
			}
			fmt.Fprintf(table, "  limit %s\t%s\tused %d of %d\t%s\n", limit.Metric, limit.Period, limit.Used, limit.Amount, state)
		}
		for _, used := range use.Today {
			fmt.Fprintf(table, "  today\t%s\t%d\t\n", used.Metric, used.Amount)
		}
		for _, used := range use.Month {
			fmt.Fprintf(table, "  this month\t%s\t%d\t\n", used.Metric, used.Amount)
		}
		if err := table.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// printPluginUsage prints counts summed by period: the whole range, each
// month or each day.
func printPluginUsage(w io.Writer, usage api.PluginUsage, by, format string) error {
	type row struct{ tenant, plugin, period, metric, detail string }
	sums := map[row]int64{}
	for _, count := range usage.Counts {
		period := usage.From + ".." + usage.To
		switch by {
		case "day":
			period = count.Day
		case "month":
			period = count.Day[:7]
		}
		sums[row{count.Tenant, count.Plugin, period, count.Metric, count.Detail}] += count.Amount
	}
	rows := slices.SortedFunc(maps.Keys(sums), func(a, b row) int {
		return strings.Compare(strings.Join([]string{a.tenant, a.plugin, a.period, a.metric, a.detail}, "\x00"), strings.Join([]string{b.tenant, b.plugin, b.period, b.metric, b.detail}, "\x00"))
	})
	if format == "csv" {
		out := csv.NewWriter(w)
		_ = out.Write([]string{"tenant", "plugin", "period", "metric", "detail", "amount"})
		for _, r := range rows {
			_ = out.Write([]string{r.tenant, r.plugin, r.period, r.metric, r.detail, strconv.FormatInt(sums[r], 10)})
		}
		out.Flush()
		return out.Error()
	}
	if format != "table" {
		return fmt.Errorf("-format is table or csv, not %q", format)
	}
	if len(rows) == 0 {
		fmt.Fprintf(w, "nothing used from %s to %s\n", usage.From, usage.To)
		return nil
	}
	table := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "TENANT\tPLUGIN\tPERIOD\tMETRIC\tDETAIL\tAMOUNT")
	for _, r := range rows {
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%d\n", r.tenant, r.plugin, r.period, r.metric, orDash(r.detail), sums[r])
	}
	return table.Flush()
}
