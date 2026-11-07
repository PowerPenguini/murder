package main

import (
	"context"
	"flag"
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/PowerPenguini/murder/internal/access"
	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
)

func runEvents(args []string) error {
	flags := flag.NewFlagSet("events", flag.ContinueOnError)
	socket := socketPath(flags)
	tenant := flags.String("tenant", "", "only events of this tenant")
	app := flags.String("app", "", "only events of this app")
	limit := flags.Int("limit", 50, "number of events (at most 1000)")
	before := flags.Int64("before", 0, "only events older than this ID (for the next page)")
	asJSON := flags.Bool("json", false, "print JSON")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() > 0 {
		return fmt.Errorf("events takes no arguments, got %v", flags.Args())
	}
	events, err := api.NewClient(*socket).Events(context.Background(), store.EventQuery{Tenant: *tenant, App: *app, Limit: *limit, Before: *before})
	if err != nil {
		return err
	}
	if *asJSON {
		return printJSON(events)
	}
	table := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "ID\tTIME\tACTOR\tACTION\tTENANT\tAPP\tDETAIL")
	for _, event := range events {
		// An access's detail is JSON, for the register: said as a line.
		if entry, ok := access.FromEvent(event); ok {
			event.Detail = entry.Said()
		}
		fmt.Fprintf(table, "%d\t%s\t%s\t%s\t%s\t%s\t%s\n", event.ID, event.At.Local().Format(time.DateTime), event.Actor, event.Action, event.Tenant, event.App, event.Detail)
	}
	return table.Flush()
}
