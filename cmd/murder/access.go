package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/user"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/PowerPenguini/murder/internal/access"
	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
)

const accessUsage = `Usage: murder access <command> [options]

  log -tenant NAME -what TEXT -reason TEXT -ticket REF [-at TIME] [-offline]
                    record an access to a tenant's data made outside Murder,
                    such as a shell in its container or a look at its files
  list [-tenant NAME] [-since TIME] [-format table|csv|json]
                    the register, oldest first: a tenant's accesses and the
                    copies of the whole state (every tenant's), or all
  sync [-offline-file F]
                    record, through the agent, the accesses written to this
                    node's offline file while the agent could not record them;
                    the agent does it by itself every minute, this at once

Every read of a tenant's data through Murder (a secret, its logs, a backup,
a copy of the state) is recorded in the register first, and refused if it
cannot be: the agent records who, when, which tenant, what, why (-reason)
and at whose request (-ticket: the customer's ticket, or "incident" or
"legal", each perhaps followed by ":REF", when no customer asked).
-offline writes the access to the offline file instead (` + access.OfflineFile + `)
when the agent cannot record it; the agent records it once it can.
`

// reasonFlags are the -reason and -ticket of a command that reads a
// tenant's data.
type reasonFlags struct {
	reason, ticket *string
}

func addReasonFlags(flags *flag.FlagSet, what string) reasonFlags {
	return reasonFlags{
		reason: flags.String("reason", "", "with "+what+": why the data is read, for the access register"),
		ticket: flags.String("ticket", "", `with `+what+`: the customer's ticket the read answers, or "incident" or "legal" (perhaps ":REF" after)`),
	}
}

func (f reasonFlags) proof() access.Proof {
	return access.Proof{Purpose: *f.reason, Ticket: *f.ticket}
}

// request is the access of kind to tenant's what, for these flags; it is
// checked here first, so a missing -reason is said as such.
func (f reasonFlags) request(kind access.Kind, tenant, what string) (access.Request, error) {
	request := access.Request{Tenant: tenant, Kind: kind, What: what, Purpose: *f.reason, Ticket: *f.ticket}
	return request, request.Normalize()
}

// operator is the person at the terminal, as sudo names them: the audit
// log says "jan via root" rather than "root" alone.
func operator() string {
	if name := os.Getenv("SUDO_USER"); name != "" && name != "root" {
		return name
	}
	return ""
}

// asOperator is client acting for the person at the terminal.
func asOperator(client *api.Client) *api.Client {
	if name := operator(); name != "" {
		return client.WithActor(name)
	}
	return client
}

// localUser is the user this runs as, as the agent names it.
func localUser() string {
	if found, err := user.Current(); err == nil {
		return found.Username
	}
	return "uid " + strconv.Itoa(os.Getuid())
}

// localActor names the person at the terminal as the agent would: the
// user, and who sudo says they are.
func localActor() string {
	if name := operator(); name != "" {
		return name + " via " + localUser()
	}
	return localUser()
}

// recordAccess records an access through the agent before it is made.
// With offline, an agent that cannot record it (not reached, no quorum)
// leaves it written to the offline file instead, said loudly; a refusal
// of the access itself (a missing reason, say) is never taken offline.
func recordAccess(ctx context.Context, client *api.Client, request access.Request, offline bool, offlineFile string) (access.Entry, error) {
	if err := request.Normalize(); err != nil {
		return access.Entry{}, err
	}
	entry, err := asOperator(client).RecordAccess(ctx, request)
	if err == nil {
		return entry, nil
	}
	var answer *api.Error
	// Changes frozen (an upgrade under way, which a rollback could undo)
	// is the agent unable to record, not a refusal of the access.
	frozen := errors.As(err, &answer) && answer.Status == http.StatusConflict && strings.Contains(answer.Message, store.ErrFrozen.Error())
	switch {
	case errors.As(err, &answer) && answer.Status >= 400 && answer.Status < 500 && !frozen:
		return access.Entry{}, fmt.Errorf("the access register refused this access: %w", err)
	case !offline:
		return access.Entry{}, fmt.Errorf("the access register could not record this access, so it is not made: %w", err)
	}
	record, writeErr := access.WriteOffline(offlineFile, request, localActor(), operator(), time.Now())
	if writeErr != nil {
		return access.Entry{}, fmt.Errorf("the agent could not record the access (%w), nor could %s: %w", err, offlineFile, writeErr)
	}
	fmt.Fprintf(os.Stderr, `
WARNING: the agent could not record this access in the register:
  %v
It is written to %s on this host alone (record %s).
The agent records it by itself once it runs and the cluster answers again;
"murder access sync" on this host does it at once. Until then the register
misses this access.

`, err, offlineFile, record.ID)
	return access.Entry{Tenant: record.Tenant, Kind: record.Kind, What: record.What, Purpose: record.Purpose, Ticket: record.Ticket,
		At: record.At, Actor: record.Actor, Offline: record.ID}, nil
}

func runAccess(args []string) error {
	if len(args) == 0 || args[0] == "-h" || args[0] == "help" {
		fmt.Fprint(os.Stderr, accessUsage)
		return flag.ErrHelp
	}
	command, args := args[0], args[1:]
	flags := flag.NewFlagSet("access "+command, flag.ContinueOnError)
	socket := socketPath(flags)
	tenant := flags.String("tenant", "", "the tenant whose data was, or is, read")
	what := flags.String("what", "", "with log: what was done, such as \"docker exec into its container\"")
	reason := addReasonFlags(flags, "log")
	at := flags.String("at", "", "with log: when it was (RFC 3339), if not now")
	since := flags.String("since", "", "with list: only accesses recorded since (RFC 3339, or a date 2006-01-02)")
	format := flags.String("format", "table", "with list: table, csv or json")
	offline := flags.Bool("offline", false, "with log: if the agent cannot record it, write it to the offline file")
	offlineFile := flags.String("offline-file", access.OfflineFile, "the offline file of accesses not recorded yet")
	positional, err := parseInterspersed(flags, args)
	if err != nil {
		return err
	}
	if len(positional) > 0 {
		return fmt.Errorf("access %s takes no arguments, got %v", command, positional)
	}
	ctx := context.Background()
	client := api.NewClient(*socket)
	switch command {
	case "log":
		request, err := reason.request(access.Manual, *tenant, *what)
		if err != nil {
			return err
		}
		if *at != "" {
			when, err := time.Parse(time.RFC3339, *at)
			if err != nil {
				return fmt.Errorf("-at: %w", err)
			}
			if *offline {
				return errors.New("-at and -offline: an offline record is of an access now")
			}
			request.AccessedAt = when
		}
		entry, err := recordAccess(ctx, client, request, *offline, *offlineFile)
		if err != nil {
			return err
		}
		if entry.Offline == "" {
			fmt.Fprintf(stdout, "recorded access %d: %s\n", entry.ID, entry.Said())
		}
		return nil
	case "list":
		var from time.Time
		if *since != "" {
			if from, err = time.Parse(time.RFC3339, *since); err != nil {
				if from, err = time.Parse(time.DateOnly, *since); err != nil {
					return fmt.Errorf("-since: %q is neither RFC 3339 nor a date (2006-01-02)", *since)
				}
			}
		}
		entries, err := client.Accesses(ctx, *tenant, from)
		if err != nil {
			return err
		}
		return printAccesses(entries, *format)
	case "sync":
		return syncAccesses(ctx, client, *offlineFile)
	default:
		fmt.Fprint(os.Stderr, accessUsage)
		return fmt.Errorf("unknown access command %q", command)
	}
}

// accessColumns are the register's columns, as a customer's extract has
// them.
var accessColumns = []string{"id", "recorded_at", "accessed_at", "actor", "tenant", "kind", "what", "purpose", "ticket", "offline_record"}

func accessRow(e access.Entry) []string {
	tenant := e.Tenant
	if tenant == "" {
		tenant = "*"
	}
	return []string{strconv.FormatInt(e.ID, 10), e.At.UTC().Format(time.RFC3339), e.When().UTC().Format(time.RFC3339), e.Actor, tenant,
		string(e.Kind), e.What, e.Purpose, e.Ticket, e.Offline}
}

func printAccesses(entries []access.Entry, format string) error {
	switch format {
	case "json":
		return printJSON(entries)
	case "csv":
		out := csv.NewWriter(stdout)
		if err := out.Write(accessColumns); err != nil {
			return err
		}
		for _, entry := range entries {
			row := accessRow(entry)
			// A spreadsheet takes a cell that begins with =, +, - or @
			// as a formula: such text is quoted as text.
			for i, cell := range row {
				if cell != "" && (cell[0] == '=' || cell[0] == '+' || cell[0] == '-' || cell[0] == '@') {
					row[i] = "'" + cell
				}
			}
			if err := out.Write(row); err != nil {
				return err
			}
		}
		out.Flush()
		return out.Error()
	case "table":
		table := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(table, "ID\tACCESSED\tACTOR\tTENANT\tKIND\tWHAT\tPURPOSE\tTICKET")
		for _, entry := range entries {
			row := accessRow(entry)
			id := row[0]
			if entry.Offline != "" {
				id += " (offline)"
			}
			fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", id, row[2], row[3], row[4], row[5], orDash(row[6]), row[7], row[8])
		}
		return table.Flush()
	default:
		return fmt.Errorf("-format must be table, csv or json, not %q", format)
	}
}

// syncAccesses records the offline file's accesses through the agent,
// each once: one recorded before, by a sync that stopped halfway, is
// found by its record's ID.
func syncAccesses(ctx context.Context, client *api.Client, path string) error {
	records, damaged, err := access.ReadOffline(path)
	if err != nil {
		return err
	}
	// Damaged lines are no record to sync: they stay, for a person to read
	// and record by hand.
	var damagedErr error
	if len(damaged) > 0 {
		damagedErr = fmt.Errorf("%d lines of %s are no record (one cut short, say), kept there: read them, record each access with murder access log, then remove them", len(damaged), path)
	}
	if len(records) == 0 {
		fmt.Fprintf(stdout, "no accesses wait in %s\n", path)
		return damagedErr
	}
	known := map[string]bool{}
	tenants := map[string]bool{}
	for _, record := range records {
		if !tenants[record.Tenant] {
			tenants[record.Tenant] = true
			entries, err := client.Accesses(ctx, record.Tenant, time.Time{})
			if err != nil {
				return err
			}
			for _, entry := range entries {
				if entry.Offline != "" {
					known[entry.Offline] = true
				}
			}
		}
	}
	synced := map[string]bool{}
	var failed error
	for _, record := range records {
		if !known[record.ID] {
			// In the name of whom the record names, as the sync's user:
			// "jan via root", or "arch via root" for a record arch wrote
			// as themselves and root syncs.
			agent := client
			if record.Operator != "" {
				agent = client.WithActor(record.Operator)
			} else if record.Actor != "" && record.Actor != localUser() {
				agent = client.WithActor(record.Actor)
			}
			entry, err := agent.RecordAccess(ctx, record.Synced())
			if err != nil {
				failed = errors.Join(failed, fmt.Errorf("record %s: %w", record.ID, err))
				continue
			}
			fmt.Fprintf(stdout, "recorded access %d: %s\n", entry.ID, entry.Said())
		}
		synced[record.ID] = true
	}
	if err := access.KeepOffline(path, synced); err != nil {
		return errors.Join(failed, err)
	}
	if failed != nil {
		return errors.Join(fmt.Errorf("%d of %d accesses recorded; the rest stay in %s: %w", len(synced), len(records), path, failed), damagedErr)
	}
	fmt.Fprintf(stdout, "%d accesses recorded from %s\n", len(synced), path)
	return damagedErr
}
