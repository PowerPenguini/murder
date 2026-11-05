package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/caddyserver/certmagic"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/domains"
	"github.com/PowerPenguini/murder/internal/edge"
	"github.com/PowerPenguini/murder/internal/secrets"
	"github.com/PowerPenguini/murder/internal/store"
)

// newDomainManager has the leader check tenants' own domains and order
// and renew their certificates, from the CA the edge's wildcard comes from.
func newDomainManager(cfg config.Agent, st *store.SQLite, key *secrets.Key, member *cluster.Cluster, log *slog.Logger) *domains.Manager {
	_, port, err := net.SplitHostPort(cfg.Edge.HTTP)
	if err != nil || port == "" {
		port = "80"
	}
	manager := &domains.Manager{
		Store: st, Key: key, BaseDomain: cfg.BaseDomain, Log: log,
		Probe: domains.ProbeEdges(func() []string { return domains.EdgeAddresses(st.Nodes(), port) }, log),
	}
	switch cfg.Edge.TLS.Mode {
	case edge.TLSACME:
		manager.CA, manager.Email = cfg.Edge.TLS.CA, cfg.Edge.TLS.Email
		if manager.CA == "" {
			manager.CA = certmagic.LetsEncryptProductionCA
		}
	case edge.TLSInternal:
		manager.NoCA = "it points at the cluster, but no certificate is ordered: edge.tls.mode is internal, not acme, on the leader"
	default:
		manager.NoCA = "it points at the cluster and is served over plain HTTP: edge.tls is off on the leader, so no certificate is ordered"
	}
	if member != nil {
		manager.Leader = member.Leader
		manager.Ready = func(ctx context.Context) error {
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			return member.EveryNodeRuns(ctx, store.DomainsSchema)
		}
	}
	return manager
}

const domainUsage = `Usage: murder tenant domain <command> TENANT [DOMAIN]

  add TENANT DOMAIN          give a tenant a host name of its own, such as
                             app.customer.com; prints the DNS record to create
  list TENANT                the tenant's domains, their state and certificates
  check TENANT DOMAIN        check its DNS now, as after changing its records
  remove TENANT DOMAIN -yes  take a domain from a tenant, with its certificate:
                             requests for it are answered 404 at once

A domain is routed once its DNS points at the cluster: a CNAME to the
tenant's own name, or A and AAAA records with the nodes' public addresses.
The cluster's leader checks it, then orders its certificate over ACME
(HTTP-01, so port 80 must reach every node) and renews it 30 days before it
expires. -json prints JSON.
`

// runDomain serves "murder tenant domain".
func runDomain(args []string) error {
	if len(args) == 0 || args[0] == "-h" || args[0] == "help" {
		fmt.Fprint(os.Stderr, domainUsage)
		return flag.ErrHelp
	}
	command, args := args[0], args[1:]
	flags := flag.NewFlagSet("tenant domain "+command, flag.ContinueOnError)
	socket := socketPath(flags)
	yes := flags.Bool("yes", false, "with remove: confirm")
	asJSON := flags.Bool("json", false, "print JSON")
	positional, err := parseInterspersed(flags, args)
	if err != nil {
		return err
	}
	need := func(count int) error {
		if len(positional) != count {
			return fmt.Errorf("tenant domain %s needs %d argument(s), got %d\n\n%s", command, count, len(positional), domainUsage)
		}
		return nil
	}
	ctx := context.Background()
	client := api.NewClient(*socket)
	switch command {
	case "list":
		if err := need(1); err != nil {
			return err
		}
		list, err := client.Domains(ctx, positional[0])
		if err != nil {
			return err
		}
		if *asJSON {
			return printJSON(list)
		}
		return printDomains(list)
	case "add":
		if err := need(2); err != nil {
			return err
		}
		added, err := client.AddDomain(ctx, positional[0], positional[1])
		if err != nil {
			return err
		}
		if *asJSON {
			return printJSON(added)
		}
		domain := added.Domain
		if !added.Added {
			fmt.Fprintf(stdout, "%s has %s already (%s)\n", domain.Tenant, domain.Name, domain.State)
			return nil
		}
		fmt.Fprintf(stdout, "%s added to %s. Point it at the cluster with:\n", domain.Name, domain.Tenant)
		printRecords(domain.Records)
		fmt.Fprintf(stdout, "The cluster checks it, then asks for its certificate: \"murder tenant domain list %s\" follows it.\n", domain.Tenant)
		return nil
	case "check":
		if err := need(2); err != nil {
			return err
		}
		if err := client.CheckDomain(ctx, positional[0], positional[1]); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "%s is checked within seconds: \"murder tenant domain list %s\" shows what was found\n", positional[1], positional[0])
		return nil
	case "remove":
		if err := need(2); err != nil {
			return err
		}
		if !*yes {
			return errors.New("remove stops serving the domain at once and drops its certificate; add -yes to confirm")
		}
		if err := client.RemoveDomain(ctx, positional[0], positional[1]); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "%s removed from %s\n", positional[1], positional[0])
		return nil
	default:
		fmt.Fprint(os.Stderr, domainUsage)
		return fmt.Errorf("unknown tenant domain command %q", command)
	}
}

func printRecords(records []domains.Record) {
	table := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	for _, record := range records {
		fmt.Fprintf(table, "  %s\t%s\t%s\n", record.Name, record.Type, record.Value)
	}
	_ = table.Flush()
}

func printDomains(list []api.Domain) error {
	if len(list) == 0 {
		fmt.Fprintln(stdout, "no domains")
		return nil
	}
	table := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "DOMAIN\tSTATE\tROUTED\tCERTIFICATE\tDNS RECORD\tDETAIL")
	for _, domain := range list {
		routed, certificate := "no", "-"
		if domain.Routed() {
			routed = "yes"
		}
		if !domain.NotAfter.IsZero() {
			certificate = "until " + domain.NotAfter.Local().Format(time.DateOnly)
		}
		var records []string
		for _, record := range domain.Records {
			records = append(records, record.Type+" "+record.Value)
		}
		detail := domain.Detail
		if !domain.NextAt.IsZero() && domain.State != store.DomainActive {
			detail = strings.TrimPrefix(detail+"; next check "+domain.NextAt.Local().Format(time.TimeOnly), "; ")
		}
		name := domain.Name
		if domain.Display != "" {
			name += " (" + domain.Display + ")"
		}
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\n", name, domain.State, routed, certificate, strings.Join(records, ", "), detail)
	}
	return table.Flush()
}
