package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/cluster"
)

const clusterUsage = `Usage: murder cluster <command> [arguments]

  status              show the cluster as this node sees it
  bootstrap           start a cluster of this node alone, with its state
  join <address>      add this node, with a new database, to the cluster of
                      the node at address (host:port on the private network)
  remove <node>       take a node out of the cluster

A node joins with the cluster's master key: copy /etc/murder/master.key
from a node of the cluster before "murder init". For now a cluster runs one
worker; the other nodes are witnesses, which only vote.
`

func runCluster(args []string) error {
	if len(args) == 0 || args[0] == "-h" || args[0] == "help" {
		fmt.Fprint(os.Stderr, clusterUsage)
		return flag.ErrHelp
	}
	flags := flag.NewFlagSet("cluster "+args[0], flag.ContinueOnError)
	socket := socketPath(flags)
	asJSON := flags.Bool("json", false, "print JSON")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	client := api.NewClient(*socket)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var status cluster.Status
	var err error
	switch command, rest := args[0], flags.Args(); {
	case command == "status" && len(rest) == 0:
		status, err = client.ClusterStatus(ctx)
	case command == "bootstrap" && len(rest) == 0:
		status, err = client.Bootstrap(ctx)
	case command == "join" && len(rest) == 1:
		status, err = client.Join(ctx, rest[0])
	case command == "remove" && len(rest) == 1:
		if err := client.RemoveNode(ctx, rest[0]); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "removed node %s from the cluster\n", rest[0])
		return nil
	default:
		fmt.Fprint(os.Stderr, clusterUsage)
		return fmt.Errorf("unknown cluster command or arguments: %v", args)
	}
	if err != nil {
		return err
	}
	if *asJSON {
		return printJSON(status)
	}
	return printCluster(status)
}

func printCluster(status cluster.Status) error {
	leader := status.Leader
	if leader == "" {
		leader = "none"
	}
	fmt.Fprintf(stdout, "node %s (%s at %s): %s, leader %s, applied log entry %d\n", status.Node, status.Role, status.Address, status.State, leader, status.Applied)
	if !status.LastContact.IsZero() {
		fmt.Fprintf(stdout, "last heard from the leader %s ago\n", time.Since(status.LastContact).Round(time.Millisecond))
	}
	table := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "NODE\tROLE\tADDRESS\tVOTE")
	for _, member := range status.Members {
		role := member.Role
		if role == "" {
			role = "-"
		}
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\n", member.Name, role, member.Address, member.Suffrage)
	}
	return table.Flush()
}
