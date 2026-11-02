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
  drain <node> -to <target>
                      move every tenant of node, with its data, to target,
                      one after the other
  replace <old> <new> move every tenant of old to new, then take old out of
                      the cluster: a server swapped for a bigger or smaller
                      one, which joined first; switch it off afterwards
  failover <node> -powered-off
                      run the tenants of node, which you made sure is
                      powered off, from their replicas; refused while it
                      answers (with cluster.failover set, the leader does
                      this by itself, after powering the node off)

A node joins with the cluster's master key: copy /etc/murder/master.key
from a node of the cluster before "murder init". Every node runs tenants.
`

func runCluster(args []string) error {
	if len(args) == 0 || args[0] == "-h" || args[0] == "help" {
		fmt.Fprint(os.Stderr, clusterUsage)
		return flag.ErrHelp
	}
	flags := flag.NewFlagSet("cluster "+args[0], flag.ContinueOnError)
	socket := socketPath(flags)
	asJSON := flags.Bool("json", false, "print JSON")
	to := flags.String("to", "", "with drain: the node to move the tenants to")
	poweredOff := flags.Bool("powered-off", false, "with failover: the node is powered off, checked by you")
	rest, err := parseInterspersed(flags, args[1:])
	if err != nil {
		return err
	}
	client := api.NewClient(*socket)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var status cluster.Status
	switch command := args[0]; {
	case command == "status" && len(rest) == 0:
		status, err = client.ClusterStatus(ctx)
	case command == "bootstrap" && len(rest) == 0:
		status, err = client.Bootstrap(ctx)
	case command == "join" && len(rest) == 1:
		status, err = client.Join(ctx, rest[0])
	case command == "drain" && len(rest) == 1 && *to != "":
		return drain(client, rest[0], *to)
	case command == "replace" && len(rest) == 2:
		if err := drain(client, rest[0], rest[1]); err != nil {
			return err
		}
		removal, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := client.RemoveNode(removal, rest[0]); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "node %s runs no tenant and left the cluster: point its DNS records at %s and switch it off (its edge serves the tenants it knew until then, not ones created or moved since)\n", rest[0], rest[1])
		return nil
	case command == "failover" && len(rest) == 1:
		if !*poweredOff {
			return fmt.Errorf("failover runs node %s's tenants elsewhere: make sure it is powered off (its tenants must not run twice), then add -powered-off", rest[0])
		}
		outcome, err := client.FailOver(ctx, rest[0])
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "%d tenants of node %s run from their replicas: %v\n", len(outcome.Promoted), rest[0], outcome.Promoted)
		if len(outcome.Stranded) > 0 {
			fmt.Fprintf(stdout, "%d had no replica and stay on %s: %v\n", len(outcome.Stranded), rest[0], outcome.Stranded)
		}
		return nil
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
	fmt.Fprintf(stdout, "node %s at %s: %s, leader %s, applied log entry %d\n", status.Node, status.Address, status.State, leader, status.Applied)
	if !status.LastContact.IsZero() {
		fmt.Fprintf(stdout, "last heard from the leader %s ago\n", time.Since(status.LastContact).Round(time.Millisecond))
	}
	table := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "NODE\tADDRESS\tVOTE")
	for _, member := range status.Members {
		fmt.Fprintf(table, "%s\t%s\t%s\n", member.Name, member.Address, member.Suffrage)
	}
	return table.Flush()
}

// drain moves every tenant of node, live or archived, to target, one after
// the other, and stops at the first that does not move.
func drain(client *api.Client, node, target string) error {
	ctx := context.Background()
	tenants, err := client.List(ctx)
	if err != nil {
		return err
	}
	moved := 0
	for _, tenant := range tenants {
		if tenant.Node != node {
			continue
		}
		start := time.Now()
		if _, err := client.Move(ctx, tenant.Spec.Name, target); err != nil {
			return fmt.Errorf("move %s: %w (moved %d before it)", tenant.Spec.Name, err, moved)
		}
		moved++
		fmt.Fprintf(stdout, "%s moved to %s in %v\n", tenant.Spec.Name, target, time.Since(start).Round(time.Second))
	}
	fmt.Fprintf(stdout, "node %s runs no tenant now (%d moved)\n", node, moved)
	return nil
}
