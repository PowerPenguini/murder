package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net/netip"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/clusterops"
)

const clusterUsage = `Usage: murder cluster <command> [arguments]

  status              show the cluster as this node sees it
  invite [-valid 1h]  let a new host join: prints the command to run on it
                      (sudo murder setup -join …), valid once
  bootstrap [-restored]
                      start a cluster of this node alone, with its state;
                      -restored rebuilds one, under a new name, from a
                      database a backup restored (the other nodes join
                      again with invitations)
  join <invitation>   add this node, with a new database and the cluster's
                      master key, to the cluster an invitation names (made
                      by "murder cluster invite" on a node of it; - reads
                      it from standard input)
  remove <node>       take a node out of the cluster
  rekey <node> <key>  give a node that lost its identity key (node.key in
                      its cluster directory) the new one, as
                      "murder node key" prints it on that node: it keeps
                      its place, tenants and data
  drain <node> -to <target>
                      move every tenant of node, with its data, to target,
                      one after the other
  balance [-dry-run]  move tenants, one after the other, from the nodes
                      running the most to those running the fewest, until
                      none runs two more than another
  replace <old> <new> move every tenant of old to new, then take old out of
                      the cluster: a server swapped for a bigger or smaller
                      one, which joined first; switch it off afterwards
  unlock [<lease>]    say which operation holds the cluster (a drain, a
                      balance, a reboot, a replace); with its lease, as
                      that prints it, take the cluster from it: for one
                      whose command died, which would otherwise hold it
                      until its lease expires (2 minutes after its last
                      renewal), or one that must stop
  failover <node> -powered-off
                      run the tenants of node, which you made sure is
                      powered off, from their replicas; refused while it
                      answers (with cluster.failover set, the leader does
                      this by itself, after powering the node off)

A node joins with an invitation, which admits its name and identity key:
"murder setup -join" sets a host up with one, master key included; on a
node set up by hand, copy /etc/murder/master.key from a node of the
cluster before "murder init", then "murder cluster join". Every node runs
tenants.

One operation changes the cluster at a time: a drain, balance, replace or
node reboot holds the cluster's operation lease while it runs, and an
upgrade or a master key rotation freezes changes; each refuses to begin
while another runs, and says which. A single tenant move or a node's
removal goes on beside nothing of these either.
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
	valid := flags.Duration("valid", time.Hour, "with invite: how long the invitation is valid (up to 24h)")
	dryRun := flags.Bool("dry-run", false, "with balance: only print the moves")
	restored := flags.Bool("restored", false, "with bootstrap: rebuild the cluster from a database a backup restored")
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
	case command == "invite" && len(rest) == 0:
		invitation, err := client.Invite(ctx, *valid)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "On the new host, as root, with murder installed:\n\n  murder setup -join %s\n\n", invitation.Invitation)
		fmt.Fprintf(stdout, "It joins it to the cluster as a node like this one. The invitation is valid once,\nuntil %s, and hands the cluster's master key to whoever brings it:\nsend it only over a channel you trust. On a host others log in to, run\n`murder setup -join -` and paste it, so it is not in the process list.\n", invitation.Expires.Local().Format(time.DateTime))
		return nil
	case command == "bootstrap" && len(rest) == 0 && *restored:
		status, err = client.BootstrapRestored(ctx)
	case command == "bootstrap" && len(rest) == 0:
		status, err = client.Bootstrap(ctx)
	case command == "rekey" && len(rest) == 2:
		if err := client.ReplaceNodeKey(ctx, rest[0], rest[1]); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "node %s has its new identity key: it is a member again\n", rest[0])
		return nil
	case command == "join" && len(rest) == 1:
		invitation := rest[0]
		if invitation == "-" {
			line, readErr := bufio.NewReader(stdin).ReadString('\n')
			if readErr != nil && line == "" {
				return fmt.Errorf("read the invitation from standard input: %w", readErr)
			}
			invitation = strings.TrimSpace(line)
		}
		if _, parseErr := cluster.ParseInvite(invitation); parseErr != nil {
			if _, addressErr := netip.ParseAddrPort(invitation); addressErr == nil {
				return fmt.Errorf("a node no longer joins with an address and the master key alone: run `murder cluster invite` on a node of the cluster, then `murder cluster join <invitation>` here")
			}
			return parseErr
		}
		status, err = client.JoinInvited(ctx, invitation)
	case command == "balance" && len(rest) == 0:
		return clusterops.Balance(context.Background(), client, *dryRun, stdout)
	case command == "drain" && len(rest) == 1 && *to != "":
		return drain(client, rest[0], *to)
	case command == "replace" && len(rest) == 2:
		remove := func(ctx context.Context, node string) error {
			removal, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()
			return client.RemoveNode(removal, node)
		}
		if err := clusterops.Replace(context.Background(), client, remove, rest[0], rest[1], stdout); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "node %s runs no tenant and left the cluster: point its DNS records at %s and switch it off (its edge serves the tenants it knew until then, not ones created or moved since)\n", rest[0], rest[1])
		return nil
	case command == "unlock" && len(rest) <= 1:
		return unlock(ctx, client, rest)
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
	if status.Lease != nil {
		fmt.Fprintf(stdout, "an operation holds the cluster: %s\n", status.Lease)
	}
	if !status.LastContact.IsZero() {
		fmt.Fprintf(stdout, "last heard from the leader %s ago\n", time.Since(status.LastContact).Round(time.Millisecond))
	}
	table := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "NODE\tADDRESS\tVOTE\tIDENTITY KEY")
	for _, member := range status.Members {
		key := member.Key
		if len(key) > 12 {
			key = key[:12] + "…"
		}
		switch {
		case member.Leaving && key == "":
			key = "leaving (taken by name)"
		case member.Leaving:
			key = "leaving (" + key + ")"
		case key == "":
			key = "none yet (taken by name)"
		}
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\n", member.Name, member.Address, member.Suffrage, key)
	}
	return table.Flush()
}

// drain moves every tenant of node to target, telling each move.
func drain(client *api.Client, node, target string) error {
	return clusterops.Drain(context.Background(), client, node, target, stdout)
}

// unlock says which operation holds the cluster's lease; with the lease
// named, it breaks that one, and only that one: a lease taken since, by
// another operation, stays.
func unlock(ctx context.Context, client *api.Client, args []string) error {
	if len(args) == 0 {
		lease, err := client.Lease(ctx)
		if err != nil {
			return err
		}
		switch {
		case lease.ID == "":
			fmt.Fprintln(stdout, "no operation holds the cluster")
		case !lease.Held:
			fmt.Fprintf(stdout, "no operation holds the cluster: the last lease expired unreleased (%s)\n", lease.Lease)
		default:
			fmt.Fprintf(stdout, "%s\n\nIt renews its lease while it runs. If its command is gone (killed, its node down), the lease\nexpires by itself; to take the cluster from it now, or to stop it where it is, run:\n\n  murder cluster unlock %s\n", lease.Lease, lease.ID)
		}
		return nil
	}
	lease, err := client.BreakLease(ctx, args[0])
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "broke the lease of %s\nif it still runs, it stops at its next renewal or request; check where its tenants are with \"murder tenant list\"\n", lease)
	return nil
}
