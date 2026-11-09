package main

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"golang.org/x/term"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/clusterops"
	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/reboot"
)

const nodeUsage = `Usage: murder node <command> [arguments]

  key [-config path]  print this node's identity key, as the cluster pins it:
                      for a node that lost its node.key, give the one its
                      agent made since to "murder cluster rekey <node> <key>"
                      on another node
  reboot [node] [-to target] [-stay] [-force] [-yes] [-wait 15m]
                      reboot a node's host (for a kernel or C library
                      update) with the least downtime: in a cluster, its
                      tenants move to other nodes first (all to -to, or
                      each to the node keeping its copy, else the one
                      running the fewest), it reboots, and once it is back
                      they move back (-stay leaves them). Run it on another
                      node, which waits for it; on the node itself, -stay
                      is needed. Refused while changes are frozen, when no
                      other node can take the tenants, when murder's units
                      do not start at boot, or when the cluster would lose
                      its majority meanwhile (-force reboots anyway). On a
                      node alone, its tenants stop until it is back: it
                      asks first (-yes does not)
`

func runNode(args []string) error {
	if len(args) == 0 || args[0] == "-h" || args[0] == "help" {
		fmt.Fprint(os.Stderr, nodeUsage)
		return flag.ErrHelp
	}
	if args[0] == "reboot" {
		return runNodeReboot(args[1:])
	}
	flags := flag.NewFlagSet("node "+args[0], flag.ContinueOnError)
	configPath := flags.String("config", defaultConfig, "agent configuration file, to find the cluster directory")
	rest, err := parseInterspersed(flags, args[1:])
	if err != nil {
		return err
	}
	if args[0] != "key" || len(rest) != 0 {
		fmt.Fprint(os.Stderr, nodeUsage)
		return fmt.Errorf("unknown node command or arguments: %v", args)
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	if cfg.Cluster == nil {
		return fmt.Errorf("%s has no cluster section: this node has no identity key", *configPath)
	}
	identity, err := cluster.ReadIdentity(cfg.Cluster.Dir)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "node %s: %s\n", cfg.Node, cluster.EncodeKey(identity.Public().(ed25519.PublicKey)))
	return nil
}

func runNodeReboot(args []string) error {
	flags := flag.NewFlagSet("node reboot", flag.ContinueOnError)
	flags.Usage = func() { fmt.Fprint(flags.Output(), nodeUsage); flags.PrintDefaults() }
	socket := socketPath(flags)
	to := flags.String("to", "", "the node every tenant moves to (each goes to its copy's node, or the one running the fewest, if unset)")
	stay := flags.Bool("stay", false, "leave the tenants where they moved once the node is back")
	force := flags.Bool("force", false, "reboot even though the cluster loses its majority meanwhile")
	yes := flags.Bool("yes", false, "on a node alone: confirm that its tenants stop until it is back")
	wait := flags.Duration("wait", 15*time.Minute, "how long the node may take to come back and be ready")
	rest, err := parseInterspersed(flags, args)
	if err != nil {
		return err
	}
	if len(rest) > 1 {
		flags.Usage()
		return flag.ErrHelp
	}
	node := ""
	if len(rest) == 1 {
		node = rest[0]
	}
	interactive := false
	if file, ok := stdin.(*os.File); ok && term.IsTerminal(int(file.Fd())) {
		interactive = true
	}
	opts := clusterops.RebootOptions{To: *to, Stay: *stay, Force: *force, Wait: *wait}
	return rebootNode(context.Background(), api.NewClient(*socket), node, opts, *yes, interactive)
}

// rebootNode reboots node: through the cluster, its tenants moved away and
// back, or, on a node alone, with its tenants stopped until it is back,
// once confirmed.
func rebootNode(ctx context.Context, client clusterops.RebootClient, node string, opts clusterops.RebootOptions, yes, interactive bool) error {
	err := clusterops.Reboot(ctx, client, node, opts, stdout)
	if !errors.Is(err, clusterops.ErrAlone) {
		return err
	}
	// Alone: this node, whatever its name, and nowhere for its tenants.
	self := "-"
	if view, err := client.Nodes(ctx); err == nil {
		if view.Frozen != "" {
			return fmt.Errorf("changes are frozen (%s): reboot once that is over", view.Frozen)
		}
		self = view.Self
	}
	if node != "" && node != "-" && node != self {
		return fmt.Errorf("this node, %s, is in no cluster with node %s: run the reboot on that node", self, node)
	}
	if opts.To != "" {
		return fmt.Errorf("node %s is in no cluster with another node: its tenants cannot move to %s", self, opts.To)
	}
	tenants, err := client.List(ctx)
	if err != nil {
		return err
	}
	var running []string
	for _, tenant := range tenants {
		if tenant.ArchivedAt.IsZero() && !tenant.Spec.Suspended {
			running = append(running, tenant.Spec.Name)
		}
	}
	// Whether it would come back, before anyone is asked.
	if err := client.Reboot(ctx, "-", reboot.Request{Check: true}); err != nil {
		return err
	}
	say := fmt.Sprintf("node %s runs alone, so nothing takes its tenants over: ", self)
	if len(running) == 0 {
		say += "it runs no tenant now, and its edge answers no request until it is back"
	} else {
		say += fmt.Sprintf("its %d tenants (%s) stop, and its edge answers no request, until it is back; murder's services and Docker start them again at boot", len(running), strings.Join(running, ", "))
	}
	switch {
	case yes:
		fmt.Fprintln(stdout, say)
	case interactive:
		fmt.Fprintln(stdout, say)
		if !(asker{in: bufio.NewReader(stdin), out: stdout}).yes("Reboot now?", false) {
			return errors.New("not rebooted")
		}
	default:
		return fmt.Errorf("%s. Add -yes to reboot it", say)
	}
	if err := client.Reboot(ctx, "-", reboot.Request{Away: opts.Wait}); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "node %s reboots now\n", self)
	return nil
}
