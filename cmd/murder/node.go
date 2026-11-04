package main

import (
	"crypto/ed25519"
	"flag"
	"fmt"
	"os"

	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/config"
)

const nodeUsage = `Usage: murder node <command> [arguments]

  key [-config path]  print this node's identity key, as the cluster pins it:
                      for a node that lost its node.key, give the one its
                      agent made since to "murder cluster rekey <node> <key>"
                      on another node
`

func runNode(args []string) error {
	if len(args) == 0 || args[0] == "-h" || args[0] == "help" {
		fmt.Fprint(os.Stderr, nodeUsage)
		return flag.ErrHelp
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
