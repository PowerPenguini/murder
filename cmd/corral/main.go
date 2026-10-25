// Command corral is the Corral agent and its command-line client.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
)

const usage = `Usage: corral <command> [arguments]

Node commands:
  init       create the master key and state directory for a node
  agent      run the agent (reconciler, edge proxy and local API)

Client commands (talk to the local agent):
  apply      create and update tenants from a fleet file
  tenant     inspect, suspend, archive, restore and purge tenants
  reconcile  run one reconcile pass now and report its errors

Run "corral <command> -h" for a command's options.
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(os.Stderr, "corral:", err)
		}
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return flag.ErrHelp
	}
	switch args[0] {
	case "init":
		return runInit(args[1:])
	case "agent":
		return runAgent(args[1:])
	case "apply":
		return runApply(args[1:])
	case "tenant":
		return runTenant(args[1:])
	case "reconcile":
		return runReconcile(args[1:])
	case "-h", "--help", "help":
		fmt.Fprint(os.Stdout, usage)
		return nil
	default:
		fmt.Fprint(os.Stderr, usage)
		return fmt.Errorf("unknown command %q", args[0])
	}
}
