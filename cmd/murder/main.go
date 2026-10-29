// Command murder is the Murder agent and its command-line client.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
)

const usage = `Usage: murder <command> [arguments]

Node commands:
  init       create the master key and the database for a node
  agent      run the agent (reconciler, edge proxy and local API)
  firewall   load the tenant firewall (murder-firewall.service runs it at boot)
  restore    put a backup in place of the node's database (agent stopped)

Client commands (talk to the local agent):
  app        apply, list, show and delete apps (specs shared by tenants)
  tenant     create, configure, suspend, archive, restore and purge tenants
  events     show the audit log: who changed what, and when
  reconcile  run one reconcile pass now and report its errors
  backup     save a copy of the agent's database to a directory

Run "murder <command> -h" for a command's options.
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(os.Stderr, "murder:", err)
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
	case "firewall":
		return runFirewall(args[1:])
	case "app":
		return runApp(args[1:])
	case "events":
		return runEvents(args[1:])
	case "tenant":
		return runTenant(args[1:])
	case "reconcile":
		return runReconcile(args[1:])
	case "backup":
		return runBackup(args[1:])
	case "restore":
		return runRestore(args[1:])
	case "-h", "--help", "help":
		fmt.Fprint(os.Stdout, usage)
		return nil
	default:
		fmt.Fprint(os.Stderr, usage)
		return fmt.Errorf("unknown command %q", args[0])
	}
}
