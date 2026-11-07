// Command murder is the Murder agent and its command-line client.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
)

// release names the release this binary was built for, as its tag; empty
// in a build from source. The release workflow sets it.
var release string

const usage = `Usage: murder <command> [arguments]

Node commands:
  setup      set this host up as a node: services, config, key, database
  init       create the master key and the database for a node
  agent      run the agent (reconciler, local API and cluster)
  edge       serve the public listeners; SIGHUP replaces it without a gap
  panel      serve the administration panel
  admin      the devices that reach the panel through its tunnel
  firewall   load the tenant firewall (murder-firewall.service runs it at boot)
  doctor     check how safely the node is set up; changes nothing
  restore    put a backup in place of the node's database (agent stopped)
  cluster    show, start, join or leave a cluster of nodes
  node       show this node's identity key in its cluster
  key        show or rotate the master key of every node
  upgrade    install a new murder binary on every node, one after the other
  version    print this binary's hash and schema version

Client commands (talk to the local agent):
  app        apply, list, show and delete apps (specs shared by tenants)
  tenant     create, configure, suspend, archive, restore and purge tenants
  events     show the audit log: who changed what, and when
  access     the register of staff access to tenants' data: record, list
  reconcile  run one reconcile pass now and report its errors
  backup     save a copy of the agent's database to a directory (recorded
             in the access register, but for -scheduled)
  backups    back tenants up to an S3 bucket, list, verify and restore them

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
	case "setup":
		return runSetup(args[1:])
	case "init":
		return runInit(args[1:])
	case "agent":
		return runAgent(args[1:])
	case "edge":
		return runEdge(args[1:])
	case "panel":
		return runPanel(args[1:])
	case "admin":
		return runAdmin(args[1:])
	case "firewall":
		return runFirewall(args[1:])
	case "doctor":
		return runDoctor(args[1:])
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
	case "cluster":
		return runCluster(args[1:])
	case "node":
		return runNode(args[1:])
	case "key":
		return runKey(args[1:])
	case "upgrade":
		return runUpgrade(args[1:])
	case "backups":
		return runBackups(args[1:])
	case "access":
		return runAccess(args[1:])
	case "version":
		return runVersion(args[1:])
	case "-h", "--help", "help":
		fmt.Fprint(os.Stdout, usage)
		return nil
	default:
		fmt.Fprint(os.Stderr, usage)
		return fmt.Errorf("unknown command %q", args[0])
	}
}
