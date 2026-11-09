package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/doctor"
	"github.com/PowerPenguini/murder/internal/setup"
	"golang.org/x/term"
)

// errProblems is doctor's error when a check failed: the report said why.
var errProblems = errors.New("the node has problems to fix")

// runDoctor checks how safely the node is set up and changes nothing. It
// needs neither the agent nor Docker running, and runs best as root.
func runDoctor(args []string) error {
	flags := flag.NewFlagSet("doctor", flag.ContinueOnError)
	configPath := flags.String("config", defaultConfig, "agent configuration file")
	fix := flags.Bool("fix", false, "first fix what is safe to on a running node, as setup does on a new one: SSH's passwords off (for whoever logs in with a key), Docker's live-restore, and on a node alone the host firewall")
	yes := flags.Bool("yes", false, "with -fix: ask nothing")
	flags.Usage = func() {
		fmt.Fprintln(flags.Output(), "Usage: murder doctor [-config file] [-fix [-yes]]")
		fmt.Fprintln(flags.Output(), "Checks the node's setup; changes nothing without -fix. Exits 1 if a check failed.")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	if *fix {
		if err := doctorFix(cfg, *configPath, *yes); err != nil {
			return err
		}
	}
	host := doctor.System(cfg)
	results := doctor.Check(context.Background(), cfg, *configPath, host)
	if report(os.Stdout, results, host.Privileged) {
		return errProblems
	}
	return nil
}

// report prints results, one a line with the fix below, then a count; it
// reports whether any check failed.
func report(out io.Writer, results []doctor.Result, privileged bool) bool {
	counts := map[doctor.Status]int{}
	for _, result := range results {
		counts[result.Status]++
		fmt.Fprintf(out, "%-4s  %-9s  %s\n", result.Status, result.Area, result.Message)
		if result.Fix != "" && result.Status != doctor.OK {
			fmt.Fprintf(out, "                 fix: %s\n", result.Fix)
		}
	}
	fmt.Fprintf(out, "\n%d failed, %d to look at, %d skipped, %d ok\n", counts[doctor.Failure], counts[doctor.Warning], counts[doctor.Skipped], counts[doctor.OK])
	if !privileged {
		fmt.Fprintln(out, "Run as root to check everything.")
	}
	return counts[doctor.Failure] > 0
}

// doctorFix shows what -fix would do, asks, and does it.
func doctorFix(cfg config.Agent, configPath string, yes bool) error {
	host := setup.System{}
	if !host.Root() {
		return errors.New("-fix changes the host: run it as root (sudo murder doctor -fix)")
	}
	ctx := context.Background()
	steps, notes := setup.Fixes(ctx, host, cfg, configPath)
	for _, note := range notes {
		fmt.Fprintln(stdout, "Note: "+note)
	}
	if len(steps) == 0 {
		fmt.Fprintln(stdout, "Nothing for -fix to do.")
		fmt.Fprintln(stdout)
		return nil
	}
	fmt.Fprintln(stdout, "Doctor will:")
	for _, step := range steps {
		fmt.Fprintln(stdout, "  - "+step.Say)
	}
	if !yes {
		if !term.IsTerminal(int(os.Stdin.Fd())) {
			return errors.New("-fix asks before it changes anything: run it in a terminal, or with -yes")
		}
		if !(asker{in: bufio.NewReader(stdin), out: stdout}).yes("\nGo on?", true) {
			return errors.New("nothing was changed")
		}
	}
	fmt.Fprintln(stdout)
	for _, step := range steps {
		fmt.Fprintln(stdout, "→ "+step.Say)
		var skipped setup.Skipped
		switch err := step.Do(ctx); {
		case errors.As(err, &skipped):
			fmt.Fprintln(stdout, "  skipped: "+skipped.Reason)
		case err != nil:
			return fmt.Errorf("%s: %w", step.Say, err)
		}
	}
	fmt.Fprintln(stdout)
	return nil
}
