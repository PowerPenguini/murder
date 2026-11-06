package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/doctor"
)

// errProblems is doctor's error when a check failed: the report said why.
var errProblems = errors.New("the node has problems to fix")

// runDoctor checks how safely the node is set up and changes nothing. It
// needs neither the agent nor Docker running, and runs best as root.
func runDoctor(args []string) error {
	flags := flag.NewFlagSet("doctor", flag.ContinueOnError)
	configPath := flags.String("config", defaultConfig, "agent configuration file")
	flags.Usage = func() {
		fmt.Fprintln(flags.Output(), "Usage: murder doctor [-config file]")
		fmt.Fprintln(flags.Output(), "Checks the node's setup without changing it; exits 1 if a check failed.")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
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
