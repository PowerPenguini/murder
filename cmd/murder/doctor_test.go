package main

import (
	"strings"
	"testing"

	"github.com/PowerPenguini/murder/internal/doctor"
)

// The report prints each finding with its fix (none for a passing one),
// counts them, and fails only on a failure.
func TestReport(t *testing.T) {
	var out strings.Builder
	failed := report(&out, []doctor.Result{
		{Status: doctor.OK, Area: "clock", Message: "the clock is synchronised", Fix: "never shown"},
		{Status: doctor.Warning, Area: "kernel", Message: "a setting", Fix: "sysctl --system"},
	}, true)
	text := out.String()
	if failed || !strings.Contains(text, "ok    clock      the clock is synchronised\n") || !strings.Contains(text, "fix: sysctl --system") || strings.Contains(text, "never shown") || strings.Contains(text, "Run as root") {
		t.Errorf("failed = %v:\n%s", failed, text)
	}
	out.Reset()
	if !report(&out, []doctor.Result{{Status: doctor.Failure, Area: "ssh", Message: "passwords"}}, false) || !strings.Contains(out.String(), "1 failed, 0 to look at, 0 skipped, 0 ok\nRun as root") {
		t.Errorf("a failure:\n%s", out.String())
	}
}
