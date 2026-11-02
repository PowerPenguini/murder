package main

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/panel"
)

// A panel whose binary was replaced restarts only once its jobs ended, and
// takes none meanwhile.
func TestThePanelRestartsOnceItsJobsEnded(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	jobs := &panel.Jobs{}
	release := make(chan struct{})
	if _, err := jobs.Start(panel.JobUpgrade, "upgrade the cluster", "jan", time.Now, log, func(context.Context, io.Writer) error {
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var stopped atomic.Bool
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		restartOnUpgrade(ctx, func() { stopped.Store(true) }, func() bool { return true }, jobs, time.Millisecond, log)
	}()
	time.Sleep(50 * time.Millisecond)
	if stopped.Load() {
		t.Fatal("the panel restarted while its upgrade ran")
	}
	close(release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the panel did not restart once the upgrade ended")
	}
	if !stopped.Load() {
		t.Fatal("restartOnUpgrade returned without stopping the panel")
	}
	if _, err := jobs.Start(panel.JobBackups, "backup", "jan", time.Now, log, func(context.Context, io.Writer) error { return nil }); err == nil {
		t.Fatal("a restarting panel began a job")
	}
}
