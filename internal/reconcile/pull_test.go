package reconcile

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// A failed pull is retried only when its delay is up, the delay doubles up
// to its cap, and a pull that works forgets the failures.
func TestPullRetriesWithGrowingDelays(t *testing.T) {
	clock := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	attempts := 0
	failing := true
	p := newPulls(context.Background(), func(context.Context, string) error {
		attempts++
		if failing {
			return errors.New("registry down")
		}
		return nil
	})
	p.now = func() time.Time { return clock }
	attempt := func() error {
		err := p.fetch("app@sha256:1")
		p.wait()
		return err
	}
	if err := attempt(); !errors.Is(err, errPulling) || attempts != 1 {
		t.Fatalf("first fetch = %v after %d attempts", err, attempts)
	}
	for _, delay := range []time.Duration{10 * time.Second, 20 * time.Second, 40 * time.Second} {
		if err := attempt(); err == nil || !strings.Contains(err.Error(), "registry down") {
			t.Fatalf("fetch before the retry = %v", err)
		}
		clock = clock.Add(delay - time.Millisecond)
		if err := attempt(); errors.Is(err, errPulling) {
			t.Fatalf("retried %v early", time.Millisecond)
		}
		clock = clock.Add(time.Millisecond)
		before := attempts
		if err := attempt(); !errors.Is(err, errPulling) || attempts != before+1 {
			t.Fatalf("retry after %v = %v (%d attempts)", delay, err, attempts)
		}
	}
	p.mu.Lock()
	p.images["app@sha256:1"].failures = 40
	p.mu.Unlock()
	clock = clock.Add(time.Hour)
	_ = attempt()
	if retryAt := p.images["app@sha256:1"].retryAt; retryAt.Sub(clock) != pullRetryMax {
		t.Fatalf("delay after many failures = %v", retryAt.Sub(clock))
	}
	failing = false
	clock = clock.Add(pullRetryMax)
	if err := attempt(); !errors.Is(err, errPulling) || len(p.images) != 0 {
		t.Fatalf("a pull that works left state behind: %v, %+v", err, p.images)
	}
}

// Failures of images no tenant wants are forgotten; one being pulled stays.
func TestPullsKeepOnlyWantedImages(t *testing.T) {
	release := make(chan struct{})
	p := newPulls(context.Background(), func(_ context.Context, image string) error {
		if image == "slow" {
			<-release
		}
		return errors.New("registry down")
	})
	_ = p.fetch("old")
	_ = p.fetch("slow")
	p.mu.Lock()
	for p.images["old"].running {
		p.mu.Unlock()
		time.Sleep(time.Millisecond)
		p.mu.Lock()
	}
	p.mu.Unlock()
	p.keep(map[string]bool{"new": true})
	p.mu.Lock()
	_, old := p.images["old"]
	_, slow := p.images["slow"]
	p.mu.Unlock()
	if old || !slow {
		t.Fatalf("after keep: old %v, slow %v", old, slow)
	}
	close(release)
	p.wait()
}
