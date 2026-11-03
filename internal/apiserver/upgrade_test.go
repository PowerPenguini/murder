package apiserver

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
)

// Upgrades run code as root on every node: a member of the socket's group
// may not ask for one.
func TestOnlyRootUpgrades(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("runs as root")
	}
	// The agent runs as another user than this test's client.
	defer func(uid uint32) { agentUID = uid }(agentUID)
	agentUID++
	f := newAPIFixture(t)
	socket := filepath.Join(t.TempDir(), "agent.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: f.handler, ConnContext: ConnContext}
	go server.Serve(listener)
	t.Cleanup(func() { _ = server.Close() })
	client := api.NewClient(socket)
	ctx := context.Background()
	for name, call := range map[string]func() error{
		"freeze":   func() error { return client.Freeze(ctx, "upgrade") },
		"thaw":     func() error { return client.Thaw(ctx) },
		"thawOwn":  func() error { return client.ThawOwn(ctx, "upgrade") },
		"takeOver": func() error { return client.TakeOverFreeze(ctx, "upgrade to a", "upgrade to b") },
		"install":  func() error { return client.Install(ctx, "-", bytes.NewReader([]byte("x")), "abc", "") },
	} {
		var apiErr *api.Error
		if err := call(); !errors.As(err, &apiErr) || apiErr.Status != http.StatusForbidden {
			t.Errorf("%s by a non-root user: %v", name, err)
		}
	}
	if f.store.Frozen() != "" {
		t.Fatal("a non-root user froze changes")
	}
}

// An upgrade thaws only its own freeze, and takes another's over only by
// naming it; a thaw without a reason, as by hand, ends any freeze.
func TestAnUpgradeThawsOnlyItsOwnFreeze(t *testing.T) {
	f := newAPIFixture(t)
	socket := filepath.Join(t.TempDir(), "agent.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: f.handler, ConnContext: ConnContext}
	go server.Serve(listener)
	t.Cleanup(func() { _ = server.Close() })
	client := api.NewClient(socket)
	ctx := context.Background()
	conflict := func(err error) bool {
		var apiErr *api.Error
		return errors.As(err, &apiErr) && apiErr.Status == http.StatusConflict
	}
	if err := client.Freeze(ctx, "upgrade to a"); err != nil {
		t.Fatal(err)
	}
	if err := client.Freeze(ctx, "upgrade to b"); !conflict(err) {
		t.Fatalf("a freeze over another upgrade's: %v", err)
	}
	if err := client.ThawOwn(ctx, "upgrade to b"); !conflict(err) || f.store.Frozen() != "upgrade to a" {
		t.Fatalf("a thaw of another's freeze: %v, frozen %q", err, f.store.Frozen())
	}
	if err := client.TakeOverFreeze(ctx, "upgrade to x", "upgrade to b"); !conflict(err) || f.store.Frozen() != "upgrade to a" {
		t.Fatalf("a takeover of a freeze that is not there: %v, frozen %q", err, f.store.Frozen())
	}
	if err := client.TakeOverFreeze(ctx, "upgrade to a", "upgrade to b"); err != nil || f.store.Frozen() != "upgrade to b" {
		t.Fatalf("a takeover: %v, frozen %q", err, f.store.Frozen())
	}
	if err := client.ThawOwn(ctx, "upgrade to b"); err != nil || f.store.Frozen() != "" {
		t.Fatalf("a thaw of its own freeze: %v, frozen %q", err, f.store.Frozen())
	}
	if err := client.Freeze(ctx, "upgrade to c"); err != nil {
		t.Fatal(err)
	}
	if err := client.Thaw(ctx); err != nil || f.store.Frozen() != "" {
		t.Fatalf("a thaw by hand: %v, frozen %q", err, f.store.Frozen())
	}
	// The panel's thaw (?if=) ends the freeze it showed alone too: a
	// rotation's, taken since, stays.
	rotation := store.RotationFreeze + "n1"
	if err := client.Freeze(ctx, rotation); err != nil {
		t.Fatal(err)
	}
	if err := client.ThawIf(ctx, "upgrade to c"); !conflict(err) || f.store.Frozen() != rotation {
		t.Fatalf("the panel's thaw of a freeze it did not show: %v, frozen %q", err, f.store.Frozen())
	}
	if err := client.ThawOwn(ctx, "upgrade to c"); !conflict(err) || f.store.Frozen() != rotation {
		t.Fatalf("an upgrade's thaw of a rotation's freeze: %v, frozen %q", err, f.store.Frozen())
	}
	if err := client.ThawIf(ctx, rotation); err != nil || f.store.Frozen() != "" {
		t.Fatalf("a thaw of the freeze shown: %v, frozen %q", err, f.store.Frozen())
	}
}
