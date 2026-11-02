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
		"freeze":  func() error { return client.Freeze(ctx, "upgrade") },
		"thaw":    func() error { return client.Thaw(ctx) },
		"install": func() error { return client.Install(ctx, "-", bytes.NewReader([]byte("x")), "abc") },
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
