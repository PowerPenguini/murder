package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/store"
)

// init never makes a master key for a database that exists: its secrets
// are sealed with the original one.
func TestInitRefusesANewKeyForADatabase(t *testing.T) {
	dir := t.TempDir()
	cfg := nodeConfig(t, dir, "")
	if err := store.Init(filepath.Join(dir, "murder.db"), "n1"); err != nil {
		t.Fatal(err)
	}
	err := runInit([]string{"-config", cfg})
	if err == nil || !strings.Contains(err.Error(), "restore the original key") {
		t.Fatalf("init without the key: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "master.key")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a new key was made: %v", err)
	}
}

// Setup waits for the agent on the socket of the config it installed, not
// the CLI's default.
func TestSetupWaitsOnTheConfiguredSocket(t *testing.T) {
	socket, _ := serveAgent(t, func(_ string, w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("[]"))
	})
	t.Setenv("MURDER_SOCKET", filepath.Join(socketDir(t), "no-agent.sock"))
	cfg := nodeConfig(t, t.TempDir(), "socket: "+socket+"\n")
	capture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := install(ctx, nil, cfg); err != nil {
		t.Fatal(err)
	}
}
