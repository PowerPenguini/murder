package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/panel"
)

// runPanel serves the administration panel, asking the local agent.
func runPanel(args []string) error {
	flags := flag.NewFlagSet("panel", flag.ContinueOnError)
	configPath := flags.String("config", defaultConfig, "agent configuration file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("process", "panel")
	server, err := panel.New(api.NewClient(cfg.Socket), log)
	if err != nil {
		return err
	}
	// Root only: the panel shows what the agent's socket protects.
	if err := os.MkdirAll(filepath.Dir(cfg.Panel.Socket), 0o755); err != nil {
		return err
	}
	if err := os.Remove(cfg.Panel.Socket); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	old := syscall.Umask(0o077)
	listener, err := net.Listen("unix", cfg.Panel.Socket)
	syscall.Umask(old)
	if err != nil {
		return err
	}
	httpServer := &http.Server{Handler: server.Handler(), ReadHeaderTimeout: 10 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdown)
	}()
	log.Info("the panel serves", "socket", cfg.Panel.Socket)
	if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	// Requests under way finish first.
	<-stopped
	return nil
}
