package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/edge"
	"github.com/PowerPenguini/murder/internal/panel"
	"github.com/PowerPenguini/murder/internal/tunnel"
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
	client := api.NewClient(cfg.Socket)
	server, err := panel.New(client, log)
	if err != nil {
		return err
	}
	server.Name = cfg.Panel.Name
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
	go restartOnUpgrade(ctx, stop, log)
	tunnelDone := make(chan error, 1)
	if t := cfg.Panel.Tunnel; t != nil {
		go func() {
			err := serveTunnel(ctx, cfg, client, server.Handler(), log)
			// The panel stops, so the failure is in the log and systemd
			// starts it again.
			if err != nil && ctx.Err() == nil {
				log.Error("the panel's tunnel stopped", "error", err)
				stop()
			}
			tunnelDone <- err
		}()
	}
	log.Info("the panel serves", "socket", cfg.Panel.Socket)
	if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	// Requests under way finish first.
	<-stopped
	if cfg.Panel.Tunnel != nil {
		return <-tunnelDone
	}
	return nil
}

// restartOnUpgrade stops the panel once its binary was replaced, as an
// upgrade does: systemd starts the new one.
func restartOnUpgrade(ctx context.Context, stop func(), log *slog.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
		}
		if exe, err := os.Readlink("/proc/self/exe"); err == nil && strings.HasSuffix(exe, " (deleted)") {
			log.Info("the murder binary was replaced: the panel restarts with it")
			stop()
			return
		}
	}
}

// serveTunnel serves the panel inside its WireGuard tunnel, over HTTPS
// with the edge's wildcard certificate.
func serveTunnel(ctx context.Context, cfg config.Agent, client *api.Client, handler http.Handler, log *slog.Logger) error {
	t := cfg.Panel.Tunnel
	private, err := tunnel.LoadKey(t.KeyFile)
	if err != nil {
		return err
	}
	// The edge's certificate; with the internal CA, once the edge made it.
	var certificates *edge.Certificates
	for attempt := 0; ; attempt++ {
		certificates, err = edge.SharedTLS(ctx, cfg.Edge.TLS, cfg.BaseDomain, log)
		if err == nil {
			break
		}
		if attempt == 5 {
			log.Warn("the panel waits for the edge's certificate", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(2 * time.Second):
		}
	}
	address, err := netip.ParseAddr(t.Address)
	if err != nil {
		return err
	}
	return panel.ServeTunnel(ctx, client, panel.TunnelConfig{
		PrivateKey: private, Address: address, Port: t.Port, Name: cfg.Panel.Name, TLS: certificates.Config,
	}, handler, log)
}
