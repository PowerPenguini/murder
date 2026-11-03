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
	// Root reaches the socket's panel through SSH: no one signs in there.
	server.Access = func(actor string) panel.AccessAgent { return client.WithActor(actor) }
	server.Actions = func(actor string) panel.ActionAgent { return client.WithActor(actor) }
	server.Cluster = func(actor string) panel.ClusterAgent { return client.WithActor(actor) }
	server.Apps = func(actor string) panel.AppAgent { return client.WithActor(actor) }
	// Beside the database: /tmp may be small, in memory, or noexec.
	server.UploadDir = filepath.Join(filepath.Dir(cfg.Database), "panel-uploads")
	// What a panel stopped mid-upgrade left behind.
	if err := os.RemoveAll(server.UploadDir); err != nil {
		log.Warn("the panel's old uploads stay", "error", err)
	}
	if t := cfg.Panel.Tunnel; t != nil {
		server.TunnelNetwork, _ = netip.ParsePrefix(t.Network)
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
	// No whole-request bound, which an upgrade's upload would outlast:
	// the panel's handler bounds each body itself.
	httpServer := &http.Server{Handler: server.Handler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: panel.IdleTimeout}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// Asked to stop, the panel takes no more jobs, but serves on until the
	// change of the cluster that runs ends: an upgrade stopped halfway
	// leaves changes frozen.
	serving, stopServing := context.WithCancel(context.Background())
	defer stopServing()
	go func() {
		<-ctx.Done()
		server.Jobs.Close()
		if busy := server.Jobs.Busy(); busy != nil {
			log.Info("the panel stops once its job ends", "job", busy.Title, "id", busy.ID, "by", busy.By)
		}
		server.Jobs.WaitBusy()
		stopServing()
	}()
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		<-serving.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdown)
	}()
	tunnelDone := make(chan error, 1)
	if t := cfg.Panel.Tunnel; t != nil {
		inTunnel, err := panel.New(client, log)
		if err != nil {
			return err
		}
		inTunnel.Jobs, inTunnel.UploadDir = server.Jobs, server.UploadDir
		inTunnel.Name, inTunnel.Access, inTunnel.TunnelNetwork, inTunnel.Actions, inTunnel.Cluster, inTunnel.Apps = server.Name, server.Access, server.TunnelNetwork, server.Actions, server.Cluster, server.Apps
		if inTunnel.Auth, err = panel.NewAuth(client, cfg.Panel.Name, log); err != nil {
			return err
		}
		go func() {
			err := serveTunnel(serving, cfg, client, inTunnel.Handler(), log)
			// The panel stops, so the failure is in the log and systemd
			// starts it again.
			if err != nil && serving.Err() == nil {
				log.Error("the panel's tunnel stopped", "error", err)
				stop()
			}
			tunnelDone <- err
		}()
	}
	go restartOnUpgrade(ctx, stop, binaryReplaced, server.Jobs, 10*time.Second, log)
	log.Info("the panel serves", "socket", cfg.Panel.Socket)
	serveErr := httpServer.Serve(listener)
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	} else {
		log.Error("the panel's socket stopped", "error", serveErr)
		stop()
	}
	<-stopped
	var tunnelErr error
	if cfg.Panel.Tunnel != nil {
		tunnelErr = <-tunnelDone
	}
	return errors.Join(serveErr, tunnelErr)
}

// restartOnUpgrade stops the panel once its binary was replaced, as an
// upgrade does, and no job of the panel runs: systemd starts the new one.
// An upgrade begun in the panel replaces the panel's binary last, then
// still has to wait for the agent and take changes again.
func restartOnUpgrade(ctx context.Context, stop func(), replaced func() bool, jobs *panel.Jobs, every time.Duration, log *slog.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		}
		if replaced() && jobs.CloseIdle() {
			log.Info("the murder binary was replaced: the panel restarts with it")
			stop()
			return
		}
	}
}

// binaryReplaced reports whether the running binary was replaced.
func binaryReplaced() bool {
	exe, err := os.Readlink("/proc/self/exe")
	return err == nil && strings.HasSuffix(exe, " (deleted)")
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
	defer certificates.Close()
	address, err := netip.ParseAddr(t.Address)
	if err != nil {
		return err
	}
	return panel.ServeTunnel(ctx, client, panel.TunnelConfig{
		PrivateKey: private, Address: address, Port: t.Port, Name: cfg.Panel.Name, TLS: certificates.Config,
	}, handler, log)
}
