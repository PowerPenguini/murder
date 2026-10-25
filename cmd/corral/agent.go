package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/PowerPenguini/corral/internal/api"
	"github.com/PowerPenguini/corral/internal/config"
	"github.com/PowerPenguini/corral/internal/edge"
	"github.com/PowerPenguini/corral/internal/reconcile"
	"github.com/PowerPenguini/corral/internal/runtime"
	"github.com/PowerPenguini/corral/internal/secrets"
	"github.com/PowerPenguini/corral/internal/store"
	"github.com/PowerPenguini/corral/internal/volume"
)

const defaultConfig = "/etc/corral/agent.yaml"

func runInit(args []string) error {
	flags := flag.NewFlagSet("init", flag.ContinueOnError)
	configPath := flags.String("config", defaultConfig, "agent configuration file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	if _, err := os.Stat(cfg.MasterKeyFile); errors.Is(err, os.ErrNotExist) {
		if err := secrets.CreateKey(cfg.MasterKeyFile); err != nil {
			return err
		}
		fmt.Printf("created master key %s — copy it to your password manager now\n", cfg.MasterKeyFile)
	} else if err != nil {
		return err
	} else {
		fmt.Printf("master key %s already exists\n", cfg.MasterKeyFile)
	}
	if err := os.MkdirAll(filepath.Dir(cfg.StateFile), 0o700); err != nil {
		return err
	}
	fmt.Printf("state directory %s is ready\n", filepath.Dir(cfg.StateFile))
	return nil
}

func runAgent(args []string) error {
	flags := flag.NewFlagSet("agent", flag.ContinueOnError)
	configPath := flags.String("config", defaultConfig, "agent configuration file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	key, err := secrets.LoadKey(cfg.MasterKeyFile)
	if err != nil {
		return err
	}
	st, err := store.OpenFile(cfg.StateFile, cfg.Ports.From, cfg.Ports.To)
	if err != nil {
		return err
	}
	docker := runtime.NewDocker(cfg.DockerSocket)
	volumes, err := newVolumes(cfg.Volumes)
	if err != nil {
		return err
	}
	reconciler := reconcile.New(st, docker, volumes, key, reconcile.Config{BaseDomain: cfg.BaseDomain, Scheme: cfg.Scheme(), DefaultLimits: cfg.Defaults}, log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	listener, err := listenSocket(cfg.Socket, cfg.SocketGroup)
	if err != nil {
		return err
	}
	apiServer := &http.Server{Handler: api.NewServer(st, reconciler, docker, key, log).Handler(), ReadHeaderTimeout: 10 * time.Second}
	go serve(log, "api", func() error { return apiServer.Serve(listener) })
	go reconciler.Run(ctx, cfg.ReconcileInterval.Duration)

	proxy := edge.NewProxy(cfg.BaseDomain, api.Routes{Store: st}, log)
	servers := []*http.Server{apiServer}
	if cfg.Edge.TLS.Mode == edge.TLSOff {
		plain := publicServer(cfg.Edge.HTTP, proxy)
		servers = append(servers, plain)
		go serve(log, "edge http", plain.ListenAndServe)
	} else {
		redirect := publicServer(cfg.Edge.HTTP, proxy.RedirectToHTTPS())
		secure := publicServer(cfg.Edge.HTTPS, proxy)
		servers = append(servers, redirect, secure)
		go serve(log, "edge http", redirect.ListenAndServe)
		go func() {
			secure.TLSConfig = obtainTLS(ctx, cfg, log)
			if secure.TLSConfig != nil {
				serve(log, "edge https", func() error { return secure.ListenAndServeTLS("", "") })
			}
		}()
	}
	log.Info("corral agent started", "baseDomain", cfg.BaseDomain, "volumes", cfg.Volumes.Driver, "tls", cfg.Edge.TLS.Mode)
	<-ctx.Done()
	// Tenant containers keep running while the agent is down or restarting;
	// only the agent's own listeners stop.
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, server := range servers {
		_ = server.Shutdown(shutdown)
	}
	log.Info("corral agent stopped")
	return nil
}

func newVolumes(cfg config.Volumes) (volume.Volumes, error) {
	switch cfg.Driver {
	case "dir":
		return volume.Dir{Root: cfg.Root}, nil
	case "zfs":
		return volume.ZFS{Parent: cfg.Parent, MountRoot: cfg.MountRoot, Run: volume.Exec}, nil
	default:
		return nil, fmt.Errorf("unknown volume driver %q", cfg.Driver)
	}
}

// listenSocket opens the API socket, readable by root and, if set, group.
func listenSocket(path, group string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	mode := os.FileMode(0o600)
	if group != "" {
		found, err := user.LookupGroup(group)
		if err != nil {
			_ = listener.Close()
			return nil, err
		}
		gid, _ := strconv.Atoi(found.Gid)
		if err := os.Chown(path, -1, gid); err != nil {
			_ = listener.Close()
			return nil, err
		}
		mode = 0o660
	}
	if err := os.Chmod(path, mode); err != nil {
		_ = listener.Close()
		return nil, err
	}
	return listener, nil
}

// publicServer has no write timeout: tenants stream server-sent events.
func publicServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute}
}

func serve(log *slog.Logger, name string, run func() error) {
	if err := run(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("listener stopped", "listener", name, "error", err)
	}
}

// obtainTLS retries until the certificate is available or ctx ends; the
// reconciler and the plain listener keep working meanwhile.
func obtainTLS(ctx context.Context, cfg config.Agent, log *slog.Logger) *tls.Config {
	delay := 10 * time.Second
	for {
		tlsConfig, err := edge.NewTLS(ctx, cfg.Edge.TLS, cfg.BaseDomain, log)
		if err == nil {
			return tlsConfig
		}
		log.Error("obtain TLS certificate", "error", err, "retryIn", delay.String())
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
		delay = min(2*delay, 10*time.Minute)
	}
}
