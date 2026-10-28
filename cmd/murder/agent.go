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
	"net/netip"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/edge"
	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/runtime"
	"github.com/PowerPenguini/murder/internal/secrets"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/volume"
)

const defaultConfig = "/etc/murder/agent.yaml"

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
	if _, err := os.Stat(cfg.Database); errors.Is(err, os.ErrNotExist) {
		node := cfg.Node
		if node == "" {
			if node, err = os.Hostname(); err != nil {
				return fmt.Errorf("set node in %s: the host name is unknown: %w", *configPath, err)
			}
		}
		if err := store.Init(cfg.Database, node); err != nil {
			return err
		}
		fmt.Printf("created empty database %s for node %s\n", cfg.Database, node)
	} else if err != nil {
		return err
	} else {
		fmt.Printf("database %s already exists\n", cfg.Database)
	}
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
	st, err := store.Open(cfg.Database, store.Options{NetworkPool: netip.MustParsePrefix(cfg.NetworkPool), Sealer: key.NewSealed})
	if err != nil {
		return err
	}
	defer st.Close()
	node := st.Node()
	if cfg.Node != "" && cfg.Node != node {
		return fmt.Errorf("config names node %q but the database belongs to node %q", cfg.Node, node)
	}
	docker := runtime.NewDocker(runtime.DockerOptions{Socket: cfg.DockerSocket, Node: node, RegistryAuthFile: cfg.RegistryAuthFile})
	volumes, err := newVolumes(cfg.Volumes)
	if err != nil {
		return err
	}
	reconciler := reconcile.New(st, docker, volumes, key, reconcile.Config{BaseDomain: cfg.BaseDomain, Scheme: cfg.Scheme(), DefaultLimits: cfg.Defaults}, log)

	// Bind every listener before serving, so a taken port stops the agent at
	// once instead of leaving it up without an edge.
	apiListener, err := listenSocket(cfg.Socket, cfg.SocketGroup)
	if err != nil {
		return err
	}
	httpListener, err := net.Listen("tcp", cfg.Edge.HTTP)
	if err != nil {
		return fmt.Errorf("edge http: %w", err)
	}
	if cfg.Edge.TLS.Mode != edge.TLSOff {
		// Only check the port now: holding it without a certificate would
		// leave clients hanging in the accept queue instead of refused.
		probe, err := net.Listen("tcp", cfg.Edge.HTTPS)
		if err != nil {
			return fmt.Errorf("edge https: %w", err)
		}
		_ = probe.Close()
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	failed := make(chan error, 3)
	serve := func(name string, server *http.Server, listener net.Listener) {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			failed <- fmt.Errorf("%s listener: %w", name, err)
		}
	}

	apiServer := &http.Server{Handler: api.NewServer(st, reconciler, docker, key, log).Handler(), ReadHeaderTimeout: 10 * time.Second, ConnContext: api.ConnContext}
	go serve("api", apiServer, apiListener)
	go reconciler.Run(ctx, cfg.ReconcileInterval.Duration)

	proxy := edge.NewProxy(cfg.BaseDomain, api.Routes{Store: st}, log)
	servers := []*http.Server{apiServer}
	if cfg.Edge.TLS.Mode == edge.TLSOff {
		plain := publicServer(proxy)
		servers = append(servers, plain)
		go serve("edge http", plain, httpListener)
	} else {
		var ready atomic.Bool
		redirect := publicServer(proxy.RedirectToHTTPS(ready.Load))
		secure := publicServer(proxy)
		servers = append(servers, redirect, secure)
		go serve("edge http", redirect, httpListener)
		go func() {
			tlsConfig := obtainTLS(ctx, cfg, log)
			if tlsConfig == nil {
				return
			}
			listener, err := net.Listen("tcp", cfg.Edge.HTTPS)
			if err != nil {
				failed <- fmt.Errorf("edge https: %w", err)
				return
			}
			ready.Store(true)
			log.Info("edge https ready")
			serve("edge https", secure, tls.NewListener(listener, tlsConfig))
		}()
	}
	log.Info("murder agent started", "node", node, "baseDomain", cfg.BaseDomain, "volumes", cfg.Volumes.Driver, "tls", cfg.Edge.TLS.Mode)
	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-failed:
		log.Error("stopping the agent", "error", runErr)
	}
	// Tenant containers keep running while the agent is down or restarting;
	// only the agent's own listeners stop.
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, server := range servers {
		_ = server.Shutdown(shutdown)
	}
	log.Info("murder agent stopped")
	return runErr
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
// It refuses to take over a socket another agent still serves.
func listenSocket(path, group string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	if conn, err := net.DialTimeout("unix", path, time.Second); err == nil {
		_ = conn.Close()
		return nil, fmt.Errorf("another agent is serving %s", path)
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
func publicServer(handler http.Handler) *http.Server {
	return &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute}
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
