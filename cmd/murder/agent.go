package main

import (
	"context"
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
	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/firewall"
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
	// Run alone, a node of a cluster would change its state apart from the
	// others', which then refuse everything they send it.
	if cfg.Cluster == nil && (st.Cluster() != "" || len(st.Nodes()) > 0) {
		return fmt.Errorf("the database belongs to cluster %s: give the node its cluster section back in %s", st.Cluster(), *configPath)
	}
	// A node that cannot apply what the cluster applied stops; its restart
	// applies the entry again.
	fatal := make(chan error, 1)
	var member *cluster.Cluster
	if cfg.Cluster != nil {
		member, err = cluster.Start(cluster.Config{
			Node: node, Address: cfg.Cluster.Address, Dir: cfg.Cluster.Dir,
			Seed: key.Derive(clusterSeed), Store: st, Log: log,
			Fatal: func(err error) {
				select {
				case fatal <- err:
				default:
				}
			},
		})
		if err != nil {
			return err
		}
		// Closed before the store, which the log applies to.
		defer member.Close()
	}
	docker := runtime.NewDocker(runtime.DockerOptions{Socket: cfg.DockerSocket, Node: node, RegistryAuthFile: cfg.RegistryAuthFile})
	volumes, err := newVolumes(cfg.Volumes)
	if err != nil {
		return err
	}
	reconciler := reconcile.New(st, docker, volumes, key, reconcile.Config{BaseDomain: cfg.BaseDomain, Scheme: cfg.Scheme(), DefaultLimits: cfg.Defaults}, log)
	// murder-firewall.service loads the rules before Docker starts the
	// tenants at boot; loading them here covers a node without that unit
	// and a ruleset flushed since.
	var firewallLoaded *atomic.Int64
	if cfg.Firewall == config.FirewallNftables {
		if err := loadFirewall(context.Background(), cfg, storeSubnets(st)); err != nil {
			return fmt.Errorf("%w (firewall: off in %s runs tenants without it)", err, *configPath)
		}
		firewallLoaded = new(atomic.Int64)
		firewallLoaded.Store(time.Now().UnixNano())
	} else {
		log.Warn("the tenant firewall is off: tenants can reach the host's services and private networks")
	}

	// Bind every listener before serving, so a taken port stops the agent at
	// once. The edge is a process of its own ("murder edge"): restarting
	// the agent leaves public traffic alone.
	apiListener, err := listenSocket(cfg.Socket, cfg.SocketGroup)
	if err != nil {
		return err
	}
	var metricsListener net.Listener
	if cfg.Metrics != "" {
		if metricsListener, err = net.Listen("tcp", cfg.Metrics); err != nil {
			return fmt.Errorf("metrics: %w", err)
		}
		if host, _, _ := net.SplitHostPort(cfg.Metrics); !privateHost(host) {
			log.Warn("metrics are served on a public address, without authentication: firewall the port", "metrics", cfg.Metrics)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	failed := make(chan error, 2)
	serve := func(name string, server *http.Server, listener net.Listener) {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			failed <- fmt.Errorf("%s listener: %w", name, err)
		}
	}

	apiHandler := api.NewServer(st, reconciler, docker, key, log)
	if member != nil {
		apiHandler.WithCluster(member)
	}
	apiServer := &http.Server{Handler: apiHandler.Handler(), ReadHeaderTimeout: 10 * time.Second, ConnContext: api.ConnContext}
	go serve("api", apiServer, apiListener)
	reconciled := make(chan struct{})
	go func() {
		defer close(reconciled)
		reconciler.Run(ctx, cfg.ReconcileInterval.Duration)
	}()
	if cfg.Firewall == config.FirewallNftables {
		go keepFirewall(ctx, cfg, st, log, firewallLoaded)
	}

	servers := []*http.Server{apiServer}
	observed := &agentMetrics{store: st, reconciler: reconciler, api: apiHandler, firewall: firewallLoaded}
	if metricsListener != nil {
		metricsServer := &http.Server{Handler: observed, ReadHeaderTimeout: 10 * time.Second}
		servers = append(servers, metricsServer)
		go serve("metrics", metricsServer, metricsListener)
	}
	log.Info("murder agent started", "node", node, "baseDomain", cfg.BaseDomain, "volumes", cfg.Volumes.Driver)
	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-failed:
		log.Error("stopping the agent", "error", runErr)
	case runErr = <-fatal:
		log.Error("stopping the agent", "error", runErr)
	}
	// Tenant containers keep running while the agent is down or restarting;
	// only the agent's own listeners stop. A pass, also one an API call
	// began, stops after the tenant step under way, which then finishes:
	// a tenant whose container is being replaced gets its new one first.
	stop()
	apiHandler.Stop()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, server := range servers {
		_ = server.Shutdown(shutdown)
	}
	<-reconciled
	log.Info("murder agent stopped")
	return runErr
}

// clusterSeed is what the cluster's certificate authority is derived from
// the master key for.
const clusterSeed = "cluster ca"

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

// firewallInterval is how often the agent loads the tenant firewall again:
// the longest a flushed ruleset or a changed DNS server goes unnoticed.
const firewallInterval = 15 * time.Second

// keepFirewall loads the tenant firewall again and again, so a firewall
// reload that dropped Murder's table does not leave tenants unguarded and
// a changed DNS server is let through.
func keepFirewall(ctx context.Context, cfg config.Agent, st store.Store, log *slog.Logger, loaded *atomic.Int64) {
	ticker := time.NewTicker(firewallInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := loadFirewall(ctx, cfg, storeSubnets(st))
			switch {
			case err == nil:
				loaded.Store(time.Now().UnixNano())
			case ctx.Err() == nil:
				log.Error("reload the tenant firewall", "error", err)
			}
		}
	}
}

// loadFirewall loads the tenant firewall the config describes for the pool
// and the tenant subnets (some may lie outside the pool after it changed),
// with the host's DNS servers as they are now.
func loadFirewall(ctx context.Context, cfg config.Agent, subnets []netip.Prefix) error {
	edgePorts, err := cfg.EdgePorts()
	if err != nil {
		return err
	}
	sources := append([]netip.Prefix{netip.MustParsePrefix(cfg.NetworkPool)}, subnets...)
	return firewall.Apply(ctx, sources, edgePorts, firewall.Resolvers())
}

func storeSubnets(st store.Store) []netip.Prefix {
	var subnets []netip.Prefix
	for _, record := range st.List() {
		if subnet, err := netip.ParsePrefix(record.Subnet); err == nil {
			subnets = append(subnets, subnet)
		}
	}
	return subnets
}

// runFirewall loads the tenant firewall once, for murder-firewall.service,
// which runs before Docker starts the tenants at boot.
func runFirewall(args []string) error {
	flags := flag.NewFlagSet("firewall", flag.ContinueOnError)
	configPath := flags.String("config", defaultConfig, "agent configuration file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	if cfg.Firewall != config.FirewallNftables {
		fmt.Println("the tenant firewall is off in", *configPath)
		return nil
	}
	subnets, err := store.Subnets(cfg.Database)
	if err != nil {
		return err
	}
	return loadFirewall(context.Background(), cfg, subnets)
}

// privateHost reports whether host is a loopback or private address, or a
// name (which the operator chose deliberately).
func privateHost(host string) bool {
	address, err := netip.ParseAddr(host)
	return err != nil || address.IsLoopback() || address.IsPrivate()
}
