package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/failover"
	"github.com/PowerPenguini/murder/internal/firewall"
	"github.com/PowerPenguini/murder/internal/move"
	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/records"
	"github.com/PowerPenguini/murder/internal/replicate"
	"github.com/PowerPenguini/murder/internal/runtime"
	"github.com/PowerPenguini/murder/internal/secrets"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/upgrade"
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
	docker := runtime.NewDocker(runtime.DockerOptions{Socket: cfg.DockerSocket, Node: node, RegistryAuthFile: cfg.RegistryAuthFile})
	volumes, err := newVolumes(cfg.Volumes)
	if err != nil {
		return err
	}
	reconciler := reconcile.New(st, docker, volumes, key, reconcile.Config{BaseDomain: cfg.BaseDomain, Scheme: cfg.Scheme(), DefaultLimits: cfg.Defaults}, log)
	// The agent's part in an upgrade: the new binary replaces this one, and
	// the agent stops for its service to start the new one.
	version, err := ownVersion()
	if err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	upgrader := &agentUpgrader{store: st, reconciler: reconciler, started: time.Now(), Node: &upgrade.Node{
		Executable: executable, Current: version, EdgePIDFile: cfg.Edge.PIDFile,
		Snapshot: st.SnapshotFile, Database: cfg.Database, Log: log,
		Restart: func() {
			// After the answer has gone out.
			time.AfterFunc(500*time.Millisecond, func() { _ = syscall.Kill(os.Getpid(), syscall.SIGTERM) })
		},
	}}
	// A node that cannot apply what the cluster applied stops; its restart
	// applies the entry again.
	fatal := make(chan error, 1)
	moves := &agentMoves{store: st, reconciler: reconciler, volumes: volumes}
	var member *cluster.Cluster
	var mover *move.Mover
	var replicator *replicate.Replicator
	var monitor *failover.Monitor
	if cfg.Cluster != nil {
		// Copies of other nodes' tenants, on ZFS only: requests for them may
		// come as soon as the cluster listens, before it is returned here.
		var started atomic.Pointer[cluster.Cluster]
		catchUp := func() error {
			if c := started.Load(); c != nil {
				return c.CatchUp()
			}
			return errors.New("the cluster is starting")
		}
		zfs, replicas := volumes.(volume.ZFS)
		var receiver cluster.Replicas
		if replicas {
			receiver = &replicate.Receiver{Store: st, Volumes: zfs, CatchUp: catchUp}
		} else {
			log.Warn("tenants' data is not copied to a second node: that needs the zfs volume driver")
		}
		member, err = cluster.Start(cluster.Config{
			Node: node, Address: cfg.Cluster.Address, Dir: cfg.Cluster.Dir,
			Seed: key.Derive(clusterSeed), Store: st, Log: log, Upgrade: upgrader, Moves: moves, Replicas: receiver,
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
		started.Store(member)
		upgrader.catchUp = member.CatchUp
		mover = &move.Mover{Store: st, Reconciler: reconciler, Volumes: volumes, Peer: clusterPeer{member}, Log: log, CatchUp: member.CatchUp}
		moves.setCluster(member, mover)
		if replicas {
			replicator = &replicate.Replicator{
				Store: st, Volumes: zfs, Peer: replicaPeer{member}, Log: log.With("component", "replication"),
				CatchUp: member.CatchUp, Moving: mover.Moving, Leader: member.Leader,
			}
		}
		monitor = newMonitor(cfg.Cluster, cfg.BaseDomain, cfg.Panel.Name, st, member, log.With("component", "failover"))
	}
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

	apiHandler := api.NewServer(st, reconciler, docker, key, log).WithUpgrader(upgrader).WithMoves(moves)
	// Backups to a bucket, if set up: this node's tenants, and the
	// cluster's state from its leader.
	var backups *agentBackups
	if cfg.Backups != nil {
		// A key file missing or unreadable leaves the agent running without
		// backups, saying why, until it is fixed: tenants keep running.
		if backups, err = newAgentBackups(cfg, st, reconciler.Exclusive, log.With("component", "backups")); err != nil {
			return err
		}
		if member != nil {
			backups.leader = member.Leader
		}
		apiHandler.WithBackups(backups)
	}
	if replicator != nil {
		apiHandler.WithReplicas(replicator)
	}
	if member != nil {
		apiHandler.WithRelayPort(cfg.Edge.RelayPort)
		apiHandler.WithCluster(member)
	}
	apiServer := &http.Server{Handler: apiHandler.Handler(), ReadHeaderTimeout: 10 * time.Second, ConnContext: api.ConnContext}
	go serve("api", apiServer, apiListener)
	// Moves an agent restart cut short leave the tenant here.
	if mover != nil {
		go recoverMoves(ctx, mover, log)
	}
	if backups != nil {
		go backups.loop(ctx)
	}
	if replicator != nil {
		go replicator.Run(ctx)
	}
	if monitor != nil {
		go monitor.Run(ctx)
	}
	reconciled := make(chan struct{})
	go func() {
		defer close(reconciled)
		// No container may start by itself while the agent waits, nor at a
		// later boot: an older release's policy would let the engine start
		// them on data a failover made stale.
		if err := reconciler.SetRestartPolicies(ctx); err != nil {
			log.Warn("change the containers' restart policy", "error", err)
		}
		go keepRestartPolicies(ctx, reconciler, log)
		// A node that was down may have lost its tenants to their replicas
		// meanwhile: it starts nothing until it knows what the cluster
		// decided. A node that cannot reach a majority starts nothing.
		if member != nil && !waitCaughtUp(ctx, member, log) {
			return
		}
		if member != nil {
			if err := st.Away(node, time.Time{}); errors.Is(err, store.ErrFrozen) {
				log.Info("this node is not marked back from a restart while an upgrade freezes changes", "error", err)
			} else if err != nil {
				log.Warn("mark this node back from a restart", "error", err)
			}
			go keepPublic(ctx, st, node, cfg.Cluster.PublicAddresses, log)
		}
		reconciler.Run(ctx, cfg.ReconcileInterval.Duration)
	}()
	if cfg.Firewall == config.FirewallNftables {
		go keepFirewall(ctx, cfg, st, log, firewallLoaded)
	}

	servers := []*http.Server{apiServer}
	observed := &agentMetrics{store: st, reconciler: reconciler, api: apiHandler, firewall: firewallLoaded, backups: backups, replicator: replicator, monitor: monitor}
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
	// A clean stop, as for a reboot, marks the node away for a while: the
	// leader does not power it off for not answering meanwhile.
	if member != nil && runErr == nil {
		// During an upgrade the freeze itself keeps the node from being
		// failed over.
		if err := st.Away(node, time.Now().Add(awayFor)); errors.Is(err, store.ErrFrozen) {
			log.Info("this node is not marked away while an upgrade freezes changes", "error", err)
		} else if err != nil {
			log.Warn("mark this node away while it restarts", "error", err)
		}
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

// keepRestartPolicies changes the restart policy of any container that
// has another, every 10 minutes, as one Docker failed to change at start.
func keepRestartPolicies(ctx context.Context, reconciler *reconcile.Reconciler, log *slog.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Minute):
		}
		if err := reconciler.SetRestartPolicies(ctx); err != nil {
			log.Warn("change the containers' restart policy", "error", err)
		}
	}
}

// newMonitor watches the other nodes from the leader. With a provider set
// up it powers a dead node off and fails it over; a token that cannot be
// read leaves failover to an operator, which the log and metrics say when
// a node dies, rather than stopping the agent.
func newMonitor(cfg *config.Cluster, baseDomain, panelName string, st *store.SQLite, member *cluster.Cluster, log *slog.Logger) *failover.Monitor {
	monitor := &failover.Monitor{Store: st, Probe: member.Ping, Leader: member.Leader, Log: log}
	if cfg.DNS != nil {
		wildcard, err := records.Wildcard(baseDomain, cfg.DNS.Zone)
		var publisher records.Publisher
		if err == nil {
			publisher, err = newPublisher(cfg.DNS, wildcard, cfg.PublicAddresses)
		}
		if err != nil {
			log.Error("the DNS records are not kept", "error", err)
		} else {
			monitor.Records = publisher
			// The panel's name, for the devices in its tunnel.
			if relative, err := records.Relative(panelName, cfg.DNS.Zone); err == nil {
				monitor.PanelRecords, _ = newPublisher(cfg.DNS, relative, cfg.PublicAddresses)
			}
		}
	}
	if cfg.Failover == nil {
		return monitor
	}
	monitor.After = cfg.Failover.After.Duration
	if h := cfg.Failover.Hetzner; h != nil {
		token, err := readSecret(h.TokenFile)
		if err != nil {
			log.Error("no automatic failover: the Hetzner token cannot be read", "error", err)
			return monitor
		}
		monitor.Fence = &failover.Hetzner{Token: token, Servers: h.Servers, Endpoint: h.Endpoint}
	}
	return monitor
}

// newPublisher writes the records of name, relative to the zone, through
// the provider.
func newPublisher(cfg *config.DNS, name string, public []string) (records.Publisher, error) {
	token, err := readSecret(cfg.TokenFile)
	if err != nil {
		return nil, err
	}
	zone := strings.TrimSuffix(cfg.Zone, ".")
	if cfg.Provider == "namecheap" {
		user, key, ok := strings.Cut(token, ":")
		if !ok {
			return nil, errors.New("the namecheap token file must hold user:apikey")
		}
		// Namecheap needs the address it is asked from; IPv4 only.
		clientIP := ""
		for _, value := range public {
			if address, err := netip.ParseAddr(value); err == nil && address.Unmap().Is4() {
				clientIP = address.Unmap().String()
				break
			}
		}
		return &records.Namecheap{User: user, APIKey: key, ClientIP: clientIP, Zone: zone, Name: name, TTL: cfg.TTL.Duration}, nil
	}
	return &records.Hetzner{Token: token, Zone: zone, Name: name, TTL: cfg.TTL.Duration, Endpoint: cfg.Endpoint}, nil
}

// keepPublic keeps this node's public addresses, as its config has them,
// recorded for the DNS records, as after the node left the cluster and
// joined it again; a freeze or a lost leader only delays it.
func keepPublic(ctx context.Context, st *store.SQLite, node string, values []string, log *slog.Logger) {
	var want []netip.Addr
	for _, value := range values {
		if address, err := netip.ParseAddr(value); err == nil {
			want = append(want, address.Unmap())
		}
	}
	slices.SortFunc(want, func(a, b netip.Addr) int { return a.Compare(b) })
	want = slices.Compact(want)
	warned := false
	for {
		if !slices.ContainsFunc(st.Nodes(), func(n store.Node) bool { return n.Name == node && slices.Equal(n.Public, want) }) {
			if err := st.SetPublic(node, want); err == nil {
				warned = false
			} else if !errors.Is(err, store.ErrFrozen) && !errors.Is(err, store.ErrNotFound) && !errors.Is(err, cluster.ErrNotJoined) && !warned {
				log.Warn("record this node's public addresses", "error", err)
				warned = true
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(30 * time.Second):
		}
	}
}

// awayFor is how long a node whose agent stopped cleanly is not failed over
// for not answering: long enough for a reboot.
const awayFor = 10 * time.Minute

// waitCaughtUp returns once this node has applied what the cluster has, or
// false when ctx ends first.
func waitCaughtUp(ctx context.Context, member *cluster.Cluster, log *slog.Logger) bool {
	for attempt := 0; ; attempt++ {
		err := member.CatchUp()
		// A node not yet in a cluster decides alone.
		if err == nil || errors.Is(err, cluster.ErrNotJoined) {
			return true
		}
		if attempt%10 == 0 {
			log.Warn("starting no tenants until this node has what the cluster decided", "error", err)
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(time.Second):
		}
	}
}

// replicaPeer reaches the nodes that keep tenants' copies.
type replicaPeer struct{ cluster *cluster.Cluster }

func (p replicaPeer) Snapshots(ctx context.Context, node, tenant string) ([]string, error) {
	return p.cluster.SnapshotsOn(ctx, node, tenant)
}

func (p replicaPeer) Send(ctx context.Context, node, tenant string, whole bool, data io.Reader) error {
	return p.cluster.ReplicateOn(ctx, node, tenant, whole, data)
}

// recoverMoves undoes moves away from this node cut short, now and every
// minute, until the agent stops.
func recoverMoves(ctx context.Context, mover *move.Mover, log *slog.Logger) {
	for {
		if err := mover.Recover(ctx); err != nil {
			log.Warn("undo moves cut short", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Minute):
		}
	}
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
