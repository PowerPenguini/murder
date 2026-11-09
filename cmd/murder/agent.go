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

	"github.com/PowerPenguini/murder/internal/apiserver"
	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/edge"
	"github.com/PowerPenguini/murder/internal/failover"
	"github.com/PowerPenguini/murder/internal/firewall"
	"github.com/PowerPenguini/murder/internal/invite"
	"github.com/PowerPenguini/murder/internal/move"
	"github.com/PowerPenguini/murder/internal/reboot"
	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/records"
	"github.com/PowerPenguini/murder/internal/releases"
	"github.com/PowerPenguini/murder/internal/replicate"
	"github.com/PowerPenguini/murder/internal/rollout"
	"github.com/PowerPenguini/murder/internal/runtime"
	"github.com/PowerPenguini/murder/internal/secrets"
	"github.com/PowerPenguini/murder/internal/setup"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/upgrade"
	measure "github.com/PowerPenguini/murder/internal/usage"
	"github.com/PowerPenguini/murder/internal/volume"
)

const defaultConfig = "/etc/murder/agent.yaml"

// digestEvery is how often the leader compares the nodes' states.
const digestEvery = 5 * time.Minute

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
		// The database's secrets are sealed with the key it was made
		// with: a new one would open none of them.
		if _, err := os.Stat(cfg.Database); err == nil {
			return fmt.Errorf("database %s exists but master key %s does not: its secrets are sealed with the original key, which a new one cannot open; restore the original key (from the copy kept offline) and run init again", cfg.Database, cfg.MasterKeyFile)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
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
	// Nothing below waits on it: systemctl may be slow while the host boots.
	go refreshInstall(cfg, log)
	key, err := secrets.LoadKey(cfg.MasterKeyFile)
	if err != nil {
		return err
	}
	// The ring follows its file: a rotation run from any node, or an edit
	// by hand, reaches everything that holds the key.
	var started atomic.Pointer[cluster.Cluster]
	ring := &keyRing{path: cfg.MasterKeyFile, key: key, member: started.Load, log: log.With("component", "keys")}
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
	logins := make([]runtime.RegistryLogin, 0, len(cfg.Registries))
	for _, registry := range cfg.Registries {
		logins = append(logins, runtime.RegistryLogin{Host: registry.Host, Username: registry.Username, TokenFile: registry.TokenFile})
	}
	docker := runtime.NewDocker(runtime.DockerOptions{Socket: cfg.DockerSocket, Node: node, Registries: logins, RegistryAuthFile: cfg.RegistryAuthFile})
	// Without live-restore, restarting Docker, as an upgrade of it does,
	// stops every tenant until this agent starts them again.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if enabled, err := docker.LiveRestore(ctx); err == nil && !enabled {
			log.Warn("Docker's live-restore is off: restarting Docker stops every tenant; add \"live-restore\": true to /etc/docker/daemon.json and `systemctl reload docker` (murder setup does)")
		}
	}()
	volumes, err := newVolumes(cfg.Volumes)
	if err != nil {
		return err
	}
	reconciler := reconcile.New(st, docker, volumes, key, reconcile.Config{BaseDomain: cfg.BaseDomain, Scheme: cfg.Scheme(), DefaultLimits: cfg.Defaults,
		// The last lines of each tenant's previous container, beside the
		// database: this node's own, never the cluster's.
		PreviousDir: filepath.Join(filepath.Dir(cfg.Database), "previous-logs")}, log)
	// What the node's tenants use, measured here and kept for a day.
	recorder := measure.NewRecorder(node)
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
	// The agent's part in a reboot of the host, as "murder node reboot"
	// asks: it reboots once the node would come back as it runs.
	rebooter := &reboot.Node{
		System: reboot.Exec{}, Firewall: cfg.Firewall == config.FirewallNftables,
		Frozen: upgrader.frozen, Delay: 500 * time.Millisecond, Log: log.With("component", "reboot"),
		Away: func(until time.Time) error {
			// Outside a cluster, nothing fails the node over.
			if _, member := st.NodeKey(node); !member {
				return nil
			}
			return st.Away(node, until)
		},
	}
	// A node that cannot apply what the cluster applied stops; its restart
	// applies the entry again.
	fatal := make(chan error, 1)
	moves := &agentMoves{store: st, reconciler: reconciler, volumes: volumes}
	var member *cluster.Cluster
	var mover *move.Mover
	var replicator *replicate.Replicator
	var monitor *failover.Monitor
	// Invitations for hosts to join the cluster, made on this node.
	invites := &invite.Book{ConfigPath: *configPath, Store: st}
	if cfg.Cluster != nil {
		// Copies of other nodes' tenants, on ZFS only: requests for them may
		// come as soon as the cluster listens, before it is returned here.
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
			Seeds: key.DeriveAll(clusterSeed), Store: st, Log: log, Upgrade: upgrader, Moves: moves, Replicas: receiver,
			Usage: peerUsage{recorder}, Invites: invites, Keys: ring, Reboot: rebooter,
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
		invites.Invitation = member.Invitation
		invites.Voter = member.Voter
		invites.Admit = member.Admit
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

	apiHandler := apiserver.NewServer(st, reconciler, docker, key, log).WithUpgrader(upgrader).WithMoves(moves).WithUsage(recorder).WithRebooter(rebooter)
	apiHandler.WithKeys(&keyRotator{ring: ring, store: st, member: started.Load, node: node, log: log.With("component", "keys")})
	go func() {
		ticker := time.NewTicker(keyCheckEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			if err := ring.reload(); err != nil {
				log.Warn("read the master key again", "error", err)
			}
		}
	}()
	// Backups to a bucket, if set up: this node's tenants, and the
	// cluster's state from its leader.
	var backups *agentBackups
	if cfg.Backups != nil {
		// A key file missing or unreadable leaves the agent running without
		// backups, saying why, until it is fixed: tenants keep running.
		var leader func() bool
		if member != nil {
			leader = member.Leader
		}
		// A replacement of a tenant's data holds its sends to its copies.
		var hold func(context.Context, string) (func(), error)
		if replicator != nil {
			hold = replicator.Hold
		}
		if backups, err = newAgentBackups(cfg, st, reconciler.Exclusive, leader, hold, log.With("component", "backups")); err != nil {
			return err
		}
		apiHandler.WithBackups(backups)
		recorder.SetBackups(backups.useReport)
	}
	if replicator != nil {
		apiHandler.WithReplicas(replicator)
	}
	if member != nil {
		apiHandler.WithRelayPort(cfg.Edge.RelayPort)
		apiHandler.WithCluster(member)
		apiHandler.WithInvites(invites)
	}
	apiServer := &http.Server{Handler: apiHandler.Handler(), ReadHeaderTimeout: 10 * time.Second, ConnContext: apiserver.ConnContext}
	go serve("api", apiServer, apiListener)
	// Moves an agent restart cut short leave the tenant here.
	if mover != nil {
		go recoverMoves(ctx, mover, log)
	}
	if backups != nil {
		go backups.Run(ctx)
	}
	if replicator != nil {
		go replicator.Run(ctx)
	}
	if monitor != nil {
		go monitor.Run(ctx)
	}
	// The leader moves apps' rollouts on, wave after wave.
	driver := &rollout.Driver{Store: st, Log: log.With("component", "rollout"),
		Status: func(ctx context.Context, record store.Record) (reconcile.Status, bool) {
			if record.Node == node {
				return reconciler.Status(record.Spec.Name)
			}
			if member == nil {
				return reconcile.Status{}, false
			}
			ask, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			var status reconcile.Status
			if err := member.StatusOn(ask, record.Node, record.Spec.Name, &status); err != nil {
				return reconcile.Status{}, false
			}
			return status, true
		}}
	if member != nil {
		driver.Leader = member.Leader
	}
	go driver.Run(ctx)
	// The leader asks apps' registries for newer images of their tags.
	var watcher *releases.Watcher
	if cfg.ImageChecks.Duration > 0 {
		watcher = &releases.Watcher{Store: st, Registry: docker, Every: cfg.ImageChecks.Duration, Log: log.With("component", "releases")}
		if member != nil {
			watcher.Leader = member.Leader
		}
		go watcher.Run(ctx)
	}
	// The leader checks tenants' own domains, and orders and renews their
	// certificates.
	domainManager := newDomainManager(cfg, st, key, member, log.With("component", "domains"))
	go domainManager.Run(ctx)
	// The leader compares the nodes' replicated states every few minutes:
	// they apply the same log, so a difference is a fault to look into.
	var digests *cluster.Digests
	if member != nil {
		digests = &cluster.Digests{}
		go member.WatchDigests(ctx, digestEvery, digests)
	}
	go newCollector(st, reconciler, docker, volumes, recorder, node, cfg.Defaults, log).Run(ctx)
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
		// The edge takes this node's routes from now on: they are the
		// cluster's, not what the node had before it went down.
		apiHandler.CaughtUp()
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
	observed := &agentMetrics{store: st, reconciler: reconciler, api: apiHandler, firewall: firewallLoaded, backups: backups, replicator: replicator, monitor: monitor, usage: recorder, digests: digests, releases: watcher, domains: domainManager}
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
	// A reboot asked for may have marked it away for longer: that stays.
	if member != nil && runErr == nil {
		markAway(st, node, later(time.Now().Add(awayFor), rebooter.AwayUntil()), awayTimeout, log)
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
	monitor := &failover.Monitor{Store: st, Probe: member.Ping, Leader: member.Leader, Term: member.Term, Log: log}
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
			// The panel's name, for the devices in its tunnel; never over
			// a tenant of that name from before it was reserved.
			label, _, _ := strings.Cut(panelName, ".")
			if _, err := st.Get(label); err == nil {
				log.Warn("the DNS records do not lead the panel's name to it: a tenant has that name; rename it, or set panel.name", "tenant", label)
			} else if relative, err := records.Relative(panelName, cfg.DNS.Zone); err == nil {
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
		// Without it, the provider asks a web service for it with no time
		// limit, and a call that hangs holds every later one.
		if clientIP == "" {
			return nil, errors.New("cluster.dns with namecheap needs this node's public IPv4 address in cluster.publicAddresses: Namecheap allows calls from listed addresses only")
		}
		return &records.Namecheap{User: user, APIKey: key, ClientIP: clientIP, Zone: zone, Name: name, TTL: cfg.TTL.Duration}, nil
	}
	return &records.Hetzner{Token: token, Zone: zone, Name: name, TTL: cfg.TTL.Duration, Endpoint: cfg.Endpoint}, nil
}

// parsePublic is the addresses the config's values name, sorted, each once.
func parsePublic(values []string) []netip.Addr {
	var addresses []netip.Addr
	for _, value := range values {
		if address, err := netip.ParseAddr(value); err == nil {
			addresses = append(addresses, address.Unmap())
		}
	}
	slices.SortFunc(addresses, func(a, b netip.Addr) int { return a.Compare(b) })
	return slices.Compact(addresses)
}

// keepPublic keeps this node's public addresses, as its config has them,
// recorded for the DNS records, as after the node left the cluster and
// joined it again; a freeze or a lost leader only delays it.
func keepPublic(ctx context.Context, st *store.SQLite, node string, values []string, log *slog.Logger) {
	want := parsePublic(values)
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
		// Soon while it is not recorded, as just after joining; then
		// every half minute.
		wait := 30 * time.Second
		if !slices.ContainsFunc(st.Nodes(), func(n store.Node) bool { return n.Name == node && slices.Equal(n.Public, want) }) {
			wait = 5 * time.Second
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// awayFor is how long a node whose agent stopped cleanly is not failed over
// for not answering: long enough for a reboot.
const awayFor = 10 * time.Minute

// later is the later of a and b.
func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// awayTimeout bounds marking the node away as the agent stops: a node that
// cannot reach the cluster's leader does not hold its stop for long.
const awayTimeout = 5 * time.Second

// awayMarker marks a node away; *store.SQLite is one.
type awayMarker interface {
	Away(node string, until time.Time) error
	NodeKey(name string) (key string, ok bool)
}

// markAway marks node away until until, as its agent stops cleanly. A node
// the state no longer names, such as one removed from the cluster, is not
// marked: no leader takes a change from it, and none fails it over. Nor
// does the stop wait for the mark longer than timeout: the change goes
// through the leader, which a node cut off from it never reaches; a write
// still under way then ends as the agent closes its part in the cluster.
func markAway(st awayMarker, node string, until time.Time, timeout time.Duration, log *slog.Logger) {
	if _, member := st.NodeKey(node); !member {
		log.Info("this node is not marked away: it is no member of the cluster")
		return
	}
	done := make(chan error, 1)
	go func() { done <- st.Away(node, until) }()
	select {
	case err := <-done:
		// During an upgrade the freeze itself keeps the node from being
		// failed over.
		if errors.Is(err, store.ErrFrozen) {
			log.Info("this node is not marked away while an upgrade freezes changes", "error", err)
		} else if err != nil {
			log.Warn("mark this node away while it restarts", "error", err)
		}
	case <-time.After(timeout):
		log.Warn("this node could not be marked away in time, as the cluster's leader did not answer; it stops all the same, and may be failed over if it stays down", "timeout", timeout.String())
	}
}

// waitCaughtUp returns once this node has applied what the cluster has, or
// false when ctx ends first.
func waitCaughtUp(ctx context.Context, member *cluster.Cluster, log *slog.Logger) bool {
	for attempt := 0; ; attempt++ {
		err := member.CatchUpContext(ctx)
		// A node not yet in a cluster decides alone.
		if err == nil || errors.Is(err, cluster.ErrNotJoined) {
			return true
		}
		if ctx.Err() != nil {
			return false
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

// keyCheckEvery is how often the agent reads the master key file again.
const keyCheckEvery = 5 * time.Second

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
	host, err := cfg.HostRules()
	if err != nil {
		return err
	}
	sources := append([]netip.Prefix{netip.MustParsePrefix(cfg.NetworkPool)}, subnets...)
	return firewall.Apply(ctx, sources, edgePorts, firewall.Resolvers(), host)
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

// refreshInstall brings the node's units and sysctl settings to this
// release's: an upgrade replaces the binary only, and the agent is the
// first of it to run. Only a node installed at the usual place, as root.
func refreshInstall(cfg config.Agent, log *slog.Logger) {
	host := setup.System{}
	if executable, err := host.Executable(); err != nil || executable != setup.Binary || !host.Root() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// The confined edge writes only its own directories, unless told more.
	written := []string{filepath.Dir(cfg.Edge.Routes), filepath.Dir(cfg.Edge.PIDFile)}
	if cfg.Edge.TLS.Mode != edge.TLSOff {
		written = append(written, cfg.Edge.TLS.Storage)
	}
	dirs := setup.EdgeWritable(written...)
	for _, dir := range dirs {
		// A missing directory cannot be made writable, nor made by the edge.
		if err := os.MkdirAll(dir, 0o700); err != nil {
			log.Warn("could not make a directory the edge writes", "dir", dir, "err", err)
		}
	}
	replaced, err := setup.Refresh(ctx, host, dirs)
	if len(replaced) > 0 {
		log.Info("brought the install files up to this release", "files", replaced)
	}
	// Until systemd has read the new unit, a restart would not apply it.
	if err == nil && slices.ContainsFunc(replaced, func(file string) bool {
		return file == setup.Target("murder-edge.service") || file == setup.EdgePathsDropIn
	}) {
		// A reload hands over to a child of the old edge, which keeps the
		// old unit's confinement; only a start under the new unit has it.
		log.Warn("murder-edge.service changed: `systemctl restart murder-edge` runs the edge under it (open requests get edge.drain to finish)")
	}
	if err != nil {
		log.Warn("could not bring the install files up to this release", "err", err)
	}
}
