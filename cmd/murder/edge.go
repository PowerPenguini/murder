package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cloudflare/tableflip"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/edge"
	"github.com/PowerPenguini/murder/internal/metrics"
	"github.com/PowerPenguini/murder/internal/secrets"
	"github.com/PowerPenguini/murder/internal/upgrade"
	measure "github.com/PowerPenguini/murder/internal/usage"
)

// slowStartSetting is the kernel setting deploy/murder-sysctl.conf turns off.
const slowStartSetting = "/proc/sys/net/ipv4/tcp_slow_start_after_idle"

// slowStartsAfterIdle reports whether the host's TCP starts a connection's
// sending rate over after a pause, which makes the first response after a
// click take several round trips. A setting that cannot be read, as off
// Linux, is not reported.
func slowStartsAfterIdle(path string) bool {
	value, err := os.ReadFile(path)
	return err == nil && strings.TrimSpace(string(value)) != "0"
}

// runEdge serves the public listeners. It holds no state: it routes with the
// last routes the agent gave it, kept on disk too, so it serves while the
// agent restarts or is down. SIGHUP replaces it with the binary now at its
// path, without closing a listener: the new process takes them over, and
// the old one finishes its requests and exits.
func runEdge(args []string) error {
	flags := flag.NewFlagSet("edge", flag.ContinueOnError)
	configPath := flags.String("config", defaultConfig, "agent configuration file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("process", "edge")
	// The edge holds little but allocates per request: collecting less
	// often spends less CPU on it, for a little more memory.
	if os.Getenv("GOGC") == "" {
		debug.SetGCPercent(200)
	}
	if slowStartsAfterIdle(slowStartSetting) {
		log.Warn("the host starts each connection's sending rate over after a pause; install deploy/murder-sysctl.conf", "setting", "net.ipv4.tcp_slow_start_after_idle")
	}
	if err := os.MkdirAll(filepath.Dir(cfg.Edge.PIDFile), 0o755); err != nil {
		return err
	}
	upgrader, err := tableflip.New(tableflip.Options{PIDFile: cfg.Edge.PIDFile})
	if err != nil {
		return err
	}
	defer upgrader.Stop()

	table := &edge.LiveTable{}
	if saved, ok, err := edge.LoadTable(cfg.Edge.Routes); err != nil {
		log.Warn("the saved routes cannot be read; waiting for the agent's", "error", err)
	} else if ok {
		table.Restore(saved)
	}
	proxy := edge.NewProxy(cfg.BaseDomain, table, log)
	// Started anew, as after a reboot, the saved routes may be out of
	// date: requests wait a little for the agent's. Taking over from an
	// edge, they are the routes it served until now.
	if !upgrader.HasParent() {
		proxy.HoldUnconfirmed(cfg.Edge.RoutesHold.Duration)
	}
	// In a cluster, requests for tenants of other nodes go on to their
	// edges, proven part of the cluster by a certificate derived from the
	// master key; this edge takes theirs on its relay port.
	var credentials *cluster.Credentials
	var key *secrets.Key
	if cfg.Cluster != nil {
		if key, err = secrets.LoadKey(cfg.MasterKeyFile); err != nil {
			return err
		}
		if credentials, err = cluster.NewCredentials(key.DeriveAll(clusterSeed), "edge", nil, time.Now()); err != nil {
			return err
		}
		proxy.WithRelay(credentials.RelayClient())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// A master key rotation changes the key file: the relay presents and
	// trusts what it holds within a few seconds, ahead of the rotation's
	// next step.
	if key != nil {
		go key.Watch(ctx, edgeKeyCheckEvery, func() {
			if err := credentials.Update(key.DeriveAll(clusterSeed), time.Now()); err != nil {
				log.Error("follow the master key ring", "error", err)
				return
			}
			log.Info("the relay follows the master key ring", "keys", key.Ring())
		}, log)
	}
	// Routes are followed until this edge has drained, not only until it
	// handed over: a request held for a moving tenant still serves it.
	following, stopFollowing := context.WithCancel(context.Background())
	defer stopFollowing()
	go followRoutes(following, api.NewClient(cfg.Socket), table, cfg.Edge.Routes, log)
	go reportServed(following, api.NewClient(cfg.Socket), edgeInstance(), proxy.Served(), table, log)
	if credentials != nil {
		go keepWarm(following, proxy, table)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
	go func() {
		for sig := range signals {
			if sig != syscall.SIGHUP {
				upgrader.Stop()
				return
			}
			log.Info("starting the edge anew to take over")
			if err := upgrader.Upgrade(); err != nil {
				log.Error("the new edge did not take over; this one goes on", "error", err)
				// The new process failed (rather than one being under way
				// already): an install waiting on it learns at once.
				// tableflip (v1.2) says "child" in exactly those errors:
				// can't start child, child exited, new child timed out.
				if strings.Contains(err.Error(), "child") {
					if err := upgrade.RecordHandoverFailure(cfg.Edge.PIDFile, err); err != nil {
						log.Warn("say that the handover failed", "error", err)
					}
				}
			}
		}
	}()
	go func() {
		<-upgrader.Exit()
		cancel()
	}()

	failed := make(chan error, 3)
	var servers []*http.Server
	var listeners []net.Listener
	serve := func(name string, server *http.Server, listener net.Listener) {
		servers = append(servers, server)
		listeners = append(listeners, listener)
		go func() {
			if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				failed <- fmt.Errorf("%s listener: %w", name, err)
			}
		}()
	}
	listen := func(name, address string) (net.Listener, error) {
		listener, err := upgrader.Listen("tcp", address)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		return listener, nil
	}
	// On a first start with TLS, plain HTTP answers while the certificate
	// is obtained (503 for tenants until HTTPS serves). An edge taking over
	// from an old one gets its certificate, from storage, before it serves
	// anything: it serves nothing the old one would serve better.
	tlsOn := cfg.Edge.TLS.Mode != edge.TLSOff
	var ready atomic.Bool
	plain := publicServer(proxy)
	if tlsOn {
		plain = publicServer(proxy.RedirectToHTTPS(ready.Load))
	}
	if !tlsOn || !upgrader.HasParent() {
		httpListener, err := listen("edge http", cfg.Edge.HTTP)
		if err != nil {
			return err
		}
		serve("edge http", plain, httpListener)
	}
	var certificates *edge.Certificates
	if tlsOn {
		if certificates = obtainTLS(ctx, cfg, log); certificates == nil {
			return nil // stopped while waiting
		}
		defer certificates.Close()
		if upgrader.HasParent() {
			// Its certificate is loaded: plain HTTP redirects at once, as
			// the edge before it did, instead of answering "starting".
			ready.Store(true)
			httpListener, err := listen("edge http", cfg.Edge.HTTP)
			if err != nil {
				return err
			}
			serve("edge http", plain, httpListener)
		}
		httpsListener, err := listen("edge https", cfg.Edge.HTTPS)
		if err != nil {
			return err
		}
		serve("edge https", publicServer(proxy), tls.NewListener(httpsListener, certificates.Config))
		ready.Store(true)
	}
	if credentials != nil {
		relayListener, err := listen("edge relay", cfg.RelayAddress())
		if err != nil {
			return err
		}
		serve("edge relay", publicServer(proxy.Relay()), tls.NewListener(relayListener, credentials.RelayServer()))
	}
	if cfg.Edge.Metrics != "" {
		metricsListener, err := listen("edge metrics", cfg.Edge.Metrics)
		if err != nil {
			return err
		}
		if host, _, _ := net.SplitHostPort(cfg.Edge.Metrics); !privateHost(host) {
			log.Warn("edge metrics are served on a public address, without authentication: firewall the port", "metrics", cfg.Edge.Metrics)
		}
		observed := &edgeMetrics{edge: proxy.Stats(), table: table}
		observed.certificates.Store(certificates)
		serve("edge metrics", &http.Server{Handler: observed, ReadHeaderTimeout: 10 * time.Second}, metricsListener)
	}
	if err := upgrader.Ready(); err != nil {
		return err
	}
	log.Info("murder edge serving", "baseDomain", cfg.BaseDomain, "tls", cfg.Edge.TLS.Mode, "routes", len(table.Load().Routes))
	var runErr error
	select {
	case <-upgrader.Exit():
	case runErr = <-failed:
		log.Error("stopping the edge", "error", runErr)
	}
	// The listeners are the new edge's now, or closed. This one stops
	// accepting and keeping connections alive, but still answers requests
	// on connections it accepted (a server shutting down drops a request
	// that arrives after it began), then finishes what it serves, and
	// closes what is left, such as event streams.
	drain, stopDraining := context.WithTimeout(context.Background(), cfg.Edge.Drain.Duration)
	defer stopDraining()
	for _, server := range servers {
		server.SetKeepAlivesEnabled(false)
	}
	for _, listener := range listeners {
		_ = listener.Close()
	}
	select {
	case <-time.After(min(handoverGrace, cfg.Edge.Drain.Duration)):
	case <-drain.Done():
	}
	for _, server := range servers {
		if err := server.Shutdown(drain); err != nil {
			_ = server.Close()
		}
	}
	// Connections taken over for a protocol switch (WebSockets) are not
	// the servers' to wait for: they get the rest of the drain too.
	proxy.WaitUpgraded(drain)
	log.Info("murder edge stopped")
	return runErr
}

// followRoutes keeps table as the agent's routes, asking again as soon as
// they change, and saves each new version. While the agent does not
// answer, the table stays as it was.
func followRoutes(ctx context.Context, client *api.Client, table *edge.LiveTable, path string, log *slog.Logger) {
	delay := time.Second
	failing := false
	for ctx.Err() == nil {
		version := table.Load().Version
		// Until the agent has confirmed routes, it is asked for its own at
		// once, not for a change from those restored from disk: requests
		// may be waiting for them.
		after := version
		if table.Updated().IsZero() {
			after = ""
		}
		request, cancel := context.WithTimeout(ctx, 2*routesPoll)
		got, err := client.Routes(request, after)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if !failing {
				log.Warn("the agent does not answer; serving the last routes", "error", err)
				failing = true
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			// Asked again every second until the agent first answers.
			if !table.Updated().IsZero() {
				delay = min(2*delay, 10*time.Second)
			}
			continue
		}
		if failing {
			log.Info("the agent answers again")
			failing = false
		}
		delay = time.Second
		if got.Version == version {
			table.Touch()
			continue
		}
		// Saved first: what is on disk is never older than what is served.
		if err := edge.SaveTable(path, got); err != nil {
			log.Warn("save the routes", "error", err)
		}
		table.Set(got)
	}
}

// handoverGrace is how long an edge that stopped accepting keeps answering
// requests on connections it accepted before it shuts down: a client sends
// its request right after connecting.
const handoverGrace = 2 * time.Second

// warmEvery keeps a connection to each other node's edge from going idle
// for long enough to close (the transport closes them after 90 s).
const warmEvery = 30 * time.Second

// edgeInstance names this edge process: while it hands over to the next,
// both tell the agent their counts, each since it started.
func edgeInstance() measure.EdgeInstance {
	id := make([]byte, 8)
	_, _ = rand.Read(id)
	return measure.EdgeInstance{ID: fmt.Sprintf("%d-%x", os.Getpid(), id), Started: time.Now()}
}

// reportServed tells the agent, every so often, how many requests this
// edge passed to its node's containers, and how they went: the agent
// measures its tenants by them.
func reportServed(ctx context.Context, client *api.Client, self measure.EdgeInstance, served *metrics.Edge, table *edge.LiveTable, log *slog.Logger) {
	ticker := time.NewTicker(servedEvery)
	defer ticker.Stop()
	quiet := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		routes := table.Load()
		served.Keep(func(tenant string) bool { _, ok := routes.Routes[tenant]; return ok })
		request, cancel := context.WithTimeout(ctx, servedEvery)
		err := client.EdgeUsage(request, self, served.Counts())
		cancel()
		// Said once while it lasts: an agent restarting, or one of an
		// older release, takes none for a while.
		switch {
		case err != nil && !quiet:
			log.Info("the agent takes no request counts for now", "error", err)
			quiet = true
		case err == nil:
			quiet = false
		}
	}
}

// servedEvery is how often the edge tells the agent its counts.
const servedEvery = 15 * time.Second

// keepWarm keeps a connection open to every other node's edge the routes
// lead to, opening one as soon as the routes name a new one.
func keepWarm(ctx context.Context, proxy *edge.Proxy, table *edge.LiveTable) {
	for {
		changed := table.Watch()
		proxy.Warm(ctx, edge.Relays(table.Load()))
		select {
		case <-ctx.Done():
			return
		case <-changed:
		case <-time.After(warmEvery):
		}
	}
}

// routesPoll is about how long the agent holds a request for new routes.
const routesPoll = 25 * time.Second

// edgeMetrics serves the edge's /metrics.
type edgeMetrics struct {
	edge         *metrics.Edge
	table        *edge.LiveTable
	certificates atomic.Pointer[edge.Certificates]
}

func (m *edgeMetrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/metrics" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	out := metrics.NewWriter(w)
	table := m.table.Load()
	// A tenant no longer routed loses its counts.
	m.edge.Keep(func(tenant string) bool { _, ok := table.Routes[tenant]; return ok })
	m.edge.Write(out)
	out.Family("murder_edge_routes", "gauge", "Tenants the edge routes.")
	out.Sample("murder_edge_routes", float64(len(table.Routes)))
	if at := m.table.Updated(); !at.IsZero() {
		out.Family("murder_edge_routes_timestamp_seconds", "gauge", "When the agent last confirmed the edge's routes: an old one means the agent does not answer.")
		out.Sample("murder_edge_routes_timestamp_seconds", metrics.Timestamp(at))
	}
	if certificates := m.certificates.Load(); certificates != nil {
		if at := certificates.NotAfter(); !at.IsZero() {
			out.Family("murder_tls_certificate_expiry_timestamp_seconds", "gauge", "When the first of the edge's certificates expires.")
			out.Sample("murder_tls_certificate_expiry_timestamp_seconds", metrics.Timestamp(at))
		}
	}
	_ = out.Err()
}

// publicServer has no write timeout: tenants stream server-sent events.
// A request's body must keep coming, however slowly, once its headers came.
func publicServer(handler http.Handler) *http.Server {
	return &http.Server{Handler: edge.UploadDeadline(handler, uploadIdle), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute, MaxHeaderBytes: 64 << 10}
}

// uploadIdle is how long a client may send nothing of a request's body.
var uploadIdle = edge.UploadIdle

// obtainTLS retries until the certificate is available or ctx ends; the
// reconciler and the plain listener keep working meanwhile.
func obtainTLS(ctx context.Context, cfg config.Agent, log *slog.Logger) *edge.Certificates {
	delay := 10 * time.Second
	for {
		certificates, err := edge.NewTLS(ctx, cfg.Edge.TLS, cfg.BaseDomain, log)
		if err == nil {
			return certificates
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

// edgeKeyCheckEvery is how often the edge reads the master key file again:
// well within a rotation's wait between steps.
const edgeKeyCheckEvery = 2 * time.Second
