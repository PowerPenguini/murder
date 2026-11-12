package panel

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/tunnel"
)

// TunnelAgent is what the tunnel asks of the agent.
type TunnelAgent interface {
	SetTunnel(ctx context.Context, end store.Tunnel) error
	Devices(ctx context.Context) ([]store.Device, error)
}

// TunnelConfig is this node's end of the panel's tunnel.
type TunnelConfig struct {
	PrivateKey string
	Address    netip.Addr
	Port       int
	// Name is the panel's host name; TLS serves its certificate.
	Name string
	TLS  *tls.Config
	// HTTPSPort and HTTPPort are where the panel listens inside the
	// tunnel; 443 and 80 if zero.
	HTTPSPort, HTTPPort int
	// Every is how often the devices are read; 5 s if zero.
	Every time.Duration
}

// ServeTunnel serves handler inside the panel's tunnel until ctx ends:
// over HTTPS, with plain HTTP sending browsers there. Only the devices the
// state lists reach it; the list is read every few seconds.
func ServeTunnel(ctx context.Context, agent TunnelAgent, config TunnelConfig, handler http.Handler, log *slog.Logger) error {
	public, err := tunnel.PublicKey(config.PrivateKey)
	if err != nil {
		return err
	}
	// The UDP port is bound here, and closes while the panel restarts: no
	// socket unit holds it, as murder-edge.socket holds the edge's ports.
	// A new panel has none of the old one's session keys, so it would drop
	// the packets held for it anyway; a device gets through once it makes
	// a new handshake, which it does on its own (docs/operations.md).
	end, err := tunnel.Start(config.PrivateKey, config.Address, config.Port, log)
	if err != nil {
		return err
	}
	defer end.Close()
	httpsPort, httpPort, every := or(config.HTTPSPort, 443), or(config.HTTPPort, 80), config.Every
	if every == 0 {
		every = 5 * time.Second
	}
	secure, err := end.Listen(httpsPort)
	if err != nil {
		return err
	}
	plain, err := end.Listen(httpPort)
	if err != nil {
		return err
	}
	// Headers within a time, idle connections closed after one; no
	// whole-request bound, which a binary's upload and a long action would
	// outlast: the panel's handler bounds each body itself.
	servers := []*http.Server{
		{Handler: handler, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: IdleTimeout, TLSConfig: config.TLS},
		// It only redirects: a whole request within a time, the body it
		// never reads (and Go drains) too.
		{Handler: redirect(config.Name), ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 10 * time.Second, IdleTimeout: IdleTimeout},
	}
	go func() { _ = servers[0].Serve(tls.NewListener(secure, config.TLS)) }()
	go func() { _ = servers[1].Serve(plain) }()
	log.Info("the panel serves in its tunnel", "address", config.Address.String(), "port", config.Port, "url", "https://"+config.Name)

	ours := store.Tunnel{Address: config.Address, Key: public, Port: config.Port}
	published, lastPeers := false, ""
	// read is when the devices were last read: the tunnel starts with
	// none let in, so it counts as read now.
	read := time.Now()
	// Failures in a row: the agent may still be starting, so only the
	// third is told.
	failed := map[string]int{}
	report := func(what, message string, err error) {
		if failed[what]++; failed[what] == 3 {
			log.Warn(message, "error", err)
		}
	}
	// Each call to the agent gets a polling interval at most: one that
	// hangs must not stop the polling, which shuts out removed devices.
	call := func() (context.Context, context.CancelFunc) { return context.WithTimeout(ctx, every) }
	for {
		// The state names this end, for the devices' configurations; as
		// after this node left the cluster and came back, again and again.
		callCtx, cancel := call()
		err := agent.SetTunnel(callCtx, ours)
		cancel()
		if err != nil && ctx.Err() == nil {
			report("tunnel", "record this node's end of the panel's tunnel", err)
		} else if err == nil {
			if !published {
				log.Info("this node's end of the panel's tunnel is recorded")
			}
			published, failed["tunnel"] = true, 0
		}
		callCtx, cancel = call()
		devices, err := agent.Devices(callCtx)
		cancel()
		if err != nil && ctx.Err() == nil {
			report("devices", "read the devices that may reach the panel", err)
		} else if err == nil {
			read, failed["devices"] = time.Now(), 0
			peers, key := devicePeers(devices)
			if key != lastPeers {
				if err := end.SetPeers(peers); err != nil {
					log.Warn("let the devices reach the panel", "error", err)
				} else {
					lastPeers = key
					log.Info("the devices that may reach the panel", "devices", len(peers))
				}
			}
		}
		// Not knowing which devices may, however the reading failed, it
		// lets none in until it knows again: one removed meanwhile must not
		// reach this node.
		if lastPeers != "" && time.Since(read) > staleDevices*every {
			if err := end.SetPeers(nil); err == nil {
				lastPeers = ""
				log.Warn("no device reaches the panel on this node until the devices can be read again")
			}
		}
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return errors.Join(servers[0].Shutdown(shutdown), servers[1].Shutdown(shutdown))
		case <-time.After(every):
		}
	}
}

// TunnelClaimer is what taking an address in the tunnel asks of the
// agent.
type TunnelClaimer interface {
	Nodes(ctx context.Context) (api.Nodes, error)
	SetTunnel(ctx context.Context, end store.Tunnel) error
}

// ClaimTunnelAddress is this node's address in the panel's tunnel, for a
// config that names none: the one the cluster's state has for this node,
// or else the lowest one below devices' that no node has, which it records
// there with end's key and port. The state refuses an address another
// node took meanwhile, and it then tries the next. It waits, every every,
// for the agent and for this node to be in the cluster.
func ClaimTunnelAddress(ctx context.Context, agent TunnelClaimer, network netip.Prefix, end store.Tunnel, every time.Duration, log *slog.Logger) (netip.Addr, error) {
	for attempt := 1; ; attempt++ {
		call, cancel := context.WithTimeout(ctx, every+5*time.Second)
		view, err := agent.Nodes(call)
		cancel()
		if err == nil {
			address, recorded, ok := tunnelAddressIn(view, network)
			switch {
			case recorded:
				return address, nil
			case !ok:
				err = fmt.Errorf("no address is left for a node in the panel's tunnel %s", network)
			default:
				end.Address = address
				call, cancel := context.WithTimeout(ctx, every+5*time.Second)
				err = agent.SetTunnel(call, end)
				cancel()
				if err == nil {
					log.Info("this node took an address in the panel's tunnel", "address", address.String())
					return address, nil
				}
			}
		}
		if attempt%5 == 3 {
			log.Warn("the panel waits for an address in its tunnel", "error", err)
		}
		select {
		case <-ctx.Done():
			return netip.Addr{}, ctx.Err()
		case <-time.After(every):
		}
	}
}

// tunnelAddressIn is the address the cluster's state has for this node in
// network (recorded), or else the lowest one no node has; ok is false when
// none is left.
func tunnelAddressIn(view api.Nodes, network netip.Prefix) (address netip.Addr, recorded, ok bool) {
	network = network.Masked()
	used := map[netip.Addr]bool{}
	for _, node := range view.Nodes {
		if node.Tunnel == nil {
			continue
		}
		if node.Name == view.Self && network.Contains(node.Tunnel.Address) {
			return node.Tunnel.Address, true, true
		}
		used[node.Tunnel.Address] = true
	}
	// Devices begin at the 100th address.
	address = network.Addr().Next()
	for i := 1; i < 100 && network.Contains(address); i++ {
		if !used[address] {
			return address, false, true
		}
		address = address.Next()
	}
	return netip.Addr{}, false, false
}

// staleDevices is how many polling intervals the devices may go unread
// before the tunnel lets no device in.
const staleDevices = 3

// devicePeers are the devices as the tunnel's peers, and a key that
// changes when they do.
func devicePeers(devices []store.Device) ([]tunnel.Peer, string) {
	slices.SortFunc(devices, func(a, b store.Device) int { return strings.Compare(a.Name, b.Name) })
	peers := make([]tunnel.Peer, 0, len(devices))
	var key strings.Builder
	for _, device := range devices {
		peers = append(peers, tunnel.Peer{Key: device.Key, Allowed: netip.PrefixFrom(device.Address, 32)})
		key.WriteString(device.Key + "=" + device.Address.String() + ";")
	}
	return peers, key.String()
}

// redirect sends plain HTTP to the panel's HTTPS address.
func redirect(name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://"+name+r.URL.RequestURI(), http.StatusMovedPermanently)
	})
}

func or(value, fallback int) int {
	if value == 0 {
		return fallback
	}
	return value
}

// tunnelAddress is where a request came from inside the tunnel.
func tunnelAddress(r *http.Request) netip.Addr {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return netip.Addr{}
	}
	address, _ := netip.ParseAddr(host)
	return address
}
