package panel

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"strings"
	"time"

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
	// Failures in a row: the agent may still be starting, so only the
	// third is told.
	failed := map[string]int{}
	report := func(what, message string, err error) {
		if failed[what]++; failed[what] == 3 {
			log.Warn(message, "error", err)
		}
	}
	for {
		// The state names this end, for the devices' configurations; as
		// after this node left the cluster and came back, again and again.
		if err := agent.SetTunnel(ctx, ours); err != nil && ctx.Err() == nil {
			report("tunnel", "record this node's end of the panel's tunnel", err)
		} else if err == nil {
			if !published {
				log.Info("this node's end of the panel's tunnel is recorded")
			}
			published, failed["tunnel"] = true, 0
		}
		if devices, err := agent.Devices(ctx); err != nil && ctx.Err() == nil {
			report("devices", "read the devices that may reach the panel", err)
			// Not knowing which devices may, it lets none in: one removed
			// meanwhile must not reach this node.
			if failed["devices"] == 3 && lastPeers != "" {
				if err := end.SetPeers(nil); err == nil {
					lastPeers = ""
					log.Warn("no device reaches the panel on this node until the devices can be read again")
				}
			}
		} else if err == nil {
			failed["devices"] = 0
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
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return errors.Join(servers[0].Shutdown(shutdown), servers[1].Shutdown(shutdown))
		case <-time.After(every):
		}
	}
}

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
