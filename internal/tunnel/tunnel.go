// Package tunnel runs the panel's WireGuard tunnel in userspace: no
// network interface on the host, so tenants cannot reach what listens in
// it, and nothing about the host's network changes. A node's end listens
// for devices; a device's end, for "murder admin connect", dials the
// nodes.
package tunnel

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"

	"golang.zx2c4.com/wireguard/conn"
	"golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/tun/netstack"
)

// MTU leaves room for WireGuard's own headers in a 1500-byte packet.
const MTU = 1420

// DefaultPort is WireGuard's usual UDP port.
const DefaultPort = 51820

// GenerateKey makes a WireGuard key pair, in base64.
func GenerateKey() (private, public string, err error) {
	key, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return "", "", err
	}
	return base64.StdEncoding.EncodeToString(key.Bytes()), base64.StdEncoding.EncodeToString(key.PublicKey().Bytes()), nil
}

// PublicKey is the public key of a private one, both in base64.
func PublicKey(private string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(private))
	if err != nil {
		return "", fmt.Errorf("a WireGuard key: %w", err)
	}
	key, err := ecdh.X25519().NewPrivateKey(raw)
	if err != nil {
		return "", fmt.Errorf("a WireGuard key: %w", err)
	}
	return base64.StdEncoding.EncodeToString(key.PublicKey().Bytes()), nil
}

// hexKey turns a base64 key into the hex wireguard-go's settings take.
func hexKey(key string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(key))
	if err != nil || len(raw) != 32 {
		return "", fmt.Errorf("a WireGuard key is 32 bytes in base64")
	}
	return hex.EncodeToString(raw), nil
}

// Peer is the other end of the tunnel: a device, as a node sees it, or a
// node, as a device does.
type Peer struct {
	Key string
	// Allowed is what the peer may send from, and is sent to: a device's
	// address, or a node's.
	Allowed netip.Prefix
	// Endpoint is where a node is reached; a device's is learned when it
	// writes.
	Endpoint netip.AddrPort
	// Keepalive, in seconds, keeps a device's way through NAT open.
	Keepalive int
}

// Tunnel is one end of the tunnel.
type Tunnel struct {
	device  *device.Device
	net     *netstack.Net
	address netip.Addr
}

// Start brings up an end of the tunnel with address inside it, and the
// private key; port 0 picks any UDP port, as a device's end does.
func Start(private string, address netip.Addr, port int, log *slog.Logger) (*Tunnel, error) {
	key, err := hexKey(private)
	if err != nil {
		return nil, err
	}
	tun, tnet, err := netstack.CreateNetTUN([]netip.Addr{address}, nil, MTU)
	if err != nil {
		return nil, err
	}
	logger := &device.Logger{
		Verbosef: func(string, ...any) {},
		Errorf:   func(format string, args ...any) { log.Warn("wireguard: " + fmt.Sprintf(format, args...)) },
	}
	dev := device.NewDevice(tun, conn.NewDefaultBind(), logger)
	settings := "private_key=" + key + "\n"
	if port > 0 {
		settings += fmt.Sprintf("listen_port=%d\n", port)
	}
	if err := dev.IpcSet(settings); err != nil {
		dev.Close()
		return nil, err
	}
	if err := dev.Up(); err != nil {
		dev.Close()
		return nil, err
	}
	return &Tunnel{device: dev, net: tnet, address: address}, nil
}

// SetPeers makes peers the only ones the tunnel talks to.
func (t *Tunnel) SetPeers(peers []Peer) error {
	var b strings.Builder
	b.WriteString("replace_peers=true\n")
	for _, peer := range peers {
		key, err := hexKey(peer.Key)
		if err != nil {
			return err
		}
		fmt.Fprintf(&b, "public_key=%s\nreplace_allowed_ips=true\nallowed_ip=%s\n", key, peer.Allowed)
		if peer.Endpoint.IsValid() {
			fmt.Fprintf(&b, "endpoint=%s\n", peer.Endpoint)
		}
		if peer.Keepalive > 0 {
			fmt.Fprintf(&b, "persistent_keepalive_interval=%d\n", peer.Keepalive)
		}
	}
	return t.device.IpcSet(b.String())
}

// Listen listens on port of this end's address, inside the tunnel.
func (t *Tunnel) Listen(port int) (net.Listener, error) {
	return t.net.ListenTCPAddrPort(netip.AddrPortFrom(t.address, uint16(port)))
}

// Dial connects to address inside the tunnel.
func (t *Tunnel) Dial(ctx context.Context, address netip.AddrPort) (net.Conn, error) {
	return t.net.DialContextTCPAddrPort(ctx, address)
}

// Address is this end's address in the tunnel.
func (t *Tunnel) Address() netip.Addr { return t.address }

// Close takes this end down.
func (t *Tunnel) Close() { t.device.Close() }

// LoadKey reads the private key in path, making one there, readable by
// its owner only, if there is none.
func LoadKey(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err == nil {
		key := strings.TrimSpace(string(data))
		if _, err := PublicKey(key); err != nil {
			return "", fmt.Errorf("%s: %w", path, err)
		}
		return key, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	private, _, err := GenerateKey()
	if err != nil {
		return "", err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", err
	}
	_, err = file.WriteString(private + "\n")
	if err = errors.Join(err, file.Close()); err != nil {
		return "", err
	}
	return private, nil
}
