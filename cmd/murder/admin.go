package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/tunnel"
)

const adminUsage = `Usage: murder admin <command> [arguments]

  device add NAME [-out FILE]   let a device reach the panel: prints its
                                WireGuard configuration and a QR code for
                                the WireGuard app; its private key is
                                shown once and kept nowhere
  device list                   the devices that may reach the panel
  device remove NAME            stop a device reaching the panel
  connect -device FILE          on a computer without the WireGuard app:
                                reach the panel through the tunnel at
                                https://<panel name>:8443 (see -listen)
`

func runAdmin(args []string) error {
	if len(args) == 0 || args[0] == "-h" || args[0] == "help" {
		fmt.Fprint(os.Stderr, adminUsage)
		return flag.ErrHelp
	}
	switch args[0] {
	case "device":
		return runDevice(args[1:])
	case "connect":
		return runConnect(args[1:])
	}
	fmt.Fprint(os.Stderr, adminUsage)
	return fmt.Errorf("unknown admin command %q", args[0])
}

func runDevice(args []string) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, adminUsage)
		return flag.ErrHelp
	}
	flags := flag.NewFlagSet("admin device "+args[0], flag.ContinueOnError)
	socket := socketPath(flags)
	configPath := flags.String("config", defaultConfig, "agent configuration file, for the tunnel's network")
	out := flags.String("out", "", "with add: write the configuration to this file (0600) rather than print it")
	rest, err := parseInterspersed(flags, args[1:])
	if err != nil {
		return err
	}
	client := api.NewClient(*socket)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	switch command := args[0]; {
	case command == "list" && len(rest) == 0:
		devices, err := client.Devices(ctx)
		if err != nil {
			return err
		}
		w := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "NAME\tADDRESS\tADDED\tBY")
		for _, d := range devices {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", d.Name, d.Address, d.CreatedAt.UTC().Format("2006-01-02 15:04"), d.CreatedBy)
		}
		return w.Flush()
	case command == "remove" && len(rest) == 1:
		if err := client.RemoveDevice(ctx, rest[0]); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "device %s no longer reaches the panel\n", rest[0])
		return nil
	case command == "add" && len(rest) == 1:
		cfg, err := config.Load(*configPath)
		if err != nil {
			return err
		}
		if cfg.Panel.Tunnel == nil {
			return errors.New("this node's config has no panel.tunnel: the panel serves in no tunnel")
		}
		return addDevice(ctx, client, rest[0], cfg, *out)
	}
	fmt.Fprint(os.Stderr, adminUsage)
	return flag.ErrHelp
}

// addDevice adds a device and prints, once, what it needs to connect.
func addDevice(ctx context.Context, client *api.Client, name string, cfg config.Agent, out string) error {
	network, err := netip.ParsePrefix(cfg.Panel.Tunnel.Network)
	if err != nil {
		return err
	}
	nodes, err := client.Nodes(ctx)
	if err != nil {
		return err
	}
	var ends []tunnel.Node
	for _, node := range nodes.Nodes {
		endpoint := firstIPv4(node.Public)
		if node.Tunnel == nil || !endpoint.IsValid() {
			fmt.Fprintf(os.Stderr, "murder: node %s is left out: its panel has not run, or it has no public address (cluster.publicAddresses)\n", node.Name)
			continue
		}
		ends = append(ends, tunnel.Node{Name: node.Name, Key: node.Tunnel.Key, Address: node.Tunnel.Address, Endpoint: netip.AddrPortFrom(endpoint, uint16(node.Tunnel.Port))})
	}
	if len(ends) == 0 {
		return errors.New("no node serves the panel in its tunnel yet: start murder-panel, with panel.tunnel and cluster.publicAddresses in its config")
	}
	private, public, err := tunnel.GenerateKey()
	if err != nil {
		return err
	}
	device, err := client.AddDevice(ctx, api.DeviceRequest{Name: name, Key: public, Network: network})
	if err != nil {
		return err
	}
	text := tunnel.DeviceConfig{PrivateKey: private, Address: device.Address, Panel: "https://" + cfg.Panel.Name, Nodes: ends}.String()
	qr, err := tunnel.QRText(text)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "Device %s reaches the panel at https://%s from %s.\nScan this with the WireGuard app (it holds the device's private key, shown once):\n\n%s\n",
		name, cfg.Panel.Name, device.Address, qr)
	if out != "" {
		if err := os.WriteFile(out, []byte(text), 0o600); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "The same configuration is in %s, for a computer's WireGuard app or murder admin connect.\n", out)
	} else {
		fmt.Fprintf(stdout, "Or import this configuration on a computer:\n\n%s\n", text)
	}
	fmt.Fprintf(stdout, "The name %s must lead to %s: the leader's DNS records do (cluster.dns), or a hosts file entry.\n", cfg.Panel.Name, nodeAddresses(ends))
	return nil
}

func firstIPv4(addresses []netip.Addr) netip.Addr {
	for _, address := range addresses {
		if address.Unmap().Is4() {
			return address.Unmap()
		}
	}
	return netip.Addr{}
}

func nodeAddresses(nodes []tunnel.Node) string {
	var out []string
	for _, node := range nodes {
		out = append(out, node.Address.String())
	}
	slices.Sort(out)
	return strings.Join(out, ", ")
}

// runConnect reaches the panel through the tunnel without the WireGuard
// app: it listens on a local port and passes each connection to the
// panel's HTTPS port inside the tunnel, at the first node that answers.
func runConnect(args []string) error {
	flags := flag.NewFlagSet("admin connect", flag.ContinueOnError)
	devicePath := flags.String("device", "", "the device's WireGuard configuration, from murder admin device add -out")
	listen := flags.String("listen", "127.0.0.1:8443", "local address to listen on")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *devicePath == "" {
		return errors.New("-device is the device's configuration file")
	}
	data, err := os.ReadFile(*devicePath)
	if err != nil {
		return err
	}
	device, err := tunnel.ParseDeviceConfig(string(data))
	if err != nil {
		return err
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	end, err := tunnel.Start(device.PrivateKey, device.Address, 0, log)
	if err != nil {
		return err
	}
	defer end.Close()
	if err := end.SetPeers(device.Peers()); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		listener.Close()
	}()
	var preferred atomic.Int64
	host := strings.TrimPrefix(device.Panel, "https://")
	_, port, _ := net.SplitHostPort(*listen)
	fmt.Fprintf(stdout, "Reaching the panel through the tunnel. Point %s at 127.0.0.1 (a hosts file entry), then open https://%s:%s\n", host, host, port)
	for {
		local, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go pass(ctx, end, device.Nodes, &preferred, local, log)
	}
}

// pass joins a local connection to the panel's HTTPS port at the first
// node that answers, beginning with the one that answered last.
func pass(ctx context.Context, end *tunnel.Tunnel, nodes []tunnel.Node, preferred *atomic.Int64, local net.Conn, log *slog.Logger) {
	defer local.Close()
	first := int(preferred.Load())
	for i := range nodes {
		index := (first + i) % len(nodes)
		node := nodes[index]
		dial, cancel := context.WithTimeout(ctx, 3*time.Second)
		remote, err := end.Dial(dial, netip.AddrPortFrom(node.Address, 443))
		cancel()
		if err != nil {
			log.Warn("a node does not answer in the tunnel", "node", node.Name, "error", err)
			continue
		}
		preferred.Store(int64(index))
		defer remote.Close()
		done := make(chan struct{}, 2)
		go func() { _, _ = io.Copy(remote, local); done <- struct{}{} }()
		go func() { _, _ = io.Copy(local, remote); done <- struct{}{} }()
		<-done
		return
	}
}
