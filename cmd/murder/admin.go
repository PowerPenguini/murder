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
	"github.com/PowerPenguini/murder/internal/panel"
	"github.com/PowerPenguini/murder/internal/tunnel"
)

const adminUsage = `Usage: murder admin <command> [arguments]

  device add NAME [-out FILE]   let a device reach the panel: prints its
                                WireGuard configuration and a QR code for
                                the WireGuard app; its private key is
                                shown once and kept nowhere
  device list                   the devices that may reach the panel
  device remove NAME            stop a device reaching the panel
  invite NAME                   let a person add a passkey: prints a link,
                                valid 24 hours, once (a new person, or
                                one who lost every passkey and code)
  user list                     the people who sign in
  user remove NAME              stop a person signing in
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
	case "invite", "user":
		return runPeople(args)
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

func runPeople(args []string) error {
	flags := flag.NewFlagSet("admin "+args[0], flag.ContinueOnError)
	socket := socketPath(flags)
	configPath := flags.String("config", defaultConfig, "agent configuration file, for the panel's name")
	rest, err := parseInterspersed(flags, args[1:])
	if err != nil {
		return err
	}
	client := api.NewClient(*socket)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	switch {
	case args[0] == "invite" && len(rest) == 1:
		cfg, err := config.Load(*configPath)
		if err != nil {
			return err
		}
		token := panel.Token(32)
		if _, err := client.InvitePanelUser(ctx, rest[0], api.InviteRequest{Hash: panel.TokenHash(token), Expires: time.Now().Add(panel.InviteLength)}); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "%s adds a passkey, once, within 24 hours, at:\n\n  https://%s/enroll#%s\n\nSend it through a channel you trust: it lets whoever has it sign in as %s.\n", rest[0], cfg.Panel.Name, token, rest[0])
		return nil
	case args[0] == "user" && len(rest) == 1 && rest[0] == "list":
		users, err := client.PanelUsers(ctx)
		if err != nil {
			return err
		}
		w := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "NAME\tPASSKEYS\tRECOVERY CODES\tADDED BY")
		for _, u := range users {
			var labels []string
			for _, k := range u.Passkeys {
				labels = append(labels, k.Label)
			}
			fmt.Fprintf(w, "%s\t%s\t%d\t%s\n", u.Name, strings.Join(labels, ", "), u.Recovery, u.CreatedBy)
		}
		return w.Flush()
	case args[0] == "user" && len(rest) == 2 && rest[0] == "remove":
		if err := client.RemovePanelUser(ctx, rest[1]); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "%s no longer signs in to the panel\n", rest[1])
		return nil
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
	ends, left := panel.Ends(nodes.Nodes)
	for _, name := range left {
		fmt.Fprintf(os.Stderr, "murder: node %s is left out: its panel has not run, or it has no public address (cluster.publicAddresses)\n", name)
	}
	if len(ends) == 0 {
		return errors.New("no node serves the panel in its tunnel yet: start murder-panel, with panel.tunnel and cluster.publicAddresses in its config")
	}
	// The file is made new, readable by its owner only: never one that
	// is there already, or a link, which would get the private key.
	var file *os.File
	if out != "" {
		if file, err = os.OpenFile(out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600); err != nil {
			return err
		}
		defer file.Close()
	}
	private, public, err := tunnel.GenerateKey()
	if err != nil {
		return err
	}
	device, err := client.AddDevice(ctx, api.DeviceRequest{Name: name, Key: public, Network: network})
	if err != nil {
		if file != nil {
			_ = os.Remove(out)
		}
		return err
	}
	config := tunnel.DeviceConfig{PrivateKey: private, Address: device.Address, Panel: "https://" + cfg.Panel.Name, Nodes: ends}
	text := config.String()
	if file != nil {
		if _, err := file.WriteString(text); err != nil {
			// Its private key is lost: the device goes too.
			_ = client.RemoveDevice(context.WithoutCancel(ctx), name)
			_ = os.Remove(out)
			return err
		}
	}
	fmt.Fprintf(stdout, "Device %s reaches the panel at https://%s from %s.\n", name, cfg.Panel.Name, device.Address)
	if qr, err := tunnel.QRText(config.Compact()); err == nil {
		fmt.Fprintf(stdout, "Scan this with the WireGuard app (it holds the device's private key, shown once):\n\n%s\n", qr)
	} else {
		fmt.Fprintf(stdout, "The configuration is too long for a QR code (%v): import it from text.\n", err)
	}
	if file != nil {
		fmt.Fprintf(stdout, "The configuration is in %s, for a computer's WireGuard app or murder admin connect.\n", out)
	} else {
		fmt.Fprintf(stdout, "The configuration, for a computer's WireGuard app or murder admin connect:\n\n%s\n", text)
	}
	fmt.Fprintf(stdout, "The name %s must lead to %s: the leader's DNS records do (cluster.dns), or a hosts file entry.\n", cfg.Panel.Name, nodeAddresses(ends))
	return nil
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
		// Each way ends on its own: a side done sending may still read.
		done := make(chan struct{}, 2)
		go func() { _, _ = io.Copy(remote, local); closeWrite(remote); done <- struct{}{} }()
		go func() { _, _ = io.Copy(local, remote); closeWrite(local); done <- struct{}{} }()
		<-done
		<-done
		return
	}
}

func closeWrite(conn net.Conn) {
	if c, ok := conn.(interface{ CloseWrite() error }); ok {
		_ = c.CloseWrite()
		return
	}
	_ = conn.Close()
}
