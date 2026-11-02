package tunnel

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"testing"
	"time"
)

func freeUDPPort(t *testing.T) int {
	t.Helper()
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	return c.LocalAddr().(*net.UDPAddr).Port
}

// A device whose key the node knows reaches what listens in the tunnel;
// one the node does not know reaches nothing.
func TestADeviceReachesThePanelThroughTheTunnel(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	nodePrivate, nodePublic, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	devicePrivate, devicePublic, _ := GenerateKey()
	strangerPrivate, _, _ := GenerateKey()
	port := freeUDPPort(t)
	nodeAddress, deviceAddress := netip.MustParseAddr("10.77.0.1"), netip.MustParseAddr("10.77.0.101")

	node, err := Start(nodePrivate, nodeAddress, port, log)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if err := node.SetPeers([]Peer{{Key: devicePublic, Allowed: netip.PrefixFrom(deviceAddress, 32)}}); err != nil {
		t.Fatal(err)
	}
	listener, err := node.Listen(8080)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "hello %s", r.RemoteAddr)
	})}
	go server.Serve(listener)
	defer server.Close()

	config := DeviceConfig{PrivateKey: devicePrivate, Address: deviceAddress, Panel: "https://panel.example.com",
		Nodes: []Node{{Name: "n1", Key: nodePublic, Address: nodeAddress, Endpoint: netip.MustParseAddrPort(fmt.Sprintf("127.0.0.1:%d", port))}}}
	// What a device's app reads is what connect reads back.
	parsed, err := ParseDeviceConfig(config.String())
	if err != nil || parsed.PrivateKey != devicePrivate || parsed.Address != deviceAddress || parsed.Panel != config.Panel || len(parsed.Nodes) != 1 || parsed.Nodes[0] != (Node{Key: nodePublic, Address: nodeAddress, Endpoint: config.Nodes[0].Endpoint}) {
		t.Fatalf("parsed %+v, %v", parsed, err)
	}
	get := func(private string, address netip.Addr) (string, error) {
		end, err := Start(private, address, 0, log)
		if err != nil {
			return "", err
		}
		defer end.Close()
		if err := end.SetPeers(parsed.Peers()); err != nil {
			return "", err
		}
		client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return end.Dial(ctx, netip.AddrPortFrom(nodeAddress, 8080))
		}}}
		response, err := client.Get("http://panel/")
		if err != nil {
			return "", err
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		return string(body), err
	}
	body, err := get(devicePrivate, deviceAddress)
	if err != nil || body != "hello 10.77.0.101:"+strings.Split(body, ":")[1] {
		t.Fatalf("through the tunnel: %q, %v", body, err)
	}
	if body, err := get(strangerPrivate, netip.MustParseAddr("10.77.0.150")); err == nil {
		t.Fatalf("a stranger got through: %q", body)
	}
	// Removed, the device reaches nothing.
	if err := node.SetPeers(nil); err != nil {
		t.Fatal(err)
	}
	if body, err := get(devicePrivate, deviceAddress); err == nil {
		t.Fatalf("a removed device got through: %q", body)
	}
}

func TestKeysAndQR(t *testing.T) {
	private, public, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	if derived, err := PublicKey(private); err != nil || derived != public {
		t.Fatalf("public key %q, want %q, %v", derived, public, err)
	}
	if _, err := PublicKey("not a key"); err == nil {
		t.Fatal("a bad key")
	}
	text, err := QRText("[Interface]\nPrivateKey = x\n")
	if err != nil || strings.Count(text, "\n") < 10 || !strings.Contains(text, "█") {
		t.Fatalf("QR %q, %v", text, err)
	}
	svg, err := QRSVG("hello")
	if err != nil || !strings.HasPrefix(svg, "<svg") || !strings.Contains(svg, "h1v1h-1z") {
		t.Fatalf("SVG %q, %v", svg, err)
	}
}
