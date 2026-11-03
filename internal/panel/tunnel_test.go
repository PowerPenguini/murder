package panel

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/tunnel"
)

// fakeAgent records this node's end and lists the devices it is given.
type fakeAgent struct {
	mu      sync.Mutex
	end     store.Tunnel
	devices []store.Device
	err     error
}

func (a *fakeAgent) SetTunnel(_ context.Context, end store.Tunnel) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.end = end
	return nil
}

func (a *fakeAgent) Devices(context.Context) ([]store.Device, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]store.Device(nil), a.devices...), a.err
}

func selfSigned(t *testing.T, name string) *tls.Config {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name}, DNSNames: []string{name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
}

// The panel serves in its tunnel, over HTTPS as its own name, to the
// devices the state lists, and records this node's end.
func TestThePanelServesInItsTunnel(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	nodePrivate, nodePublic, _ := tunnel.GenerateKey()
	devicePrivate, devicePublic, _ := tunnel.GenerateKey()
	udp, _ := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	port := udp.LocalAddr().(*net.UDPAddr).Port
	udp.Close()
	nodeAddress, deviceAddress := netip.MustParseAddr("10.77.0.1"), netip.MustParseAddr("10.77.0.101")
	agent := &fakeAgent{devices: []store.Device{{Name: "laptop", Key: devicePublic, Address: deviceAddress}}}
	s, err := New(Demo{Now: demoNow}, log)
	if err != nil {
		t.Fatal(err)
	}
	s.Name = "panel.example.com"
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() {
		served <- ServeTunnel(ctx, agent, TunnelConfig{PrivateKey: nodePrivate, Address: nodeAddress, Port: port, Name: s.Name,
			TLS: selfSigned(t, s.Name), HTTPSPort: 8443, HTTPPort: 8080, Every: 50 * time.Millisecond}, s.Handler(), log)
	}()
	defer func() {
		cancel()
		if err := <-served; err != nil {
			t.Error(err)
		}
	}()

	device, err := tunnel.Start(devicePrivate, deviceAddress, 0, log)
	if err != nil {
		t.Fatal(err)
	}
	defer device.Close()
	config := tunnel.DeviceConfig{PrivateKey: devicePrivate, Address: deviceAddress, Nodes: []tunnel.Node{{Name: "n1", Key: nodePublic, Address: nodeAddress, Endpoint: netip.MustParseAddrPort(fmt.Sprintf("127.0.0.1:%d", port))}}}
	if err := device.SetPeers(config.Peers()); err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true, ServerName: "panel.example.com"},
			DialContext: func(ctx context.Context, _, address string) (net.Conn, error) {
				_, port, _ := net.SplitHostPort(address)
				return device.Dial(ctx, netip.MustParseAddrPort("10.77.0.1:"+port))
			},
		}}
	var response *http.Response
	for range 50 { // until the node has read the devices
		if response, err = client.Get("https://panel.example.com:8443/"); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil || response.StatusCode != http.StatusOK || response.TLS == nil {
		t.Fatalf("the panel over HTTPS in the tunnel: %v, %v", response, err)
	}
	response.Body.Close()
	if redirect, err := client.Get("http://panel.example.com:8080/events?x=1"); err != nil || redirect.Header.Get("Location") != "https://panel.example.com/events?x=1" {
		t.Fatalf("plain HTTP: %v, %v", redirect, err)
	}
	agent.mu.Lock()
	end := agent.end
	agent.mu.Unlock()
	if end != (store.Tunnel{Address: nodeAddress, Key: nodePublic, Port: port}) {
		t.Fatalf("recorded end %+v", end)
	}
	// Not knowing the devices, as a node cut off from the cluster, it lets
	// none in.
	agent.mu.Lock()
	agent.err = errors.New("no leader")
	agent.mu.Unlock()
	time.Sleep(300 * time.Millisecond)
	client.Transport.(*http.Transport).CloseIdleConnections()
	client.Timeout = 2 * time.Second
	if _, err := client.Get("https://panel.example.com:8443/"); err == nil {
		t.Fatal("a removed device reached the panel")
	}
}

// hangingAgent is a fakeAgent whose calls can be made to hang until their
// context ends, as an agent that stopped answering.
type hangingAgent struct {
	*fakeAgent
	hangTunnel, hangDevices atomic.Bool
	// hanging counts the calls that hung.
	hanging atomic.Int64
}

func (a *hangingAgent) SetTunnel(ctx context.Context, end store.Tunnel) error {
	if a.hangTunnel.Load() {
		a.hanging.Add(1)
		<-ctx.Done()
		return ctx.Err()
	}
	return a.fakeAgent.SetTunnel(ctx, end)
}

func (a *hangingAgent) Devices(ctx context.Context) ([]store.Device, error) {
	if a.hangDevices.Load() {
		a.hanging.Add(1)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return a.fakeAgent.Devices(ctx)
}

// A call to the agent that hangs neither stops the panel from shutting
// out a removed device nor lets a device in while the devices cannot be
// read.
func TestAHangingAgentLetsNoRemovedDeviceIn(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	nodePrivate, nodePublic, _ := tunnel.GenerateKey()
	devicePrivate, devicePublic, _ := tunnel.GenerateKey()
	udp, _ := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	port := udp.LocalAddr().(*net.UDPAddr).Port
	udp.Close()
	nodeAddress, deviceAddress := netip.MustParseAddr("10.77.0.1"), netip.MustParseAddr("10.77.0.101")
	laptop := []store.Device{{Name: "laptop", Key: devicePublic, Address: deviceAddress}}
	agent := &hangingAgent{fakeAgent: &fakeAgent{devices: laptop}}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	go func() {
		served <- ServeTunnel(ctx, agent, TunnelConfig{PrivateKey: nodePrivate, Address: nodeAddress, Port: port, Name: "panel.example.com",
			TLS: selfSigned(t, "panel.example.com"), HTTPSPort: 8443, HTTPPort: 8080, Every: 50 * time.Millisecond}, ok, log)
	}()
	defer func() {
		cancel()
		if err := <-served; err != nil {
			t.Error(err)
		}
	}()
	device, err := tunnel.Start(devicePrivate, deviceAddress, 0, log)
	if err != nil {
		t.Fatal(err)
	}
	defer device.Close()
	config := tunnel.DeviceConfig{PrivateKey: devicePrivate, Address: deviceAddress, Nodes: []tunnel.Node{{Name: "n1", Key: nodePublic, Address: nodeAddress, Endpoint: netip.MustParseAddrPort(fmt.Sprintf("127.0.0.1:%d", port))}}}
	// connect starts the device's handshake anew once the node had time to
	// let it in: a device the node dropped would otherwise wait for
	// WireGuard to notice, 15 seconds on.
	connect := func() {
		t.Helper()
		time.Sleep(200 * time.Millisecond)
		if err := device.SetPeers(nil); err != nil {
			t.Fatal(err)
		}
		if err := device.SetPeers(config.Peers()); err != nil {
			t.Fatal(err)
		}
	}
	connect()
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true, ServerName: "panel.example.com"},
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return device.Dial(ctx, netip.MustParseAddrPort("10.77.0.1:8443"))
		},
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Timeout: time.Second, Transport: transport}
	reaches := func() bool {
		transport.CloseIdleConnections()
		response, err := client.Get("https://panel.example.com:8443/")
		if err != nil {
			return false
		}
		response.Body.Close()
		return true
	}
	// eventually waits for the panel to let the device in, or not: shut
	// out, twice in a row, so that one slow answer does not pass for it.
	eventually := func(want bool, what string) {
		t.Helper()
		start := time.Now()
		for settled := 0; settled < 1 || !want && settled < 2; {
			if reaches() == want {
				settled++
				continue
			}
			settled = 0
			if time.Since(start) > 15*time.Second {
				t.Fatal(what)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	eventually(true, "the listed device does not reach the panel")

	// Recording this node's end hangs; the device is removed meanwhile.
	agent.hangTunnel.Store(true)
	for agent.hanging.Load() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	agent.mu.Lock()
	agent.devices = nil
	agent.mu.Unlock()
	eventually(false, "a device removed while the agent hung still reaches the panel")

	// Listed again, it is let in again, though the agent still hangs there.
	agent.mu.Lock()
	agent.devices = laptop
	agent.mu.Unlock()
	connect()
	eventually(true, "the device listed again does not reach the panel")

	// Reading the devices hangs: not knowing them, the panel lets none in.
	agent.hangDevices.Store(true)
	eventually(false, "a device reaches the panel while the devices cannot be read")
	agent.hangDevices.Store(false)
	connect()
	eventually(true, "the device does not reach the panel once the devices are read again")
}
