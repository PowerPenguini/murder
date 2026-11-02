package panel

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/netip"
	"sync"
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
	return append([]store.Device(nil), a.devices...), nil
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
	// Removed from the state, the device reaches nothing.
	agent.mu.Lock()
	agent.devices = nil
	agent.mu.Unlock()
	time.Sleep(300 * time.Millisecond)
	client.Transport.(*http.Transport).CloseIdleConnections()
	client.Timeout = 2 * time.Second
	if _, err := client.Get("https://panel.example.com:8443/"); err == nil {
		t.Fatal("a removed device reached the panel")
	}
}
