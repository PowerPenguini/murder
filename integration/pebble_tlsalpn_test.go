//go:build docker && pebble

package integration

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/domains"
	"github.com/PowerPenguini/murder/internal/edge"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

// With port 80 out of the CA's reach, a tenant's domain gets its
// certificate over TLS-ALPN-01, which the edge answers on its HTTPS port.
// Pebble checks HTTP-01 at a port nothing listens on, as a firewall that
// drops port 80 leaves it.
func TestPebbleIssuesOverTLSALPNWithPort80Closed(t *testing.T) {
	needPebble(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	httpPort, httpsPort, closed := freePort(t), freePort(t), freePort(t)
	dir := t.TempDir()
	directory, _, dnsPort, insecure := startPebble(t, dir, closed, httpsPort)

	tenant := "it-" + hex.EncodeToString(randomBytes(3))
	repository := "murder-it/" + tenant
	buildTestImage(t, "v1", repository+":v1")
	cleanupTenant(t, tenant)
	n := newNode(t, tenant)
	app := spec.Tenant{Image: repository + ":v1", HealthPath: "/health", User: strconv.Itoa(os.Getuid()) + ":" + strconv.Itoa(os.Getgid()),
		Env: map[string]string{"DATA_DIR": "/data"}, Limits: spec.Limits{MemoryMB: 64, CPUs: 0.5, PIDs: 64}}
	if _, err := n.client.PutApp(ctx, "it-app", app); err != nil {
		t.Fatal(err)
	}
	if _, err := n.client.Create(ctx, api.TenantRequest{Name: tenant, App: "it-app"}); err != nil {
		t.Fatal(err)
	}
	n.waitHealthy(t, n.reconciler, tenant)

	// The edge as "murder edge" runs it; its plain HTTP listener is only
	// the leader's to reach, as on a node whose port 80 is filtered.
	table := &edge.LiveTable{}
	go followRoutes(ctx, n.client, table)
	proxy := edge.NewProxy(baseDomain, table, n.log)
	httpListener, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(httpPort))
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: proxy.RedirectToHTTPS(func() bool { return true })}
	go httpServer.Serve(httpListener)
	defer httpServer.Close()
	certificates, err := edge.NewTLS(ctx, edge.TLSConfig{Mode: edge.TLSInternal, Storage: filepath.Join(dir, "certs")}, baseDomain, n.log)
	if err != nil {
		t.Fatal(err)
	}
	certificates.ServeDomains(edge.NewDomainCertificates(table, func(domain, sealed string) (string, error) {
		return n.key.Open(store.DomainKeyContext(domain), sealed)
	}, n.log))
	var alpnHandshakes atomic.Int64
	config := certificates.Config.Clone()
	getCertificate := config.GetCertificate
	config.GetCertificate = func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
		if len(hello.SupportedProtos) == 1 && hello.SupportedProtos[0] == "acme-tls/1" {
			alpnHandshakes.Add(1)
		}
		return getCertificate(hello)
	}
	httpsListener, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(httpsPort))
	if err != nil {
		t.Fatal(err)
	}
	httpsServer := &http.Server{Handler: proxy}
	go httpsServer.Serve(tls.NewListener(httpsListener, config))
	defer httpsServer.Close()

	manager := &domains.Manager{
		Store: n.store, Key: n.key, BaseDomain: baseDomain, CA: directory, Email: "ops@murder.test", HTTPClient: insecure,
		Resolver: domains.NewResolver("127.0.0.1:" + strconv.Itoa(dnsPort)),
		Public:   func() []netip.Addr { return []netip.Addr{netip.MustParseAddr("127.0.0.1")} },
		Probe:    domains.ProbeEdges(func() []string { return []string{"127.0.0.1:" + strconv.Itoa(httpPort)} }, n.log),
		Log:      n.log,
	}
	const name = "alpn.customer.test"
	if _, err := n.client.AddDomain(ctx, tenant, name); err != nil {
		t.Fatal(err)
	}
	manager.Step(ctx)
	domain, err := n.store.Domain(name)
	if err != nil || domain.State != store.DomainActive || domain.Certificate == "" {
		t.Fatalf("after the pass: %+v %v", domain, err)
	}
	if alpnHandshakes.Load() == 0 {
		t.Fatal("the CA made no acme-tls/1 handshake: the certificate came another way")
	}
	if len(n.store.Challenges()) != 0 {
		t.Fatalf("challenges left behind: %+v", n.store.Challenges())
	}
	waitFor(t, "the edge serves the domain's certificate", func() error {
		conn, err := tls.Dial("tcp", "127.0.0.1:"+strconv.Itoa(httpsPort), &tls.Config{ServerName: name, InsecureSkipVerify: true})
		if err != nil {
			return err
		}
		defer conn.Close()
		if names := conn.ConnectionState().PeerCertificates[0].DNSNames; len(names) != 1 || names[0] != name {
			return fmt.Errorf("served %s", strings.Join(names, ", "))
		}
		return nil
	})
}
