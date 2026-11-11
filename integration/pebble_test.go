//go:build docker && pebble

package integration

// The Pebble test orders a real certificate for a tenant's domain from
// Pebble, Let's Encrypt's ACME test server, and its DNS from
// pebble-challtestsrv, both in Docker on the host's network. Run with:
//
//	go test -tags 'docker pebble' -count=1 -run Pebble ./integration

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/domains"
	"github.com/PowerPenguini/murder/internal/edge"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

// freePort is a TCP port of the loopback no one listens on now.
func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

// container runs an image on the host's network until the test ends.
func container(t *testing.T, args ...string) {
	t.Helper()
	name := "murder-pebble-" + hex.EncodeToString(randomBytes(4))
	run := append([]string{"run", "-d", "--rm", "--network", "host", "--name", name}, args...)
	if output, err := exec.Command("docker", run...).CombinedOutput(); err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(run, " "), err, output)
	}
	t.Cleanup(func() {
		if t.Failed() {
			logs, _ := exec.Command("docker", "logs", "--tail", "60", name).CombinedOutput()
			t.Logf("%s:\n%s", name, logs)
		}
		_ = exec.Command("docker", "rm", "-f", name).Run()
	})
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// waitFor retries check until it reports nil or a minute passed.
func waitFor(t *testing.T, what string, check func() error) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	var err error
	for time.Now().Before(deadline) {
		if err = check(); err == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s: %v", what, err)
}

// followRoutes keeps table as the agent's routes, as "murder edge" does.
func followRoutes(ctx context.Context, client *api.Client, table *edge.LiveTable) {
	for ctx.Err() == nil {
		got, err := client.Routes(ctx, table.Load().Version)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		table.Set(got)
	}
}

func TestPebbleIssuesACertificateForATenantsDomain(t *testing.T) {
	needPebble(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	httpPort, httpsPort, tlsPort := freePort(t), freePort(t), freePort(t)

	dir := t.TempDir()
	directory, pebbleAdmin, dnsPort, insecure := startPebble(t, dir, httpPort, tlsPort)

	// A tenant on Docker, as the lifecycle test runs one.
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

	// The edge, as "murder edge" runs it: the agent's routes, plain HTTP
	// that answers challenges and redirects, HTTPS with the wildcard of
	// the internal CA and the tenants' domains' certificates by name.
	table := &edge.LiveTable{}
	go followRoutes(ctx, n.client, table)
	proxy := edge.NewProxy(baseDomain, table, n.log)
	var challengesAnswered atomic.Int64
	plain := proxy.RedirectToHTTPS(func() bool { return true })
	httpServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/.well-known/acme-challenge/") {
			challengesAnswered.Add(1)
		}
		plain.ServeHTTP(w, r)
	})}
	httpListener, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(httpPort))
	if err != nil {
		t.Fatal(err)
	}
	go httpServer.Serve(httpListener)
	defer httpServer.Close()
	certificates, err := edge.NewTLS(ctx, edge.TLSConfig{Mode: edge.TLSInternal, Storage: filepath.Join(dir, "certs")}, baseDomain, n.log)
	if err != nil {
		t.Fatal(err)
	}
	certificates.ServeDomains(edge.NewDomainCertificates(table, func(domain, sealed string) (string, error) {
		return n.key.Open(store.DomainKeyContext(domain), sealed)
	}, n.log))
	httpsListener, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(httpsPort))
	if err != nil {
		t.Fatal(err)
	}
	httpsServer := &http.Server{Handler: proxy}
	go httpsServer.Serve(tls.NewListener(httpsListener, certificates.Config))
	defer httpsServer.Close()

	// The leader's manager, with Pebble as its CA and challtestsrv as DNS.
	var offset atomic.Int64
	manager := func() *domains.Manager {
		return &domains.Manager{
			Store: n.store, Key: n.key, BaseDomain: baseDomain, CA: directory, Email: "ops@murder.test", HTTPClient: insecure,
			Resolver: domains.NewResolver("127.0.0.1:" + strconv.Itoa(dnsPort)),
			Public:   func() []netip.Addr { return []netip.Addr{netip.MustParseAddr("127.0.0.1")} },
			Probe:    domains.ProbeEdges(func() []string { return []string{"127.0.0.1:" + strconv.Itoa(httpPort)} }, n.log),
			Log:      n.log, Now: func() time.Time { return time.Now().Add(time.Duration(offset.Load())) },
		}
	}
	first := manager()

	const name = "shop.customer.test"
	added, err := n.client.AddDomain(ctx, tenant, "Shop.Customer.Test.")
	if err != nil || added.Domain.Name != name || added.Domain.State != store.DomainVerifying {
		t.Fatalf("add: %+v %v", added, err)
	}
	// Not routed before its DNS was checked.
	noRedirect := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	if status := statusOf(t, noRedirect, "http://127.0.0.1:"+strconv.Itoa(httpPort)+"/", name); status != http.StatusNotFound {
		t.Fatalf("a domain not checked yet answered %d", status)
	}

	first.Step(ctx)
	domain, err := n.store.Domain(name)
	if err != nil || domain.State != store.DomainActive || !domain.Verified() || domain.Certificate == "" || domain.NotAfter.Before(time.Now().Add(80*24*time.Hour)) {
		t.Fatalf("after the first pass: %+v %v", domain, err)
	}
	if challengesAnswered.Load() == 0 {
		t.Fatal("the edge answered no challenge: the certificate came without HTTP-01")
	}
	if strings.Contains(domain.Key, "PRIVATE KEY") {
		t.Fatal("the certificate's key is stored in the clear")
	}
	if len(n.store.Challenges()) != 0 {
		t.Fatalf("challenges left behind: %+v", n.store.Challenges())
	}
	account, ok := n.store.ACMEAccount(directory)
	if !ok || account.Location == "" {
		t.Fatal("no ACME account kept")
	}
	events, _ := n.client.Events(ctx, store.EventQuery{Tenant: tenant, Limit: 3})
	if events[0].Action != "domain.issued" || events[1].Action != "domain.verified" || events[2].Action != "domain.added" {
		t.Fatalf("events %+v", events)
	}

	// Pebble's root, which the certificate chains to.
	roots := x509.NewCertPool()
	response, err := insecure.Get(fmt.Sprintf("https://127.0.0.1:%d/roots/0", pebbleAdmin))
	if err != nil {
		t.Fatal(err)
	}
	rootPEM, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if !roots.AppendCertsFromPEM(rootPEM) {
		t.Fatalf("Pebble's root: %s", rootPEM)
	}
	served := func() *x509.Certificate {
		t.Helper()
		var leaf *x509.Certificate
		waitFor(t, "the edge serves the domain's certificate", func() error {
			conn, err := tls.Dial("tcp", "127.0.0.1:"+strconv.Itoa(httpsPort), &tls.Config{ServerName: name, RootCAs: roots})
			if err != nil {
				return err
			}
			defer conn.Close()
			leaf = conn.ConnectionState().PeerCertificates[0]
			return nil
		})
		return leaf
	}
	leaf := served()
	if leaf.Subject.CommonName != name && (len(leaf.DNSNames) != 1 || leaf.DNSNames[0] != name) {
		t.Fatalf("served %v", leaf.DNSNames)
	}
	// The tenant's name below the base domain keeps the wildcard.
	conn, err := tls.Dial("tcp", "127.0.0.1:"+strconv.Itoa(httpsPort), &tls.Config{ServerName: tenant + "." + baseDomain, InsecureSkipVerify: true})
	if err != nil {
		t.Fatal(err)
	}
	if names := conn.ConnectionState().PeerCertificates[0].DNSNames; len(names) == 0 || names[0] != "*."+baseDomain {
		t.Fatalf("the tenant's own name got %v", names)
	}
	conn.Close()

	// The request reaches the tenant, over HTTPS by its own name; plain
	// HTTP goes there.
	secure := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: roots},
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, "127.0.0.1:"+strconv.Itoa(httpsPort))
		},
	}}
	response, err = secure.Get("https://" + name + "/info")
	if err != nil {
		t.Fatal(err)
	}
	var info struct{ Host, Proto string }
	if err := json.NewDecoder(response.Body).Decode(&info); err != nil || response.StatusCode != 200 || info.Host != name || info.Proto != "https" || response.Header.Get("Strict-Transport-Security") == "" {
		t.Fatalf("the tenant over HTTPS: %s %+v %v", response.Status, info, err)
	}
	response.Body.Close()
	request, _ := http.NewRequest(http.MethodGet, "http://127.0.0.1:"+strconv.Itoa(httpPort)+"/info?x=1", nil)
	request.Host = name
	if response, err = noRedirect.Do(request); err != nil || response.StatusCode != http.StatusPermanentRedirect || response.Header.Get("Location") != "https://"+name+"/info?x=1" {
		t.Fatalf("plain HTTP: %v %v", response, err)
	}
	response.Body.Close()

	// Made canonical, the tenant's own name redirects to it, over plain
	// HTTP and HTTPS alike, and the domain itself is served.
	if err := n.client.SetCanonical(ctx, tenant, name); err != nil {
		t.Fatal(err)
	}
	insecureNoRedirect := &http.Client{Timeout: 10 * time.Second, CheckRedirect: noRedirect.CheckRedirect, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	waitFor(t, "the tenant's own name redirects to its canonical host", func() error {
		for _, url := range []string{"http://127.0.0.1:" + strconv.Itoa(httpPort), "https://127.0.0.1:" + strconv.Itoa(httpsPort)} {
			request, _ := http.NewRequest(http.MethodGet, url+"/info?x=1", nil)
			request.Host = tenant + "." + baseDomain
			response, err := insecureNoRedirect.Do(request)
			if err != nil {
				return err
			}
			response.Body.Close()
			if response.StatusCode != http.StatusPermanentRedirect || response.Header.Get("Location") != "https://"+name+"/info?x=1" {
				return fmt.Errorf("%s: %s to %q", url, response.Status, response.Header.Get("Location"))
			}
		}
		return nil
	})
	if response, err = secure.Get("https://" + name + "/info"); err != nil || response.StatusCode != 200 {
		t.Fatalf("the canonical host: %v %v", response, err)
	}
	response.Body.Close()

	// A new leader renews it 30 days before it expires, with the
	// cluster's account, which it reads from the state: the one before
	// stopped leading.
	first.Leader = func() bool { return false }
	second := manager()
	offset.Store(int64(61 * 24 * time.Hour))
	second.Step(ctx)
	renewed, err := n.store.Domain(name)
	if err != nil || renewed.State != store.DomainActive || renewed.Certificate == domain.Certificate || renewed.Key == domain.Key {
		t.Fatalf("after the renewal: %+v %v", renewed, err)
	}
	if again, _ := n.store.ACMEAccount(directory); again.Location != account.Location {
		t.Fatalf("the new leader made another account: %s, not %s", again.Location, account.Location)
	}
	if events, _ := n.client.Events(ctx, store.EventQuery{Tenant: tenant, Limit: 1}); events[0].Action != "domain.renewed" {
		t.Fatalf("events %+v", events)
	}
	waitFor(t, "the edge serves the renewed certificate", func() error {
		if now := served(); now.SerialNumber.Cmp(leaf.SerialNumber) == 0 {
			return fmt.Errorf("still serial %v", now.SerialNumber)
		}
		return nil
	})

	// Removed, it is served no more.
	if err := n.client.RemoveDomain(ctx, tenant, name); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the edge forgets the domain", func() error {
		if status := statusOf(t, noRedirect, "http://127.0.0.1:"+strconv.Itoa(httpPort)+"/", name); status != http.StatusNotFound {
			return fmt.Errorf("answered %d", status)
		}
		return nil
	})

	// The leader revokes its certificate with the CA, which says so.
	second.Step(ctx)
	if left := n.store.Revocations(); len(left) != 0 {
		t.Fatalf("not revoked: %+v", left)
	}
	if events, _ := n.client.Events(ctx, store.EventQuery{Tenant: tenant, Limit: 1}); events[0].Action != "domain.revoked" {
		t.Fatalf("events %+v", events)
	}
	block, _ := pem.Decode([]byte(renewed.Certificate))
	revokedLeaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	response, err = insecure.Get(fmt.Sprintf("https://127.0.0.1:%d/cert-status-by-serial/%x", pebbleAdmin, revokedLeaf.SerialNumber))
	if err != nil {
		t.Fatal(err)
	}
	status, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if !strings.Contains(string(status), "Revoked") {
		t.Fatalf("Pebble has the certificate as %s", status)
	}
}

// needPebble skips a test without Docker or Pebble's images.
func needPebble(t *testing.T) {
	t.Helper()
	if _, err := os.Stat(dockerSocket()); err != nil {
		t.Skipf("no Docker socket: %v", err)
	}
	for _, image := range []string{"ghcr.io/letsencrypt/pebble:latest", "ghcr.io/letsencrypt/pebble-challtestsrv:latest"} {
		if output, err := exec.Command("docker", "image", "inspect", image).CombinedOutput(); err != nil {
			if output, err = exec.Command("docker", "pull", "-q", image).CombinedOutput(); err != nil {
				t.Skipf("no %s: %v\n%s", image, err, output)
			}
		}
	}
}

// startPebble runs pebble-challtestsrv, whose DNS leads every name to the
// loopback, and Pebble, which checks HTTP-01 challenges at httpPort and
// TLS-ALPN-01 ones at tlsPort of the loopback. It returns Pebble's
// directory, its management port, the DNS server's port and a client for
// Pebble's own API, which speaks HTTPS with a test certificate.
func startPebble(t *testing.T, dir string, httpPort, tlsPort int) (string, int, int, *http.Client) {
	t.Helper()
	acmePort, pebbleAdmin, dnsPort, dnsAdmin := freePort(t), freePort(t), freePort(t), freePort(t)
	// DNS: every name leads to the loopback, where the edge listens; no
	// IPv6, where it does not.
	container(t, "ghcr.io/letsencrypt/pebble-challtestsrv:latest",
		"-dnsserver", "127.0.0.1:"+strconv.Itoa(dnsPort), "-management", "127.0.0.1:"+strconv.Itoa(dnsAdmin),
		"-http01", "", "-https01", "", "-tlsalpn01", "", "-doh", "", "-defaultIPv4", "127.0.0.1", "-defaultIPv6", "")
	config := map[string]any{"pebble": map[string]any{
		"listenAddress": "127.0.0.1:" + strconv.Itoa(acmePort), "managementListenAddress": "127.0.0.1:" + strconv.Itoa(pebbleAdmin),
		"certificate": "test/certs/localhost/cert.pem", "privateKey": "test/certs/localhost/key.pem",
		"httpPort": httpPort, "tlsPort": tlsPort, "ocspResponderURL": "", "externalAccountBindingRequired": false,
		"retryAfter": map[string]int{"authz": 1, "order": 1}, "keyAlgorithm": "ecdsa",
		"profiles": map[string]any{"default": map[string]any{"description": "90 days", "validityPeriod": 7776000}},
	}}
	data, _ := json.Marshal(config)
	if err := os.WriteFile(filepath.Join(dir, "pebble.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	container(t, "-v", dir+":/murder:ro", "-e", "PEBBLE_VA_NOSLEEP=1", "-e", "PEBBLE_WFE_NONCEREJECT=0", "-e", "PEBBLE_AUTHZREUSE=0",
		"ghcr.io/letsencrypt/pebble:latest", "-config", "/murder/pebble.json", "-dnsserver", "127.0.0.1:"+strconv.Itoa(dnsPort))
	// Pebble's own API speaks HTTPS with a test certificate.
	insecure := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	directory := fmt.Sprintf("https://127.0.0.1:%d/dir", acmePort)
	waitFor(t, "Pebble answers", func() error {
		response, err := insecure.Get(directory)
		if err == nil {
			response.Body.Close()
		}
		return err
	})

	return directory, pebbleAdmin, dnsPort, insecure
}

func statusOf(t *testing.T, client *http.Client, url, host string) int {
	t.Helper()
	request, _ := http.NewRequest(http.MethodGet, url, nil)
	request.Host = host
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	return response.StatusCode
}
