package domains

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"log/slog"
	"math/big"
	"net/netip"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/metrics"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

// fakeKey seals by saying what it sealed.
type fakeKey struct{}

func (fakeKey) Seal(context, plaintext string) (string, error) {
	return "sealed[" + context + "]" + plaintext, nil
}

func (fakeKey) Open(context, sealed string) (string, error) {
	plain, ok := strings.CutPrefix(sealed, "sealed["+context+"]")
	if !ok {
		return "", errors.New("does not open")
	}
	return plain, nil
}

func newStore(t *testing.T, tenants ...string) *store.SQLite {
	t.Helper()
	path := filepath.Join(t.TempDir(), "murder.db")
	if err := store.Init(path, "n1"); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(path, store.Options{NetworkPool: netip.MustParsePrefix("10.240.0.0/16"), Sealer: func(string, string, spec.Secret) (string, error) { return "x", nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	app := spec.Tenant{Image: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", HealthPath: "/health", User: "1000:1000"}
	if _, err := st.PutApp(store.App{Name: "web", Spec: app}, "test"); err != nil {
		t.Fatal(err)
	}
	for _, name := range tenants {
		if _, err := st.Create(store.NewTenant{Name: name, App: "web"}, "test"); err != nil {
			t.Fatal(err)
		}
	}
	return st
}

// leafFor is a certificate for name, valid from issued for lifetime, with
// its key, PEM.
func leafFor(t *testing.T, name string, issued time.Time, lifetime time.Duration) (string, string, *x509.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name}, DNSNames: []string{name},
		NotBefore: issued, NotAfter: issued.Add(lifetime), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, _ := x509.ParseCertificate(der)
	keyDER, _ := x509.MarshalPKCS8PrivateKey(key)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})), leaf
}

// fixture is a manager over a store, a clock it sets, DNS it sets and a
// CA that issues, or refuses, as it is told.
type fixture struct {
	t       *testing.T
	st      *store.SQLite
	m       *Manager
	now     time.Time
	dns     fakeResolver
	refuse  error
	ordered []string
}

func newFixture(t *testing.T) *fixture {
	// Past the store's own clock, which domains are added by.
	f := &fixture{t: t, st: newStore(t, "acme"), now: time.Now().UTC().Add(time.Minute).Truncate(time.Second)}
	f.dns = fakeResolver{cnames: map[string]string{}, addresses: map[string][]string{"acme.murder.example.net": {"203.0.113.10"}}}
	f.m = &Manager{
		Store: f.st, Key: fakeKey{}, BaseDomain: "murder.example.net", CA: "https://ca.example/directory",
		Resolver: resolverFunc(func() Resolver { return f.dns }),
		Public:   func() []netip.Addr { return []netip.Addr{netip.MustParseAddr("203.0.113.10")} },
		Log:      slog.New(slog.DiscardHandler), Now: func() time.Time { return f.now },
	}
	f.m.obtain = func(_ context.Context, name string) (string, string, *x509.Certificate, error) {
		f.ordered = append(f.ordered, name)
		if f.refuse != nil {
			return "", "", nil, f.refuse
		}
		chain, key, leaf := leafFor(t, name, f.now, 90*24*time.Hour)
		return chain, key, leaf, nil
	}
	return f
}

// resolverFunc reads the resolver anew at each lookup, as a test changes it.
type resolverFunc func() Resolver

func (r resolverFunc) LookupCNAME(ctx context.Context, host string) (string, error) {
	return r().LookupCNAME(ctx, host)
}

func (r resolverFunc) LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error) {
	return r().LookupNetIP(ctx, network, host)
}

func (f *fixture) domain(name string) store.Domain {
	f.t.Helper()
	d, err := f.st.Domain(name)
	if err != nil {
		f.t.Fatal(err)
	}
	return d
}

func (f *fixture) actions(n int) []string {
	f.t.Helper()
	events, err := f.st.Events(store.EventQuery{Limit: n})
	if err != nil {
		f.t.Fatal(err)
	}
	var out []string
	for _, e := range events {
		out = append(out, e.Action)
	}
	return out
}

// A domain waits for its DNS, checked less and less often; once it points
// at the cluster it is routed and gets its certificate, sealed.
func TestADomainWaitsForItsDNSThenGetsItsCertificate(t *testing.T) {
	f := newFixture(t)
	if _, _, err := f.st.AddDomain("acme", "app.customer.com", "alice"); err != nil {
		t.Fatal(err)
	}
	f.m.Step(t.Context())
	d := f.domain("app.customer.com")
	if d.State != store.DomainPending || d.Attempts != 1 || !d.NextAt.Equal(f.now.Add(time.Minute)) || d.Verified() || d.Detail != "app.customer.com has no DNS records yet" {
		t.Fatalf("first check: %+v", d)
	}
	if got := f.actions(1); got[0] != "domain.pending" {
		t.Fatalf("events %v", got)
	}
	// Not due yet: nothing.
	version := f.st.Domains()
	f.m.Step(t.Context())
	if got := f.st.Domains(); got[0] != version[0] {
		t.Fatal("a domain not due was looked at")
	}
	// Due, the same: checked again later, said once.
	f.now = f.now.Add(time.Minute)
	f.m.Step(t.Context())
	d = f.domain("app.customer.com")
	if d.Attempts != 2 || !d.NextAt.Equal(f.now.Add(2*time.Minute)) {
		t.Fatalf("second check: %+v", d)
	}
	if got := f.actions(2); got[0] != "domain.pending" || got[1] == "domain.pending" {
		t.Fatalf("the same answer was said twice: %v", got)
	}
	// Pointed elsewhere, then at the cluster.
	f.dns.addresses["app.customer.com"] = []string{"192.0.2.80"}
	f.now = f.now.Add(2 * time.Minute)
	f.m.Step(t.Context())
	if d = f.domain("app.customer.com"); d.Detail != "it points at 192.0.2.80, which no node of the cluster has" || d.Attempts != 3 {
		t.Fatalf("pointing elsewhere: %+v", d)
	}
	delete(f.dns.addresses, "app.customer.com")
	f.dns.cnames["app.customer.com"] = "acme.murder.example.net"
	if err := f.st.CheckDomainNow("acme", "app.customer.com", "alice"); err != nil {
		t.Fatal(err)
	}
	f.m.Step(t.Context())
	d = f.domain("app.customer.com")
	if d.State != store.DomainActive || d.Attempts != 0 || !d.Verified() || d.Certificate == "" || !strings.HasPrefix(d.Key, "sealed["+store.DomainKeyContext("app.customer.com")+"]-----BEGIN PRIVATE KEY") {
		t.Fatalf("issued: %+v", d)
	}
	if want := f.now.Add(60 * 24 * time.Hour); !d.NextAt.Equal(want) || !d.NotAfter.Equal(f.now.Add(90*24*time.Hour)) {
		t.Fatalf("renewal at %v, not %v; expires %v", d.NextAt, want, d.NotAfter)
	}
	if got := f.actions(2); got[0] != "domain.issued" || got[1] != "domain.verified" {
		t.Fatalf("events %v", got)
	}
	// Due for nothing until its renewal.
	f.now = f.now.Add(30 * 24 * time.Hour)
	f.m.Step(t.Context())
	if len(f.ordered) != 1 {
		t.Fatalf("ordered %v", f.ordered)
	}
}

// A certificate is renewed 30 days before it expires; a renewal that fails
// keeps the certificate served and is tried again, less and less often.
func TestRenewalsKeepTheCertificateUntilTheyWork(t *testing.T) {
	f := newFixture(t)
	f.dns.cnames["app.customer.com"] = "acme.murder.example.net"
	if _, _, err := f.st.AddDomain("acme", "app.customer.com", "alice"); err != nil {
		t.Fatal(err)
	}
	f.m.Step(t.Context())
	issued := f.domain("app.customer.com")
	f.now = issued.NextAt
	f.refuse = errors.New("urn:ietf:params:acme:error:rateLimited: too many certificates")
	f.m.Step(t.Context())
	d := f.domain("app.customer.com")
	if d.State != store.DomainRenewing || d.Attempts != 1 || d.Certificate != issued.Certificate || d.Key != issued.Key || !d.NextAt.Equal(f.now.Add(5*time.Minute)) || !strings.Contains(d.Detail, "rateLimited") {
		t.Fatalf("a failed renewal: %+v", d)
	}
	if got := f.actions(1); got[0] != "domain.failed" {
		t.Fatalf("events %v", got)
	}
	f.now = d.NextAt
	f.m.Step(t.Context())
	if d = f.domain("app.customer.com"); d.Attempts != 2 || !d.NextAt.Equal(f.now.Add(10*time.Minute)) {
		t.Fatalf("a second failed renewal: %+v", d)
	}
	// The DNS moved away: no order, the certificate stays.
	delete(f.dns.cnames, "app.customer.com")
	f.now = d.NextAt
	orders := len(f.ordered)
	f.m.Step(t.Context())
	if d = f.domain("app.customer.com"); d.State != store.DomainRenewing || !strings.HasPrefix(d.Detail, "renewal waits: ") || len(f.ordered) != orders || d.Certificate != issued.Certificate {
		t.Fatalf("a renewal while the DNS points elsewhere: %+v", d)
	}
	f.dns.cnames["app.customer.com"] = "acme.murder.example.net"
	f.refuse = nil
	f.now = d.NextAt
	f.m.Step(t.Context())
	if d = f.domain("app.customer.com"); d.State != store.DomainActive || d.Attempts != 0 || d.Certificate == issued.Certificate || !d.NotAfter.After(issued.NotAfter) {
		t.Fatalf("renewed: %+v", d)
	}
	if got := f.actions(1); got[0] != "domain.renewed" {
		t.Fatalf("events %v", got)
	}
}

// A first order that fails leaves the domain failed, routed, tried again
// with backoff.
func TestAFirstOrderThatFailsIsTriedAgain(t *testing.T) {
	f := newFixture(t)
	f.dns.cnames["app.customer.com"] = "acme.murder.example.net"
	f.refuse = errors.New("urn:ietf:params:acme:error:unauthorized: 403 at 203.0.113.10")
	if _, _, err := f.st.AddDomain("acme", "app.customer.com", "alice"); err != nil {
		t.Fatal(err)
	}
	f.m.Step(t.Context())
	d := f.domain("app.customer.com")
	if d.State != store.DomainFailed || d.Attempts != 1 || !d.Verified() || !d.NextAt.Equal(f.now.Add(5*time.Minute)) || d.Certificate != "" {
		t.Fatalf("failed: %+v", d)
	}
	for _, wait := range []time.Duration{10 * time.Minute, 20 * time.Minute, 40 * time.Minute} {
		f.now = d.NextAt
		f.m.Step(t.Context())
		if d = f.domain("app.customer.com"); !d.NextAt.Equal(f.now.Add(wait)) {
			t.Fatalf("after %d failures, next in %v: %+v", d.Attempts, d.NextAt.Sub(f.now), d)
		}
	}
	if got := backoff(30, orderFirst, orderMost); got != orderMost {
		t.Fatalf("backoff at the most: %v", got)
	}
}

// Nothing happens off the leader, while frozen, before every node runs
// this release, for an archived tenant, and nothing is ordered without a
// CA.
func TestTheManagerWaitsItsTurn(t *testing.T) {
	f := newFixture(t)
	f.dns.cnames["app.customer.com"] = "acme.murder.example.net"
	if _, _, err := f.st.AddDomain("acme", "app.customer.com", "alice"); err != nil {
		t.Fatal(err)
	}
	unchanged := func(why string) {
		t.Helper()
		f.m.Step(t.Context())
		if d := f.domain("app.customer.com"); d.State != store.DomainVerifying {
			t.Fatalf("%s: %+v", why, d)
		}
	}
	leads := false
	f.m.Leader = func() bool { return leads }
	unchanged("off the leader")
	leads = true
	asked := 0
	f.m.Ready = func(context.Context) error {
		asked++
		return errors.New("node n2 runs a release of schema version 17, before 18")
	}
	unchanged("before every node runs this release")
	if err := f.st.Freeze("upgrade", "root"); err != nil {
		t.Fatal(err)
	}
	unchanged("frozen")
	if err := f.st.Thaw("root"); err != nil {
		t.Fatal(err)
	}
	if err := f.st.Archive("acme", "alice"); err != nil {
		t.Fatal(err)
	}
	f.m.Ready = nil
	unchanged("archived")
	if _, err := f.st.Restore("acme", "alice"); err != nil {
		t.Fatal(err)
	}
	f.m.CA, f.m.NoCA = "", "no certificate is ordered: edge.tls is off"
	f.m.Step(t.Context())
	if d := f.domain("app.customer.com"); d.State != store.DomainPending || !d.Verified() || d.Detail != f.m.NoCA || len(f.ordered) != 0 {
		t.Fatalf("without a CA: %+v", d)
	}
	ready := 0
	f.m.Ready = func(context.Context) error { ready++; return nil }
	f.m.ready = time.Time{}
	f.m.CA = "https://ca.example/directory"
	if err := f.st.CheckDomainNow("acme", "app.customer.com", "alice"); err != nil {
		t.Fatal(err)
	}
	f.m.Step(t.Context())
	if err := f.st.CheckDomainNow("acme", "app.customer.com", "alice"); err != nil {
		t.Fatal(err)
	}
	f.m.Step(t.Context())
	if ready != 1 || asked == 0 {
		t.Fatalf("every node's release asked %d times", ready)
	}
}

func TestRenewAt(t *testing.T) {
	issued := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	if got := RenewAt(issued, issued.Add(90*24*time.Hour)); !got.Equal(issued.Add(60 * 24 * time.Hour)) {
		t.Errorf("90 days: %v", got)
	}
	if got := RenewAt(issued, issued.Add(6*24*time.Hour)); !got.Equal(issued.Add(4 * 24 * time.Hour)) {
		t.Errorf("6 days: %v", got)
	}
	if got := RenewAt(time.Time{}, issued); !got.Equal(issued.Add(-30 * 24 * time.Hour)) {
		t.Errorf("issued unknown: %v", got)
	}
}

func TestMetrics(t *testing.T) {
	f := newFixture(t)
	f.dns.cnames["app.customer.com"] = "acme.murder.example.net"
	for _, name := range []string{"app.customer.com", "wait.customer.com"} {
		if _, _, err := f.st.AddDomain("acme", name, "alice"); err != nil {
			t.Fatal(err)
		}
	}
	f.m.Step(t.Context())
	var out bytes.Buffer
	f.m.WriteMetrics(metrics.NewWriter(&out))
	text := out.String()
	for _, want := range []string{
		`murder_domain_state{domain="app.customer.com",tenant="acme",state="active"} 1`,
		`murder_domain_state{domain="wait.customer.com",tenant="acme",state="pending"} 1`,
		`murder_domain_certificate_expiry_timestamp_seconds{domain="app.customer.com",tenant="acme"} `,
		`murder_domain_failing{domain="app.customer.com",tenant="acme"} 0`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("no %s in\n%s", want, text)
		}
	}
	if strings.Contains(text, `expiry_timestamp_seconds{domain="wait.customer.com"`) {
		t.Error("an expiry for a domain without a certificate")
	}
	f.m.Leader = func() bool { return false }
	out.Reset()
	f.m.WriteMetrics(metrics.NewWriter(&out))
	if out.Len() != 0 {
		t.Errorf("metrics off the leader: %s", out.String())
	}
}
