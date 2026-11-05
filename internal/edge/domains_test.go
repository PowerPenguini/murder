package edge

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// selfSigned is a certificate for names, PEM, and its key, PEM.
func selfSigned(t *testing.T, names ...string) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: names[0]}, DNSNames: names,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalPKCS8PrivateKey(key)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
}

// sealed "seals" a key as the tests' opener opens it.
func sealed(domain, key string) string { return "sealed for " + domain + ":" + key }

func opener(domain, value string) (string, error) {
	key, ok := strings.CutPrefix(value, "sealed for "+domain+":")
	if !ok {
		return "", errors.New("does not open")
	}
	return key, nil
}

func get(t *testing.T, client *http.Client, url, host string) (*http.Response, string) {
	t.Helper()
	request, _ := http.NewRequest(http.MethodGet, url, nil)
	request.Host = host
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	return response, string(body)
}

// A tenant's own domain is routed as its name below the base domain is,
// on this node and through another's edge; one not routed is unknown.
func TestTenantsOwnDomainsAreRouted(t *testing.T) {
	seen := make(chan string, 10)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Host + " " + r.Header.Get("X-Forwarded-Host")
		_, _ = io.WriteString(w, "acme")
	}))
	defer backend.Close()
	creds := credentials(t, 7)
	table := func(route Route) *LiveTable {
		live := &LiveTable{}
		live.Set(NewTableWith(map[string]Route{"acme": route, "sleep": {Suspended: true}},
			map[string]DomainRoute{"app.customer.com": {Tenant: "acme"}, "nap.customer.com": {Tenant: "sleep"}, "gone.customer.com": {Tenant: "purged"}}, nil))
		return live
	}
	backProxy := NewProxy("example.com", table(Route{Backend: strings.TrimPrefix(backend.URL, "http://")}), quietLog()).WithRelay(creds.RelayClient())
	back := httptest.NewUnstartedServer(backProxy.Relay())
	back.TLS = creds.RelayServer()
	back.StartTLS()
	defer back.Close()
	front := httptest.NewServer(NewProxy("example.com", table(Route{Relay: strings.TrimPrefix(back.URL, "https://")}), quietLog()).WithRelay(creds.RelayClient()))
	defer front.Close()
	direct := httptest.NewServer(backProxy)
	defer direct.Close()

	for name, url := range map[string]string{"on its node": direct.URL, "through another node": front.URL} {
		response, body := get(t, http.DefaultClient, url+"/x", "App.Customer.com.")
		if response.StatusCode != 200 || body != "acme" {
			t.Fatalf("%s: %s %q", name, response.Status, body)
		}
		if got := <-seen; got != "App.Customer.com. App.Customer.com." {
			t.Fatalf("%s: the app saw %q", name, got)
		}
	}
	if response, _ := get(t, http.DefaultClient, direct.URL, "nap.customer.com"); response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("a suspended tenant's domain: %s", response.Status)
	}
	for _, host := range []string{"else.customer.com", "gone.customer.com", "customer.com"} {
		if response, _ := get(t, http.DefaultClient, direct.URL, host); response.StatusCode != http.StatusNotFound {
			t.Fatalf("%s: %s", host, response.Status)
		}
	}
}

// Every edge answers an ACME challenge for the domain it is of, on plain
// HTTP, ahead of the redirect to HTTPS; a domain with its certificate goes
// to HTTPS, one without waits for it.
func TestChallengesAndRedirectsOfOwnDomains(t *testing.T) {
	certificate, key := selfSigned(t, "app.customer.com")
	live := &LiveTable{}
	live.Set(NewTableWith(map[string]Route{"acme": {Backend: "127.0.0.1:1"}},
		map[string]DomainRoute{"app.customer.com": {Tenant: "acme", Certificate: certificate, Key: sealed("app.customer.com", key)}, "new.customer.com": {Tenant: "acme"}},
		map[string]ChallengeRoute{"tok-1": {Domain: "new.customer.com", KeyAuth: "tok-1.thumb"}}))
	proxy := NewProxy("example.com", live, quietLog())
	plain := httptest.NewServer(proxy.RedirectToHTTPS(func() bool { return true }))
	defer plain.Close()
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	response, body := get(t, noRedirect, plain.URL+"/.well-known/acme-challenge/tok-1", "new.customer.com")
	if response.StatusCode != 200 || body != "tok-1.thumb" || response.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("the challenge: %s %q", response.Status, body)
	}
	// Of another name, or another token: not this challenge.
	if response, _ := get(t, noRedirect, plain.URL+"/.well-known/acme-challenge/tok-1", "app.customer.com"); response.StatusCode != http.StatusPermanentRedirect {
		t.Fatalf("the challenge asked for another domain: %s", response.Status)
	}
	if response, _ := get(t, noRedirect, plain.URL+"/.well-known/acme-challenge/tok-2", "new.customer.com"); response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("another token: %s", response.Status)
	}
	if response, _ := get(t, noRedirect, plain.URL+"/.well-known/acme-challenge/tok-1", "else.customer.com"); response.StatusCode != http.StatusNotFound {
		t.Fatalf("an unknown host: %s", response.Status)
	}
	response, _ = get(t, noRedirect, plain.URL+"/login?next=1", "app.customer.com")
	if response.StatusCode != http.StatusPermanentRedirect || response.Header.Get("Location") != "https://app.customer.com/login?next=1" {
		t.Fatalf("a domain with its certificate: %s %s", response.Status, response.Header.Get("Location"))
	}
	if response, body := get(t, noRedirect, plain.URL+"/", "new.customer.com"); response.StatusCode != http.StatusServiceUnavailable || !strings.Contains(body, "certificate is on its way") {
		t.Fatalf("a domain without one: %s %q", response.Status, body)
	}
	// Without TLS the edge serves plain HTTP, and answers challenges too.
	open := httptest.NewServer(proxy)
	defer open.Close()
	if response, body := get(t, http.DefaultClient, open.URL+"/.well-known/acme-challenge/tok-1", "new.customer.com"); response.StatusCode != 200 || body != "tok-1.thumb" {
		t.Fatalf("the challenge without TLS: %s %q", response.Status, body)
	}
}

// The edge serves a tenant's domain's certificate to a client that asks
// for its name, and the wildcard to every other; a key that does not open
// leaves the wildcard served.
func TestTheCertificateIsChosenByName(t *testing.T) {
	wildcardCert, wildcardKey := selfSigned(t, "*.example.com", "example.com")
	wildcard, err := tls.X509KeyPair([]byte(wildcardCert), []byte(wildcardKey))
	if err != nil {
		t.Fatal(err)
	}
	ownCert, ownKey := selfSigned(t, "app.customer.com")
	otherCert, otherKey := selfSigned(t, "bad.customer.com")
	live := &LiveTable{}
	live.Set(NewTableWith(map[string]Route{"acme": {Backend: "127.0.0.1:1"}}, map[string]DomainRoute{
		"app.customer.com": {Tenant: "acme", Certificate: ownCert, Key: sealed("app.customer.com", ownKey)},
		"bad.customer.com": {Tenant: "acme", Certificate: otherCert, Key: sealed("else", otherKey)},
		"new.customer.com": {Tenant: "acme"},
	}, nil))
	certificates := &Certificates{Config: &tls.Config{GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return &wildcard, nil }}}
	served := NewDomainCertificates(live, opener, quietLog())
	certificates.ServeDomains(served)
	listener, err := tls.Listen("tcp", "127.0.0.1:0", certificates.Config)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() { _ = conn.(*tls.Conn).Handshake(); conn.Close() }()
		}
	}()
	servedFor := func(name string) string {
		t.Helper()
		conn, err := tls.Dial("tcp", listener.Addr().String(), &tls.Config{ServerName: name, InsecureSkipVerify: true})
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		return conn.ConnectionState().PeerCertificates[0].Subject.CommonName
	}
	for name, want := range map[string]string{"app.customer.com": "app.customer.com", "APP.customer.com": "app.customer.com", "acme.example.com": "*.example.com", "new.customer.com": "*.example.com", "bad.customer.com": "*.example.com"} {
		if got := servedFor(name); got != want {
			t.Errorf("for %s: %s, want %s", name, got, want)
		}
	}
	// A new certificate in the routes is served at once.
	renewed, renewedKey := selfSigned(t, "app.customer.com", "renewed.example")
	live.Set(NewTableWith(map[string]Route{"acme": {}}, map[string]DomainRoute{"app.customer.com": {Tenant: "acme", Certificate: renewed, Key: sealed("app.customer.com", renewedKey)}}, nil))
	conn, err := tls.Dial("tcp", listener.Addr().String(), &tls.Config{ServerName: "app.customer.com", InsecureSkipVerify: true})
	if err != nil {
		t.Fatal(err)
	}
	if names := conn.ConnectionState().PeerCertificates[0].DNSNames; len(names) != 2 {
		t.Errorf("the renewed certificate was not served: %v", names)
	}
	conn.Close()
}

// A table with domains and challenges is versioned by them, and saved and
// loaded whole; one without keeps the version it had before domains.
func TestTablesKeepTheirDomains(t *testing.T) {
	routes := map[string]Route{"acme": {Backend: "10.240.0.2:8080"}}
	plain := NewTable(routes)
	withDomain := NewTableWith(routes, map[string]DomainRoute{"app.customer.com": {Tenant: "acme"}}, nil)
	withCertificate := NewTableWith(routes, map[string]DomainRoute{"app.customer.com": {Tenant: "acme", Certificate: "C", Key: "K"}}, nil)
	withChallenge := NewTableWith(routes, map[string]DomainRoute{"app.customer.com": {Tenant: "acme"}}, map[string]ChallengeRoute{"t": {Domain: "app.customer.com", KeyAuth: "t.x"}})
	if NewTableWith(routes, map[string]DomainRoute{}, nil).Version != plain.Version {
		t.Error("an empty set of domains changed the version")
	}
	versions := map[string]bool{}
	for _, table := range []Table{plain, withDomain, withCertificate, withChallenge} {
		versions[table.Version] = true
	}
	if len(versions) != 4 {
		t.Fatalf("versions %v", versions)
	}
	path := t.TempDir() + "/routes.json"
	if err := SaveTable(path, withChallenge); err != nil {
		t.Fatal(err)
	}
	loaded, ok, err := LoadTable(path)
	if err != nil || !ok || loaded.Version != withChallenge.Version || loaded.Domains["app.customer.com"].Tenant != "acme" || loaded.Challenges["t"].KeyAuth != "t.x" {
		t.Fatalf("loaded %+v %v %v", loaded, ok, err)
	}
}

// A connection that asks for no name gets the wildcard.
func TestNoServerNameGetsTheFallback(t *testing.T) {
	served := NewDomainCertificates(&LiveTable{}, opener, quietLog())
	if served.Certificate("") != nil || served.Certificate("app.customer.com") != nil {
		t.Fatal("a certificate from nowhere")
	}
	var nilServed *DomainCertificates
	if nilServed.Certificate("x") != nil {
		t.Fatal("a nil set served something")
	}
}
