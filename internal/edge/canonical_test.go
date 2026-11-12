package edge

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A tenant's hosts other than its canonical one redirect there, path and
// query kept, once the canonical host can serve the request; challenges
// are answered on every host all the same, and the canonical host itself
// is served.
func TestOtherHostsRedirectToTheCanonicalOne(t *testing.T) {
	certificate, key := selfSigned(t, "customer.com")
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "app at "+r.Host) }))
	defer backend.Close()
	route := Route{Backend: strings.TrimPrefix(backend.URL, "http://"), Canonical: "customer.com"}
	domains := map[string]DomainRoute{
		"customer.com":       {Tenant: "acme", Certificate: certificate, Key: sealed("customer.com", key)},
		"www.customer.com":   {Tenant: "acme"},
		"other.customer.com": {Tenant: "other"},
	}
	challenges := map[string]ChallengeRoute{"tok-1": {Domain: "www.customer.com", KeyAuth: "tok-1.thumb"}}
	live := &LiveTable{}
	live.Set(NewTableWith(map[string]Route{"acme": route, "other": {Backend: "127.0.0.1:1"}}, domains, challenges))
	proxy := NewProxy("example.com", live, quietLog())
	plain := httptest.NewServer(proxy.RedirectToHTTPS(func() bool { return true }))
	defer plain.Close()
	secure := httptest.NewTLSServer(proxy)
	defer secure.Close()
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}

	redirected := func(name, url, host, want string) {
		t.Helper()
		response, _ := get(t, noRedirect, url, host)
		if response.StatusCode != http.StatusPermanentRedirect || response.Header.Get("Location") != want {
			t.Fatalf("%s: %s to %q, want %q", name, response.Status, response.Header.Get("Location"), want)
		}
	}
	// Plain HTTP goes to the canonical host over HTTPS in one redirect,
	// for a domain that has no certificate of its own as well.
	redirected("plain, own name", plain.URL+"/a/b?x=1&y=2", "acme.example.com", "https://customer.com/a/b?x=1&y=2")
	redirected("plain, a domain without its certificate", plain.URL+"/login", "Www.Customer.com.", "https://customer.com/login")
	redirected("plain, the canonical host", plain.URL+"/", "customer.com", "https://customer.com/")
	redirected("HTTPS, own name", secure.URL+"/p?q", "acme.example.com", "https://customer.com/p?q")
	response, _ := get(t, noRedirect, secure.URL+"/p", "acme.example.com")
	if response.Header.Get("Strict-Transport-Security") == "" {
		t.Error("a redirect over HTTPS has no HSTS")
	}
	// The canonical host is served, over HTTPS.
	if response, body := get(t, noRedirect, secure.URL+"/", "customer.com"); response.StatusCode != 200 || body != "app at customer.com" {
		t.Fatalf("the canonical host: %s %q", response.Status, body)
	}
	// A CA checking a challenge of a redirected host gets its answer.
	for _, url := range []string{plain.URL, secure.URL} {
		if response, body := get(t, noRedirect, url+"/.well-known/acme-challenge/tok-1", "www.customer.com"); response.StatusCode != 200 || body != "tok-1.thumb" {
			t.Fatalf("the challenge of a redirected host at %s: %s %q", url, response.Status, body)
		}
	}
	// Another tenant's host is no business of this one's.
	if response, _ := get(t, noRedirect, plain.URL+"/", "other.customer.com"); response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("another tenant's domain: %s %s", response.Status, response.Header.Get("Location"))
	}
	// A 308 keeps a form's method and body.
	request, _ := http.NewRequest(http.MethodPost, secure.URL+"/form", strings.NewReader("a=1"))
	request.Host = "acme.example.com"
	if response, err := noRedirect.Do(request); err != nil || response.StatusCode != http.StatusPermanentRedirect {
		t.Fatalf("a POST: %v %v", response, err)
	}
}

// Until the canonical host can serve a request, every host serves its
// tenant: a domain without its certificate takes no HTTPS redirects, one
// the edges do not route takes none at all, nor does another tenant's.
func TestNoRedirectToAHostThatCannotServe(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "app") }))
	defer backend.Close()
	address := strings.TrimPrefix(backend.URL, "http://")
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	for name, canonical := range map[string]string{
		"a domain without its certificate": "new.customer.com",
		"a domain not routed":              "unchecked.customer.com",
		"another tenant's domain":          "theirs.customer.com",
		"another tenant's own name":        "else.example.com",
	} {
		live := &LiveTable{}
		live.Set(NewTableWith(map[string]Route{"acme": {Backend: address, Canonical: canonical}, "else": {Backend: address}},
			map[string]DomainRoute{"new.customer.com": {Tenant: "acme"}, "theirs.customer.com": {Tenant: "else"}}, nil))
		secure := httptest.NewTLSServer(NewProxy("example.com", live, quietLog()))
		response, body := get(t, noRedirect, secure.URL+"/", "acme.example.com")
		secure.Close()
		if response.StatusCode != 200 || body != "app" {
			t.Errorf("%s: %s %s", name, response.Status, response.Header.Get("Location"))
		}
	}
	// Without TLS, a routed domain takes redirects over plain HTTP.
	live := &LiveTable{}
	live.Set(NewTableWith(map[string]Route{"acme": {Backend: address, Canonical: "new.customer.com"}}, map[string]DomainRoute{"new.customer.com": {Tenant: "acme"}}, nil))
	open := httptest.NewServer(NewProxy("example.com", live, quietLog()))
	defer open.Close()
	if response, _ := get(t, noRedirect, open.URL+"/x?y", "acme.example.com"); response.StatusCode != http.StatusPermanentRedirect || response.Header.Get("Location") != "http://new.customer.com/x?y" {
		t.Fatalf("without TLS: %s %s", response.Status, response.Header.Get("Location"))
	}
	// Nor does plain HTTP redirect before HTTPS serves.
	plain := httptest.NewServer(NewProxy("example.com", live, quietLog()).RedirectToHTTPS(func() bool { return false }))
	defer plain.Close()
	if response, _ := get(t, noRedirect, plain.URL+"/", "acme.example.com"); response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("before HTTPS serves: %s %s", response.Status, response.Header.Get("Location"))
	}
}

// A canonical host versions the table.
func TestCanonicalHostsVersionTheTable(t *testing.T) {
	versions := map[string]bool{}
	for _, canonical := range []string{"", "app.customer.com", "acme.example.com"} {
		versions[NewTable(map[string]Route{"acme": {Backend: "10.240.0.2:8080", Canonical: canonical}}).Version] = true
	}
	if len(versions) != 3 {
		t.Fatalf("versions %v", versions)
	}
}

// certificateBetween is a certificate for name valid from notBefore to
// notAfter, PEM, with its key, PEM.
func certificateBetween(t *testing.T, name string, notBefore, notAfter time.Time) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name}, DNSNames: []string{name},
		NotBefore: notBefore, NotAfter: notAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalPKCS8PrivateKey(key)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
}

// A canonical domain whose certificate is missing, expired or not valid
// yet takes no redirects, over plain HTTP or HTTPS: plain HTTP goes to
// HTTPS on the host asked, and HTTPS serves the tenant there, as without
// a canonical host. Once its certificate is valid, both redirect to it.
func TestNoRedirectToACanonicalDomainWithoutAValidCertificate(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "app at "+r.Host) }))
	defer backend.Close()
	address := strings.TrimPrefix(backend.URL, "http://")
	wwwCert, wwwKey := selfSigned(t, "www.customer.com")
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	now := time.Now()
	expired, expiredKey := certificateBetween(t, "customer.com", now.Add(-90*24*time.Hour), now.Add(-time.Minute))
	early, earlyKey := certificateBetween(t, "customer.com", now.Add(time.Hour), now.Add(90*24*time.Hour))
	valid, validKey := certificateBetween(t, "customer.com", now.Add(-time.Hour), now.Add(90*24*time.Hour))
	serve := func(certificate, key string) (plain, secure *httptest.Server) {
		live := &LiveTable{}
		canonical := DomainRoute{Tenant: "acme"}
		if certificate != "" {
			canonical.Certificate, canonical.Key = certificate, sealed("customer.com", key)
		}
		live.Set(NewTableWith(map[string]Route{"acme": {Backend: address, Canonical: "customer.com"}}, map[string]DomainRoute{
			"customer.com":     canonical,
			"www.customer.com": {Tenant: "acme", Certificate: wwwCert, Key: sealed("www.customer.com", wwwKey)},
		}, nil))
		proxy := NewProxy("example.com", live, quietLog())
		return httptest.NewServer(proxy.RedirectToHTTPS(func() bool { return true })), httptest.NewTLSServer(proxy)
	}
	for name, c := range map[string]struct{ certificate, key string }{
		"missing":       {},
		"expired":       {expired, expiredKey},
		"not valid yet": {early, earlyKey},
	} {
		plain, secure := serve(c.certificate, c.key)
		for _, host := range []string{"www.customer.com", "acme.example.com"} {
			response, _ := get(t, noRedirect, plain.URL+"/a?b=1", host)
			if want := "https://" + host + "/a?b=1"; response.StatusCode != http.StatusPermanentRedirect || response.Header.Get("Location") != want {
				t.Errorf("%s certificate, plain HTTP to %s: %s to %q, want %q", name, host, response.Status, response.Header.Get("Location"), want)
			}
			response, body := get(t, noRedirect, secure.URL+"/a?b=1", host)
			if response.StatusCode != 200 || body != "app at "+host {
				t.Errorf("%s certificate, HTTPS to %s: %s %q to %q", name, host, response.Status, body, response.Header.Get("Location"))
			}
		}
		plain.Close()
		secure.Close()
	}
	plain, secure := serve(valid, validKey)
	defer plain.Close()
	defer secure.Close()
	for _, url := range []string{plain.URL, secure.URL} {
		for _, host := range []string{"www.customer.com", "acme.example.com"} {
			if response, _ := get(t, noRedirect, url+"/a?b=1", host); response.StatusCode != http.StatusPermanentRedirect || response.Header.Get("Location") != "https://customer.com/a?b=1" {
				t.Errorf("a valid certificate, %s to %s: %s to %q", url, host, response.Status, response.Header.Get("Location"))
			}
		}
	}
}

// A canonical domain's certificate that expires while the edge runs stops
// the redirects from then on.
func TestRedirectsStopWhenTheCanonicalCertificateExpires(t *testing.T) {
	now := time.Now()
	certificate, key := certificateBetween(t, "customer.com", now.Add(-time.Hour), now.Add(time.Hour))
	live := &LiveTable{}
	live.Set(NewTableWith(map[string]Route{"acme": {Backend: "127.0.0.1:1", Canonical: "customer.com"}},
		map[string]DomainRoute{"customer.com": {Tenant: "acme", Certificate: certificate, Key: sealed("customer.com", key)}}, nil))
	proxy := NewProxy("example.com", live, quietLog())
	if _, ok := proxy.canonicalHost("acme.example.com", "https"); !ok {
		t.Fatal("no redirect while the certificate is valid")
	}
	proxy.now = func() time.Time { return now.Add(2 * time.Hour) }
	if host, ok := proxy.canonicalHost("acme.example.com", "https"); ok {
		t.Fatalf("a redirect to %s after its certificate expired", host)
	}
	// Plain HTTP, with TLS off, needs no certificate.
	if _, ok := proxy.canonicalHost("acme.example.com", "http"); !ok {
		t.Fatal("no redirect over plain HTTP with TLS off")
	}
}
