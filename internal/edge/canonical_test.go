package edge

import (
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
