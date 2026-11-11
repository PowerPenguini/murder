package edge

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"encoding/asn1"
	"net/http"
	"net/http/httptest"
	"testing"
)

// acmeIdentifier is the extension a TLS-ALPN-01 certificate holds its
// key authorization's digest in (RFC 8737).
var acmeIdentifier = asn1.ObjectIdentifier{1, 3, 6, 1, 5, 5, 7, 1, 31}

// A CA asking for acme-tls/1 gets the certificate of the challenge under
// way for the name it asks, which holds the key authorization's digest;
// no other name gets one, and other clients get the certificates they
// always did, over the protocols they always did.
func TestTLSALPNChallengesAreAnswered(t *testing.T) {
	wildcardCert, wildcardKey := selfSigned(t, "*.example.com", "example.com")
	wildcard, err := tls.X509KeyPair([]byte(wildcardCert), []byte(wildcardKey))
	if err != nil {
		t.Fatal(err)
	}
	ownCert, ownKey := selfSigned(t, "app.customer.com")
	live := &LiveTable{}
	live.Set(NewTableWith(map[string]Route{"acme": {Backend: "127.0.0.1:1"}}, map[string]DomainRoute{
		"app.customer.com": {Tenant: "acme", Certificate: ownCert, Key: sealed("app.customer.com", ownKey)},
		"new.customer.com": {Tenant: "acme"},
	}, map[string]ChallengeRoute{"tok-1": {Domain: "new.customer.com", KeyAuth: "tok-1.thumb"}}))
	certificates := &Certificates{Config: &tls.Config{NextProtos: []string{"h2", "http/1.1"}, GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return &wildcard, nil }}}
	certificates.ServeDomains(NewDomainCertificates(live, opener, quietLog()))
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	server.TLS = certificates.Config
	server.StartTLS()
	defer server.Close()
	address := server.Listener.Addr().String()

	handshake := func(name string, protos ...string) (tls.ConnectionState, error) {
		conn, err := tls.Dial("tcp", address, &tls.Config{ServerName: name, NextProtos: protos, InsecureSkipVerify: true})
		if err != nil {
			return tls.ConnectionState{}, err
		}
		defer conn.Close()
		return conn.ConnectionState(), nil
	}
	state, err := handshake("New.Customer.com.", "acme-tls/1")
	if err != nil {
		t.Fatal(err)
	}
	leaf := state.PeerCertificates[0]
	if state.NegotiatedProtocol != "acme-tls/1" || len(leaf.DNSNames) != 1 || leaf.DNSNames[0] != "new.customer.com" {
		t.Fatalf("the challenge's handshake: %q %v", state.NegotiatedProtocol, leaf.DNSNames)
	}
	digest := sha256.Sum256([]byte("tok-1.thumb"))
	found := false
	for _, extension := range leaf.Extensions {
		var value []byte
		if extension.Id.Equal(acmeIdentifier) && extension.Critical {
			if _, err := asn1.Unmarshal(extension.Value, &value); err == nil && bytes.Equal(value, digest[:]) {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("the certificate holds no acmeIdentifier for the key authorization")
	}
	// The same challenge again: the same certificate.
	if again, _ := handshake("new.customer.com", "acme-tls/1"); !bytes.Equal(again.PeerCertificates[0].Raw, leaf.Raw) {
		t.Error("the challenge's certificate was made anew")
	}
	// A name with no challenge under way gets no handshake, not its real
	// certificate. (A client that names nothing gets httptest's own
	// certificate, which Go serves without asking GetCertificate; a CA
	// always names the domain.)
	for _, name := range []string{"app.customer.com", "acme.example.com"} {
		if _, err := handshake(name, "acme-tls/1"); err == nil {
			t.Errorf("%q: a handshake over acme-tls/1 without a challenge", name)
		}
	}
	// Clients that ask for HTTP get what they always did.
	if state, err := handshake("app.customer.com", "h2", "http/1.1"); err != nil || state.NegotiatedProtocol != "h2" || state.PeerCertificates[0].Subject.CommonName != "app.customer.com" {
		t.Fatalf("a browser: %+v %v", state.NegotiatedProtocol, err)
	}
	if state, err := handshake("new.customer.com"); err != nil || state.PeerCertificates[0].Subject.CommonName != "*.example.com" {
		t.Fatalf("a client without ALPN: %v", err)
	}
	// Once the challenge is over, none is answered.
	live.Set(NewTableWith(map[string]Route{"acme": {Backend: "127.0.0.1:1"}}, map[string]DomainRoute{"new.customer.com": {Tenant: "acme"}}, nil))
	if _, err := handshake("new.customer.com", "acme-tls/1"); err == nil {
		t.Fatal("a challenge answered after it ended")
	}
}
