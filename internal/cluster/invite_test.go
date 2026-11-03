package cluster

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"
)

type fakeInvites struct {
	secret []byte
	nodes  []string
}

func (f *fakeInvites) Redeem(_ context.Context, secret []byte, node string) (Bundle, error) {
	if !bytes.Equal(secret, f.secret) {
		return Bundle{}, ErrInvite
	}
	if node == "taken" {
		return Bundle{}, errors.New("a node named taken is in the cluster already")
	}
	f.nodes = append(f.nodes, node)
	return Bundle{Cluster: "acme", Key: []byte("key"), Config: []byte("node: n1\n"), Files: map[string][]byte{"/etc/murder/dns.token": []byte("t")}, TunnelAddress: "10.77.0.2"}, nil
}

// serveInvites starts a node's cluster port that takes invitations.
func serveInvites(t *testing.T, seed byte, invites Invites) (*listener, *Credentials) {
	t.Helper()
	credentials, err := NewCredentials(bytes.Repeat([]byte{seed}, ed25519.SeedSize), "n1", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	l, err := listen("127.0.0.1:0", credentials, func() string { return "acme" }, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: joinHandler(invites)}
	go server.Serve(l.joinListener())
	t.Cleanup(func() { _ = server.Close(); _ = l.close() })
	return l, credentials
}

func TestAHostJoinsWithAnInvitation(t *testing.T) {
	invites := &fakeInvites{secret: bytes.Repeat([]byte{7}, inviteSecretSize)}
	l, credentials := serveInvites(t, 1, invites)
	invite := Invite{Address: l.inner.Addr().String(), Secret: invites.secret, CA: credentials.CAKey()}
	parsed, err := ParseInvite(invite.String())
	if err != nil || parsed.Address != invite.Address || !bytes.Equal(parsed.Secret, invite.Secret) || !parsed.CA.Equal(invite.CA) {
		t.Fatalf("parsed %+v, %v from %s", parsed, err, invite)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	bundle, err := Join(ctx, parsed, "n2")
	if err != nil || bundle.Cluster != "acme" || string(bundle.Files["/etc/murder/dns.token"]) != "t" || bundle.TunnelAddress != "10.77.0.2" {
		t.Fatalf("bundle %+v, %v", bundle, err)
	}
	if len(invites.nodes) != 1 || invites.nodes[0] != "n2" {
		t.Fatalf("redeemed for %v", invites.nodes)
	}
	// A wrong secret learns nothing.
	wrong := parsed
	wrong.Secret = bytes.Repeat([]byte{8}, inviteSecretSize)
	if _, err := Join(ctx, wrong, "n2"); err == nil || !strings.Contains(err.Error(), "unknown, used or expired") {
		t.Fatalf("a wrong secret: %v", err)
	}
	if _, err := Join(ctx, parsed, "taken"); err == nil || !strings.Contains(err.Error(), "in the cluster already") {
		t.Fatalf("a taken name: %v", err)
	}
}

// A host trusts only the cluster its invitation names: another one, with
// another master key, at that address gets no secret.
func TestAHostTalksOnlyToTheClusterItsInvitationNames(t *testing.T) {
	invites := &fakeInvites{secret: bytes.Repeat([]byte{7}, inviteSecretSize)}
	l, _ := serveInvites(t, 2, invites)
	other, err := NewCredentials(bytes.Repeat([]byte{3}, ed25519.SeedSize), "x", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	invite := Invite{Address: l.inner.Addr().String(), Secret: invites.secret, CA: other.CAKey()}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := Join(ctx, invite, "n2"); err == nil || !strings.Contains(err.Error(), "not the cluster's") {
		t.Fatalf("an impostor: %v", err)
	}
	if len(invites.nodes) != 0 {
		t.Fatal("the secret reached a node of another cluster")
	}
}

// Without a certificate, a host speaks only the invitation's protocol.
func TestTheClusterPortStillWantsANodesCertificate(t *testing.T) {
	l, _ := serveInvites(t, 4, &fakeInvites{})
	for _, protocol := range []string{protocolHTTP, protocolRaft} {
		conn, err := tls.Dial("tcp", l.inner.Addr().String(), &tls.Config{InsecureSkipVerify: true, NextProtos: []string{protocolJoin, protocol}, MinVersion: tls.VersionTLS13})
		if err == nil {
			// TLS 1.3 reports a missing client certificate on the first read.
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			_, err = conn.Read(make([]byte, 1))
			conn.Close()
		}
		if err == nil {
			t.Fatalf("a host without a certificate got %s", protocol)
		}
	}
}

func TestParseInviteRefusesDamagedOnes(t *testing.T) {
	good := Invite{Address: "10.0.0.2:7400", Secret: bytes.Repeat([]byte{1}, inviteSecretSize), CA: make(ed25519.PublicKey, ed25519.PublicKeySize)}.String()
	for _, text := range []string{"", "10.0.0.2:7400", "host:7400/" + strings.SplitN(good, "/", 2)[1], good[:len(good)-3], strings.Replace(good, ".", "", 1)} {
		if _, err := ParseInvite(text); err == nil {
			t.Errorf("accepted %q", text)
		}
	}
	if _, err := ParseInvite(" " + good + "\n"); err != nil {
		t.Fatal(err)
	}
}

// The edges' relay takes nodes only, over HTTP/2, and no invitations.
func TestTheRelayStaysForNodesOverHTTP2(t *testing.T) {
	credentials, err := NewCredentials(bytes.Repeat([]byte{5}, ed25519.SeedSize), "n1", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	server := credentials.RelayServer()
	if server.GetConfigForClient != nil || server.ClientAuth != tls.RequireAndVerifyClientCert || len(server.NextProtos) != 2 || server.NextProtos[0] != "h2" {
		t.Fatalf("relay config %+v", server)
	}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", server)
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
			_ = conn.(*tls.Conn).Handshake()
			time.Sleep(100 * time.Millisecond)
			conn.Close()
		}
	}()
	client := credentials.RelayClient()
	client.NextProtos = []string{"h2", protocolHTTP}
	conn, err := tls.Dial("tcp", listener.Addr().String(), client)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if got := conn.ConnectionState().NegotiatedProtocol; got != "h2" {
		t.Fatalf("negotiated %q", got)
	}
}

// Hosts without a certificate hold few connections, and not for long.
func TestInvitedConnectionsAreFew(t *testing.T) {
	l, _ := serveInvites(t, 6, &fakeInvites{})
	var held []*tls.Conn
	defer func() {
		for _, conn := range held {
			conn.Close()
		}
	}()
	refused := 0
	for range joins + 4 {
		conn, err := tls.Dial("tcp", l.inner.Addr().String(), &tls.Config{InsecureSkipVerify: true, NextProtos: []string{protocolJoin}, MinVersion: tls.VersionTLS13})
		if err != nil {
			refused++
			continue
		}
		held = append(held, conn)
	}
	// Past the limit, the node closes the connection: a read ends at once.
	closed := 0
	for _, conn := range held {
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		if _, err := conn.Read(make([]byte, 1)); err != nil && !isTimeout(err) {
			closed++
		}
	}
	if refused+closed < 4 {
		t.Fatalf("%d of %d connections refused or closed, want at least 4", refused+closed, joins+4)
	}
}

func isTimeout(err error) bool {
	var timeout interface{ Timeout() bool }
	return errors.As(err, &timeout) && timeout.Timeout()
}
