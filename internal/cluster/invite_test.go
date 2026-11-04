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
	keys   []ed25519.PublicKey
}

func (f *fakeInvites) Redeem(_ context.Context, secret []byte, node string, key ed25519.PublicKey) (Bundle, error) {
	if !bytes.Equal(secret, f.secret) {
		return Bundle{}, ErrInvite
	}
	if node == "taken" {
		return Bundle{}, errors.New("a node named taken is in the cluster already")
	}
	f.nodes = append(f.nodes, node)
	f.keys = append(f.keys, key)
	return Bundle{Cluster: "acme", Key: []byte("key"), Config: []byte("node: n1\n"), Files: map[string][]byte{"/etc/murder/dns.token": []byte("t")}, TunnelAddress: "10.77.0.2", Members: map[string]string{"n1": "a2V5"}}, nil
}

// serveInvites starts the cluster port of node n1, with an identity key of
// its own, that takes invitations.
func serveInvites(t *testing.T, seed byte, invites Invites) (*listener, *Credentials) {
	t.Helper()
	identity, err := NewIdentity()
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := NewCredentials([][]byte{bytes.Repeat([]byte{seed}, ed25519.SeedSize)}, "n1", identity, time.Now())
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
	invite := Invite{Address: l.inner.Addr().String(), Node: "n1", Key: credentials.identity.Public().(ed25519.PublicKey), Secret: invites.secret}
	parsed, err := ParseInvite(invite.String())
	if err != nil || parsed.Address != invite.Address || parsed.Node != "n1" || !bytes.Equal(parsed.Secret, invite.Secret) || !parsed.Key.Equal(invite.Key) {
		t.Fatalf("parsed %+v, %v from %s", parsed, err, invite)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	identity, err := NewIdentity()
	if err != nil {
		t.Fatal(err)
	}
	public := identity.Public().(ed25519.PublicKey)
	bundle, err := Redeem(ctx, parsed, "n2", public)
	if err != nil || bundle.Cluster != "acme" || string(bundle.Files["/etc/murder/dns.token"]) != "t" || bundle.TunnelAddress != "10.77.0.2" || bundle.Members["n1"] != "a2V5" {
		t.Fatalf("bundle %+v, %v", bundle, err)
	}
	// The host's identity key reaches the node, which admits it.
	if len(invites.nodes) != 1 || invites.nodes[0] != "n2" || !invites.keys[0].Equal(public) {
		t.Fatalf("redeemed for %v, %v", invites.nodes, invites.keys)
	}
	// A wrong secret learns nothing.
	wrong := parsed
	wrong.Secret = bytes.Repeat([]byte{8}, inviteSecretSize)
	if _, err := Redeem(ctx, wrong, "n2", public); err == nil || !strings.Contains(err.Error(), "unknown, used or expired") {
		t.Fatalf("a wrong secret: %v", err)
	}
	if _, err := Redeem(ctx, parsed, "taken", public); err == nil || !strings.Contains(err.Error(), "in the cluster already") {
		t.Fatalf("a taken name: %v", err)
	}
}

// A host trusts only the node its invitation names, by its identity key:
// another node at that address gets no secret, even one with the cluster's
// master key, which would otherwise take the secret and hand the host
// members of its own. A node of another master key is refused too, by the
// joining node that knows its own.
func TestAHostTalksOnlyToTheNodeItsInvitationNames(t *testing.T) {
	_, legitimate := serveInvites(t, 1, &fakeInvites{})
	capture := &fakeInvites{secret: bytes.Repeat([]byte{7}, inviteSecretSize)}
	impostor, _ := serveInvites(t, 1, capture)
	invite := Invite{Address: impostor.inner.Addr().String(), Node: "n1", Key: legitimate.identity.Public().(ed25519.PublicKey), Secret: capture.secret}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	identity, err := NewIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if bundle, err := Redeem(ctx, invite, "n2", identity.Public().(ed25519.PublicKey)); err == nil || !strings.Contains(err.Error(), "not n1, the node that made the invitation") {
		t.Fatalf("an impostor with the master key: %+v, %v", bundle, err)
	}
	if len(capture.nodes) != 0 {
		t.Fatal("the secret reached a node other than the one the invitation names")
	}
	// The right node, but not of the authority the joining node trusts.
	other, err := NewCredentials([][]byte{bytes.Repeat([]byte{3}, ed25519.SeedSize)}, "x", nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	listening, credentials := serveInvites(t, 2, capture)
	invite = Invite{Address: listening.inner.Addr().String(), Node: "n1", Key: credentials.identity.Public().(ed25519.PublicKey), Secret: capture.secret, CA: other.CAKey()}
	if _, err := Redeem(ctx, invite, "n2", identity.Public().(ed25519.PublicKey)); !errors.Is(err, errOtherAuthority) {
		t.Fatalf("a node of another authority: %v", err)
	}
	if len(capture.nodes) != 0 {
		t.Fatal("the secret reached a node of another authority")
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
	good := Invite{Address: "10.0.0.2:7400", Node: "fsn-1", Secret: bytes.Repeat([]byte{1}, inviteSecretSize), Key: make(ed25519.PublicKey, ed25519.PublicKeySize)}.String()
	for _, text := range []string{"", "10.0.0.2:7400", "host:7400/" + strings.SplitN(good, "/", 2)[1], good[:len(good)-3], strings.Replace(good, ".", "", 1), strings.Replace(good, "/fsn-1/", "//", 1)} {
		if _, err := ParseInvite(text); err == nil {
			t.Errorf("accepted %q", text)
		}
	}
	if invite, err := ParseInvite(" " + good + "\n"); err != nil || invite.Node != "fsn-1" {
		t.Fatalf("%+v, %v", invite, err)
	}
	// An invitation of a release before identity keys names no node.
	old := "10.0.0.2:7400/" + strings.Repeat("A", 27) + "." + strings.Repeat("A", 43)
	if _, err := ParseInvite(old); !errors.Is(err, ErrOldInvite) {
		t.Fatalf("an old invitation: %v", err)
	}
}

// The edges' relay takes nodes only, over HTTP/2, and no invitations.
func TestTheRelayStaysForNodesOverHTTP2(t *testing.T) {
	credentials, err := NewCredentials([][]byte{bytes.Repeat([]byte{5}, ed25519.SeedSize)}, "n1", nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	server := credentials.RelayServer()
	// What a handshake gets, one asking for an invitation's protocol too.
	for _, protos := range [][]string{{"h2"}, {protocolJoin}} {
		got, err := server.GetConfigForClient(&tls.ClientHelloInfo{SupportedProtos: protos})
		if err != nil || got.ClientAuth != tls.RequireAndVerifyClientCert || len(got.NextProtos) != 2 || got.NextProtos[0] != "h2" {
			t.Fatalf("relay config for %v: %+v, %v", protos, got, err)
		}
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
