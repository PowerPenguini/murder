package invite

import (
	"context"
	"crypto/ed25519"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/store"
)

type fakeStore struct {
	cluster string
	nodes   []store.Node
	notes   []string
}

func (f *fakeStore) Cluster() string     { return f.cluster }
func (f *fakeStore) Nodes() []store.Node { return f.nodes }
func (f *fakeStore) Note(action, detail, actor string) error {
	f.notes = append(f.notes, action+" "+detail+" "+actor)
	return nil
}

// book is a node's invitations, with its config and files in dir.
func book(t *testing.T, extra string) (*Book, *fakeStore, string) {
	t.Helper()
	dir := t.TempDir()
	write := func(name, data string, mode os.FileMode) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), mode); err != nil {
			t.Fatal(err)
		}
	}
	write("master.key", "a2V5\n", 0o600)
	write("hetzner.token", "hcloud\n", 0o600)
	write("ghcr.token", "ghp\n", 0o600)
	write("agent.yaml", `node: n1
baseDomain: example.com
volumes: {driver: zfs, parent: tank/murder/tenants}
masterKeyFile: `+dir+`/master.key
registries:
  - {host: ghcr.io, username: bot, tokenFile: `+dir+`/ghcr.token}
edge:
  tls: {mode: acme, email: ops@example.com, dnsProvider: hetzner, dnsTokenFile: `+dir+`/hetzner.token}
cluster:
  address: 10.0.0.2:7400
  failover: {hetzner: {tokenFile: `+dir+`/hetzner.token}}
panel:
  tunnel: {address: 10.77.0.1}
`+extra, 0o644)
	st := &fakeStore{cluster: "acme", nodes: []store.Node{{Name: "n1"}}}
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	b := &Book{ConfigPath: filepath.Join(dir, "agent.yaml"), Store: st, Now: func() time.Time { return now },
		Invitation: func(secret []byte) cluster.Invite {
			return cluster.Invite{Address: "10.0.0.2:7400", Secret: secret, CA: make(ed25519.PublicKey, ed25519.PublicKeySize)}
		}}
	return b, st, dir
}

func TestAnInvitationIsTakenOnce(t *testing.T) {
	b, st, dir := book(t, "")
	invite, expires, err := b.Create(time.Hour, "root")
	if err != nil || !expires.Equal(time.Date(2026, 11, 3, 13, 0, 0, 0, time.UTC)) || len(invite.Secret) != 20 {
		t.Fatalf("invite %+v until %v, %v", invite, expires, err)
	}
	bundle, err := b.Redeem(context.Background(), invite.Secret, "n2")
	if err != nil {
		t.Fatal(err)
	}
	if bundle.Cluster != "acme" || string(bundle.Key) != "a2V5\n" || !strings.Contains(string(bundle.Config), "baseDomain: example.com") || bundle.TunnelAddress != "10.77.0.2" {
		t.Fatalf("bundle %+v", bundle)
	}
	if len(bundle.Files) != 2 || string(bundle.Files[dir+"/hetzner.token"]) != "hcloud\n" || string(bundle.Files[dir+"/ghcr.token"]) != "ghp\n" {
		t.Fatalf("files %v", bundle.Files)
	}
	if _, err := b.Redeem(context.Background(), invite.Secret, "n3"); !errors.Is(err, cluster.ErrInvite) {
		t.Fatalf("a used invitation: %v", err)
	}
	if notes := strings.Join(st.notes, "\n"); !strings.Contains(notes, "cluster.invited a host may join until 2026-11-03T13:00:00Z root") || !strings.Contains(notes, "cluster.invitation-used by n2") {
		t.Fatalf("audit log %s", notes)
	}
	// The next host gets the next address in the tunnel.
	second, _, _ := b.Create(time.Hour, "root")
	next, err := b.Redeem(context.Background(), second.Secret, "n3")
	if err != nil || next.TunnelAddress != "10.77.0.3" {
		t.Fatalf("second host %q, %v", next.TunnelAddress, err)
	}
}

func TestAnInvitationExpiresAndKeepsTakenNames(t *testing.T) {
	b, st, _ := book(t, "")
	invite, _, _ := b.Create(time.Minute, "root")
	if _, err := b.Redeem(context.Background(), invite.Secret, "n1"); err == nil || !strings.Contains(err.Error(), "in the cluster already") {
		t.Fatalf("a taken name: %v", err)
	}
	if _, err := b.Redeem(context.Background(), invite.Secret, "bad name"); err == nil {
		t.Fatal("a bad name was taken")
	}
	// Refusing a name leaves the invitation for a better one.
	clock := time.Date(2026, 11, 3, 12, 2, 0, 0, time.UTC)
	b.Now = func() time.Time { return clock }
	if _, err := b.Redeem(context.Background(), invite.Secret, "n2"); !errors.Is(err, cluster.ErrInvite) {
		t.Fatalf("an expired invitation: %v", err)
	}
	if _, _, err := b.Create(25*time.Hour, "root"); err == nil {
		t.Fatal("an invitation for more than a day")
	}
	st.cluster = ""
	if _, _, err := b.Create(time.Hour, "root"); err == nil || !strings.Contains(err.Error(), "not part of a cluster") {
		t.Fatalf("an invitation outside a cluster: %v", err)
	}
	b2, _, _ := book(t, "")
	b2.Invitation = nil
	if _, _, err := b2.Create(time.Hour, "root"); err == nil {
		t.Fatal("an invitation before the cluster runs")
	}
}

func TestOnlyFilesBesideTheConfigGo(t *testing.T) {
	b, _, _ := book(t, "registryAuthFile: /root/.docker/config.json\n")
	// Said when the invitation is made, not once it is spent.
	if _, _, err := b.Create(time.Hour, "root"); err == nil || !strings.Contains(err.Error(), "is not in") {
		t.Fatalf("a file elsewhere: %v", err)
	}
}

func TestTunnelAddressesSkipThoseInUse(t *testing.T) {
	b, st, _ := book(t, "")
	st.nodes = append(st.nodes, store.Node{Name: "n2", Tunnel: &store.Tunnel{Address: netip.MustParseAddr("10.77.0.2")}})
	invite, _, _ := b.Create(time.Hour, "root")
	bundle, err := b.Redeem(context.Background(), invite.Secret, "n3")
	if err != nil || bundle.TunnelAddress != "10.77.0.3" {
		t.Fatalf("address %q, %v", bundle.TunnelAddress, err)
	}
}
