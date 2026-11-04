package invite

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/store"
)

type fakeStore struct {
	frozen     string
	cluster    string
	nodes      []store.Node
	notes      []string
	admissions []store.Admission
}

// admit is what the leader does as it admits a host: here, it records it.
func (f *fakeStore) admit(_ context.Context, name string, key ed25519.PublicKey) error {
	f.admissions = append(f.admissions, store.Admission{Name: name, Key: cluster.EncodeKey(key)})
	return nil
}

func (f *fakeStore) Frozen() string      { return f.frozen }
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
	st := &fakeStore{cluster: "acme", nodes: []store.Node{{Name: "n1", Key: "bjE="}}}
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	b := &Book{ConfigPath: filepath.Join(dir, "agent.yaml"), Store: st, Now: func() time.Time { return now }, Admit: st.admit,
		Invitation: func(secret []byte) cluster.Invite {
			return cluster.Invite{Address: "10.0.0.2:7400", Secret: secret, CA: make(ed25519.PublicKey, ed25519.PublicKeySize)}
		}}
	return b, st, dir
}

// hostKey is a host's identity key, made of fill.
func hostKey(fill byte) ed25519.PublicKey {
	return ed25519.PublicKey(bytes.Repeat([]byte{fill}, ed25519.PublicKeySize))
}

func TestAnInvitationIsTakenOnce(t *testing.T) {
	b, st, dir := book(t, "")
	invite, expires, err := b.Create(time.Hour, "root")
	if err != nil || !expires.Equal(time.Date(2026, 11, 3, 13, 0, 0, 0, time.UTC)) || len(invite.Secret) != 20 {
		t.Fatalf("invite %+v until %v, %v", invite, expires, err)
	}
	// A host of a release before identity keys sends none: refused, and
	// the invitation stays.
	if _, err := b.Redeem(context.Background(), invite.Secret, "n2", nil); err == nil || !strings.Contains(err.Error(), "no identity key") {
		t.Fatalf("a host without a key: %v", err)
	}
	bundle, err := b.Redeem(context.Background(), invite.Secret, "n2", hostKey(2))
	if err != nil {
		t.Fatal(err)
	}
	if bundle.Cluster != "acme" || string(bundle.Key) != "a2V5\n" || !strings.Contains(string(bundle.Config), "baseDomain: example.com") || !tunnelAddress(bundle.TunnelAddress, "10.77.0.1") {
		t.Fatalf("bundle %+v", bundle)
	}
	// The leader admits the host with its key; the host learns the
	// members' keys.
	if len(st.admissions) != 1 || st.admissions[0].Name != "n2" || st.admissions[0].Key != cluster.EncodeKey(hostKey(2)) {
		t.Fatalf("admissions %+v", st.admissions)
	}
	if len(bundle.Members) != 1 || bundle.Members["n1"] != "bjE=" {
		t.Fatalf("members %v", bundle.Members)
	}
	if len(bundle.Files) != 2 || string(bundle.Files[dir+"/hetzner.token"]) != "hcloud\n" || string(bundle.Files[dir+"/ghcr.token"]) != "ghp\n" {
		t.Fatalf("files %v", bundle.Files)
	}
	if _, err := b.Redeem(context.Background(), invite.Secret, "n3", hostKey(3)); !errors.Is(err, cluster.ErrInvite) {
		t.Fatalf("a used invitation: %v", err)
	}
	if notes := strings.Join(st.notes, "\n"); !strings.Contains(notes, "cluster.invited a host may join until 2026-11-03T13:00:00Z root") || !strings.Contains(notes, "cluster.invitation-used by n2") {
		t.Fatalf("audit log %s", notes)
	}
	// The next host gets the next address in the tunnel.
	second, _, _ := b.Create(time.Hour, "root")
	next, err := b.Redeem(context.Background(), second.Secret, "n3", hostKey(3))
	if err != nil || !tunnelAddress(next.TunnelAddress, "10.77.0.1", bundle.TunnelAddress) {
		t.Fatalf("second host %q, %v", next.TunnelAddress, err)
	}
}

func TestAnInvitationExpiresAndKeepsTakenNames(t *testing.T) {
	b, st, _ := book(t, "")
	invite, _, _ := b.Create(time.Minute, "root")
	if _, err := b.Redeem(context.Background(), invite.Secret, "n1", hostKey(1)); err == nil || !strings.Contains(err.Error(), "in the cluster already") {
		t.Fatalf("a taken name: %v", err)
	}
	// A node the state names but the voters do not, as in a cluster
	// rebuilt from a backup, may be let in again.
	b.Voter = func(string) bool { return false }
	if _, err := b.Redeem(context.Background(), invite.Secret, "n1", hostKey(1)); err != nil {
		t.Fatalf("a node of the state that is no voter: %v", err)
	}
	invite, _, _ = b.Create(time.Minute, "root")
	b.Voter = func(name string) bool { return name == "n1" }
	if _, err := b.Redeem(context.Background(), invite.Secret, "n1", hostKey(1)); err == nil || !strings.Contains(err.Error(), "in the cluster already") {
		t.Fatalf("a voter's name: %v", err)
	}
	if _, err := b.Redeem(context.Background(), invite.Secret, "bad name", hostKey(1)); err == nil {
		t.Fatal("a bad name was taken")
	}
	// Refusing a name leaves the invitation for a better one.
	clock := time.Date(2026, 11, 3, 12, 2, 0, 0, time.UTC)
	b.Now = func() time.Time { return clock }
	if _, err := b.Redeem(context.Background(), invite.Secret, "n2", hostKey(2)); !errors.Is(err, cluster.ErrInvite) {
		t.Fatalf("an expired invitation: %v", err)
	}
	if _, _, err := b.Create(25*time.Hour, "root"); err == nil {
		t.Fatal("an invitation for more than a day")
	}
	st.frozen = "master key rotation from n1"
	if _, _, err := b.Create(time.Hour, "root"); err == nil || !strings.Contains(err.Error(), "takes no new node now") {
		t.Fatalf("an invitation during a rotation: %v", err)
	}
	st.frozen = ""
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
	// Every address but one is taken: that one is given.
	for i := 2; i < 100; i++ {
		if i != 57 {
			st.nodes = append(st.nodes, store.Node{Name: fmt.Sprintf("taken-%d", i), Tunnel: &store.Tunnel{Address: netip.MustParseAddr(fmt.Sprintf("10.77.0.%d", i))}})
		}
	}
	invite, _, _ := b.Create(time.Hour, "root")
	bundle, err := b.Redeem(context.Background(), invite.Secret, "n3", hostKey(3))
	if err != nil || bundle.TunnelAddress != "10.77.0.57" {
		t.Fatalf("address %q, %v", bundle.TunnelAddress, err)
	}
}

// tunnelAddress reports whether address is a node's in the tunnel's
// network, below devices', and none of taken.
func tunnelAddress(address string, taken ...string) bool {
	a, err := netip.ParseAddr(address)
	return err == nil && netip.MustParsePrefix("10.77.0.0/24").Contains(a) && a.As4()[3] >= 2 && a.As4()[3] < 100 && !slices.Contains(taken, address)
}

// The leader has the last word on an admission: one it refuses, say for a
// name that became a voter meanwhile, spends no invitation.
func TestAnAdmissionTheLeaderRefusesKeepsTheInvitation(t *testing.T) {
	b, st, _ := book(t, "")
	invite, _, _ := b.Create(time.Hour, "root")
	b.Admit = func(context.Context, string, ed25519.PublicKey) error {
		return fmt.Errorf("%w: a node named n2 is in the cluster already", store.ErrConflict)
	}
	if _, err := b.Redeem(context.Background(), invite.Secret, "n2", hostKey(2)); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("a refused admission: %v", err)
	}
	b.Admit = st.admit
	if _, err := b.Redeem(context.Background(), invite.Secret, "n3", hostKey(3)); err != nil {
		t.Fatalf("the invitation after a refused admission: %v", err)
	}
}
