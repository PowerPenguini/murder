package cluster

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/PowerPenguini/murder/internal/store"
)

// forged is what whoever holds the master key makes: a certificate of the
// cluster's authority naming node, over a key of their own (identity, or
// one made for this run if nil).
func forged(t *testing.T, member *node, name string, identity ed25519.PrivateKey) *Credentials {
	t.Helper()
	credentials, err := NewCredentials(member.key.DeriveAll("cluster ca"), name, identity, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return credentials
}

// clientOf sends requests with credentials, naming cluster, and checks
// nothing of the node it reaches: what a stranger with the master key
// does.
func clientOf(credentials *Credentials, cluster string) *http.Client {
	return &http.Client{Transport: namedTransport{cluster: func() string { return cluster }, next: &http.Transport{
		DialTLSContext: func(ctx context.Context, _, address string) (net.Conn, error) {
			return dial(ctx, credentials, protocolHTTP, address)
		},
	}}}
}

// status sends a request with client and returns its answer's status.
func status(t *testing.T, client *http.Client, method, url string, body []byte) int {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx(t), method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	return response.StatusCode
}

// statusOf is status, decoding the answer into out.
func statusOf(t *testing.T, client *http.Client, method, url string, body []byte, out any) int {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx(t), method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer response.Body.Close()
	_ = json.NewDecoder(response.Body).Decode(out)
	return response.StatusCode
}

// raftRefused reports whether the node at address closes, unread, a Raft
// connection made with credentials naming cluster.
func raftRefused(t *testing.T, credentials *Credentials, cluster, address string) bool {
	t.Helper()
	conn, err := dial(ctx(t), credentials, protocolRaft, address)
	if err != nil {
		return true
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, clusterHeaderPrefix+cluster+"\n"); err != nil {
		return true
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, err = conn.Read(make([]byte, 1))
	return errors.Is(err, io.EOF)
}

// leaderOf is the node that leads the cluster of nodes.
func leaderOf(t *testing.T, nodes []*node) *node {
	t.Helper()
	var leader *node
	eventually(t, "a leader", func() bool {
		for _, n := range nodes {
			if n.cluster.Leader() {
				leader = n
			}
		}
		return leader != nil
	})
	return leader
}

// A certificate the master key makes, naming a member, does not carry the
// member's identity key: no node takes it for the member.
func TestACertificateMintedWithTheMasterKeyIsRefused(t *testing.T) {
	nodes := threeNodes(t)
	leader := leaderOf(t, nodes)
	cluster := leader.store.Cluster()
	for _, n := range nodes {
		if key, _ := leader.store.NodeKey(n.name); key != EncodeKey(n.cluster.IdentityKey()) {
			t.Fatalf("the state holds %q for %s", key, n.name)
		}
	}
	victim := nodes[1]
	if victim == leader {
		victim = nodes[2]
	}
	mallory, err := NewIdentity()
	if err != nil {
		t.Fatal(err)
	}
	impostor := forged(t, leader, victim.name, mallory)
	client := clientOf(impostor, cluster)
	base := "https://" + leader.address
	batch, _ := store.EncodeBatch(store.Batch{Schema: store.SchemaVersion, Statements: []store.Statement{{SQL: `DELETE FROM nodes`}}})
	for _, request := range []struct{ method, path string }{
		{http.MethodGet, "/cluster/v1/ping"},
		{http.MethodPost, "/cluster/v1/commit"},
		{http.MethodPut, "/cluster/v1/node/binary"},
		{http.MethodGet, "/cluster/v1/snapshot"},
		{http.MethodPost, "/cluster/v1/keys"},
		{http.MethodDelete, "/cluster/v1/nodes/" + nodes[0].name},
	} {
		if got := status(t, client, request.method, base+request.path, batch); got != http.StatusForbidden {
			t.Errorf("%s %s from a forged %s: %d", request.method, request.path, victim.name, got)
		}
	}
	if !raftRefused(t, impostor, cluster, leader.address) {
		t.Error("Raft took a forged certificate")
	}
	// Nor does it join in the member's place, or as a new node.
	join := marshal(joinRequest{Address: "127.0.0.1:1", Check: true})
	if got := status(t, client, http.MethodPost, base+"/cluster/v1/nodes", join); got != http.StatusForbidden && got != http.StatusConflict {
		t.Errorf("a forged join check: %d", got)
	}
	stranger := clientOf(forged(t, leader, "x-1", mallory), cluster)
	if got := status(t, stranger, http.MethodPost, base+"/cluster/v1/nodes", join); got != http.StatusForbidden {
		t.Errorf("a join without an admission: %d", got)
	}
	// A node dialing the forged one does not take it for the member
	// either.
	if err := victim.cluster.knownServer(victim.name, mallory.Public().(ed25519.PublicKey)); err == nil {
		t.Error("a node took a forged certificate for a member's")
	}
	// The member itself, with its identity key, is served.
	if got := status(t, clientOf(victim.cluster.credentials, cluster), http.MethodGet, base+"/cluster/v1/ping", nil); got != http.StatusOK {
		t.Errorf("the member itself: %d", got)
	}
	if len(leader.store.Nodes()) != 3 {
		t.Fatalf("nodes %+v", leader.store.Nodes())
	}
}

// A node removed from the cluster keeps its key and certificate, which
// now match no member: it is refused everywhere, and Raft connections it
// had are closed.
func TestARemovedNodeIsRefused(t *testing.T) {
	nodes := threeNodes(t)
	cluster := nodes[0].store.Cluster()
	removed := nodes[2]
	// A Raft connection it made while a member.
	conn, err := removed.cluster.listener.raftLayer().Dial(raft.ServerAddress(nodes[0].address), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := nodes[0].cluster.Remove(ctx(t), removed.name, "alice"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the removal reaches every node", func() bool {
		return len(nodes[0].store.Nodes()) == 2 && len(nodes[1].store.Nodes()) == 2 && !nodes[0].cluster.isVoter(removed.name)
	})
	client := clientOf(removed.cluster.credentials, cluster)
	removed.cluster.credentials.servers.Store(nil)
	for _, path := range []string{"/cluster/v1/ping", "/cluster/v1/snapshot"} {
		if got := status(t, client, http.MethodGet, "https://"+nodes[1].address+path, nil); got != http.StatusForbidden {
			t.Errorf("GET %s from a removed node: %d", path, got)
		}
	}
	if !raftRefused(t, removed.cluster.credentials, cluster, nodes[1].address) {
		t.Error("Raft took a removed node")
	}
	nodes[0].cluster.listener.sweep(nodes[0].cluster.raftPeer)
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("the removed node's Raft connection: %v", err)
	}
}

// storeInvites admits hosts as an invitation does, through a node's store.
type storeInvites struct {
	n      *node
	secret []byte
}

func (s storeInvites) Redeem(_ context.Context, secret []byte, node string, key ed25519.PublicKey) (Bundle, error) {
	if !bytes.Equal(secret, s.secret) {
		return Bundle{}, ErrInvite
	}
	if err := s.n.cluster.Admit(context.Background(), node, key); err != nil {
		return Bundle{}, err
	}
	return Bundle{Cluster: s.n.store.Cluster(), Members: members(s.n)}, nil
}

// A node joins only once an invitation admitted its name and identity
// key, and only for a while.
func TestAJoinTakesAnInvitation(t *testing.T) {
	worker := newNode(t, "fsn-1")
	secret := bytes.Repeat([]byte{9}, inviteSecretSize)
	worker.stop()
	worker.invites = storeInvites{n: worker, secret: secret}
	worker.start()
	if err := worker.cluster.Bootstrap(ctx(t), "alice", false); err != nil {
		t.Fatal(err)
	}
	if key, _ := worker.store.NodeKey("fsn-1"); key != EncodeKey(worker.cluster.IdentityKey()) {
		t.Fatalf("the bootstrap pinned %q", key)
	}
	// With the master key alone, no join: neither the state nor a place.
	joiner := newNode(t, "nbg-1")
	if err := joiner.cluster.Join(ctx(t), worker.address, members(worker), "mallory"); err == nil || !strings.Contains(err.Error(), "not admitted") {
		t.Fatalf("a join without an admission: %v", err)
	}
	if joiner.store.Cluster() != "" || len(worker.store.Nodes()) != 1 {
		t.Fatalf("the join took the state, or a place: %+v", worker.store.Nodes())
	}
	if err := joiner.cluster.Join(ctx(t), worker.address, nil, "mallory"); err == nil || !strings.Contains(err.Error(), "invitation") {
		t.Fatalf("a join without the members' keys: %v", err)
	}
	// An admission expires.
	if err := worker.store.Admit("nbg-1", EncodeKey(joiner.cluster.IdentityKey()), time.Now().Add(time.Second), worker.cluster.isVoter, "root"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	if err := joiner.cluster.Join(ctx(t), worker.address, members(worker), "alice"); err == nil || !strings.Contains(err.Error(), "not admitted") {
		t.Fatalf("a join on an expired admission: %v", err)
	}
	// A joining node trusts the nodes the invitation names, with their
	// keys, and no other.
	wrong := members(worker)
	wrong["fsn-1"] = EncodeKey(joiner.cluster.IdentityKey())
	if err := worker.store.Admit("nbg-1", EncodeKey(joiner.cluster.IdentityKey()), time.Now().Add(time.Hour), worker.cluster.isVoter, "root"); err != nil {
		t.Fatal(err)
	}
	if err := joiner.cluster.Join(ctx(t), worker.address, wrong, "alice"); err == nil || !strings.Contains(err.Error(), "identity key the invitation named") {
		t.Fatalf("a join trusting another key for the node: %v", err)
	}
	// A node of another master key is refused before the invitation is
	// used, and so is an invitation that names another node than the one at
	// its address.
	stranger := &node{t: t, name: "evil", dir: t.TempDir(), key: masterKey(t, 8), address: "127.0.0.1:0", fatal: make(chan error, 1)}
	if err := store.Init(filepath.Join(stranger.dir, "murder.db"), "evil"); err != nil {
		t.Fatal(err)
	}
	stranger.start()
	t.Cleanup(stranger.stop)
	if err := stranger.cluster.JoinInvited(ctx(t), worker.cluster.Invitation(secret), "mallory"); err == nil || !strings.Contains(err.Error(), "another master key") {
		t.Fatalf("an invitation of another cluster: %v", err)
	}
	misnamed := worker.cluster.Invitation(secret)
	misnamed.Key = joiner.cluster.IdentityKey()
	if err := joiner.cluster.JoinInvited(ctx(t), misnamed, "alice"); err == nil || !strings.Contains(err.Error(), "the node that made the invitation") {
		t.Fatalf("an invitation naming another node: %v", err)
	}
	// With an invitation it joins, its key pinned.
	if err := joiner.cluster.JoinInvited(ctx(t), worker.cluster.Invitation(secret), "alice"); err != nil {
		t.Fatal(err)
	}
	if key, _ := worker.store.NodeKey("nbg-1"); key != EncodeKey(joiner.cluster.IdentityKey()) {
		t.Fatalf("the join pinned %q", key)
	}
	if worker.store.Admitted("nbg-1", EncodeKey(joiner.cluster.IdentityKey()), time.Now()) {
		t.Fatal("the admission was not used up")
	}
	// The key survives a restart.
	before := joiner.cluster.IdentityKey()
	joiner.stop()
	joiner.start()
	if !joiner.cluster.IdentityKey().Equal(before) {
		t.Fatal("the identity key changed with a restart")
	}
	if _, err := worker.store.PutApp(store.App{Name: "web", Spec: appSpec()}, "alice"); err != nil {
		t.Fatal(err)
	}
	sameState(t, []*node{worker, joiner})
}

// A cluster from before identity keys keeps working: nodes the state holds
// no key for are taken by name, until each registers its own, which it does
// once every node runs a release that has keys. From then on the key is
// pinned.
func TestKeysOfAClusterFromBeforeArePinnedOnceRegistered(t *testing.T) {
	nodes := threeNodes(t)
	// The state as a cluster of the release before left it: no keys. No
	// change does that, so each node's database is made so, stopped.
	sameState(t, nodes)
	for _, n := range nodes {
		n.stop()
	}
	for _, n := range nodes {
		unpinned(t, filepath.Join(n.dir, "murder.db"))
		n.unregistered = true
	}
	for _, n := range nodes {
		n.start()
	}
	leader := leaderOf(t, nodes)
	cluster := leader.store.Cluster()
	for _, n := range nodes {
		eventually(t, n.name+" holds no keys", func() bool {
			for _, node := range n.store.Nodes() {
				if node.Key != "" {
					return false
				}
			}
			return true
		})
	}
	victim := nodes[1]
	if victim == leader {
		victim = nodes[2]
	}
	// Until the node registers its key, its name is enough, with a key of
	// a release before (not ed25519) or any other.
	for _, identity := range []ed25519.PrivateKey{nil, mustIdentity(t)} {
		if got := status(t, clientOf(forged(t, leader, victim.name, identity), cluster), http.MethodGet, "https://"+leader.address+"/cluster/v1/ping", nil); got != http.StatusOK {
			t.Fatalf("a node of a cluster from before: %d", got)
		}
	}
	// No key is registered while changes are frozen, as by an upgrade
	// (which thaws once every node runs the new release), and the freeze,
	// with its owner and operation, stays as it is.
	freeze := store.UpgradeFreeze + "0123456789ab from node " + leader.name + ", operation 00aa11bb22cc"
	if err := leader.store.Freeze(freeze, "root"); err != nil {
		t.Fatal(err)
	}
	eventually(t, victim.name+" sees the freeze", func() bool { return victim.store.Frozen() == freeze })
	if done, err := victim.cluster.registerKey(ctx(t)); done || err != nil {
		t.Fatalf("registered under an upgrade's freeze: %v, %v", done, err)
	}
	if key, _ := leader.store.NodeKey(victim.name); key != "" || leader.store.Frozen() != freeze {
		t.Fatalf("under the freeze: key %q, freeze %q", key, leader.store.Frozen())
	}
	if err := leader.store.ThawOwn(freeze, "root"); err != nil {
		t.Fatal(err)
	}
	// A node registers its key only once every node answers that it runs a
	// release that has keys.
	other := nodes[0]
	if other == victim {
		other = nodes[2]
	}
	other.stop()
	if done, err := victim.cluster.registerKey(ctx(t)); done || err != nil {
		t.Fatalf("registered with a node away: %v, %v", done, err)
	}
	other.start()
	eventually(t, victim.name+" registers its key", func() bool {
		done, err := victim.cluster.registerKey(ctx(t))
		return done && err == nil
	})
	want := EncodeKey(victim.cluster.IdentityKey())
	for _, n := range nodes {
		eventually(t, n.name+" pins "+victim.name+"'s key", func() bool {
			key, _ := n.store.NodeKey(victim.name)
			return key == want
		})
	}
	// From then on a certificate without its key is refused; the node
	// itself is served.
	if got := status(t, clientOf(forged(t, leader, victim.name, mustIdentity(t)), cluster), http.MethodGet, "https://"+leader.address+"/cluster/v1/ping", nil); got != http.StatusForbidden {
		t.Fatalf("a forged certificate once the key is pinned: %d", got)
	}
	if got := status(t, clientOf(victim.cluster.credentials, cluster), http.MethodGet, "https://"+leader.address+"/cluster/v1/ping", nil); got != http.StatusOK {
		t.Fatalf("the node itself: %d", got)
	}
	// A key once pinned is not replaced.
	leader = leaderOf(t, nodes)
	if err := leader.store.SetNodeKey(victim.name, EncodeKey(mustIdentity(t).Public().(ed25519.PublicKey)), "mallory"); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("a second key: %v", err)
	}
	// The others register theirs as they go on.
	for _, n := range nodes {
		eventually(t, n.name+" registers its key", func() bool {
			done, err := n.cluster.registerKey(ctx(t))
			return done && err == nil
		})
	}
	leader = leaderOf(t, nodes)
	applied := leader.store.Applied()
	for _, n := range nodes {
		eventually(t, n.name+" applied what the leader did", func() bool { return n.store.Applied() >= applied })
	}
	sameState(t, nodes)
}

// unpinned makes the stopped node's database at path hold no identity
// keys, as one of a cluster from before them does.
func unpinned(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT sql FROM sqlite_master WHERE type = 'trigger' AND tbl_name = 'nodes'`)
	if err != nil {
		t.Fatal(err)
	}
	var triggers []string
	for rows.Next() {
		var trigger string
		if err := rows.Scan(&trigger); err != nil {
			t.Fatal(err)
		}
		triggers = append(triggers, trigger)
	}
	rows.Close()
	if len(triggers) != 2 {
		t.Fatalf("triggers %q", triggers)
	}
	statements := []string{`DROP TRIGGER nodes_key_kept`, `DROP TRIGGER nodes_key_not_replaced`, `UPDATE nodes SET key = ''`}
	for _, statement := range append(statements, triggers...) {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
}

func mustIdentity(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	key, err := NewIdentity()
	if err != nil {
		t.Fatal(err)
	}
	return key
}

// A node keeps its identity key in its cluster directory, private, and
// refuses a damaged one rather than make another.
func TestTheIdentityKeyIsKept(t *testing.T) {
	dir := t.TempDir()
	key, err := LoadIdentity(dir)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(IdentityPath(dir))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("the key file: %v, %v", info, err)
	}
	again, err := LoadIdentity(dir)
	if err != nil || !again.Equal(key) {
		t.Fatalf("read again: %v", err)
	}
	if err := os.WriteFile(IdentityPath(dir), []byte("garbage"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadIdentity(dir); err == nil {
		t.Fatal("a damaged key was taken")
	}
}

// A member is trusted with the state, but a key the state holds stays,
// whoever asks: no batch changes it or the schema that keeps it, and a node
// registers only the key its certificate carries, for its own name.
func TestAMembersBatchKeepsPinnedKeys(t *testing.T) {
	nodes := threeNodes(t)
	leader := leaderOf(t, nodes)
	victim := nodes[0]
	attacker := nodes[1]
	if victim == leader {
		victim = nodes[2]
	}
	if attacker == leader {
		attacker = nodes[2]
	}
	if victim == attacker {
		victim = nodes[0]
		if victim == leader {
			victim = nodes[1]
		}
	}
	key := EncodeKey(mustIdentity(t).Public().(ed25519.PublicKey))
	client := clientOf(attacker.cluster.credentials, leader.store.Cluster())
	for _, statements := range [][]string{
		{"UPDATE nodes SET key = '" + key + "' WHERE name = '" + victim.name + "'"},
		{"DROP TRIGGER nodes_key_kept", "DROP TRIGGER nodes_key_not_replaced", "UPDATE nodes SET key = '" + key + "'"},
		{"UPDATE nodes SET key = '" + key + "' WHERE name = '" + victim.name + "'; COMMIT"},
	} {
		digest, err := leader.store.Digest(ctx(t))
		if err != nil {
			t.Fatal(err)
		}
		batch := store.Batch{Schema: store.SchemaVersion, Version: digest.Version}
		for _, statement := range statements {
			batch.Statements = append(batch.Statements, store.Statement{SQL: statement})
		}
		data, _ := store.EncodeBatch(batch)
		// Refused before the log, or by every node as the log applies it.
		var result commitResult
		got := statusOf(t, client, http.MethodPost, "https://"+leader.address+"/cluster/v1/commit", data, &result)
		if got != http.StatusBadRequest && (got != http.StatusOK || result.Error == "") {
			t.Errorf("%q through commit: %d %+v", statements, got, result)
		}
	}
	for _, n := range nodes {
		if pinned, _ := n.store.NodeKey(victim.name); pinned != EncodeKey(victim.cluster.IdentityKey()) {
			t.Fatalf("%s holds %q for the victim", n.name, pinned)
		}
	}
	// The leader takes a node's own key from the node alone, and only the
	// one its certificate carries.
	impostor := clientOf(forged(t, leader, attacker.name, mustIdentity(t)), leader.store.Cluster())
	if got := status(t, impostor, http.MethodPost, "https://"+leader.address+"/cluster/v1/node/key", marshal(identityRequest{Key: key})); got != http.StatusForbidden {
		t.Errorf("a forged node registers a key: %d", got)
	}
	if got := status(t, client, http.MethodPost, "https://"+leader.address+"/cluster/v1/node/key", marshal(identityRequest{Key: key})); got != http.StatusForbidden {
		t.Errorf("a node registers a key not its own: %d", got)
	}
	// Other changes go through commit as before.
	if _, err := attacker.store.PutApp(store.App{Name: "web", Spec: appSpec()}, "alice"); err != nil {
		t.Fatal(err)
	}
	sameState(t, nodes)
}

// A voter the state does not name, as a removal leaves it between the
// state and the voters, passes by the key it had alone: not by its name,
// on Raft or as a node another one dials.
func TestRaftTakesNoVoterByNameAlone(t *testing.T) {
	nodes := threeNodes(t)
	leader := leaderOf(t, nodes)
	victim := nodes[0]
	if victim == leader {
		victim = nodes[1]
	}
	// As a removal that stopped half way leaves it.
	if err := leader.store.RemoveNode(victim.name, "alice"); err != nil {
		t.Fatal(err)
	}
	if !leader.cluster.isVoter(victim.name) {
		t.Fatal("not a voter")
	}
	forgedKey := mustIdentity(t).Public().(ed25519.PublicKey)
	if err := leader.cluster.raftPeer(victim.name, forgedKey); err == nil {
		t.Fatal("a voter the state does not name was taken with any key")
	}
	if err := leader.cluster.knownServer(victim.name, forgedKey); err == nil {
		t.Fatal("a node dialing took a voter the state does not name")
	}
	if !raftRefused(t, forged(t, leader, victim.name, mustIdentity(t)), leader.store.Cluster(), leader.address) {
		t.Fatal("Raft took a forged certificate naming a voter the state does not name")
	}
	// The node itself, with the key it had, is taken until it is out of the
	// voters too, which a removal asked again finishes.
	if err := leader.cluster.raftPeer(victim.name, victim.cluster.IdentityKey()); err != nil {
		t.Fatalf("the voter on its way out, with its own key: %v", err)
	}
	if err := leader.cluster.Remove(ctx(t), victim.name, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := leader.cluster.raftPeer(victim.name, victim.cluster.IdentityKey()); err == nil {
		t.Fatal("a node out of the voters and the state was taken")
	}
}

// A removal refused while changes are frozen, as by an upgrade, changes
// nothing: not the state, not the voters the upgrade counts on. Once
// changes are taken again it goes through.
func TestARemovalUnderAFreezeChangesNoVoter(t *testing.T) {
	nodes := threeNodes(t)
	leader := leaderOf(t, nodes)
	victim := nodes[0]
	if victim == leader {
		victim = nodes[1]
	}
	freeze := store.UpgradeFreeze + "0123456789ab from node " + leader.name + ", operation 00aa11bb22cc"
	if err := leader.store.Freeze(freeze, "root"); err != nil {
		t.Fatal(err)
	}
	if err := leader.cluster.Remove(ctx(t), victim.name, "root"); err == nil || !strings.Contains(err.Error(), "frozen") {
		t.Fatalf("a removal under a freeze: %v", err)
	}
	if _, named := leader.store.NodeKey(victim.name); !named || !leader.cluster.isVoter(victim.name) {
		t.Fatal("a removal refused under a freeze changed the state or the voters")
	}
	if err := leader.store.ThawOwn(freeze, "root"); err != nil {
		t.Fatal(err)
	}
	if err := leader.cluster.Remove(ctx(t), victim.name, "root"); err != nil {
		t.Fatal(err)
	}
	if _, named := leader.store.NodeKey(victim.name); named || leader.cluster.isVoter(victim.name) {
		t.Fatal("the removal did not finish once changes were taken again")
	}
}

// The leader taken out, from another node or from itself, hands leadership
// over, and the next leader takes it out of the state and the voters.
func TestRemovingTheLeaderFinishes(t *testing.T) {
	for _, from := range []string{"another node", "itself"} {
		t.Run(from, func(t *testing.T) {
			nodes := threeNodes(t)
			leader := leaderOf(t, nodes)
			caller := leader
			if from == "another node" {
				caller = nodes[0]
				if caller == leader {
					caller = nodes[1]
				}
			}
			if err := caller.cluster.Remove(ctx(t), leader.name, "root"); err != nil {
				t.Fatalf("remove the leader from %s: %v", from, err)
			}
			for _, n := range nodes {
				if n == leader {
					continue
				}
				eventually(t, n.name+" knows the leader is gone", func() bool {
					_, named := n.store.NodeKey(leader.name)
					return !named && !n.cluster.isVoter(leader.name)
				})
			}
			// The two left make a cluster that takes changes.
			rest := []*node{}
			for _, n := range nodes {
				if n != leader {
					rest = append(rest, n)
				}
			}
			if _, err := rest[0].store.PutApp(store.App{Name: "web", Spec: appSpec()}, "alice"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// An admission that got in before a node became a voter replaces nothing
// once it is one, and no admission is made for a voter's name.
func TestAnAdmissionDoesNotReplaceAVotersKey(t *testing.T) {
	nodes := threeNodes(t)
	leader := leaderOf(t, nodes)
	victim := nodes[0]
	if victim == leader {
		victim = nodes[1]
	}
	identity := mustIdentity(t)
	key := EncodeKey(identity.Public().(ed25519.PublicKey))
	if err := leader.cluster.Admit(ctx(t), victim.name, identity.Public().(ed25519.PublicKey)); err == nil || !strings.Contains(err.Error(), "in the cluster already") {
		t.Fatalf("an admission for a voter: %v", err)
	}
	// As if the check had run before the node became a voter.
	if err := leader.store.Admit(victim.name, key, time.Now().Add(time.Hour), func(string) bool { return false }, "mallory"); err != nil {
		t.Fatal(err)
	}
	client := clientOf(forged(t, leader, victim.name, identity), leader.store.Cluster())
	for _, check := range []bool{true, false} {
		got := status(t, client, http.MethodPost, "https://"+leader.address+"/cluster/v1/nodes", marshal(joinRequest{Address: victim.address, Check: check}))
		if got == http.StatusOK {
			t.Errorf("a join (check %v) with another key for a voter: %d", check, got)
		}
	}
	if pinned, _ := leader.store.NodeKey(victim.name); pinned != EncodeKey(victim.cluster.IdentityKey()) {
		t.Fatalf("the voter's key is %q", pinned)
	}
}

// A connection to a node is not used once the node left, or another took
// its place: the next request dials again, and checks whom it reached.
func TestARemovedNodesConnectionIsNotReused(t *testing.T) {
	nodes := threeNodes(t)
	leader := leaderOf(t, nodes)
	victim := nodes[0]
	if victim == leader {
		victim = nodes[1]
	}
	var answer releaseAnswer
	if err := leader.cluster.call(ctx(t), victim.address, http.MethodGet, "/cluster/v1/release", nil, &answer); err != nil {
		t.Fatal(err)
	}
	if err := leader.cluster.Remove(ctx(t), victim.name, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := leader.cluster.knownServer(victim.name, victim.cluster.IdentityKey()); err == nil {
		t.Fatal("the removal did not finish")
	}
	if err := leader.cluster.call(ctx(t), victim.address, http.MethodGet, "/cluster/v1/release", nil, &answer); err == nil {
		t.Fatal("a connection to the removed node served a request")
	}
	// A new node takes the name back, with another key: requests reach it,
	// and only it.
	oldKey := victim.cluster.IdentityKey()
	victim.stop()
	again := newNode(t, victim.name)
	if err := again.cluster.Join(ctx(t), leader.address, invite(t, leader, again), "alice"); err != nil {
		t.Fatal(err)
	}
	if err := leader.cluster.Ping(ctx(t), again.name); err != nil {
		t.Fatalf("the node that took the name back: %v", err)
	}
	if err := leader.cluster.knownServer(again.name, oldKey); err == nil {
		t.Fatal("the old key passes for the name")
	}
}

// heldReplicas keeps a copy request open until its body ends.
type heldReplicas struct{ started chan struct{} }

func (h heldReplicas) Snapshots(context.Context, string, string) ([]string, error) { return nil, nil }

func (h heldReplicas) Receive(_ context.Context, _, _ string, _ bool, data io.Reader) error {
	h.started <- struct{}{}
	_, err := io.Copy(io.Discard, data)
	return err
}

// A connection busy as its node leaves goes back to the client's pool once
// its request is done; the next request on it checks the node again, so
// none reaches a node that is no longer a member.
func TestAConnectionBusyAsItsNodeLeavesServesNoMore(t *testing.T) {
	nodes := threeNodes(t)
	leader := leaderOf(t, nodes)
	victim, other := nodes[0], nodes[1]
	if victim == leader {
		victim = nodes[2]
	}
	if other == leader || other == victim {
		other = nodes[2]
		if other == leader || other == victim {
			other = nodes[1]
		}
	}
	held := heldReplicas{started: make(chan struct{}, 1)}
	victim.stop()
	victim.replicas = held
	victim.start()
	eventually(t, victim.name+" answers", func() bool { return leader.cluster.Ping(ctx(t), victim.name) == nil })
	reader, writer := io.Pipe()
	done := make(chan error, 1)
	go func() {
		request, _ := http.NewRequestWithContext(ctx(t), http.MethodPut, "https://"+victim.address+"/cluster/v1/tenants/acme/replica?whole=1", reader)
		response, err := leader.cluster.client.Do(request)
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
		}
		done <- err
	}()
	<-held.started
	if err := leader.cluster.Remove(ctx(t), victim.name, "alice"); err != nil {
		t.Fatal(err)
	}
	// A request elsewhere meanwhile, as other work would make.
	if err := leader.cluster.Ping(ctx(t), other.name); err != nil {
		t.Fatal(err)
	}
	_ = writer.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	var answer releaseAnswer
	if err := leader.cluster.call(ctx(t), victim.address, http.MethodGet, "/cluster/v1/release", nil, &answer); err == nil {
		t.Fatal("a connection to a node no longer a member served a request")
	}
}

// The key a removal keeps for a voter on its way out goes once the node is
// out of the voters: right after the removal, or, for one that stopped
// half way and was finished otherwise, by the leader's upkeep. Only keys
// still needed stay, in the state and its snapshots.
func TestADepartedKeyGoesWithItsVoter(t *testing.T) {
	leader := newNode(t, "leader")
	if err := leader.cluster.Bootstrap(ctx(t), "test", false); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		name := fmt.Sprintf("retired-%d", i)
		if err := leader.store.PutNode(store.Node{Name: name, Address: fmt.Sprintf("127.0.0.1:%d", 9000+i), Key: EncodeKey(mustIdentity(t).Public().(ed25519.PublicKey))}, "test"); err != nil {
			t.Fatal(err)
		}
		if err := leader.cluster.Remove(ctx(t), name, "test"); err != nil {
			t.Fatal(err)
		}
		if _, ok := leader.store.DepartedKey(name); ok {
			t.Errorf("the key of %s stayed after its removal, with no voter", name)
		}
	}
	// A removal that stopped after the state: the upkeep finishes it.
	if err := leader.store.PutNode(store.Node{Name: "stopped", Address: "127.0.0.1:9100", Key: EncodeKey(mustIdentity(t).Public().(ed25519.PublicKey))}, "test"); err != nil {
		t.Fatal(err)
	}
	if err := leader.store.RemoveNode("stopped", "test"); err != nil {
		t.Fatal(err)
	}
	if _, ok := leader.store.DepartedKey("stopped"); !ok {
		t.Fatal("no key kept for a node taken out of the state")
	}
	leader.cluster.forgetDeparted()
	if names := leader.store.Departed(); len(names) != 0 {
		t.Fatalf("keys kept: %v", names)
	}
}

// A voter's key stays while it votes, whatever the upkeep does.
func TestADepartedKeyStaysWhileItsNodeVotes(t *testing.T) {
	nodes := threeNodes(t)
	leader := leaderOf(t, nodes)
	victim := nodes[0]
	if victim == leader {
		victim = nodes[1]
	}
	if err := leader.store.RemoveNode(victim.name, "alice"); err != nil {
		t.Fatal(err)
	}
	leader.cluster.forgetDeparted()
	if key, ok := leader.store.DepartedKey(victim.name); !ok || key != EncodeKey(victim.cluster.IdentityKey()) {
		t.Fatalf("the key of a voter on its way out went: %q, %v", key, ok)
	}
}

// twoNodes starts a cluster of two nodes.
func twoNodes(t *testing.T) []*node {
	t.Helper()
	first := newNode(t, "n0")
	if err := first.cluster.Bootstrap(ctx(t), "alice", false); err != nil {
		t.Fatal(err)
	}
	second := newNode(t, "n1")
	if err := second.cluster.Join(ctx(t), first.address, invite(t, first, second), "alice"); err != nil {
		t.Fatal(err)
	}
	nodes := []*node{first, second}
	for _, n := range nodes {
		eventually(t, n.name+" knows both nodes", func() bool { return len(n.store.Nodes()) == 2 })
	}
	return nodes
}

// A node with no key yet, in a cluster from before keys, whose removal
// stopped after the state, still votes by its name until it is out of the
// voters: a cluster of two restarted then elects a leader, and the removal
// finishes.
func TestAKeylessVoterLeftHalfWayStillVotes(t *testing.T) {
	nodes := twoNodes(t)
	sameState(t, nodes)
	for _, n := range nodes {
		n.stop()
	}
	for _, n := range nodes {
		unpinned(t, filepath.Join(n.dir, "murder.db"))
		n.unregistered = true
	}
	for _, n := range nodes {
		n.start()
	}
	leader := leaderOf(t, nodes)
	victim := nodes[0]
	if victim == leader {
		victim = nodes[1]
	}
	if err := leader.store.RemoveNode(victim.name, "alice"); err != nil {
		t.Fatal(err)
	}
	if key, ok := leader.store.DepartedKey(victim.name); !ok || key != "" {
		t.Fatalf("departed %q, %v", key, ok)
	}
	if err := leader.cluster.raftPeer(victim.name, victim.cluster.IdentityKey()); err != nil {
		t.Fatalf("the voter on its way out: %v", err)
	}
	// Every connection lost: both nodes restart.
	leader.stop()
	victim.stop()
	leader.start()
	victim.start()
	leader = leaderOf(t, nodes)
	survivor := nodes[0]
	if survivor == victim {
		survivor = nodes[1]
	}
	if err := survivor.cluster.Remove(ctx(t), victim.name, "alice"); err != nil {
		t.Fatalf("finish the removal: %v", err)
	}
	if survivor.cluster.isVoter(victim.name) {
		t.Fatal("still a voter")
	}
	if _, ok := survivor.store.DepartedKey(victim.name); ok {
		t.Fatal("the departure stayed")
	}
	if _, err := survivor.store.PutApp(store.App{Name: "web", Spec: appSpec()}, "alice"); err != nil {
		t.Fatal(err)
	}
}

// A node whose removal stopped after the state finishes it itself, asked
// again: it is served for its own removal, and nothing else.
func TestANodeFinishesItsOwnRemoval(t *testing.T) {
	nodes := threeNodes(t)
	leader := leaderOf(t, nodes)
	victim, other := nodes[0], nodes[1]
	if victim == leader {
		victim = nodes[2]
	}
	if other == leader || other == victim {
		other = nodes[2]
		if other == leader || other == victim {
			other = nodes[1]
		}
	}
	if err := leader.store.RemoveNode(victim.name, "alice"); err != nil {
		t.Fatal(err)
	}
	eventually(t, victim.name+" applied its removal from the state", func() bool {
		_, named := victim.store.NodeKey(victim.name)
		return !named
	})
	// Nothing but its own removal.
	client := clientOf(victim.cluster.credentials, leader.store.Cluster())
	victim.cluster.credentials.servers.Store(nil)
	if got := status(t, client, http.MethodGet, "https://"+leader.address+"/cluster/v1/ping", nil); got != http.StatusForbidden {
		t.Errorf("a node on its way out pinged: %d", got)
	}
	if got := status(t, client, http.MethodDelete, "https://"+leader.address+"/cluster/v1/nodes/"+other.name, nil); got != http.StatusForbidden {
		t.Errorf("a node on its way out removed another: %d", got)
	}
	// A forged certificate naming it gets nothing.
	forgedClient := clientOf(forged(t, leader, victim.name, mustIdentity(t)), leader.store.Cluster())
	if got := status(t, forgedClient, http.MethodDelete, "https://"+leader.address+"/cluster/v1/nodes/"+victim.name, nil); got != http.StatusForbidden {
		t.Errorf("a forged certificate removed the node: %d", got)
	}
	victim.cluster.credentials.checkServers(victim.cluster.knownServer)
	if err := victim.cluster.Remove(ctx(t), victim.name, "alice"); err != nil {
		t.Fatalf("the node finishes its own removal: %v", err)
	}
	if leader.cluster.isVoter(victim.name) {
		t.Fatal("still a voter")
	}
	if _, ok := leader.store.DepartedKey(victim.name); ok {
		t.Fatal("the departure stayed")
	}
}

// A node that lost its key file comes back with a new key, given it by an
// operator on another member: it keeps its place, its tenants and their
// data. The old key is refused, the new one taken; not while changes are
// frozen.
func TestALostKeyIsReplacedAndTheNodeKeepsItsTenants(t *testing.T) {
	nodes := threeNodes(t)
	leader := leaderOf(t, nodes)
	victim, other := nodes[0], nodes[1]
	if victim == leader {
		victim = nodes[2]
	}
	if other == leader || other == victim {
		other = nodes[2]
		if other == leader || other == victim {
			other = nodes[1]
		}
	}
	if _, err := leader.store.PutApp(store.App{Name: "web", Spec: appSpec()}, "root"); err != nil {
		t.Fatal(err)
	}
	if _, err := leader.store.Create(store.NewTenant{Name: "acme", App: "web", Node: victim.name}, "root"); err != nil {
		t.Fatal(err)
	}
	eventually(t, victim.name+" has the tenant", func() bool { _, err := victim.store.Get("acme"); return err == nil })
	// The key file is lost; the agent makes another as it starts.
	oldKey := victim.cluster.IdentityKey()
	victim.stop()
	saved := t.TempDir()
	if err := os.Rename(IdentityPath(filepath.Join(victim.dir, "raft")), IdentityPath(saved)); err != nil {
		t.Fatal(err)
	}
	victim.start()
	newKey := victim.cluster.IdentityKey()
	if newKey.Equal(oldKey) {
		t.Fatal("the same key again")
	}
	if err := leader.cluster.Ping(ctx(t), victim.name); err == nil {
		t.Fatal("a node with another key than the one pinned was reached")
	}
	// Removing it is refused: it runs a tenant.
	if err := leader.cluster.Remove(ctx(t), victim.name, "root"); err == nil || !strings.Contains(err.Error(), "runs tenants") {
		t.Fatalf("remove a node that runs tenants: %v", err)
	}
	// Not while changes are frozen.
	freeze := store.UpgradeFreeze + "0123456789ab from node " + leader.name + ", operation 00aa11bb22cc"
	if err := leader.store.Freeze(freeze, "root"); err != nil {
		t.Fatal(err)
	}
	if err := other.cluster.ReplaceKey(ctx(t), victim.name, newKey, "root"); err == nil || !strings.Contains(err.Error(), "frozen") {
		t.Fatalf("a new key under a freeze: %v", err)
	}
	if err := leader.store.ThawOwn(freeze, "root"); err != nil {
		t.Fatal(err)
	}
	// On another member, the operator gives it the key it shows.
	if err := other.cluster.ReplaceKey(ctx(t), victim.name, newKey, "root"); err != nil {
		t.Fatal(err)
	}
	if key, _ := leader.store.NodeKey(victim.name); key != EncodeKey(newKey) {
		t.Fatalf("pinned %q", key)
	}
	eventually(t, victim.name+" is reached again", func() bool { return leader.cluster.Ping(ctx(t), victim.name) == nil })
	// Its tenant stays its own, and it follows the cluster again.
	if tenant, err := leader.store.Get("acme"); err != nil || tenant.Node != victim.name {
		t.Fatalf("tenant %+v, %v", tenant, err)
	}
	if _, err := leader.store.PutApp(store.App{Name: "other", Spec: appSpec()}, "root"); err != nil {
		t.Fatal(err)
	}
	eventually(t, victim.name+" follows the cluster", func() bool { _, err := victim.store.App("other"); return err == nil })
	if tenant, err := victim.store.Get("acme"); err != nil || tenant.Node != victim.name {
		t.Fatalf("tenant on %s: %+v, %v", victim.name, tenant, err)
	}
	// The old key is a stranger.
	old, err := LoadIdentity(saved)
	if err != nil {
		t.Fatal(err)
	}
	if got := status(t, clientOf(forged(t, leader, victim.name, old), leader.store.Cluster()), http.MethodGet, "https://"+leader.address+"/cluster/v1/ping", nil); got != http.StatusForbidden {
		t.Fatalf("the old key: %d", got)
	}
}

// A cluster is rebuilt from a backup: restored on one node, bootstrapped
// there from the restored state under a new name, with that node's key in
// place of the one the backup held; another node joins again with an
// invitation and a new database.
func TestABackupRebuildsTheCluster(t *testing.T) {
	nodes := twoNodes(t)
	first, second := nodes[0], nodes[1]
	if _, err := first.store.PutApp(store.App{Name: "web", Spec: appSpec()}, "root"); err != nil {
		t.Fatal(err)
	}
	if _, err := first.store.Create(store.NewTenant{Name: "acme", App: "web", Node: second.name}, "root"); err != nil {
		t.Fatal(err)
	}
	sameState(t, nodes)
	oldCluster := first.store.Cluster()
	var data bytes.Buffer
	if err := first.store.Backup(&data); err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(t.TempDir(), "backup.db")
	if err := os.WriteFile(backup, data.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	// Every node's cluster directory goes aside.
	for _, n := range nodes {
		n.stop()
		if err := os.Rename(filepath.Join(n.dir, "raft"), filepath.Join(n.dir, "raft.aside")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Restore(backup, filepath.Join(first.dir, "murder.db")); err != nil {
		t.Fatal(err)
	}
	first.start()
	if err := first.cluster.Bootstrap(ctx(t), "root", false); !errors.Is(err, ErrMember) || !strings.Contains(err.Error(), "-restored") {
		t.Fatalf("a bootstrap of restored state without -restored: %v", err)
	}
	if err := first.cluster.Bootstrap(ctx(t), "root", true); err != nil {
		t.Fatal(err)
	}
	if first.store.Cluster() == oldCluster || first.store.Cluster() == "" {
		t.Fatalf("cluster %q", first.store.Cluster())
	}
	if key, _ := first.store.NodeKey(first.name); key != EncodeKey(first.cluster.IdentityKey()) {
		t.Fatalf("pinned %q", key)
	}
	// Done: asked again, it says so.
	if err := first.cluster.Bootstrap(ctx(t), "root", true); !errors.Is(err, ErrMember) {
		t.Fatalf("a bootstrap again: %v", err)
	}
	// The other node joins with a new database and an invitation; its row,
	// tenants and address are as the backup has them.
	if err := os.Remove(filepath.Join(second.dir, "murder.db")); err != nil {
		t.Fatal(err)
	}
	if err := store.Init(filepath.Join(second.dir, "murder.db"), second.name); err != nil {
		t.Fatal(err)
	}
	second.start()
	if err := second.cluster.Join(ctx(t), first.address, invite(t, first, second), "root"); err != nil {
		t.Fatal(err)
	}
	if key, _ := first.store.NodeKey(second.name); key != EncodeKey(second.cluster.IdentityKey()) {
		t.Fatalf("pinned %q for %s", key, second.name)
	}
	if tenant, err := second.store.Get("acme"); err != nil || tenant.Node != second.name {
		t.Fatalf("tenant %+v, %v", tenant, err)
	}
	if _, err := second.store.PutApp(store.App{Name: "other", Spec: appSpec()}, "root"); err != nil {
		t.Fatal(err)
	}
	sameState(t, nodes)
}

// cluster status tells a voter on its way out, with the key Raft takes it
// by, from one that has registered none yet.
func TestStatusShowsAVoterOnItsWayOut(t *testing.T) {
	nodes := threeNodes(t)
	leader := leaderOf(t, nodes)
	victim := nodes[0]
	if victim == leader {
		victim = nodes[1]
	}
	if err := leader.store.RemoveNode(victim.name, "root"); err != nil {
		t.Fatal(err)
	}
	status, err := leader.cluster.Status()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, member := range status.Members {
		switch {
		case member.Name == victim.name:
			found = true
			if !member.Leaving || member.Key != EncodeKey(victim.cluster.IdentityKey()) {
				t.Fatalf("the voter on its way out: %+v", member)
			}
		case member.Leaving || member.Key == "":
			t.Fatalf("a member: %+v", member)
		}
	}
	if !found {
		t.Fatalf("members %+v", status.Members)
	}
}

// The upkeep and a join or removal of the same name do not interleave: a
// departure the upkeep looked at, gone meanwhile as the node came back and
// was taken out again, is not the one it drops.
func TestTheUpkeepKeepsTheDepartureOfANodeThatCameBack(t *testing.T) {
	nodes := threeNodes(t)
	leader := leaderOf(t, nodes)
	// A departure of x-1 with no voter: a removal finished another way.
	if err := leader.store.PutNode(store.Node{Name: "x-1", Address: "127.0.0.1:9", Key: EncodeKey(mustIdentity(t).Public().(ed25519.PublicKey))}, "root"); err != nil {
		t.Fatal(err)
	}
	if err := leader.store.RemoveNode("x-1", "root"); err != nil {
		t.Fatal(err)
	}
	paused, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	hook := func(name string) {
		if name == "x-1" {
			once.Do(func() { close(paused); <-release })
		}
	}
	leader.cluster.departedHook.Store(&hook)
	cleaned := make(chan struct{})
	go func() { leader.cluster.forgetDeparted(); close(cleaned) }()
	<-paused
	// x-1 comes back meanwhile: its admission waits for the upkeep.
	x := newNode(t, "x-1")
	admitted := make(chan error, 1)
	go func() { admitted <- leader.cluster.Admit(context.Background(), x.name, x.cluster.IdentityKey()) }()
	select {
	case err := <-admitted:
		t.Fatalf("an admission went on while the upkeep held the departure: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	close(release)
	<-cleaned
	leader.cluster.departedHook.Store(nil)
	if err := <-admitted; err != nil {
		t.Fatal(err)
	}
	if err := x.cluster.Join(ctx(t), leader.address, members(leader), "root"); err != nil {
		t.Fatal(err)
	}
	// Taken out of the state again, it votes still: its new departure stays
	// through the upkeep, and Raft takes it by its key.
	if err := leader.store.RemoveNode(x.name, "root"); err != nil {
		t.Fatal(err)
	}
	leader.cluster.forgetDeparted()
	if key, ok := leader.store.DepartedKey(x.name); !ok || key != EncodeKey(x.cluster.IdentityKey()) {
		t.Fatalf("the new departure: %q, %v", key, ok)
	}
	if err := leader.cluster.raftPeer(x.name, x.cluster.IdentityKey()); err != nil {
		t.Fatal(err)
	}
	if err := leader.cluster.Remove(ctx(t), x.name, "root"); err != nil {
		t.Fatal(err)
	}
}

// A cluster rebuilt from a backup always gets a new name, even on the node
// the backup came from, with the key it had: a node of the cluster before
// is never taken for one of it.
func TestARestoredClusterGetsANewNameOnItsOwnNode(t *testing.T) {
	n := newNode(t, "first")
	if err := n.cluster.Bootstrap(ctx(t), "root", false); err != nil {
		t.Fatal(err)
	}
	old := n.store.Cluster()
	var backup bytes.Buffer
	if err := n.store.Backup(&backup); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(n.dir, "backup.db")
	if err := os.WriteFile(path, backup.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	n.stop()
	// The Raft state goes; node.key stays.
	dir := filepath.Join(n.dir, "raft")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() != IdentityFile {
			if err := os.RemoveAll(filepath.Join(dir, entry.Name())); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := store.Restore(path, filepath.Join(n.dir, "murder.db")); err != nil {
		t.Fatal(err)
	}
	n.start()
	if err := n.cluster.Bootstrap(ctx(t), "root", true); err != nil {
		t.Fatal(err)
	}
	if n.store.Cluster() == old || n.store.Cluster() == "" {
		t.Fatalf("the rebuilt cluster is named %q, the one before %q", n.store.Cluster(), old)
	}
	if n.cluster.rebuilding() != "" {
		t.Fatal("the rebuild is not done")
	}
	renamed := n.store.Cluster()
	if err := n.cluster.Bootstrap(ctx(t), "root", true); !errors.Is(err, ErrMember) || n.store.Cluster() != renamed {
		t.Fatalf("asked again: %v, named %q", err, n.store.Cluster())
	}
}

// Raft connections with a node given another key, or removed, close, both
// those the node made and those made to it: the node takes no more of the
// log over a connection it had before.
func TestRaftConnectionsGoWithAReplacedOrRemovedKey(t *testing.T) {
	for _, change := range []string{"rekey", "removal"} {
		t.Run(change, func(t *testing.T) {
			nodes := threeNodes(t)
			leader := leaderOf(t, nodes)
			victim := nodes[0]
			if victim == leader {
				victim = nodes[1]
			}
			sameState(t, nodes)
			// A Raft connection the victim made too, as a candidate would.
			made, err := victim.cluster.listener.raftLayer().Dial(raft.ServerAddress(leader.address), 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer made.Close()
			switch change {
			case "rekey":
				if err := leader.cluster.ReplaceKey(ctx(t), victim.name, mustIdentity(t).Public().(ed25519.PublicKey), "root"); err != nil {
					t.Fatal(err)
				}
			case "removal":
				if err := leader.cluster.Remove(ctx(t), victim.name, "root"); err != nil {
					t.Fatal(err)
				}
			}
			for _, n := range nodes {
				if n == victim {
					continue
				}
				eventually(t, n.name+" holds no Raft connection with "+victim.name, func() bool {
					n.cluster.listener.mu.Lock()
					defer n.cluster.listener.mu.Unlock()
					for conn := range n.cluster.listener.raftConns {
						if conn.node == victim.name {
							return false
						}
					}
					return true
				})
			}
			if _, err := leader.store.PutApp(store.App{Name: "after", Spec: appSpec()}, "root"); err != nil {
				t.Fatal(err)
			}
			time.Sleep(2 * time.Second)
			if _, err := victim.store.App("after"); err == nil {
				t.Fatal("the node took the log after its key went")
			}
		})
	}
}

// A rebuild's note is the node's own, a file in its cluster directory: no
// part of the database, which every node holds alike.
func TestTheRebuildNoteIsNoPartOfTheState(t *testing.T) {
	n := newNode(t, "first")
	before := metaKeys(t, filepath.Join(n.dir, "murder.db"))
	if err := beginRebuild(filepath.Join(n.dir, "raft"), "old"); err != nil {
		t.Fatal(err)
	}
	if n.cluster.rebuilding() != "old" {
		t.Fatalf("the note reads %q", n.cluster.rebuilding())
	}
	if after := metaKeys(t, filepath.Join(n.dir, "murder.db")); !slices.Equal(before, after) {
		t.Fatalf("meta keys %v, then %v", before, after)
	}
	if err := endRebuild(filepath.Join(n.dir, "raft")); err != nil || n.cluster.rebuilding() != "" {
		t.Fatalf("the note stays: %v", err)
	}
}

// metaKeys lists the meta keys of the database at path.
func metaKeys(t *testing.T, path string) []string {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT key FROM meta ORDER BY key`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		_ = rows.Scan(&key)
		keys = append(keys, key)
	}
	return keys
}
