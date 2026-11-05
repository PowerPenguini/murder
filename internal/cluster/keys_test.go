package cluster

import (
	"bytes"
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/rotation"
	"github.com/PowerPenguini/murder/internal/secrets"
	"github.com/PowerPenguini/murder/internal/store"
)

// keyFile is a master key ring of one key, filled with fill, in path.
func keyFile(t *testing.T, path string, fill byte) *secrets.Key {
	t.Helper()
	if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{fill}, 32))+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	key, err := secrets.LoadKey(path)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

// nodeKeys is a test node's ring, followed as the agent follows it.
type nodeKeys struct{ n *node }

func (k nodeKeys) path() string { return filepath.Join(k.n.dir, "master.key") }

func (k nodeKeys) Ring() []secrets.Entry { return k.n.key.Ring() }
func (k nodeKeys) Add(raw []byte) error  { return k.after(secrets.AddNext(k.path(), raw)) }
func (k nodeKeys) Use(id string) error   { return k.after(secrets.Use(k.path(), id)) }
func (k nodeKeys) Drop(id string) error  { return k.after(secrets.Drop(k.path(), id)) }

func (k nodeKeys) after(err error) error {
	if err != nil {
		return err
	}
	if changed, err := k.n.key.Reload(); err != nil || !changed {
		return err
	}
	return k.n.cluster.Rekey(k.n.key.DeriveAll("cluster ca"))
}

// ring is a node's ring as a rotation run from from sees it.
type ring struct {
	from *node
	to   string
}

func (r ring) Ring(ctx context.Context) ([]secrets.Entry, error) {
	return r.from.cluster.KeyRingOn(ctx, r.to)
}

func (r ring) Add(ctx context.Context, raw []byte) error {
	_, err := r.from.cluster.AddKeyOn(ctx, r.to, raw)
	return err
}

func (r ring) Use(ctx context.Context, id string) error {
	_, err := r.from.cluster.UseKeyOn(ctx, r.to, id)
	return err
}

func (r ring) Drop(ctx context.Context, id string) error {
	_, err := r.from.cluster.DropKeyOn(ctx, r.to, id)
	return err
}

func TestTheMasterKeyRotatesWhileTheClusterRuns(t *testing.T) {
	nodes := threeNodes(t)
	from := nodes[1]
	if _, err := nodes[0].store.PutApp(store.App{Name: "web", Spec: appSpec()}, "alice"); err != nil {
		t.Fatal(err)
	}
	before, err := nodes[0].store.Create(store.NewTenant{Name: "acme", App: "web"}, "alice")
	if err != nil {
		t.Fatal(err)
	}
	old := from.key.Current()

	// Writes go on from every node while the key rotates.
	done := make(chan struct{})
	writes := make(chan error, 1)
	go func() {
		defer close(writes)
		for i := 0; ; i++ {
			select {
			case <-done:
				return
			default:
			}
			if err := nodes[i%3].store.Note("test.write", "during the rotation", "test"); err != nil {
				writes <- err
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	r := &rotation.Rotation{
		Nodes:  []string{"fsn-1", "nbg-1", "hel-1"},
		On:     func(node string) rotation.Ring { return ring{from: from, to: node} },
		Raw:    from.key.Raw,
		Settle: 200 * time.Millisecond,
		Reseal: func(string) (int, error) {
			return from.store.ResealSecrets(from.key.Reseal, "alice")
		},
	}
	result, err := r.Run(ctx(t))
	close(done)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-writes; err != nil {
		t.Fatalf("a write during the rotation: %v", err)
	}
	if result.Key == old || result.Resealed != 1 || !slices.Equal(result.Dropped, []string{old}) {
		t.Fatalf("result %+v", result)
	}
	for _, n := range nodes {
		if ring := n.key.Ring(); len(ring) != 1 || ring[0].ID != result.Key {
			t.Fatalf("%s: %+v", n.name, ring)
		}
	}
	sameState(t, nodes)
	for _, n := range nodes {
		record, _ := n.store.Get("acme")
		if record.Sealed["SESSION"] == before.Sealed["SESSION"] {
			t.Fatalf("%s: the secret was not sealed again", n.name)
		}
		if _, err := n.key.Open(secrets.Context("acme", "SESSION"), record.Sealed["SESSION"]); err != nil {
			t.Fatalf("%s: %v", n.name, err)
		}
	}
	// New connections work with the new key alone.
	for _, n := range nodes {
		n.cluster.client.CloseIdleConnections()
	}
	if _, err := nodes[2].store.Create(store.NewTenant{Name: "beta", App: "web"}, "bob"); err != nil {
		t.Fatalf("a write after the rotation: %v", err)
	}
	// A node with the old key alone is a stranger now.
	stranger, err := NewCredentials(masterKey(t, 7).DeriveAll("cluster ca"), "fsn-1", nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if conn, err := dial(ctx(t), stranger, protocolHTTP, nodes[0].address); err == nil {
		_ = conn.Close()
		t.Fatal("a node with the old key connected")
	}
}
