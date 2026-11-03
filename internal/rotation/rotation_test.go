package rotation

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/PowerPenguini/murder/internal/secrets"
)

// fakeRing is a node's ring: keys, the one in use first, with next ones
// marked.
type fakeRing struct {
	name  string
	keys  []string
	next  map[string]bool
	raw   map[string][]byte
	down  bool
	calls *[]string
}

func (f *fakeRing) Ring(context.Context) ([]secrets.Entry, error) {
	if f.down {
		return nil, errors.New("does not answer")
	}
	var out []secrets.Entry
	for i, id := range f.keys {
		state := secrets.StateOld
		switch {
		case i == 0:
			state = secrets.StateCurrent
		case f.next[id]:
			state = secrets.StateNext
		}
		out = append(out, secrets.Entry{ID: id, State: state})
	}
	return out, nil
}

func (f *fakeRing) Add(_ context.Context, raw []byte) error {
	id := secrets.KeyID(raw)
	*f.calls = append(*f.calls, f.name+" add "+id)
	if !slices.Contains(f.keys, id) {
		f.keys = append(f.keys, id)
		f.next[id] = true
		f.raw[id] = raw
	}
	return nil
}

func (f *fakeRing) Use(_ context.Context, id string) error {
	*f.calls = append(*f.calls, f.name+" use "+id)
	at := slices.Index(f.keys, id)
	if at < 0 {
		return errors.New("no such key")
	}
	f.keys = append([]string{id}, slices.Delete(f.keys, at, at+1)...)
	delete(f.next, id)
	return nil
}

func (f *fakeRing) Drop(_ context.Context, id string) error {
	*f.calls = append(*f.calls, f.name+" drop "+id)
	f.keys = slices.DeleteFunc(f.keys, func(k string) bool { return k == id })
	return nil
}

type cluster struct {
	rings map[string]*fakeRing
	calls []string
	nodes []string
}

func newCluster(nodes ...string) *cluster {
	c := &cluster{rings: map[string]*fakeRing{}, nodes: nodes}
	for _, node := range nodes {
		c.rings[node] = &fakeRing{name: node, keys: []string{"old"}, next: map[string]bool{}, raw: map[string][]byte{}, calls: &c.calls}
	}
	return c
}

func (c *cluster) rotation(self string, resealed *string) *Rotation {
	return &Rotation{
		Nodes:  c.nodes,
		On:     func(node string) Ring { return c.rings[node] },
		Raw:    func(id string) ([]byte, bool) { raw, ok := c.rings[self].raw[id]; return raw, ok },
		Settle: 1,
		Reseal: func(key string) (int, error) {
			if c.rings[self].keys[0] != key {
				return 0, errors.New("this node does not use " + key)
			}
			*resealed = key
			c.calls = append(c.calls, "reseal "+key)
			return 3, nil
		},
	}
}

func TestEveryNodeTrustsThenUsesThenDrops(t *testing.T) {
	c := newCluster("n1", "n2", "n3")
	var resealed string
	result, err := c.rotation("n1", &resealed).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	key := result.Key
	want := []string{
		"n1 add " + key, "n2 add " + key, "n3 add " + key,
		"n1 use " + key, "n2 use " + key, "n3 use " + key,
		"reseal " + key,
		"n1 drop old", "n2 drop old", "n3 drop old",
	}
	if !slices.Equal(c.calls, want) {
		t.Fatalf("calls\n%s\nwant\n%s", strings.Join(c.calls, "\n"), strings.Join(want, "\n"))
	}
	if result.Resealed != 3 || !slices.Equal(result.Dropped, []string{"old"}) || len(result.Steps) != 11 {
		t.Fatalf("result %+v", result)
	}
	for _, node := range c.nodes {
		if !slices.Equal(c.rings[node].keys, []string{key}) {
			t.Fatalf("%s: %v", node, c.rings[node].keys)
		}
	}
}

func TestARotationStoppedHalfWayGoesOn(t *testing.T) {
	c := newCluster("n1", "n2")
	var resealed string
	// A rotation does nothing while a node does not answer.
	c.rings["n2"].down = true
	if _, err := c.rotation("n1", &resealed).Run(context.Background()); err == nil || !strings.Contains(err.Error(), "node n2") {
		t.Fatalf("a node down: %v", err)
	}
	raw, _ := secrets.NewRaw()
	id := secrets.KeyID(raw)
	_ = c.rings["n1"].Add(context.Background(), raw)
	// One that made a key and gave it to n1 alone stopped there.
	c.rings["n2"].down = false
	c.calls = nil
	// Run from n2, which lacks the key: it cannot send it.
	if _, err := c.rotation("n2", &resealed).Run(context.Background()); err == nil || !strings.Contains(err.Error(), "run the rotation there") {
		t.Fatalf("from a node without the key: %v", err)
	}
	result, err := c.rotation("n1", &resealed).Run(context.Background())
	if err != nil || result.Key != id || resealed != id {
		t.Fatalf("%+v, %v", result, err)
	}
	want := []string{"n2 add " + id, "n1 use " + id, "n2 use " + id, "reseal " + id, "n1 drop old", "n2 drop old"}
	if !slices.Equal(c.calls, want) {
		t.Fatalf("calls %v", c.calls)
	}
}

func TestTwoRotationsAtOnceStop(t *testing.T) {
	c := newCluster("n1", "n2")
	a, _ := secrets.NewRaw()
	b, _ := secrets.NewRaw()
	_ = c.rings["n1"].Add(context.Background(), a)
	_ = c.rings["n2"].Add(context.Background(), b)
	var resealed string
	if _, err := c.rotation("n1", &resealed).Run(context.Background()); err == nil || !strings.Contains(err.Error(), "several next keys") {
		t.Fatalf("two at once: %v", err)
	}
}

func TestAResealThatFailsKeepsTheOldKeys(t *testing.T) {
	c := newCluster("n1")
	r := c.rotation("n1", new(string))
	r.Reseal = func(string) (int, error) { return 0, errors.New("secret x does not open") }
	if _, err := r.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "the old keys stay") {
		t.Fatalf("%v", err)
	}
	if !slices.Contains(c.rings["n1"].keys, "old") {
		t.Fatal("the old key went")
	}
}
