package secrets

import (
	"os"
	"strings"
	"testing"
)

func TestARingRotates(t *testing.T) {
	key, path := newTestKey(t)
	old := key.Current()
	sealed, _ := key.Seal(Context("acme", "TOKEN"), "s3cret")
	raw, err := NewRaw()
	if err != nil {
		t.Fatal(err)
	}
	next := KeyID(raw)
	if err := AddNext(path, raw); err != nil {
		t.Fatal(err)
	}
	if err := AddNext(path, raw); err != nil {
		t.Fatal(err)
	}
	if changed, err := key.Reload(); err != nil || !changed {
		t.Fatalf("reload %v, %v", changed, err)
	}
	if ring := key.Ring(); len(ring) != 2 || ring[0] != (Entry{ID: old, State: StateCurrent}) || ring[1] != (Entry{ID: next, State: StateNext}) {
		t.Fatalf("ring %+v", ring)
	}
	if seeds := key.DeriveAll("cluster ca"); len(seeds) != 2 || string(seeds[0]) != string(key.Derive("cluster ca")) {
		t.Fatal("derived seeds")
	}
	// Still sealed with the key in use.
	if again, changed, err := key.Reseal(Context("acme", "TOKEN"), sealed); err != nil || changed || again != sealed {
		t.Fatalf("reseal before use: %v, %v", changed, err)
	}

	if err := Use(path, next); err != nil {
		t.Fatal(err)
	}
	if _, err := key.Reload(); err != nil {
		t.Fatal(err)
	}
	if ring := key.Ring(); ring[0] != (Entry{ID: next, State: StateCurrent}) || ring[1] != (Entry{ID: old, State: StateOld}) {
		t.Fatalf("ring %+v", ring)
	}
	if plain, err := key.Open(Context("acme", "TOKEN"), sealed); err != nil || plain != "s3cret" {
		t.Fatalf("an older key's value: %q, %v", plain, err)
	}
	resealed, changed, err := key.Reseal(Context("acme", "TOKEN"), sealed)
	if err != nil || !changed || resealed == sealed {
		t.Fatalf("reseal: %v, %v", changed, err)
	}
	if err := Drop(path, next); err == nil {
		t.Fatal("dropped the key in use")
	}
	if err := Drop(path, old); err != nil {
		t.Fatal(err)
	}
	if _, err := key.Reload(); err != nil {
		t.Fatal(err)
	}
	if _, err := key.Open(Context("acme", "TOKEN"), sealed); err == nil {
		t.Fatal("a dropped key still opens")
	}
	if plain, err := key.Open(Context("acme", "TOKEN"), resealed); err != nil || plain != "s3cret" {
		t.Fatalf("resealed: %q, %v", plain, err)
	}
	// A ring of one is one line, as before rings.
	data, _ := os.ReadFile(path)
	if strings.Count(string(data), "\n") != 1 || strings.HasPrefix(string(data), "next") {
		t.Fatalf("file %q", data)
	}
	if info, _ := os.Stat(path); info.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v", info.Mode())
	}
	if err := Use(path, "ffffffff"); !IsNoKey(err) {
		t.Fatalf("use of a missing key: %v", err)
	}
}

func TestRingFiles(t *testing.T) {
	a, b := strings.Repeat("A", 43)+"=", strings.Repeat("B", 43)+"="
	for _, bad := range []string{"", "next " + a + "\n", a + "\n" + a + "\n", "short\n"} {
		if _, err := parseRing(bad); err == nil {
			t.Errorf("%q read", bad)
		}
	}
	r, err := parseRing("# a comment\n" + a + "\n\nnext " + b + "\n")
	if err != nil || len(r.keys) != 2 || !r.keys[1].next {
		t.Fatalf("%+v, %v", r, err)
	}
}

func TestDigestFollowsTheValueNotTheKey(t *testing.T) {
	if Digest("acme/A", "x") == Digest("acme/A", "y") || Digest("acme/A", "x") == Digest("beta/A", "x") || Digest("acme/A", "x") != Digest("acme/A", "x") {
		t.Fatal("digests")
	}
}
