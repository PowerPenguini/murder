package records

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/libdns/libdns"
)

func TestWildcard(t *testing.T) {
	for _, c := range []struct{ base, zone, want string }{
		{"example.com", "example.com", "*"},
		{"apps.example.com.", "Example.com", "*.apps"},
		{"a.b.example.com", "example.com", "*.a.b"},
	} {
		if got, err := Wildcard(c.base, c.zone); err != nil || got != c.want {
			t.Errorf("Wildcard(%q, %q) = %q, %v; want %q", c.base, c.zone, got, err, c.want)
		}
	}
	for _, c := range [][2]string{{"example.com", "other.com"}, {"badexample.com", "example.com"}, {"example.com", ""}} {
		if _, err := Wildcard(c[0], c[1]); err == nil {
			t.Errorf("Wildcard(%q, %q) took it", c[0], c[1])
		}
	}
}

// fakeDNS is the Hetzner Cloud DNS API for one zone.
type fakeDNS struct {
	mu      sync.Mutex
	rrsets  map[string][]string // "name/type" → values
	ttl     map[string]int
	calls   []string
	actions map[string]int // id → reads left until success
	fail    bool
}

func (f *fakeDNS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Header.Get("Authorization") != "Bearer secret" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	f.calls = append(f.calls, r.Method+" "+r.URL.Path)
	path := strings.TrimPrefix(r.URL.Path, "/zones/example.com/rrsets")
	action := func() {
		id := "7"
		f.actions[id] = 2
		status := "running"
		_ = json.NewEncoder(w).Encode(map[string]any{"action": map[string]any{"id": 7, "status": status}})
	}
	var body struct {
		Name, Type string
		TTL        int
		Records    []struct{ Value string }
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	values := func() []string {
		var out []string
		for _, record := range body.Records {
			out = append(out, record.Value)
		}
		return out
	}
	switch {
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/actions/"):
		left := f.actions["7"]
		status := "running"
		if left <= 1 {
			status = "success"
			if f.fail {
				status = "error"
			}
		}
		f.actions["7"] = left - 1
		answer := map[string]any{"id": 7, "status": status}
		if status == "error" {
			answer["error"] = map[string]string{"code": "boom", "message": "it broke"}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"action": answer})
	case r.Method == http.MethodGet:
		key := strings.TrimPrefix(path, "/")
		current, ok := f.rrsets[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"code":"not_found","message":"no"}}`))
			return
		}
		var records []map[string]string
		for _, value := range current {
			records = append(records, map[string]string{"value": value})
		}
		ttl := any(nil)
		if value, ok := f.ttl[key]; ok {
			ttl = value
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"rrset": map[string]any{"records": records, "ttl": ttl}})
	case r.Method == http.MethodPost && path == "":
		f.rrsets[body.Name+"/"+body.Type] = values()
		f.ttl[body.Name+"/"+body.Type] = body.TTL
		action()
	case r.Method == http.MethodPost && strings.HasSuffix(path, "/actions/change_ttl"):
		f.ttl[strings.TrimSuffix(strings.TrimPrefix(path, "/"), "/actions/change_ttl")] = body.TTL
		action()
	case r.Method == http.MethodPost && strings.HasSuffix(path, "/actions/set_records"):
		key := strings.TrimSuffix(strings.TrimPrefix(path, "/"), "/actions/set_records")
		f.rrsets[key] = values()
		action()
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func addrs(values ...string) []netip.Addr {
	var out []netip.Addr
	for _, value := range values {
		out = append(out, netip.MustParseAddr(value))
	}
	return out
}

// The records are created, replaced in one action when they differ, and
// left alone when they match; a family without addresses is not touched.
func TestHetznerPublishes(t *testing.T) {
	fake := &fakeDNS{rrsets: map[string][]string{"*/AAAA": {"2001:db8::9"}}, ttl: map[string]int{"*/AAAA": 300}, actions: map[string]int{}}
	server := httptest.NewServer(fake)
	defer server.Close()
	h := &Hetzner{Token: "secret", Zone: "example.com", Name: "*", Endpoint: server.URL, Poll: time.Millisecond}
	ctx := context.Background()
	if err := h.Publish(ctx, addrs("203.0.113.2", "203.0.113.1", "203.0.113.2")); err != nil {
		t.Fatal(err)
	}
	if got := fake.rrsets["*/A"]; !slices.Equal(got, []string{"203.0.113.1", "203.0.113.2"}) || fake.ttl["*/A"] != 60 {
		t.Fatalf("A records %v, ttl %d", got, fake.ttl["*/A"])
	}
	// IPv6 untouched: no IPv6 address was given.
	if got := fake.rrsets["*/AAAA"]; !slices.Equal(got, []string{"2001:db8::9"}) {
		t.Fatalf("AAAA records %v", got)
	}
	fake.calls = nil
	if err := h.Publish(ctx, addrs("203.0.113.1", "203.0.113.2")); err != nil || !slices.Equal(fake.calls, []string{"GET /zones/example.com/rrsets/*/A"}) {
		t.Fatalf("a matching set written: %v, %v", fake.calls, err)
	}
	fake.calls = nil
	if err := h.Publish(ctx, addrs("203.0.113.1", "2001:db8::1")); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(fake.rrsets["*/A"], []string{"203.0.113.1"}) || !slices.Equal(fake.rrsets["*/AAAA"], []string{"2001:db8::1"}) {
		t.Fatalf("records %v", fake.rrsets)
	}
	// The TTL of records already there follows the config too.
	if fake.ttl["*/AAAA"] != 60 {
		t.Fatalf("AAAA ttl %d", fake.ttl["*/AAAA"])
	}
	if !slices.Contains(fake.calls, "POST /zones/example.com/rrsets/*/A/actions/set_records") || !slices.Contains(fake.calls, "GET /actions/7") {
		t.Fatalf("calls %v", fake.calls)
	}
	// A failed action is an error.
	fake.fail = true
	if err := h.Publish(ctx, addrs("203.0.113.3")); err == nil || !strings.Contains(err.Error(), "it broke") {
		t.Fatalf("a failed action: %v", err)
	}
	// Nothing at all is not written.
	fake.calls = nil
	if err := h.Publish(ctx, nil); err != nil || len(fake.calls) != 0 {
		t.Fatalf("no addresses: %v, %v", fake.calls, err)
	}
	// A wrong token is an error.
	h.Token = "wrong"
	if err := h.Publish(ctx, addrs("203.0.113.1")); err == nil {
		t.Fatal("a wrong token published")
	}
}

type fakeSetter struct {
	zone    string
	records []libdns.Record
	zoneNow []libdns.Record
	sets    int
}

func (f *fakeSetter) GetRecords(context.Context, string) ([]libdns.Record, error) {
	return f.zoneNow, nil
}

func (f *fakeSetter) SetRecords(_ context.Context, zone string, records []libdns.Record) ([]libdns.Record, error) {
	f.zone, f.records = zone, records
	f.sets++
	return records, nil
}

func TestNamecheapSetsTheRecordsOfTheName(t *testing.T) {
	fake := &fakeSetter{zoneNow: []libdns.Record{libdns.Address{Name: "www", TTL: time.Hour, IP: netip.MustParseAddr("198.51.100.1")}}}
	n := &Namecheap{Zone: "example.com", Name: "*.apps", api: fake}
	if err := n.Publish(context.Background(), addrs("2001:db8::1", "203.0.113.1")); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, record := range fake.records {
		rr := record.RR()
		got = append(got, rr.Name+" "+rr.Type+" "+rr.Data+" "+rr.TTL.String())
	}
	want := []string{"*.apps A 203.0.113.1 1m0s", "*.apps AAAA 2001:db8::1 1m0s"}
	if fake.zone != "example.com" || !slices.Equal(got, want) {
		t.Fatalf("set %s %v, want %v", fake.zone, got, want)
	}
	// Records already as wanted are not written again: a write sets the
	// domain's whole host list.
	fake.zoneNow = append(fake.zoneNow, fake.records...)
	if err := n.Publish(context.Background(), addrs("203.0.113.1", "2001:db8::1")); err != nil || fake.sets != 1 {
		t.Fatalf("written again: %d sets, %v", fake.sets, err)
	}
}

type stuckAPI struct{}

func (stuckAPI) GetRecords(context.Context, string) ([]libdns.Record, error) {
	select {}
}

func (stuckAPI) SetRecords(context.Context, string, []libdns.Record) ([]libdns.Record, error) {
	select {}
}

// A provider call that never returns, as one without a time limit, still
// ends with the write's context.
func TestNamecheapEndsWithItsContext(t *testing.T) {
	n := &Namecheap{Zone: "example.com", Name: "*", api: stuckAPI{}}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := n.Publish(ctx, addrs("203.0.113.1")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a stuck call: %v", err)
	}
}

func TestRelative(t *testing.T) {
	if got, err := Relative("panel.apps.example.com", "example.com."); err != nil || got != "panel.apps" {
		t.Fatalf("got %q, %v", got, err)
	}
	for _, bad := range [][2]string{{"example.com", "example.com"}, {"panel.other.com", "example.com"}, {"panel.example.com", ""}} {
		if _, err := Relative(bad[0], bad[1]); err == nil {
			t.Errorf("Relative(%q, %q) took it", bad[0], bad[1])
		}
	}
}
