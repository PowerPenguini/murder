package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/clusterops"
	"github.com/PowerPenguini/murder/internal/reboot"
	"github.com/PowerPenguini/murder/internal/spec"
)

// aloneAgent is the agent of a node in no cluster, which reboots.
type aloneAgent struct {
	mu       sync.Mutex
	frozen   string
	notReady string
	tenants  []api.Tenant
	// requests are the reboot requests it took, in order.
	requests []reboot.Request
}

func (a *aloneAgent) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster":
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"this node is not in a cluster"}`))
	case r.Method == http.MethodGet && r.URL.Path == "/v1/nodes":
		_ = json.NewEncoder(w).Encode(api.Nodes{Self: "solo", Frozen: a.frozen})
	case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
		_ = json.NewEncoder(w).Encode(a.tenants)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/nodes/-/reboot":
		body, _ := io.ReadAll(r.Body)
		var request reboot.Request
		_ = json.Unmarshal(body, &request)
		if a.notReady != "" {
			w.WriteHeader(http.StatusUnprocessableEntity)
			_ = json.NewEncoder(w).Encode(api.ErrorBody{Error: a.notReady})
			return
		}
		a.requests = append(a.requests, request)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"unexpected call"}`))
	}
}

func startAloneAgent(t *testing.T, agent *aloneAgent) *api.Client {
	t.Helper()
	socket := filepath.Join(socketDir(t), "agent.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: agent}
	go server.Serve(listener)
	t.Cleanup(func() { _ = server.Close() })
	return api.NewClient(socket)
}

func aloneTenants() []api.Tenant {
	archived := api.Tenant{Spec: spec.Tenant{Name: "old"}, Node: "solo", ArchivedAt: time.Now()}
	return []api.Tenant{{Spec: spec.Tenant{Name: "shop"}, Node: "solo"}, {Spec: spec.Tenant{Name: "blog"}, Node: "solo"}, archived}
}

// Alone, the node's tenants stop until it is back: it says so, and
// reboots only once told -yes or answered yes.
func TestANodeAloneRebootsOnlyOnceConfirmed(t *testing.T) {
	agent := &aloneAgent{tenants: aloneTenants()}
	client := startAloneAgent(t, agent)
	out := capture(t)
	opts := clusterops.RebootOptions{Wait: 10 * time.Minute}
	err := rebootNode(context.Background(), client, "", opts, false, false)
	if err == nil || !strings.Contains(err.Error(), "its 2 tenants (shop, blog) stop, and its edge answers no request, until it is back") || !strings.Contains(err.Error(), "Add -yes") {
		t.Fatalf("without -yes: %v", err)
	}
	// Only checked whether it would come back.
	if len(agent.requests) != 1 || !agent.requests[0].Check {
		t.Fatalf("requests %+v", agent.requests)
	}
	// Answered no in a terminal.
	before := stdin
	t.Cleanup(func() { stdin = before })
	stdin = strings.NewReader("n\n")
	if err := rebootNode(context.Background(), client, "", opts, false, true); err == nil || err.Error() != "not rebooted" {
		t.Fatalf("answered no: %v", err)
	}
	stdin = strings.NewReader("y\n")
	if err := rebootNode(context.Background(), client, "", opts, false, true); err != nil {
		t.Fatal(err)
	}
	if last := agent.requests[len(agent.requests)-1]; last.Check || last.Away != 10*time.Minute {
		t.Fatalf("last request %+v", last)
	}
	out.Reset()
	if err := rebootNode(context.Background(), client, "solo", opts, true, false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "node solo runs alone") || !strings.Contains(out.String(), "node solo reboots now") {
		t.Fatalf("output:\n%s", out.String())
	}
}

func TestANodeAloneThatCannotRebootSaysWhy(t *testing.T) {
	for _, c := range []struct {
		name  string
		set   func(*aloneAgent)
		node  string
		opts  clusterops.RebootOptions
		wants string
	}{
		{"units off", func(a *aloneAgent) { a.notReady = "murder-agent.service is disabled" }, "", clusterops.RebootOptions{}, "murder-agent.service is disabled"},
		{"frozen", func(a *aloneAgent) { a.frozen = "upgrade to abc" }, "", clusterops.RebootOptions{}, "changes are frozen (upgrade to abc)"},
		{"another node", func(*aloneAgent) {}, "n2", clusterops.RebootOptions{}, "in no cluster with node n2"},
		{"a target", func(*aloneAgent) {}, "", clusterops.RebootOptions{To: "n2"}, "its tenants cannot move to n2"},
	} {
		t.Run(c.name, func(t *testing.T) {
			agent := &aloneAgent{tenants: aloneTenants()}
			c.set(agent)
			client := startAloneAgent(t, agent)
			capture(t)
			err := rebootNode(context.Background(), client, c.node, c.opts, true, false)
			if err == nil || !strings.Contains(err.Error(), c.wants) {
				t.Fatalf("reboot: %v, want %q", err, c.wants)
			}
			for _, request := range agent.requests {
				if !request.Check {
					t.Fatalf("rebooted all the same: %+v", agent.requests)
				}
			}
		})
	}
}

func TestNodeRebootTakesOneNodeAtMost(t *testing.T) {
	capture(t)
	if err := runNode([]string{"reboot", "n1", "n2"}); err == nil {
		t.Fatal("two nodes taken")
	}
}
