package main

import (
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/domains"
)

// domainAgent answers the domain calls the CLI makes.
func domainAgent(t *testing.T) (string, *[]string) {
	t.Helper()
	socket := filepath.Join(socketDir(t), "agent.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	var calls []string
	mux := http.NewServeMux()
	domain := api.Domain{Name: "app.customer.com", Tenant: "acme", State: "pending", Detail: "app.customer.com has no DNS records yet",
		NextAt: time.Now().Add(time.Minute), Records: []domains.Record{{Type: "CNAME", Name: "app.customer.com", Value: "acme.example.com"}}}
	mux.HandleFunc("POST /v1/tenants/acme/domains", func(w http.ResponseWriter, r *http.Request) {
		var request api.DomainRequest
		_ = json.NewDecoder(r.Body).Decode(&request)
		calls = append(calls, "add "+request.Name)
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(api.DomainAdded{Domain: domain, Added: true})
	})
	mux.HandleFunc("GET /v1/tenants/acme/domains", func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, "list")
		_ = json.NewEncoder(w).Encode([]api.Domain{domain})
	})
	mux.HandleFunc("DELETE /v1/tenants/acme/domains/{domain}", func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, "remove "+r.PathValue("domain"))
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /v1/tenants/acme/domains/{domain}/check", func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, "check "+r.PathValue("domain"))
		w.WriteHeader(http.StatusNoContent)
	})
	server := &http.Server{Handler: mux}
	go server.Serve(listener)
	t.Cleanup(func() { _ = server.Close() })
	return socket, &calls
}

func TestTenantDomainCommands(t *testing.T) {
	socket, calls := domainAgent(t)
	out := capture(t)
	if err := runTenant([]string{"domain", "add", "acme", "App.Customer.com", "-socket", socket}); err != nil {
		t.Fatal(err)
	}
	if text := out.String(); !strings.Contains(text, "app.customer.com added to acme") || !strings.Contains(text, "app.customer.com  CNAME  acme.example.com") {
		t.Fatalf("add said:\n%s", text)
	}
	out.Reset()
	if err := runTenant([]string{"domain", "list", "acme", "-socket", socket}); err != nil {
		t.Fatal(err)
	}
	if text := out.String(); !strings.Contains(text, "DOMAIN") || !strings.Contains(text, "pending") || !strings.Contains(text, "CNAME acme.example.com") || !strings.Contains(text, "next check") {
		t.Fatalf("list said:\n%s", text)
	}
	out.Reset()
	if err := runTenant([]string{"domain", "remove", "acme", "app.customer.com", "-socket", socket}); err == nil || !strings.Contains(err.Error(), "-yes") {
		t.Fatalf("remove without -yes: %v", err)
	}
	if err := runTenant([]string{"domain", "remove", "acme", "app.customer.com", "-yes", "-socket", socket}); err != nil {
		t.Fatal(err)
	}
	if err := runTenant([]string{"domain", "check", "acme", "app.customer.com", "-socket", socket}); err != nil {
		t.Fatal(err)
	}
	if err := runTenant([]string{"domain", "add", "acme", "-socket", socket}); err == nil {
		t.Fatal("add without a domain")
	}
	want := []string{"add App.Customer.com", "list", "remove app.customer.com", "check app.customer.com"}
	if strings.Join(*calls, "|") != strings.Join(want, "|") {
		t.Fatalf("calls %v", *calls)
	}
}

func TestANodeAlonesDomainsPointAtItsConfiguredAddresses(t *testing.T) {
	cfg := config.Agent{BaseDomain: "murder.internal", PublicAddresses: []string{"203.0.113.10", "2001:db8::10", "203.0.113.10"}}
	cfg.Edge.HTTP = ":80"
	manager := newDomainManager(cfg, nil, nil, nil, slog.New(slog.DiscardHandler))
	if manager.Public == nil {
		t.Fatal("a node alone's manager has no public addresses")
	}
	got := manager.Public()
	if len(got) != 2 || got[0].String() != "203.0.113.10" || got[1].String() != "2001:db8::10" {
		t.Fatalf("public = %v", got)
	}
}
